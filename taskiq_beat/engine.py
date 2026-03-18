from __future__ import annotations

import asyncio
import heapq
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from taskiq_beat.config import SchedulerConfig
from taskiq_beat.models import SchedulerJob, SchedulerRun
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository, RunRepository
from taskiq_beat.scheduler import Scheduler

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True, order=True)
class SchedulerHeapItem:
    next_run_at: datetime
    updated_at: datetime
    job_id: str


@dataclass(slots=True, frozen=True)
class SchedulerJobState:
    job_id: str
    is_enabled: bool
    next_run_at: datetime | None
    updated_at: datetime

    @classmethod
    def from_job(cls, job: SchedulerJob) -> "SchedulerJobState":
        next_run_at = job.next_run_at.astimezone(UTC) if job.next_run_at is not None else None
        return cls(
            job_id=str(job.id),
            is_enabled=job.is_enabled,
            next_run_at=next_run_at,
            updated_at=job.updated_at.astimezone(UTC),
        )


@dataclass(slots=True, frozen=True)
class PreparedDispatch:
    job_id: str
    task_name: str
    task: Any
    trigger: Any
    args: list[Any]
    kwargs: dict[str, Any]
    scheduled_for: datetime


@dataclass(slots=True, frozen=True)
class DispatchOutcome:
    job_id: str
    status: str
    broker_task_id: str | None = None
    error: str | None = None
    dispatched_at: datetime | None = None
    finished_at: datetime | None = None


class SchedulerEngine:
    def __init__(
        self,
        *,
        session_factory,
        registry: TaskRegistry,
        config: SchedulerConfig | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.registry = registry
        self.config = config or SchedulerConfig()
        self.jobs: dict[str, SchedulerJobState] = {}
        self.heap: list[SchedulerHeapItem] = []
        self.last_sync_at: datetime = datetime.now(UTC)
        self.stop_event = asyncio.Event()
        self.wakeup_event = asyncio.Event()
        self.runner_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self.runner_task is not None:
            return
        await self.sync_all()
        self.stop_event.clear()
        self.runner_task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self.stop_event.set()
        self.wakeup_event.set()
        if self.runner_task is not None:
            self.runner_task.cancel()
        await asyncio.gather(*(task for task in (self.runner_task,) if task is not None), return_exceptions=True)
        self.runner_task = None

    async def run(self) -> None:
        while not self.stop_event.is_set():
            await self.run_once()
            await self.wait_next_tick()

    async def run_once(self) -> None:
        await self.sync_storage()
        await self.dispatch_due_jobs()

    async def wait_next_tick(self) -> None:
        timeout = self.get_sleep_timeout()
        try:
            await asyncio.wait_for(self.wakeup_event.wait(), timeout=timeout)
        except TimeoutError:
            return
        finally:
            self.wakeup_event.clear()

    def get_sleep_timeout(self) -> float:
        if not self.heap:
            return self.config.sync_interval_seconds
        head = self.heap[0]
        seconds_left = (head.next_run_at - datetime.now(UTC)).total_seconds()
        return max(min(seconds_left, self.config.sync_interval_seconds), self.config.idle_sleep_seconds)

    async def sync_storage(self) -> None:
        current_time = datetime.now(UTC)
        if self.jobs and (current_time - self.last_sync_at).total_seconds() < self.config.sync_interval_seconds:
            return
        await self.sync_all()

    async def sync_all(self) -> None:
        async with self.session_factory() as session:
            active_jobs = await JobRepository.list_active(session)
        self.jobs = {}
        self.heap = []
        self.merge_jobs(active_jobs)
        current_time = datetime.now(UTC)
        self.last_sync_at = current_time

    def merge_jobs(self, jobs: list[SchedulerJob]) -> None:
        for job in jobs:
            state = SchedulerJobState.from_job(job)
            self.jobs[state.job_id] = state
            if state.is_enabled and state.next_run_at is not None:
                heapq.heappush(
                    self.heap,
                    SchedulerHeapItem(
                        next_run_at=state.next_run_at,
                        updated_at=state.updated_at,
                        job_id=state.job_id,
                    ),
                )

    def upsert_job(self, job: SchedulerJob) -> None:
        state = SchedulerJobState.from_job(job)
        self.jobs[state.job_id] = state
        if state.is_enabled and state.next_run_at is not None:
            heapq.heappush(
                self.heap,
                SchedulerHeapItem(
                    next_run_at=state.next_run_at,
                    updated_at=state.updated_at,
                    job_id=state.job_id,
                ),
            )
        self.wakeup_event.set()

    def remove_job(self, job_id: str) -> None:
        self.jobs.pop(job_id, None)
        self.wakeup_event.set()

    async def dispatch_due_jobs(self) -> None:
        while True:
            current_time = datetime.now(UTC)
            due_jobs = self.collect_due_jobs(current_time)
            if not due_jobs:
                return
            await self.dispatch_jobs_batch(due_jobs, current_time)

    def collect_due_jobs(self, current_time: datetime) -> list[SchedulerJobState]:
        due_jobs: list[SchedulerJobState] = []
        while self.heap and self.heap[0].next_run_at <= current_time and len(due_jobs) < self.config.dispatch_batch_size:
            item = heapq.heappop(self.heap)
            job = self.jobs.get(item.job_id)
            if job is None:
                continue
            if not job.is_enabled or job.next_run_at is None:
                continue
            if job.updated_at != item.updated_at or job.next_run_at != item.next_run_at:
                continue
            due_jobs.append(job)
        return due_jobs

    async def dispatch_jobs_batch(self, cached_jobs: list[SchedulerJobState], current_time: datetime) -> None:
        async with self.session_factory() as session:
            jobs = await JobRepository.list_by_ids(session, [job.job_id for job in cached_jobs])
            jobs_by_id = {str(job.id): job for job in jobs}

            runnable_jobs: list[SchedulerJob] = []
            prepared_dispatches: list[PreparedDispatch] = []

            for cached_job in cached_jobs:
                job = jobs_by_id.get(cached_job.job_id)
                if job is None:
                    self.remove_job(cached_job.job_id)
                    continue
                if not job.is_enabled or job.next_run_at is None:
                    self.upsert_job(job)
                    continue
                if (
                    job.updated_at.astimezone(UTC) != cached_job.updated_at
                    or job.next_run_at.astimezone(UTC) != cached_job.next_run_at
                ):
                    self.upsert_job(job)
                    continue

                runnable_jobs.append(job)
                prepared_dispatches.append(
                    PreparedDispatch(
                        job_id=str(job.id),
                        task_name=job.task_name,
                        task=self.registry.get_task(job.task_name),
                        trigger=Scheduler.build_trigger(job),
                        args=list(job.task_args or []),
                        kwargs=dict(job.task_kwargs or {}),
                        scheduled_for=job.next_run_at,
                    )
                )

            if not prepared_dispatches:
                await session.rollback()
                return

            outcomes = await self.execute_prepared_dispatches(prepared_dispatches)
            runs_to_create: list[SchedulerRun] = []

            for job, prepared, outcome in zip(runnable_jobs, prepared_dispatches, outcomes, strict=True):
                self.apply_dispatch_outcome(
                    job=job,
                    prepared=prepared,
                    outcome=outcome,
                    current_time=current_time,
                    runs_to_create=runs_to_create,
                )

            if self.config.record_runs:
                await RunRepository.create_many(session, runs_to_create)

            await session.commit()

            for job in runnable_jobs:
                self.upsert_job(job) if job.is_enabled and job.next_run_at is not None else self.remove_job(str(job.id))

    async def execute_prepared_dispatches(self, prepared_dispatches: list[PreparedDispatch]) -> list[DispatchOutcome]:
        semaphore = asyncio.Semaphore(self.config.dispatch_concurrency)

        async def run_single(prepared: PreparedDispatch) -> DispatchOutcome:
            async with semaphore:
                dispatched_at = datetime.now(UTC)
                try:
                    broker_task = await prepared.task.kiq(*prepared.args, **prepared.kwargs)
                except Exception as exc:
                    finished_at = datetime.now(UTC)
                    log.exception("Scheduler dispatch failed for %s", prepared.task_name)
                    return DispatchOutcome(
                        job_id=prepared.job_id,
                        status="failed",
                        error=str(exc),
                        dispatched_at=dispatched_at,
                        finished_at=finished_at,
                    )
                finished_at = datetime.now(UTC)
                return DispatchOutcome(
                    job_id=prepared.job_id,
                    status="dispatched",
                    broker_task_id=getattr(broker_task, "task_id", None),
                    dispatched_at=dispatched_at,
                    finished_at=finished_at,
                )

        return list(await asyncio.gather(*(run_single(prepared) for prepared in prepared_dispatches)))

    def apply_dispatch_outcome(
        self,
        *,
        job: SchedulerJob,
        prepared: PreparedDispatch,
        outcome: DispatchOutcome,
        current_time: datetime,
        runs_to_create: list[SchedulerRun],
    ) -> None:
        if outcome.status == "failed":
            retry_from = outcome.finished_at or outcome.dispatched_at or current_time
            job.last_error = outcome.error
            job.next_run_at = retry_from + timedelta(seconds=self.config.dispatch_retry_seconds)
            job.updated_at = retry_from
            if self.config.record_runs:
                runs_to_create.append(
                    SchedulerRun(
                        job_id=job.id,
                        status="failed",
                        scheduled_for=prepared.scheduled_for,
                        dispatched_at=outcome.dispatched_at,
                        finished_at=outcome.finished_at or retry_from,
                        error=outcome.error,
                        created_at=retry_from,
                        updated_at=retry_from,
                    )
                )
            return

        dispatched_at = outcome.dispatched_at or current_time
        finished_at = outcome.finished_at or dispatched_at
        job.last_error = None
        job.last_run_at = dispatched_at
        job.last_dispatched_task_id = outcome.broker_task_id
        job.dispatch_count += 1
        job.updated_at = finished_at

        if job.kind == "one_off":
            job.is_enabled = False
            job.next_run_at = None
        else:
            next_run_at = prepared.trigger.get_next_run_at(dispatched_at, anchor=job.created_at)
            job.next_run_at = next_run_at
            if next_run_at is None:
                job.is_enabled = False

        if self.config.record_runs:
            runs_to_create.append(
                SchedulerRun(
                    job_id=job.id,
                    status="dispatched",
                    scheduled_for=prepared.scheduled_for,
                    dispatched_at=dispatched_at,
                    finished_at=finished_at,
                    broker_task_id=outcome.broker_task_id,
                    created_at=finished_at,
                    updated_at=finished_at,
                )
            )
