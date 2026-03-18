from __future__ import annotations

import asyncio
import heapq
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

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
        current_time = datetime.now(UTC)
        while self.heap and self.heap[0].next_run_at <= current_time:
            item = heapq.heappop(self.heap)
            job = self.jobs.get(item.job_id)
            if job is None:
                continue
            if not job.is_enabled or job.next_run_at is None:
                continue
            if job.updated_at != item.updated_at or job.next_run_at != item.next_run_at:
                continue
            await self.dispatch_job(job)

    async def dispatch_job(self, cached_job: SchedulerJobState) -> None:
        current_time = datetime.now(UTC)
        async with self.session_factory() as session:
            job = await JobRepository.get_by_id(session, cached_job.job_id)
            if job is None:
                await session.rollback()
                self.remove_job(cached_job.job_id)
                return
            if not job.is_enabled or job.next_run_at is None:
                await session.rollback()
                self.upsert_job(job)
                return
            if (
                job.updated_at.astimezone(UTC) != cached_job.updated_at
                or job.next_run_at.astimezone(UTC) != cached_job.next_run_at
            ):
                self.upsert_job(job)
                await session.rollback()
                return

            await self.dispatch_job_in_session(session, job, current_time)
            await session.commit()
            self.upsert_job(job) if job.is_enabled and job.next_run_at is not None else self.remove_job(str(job.id))

    async def dispatch_job_in_session(self, session: AsyncSession, job: SchedulerJob, current_time: datetime) -> None:
        task = self.registry.get_task(job.task_name)
        trigger = Scheduler.build_trigger(job)
        scheduled_for = job.next_run_at

        try:
            broker_task = await task.kiq(*list(job.task_args or []), **dict(job.task_kwargs or {}))
        except Exception as exc:
            job.last_error = str(exc)
            job.next_run_at = current_time + timedelta(seconds=self.config.dispatch_retry_seconds)
            job.updated_at = current_time
            await RunRepository.create(
                session,
                SchedulerRun(
                    job_id=job.id,
                    status="failed",
                    scheduled_for=scheduled_for or current_time,
                    finished_at=current_time,
                    error=str(exc),
                    created_at=current_time,
                    updated_at=current_time,
                ),
            )
            log.exception("Scheduler dispatch failed for %s", job.task_name)
            return

        job.last_error = None
        job.last_run_at = current_time
        job.last_dispatched_task_id = getattr(broker_task, "task_id", None)
        job.dispatch_count += 1
        job.updated_at = current_time

        if job.kind == "one_off":
            job.is_enabled = False
            job.next_run_at = None
        else:
            next_run_at = trigger.get_next_run_at(current_time, anchor=job.created_at)
            job.next_run_at = next_run_at
            if next_run_at is None:
                job.is_enabled = False

        await RunRepository.create(
            session,
            SchedulerRun(
                job_id=job.id,
                status="dispatched",
                scheduled_for=scheduled_for or current_time,
                dispatched_at=current_time,
                finished_at=current_time,
                broker_task_id=getattr(broker_task, "task_id", None),
                created_at=current_time,
                updated_at=current_time,
            ),
        )
