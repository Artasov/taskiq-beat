from __future__ import annotations

import asyncio
import heapq
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskiq_beat.config import SchedulerConfig
from taskiq_beat.datetime_utils import normalize_utc
from taskiq_beat.models import SchedulerJob, SchedulerRun
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository, RunRepository
from taskiq_beat.scheduler import Scheduler, SchedulerTrigger
from taskiq_beat.triggers import PeriodicSchedule
from taskiq_beat.types import TaskiqTask

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class SchedulerHeapItem:
    """Heap entry used to wake the engine when a job may become runnable."""

    wake_at: datetime
    updated_at: datetime
    job_id: str

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SchedulerHeapItem):
            return NotImplemented
        if self.wake_at != other.wake_at:
            return self.wake_at < other.wake_at
        if self.updated_at != other.updated_at:
            return self.updated_at < other.updated_at
        return self.job_id < other.job_id


@dataclass(slots=True, frozen=True)
class SchedulerJobState:
    """Compact in-memory snapshot of the fields relevant for dispatch."""

    job_id: str
    is_enabled: bool
    next_run_at: datetime | None
    updated_at: datetime
    claimed_by: str | None
    claim_expires_at: datetime | None

    @classmethod
    def from_job(cls, job: SchedulerJob) -> SchedulerJobState:
        """Project a database model into the engine's lightweight state."""
        next_run_at = normalize_utc(job.next_run_at) if job.next_run_at is not None else None
        return cls(
            job_id=str(job.id),
            is_enabled=job.is_enabled,
            next_run_at=next_run_at,
            updated_at=normalize_utc(job.updated_at),
            claimed_by=job.claimed_by,
            claim_expires_at=normalize_utc(job.claim_expires_at) if job.claim_expires_at is not None else None,
        )

    def get_wake_at(self, scheduler_id: str) -> datetime | None:
        """Return when this scheduler should wake up for the job again."""
        if not self.is_enabled or self.next_run_at is None:
            return None
        if self.claimed_by and self.claimed_by != scheduler_id and self.claim_expires_at is not None:
            return max(self.next_run_at, self.claim_expires_at)
        return self.next_run_at


@dataclass(slots=True, frozen=True)
class PreparedDispatch:
    """Dispatch payload captured after a job has been successfully claimed."""

    job_id: str
    task_name: str
    task: TaskiqTask
    trigger: SchedulerTrigger
    args: list[object]
    kwargs: dict[str, object]
    scheduled_for: datetime
    claim_started_at: datetime


@dataclass(slots=True, frozen=True)
class DispatchOutcome:
    """Result of trying to enqueue a job into the broker."""

    job_id: str
    status: str
    broker_task_id: str | None = None
    error: str | None = None
    dispatched_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class SchedulerHealthSnapshot:
    """Observable engine counters and timestamps for monitoring."""

    scheduler_id: str
    is_running: bool
    active_job_count: int
    heap_size: int
    sync_count: int
    claimed_job_count: int
    dispatched_job_count: int
    failed_dispatch_count: int
    claim_conflict_count: int
    cleaned_run_count: int
    last_sync_at: datetime | None
    last_dispatch_started_at: datetime | None
    last_dispatch_finished_at: datetime | None
    last_cleanup_at: datetime | None


class SchedulerEngine:
    """Background loop that syncs jobs, claims due work, and dispatches tasks."""

    def __init__(
            self,
            *,
            session_factory: async_sessionmaker[AsyncSession],
            registry: TaskRegistry,
            config: SchedulerConfig | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.registry = registry
        self.config = config or SchedulerConfig()
        self.scheduler_id = self.config.scheduler_id
        self.jobs: dict[str, SchedulerJobState] = {}
        self.heap: list[SchedulerHeapItem] = []
        self.last_sync_at: datetime = datetime.now(UTC)
        self.last_dispatch_started_at: datetime | None = None
        self.last_dispatch_finished_at: datetime | None = None
        self.last_cleanup_at: datetime | None = None
        self.sync_count = 0
        self.claimed_job_count = 0
        self.dispatched_job_count = 0
        self.failed_dispatch_count = 0
        self.claim_conflict_count = 0
        self.cleaned_run_count = 0
        self.stop_event = asyncio.Event()
        self.wakeup_event = asyncio.Event()
        self.runner_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Prime in-memory state and start the background runner task."""
        if self.runner_task is not None:
            log.debug("Scheduler engine start skipped because it is already running.")
            return
        log.info("Starting scheduler engine.")
        await self.sync_all()
        self.stop_event.clear()
        self.runner_task = asyncio.create_task(self.run())
        log.info("Scheduler engine started.")

    async def stop(self) -> None:
        """Stop the background loop and cancel the active runner task."""
        log.info("Stopping scheduler engine.")
        self.stop_event.set()
        self.wakeup_event.set()
        if self.runner_task is not None:
            self.runner_task.cancel()
        await asyncio.gather(*(task for task in (self.runner_task,) if task is not None), return_exceptions=True)
        self.runner_task = None
        log.info("Scheduler engine stopped.")

    async def run(self) -> None:
        """Main polling loop for sync, dispatch, and cleanup."""
        while not self.stop_event.is_set():
            await self.run_once()
            await self.wait_next_tick()

    async def run_once(self) -> None:
        """Execute one engine iteration."""
        await self.sync_storage()
        await self.dispatch_due_jobs()
        await self.cleanup_runs()

    async def wait_next_tick(self) -> None:
        """Sleep until the next wakeup signal or the computed timeout."""
        timeout = self.get_sleep_timeout()
        try:
            await asyncio.wait_for(self.wakeup_event.wait(), timeout=timeout)
        except TimeoutError:
            return
        finally:
            self.wakeup_event.clear()

    def get_sleep_timeout(self) -> float:
        """Choose the next sleep interval from heap state and config bounds."""
        if not self.heap:
            return self.config.sync_interval_seconds
        head = self.heap[0]
        seconds_left = (head.wake_at - datetime.now(UTC)).total_seconds()
        return max(min(seconds_left, self.config.sync_interval_seconds), self.config.idle_sleep_seconds)

    async def sync_storage(self) -> None:
        """Refresh all jobs from storage when the sync interval has elapsed."""
        current_time = datetime.now(UTC)
        if self.jobs and (current_time - self.last_sync_at).total_seconds() < self.config.sync_interval_seconds:
            return
        await self.sync_all()

    async def sync_all(self) -> None:
        """Reload active jobs from storage and rebuild the wakeup heap."""
        async with self.session_factory() as session:
            active_jobs = await JobRepository.list_active(session)
        self.jobs = {}
        self.heap = []
        self.merge_jobs(active_jobs)
        current_time = datetime.now(UTC)
        self.last_sync_at = current_time
        self.sync_count += 1
        log.info("Scheduler engine synced jobs from storage.", extra={"active_job_count": len(active_jobs)})

    def merge_jobs(self, jobs: list[SchedulerJob]) -> None:
        """Merge storage rows into in-memory state and wakeup heap."""
        for job in jobs:
            state = SchedulerJobState.from_job(job)
            self.jobs[state.job_id] = state
            wake_at = state.get_wake_at(self.scheduler_id)
            if wake_at is not None:
                self.push_heap_item(
                    SchedulerHeapItem(
                        wake_at=wake_at,
                        updated_at=state.updated_at,
                        job_id=state.job_id,
                    ),
                )

    def upsert_job(self, job: SchedulerJob) -> None:
        """Apply a single job update to in-memory state and wake up the loop."""
        state = SchedulerJobState.from_job(job)
        self.jobs[state.job_id] = state
        wake_at = state.get_wake_at(self.scheduler_id)
        if wake_at is not None:
            self.push_heap_item(
                SchedulerHeapItem(
                    wake_at=wake_at,
                    updated_at=state.updated_at,
                    job_id=state.job_id,
                ),
            )
        self.wakeup_event.set()
        log.debug(
            "Scheduler engine upserted in-memory job state.",
            extra={
                "job_id": state.job_id,
                "next_run_at": state.next_run_at.isoformat() if state.next_run_at else None,
                "wake_at": wake_at.isoformat() if wake_at else None,
                "claimed_by": state.claimed_by,
            },
        )

    def remove_job(self, job_id: str) -> None:
        """Drop a job from in-memory state."""
        self.jobs.pop(job_id, None)
        self.wakeup_event.set()
        log.debug("Scheduler engine removed in-memory job state.", extra={"job_id": job_id})

    def push_heap_item(self, item: SchedulerHeapItem) -> None:
        """Push a new wakeup candidate into the min-heap."""
        heapq.heappush(self.heap, item)

    async def dispatch_due_jobs(self) -> None:
        """Dispatch due jobs in batches until no runnable jobs remain."""
        while True:
            current_time = datetime.now(UTC)
            due_jobs = self.collect_due_jobs(current_time)
            if not due_jobs:
                return
            await self.dispatch_jobs_batch(due_jobs, current_time)

    def collect_due_jobs(self, current_time: datetime) -> list[SchedulerJobState]:
        """Pop due heap entries and discard stale snapshots on the way."""
        due_jobs: list[SchedulerJobState] = []
        while self.heap and self.heap[0].wake_at <= current_time and len(due_jobs) < self.config.dispatch_batch_size:
            item = heapq.heappop(self.heap)
            job = self.jobs.get(item.job_id)
            if job is None:
                continue
            wake_at = job.get_wake_at(self.scheduler_id)
            if wake_at is None:
                continue
            # Heap items are append-only; stale entries are filtered here.
            if job.updated_at != item.updated_at or wake_at != item.wake_at:
                continue
            due_jobs.append(job)
        return due_jobs

    async def dispatch_jobs_batch(self, cached_jobs: list[SchedulerJobState], current_time: datetime) -> None:
        """Claim a batch of jobs, dispatch them, and persist the outcomes."""
        self.last_dispatch_started_at = current_time
        async with self.session_factory() as read_session:
            jobs = await JobRepository.list_by_ids(read_session, [job.job_id for job in cached_jobs])
        jobs_by_id = {str(job.id): job for job in jobs}
        claim_candidates: list[SchedulerJob] = []

        for cached_job in cached_jobs:
            job = jobs_by_id.get(cached_job.job_id)
            if job is None:
                self.remove_job(cached_job.job_id)
                continue
            if not job.is_enabled or job.next_run_at is None:
                self.upsert_job(job)
                continue
            if normalize_utc(job.updated_at) != cached_job.updated_at:
                self.upsert_job(job)
                continue
            if job.claimed_by and job.claimed_by != self.scheduler_id and job.claim_expires_at is not None:
                # Another scheduler still owns a live lease for this job.
                if normalize_utc(job.claim_expires_at) > current_time:
                    self.upsert_job(job)
                    self.claim_conflict_count += 1
                    continue
            claim_candidates.append(job)

        if not claim_candidates:
            self.last_dispatch_finished_at = datetime.now(UTC)
            return

        prepared_dispatches: list[PreparedDispatch] = []
        async with self.session_factory() as claim_session:
            for job in claim_candidates:
                claimed_job = await JobRepository.claim_for_dispatch(
                    claim_session,
                    job_id=str(job.id),
                    claimed_at=current_time,
                    owner=self.scheduler_id,
                    lease_ttl_seconds=self.config.claim_ttl_seconds,
                )
                if claimed_job is None:
                    refreshed_job = await JobRepository.get_by_id(claim_session, str(job.id))
                    if refreshed_job is None:
                        self.remove_job(str(job.id))
                    else:
                        self.upsert_job(refreshed_job)
                        self.claim_conflict_count += 1
                    continue
                prepared_dispatches.append(
                    PreparedDispatch(
                        job_id=str(claimed_job.id),
                        task_name=claimed_job.task_name,
                        task=self.registry.get_task(claimed_job.task_name),
                        trigger=Scheduler.build_trigger(claimed_job),
                        args=list(claimed_job.task_args or []),
                        kwargs=dict(claimed_job.task_kwargs or {}),
                        scheduled_for=claimed_job.next_run_at or current_time,
                        claim_started_at=current_time,
                    ),
                )
            await claim_session.commit()

        if not prepared_dispatches:
            self.last_dispatch_finished_at = datetime.now(UTC)
            return

        self.claimed_job_count += len(prepared_dispatches)
        log.info("Dispatching scheduler jobs batch.", extra={"job_count": len(prepared_dispatches)})
        outcomes = await self.execute_prepared_dispatches(prepared_dispatches)
        runs_to_create: list[SchedulerRun] = []

        async with self.session_factory() as finalize_session:
            runnable_job_ids = [prepared.job_id for prepared in prepared_dispatches]
            runnable_jobs = await JobRepository.list_by_ids(
                finalize_session,
                runnable_job_ids,
            )
            runnable_jobs_by_id = {str(job.id): job for job in runnable_jobs}
            finalized_jobs: list[SchedulerJob] = []

            for prepared, outcome in zip(prepared_dispatches, outcomes, strict=True):
                job = runnable_jobs_by_id.get(prepared.job_id)
                if job is None:
                    self.remove_job(prepared.job_id)
                    continue
                claimed_at = normalize_utc(job.claimed_at) if job.claimed_at is not None else None
                if job.claimed_by != self.scheduler_id or claimed_at != prepared.claim_started_at:
                    self.upsert_job(job)
                    self.claim_conflict_count += 1
                    continue
                self.apply_dispatch_outcome(
                    job=job,
                    prepared=prepared,
                    outcome=outcome,
                    current_time=current_time,
                    runs_to_create=runs_to_create,
                )
                finalized_jobs.append(job)

            if self.config.record_runs:
                await RunRepository.create_many(finalize_session, runs_to_create)

            await finalize_session.commit()

        for job in finalized_jobs:
            self.upsert_job(job) if job.is_enabled and job.next_run_at is not None else self.remove_job(str(job.id))
        self.last_dispatch_finished_at = datetime.now(UTC)

    async def execute_prepared_dispatches(self, prepared_dispatches: list[PreparedDispatch]) -> list[DispatchOutcome]:
        """Dispatch claimed jobs concurrently while extending their claim lease."""
        semaphore = asyncio.Semaphore(self.config.dispatch_concurrency)
        keepalive_stop = asyncio.Event()

        async def keep_claims_alive() -> None:
            refresh_interval = max(self.config.claim_ttl_seconds / 3, 0.1)
            grouped_job_ids: dict[datetime, list[str]] = {}
            for prepared in prepared_dispatches:
                grouped_job_ids.setdefault(prepared.claim_started_at, []).append(prepared.job_id)

            while not keepalive_stop.is_set():
                try:
                    await asyncio.wait_for(keepalive_stop.wait(), timeout=refresh_interval)
                    return
                except TimeoutError:
                    # Claims are grouped by claimed_at because lease refresh uses
                    # the original claim timestamp as part of the update filter.
                    current_time = datetime.now(UTC)
                    async with self.session_factory() as session:
                        for claim_started_at, job_ids in grouped_job_ids.items():
                            await JobRepository.extend_claims(
                                session,
                                job_ids=job_ids,
                                owner=self.scheduler_id,
                                claimed_at=claim_started_at,
                                lease_ttl_seconds=self.config.claim_ttl_seconds,
                                current_time=current_time,
                            )
                        await session.commit()

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
                log.info(
                    "Scheduler job dispatched.",
                    extra={
                        "job_id": prepared.job_id,
                        "task_name": prepared.task_name,
                        "broker_task_id": getattr(broker_task, "task_id", None),
                    },
                )
                return DispatchOutcome(
                    job_id=prepared.job_id,
                    status="dispatched",
                    broker_task_id=getattr(broker_task, "task_id", None),
                    dispatched_at=dispatched_at,
                    finished_at=finished_at,
                )

        keepalive_task = asyncio.create_task(keep_claims_alive())
        try:
            return list(await asyncio.gather(*(run_single(prepared) for prepared in prepared_dispatches)))
        finally:
            keepalive_stop.set()
            await asyncio.gather(keepalive_task, return_exceptions=True)

    def apply_dispatch_outcome(
            self,
            *,
            job: SchedulerJob,
            prepared: PreparedDispatch,
            outcome: DispatchOutcome,
            current_time: datetime,
            runs_to_create: list[SchedulerRun],
    ) -> None:
        """Apply the broker result and compute the job's next persisted state."""
        job.claimed_by = None
        job.claimed_at = None
        job.claim_expires_at = None
        if outcome.status == "failed":
            retry_from = outcome.finished_at or outcome.dispatched_at or current_time
            retry_at = retry_from + timedelta(seconds=self.config.dispatch_retry_seconds)
            job.last_error = outcome.error
            job.next_run_at = retry_at
            job.updated_at = retry_from
            self.failed_dispatch_count += 1
            log.warning(
                "Scheduler job dispatch failed; retry scheduled.",
                extra={
                    "job_id": str(job.id),
                    "task_name": job.task_name,
                    "retry_at": retry_at.isoformat(),
                },
            )
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
                    ),
                )
            return

        dispatched_at = outcome.dispatched_at or current_time
        finished_at = outcome.finished_at or dispatched_at
        job.last_error = None
        job.last_run_at = dispatched_at
        job.last_dispatched_task_id = outcome.broker_task_id
        job.dispatch_count += 1
        job.updated_at = finished_at
        self.dispatched_job_count += 1

        if job.kind == "one_off":
            job.is_enabled = False
            job.next_run_at = None
        else:
            assert isinstance(prepared.trigger, PeriodicSchedule)
            # Periodic schedules advance from actual dispatch time so delayed runs
            # do not immediately schedule duplicate catch-up executions.
            next_run_at = prepared.trigger.get_next_run_at(dispatched_at, anchor=job.created_at)
            job.next_run_at = next_run_at
            if next_run_at is None:
                job.is_enabled = False
        logged_next_run_at = SchedulerJobState.from_job(job).next_run_at
        logged_next_run_at_iso = logged_next_run_at.isoformat() if logged_next_run_at is not None else None
        log.debug(
            "Scheduler job state updated after dispatch.",
            extra={
                "job_id": str(job.id),
                "task_name": job.task_name,
                "is_enabled": job.is_enabled,
                "next_run_at": logged_next_run_at_iso,
            },
        )

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
                ),
            )

    async def cleanup_runs(self) -> None:
        """Periodically purge old run history when retention is configured."""
        retention_days = self.config.run_history_retention_days
        if retention_days is None:
            return
        current_time = datetime.now(UTC)
        if self.last_cleanup_at is not None:
            elapsed = (current_time - self.last_cleanup_at).total_seconds()
            if elapsed < self.config.run_cleanup_interval_seconds:
                return
        cutoff = current_time - timedelta(days=retention_days)
        async with self.session_factory() as session:
            deleted_runs = await RunRepository.purge_finished_before(session, cutoff)
            await session.commit()
        self.last_cleanup_at = current_time
        self.cleaned_run_count += deleted_runs
        if deleted_runs:
            log.info(
                "Scheduler engine cleaned old run history.",
                extra={"deleted_run_count": deleted_runs, "finished_before": cutoff.isoformat()},
            )

    def get_health_snapshot(self) -> SchedulerHealthSnapshot:
        """Return a point-in-time summary of engine state."""
        active_job_count = sum(1 for job in self.jobs.values() if job.is_enabled and job.next_run_at is not None)
        return SchedulerHealthSnapshot(
            scheduler_id=self.scheduler_id,
            is_running=self.runner_task is not None and not self.runner_task.done(),
            active_job_count=active_job_count,
            heap_size=len(self.heap),
            sync_count=self.sync_count,
            claimed_job_count=self.claimed_job_count,
            dispatched_job_count=self.dispatched_job_count,
            failed_dispatch_count=self.failed_dispatch_count,
            claim_conflict_count=self.claim_conflict_count,
            cleaned_run_count=self.cleaned_run_count,
            last_sync_at=self.last_sync_at,
            last_dispatch_started_at=self.last_dispatch_started_at,
            last_dispatch_finished_at=self.last_dispatch_finished_at,
            last_cleanup_at=self.last_cleanup_at,
        )
