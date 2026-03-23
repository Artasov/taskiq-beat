from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskiq_beat.config import SchedulerConfig
from taskiq_beat.engine import SchedulerEngine, SchedulerHealthSnapshot
from taskiq_beat.models import SchedulerJob
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository, RunRepository
from taskiq_beat.scheduler import Scheduler
from taskiq_beat.triggers import OneOffSchedule, PeriodicSchedule
from taskiq_beat.types import TaskiqBroker, TaskLoader, TaskReference

log = logging.getLogger(__name__)


class SchedulerApp:
    """High-level facade for registering, controlling, and running schedules."""

    def __init__(
            self,
            *,
            broker: TaskiqBroker,
            session_factory: async_sessionmaker[AsyncSession],
            config: SchedulerConfig | None = None,
            task_loader: TaskLoader | None = None,
    ) -> None:
        self.broker = broker
        self.session_factory = session_factory
        self.config = config or SchedulerConfig()
        self.registry = TaskRegistry(broker=broker, task_loader=task_loader)
        self.engine = SchedulerEngine(
            session_factory=session_factory,
            registry=self.registry,
            config=self.config,
        )

    def create_scheduler(
            self,
            *,
            task: TaskReference,
            trigger: PeriodicSchedule | OneOffSchedule,
            job_id: str | None = None,
            name: str | None = None,
            description: str | None = None,
            args: list[object] | None = None,
            kwargs: dict[str, object] | None = None,
            metadata: dict[str, object] | None = None,
            is_enabled: bool = True,
    ) -> Scheduler:
        """Build an in-memory scheduler definition bound to this app."""
        return Scheduler(
            task=task,
            trigger=trigger,
            job_id=job_id,
            name=name,
            description=description,
            args=list(args or []),
            kwargs=dict(kwargs or {}),
            metadata=dict(metadata or {}),
            is_enabled=is_enabled,
            registry=self.registry,
            engine=self.engine,
        )

    async def upsert_schedule(
            self,
            session: AsyncSession,
            *,
            task: TaskReference,
            trigger: PeriodicSchedule | OneOffSchedule,
            job_id: str | None = None,
            name: str | None = None,
            description: str | None = None,
            args: list[object] | None = None,
            kwargs: dict[str, object] | None = None,
            metadata: dict[str, object] | None = None,
            is_enabled: bool = True,
    ) -> SchedulerJob:
        """Create or update a persisted schedule in a single call."""
        scheduler = self.create_scheduler(
            task=task,
            trigger=trigger,
            job_id=job_id,
            name=name,
            description=description,
            args=args,
            kwargs=kwargs,
            metadata=metadata,
            is_enabled=is_enabled,
        )
        return await scheduler.upsert(session)

    @staticmethod
    async def sync_schedules(session: AsyncSession, schedulers: Sequence[Scheduler]) -> list[SchedulerJob]:
        """Persist a batch of scheduler definitions sequentially."""
        log.info("Syncing scheduler definitions.", extra={"scheduler_count": len(schedulers)})
        jobs: list[SchedulerJob] = []
        for scheduler in schedulers:
            jobs.append(await scheduler.upsert(session))
        log.info("Scheduler definitions synced.", extra={"job_count": len(jobs)})
        return jobs

    async def start(self) -> None:
        """Load tasks and start the background scheduler engine."""
        log.info("Starting scheduler app.")
        self.registry.load()
        await self.engine.start()
        log.info("Scheduler app started.")

    async def stop(self) -> None:
        """Stop the background scheduler engine."""
        log.info("Stopping scheduler app.")
        await self.engine.stop()
        log.info("Scheduler app stopped.")

    @staticmethod
    async def get_job(session: AsyncSession, job_id: str) -> SchedulerJob:
        """Return a stored job or raise if it does not exist."""
        job = await JobRepository.get_by_id(session, job_id)
        if job is None:
            raise ValueError(f"Scheduler job '{job_id}' was not found.")
        return job

    async def pause(self, session: AsyncSession, job_id: str) -> SchedulerJob:
        """Disable a job and clear any active claim."""
        job = await self.get_job(session, job_id)
        job.is_enabled = False
        job.next_run_at = None
        job.claimed_by = None
        job.claimed_at = None
        job.claim_expires_at = None
        job.updated_at = datetime.now(UTC)
        await session.commit()
        self.engine.upsert_job(job)
        log.info("Scheduler job paused.", extra={"job_id": str(job.id), "task_name": job.task_name})
        return job

    async def resume(self, session: AsyncSession, job_id: str) -> SchedulerJob:
        """Re-enable a job and recalculate its next run from current time."""
        job = await self.get_job(session, job_id)
        trigger = Scheduler.build_trigger(job)
        current_time = datetime.now(UTC)
        job.is_enabled = True
        job.claimed_by = None
        job.claimed_at = None
        job.claim_expires_at = None
        job.next_run_at = self.create_scheduler(
            task=job.task_name,
            trigger=trigger,
        ).get_next_run_at(current_time, anchor=job.created_at)
        job.updated_at = current_time
        await session.commit()
        self.engine.upsert_job(job)
        log.info("Scheduler job resumed.", extra={"job_id": str(job.id), "task_name": job.task_name})
        return job

    async def delete(self, session: AsyncSession, job_id: str) -> None:
        """Delete a persisted job and drop it from in-memory state."""
        job = await self.get_job(session, job_id)
        task_name = job.task_name
        await session.delete(job)
        await session.commit()
        self.engine.remove_job(str(job.id))
        log.info("Scheduler job deleted.", extra={"job_id": str(job.id), "task_name": task_name})

    async def run_now(self, session: AsyncSession, job_id: str) -> SchedulerJob:
        """Force the job to become immediately dispatchable."""
        job = await self.get_job(session, job_id)
        current_time = datetime.now(UTC)
        job.is_enabled = True
        job.claimed_by = None
        job.claimed_at = None
        job.claim_expires_at = None
        job.next_run_at = current_time
        job.updated_at = current_time
        await session.commit()
        self.engine.upsert_job(job)
        log.info("Scheduler job marked to run now.", extra={"job_id": str(job.id), "task_name": job.task_name})
        return job

    @staticmethod
    async def purge_runs(session: AsyncSession, *, finished_before: datetime) -> int:
        """Delete historical run records finished before the cutoff."""
        deleted_runs = await RunRepository.purge_finished_before(session, finished_before)
        await session.commit()
        log.info(
            "Scheduler run history purged.",
            extra={"deleted_run_count": deleted_runs, "finished_before": finished_before.isoformat()},
        )
        return deleted_runs

    def get_health_snapshot(self) -> SchedulerHealthSnapshot:
        """Expose a read-only view of engine counters and timestamps."""
        return self.engine.get_health_snapshot()
