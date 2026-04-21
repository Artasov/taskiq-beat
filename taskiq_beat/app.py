from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskiq_beat.builders import (
    ChainScheduleBuilder,
    SingleScheduleBuilder,
    make_chain_builder,
    make_single_builder,
)
from taskiq_beat.chains import ChainFailurePolicy, ChainStep, register_chain_orchestrator
from taskiq_beat.config import SchedulerConfig
from taskiq_beat.engine import SchedulerEngine, SchedulerHealthSnapshot
from taskiq_beat.models import SchedulerJob
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository, RunRepository
from taskiq_beat.scheduler import Scheduler
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
        register_chain_orchestrator(broker)
        self.registry = TaskRegistry(broker=broker, task_loader=task_loader)
        self.engine = SchedulerEngine(
            session_factory=session_factory,
            registry=self.registry,
            config=self.config,
        )

    def single(
            self,
            *,
            task: TaskReference,
            args: Sequence[object] | None = None,
            kwargs: dict[str, object] | None = None,
            metadata: dict[str, object] | None = None,
    ) -> SingleScheduleBuilder:
        """Build a schedule definition for a single task."""
        return make_single_builder(
            self,
            task=task,
            args=args,
            kwargs=kwargs,
            metadata=metadata,
        )

    def chain(
            self,
            *,
            steps: Sequence[ChainStep],
            on_failure: ChainFailurePolicy = "stop",
            max_chain_attempts: int = 1,
            default_step_max_attempts: int = 1,
            default_step_retry_delay_seconds: float = 0.0,
            default_step_timeout_seconds: float | None = None,
            wait_poll_interval_seconds: float = 0.5,
            metadata: dict[str, object] | None = None,
    ) -> ChainScheduleBuilder:
        """Build a schedule definition for a sequential task chain."""
        return make_chain_builder(
            self,
            steps=steps,
            on_failure=on_failure,
            max_chain_attempts=max_chain_attempts,
            default_step_max_attempts=default_step_max_attempts,
            default_step_retry_delay_seconds=default_step_retry_delay_seconds,
            default_step_timeout_seconds=default_step_timeout_seconds,
            wait_poll_interval_seconds=wait_poll_interval_seconds,
            metadata=metadata,
        )

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
        scheduler = Scheduler(task=job.task_name, trigger=trigger, registry=self.registry, engine=self.engine)
        job.next_run_at = scheduler.get_next_run_at(current_time, anchor=job.created_at)
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
