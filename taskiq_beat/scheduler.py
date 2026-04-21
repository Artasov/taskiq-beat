from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from taskiq_beat.datetime_utils import normalize_utc
from taskiq_beat.models import SchedulerJob, SchedulerRun
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository
from taskiq_beat.triggers import ImmediateDispatch, OneOffSchedule, PeriodicSchedule
from taskiq_beat.types import TaskReference

if TYPE_CHECKING:
    from taskiq_beat.engine import SchedulerEngine

log = logging.getLogger(__name__)

SchedulerTrigger = PeriodicSchedule | OneOffSchedule | ImmediateDispatch


@dataclass(slots=True)
class Scheduler:
    """Declarative description of a single persisted scheduler job."""

    task: TaskReference
    trigger: SchedulerTrigger
    job_id: str | None = None
    name: str | None = None
    description: str | None = None
    args: list[object] = field(default_factory=list)
    kwargs: dict[str, object] = field(default_factory=dict)
    metadata: dict[str, object] = field(default_factory=dict)
    is_enabled: bool = True
    registry: TaskRegistry | None = None
    engine: SchedulerEngine | None = None

    async def schedule(self, session: AsyncSession) -> SchedulerJob:
        """Create a new job row from this scheduler definition."""
        job = self.build_new_job()
        await JobRepository.create(session, job)
        dispatch_error: Exception | None = None
        if isinstance(self.trigger, ImmediateDispatch):
            dispatch_error = await self.dispatch_immediate(session, job)
        await session.commit()
        self.notify_engine(job)
        log.info("Scheduler job created.", extra={"job_id": str(job.id), "task_name": job.task_name, "kind": job.kind})
        if dispatch_error is not None:
            raise dispatch_error
        return job

    async def upsert(self, session: AsyncSession) -> SchedulerJob:
        """Insert a new job or update an existing one in place."""
        if isinstance(self.trigger, ImmediateDispatch):
            raise ValueError("ImmediateDispatch cannot be used with upsert; use schedule() instead.")
        if self.job_id is None:
            return await self.schedule(session)
        existing_job = await JobRepository.get_by_id(session, self.job_id)
        job = self.build_existing_or_new_job(existing_job=existing_job)
        if existing_job is None:
            await JobRepository.create(session, job)
        await session.commit()
        self.notify_engine(job)
        log.info(
            "Scheduler job upserted.",
            extra={
                "job_id": str(job.id),
                "task_name": job.task_name,
                "kind": job.kind,
                "created": existing_job is None,
            },
        )
        return job

    async def dispatch_immediate(self, session: AsyncSession, job: SchedulerJob) -> Exception | None:
        """Enqueue the job's task via broker.kiq and persist a run record."""
        task = self.get_registry().get_task(job.task_name)
        dispatched_at = datetime.now(UTC)
        broker_task_id: str | None = None
        error: Exception | None = None
        try:
            broker_task = await task.kiq(*job.task_args, **job.task_kwargs)
            broker_task_id = getattr(broker_task, "task_id", None)
        except Exception as exc:
            error = exc
            log.exception(
                "Immediate dispatch failed.",
                extra={"job_id": str(job.id), "task_name": job.task_name},
            )
        finished_at = datetime.now(UTC)
        job.last_run_at = dispatched_at
        job.last_dispatched_task_id = broker_task_id
        job.dispatch_count += 1
        job.updated_at = finished_at
        if error is None:
            job.last_error = None
        else:
            job.last_error = str(error)
        if self.should_record_runs():
            session.add(
                SchedulerRun(
                    job_id=job.id,
                    status="failed" if error is not None else "dispatched",
                    scheduled_for=dispatched_at,
                    dispatched_at=dispatched_at,
                    finished_at=finished_at,
                    broker_task_id=broker_task_id,
                    error=str(error) if error is not None else None,
                    created_at=finished_at,
                    updated_at=finished_at,
                ),
            )
        if error is None:
            log.info(
                "Immediate dispatch succeeded.",
                extra={"job_id": str(job.id), "task_name": job.task_name, "broker_task_id": broker_task_id},
            )
        return error

    def should_record_runs(self) -> bool:
        """Return True when the bound engine is configured to write run history."""
        if self.engine is None:
            return True
        return self.engine.config.record_runs

    def build_new_job(self) -> SchedulerJob:
        """Materialize a fresh database model from the scheduler definition."""
        task_name = self.get_registry().validate_task(self.task)
        TaskRegistry.validate_payload(self.args, self.kwargs, self.metadata)
        created_at = datetime.now(UTC)
        is_immediate = isinstance(self.trigger, ImmediateDispatch)
        is_enabled = False if is_immediate else self.is_enabled
        next_run_at = None if is_immediate else self.get_next_run_at(created_at)
        job_kwargs: dict[str, object] = {}
        if self.job_id is not None:
            job_kwargs["id"] = self.job_id
        return SchedulerJob(
            name=self.name,
            description=self.description,
            task_name=task_name,
            kind=self.get_kind(),
            strategy=self.get_strategy(),
            trigger_payload=self.trigger.to_payload(),
            task_args=list(self.args),
            task_kwargs=dict(self.kwargs),
            metadata_payload=dict(self.metadata),
            is_enabled=is_enabled,
            next_run_at=next_run_at if is_enabled else None,
            created_at=created_at,
            updated_at=created_at,
            **job_kwargs,
        )

    def build_existing_or_new_job(self, *, existing_job: SchedulerJob | None) -> SchedulerJob:
        """Reuse an existing row and reschedule only when timing inputs changed."""
        if existing_job is None:
            return self.build_new_job()

        task_name = self.get_registry().validate_task(self.task)
        TaskRegistry.validate_payload(self.args, self.kwargs, self.metadata)
        current_time = datetime.now(UTC)
        kind = self.get_kind()
        strategy = self.get_strategy()
        trigger_payload = self.trigger.to_payload()
        args = list(self.args)
        kwargs = dict(self.kwargs)
        metadata = dict(self.metadata)

        # Only fields that affect dispatch timing should trigger a recalculation.
        schedule_changed = (
                existing_job.task_name != task_name
                or existing_job.kind != kind
                or existing_job.strategy != strategy
                or dict(existing_job.trigger_payload or {}) != trigger_payload
                or list(existing_job.task_args or []) != args
                or dict(existing_job.task_kwargs or {}) != kwargs
        )

        existing_job.name = self.name
        existing_job.description = self.description
        existing_job.task_name = task_name
        existing_job.kind = kind
        existing_job.strategy = strategy
        existing_job.trigger_payload = trigger_payload
        existing_job.task_args = args
        existing_job.task_kwargs = kwargs
        existing_job.metadata_payload = metadata
        existing_job.claimed_by = None
        existing_job.claimed_at = None
        existing_job.claim_expires_at = None
        existing_job.updated_at = current_time

        if not self.is_enabled:
            existing_job.is_enabled = False
            existing_job.next_run_at = None
            return existing_job

        should_recalculate = schedule_changed or not existing_job.is_enabled or existing_job.next_run_at is None
        existing_job.is_enabled = True
        if should_recalculate:
            existing_job.next_run_at = self.get_next_run_at(current_time, anchor=existing_job.created_at)
            log.debug(
                "Recalculated next run for scheduler job.",
                extra={"job_id": str(existing_job.id), "task_name": existing_job.task_name},
            )
        return existing_job

    def get_registry(self) -> TaskRegistry:
        """Return the bound registry or fail if the scheduler is detached."""
        if self.registry is None:
            raise RuntimeError("Scheduler registry is not configured.")
        return self.registry

    def get_kind(self) -> str:
        if isinstance(self.trigger, OneOffSchedule | ImmediateDispatch):
            return "one_off"
        return "periodic"

    def get_strategy(self) -> str:
        if isinstance(self.trigger, ImmediateDispatch):
            return "immediate"
        if isinstance(self.trigger, OneOffSchedule):
            return "one_off"
        return self.trigger.strategy

    def get_next_run_at(self, current_time: datetime, *, anchor: datetime | None = None) -> datetime | None:
        """Compute the next fire time for the current trigger."""
        normalized_current_time = normalize_utc(current_time)
        normalized_anchor = normalize_utc(anchor) if anchor is not None else normalized_current_time
        if isinstance(self.trigger, ImmediateDispatch):
            return None
        if isinstance(self.trigger, OneOffSchedule):
            run_at = normalize_utc(self.trigger.run_at)
            return run_at if run_at > normalized_current_time else normalized_current_time
        return self.trigger.get_next_run_at(normalized_current_time, anchor=normalized_anchor)

    @classmethod
    def build_trigger(cls, job: SchedulerJob) -> SchedulerTrigger:
        """Recreate the trigger object stored in the job payload."""
        if job.strategy == "immediate":
            return ImmediateDispatch.from_payload(job.trigger_payload)
        if job.kind == "one_off":
            return OneOffSchedule.from_payload(job.trigger_payload)
        return PeriodicSchedule.from_payload(job.trigger_payload)

    def notify_engine(self, job: SchedulerJob) -> None:
        """Push the latest job state into the running in-memory engine."""
        if self.engine is None:
            return
        self.engine.upsert_job(job)
