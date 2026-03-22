from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from taskiq_beat.datetime_utils import normalize_utc
from taskiq_beat.models import SchedulerJob
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository
from taskiq_beat.triggers import OneOffSchedule, PeriodicSchedule
from taskiq_beat.types import TaskReference

if TYPE_CHECKING:
    from taskiq_beat.engine import SchedulerEngine

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Scheduler:
    task: TaskReference
    trigger: PeriodicSchedule | OneOffSchedule
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
        job = self.build_new_job()
        await JobRepository.create(session, job)
        await session.commit()
        self.notify_engine(job)
        log.info("Scheduler job created.", extra={"job_id": str(job.id), "task_name": job.task_name, "kind": job.kind})
        return job

    async def upsert(self, session: AsyncSession) -> SchedulerJob:
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

    def build_new_job(self) -> SchedulerJob:
        task_name = self.get_registry().validate_task(self.task)
        TaskRegistry.validate_payload(self.args, self.kwargs, self.metadata)
        created_at = datetime.now(UTC)
        next_run_at = self.get_next_run_at(created_at)
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
            is_enabled=self.is_enabled,
            next_run_at=next_run_at if self.is_enabled else None,
            created_at=created_at,
            updated_at=created_at,
            **job_kwargs,
        )

    def build_existing_or_new_job(self, *, existing_job: SchedulerJob | None) -> SchedulerJob:
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
        if self.registry is None:
            raise RuntimeError("Scheduler registry is not configured.")
        return self.registry

    def get_kind(self) -> str:
        return "one_off" if isinstance(self.trigger, OneOffSchedule) else "periodic"

    def get_strategy(self) -> str:
        return "one_off" if isinstance(self.trigger, OneOffSchedule) else self.trigger.strategy

    def get_next_run_at(self, current_time: datetime, *, anchor: datetime | None = None) -> datetime | None:
        normalized_current_time = normalize_utc(current_time)
        normalized_anchor = normalize_utc(anchor) if anchor is not None else normalized_current_time
        if isinstance(self.trigger, OneOffSchedule):
            run_at = normalize_utc(self.trigger.run_at)
            return run_at if run_at > normalized_current_time else normalized_current_time
        return self.trigger.get_next_run_at(normalized_current_time, anchor=normalized_anchor)

    @classmethod
    def build_trigger(cls, job: SchedulerJob) -> PeriodicSchedule | OneOffSchedule:
        if job.kind == "one_off":
            return OneOffSchedule.from_payload(job.trigger_payload)
        return PeriodicSchedule.from_payload(job.trigger_payload)

    def notify_engine(self, job: SchedulerJob) -> None:
        if self.engine is None:
            return
        self.engine.upsert_job(job)
