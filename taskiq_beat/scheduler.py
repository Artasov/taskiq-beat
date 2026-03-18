from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from taskiq_beat.models import SchedulerJob
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository
from taskiq_beat.triggers import OneOffSchedule, PeriodicSchedule


@dataclass(slots=True)
class Scheduler:
    task: Any
    trigger: PeriodicSchedule | OneOffSchedule
    name: str | None = None
    description: str | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    is_enabled: bool = True
    registry: TaskRegistry | None = None
    engine: Any = None

    async def schedule(self, session: AsyncSession) -> SchedulerJob:
        task_name = self.get_registry().validate_task(self.task)
        TaskRegistry.validate_payload(self.args, self.kwargs, self.metadata)
        created_at = datetime.now(UTC)
        next_run_at = self.get_next_run_at(created_at)
        job = SchedulerJob(
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
        )
        await JobRepository.create(session, job)
        await session.commit()
        self.notify_engine(job)
        return job

    def get_registry(self) -> TaskRegistry:
        if self.registry is None:
            raise RuntimeError("Scheduler registry is not configured.")
        return self.registry

    def get_kind(self) -> str:
        return "one_off" if isinstance(self.trigger, OneOffSchedule) else "periodic"

    def get_strategy(self) -> str:
        return "one_off" if isinstance(self.trigger, OneOffSchedule) else self.trigger.strategy

    def get_next_run_at(self, current_time: datetime, *, anchor: datetime | None = None) -> datetime | None:
        normalized_anchor = anchor or current_time
        if isinstance(self.trigger, OneOffSchedule):
            return self.trigger.run_at.astimezone(UTC) if self.trigger.run_at.astimezone(UTC) > current_time else current_time
        return self.trigger.get_next_run_at(current_time, anchor=normalized_anchor)

    @classmethod
    def build_trigger(cls, job: SchedulerJob) -> PeriodicSchedule | OneOffSchedule:
        if job.kind == "one_off":
            return OneOffSchedule.from_payload(job.trigger_payload)
        return PeriodicSchedule.from_payload(job.trigger_payload)

    def notify_engine(self, job: SchedulerJob) -> None:
        if self.engine is None:
            return
        self.engine.upsert_job(job)
