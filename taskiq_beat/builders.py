from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from taskiq_beat.chains import CHAIN_ORCHESTRATOR_TASK_NAME, ChainFailurePolicy, ChainStep, TaskChain
from taskiq_beat.models import SchedulerJob
from taskiq_beat.scheduler import Scheduler, SchedulerTrigger
from taskiq_beat.triggers import ImmediateDispatch
from taskiq_beat.types import TaskReference

if TYPE_CHECKING:
    from taskiq_beat.app import SchedulerApp


@dataclass(slots=True)
class SingleScheduleBuilder:
    """Fluent builder that describes a single-task schedule for an app."""

    app: SchedulerApp
    task: TaskReference
    args: list[object] = field(default_factory=list)
    kwargs: dict[str, object] = field(default_factory=dict)
    metadata: dict[str, object] = field(default_factory=dict)

    async def schedule(
            self,
            session: AsyncSession,
            *,
            trigger: SchedulerTrigger | None = None,
            job_id: str | None = None,
            name: str | None = None,
            description: str | None = None,
            is_enabled: bool = True,
    ) -> SchedulerJob:
        """Create a new job from this builder."""
        return await self.build_scheduler(
            trigger=trigger,
            job_id=job_id,
            name=name,
            description=description,
            is_enabled=is_enabled,
        ).schedule(session)

    async def upsert(
            self,
            session: AsyncSession,
            *,
            trigger: SchedulerTrigger,
            job_id: str,
            name: str | None = None,
            description: str | None = None,
            is_enabled: bool = True,
    ) -> SchedulerJob:
        """Insert a new job or update an existing one with a stable id."""
        return await self.build_scheduler(
            trigger=trigger,
            job_id=job_id,
            name=name,
            description=description,
            is_enabled=is_enabled,
        ).upsert(session)

    def build_scheduler(
            self,
            *,
            trigger: SchedulerTrigger | None,
            job_id: str | None,
            name: str | None,
            description: str | None,
            is_enabled: bool,
    ) -> Scheduler:
        """Assemble the internal Scheduler definition for this builder."""
        return Scheduler(
            task=self.task,
            trigger=trigger or ImmediateDispatch(),
            job_id=job_id,
            name=name,
            description=description,
            args=list(self.args),
            kwargs=dict(self.kwargs),
            metadata=dict(self.metadata),
            is_enabled=is_enabled,
            registry=self.app.registry,
            engine=self.app.engine,
        )


@dataclass(slots=True)
class ChainScheduleBuilder:
    """Fluent builder that describes a task-chain schedule for an app."""

    app: SchedulerApp
    chain: TaskChain
    metadata: dict[str, object] = field(default_factory=dict)

    async def schedule(
            self,
            session: AsyncSession,
            *,
            trigger: SchedulerTrigger | None = None,
            job_id: str | None = None,
            name: str | None = None,
            description: str | None = None,
            is_enabled: bool = True,
    ) -> SchedulerJob:
        """Create a new chain job from this builder."""
        return await self.build_scheduler(
            trigger=trigger,
            job_id=job_id,
            name=name,
            description=description,
            is_enabled=is_enabled,
        ).schedule(session)

    async def upsert(
            self,
            session: AsyncSession,
            *,
            trigger: SchedulerTrigger,
            job_id: str,
            name: str | None = None,
            description: str | None = None,
            is_enabled: bool = True,
    ) -> SchedulerJob:
        """Insert or update a chain job with a stable id."""
        return await self.build_scheduler(
            trigger=trigger,
            job_id=job_id,
            name=name,
            description=description,
            is_enabled=is_enabled,
        ).upsert(session)

    def build_scheduler(
            self,
            *,
            trigger: SchedulerTrigger | None,
            job_id: str | None,
            name: str | None,
            description: str | None,
            is_enabled: bool,
    ) -> Scheduler:
        """Assemble the internal Scheduler that dispatches the chain orchestrator."""
        orchestrator_kwargs = self.chain.to_orchestrator_kwargs(self.app.registry)
        chain_metadata: dict[str, object] = {
            "chain": True,
            "chain_step_count": len(self.chain.steps),
        }
        chain_metadata.update(self.metadata)
        return Scheduler(
            task=CHAIN_ORCHESTRATOR_TASK_NAME,
            trigger=trigger or ImmediateDispatch(),
            job_id=job_id,
            name=name,
            description=description,
            args=[],
            kwargs=orchestrator_kwargs,
            metadata=chain_metadata,
            is_enabled=is_enabled,
            registry=self.app.registry,
            engine=self.app.engine,
        )


def make_single_builder(
        app: SchedulerApp,
        *,
        task: TaskReference,
        args: Sequence[object] | None = None,
        kwargs: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
) -> SingleScheduleBuilder:
    """Build a SingleScheduleBuilder bound to the given app."""
    return SingleScheduleBuilder(
        app=app,
        task=task,
        args=list(args or []),
        kwargs=dict(kwargs or {}),
        metadata=dict(metadata or {}),
    )


def make_chain_builder(
        app: SchedulerApp,
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
    """Build a ChainScheduleBuilder bound to the given app."""
    chain = TaskChain(
        steps=list(steps),
        on_failure=on_failure,
        max_chain_attempts=max_chain_attempts,
        default_step_max_attempts=default_step_max_attempts,
        default_step_retry_delay_seconds=default_step_retry_delay_seconds,
        default_step_timeout_seconds=default_step_timeout_seconds,
        wait_poll_interval_seconds=wait_poll_interval_seconds,
    )
    return ChainScheduleBuilder(
        app=app,
        chain=chain,
        metadata=dict(metadata or {}),
    )
