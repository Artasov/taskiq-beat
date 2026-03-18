from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from taskiq_beat.config import SchedulerConfig
from taskiq_beat.engine import SchedulerEngine
from taskiq_beat.models import SchedulerJob
from taskiq_beat.registry import TaskRegistry
from taskiq_beat.repositories import JobRepository
from taskiq_beat.scheduler import Scheduler

TaskLoader = Callable[[], tuple[str, ...]]


class SchedulerApp:
    def __init__(
        self,
        *,
        broker: Any,
        session_factory: Any,
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

    def create_scheduler(self, *args, **kwargs) -> Scheduler:
        return Scheduler(*args, **kwargs, registry=self.registry, engine=self.engine)

    async def start(self) -> None:
        self.registry.load()
        await self.engine.start()

    async def stop(self) -> None:
        await self.engine.stop()

    async def get_job(self, session: AsyncSession, job_id: str) -> SchedulerJob:
        job = await JobRepository.get_by_id(session, job_id)
        if job is None:
            raise ValueError(f"Scheduler job '{job_id}' was not found.")
        return job

    async def pause(self, session: AsyncSession, job_id: str) -> SchedulerJob:
        job = await self.get_job(session, job_id)
        job.is_enabled = False
        job.next_run_at = None
        job.updated_at = datetime.now(UTC)
        await session.commit()
        self.engine.upsert_job(job)
        return job

    async def resume(self, session: AsyncSession, job_id: str) -> SchedulerJob:
        job = await self.get_job(session, job_id)
        trigger = Scheduler.build_trigger(job)
        current_time = datetime.now(UTC)
        job.is_enabled = True
        job.next_run_at = self.create_scheduler(task=job.task_name, trigger=trigger).get_next_run_at(current_time, anchor=job.created_at)
        job.updated_at = current_time
        await session.commit()
        self.engine.upsert_job(job)
        return job

    async def delete(self, session: AsyncSession, job_id: str) -> None:
        job = await self.get_job(session, job_id)
        await session.delete(job)
        await session.commit()
        self.engine.remove_job(str(job.id))

    async def run_now(self, session: AsyncSession, job_id: str) -> SchedulerJob:
        job = await self.get_job(session, job_id)
        current_time = datetime.now(UTC)
        job.is_enabled = True
        job.next_run_at = current_time
        job.updated_at = current_time
        await session.commit()
        self.engine.upsert_job(job)
        return job
