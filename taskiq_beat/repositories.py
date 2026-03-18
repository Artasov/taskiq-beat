from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from taskiq_beat.models import SchedulerJob, SchedulerRun


class JobRepository:
    @classmethod
    async def create(cls, session: AsyncSession, job: SchedulerJob) -> SchedulerJob:
        session.add(job)
        await session.flush()
        return job

    @classmethod
    async def get_by_id(cls, session: AsyncSession, job_id: str) -> SchedulerJob | None:
        return await session.get(SchedulerJob, job_id)

    @classmethod
    async def list_active(cls, session: AsyncSession) -> list[SchedulerJob]:
        query = (
            select(SchedulerJob)
            .where(SchedulerJob.is_enabled.is_(True), SchedulerJob.next_run_at.is_not(None))
            .order_by(SchedulerJob.next_run_at.asc())
        )
        return list((await session.execute(query)).scalars())


class RunRepository:
    @classmethod
    async def create(cls, session: AsyncSession, run: SchedulerRun) -> SchedulerRun:
        session.add(run)
        await session.flush()
        return run
