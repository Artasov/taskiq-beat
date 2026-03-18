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
    async def list_by_ids(cls, session: AsyncSession, job_ids: list[str]) -> list[SchedulerJob]:
        if not job_ids:
            return []
        query = select(SchedulerJob).where(SchedulerJob.id.in_(job_ids))
        return list((await session.execute(query)).scalars())

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

    @classmethod
    async def create_many(cls, session: AsyncSession, runs: list[SchedulerRun]) -> None:
        if not runs:
            return
        session.add_all(runs)
        await session.flush()
