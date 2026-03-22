from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import delete, or_, select, update
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

    @classmethod
    async def claim_for_dispatch(
            cls,
            session: AsyncSession,
            *,
            job_id: str,
            claimed_at: datetime,
            owner: str,
            lease_ttl_seconds: int,
    ) -> SchedulerJob | None:
        claim_expires_at = claimed_at + timedelta(seconds=lease_ttl_seconds)
        conditions = (
            SchedulerJob.id == job_id,
            SchedulerJob.is_enabled.is_(True),
            SchedulerJob.next_run_at.is_not(None),
            SchedulerJob.next_run_at <= claimed_at,
            or_(
                SchedulerJob.claim_expires_at.is_(None),
                SchedulerJob.claim_expires_at <= claimed_at,
                SchedulerJob.claimed_by == owner,
            ),
        )
        statement = (
            update(SchedulerJob)
            .where(*conditions)
            .values(
                claimed_by=owner,
                claimed_at=claimed_at,
                claim_expires_at=claim_expires_at,
                updated_at=claimed_at,
            )
            .execution_options(synchronize_session=False)
        )
        result = await session.execute(statement)
        rowcount = int(getattr(result, "rowcount", 0) or 0)
        if rowcount != 1:
            return None
        await session.flush()
        session.expire_all()
        return await session.get(SchedulerJob, job_id, populate_existing=True)

    @classmethod
    async def extend_claims(
            cls,
            session: AsyncSession,
            *,
            job_ids: list[str],
            owner: str,
            claimed_at: datetime,
            lease_ttl_seconds: int,
            current_time: datetime,
    ) -> int:
        if not job_ids:
            return 0
        statement = (
            update(SchedulerJob)
            .where(
                SchedulerJob.id.in_(job_ids),
                SchedulerJob.claimed_by == owner,
                SchedulerJob.claimed_at == claimed_at,
            )
            .values(claim_expires_at=current_time + timedelta(seconds=lease_ttl_seconds))
            .execution_options(synchronize_session=False)
        )
        result = await session.execute(statement)
        return int(getattr(result, "rowcount", 0) or 0)


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

    @classmethod
    async def purge_finished_before(cls, session: AsyncSession, finished_before: datetime) -> int:
        statement = delete(SchedulerRun).where(
            SchedulerRun.finished_at.is_not(None),
            SchedulerRun.finished_at < finished_before,
        )
        result = await session.execute(statement)
        return int(getattr(result, "rowcount", 0) or 0)
