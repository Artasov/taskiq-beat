from __future__ import annotations

from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from taskiq_beat.models import SchedulerBase


async def clear_scheduler_tables(factory: async_sessionmaker[AsyncSession]) -> None:
    """Remove scheduler rows from all tables while preserving the schema."""
    async with factory() as session:
        if session.bind is not None and session.bind.dialect.name == "sqlite":
            await session.execute(text("PRAGMA foreign_keys=OFF"))
        for table in reversed(list(SchedulerBase.metadata.tables.values())):
            await session.execute(delete(table))
        if session.bind is not None and session.bind.dialect.name == "sqlite":
            await session.execute(text("PRAGMA foreign_keys=ON"))
        await session.commit()
