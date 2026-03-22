from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat.models import SchedulerBase


@pytest.fixture(scope="session")
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    test_db_path = Path(tempfile.gettempdir()) / f"taskiq_beat_{uuid4().hex}.sqlite3"
    database_url = f"sqlite+aiosqlite:///{test_db_path.as_posix()}"
    engine = create_async_engine(database_url, future=True)
    async with engine.begin() as connection:
        await connection.run_sync(SchedulerBase.metadata.create_all)
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    try:
        yield factory
    finally:
        await engine.dispose()
        test_db_path.unlink(missing_ok=True)


async def clear_database(factory: async_sessionmaker[AsyncSession]) -> None:
    async with factory() as session:
        if session.bind is not None and session.bind.dialect.name == "sqlite":
            await session.execute(text("PRAGMA foreign_keys=OFF"))
        for table in reversed(list(SchedulerBase.metadata.tables.values())):
            await session.execute(delete(table))
        if session.bind is not None and session.bind.dialect.name == "sqlite":
            await session.execute(text("PRAGMA foreign_keys=ON"))
        await session.commit()


@pytest.fixture(autouse=True)
async def clean_database(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[None]:
    await clear_database(session_factory)
    yield
    await clear_database(session_factory)


@pytest.fixture()
async def db_session(
    session_factory: async_sessionmaker[AsyncSession],
    clean_database: None,
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


@pytest.fixture()
def broker() -> InMemoryBroker:
    return InMemoryBroker()
