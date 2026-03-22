from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest
from _pytest.logging import LogCaptureFixture
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat.db_utils import clear_scheduler_tables
from taskiq_beat.models import SchedulerBase


@pytest.fixture(scope="session")
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    test_db_path = Path(tempfile.gettempdir()) / f"taskiq_beat_{uuid4().hex}.sqlite3"
    database_url = f"sqlite+aiosqlite:///{test_db_path.as_posix()}"
    engine = create_async_engine(database_url, future=True)
    connection = await engine.connect()
    transaction = await connection.begin()
    try:
        await connection.run_sync(SchedulerBase.metadata.create_all)
        await transaction.commit()
    finally:
        await connection.close()
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    try:
        yield factory
    finally:
        await engine.dispose()
        test_db_path.unlink(missing_ok=True)


async def clear_database(factory: async_sessionmaker[AsyncSession]) -> None:
    await clear_scheduler_tables(factory)


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


@pytest.fixture()
def log_capture(caplog: LogCaptureFixture) -> LogCaptureFixture:
    return caplog
