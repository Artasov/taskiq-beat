from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from taskiq_beat import IntervalTrigger, OneOffSchedule, PeriodicSchedule, SchedulerApp, SchedulerConfig
from taskiq_beat.models import SchedulerRun


@pytest.fixture()
async def scheduler_app(session_factory, broker) -> SchedulerApp:
    @broker.task(task_name="tests.ping")
    async def ping_task() -> None:
        return None

    return SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(dispatch_retry_seconds=1),
    )


@pytest.mark.asyncio()
async def test_engine_dispatches_one_off_job(monkeypatch, session_factory, scheduler_app: SchedulerApp) -> None:
    dispatched: list[str] = []
    task = scheduler_app.registry.get_task("tests.ping")

    async def fake_kiq(*args, **kwargs):
        dispatched.append("called")

        class Result:
            task_id = "task-1"

        return Result()

    monkeypatch.setattr(task, "kiq", fake_kiq)

    async with session_factory() as session:
        job = await scheduler_app.create_scheduler(
            task=task,
            trigger=OneOffSchedule(run_at=datetime.now(UTC) - timedelta(seconds=1)),
        ).schedule(session)

    await scheduler_app.engine.run_once()

    async with session_factory() as session:
        stored_job = await scheduler_app.get_job(session, job.id)
        runs = list((await session.execute(select(SchedulerRun))).scalars())

    assert dispatched == ["called"]
    assert stored_job.is_enabled is False
    assert stored_job.next_run_at is None
    assert stored_job.dispatch_count == 1
    assert len(runs) == 1
    assert runs[0].status == "dispatched"


@pytest.mark.asyncio()
async def test_engine_retries_after_dispatch_error(monkeypatch, session_factory, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.ping")

    async def fake_kiq(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(task, "kiq", fake_kiq)

    async with session_factory() as session:
        job = await scheduler_app.create_scheduler(
            task=task,
            trigger=OneOffSchedule(run_at=datetime.now(UTC) - timedelta(seconds=1)),
        ).schedule(session)

    await scheduler_app.engine.run_once()

    async with session_factory() as session:
        stored_job = await scheduler_app.get_job(session, job.id)
        run = (await session.execute(select(SchedulerRun))).scalar_one()

    assert stored_job.is_enabled is True
    assert stored_job.next_run_at is not None
    assert stored_job.last_error == "boom"
    assert run.status == "failed"
    assert run.error == "boom"


@pytest.mark.asyncio()
async def test_engine_restores_jobs_from_storage(session_factory, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.ping")

    async with session_factory() as session:
        job = await scheduler_app.create_scheduler(
            task=task,
            trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        ).schedule(session)

    restored_app = SchedulerApp(
        broker=scheduler_app.broker,
        session_factory=session_factory,
        config=SchedulerConfig(),
    )
    await restored_app.engine.sync_all()

    assert str(job.id) in restored_app.engine.jobs
    assert any(item.job_id == str(job.id) for item in restored_app.engine.heap)


@pytest.mark.asyncio()
async def test_engine_is_updated_immediately_when_new_job_is_scheduled(session_factory, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.ping")

    async with session_factory() as session:
        job = await scheduler_app.create_scheduler(
            task=task,
            trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        ).schedule(session)

    assert str(job.id) in scheduler_app.engine.jobs
    assert any(item.job_id == str(job.id) for item in scheduler_app.engine.heap)


@pytest.mark.asyncio()
async def test_engine_dispatches_periodic_job_multiple_times(monkeypatch, session_factory, scheduler_app: SchedulerApp) -> None:
    dispatched: list[str] = []
    task = scheduler_app.registry.get_task("tests.ping")

    async def fake_kiq(*args, **kwargs):
        dispatched.append("called")

        class Result:
            task_id = f"task-{len(dispatched)}"

        return Result()

    monkeypatch.setattr(task, "kiq", fake_kiq)

    async with session_factory() as session:
        job = await scheduler_app.create_scheduler(
            task=task,
            trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=1)),
        ).schedule(session)
        job.next_run_at = datetime.now(UTC) - timedelta(seconds=1)
        job.updated_at = datetime.now(UTC)
        await session.commit()

    await scheduler_app.engine.sync_all()
    await scheduler_app.engine.run_once()
    await asyncio.sleep(1.1)
    await scheduler_app.engine.run_once()

    async with session_factory() as session:
        stored_job = await scheduler_app.get_job(session, job.id)
        runs = list((await session.execute(select(SchedulerRun).where(SchedulerRun.job_id == job.id))).scalars())

    assert len(dispatched) >= 2
    assert stored_job.dispatch_count >= 2
    assert stored_job.is_enabled is True
    assert len(runs) >= 2
