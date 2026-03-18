from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from taskiq_beat import IntervalTrigger, PeriodicSchedule, SchedulerApp, SchedulerConfig
from taskiq_beat.models import SchedulerJob


@pytest.fixture()
async def scheduler_app(session_factory, broker) -> SchedulerApp:
    @broker.task(task_name="tests.echo")
    async def echo_task() -> dict[str, bool]:
        return {"ok": True}

    return SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(),
    )


@pytest.mark.asyncio()
async def test_scheduler_creates_periodic_job(db_session, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.echo")
    scheduler = scheduler_app.create_scheduler(
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=10)),
        name="Echo",
        metadata={"scope": "tests"},
    )

    job = await scheduler.schedule(db_session)

    assert job.task_name == "tests.echo"
    assert job.kind == "periodic"
    assert job.strategy == "interval"
    assert job.next_run_at is not None
    assert job.metadata_payload == {"scope": "tests"}


@pytest.mark.asyncio()
async def test_scheduler_rejects_unknown_task(db_session, scheduler_app: SchedulerApp) -> None:
    scheduler = scheduler_app.create_scheduler(
        task="tests.unknown",
        trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=10)),
    )

    with pytest.raises(ValueError, match="not registered"):
        await scheduler.schedule(db_session)


@pytest.mark.asyncio()
async def test_scheduler_rejects_non_json_payload(db_session, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.echo")
    scheduler = scheduler_app.create_scheduler(
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=10)),
        metadata={"bad": object()},
    )

    with pytest.raises(TypeError):
        await scheduler.schedule(db_session)


@pytest.mark.asyncio()
async def test_scheduler_pause_resume_run_now_and_delete(db_session, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.echo")
    scheduler = scheduler_app.create_scheduler(
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
    )
    job = await scheduler.schedule(db_session)
    initial_next_run_at = job.next_run_at

    paused = await scheduler_app.pause(db_session, job.id)
    assert paused.is_enabled is False
    assert paused.next_run_at is None

    resumed = await scheduler_app.resume(db_session, job.id)
    assert resumed.is_enabled is True
    assert resumed.next_run_at is not None
    assert resumed.next_run_at >= initial_next_run_at

    forced = await scheduler_app.run_now(db_session, job.id)
    assert forced.next_run_at is not None
    assert abs((forced.next_run_at - datetime.now(UTC)).total_seconds()) < 1

    await scheduler_app.delete(db_session, job.id)

    deleted = (await db_session.execute(select(SchedulerJob).where(SchedulerJob.id == job.id))).scalar_one_or_none()
    assert deleted is None


@pytest.mark.asyncio()
async def test_scheduler_app_start_and_stop(scheduler_app: SchedulerApp) -> None:
    await scheduler_app.start()
    assert scheduler_app.engine.runner_task is not None
    await scheduler_app.stop()
    assert scheduler_app.engine.runner_task is None
