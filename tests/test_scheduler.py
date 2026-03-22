from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from taskiq_beat import IntervalTrigger, PeriodicSchedule, SchedulerApp, SchedulerConfig
from taskiq_beat.models import SchedulerJob, SchedulerRun


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
async def test_scheduler_upsert_creates_and_updates_job_without_duplicates(
    db_session, scheduler_app: SchedulerApp
) -> None:
    task = scheduler_app.registry.get_task("tests.echo")
    scheduler = scheduler_app.create_scheduler(
        job_id="system.echo",
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        name="System echo",
    )

    job = await scheduler.upsert(db_session)
    initial_next_run_at = job.next_run_at
    assert job.id == "system.echo"

    updated_job = await scheduler_app.create_scheduler(
        job_id="system.echo",
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=2)),
        name="System echo updated",
    ).upsert(db_session)

    jobs = list((await db_session.execute(select(SchedulerJob))).scalars())

    assert len(jobs) == 1
    assert updated_job.id == "system.echo"
    assert updated_job.name == "System echo updated"
    assert updated_job.next_run_at is not None
    assert initial_next_run_at is not None
    assert updated_job.next_run_at > initial_next_run_at


@pytest.mark.asyncio()
async def test_scheduler_upsert_preserves_next_run_for_unchanged_job(db_session, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.echo")
    scheduler = scheduler_app.create_scheduler(
        job_id="system.stable",
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
    )

    job = await scheduler.upsert(db_session)
    initial_next_run_at = job.next_run_at

    updated_job = await scheduler.upsert(db_session)

    assert updated_job.next_run_at == initial_next_run_at


@pytest.mark.asyncio()
async def test_scheduler_app_sync_schedules_upserts_multiple_jobs(db_session, scheduler_app: SchedulerApp) -> None:
    task = scheduler_app.registry.get_task("tests.echo")
    jobs = await scheduler_app.sync_schedules(
        db_session,
        [
            scheduler_app.create_scheduler(
                job_id="system.echo.1",
                task=task,
                trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
                name="Echo 1",
            ),
            scheduler_app.create_scheduler(
                job_id="system.echo.2",
                task=task,
                trigger=PeriodicSchedule(interval=IntervalTrigger(hours=2)),
                name="Echo 2",
            ),
        ],
    )

    jobs_after_repeat = await scheduler_app.sync_schedules(
        db_session,
        [
            scheduler_app.create_scheduler(
                job_id="system.echo.1",
                task=task,
                trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
                name="Echo 1",
            ),
            scheduler_app.create_scheduler(
                job_id="system.echo.2",
                task=task,
                trigger=PeriodicSchedule(interval=IntervalTrigger(hours=2)),
                name="Echo 2 updated",
            ),
        ],
    )

    stored_jobs = list((await db_session.execute(select(SchedulerJob).order_by(SchedulerJob.id.asc()))).scalars())

    assert [job.id for job in jobs] == ["system.echo.1", "system.echo.2"]
    assert [job.id for job in jobs_after_repeat] == ["system.echo.1", "system.echo.2"]
    assert len(stored_jobs) == 2
    assert stored_jobs[1].name == "Echo 2 updated"


@pytest.mark.asyncio()
async def test_scheduler_app_start_and_stop(scheduler_app: SchedulerApp) -> None:
    await scheduler_app.start()
    assert scheduler_app.engine.runner_task is not None
    await scheduler_app.stop()
    assert scheduler_app.engine.runner_task is None


@pytest.mark.asyncio()
async def test_scheduler_app_logs_job_lifecycle(db_session, scheduler_app: SchedulerApp, log_capture) -> None:
    log_capture.set_level(logging.INFO, logger="taskiq_beat.app")
    task = scheduler_app.registry.get_task("tests.echo")
    job = await scheduler_app.create_scheduler(
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
    ).schedule(db_session)

    await scheduler_app.pause(db_session, job.id)
    await scheduler_app.resume(db_session, job.id)
    await scheduler_app.run_now(db_session, job.id)
    await scheduler_app.delete(db_session, job.id)

    messages = [record.getMessage() for record in log_capture.records if record.name == "taskiq_beat.app"]

    assert "Scheduler job paused." in messages
    assert "Scheduler job resumed." in messages
    assert "Scheduler job marked to run now." in messages
    assert "Scheduler job deleted." in messages


@pytest.mark.asyncio()
async def test_scheduler_app_purges_run_history(db_session, scheduler_app: SchedulerApp) -> None:
    old_run = SchedulerJob(
        id="job-with-runs",
        task_name="tests.echo",
        kind="one_off",
        strategy="one_off",
        trigger_payload={"run_at": datetime.now(UTC).isoformat()},
        task_args=[],
        task_kwargs={},
        metadata_payload={},
        is_enabled=False,
        next_run_at=None,
        created_at=datetime.now(UTC) - timedelta(days=10),
        updated_at=datetime.now(UTC) - timedelta(days=10),
    )
    db_session.add(old_run)
    await db_session.flush()
    db_session.add(
        SchedulerRun(
            job_id=old_run.id,
            status="dispatched",
            scheduled_for=datetime.now(UTC) - timedelta(days=10),
            dispatched_at=datetime.now(UTC) - timedelta(days=10),
            finished_at=datetime.now(UTC) - timedelta(days=10),
            broker_task_id="task-1",
            created_at=datetime.now(UTC) - timedelta(days=10),
            updated_at=datetime.now(UTC) - timedelta(days=10),
        )
    )
    await db_session.commit()

    deleted_runs = await scheduler_app.purge_runs(
        db_session,
        finished_before=datetime.now(UTC) - timedelta(days=1),
    )

    remaining_runs = list((await db_session.execute(select(SchedulerRun))).scalars())

    assert deleted_runs == 1
    assert remaining_runs == []


@pytest.mark.asyncio()
async def test_scheduler_app_exposes_health_snapshot(scheduler_app: SchedulerApp) -> None:
    snapshot = scheduler_app.get_health_snapshot()

    assert snapshot.scheduler_id == scheduler_app.config.scheduler_id
    assert snapshot.heap_size == 0
    assert snapshot.sync_count == 0


@pytest.mark.asyncio()
async def test_scheduler_resume_handles_sqlite_like_naive_timestamps(
    db_session,
    scheduler_app: SchedulerApp,
) -> None:
    task = scheduler_app.registry.get_task("tests.echo")
    job = await scheduler_app.create_scheduler(
        task=task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
    ).schedule(db_session)

    job.created_at = job.created_at.replace(tzinfo=None)
    job.updated_at = job.updated_at.replace(tzinfo=None)
    await db_session.commit()

    paused = await scheduler_app.pause(db_session, job.id)
    assert paused.is_enabled is False

    resumed = await scheduler_app.resume(db_session, job.id)

    assert resumed.is_enabled is True
    assert resumed.next_run_at is not None
