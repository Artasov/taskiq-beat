from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from taskiq_beat import IntervalTrigger, OneOffSchedule, PeriodicSchedule, SchedulerApp, SchedulerConfig
from taskiq_beat.models import SchedulerJob, SchedulerRun


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
async def test_engine_is_updated_immediately_when_new_job_is_scheduled(
    session_factory, scheduler_app: SchedulerApp
) -> None:
    task = scheduler_app.registry.get_task("tests.ping")

    async with session_factory() as session:
        job = await scheduler_app.create_scheduler(
            task=task,
            trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        ).schedule(session)

    assert str(job.id) in scheduler_app.engine.jobs
    assert any(item.job_id == str(job.id) for item in scheduler_app.engine.heap)


@pytest.mark.asyncio()
async def test_engine_dispatches_periodic_job_multiple_times(
    monkeypatch, session_factory, scheduler_app: SchedulerApp
) -> None:
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


@pytest.mark.asyncio()
async def test_engine_logs_dispatch_lifecycle(
    monkeypatch, session_factory, scheduler_app: SchedulerApp, caplog
) -> None:
    caplog.set_level(logging.INFO, logger="taskiq_beat.engine")
    task = scheduler_app.registry.get_task("tests.ping")

    async def fake_kiq(*args, **kwargs):
        class Result:
            task_id = "task-1"

        return Result()

    monkeypatch.setattr(task, "kiq", fake_kiq)

    async with session_factory() as session:
        await scheduler_app.create_scheduler(
            task=task,
            trigger=OneOffSchedule(run_at=datetime.now(UTC) - timedelta(seconds=1)),
        ).schedule(session)

    await scheduler_app.engine.run_once()

    messages = [record.getMessage() for record in caplog.records if record.name == "taskiq_beat.engine"]

    assert "Dispatching scheduler jobs batch." in messages
    assert "Scheduler job dispatched." in messages


@pytest.mark.asyncio()
async def test_engine_claims_job_once_across_multiple_schedulers(monkeypatch, session_factory, broker) -> None:
    dispatched: list[str] = []

    @broker.task(task_name="tests.claim_once")
    async def ping_task() -> None:
        return None

    task = broker.find_task("tests.claim_once")

    async def fake_kiq(*args, **kwargs):
        dispatched.append("called")

        class Result:
            task_id = "task-claim-once"

        await asyncio.sleep(0.05)
        return Result()

    monkeypatch.setattr(task, "kiq", fake_kiq)

    scheduler_a = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(scheduler_id="scheduler-a", claim_ttl_seconds=5),
    )
    scheduler_b = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(scheduler_id="scheduler-b", claim_ttl_seconds=5),
    )

    async with session_factory() as session:
        job = await scheduler_a.create_scheduler(
            task=task,
            trigger=OneOffSchedule(run_at=datetime.now(UTC) - timedelta(seconds=1)),
        ).schedule(session)

    await scheduler_a.engine.sync_all()
    await scheduler_b.engine.sync_all()
    await asyncio.gather(
        scheduler_a.engine.run_once(),
        scheduler_b.engine.run_once(),
    )

    async with session_factory() as session:
        stored_job = await scheduler_a.get_job(session, job.id)
        runs = list((await session.execute(select(SchedulerRun).where(SchedulerRun.job_id == job.id))).scalars())

    assert dispatched == ["called"]
    assert stored_job.is_enabled is False
    assert len(runs) == 1


@pytest.mark.asyncio()
async def test_engine_keeps_claim_alive_during_slow_dispatch(monkeypatch, session_factory, broker) -> None:
    dispatched: list[str] = []

    @broker.task(task_name="tests.slow_claim")
    async def slow_task() -> None:
        return None

    task = broker.find_task("tests.slow_claim")

    async def fake_kiq(*args, **kwargs):
        dispatched.append("called")

        class Result:
            task_id = f"slow-task-{len(dispatched)}"

        await asyncio.sleep(1.2)
        return Result()

    monkeypatch.setattr(task, "kiq", fake_kiq)

    scheduler_a = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(scheduler_id="scheduler-a", claim_ttl_seconds=1),
    )
    scheduler_b = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(scheduler_id="scheduler-b", claim_ttl_seconds=1),
    )

    async with session_factory() as session:
        job = await scheduler_a.create_scheduler(
            task=task,
            trigger=OneOffSchedule(run_at=datetime.now(UTC) - timedelta(seconds=1)),
        ).schedule(session)

    await scheduler_a.engine.sync_all()
    await scheduler_b.engine.sync_all()

    scheduler_a_task = asyncio.create_task(scheduler_a.engine.run_once())
    await asyncio.sleep(1.05)
    await scheduler_b.engine.run_once()
    await scheduler_a_task

    async with session_factory() as session:
        stored_job = await scheduler_a.get_job(session, job.id)
        runs = list((await session.execute(select(SchedulerRun).where(SchedulerRun.job_id == job.id))).scalars())

    assert dispatched == ["called"]
    assert stored_job.is_enabled is False
    assert len(runs) == 1


@pytest.mark.asyncio()
async def test_engine_cleans_old_runs_when_retention_is_enabled(session_factory, broker) -> None:
    @broker.task(task_name="tests.cleanup")
    async def cleanup_task() -> None:
        return None

    scheduler_app = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(run_history_retention_days=0, run_cleanup_interval_seconds=1),
    )

    async with session_factory() as session:
        job = SchedulerJob(
            id="cleanup-job",
            task_name="tests.cleanup",
            kind="one_off",
            strategy="one_off",
            trigger_payload={"run_at": datetime.now(UTC).isoformat()},
            task_args=[],
            task_kwargs={},
            metadata_payload={},
            is_enabled=False,
            next_run_at=None,
            created_at=datetime.now(UTC) - timedelta(days=1),
            updated_at=datetime.now(UTC) - timedelta(days=1),
        )
        session.add(job)
        await session.flush()
        session.add(
            SchedulerRun(
                job_id=job.id,
                status="dispatched",
                scheduled_for=datetime.now(UTC) - timedelta(days=1),
                dispatched_at=datetime.now(UTC) - timedelta(days=1),
                finished_at=datetime.now(UTC) - timedelta(days=1),
                broker_task_id="cleanup-task-1",
                created_at=datetime.now(UTC) - timedelta(days=1),
                updated_at=datetime.now(UTC) - timedelta(days=1),
            )
        )
        await session.commit()

    await scheduler_app.engine.cleanup_runs()

    async with session_factory() as session:
        runs = list((await session.execute(select(SchedulerRun))).scalars())

    assert runs == []
    assert scheduler_app.engine.cleaned_run_count == 1
