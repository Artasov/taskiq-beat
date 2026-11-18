from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from taskiq_beat.app import SchedulerApp, SchedulerClient
from taskiq_beat.chains import CHAIN_ORCHESTRATOR_TASK_NAME, ChainStep
from taskiq_beat.config import SchedulerConfig
from taskiq_beat.models import SchedulerJob, SchedulerRun
from taskiq_beat.triggers import IntervalTrigger, OneOffSchedule, PeriodicSchedule


@pytest.mark.asyncio
@pytest.mark.parametrize("record_runs", [True, False])
async def test_client_publishes_immediately_with_task_labels_and_configured_history(
    broker, db_session, session_factory, monkeypatch, record_runs
) -> None:
    client = SchedulerClient(
        broker=broker, session_factory=session_factory, config=SchedulerConfig(record_runs=record_runs),
    )
    send = AsyncMock()
    monkeypatch.setattr(broker, "kick", send)

    @broker.task(task_name="client.echo", queue_name="reports")
    async def echo(value: int) -> int:
        return value

    job = await client.single(task=echo, args=[42]).schedule(db_session)
    send.assert_awaited_once()
    message = send.await_args.args[0]
    assert message.task_name == "client.echo"
    assert message.labels["queue_name"] == "reports"
    assert message.task_id == job.last_dispatched_task_id
    assert job.dispatch_count == 1
    runs = list(await db_session.scalars(select(SchedulerRun)))
    assert [run.broker_task_id for run in runs] == ([message.task_id] if record_runs else [])
    assert client.engine is None
    assert client.registry.task_loader is None


@pytest.mark.asyncio
async def test_client_writes_schedules_for_a_separate_engine(broker, db_session, session_factory, monkeypatch) -> None:
    client = SchedulerClient(broker=broker, session_factory=session_factory)
    runtime = SchedulerApp(broker=broker, session_factory=session_factory)
    send = AsyncMock()
    monkeypatch.setattr(broker, "kick", send)

    @broker.task(task_name="client.periodic")
    async def tick() -> None:
        pass

    periodic = await client.single(task=tick).upsert(
        db_session, trigger=PeriodicSchedule(interval=IntervalTrigger(hours=12)), job_id="periodic",
    )
    one_off = await client.single(task="client.periodic").schedule(
        db_session, trigger=OneOffSchedule(run_at=datetime.now(UTC) - timedelta(seconds=10)),
    )
    await client.pause(db_session, periodic.id)
    resumed = await client.resume(db_session, periodic.id)
    assert resumed.next_run_at is not None
    assert not runtime.engine.jobs
    send.assert_not_awaited()

    await runtime.engine.sync_all()
    await runtime.engine.run_once()
    send.assert_awaited_once()
    await db_session.refresh(one_off)
    assert one_off.dispatch_count == 1
    await client.delete(db_session, periodic.id)
    assert await db_session.get(SchedulerJob, periodic.id) is None


@pytest.mark.asyncio
async def test_client_rejects_unknown_tasks_and_can_build_chains(broker, db_session, session_factory) -> None:
    client = SchedulerClient(broker=broker, session_factory=session_factory)
    with pytest.raises(ValueError, match="not registered"):
        await client.single(task="client.unknown").schedule(db_session)
    assert broker.find_task(CHAIN_ORCHESTRATOR_TASK_NAME) is None

    @broker.task(task_name="client.step")
    async def step() -> None:
        pass

    job = await client.chain(steps=[ChainStep(task=step)]).schedule(
        db_session, trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(days=1)),
    )
    assert job.task_name == CHAIN_ORCHESTRATOR_TASK_NAME
    assert job.task_kwargs["steps"][0]["task_name"] == step.task_name
    assert client.engine is None


def test_worker_bootstrap_retries_failed_load_and_caches_success(broker, session_factory) -> None:
    attempts = 0

    def load():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ImportError("missing module")
        return ["tasks.example"]

    runtime = SchedulerApp(broker=broker, session_factory=session_factory, task_loader=load)
    assert attempts == 0
    with pytest.raises(ImportError, match="missing module"):
        runtime.worker_broker()
    assert runtime.load_tasks() == ("tasks.example",)
    assert runtime.worker_broker() is broker
    assert attempts == 2


def test_declarative_catalog_loads_only_at_explicit_process_start(broker, session_factory, monkeypatch) -> None:
    imported = []
    monkeypatch.setattr("taskiq_beat.discovery.importlib.import_module", imported.append)
    runtime = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        bootstrap_modules=["domain.models"],
        task_modules=["domain.mail.tasks", "domain.billing.tasks"],
    )
    with pytest.raises(ValueError, match="not registered"):
        runtime.registry.get_task("domain.unknown")
    assert not imported
    assert runtime.worker_broker() is broker
    assert runtime.load_tasks() == ("domain.mail.tasks", "domain.billing.tasks")
    assert imported == ["domain.models", "domain.mail.tasks", "domain.billing.tasks"]
