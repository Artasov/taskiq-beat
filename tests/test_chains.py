from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from taskiq import InMemoryBroker

from taskiq_beat import (
    CHAIN_ORCHESTRATOR_TASK_NAME,
    ChainAbortedError,
    ChainStep,
    ChainStepFailedError,
    ImmediateDispatch,
    IntervalTrigger,
    OneOffSchedule,
    PeriodicSchedule,
    SchedulerApp,
    SchedulerConfig,
    TaskChain,
)
from taskiq_beat.chains import ChainOrchestrator


@pytest.fixture()
def chain_broker() -> InMemoryBroker:
    return InMemoryBroker()


@pytest.fixture()
async def chain_app(session_factory, chain_broker: InMemoryBroker) -> SchedulerApp:
    await chain_broker.startup()

    chain_broker.state.executions = []
    chain_broker.state.failures = {}

    @chain_broker.task(task_name="tests.chain.step_a")
    async def step_a(value: int) -> int:
        chain_broker.state.executions.append(("step_a", value))
        return value + 1

    @chain_broker.task(task_name="tests.chain.step_b")
    async def step_b(value: int, *, suffix: str = "") -> str:
        chain_broker.state.executions.append(("step_b", value))
        return f"{value}{suffix}"

    @chain_broker.task(task_name="tests.chain.flaky")
    async def flaky(name: str) -> str:
        chain_broker.state.executions.append(("flaky", name))
        remaining = chain_broker.state.failures.get(name, 0)
        if remaining > 0:
            chain_broker.state.failures[name] = remaining - 1
            raise RuntimeError(f"flaky-{name}-{remaining}")
        return f"ok-{name}"

    return SchedulerApp(
        broker=chain_broker,
        session_factory=session_factory,
        config=SchedulerConfig(
            sync_interval_seconds=0.05,
            idle_sleep_seconds=0.01,
            claim_ttl_seconds=5,
            dispatch_retry_seconds=1,
        ),
    )


def test_task_chain_requires_at_least_one_step() -> None:
    with pytest.raises(ValueError, match="at least one step"):
        TaskChain(steps=[])


def test_task_chain_rejects_invalid_failure_policy() -> None:
    with pytest.raises(ValueError, match="on_failure"):
        TaskChain(steps=[ChainStep(task="x")], on_failure="skip")  # type: ignore[arg-type]


def test_chain_step_rejects_bad_attempts() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        ChainStep(task="x", max_attempts=0)


def test_chain_step_rejects_bad_post_delay() -> None:
    with pytest.raises(ValueError, match="post_delay_ms"):
        ChainStep(task="x", post_delay_ms=-1)


def test_scheduler_app_registers_chain_orchestrator_task(chain_app: SchedulerApp) -> None:
    task = chain_app.broker.find_task(CHAIN_ORCHESTRATOR_TASK_NAME)
    assert task is not None


@pytest.mark.asyncio()
async def test_chain_builder_persists_job(db_session, chain_app: SchedulerApp) -> None:
    job = await chain_app.chain(
        steps=[
            ChainStep(task="tests.chain.step_a", args=[10]),
            ChainStep(task="tests.chain.step_b", args=[1], kwargs={"suffix": "!"}, post_delay_ms=250),
        ],
    ).schedule(
        db_session,
        trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(hours=1)),
        name="My chain",
    )

    assert job.task_name == CHAIN_ORCHESTRATOR_TASK_NAME
    assert job.metadata_payload["chain"] is True
    assert job.metadata_payload["chain_step_count"] == 2
    assert job.task_kwargs["on_failure"] == "stop"
    assert len(job.task_kwargs["steps"]) == 2
    assert job.task_kwargs["steps"][0]["task_name"] == "tests.chain.step_a"
    assert job.task_kwargs["steps"][1]["post_delay_ms"] == 250


@pytest.mark.asyncio()
async def test_chain_builder_defaults_to_immediate_dispatch(db_session, chain_app: SchedulerApp) -> None:
    job = await chain_app.chain(
        steps=[ChainStep(task="tests.chain.step_a", args=[10])],
    ).schedule(db_session)

    assert job.task_name == CHAIN_ORCHESTRATOR_TASK_NAME
    assert job.kind == "one_off"
    assert job.strategy == "immediate"
    assert job.is_enabled is False
    assert job.next_run_at is None
    assert job.dispatch_count == 1
    assert job.last_dispatched_task_id is not None


@pytest.mark.asyncio()
async def test_chain_builder_rejects_immediate_dispatch_upsert(db_session, chain_app: SchedulerApp) -> None:
    with pytest.raises(ValueError, match="ImmediateDispatch cannot be used with upsert"):
        await chain_app.chain(
            steps=[ChainStep(task="tests.chain.step_a", args=[10])],
        ).upsert(
            db_session,
            trigger=ImmediateDispatch(),
            job_id="tests.chain.immediate",
        )


@pytest.mark.asyncio()
async def test_chain_orchestrator_runs_steps_sequentially(chain_app: SchedulerApp) -> None:
    orchestrator = ChainOrchestrator(chain_app.broker)
    result = await orchestrator.execute(
        steps=[
            {"task_name": "tests.chain.step_a", "args": [41], "kwargs": {}},
            {"task_name": "tests.chain.step_b", "args": [7], "kwargs": {"suffix": "x"}},
        ],
        on_failure="stop",
        max_chain_attempts=1,
        default_step_max_attempts=1,
        default_step_retry_delay_seconds=0.0,
        default_step_timeout_seconds=None,
        wait_poll_interval_seconds=0.01,
    )
    executions = chain_app.broker.state.executions
    assert result["status"] == "completed"
    assert result["completed_steps"] == 2
    assert [name for name, _ in executions] == ["step_a", "step_b"]


@pytest.mark.asyncio()
async def test_chain_orchestrator_uses_post_delay_between_steps(monkeypatch, chain_app: SchedulerApp) -> None:
    delayed_steps: list[int | None] = []

    async def fake_sleep_after_step(self: ChainOrchestrator, step: dict) -> None:
        delayed_steps.append(step.get("post_delay_ms"))

    monkeypatch.setattr(ChainOrchestrator, "sleep_after_step", fake_sleep_after_step)

    orchestrator = ChainOrchestrator(chain_app.broker)
    await orchestrator.execute(
        steps=[
            {"task_name": "tests.chain.step_a", "args": [1], "kwargs": {}, "post_delay_ms": 100},
            {"task_name": "tests.chain.step_a", "args": [2], "kwargs": {}, "post_delay_ms": 200},
            {"task_name": "tests.chain.step_b", "args": [3], "kwargs": {}},
        ],
        on_failure="stop",
        max_chain_attempts=1,
        default_step_max_attempts=1,
        default_step_retry_delay_seconds=0.0,
        default_step_timeout_seconds=None,
        wait_poll_interval_seconds=0.01,
    )

    assert delayed_steps == [100, 200]


@pytest.mark.asyncio()
async def test_chain_orchestrator_sleep_after_step_converts_ms_to_seconds(
    monkeypatch,
    chain_app: SchedulerApp,
) -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    orchestrator = ChainOrchestrator(chain_app.broker)
    await orchestrator.sleep_after_step({"post_delay_ms": 250})
    await orchestrator.sleep_after_step({"post_delay_ms": 0})
    await orchestrator.sleep_after_step({})

    assert delays == [0.25]


@pytest.mark.asyncio()
async def test_chain_orchestrator_retries_step_until_success(chain_app: SchedulerApp) -> None:
    chain_app.broker.state.failures["flaky-1"] = 2
    orchestrator = ChainOrchestrator(chain_app.broker)
    result = await orchestrator.execute(
        steps=[
            {
                "task_name": "tests.chain.flaky",
                "args": ["flaky-1"],
                "kwargs": {},
                "max_attempts": 3,
                "retry_delay_seconds": 0,
                "timeout_seconds": None,
            },
        ],
        on_failure="stop",
        max_chain_attempts=1,
        default_step_max_attempts=1,
        default_step_retry_delay_seconds=0.0,
        default_step_timeout_seconds=None,
        wait_poll_interval_seconds=0.01,
    )
    assert result["status"] == "completed"
    assert result["step_attempts"] == [3]


@pytest.mark.asyncio()
async def test_chain_orchestrator_stops_on_failure(chain_app: SchedulerApp) -> None:
    chain_app.broker.state.failures["flaky-2"] = 5
    orchestrator = ChainOrchestrator(chain_app.broker)
    with pytest.raises(ChainAbortedError) as exc_info:
        await orchestrator.execute(
            steps=[
                {"task_name": "tests.chain.step_a", "args": [1], "kwargs": {}},
                {
                    "task_name": "tests.chain.flaky",
                    "args": ["flaky-2"],
                    "kwargs": {},
                    "max_attempts": 2,
                    "retry_delay_seconds": 0,
                },
                {"task_name": "tests.chain.step_b", "args": [2], "kwargs": {}},
            ],
            on_failure="stop",
            max_chain_attempts=1,
            default_step_max_attempts=1,
            default_step_retry_delay_seconds=0.0,
            default_step_timeout_seconds=None,
            wait_poll_interval_seconds=0.01,
        )
    step_names = [name for name, _ in chain_app.broker.state.executions]
    assert step_names == ["step_a", "flaky", "flaky"]
    assert isinstance(exc_info.value.last_error, ChainStepFailedError)
    assert exc_info.value.last_error.step_index == 1
    assert exc_info.value.chain_attempts == 1


@pytest.mark.asyncio()
async def test_chain_orchestrator_restarts_chain(chain_app: SchedulerApp) -> None:
    chain_app.broker.state.failures["flaky-3"] = 1
    orchestrator = ChainOrchestrator(chain_app.broker)
    result = await orchestrator.execute(
        steps=[
            {"task_name": "tests.chain.step_a", "args": [1], "kwargs": {}},
            {
                "task_name": "tests.chain.flaky",
                "args": ["flaky-3"],
                "kwargs": {},
                "max_attempts": 1,
            },
        ],
        on_failure="restart",
        max_chain_attempts=3,
        default_step_max_attempts=1,
        default_step_retry_delay_seconds=0.0,
        default_step_timeout_seconds=None,
        wait_poll_interval_seconds=0.01,
    )
    step_names = [name for name, _ in chain_app.broker.state.executions]
    assert step_names == ["step_a", "flaky", "step_a", "flaky"]
    assert result["chain_attempts"] == 2


@pytest.mark.asyncio()
async def test_chain_orchestrator_rejects_unknown_task(chain_app: SchedulerApp) -> None:
    orchestrator = ChainOrchestrator(chain_app.broker)
    with pytest.raises(ChainAbortedError) as exc_info:
        await orchestrator.execute(
            steps=[{"task_name": "tests.chain.does_not_exist", "args": [], "kwargs": {}}],
            on_failure="stop",
            max_chain_attempts=1,
            default_step_max_attempts=1,
            default_step_retry_delay_seconds=0.0,
            default_step_timeout_seconds=None,
            wait_poll_interval_seconds=0.01,
        )
    assert "not registered" in exc_info.value.last_error.reason


@pytest.mark.asyncio()
async def test_chain_builder_accepts_task_object_directly(db_session, chain_app: SchedulerApp) -> None:
    step_a_task = chain_app.registry.get_task("tests.chain.step_a")
    step_b_task = chain_app.registry.get_task("tests.chain.step_b")

    job = await chain_app.chain(
        steps=[
            ChainStep(task=step_a_task, args=[1]),
            ChainStep(task=step_b_task, args=[2]),
        ],
    ).schedule(
        db_session,
        trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(hours=1)),
    )

    assert [step["task_name"] for step in job.task_kwargs["steps"]] == [
        "tests.chain.step_a",
        "tests.chain.step_b",
    ]


@pytest.mark.asyncio()
async def test_scheduler_dispatches_chain_end_to_end(db_session, chain_app: SchedulerApp) -> None:
    await chain_app.start()
    try:
        await chain_app.chain(
            steps=[
                ChainStep(task="tests.chain.step_a", args=[0]),
                ChainStep(task="tests.chain.step_b", args=[5], kwargs={"suffix": "?"}),
            ],
            wait_poll_interval_seconds=0.01,
        ).upsert(
            db_session,
            trigger=OneOffSchedule(run_at=datetime.now(UTC)),
            job_id="tests.chain.one_off",
            name="Chain one-off",
        )

        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            names = [name for name, _ in chain_app.broker.state.executions]
            if names == ["step_a", "step_b"]:
                break
            await asyncio.sleep(0.05)

        assert [name for name, _ in chain_app.broker.state.executions] == ["step_a", "step_b"]
    finally:
        await chain_app.stop()


@pytest.mark.asyncio()
async def test_chain_builder_upsert_updates_existing_job(db_session, chain_app: SchedulerApp) -> None:
    first = await chain_app.chain(
        steps=[ChainStep(task="tests.chain.step_a", args=[1])],
    ).upsert(
        db_session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        job_id="tests.chain.stable",
        name="Chain",
    )
    second = await chain_app.chain(
        steps=[
            ChainStep(task="tests.chain.step_a", args=[2]),
            ChainStep(task="tests.chain.step_b", args=[3]),
        ],
    ).upsert(
        db_session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        job_id="tests.chain.stable",
        name="Chain",
    )
    assert first.id == second.id
    assert second.task_kwargs["steps"][0]["args"] == [2]
    assert len(second.task_kwargs["steps"]) == 2
