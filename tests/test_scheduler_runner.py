from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import OperationalError
from taskiq import InMemoryBroker

from taskiq_beat import SchedulerApp, SchedulerConfig
from taskiq_beat.scheduler_runner import ImportPathResolver, SchedulerRunner, SchedulerRunnerCli

SESSION_FACTORY = None


def test_import_path_resolver_loads_attribute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = tmp_path / "sample_runner_target.py"
    module_path.write_text("VALUE = 42\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    assert ImportPathResolver.load("sample_runner_target:VALUE") == 42


def test_cli_builds_runner_from_import_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    module_path = tmp_path / "sample_scheduler_app.py"
    module_path.write_text(
        "\n".join(
            [
                "from taskiq import InMemoryBroker",
                "from taskiq_beat import SchedulerApp",
                "from tests.test_scheduler_runner import SESSION_FACTORY",
                "broker = InMemoryBroker()",
                "scheduler_app = SchedulerApp(broker=broker, session_factory=SESSION_FACTORY)",
                "def startup_hook():",
                "    return None",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(sys.modules[__name__], "SESSION_FACTORY", session_factory, raising=False)

    runner = SchedulerRunnerCli.build_runner(
        [
            "sample_scheduler_app:scheduler_app",
            "--startup",
            "sample_scheduler_app:startup_hook",
            "--retry-delay",
            "2",
            "--skip-broker-lifespan",
        ]
    )

    assert isinstance(runner, SchedulerRunner)
    assert runner.manage_broker is False
    assert runner.retry_delay_seconds == 2
    assert len(runner.startup_hooks) == 1


def test_cli_loads_scheduler_app_from_current_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    module_path = tmp_path / "cwd_scheduler_app.py"
    module_path.write_text(
        "\n".join(
            [
                "from taskiq import InMemoryBroker",
                "from taskiq_beat import SchedulerApp",
                "from tests.test_scheduler_runner import SESSION_FACTORY",
                "broker = InMemoryBroker()",
                "scheduler_app = SchedulerApp(broker=broker, session_factory=SESSION_FACTORY)",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys.modules[__name__], "SESSION_FACTORY", session_factory, raising=False)
    monkeypatch.setattr(sys, "path", [path for path in sys.path if path not in ("", ".")])

    runner = SchedulerRunnerCli.build_runner(["cwd_scheduler_app:scheduler_app"])

    assert isinstance(runner, SchedulerRunner)


@pytest.mark.asyncio()
async def test_runner_starts_and_stops_scheduler(session_factory) -> None:
    broker = InMemoryBroker()

    @broker.task(task_name="tests.runner_ping")
    async def runner_ping() -> None:
        return None

    calls: list[str] = []

    def load_tasks() -> tuple[str, ...]:
        calls.append("load")
        return ()

    async def startup_hook() -> None:
        calls.append("startup")

    scheduler_app = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(sync_interval_seconds=60),
        task_loader=load_tasks,
    )
    runner = SchedulerRunner(
        scheduler_app,
        startup_hooks=(startup_hook,),
        manage_broker=True,
    )

    await runner.start()
    await runner.start()
    assert calls == ["load", "startup"]
    assert scheduler_app.get_health_snapshot().is_running is True

    await runner.stop()
    await runner.stop()
    assert scheduler_app.get_health_snapshot().is_running is False


@pytest.mark.parametrize("failure", [ConnectionError(), OperationalError("", {}, Exception("unavailable"))])
async def test_runner_cleans_partial_start_before_retry(session_factory, monkeypatch, failure) -> None:
    app = SchedulerApp(broker=InMemoryBroker(), session_factory=session_factory)
    shutdown = asyncio.Event()
    calls: list[str] = []

    async def start_app() -> None:
        calls.append("app.start")
        if calls.count("app.start") == 1:
            raise failure
        shutdown.set()

    monkeypatch.setattr(app.broker, "startup", lambda: calls.append("broker.start"))
    monkeypatch.setattr(app.broker, "shutdown", lambda: calls.append("broker.stop"))
    monkeypatch.setattr(app, "start", start_app)
    monkeypatch.setattr(app, "stop", AsyncMock(side_effect=lambda: calls.append("app.stop")))
    runner = SchedulerRunner(app, retry_delay_seconds=0.001)
    await asyncio.wait_for(runner.run_forever(shutdown), timeout=2)

    assert calls == ["broker.start", "app.start", "app.stop", "broker.stop"] * 2


async def test_runner_cancels_startup_on_shutdown(session_factory, monkeypatch) -> None:
    app = SchedulerApp(broker=InMemoryBroker(), session_factory=session_factory)
    started = asyncio.Event()
    shutdown = asyncio.Event()

    async def blocked_startup() -> None:
        started.set()
        await asyncio.Event().wait()

    release = AsyncMock()
    monkeypatch.setattr(app.broker, "startup", blocked_startup)
    monkeypatch.setattr(app.broker, "shutdown", release)
    runner = SchedulerRunner(app)
    running = asyncio.create_task(runner.run_forever(shutdown))
    await asyncio.wait_for(started.wait(), timeout=2)
    shutdown.set()
    await asyncio.wait_for(running, timeout=2)
    release.assert_awaited_once()


async def test_runner_propagates_permanent_errors_without_retry(session_factory) -> None:
    app = SchedulerApp(broker=InMemoryBroker(), session_factory=session_factory)
    hook = AsyncMock(side_effect=ValueError("invalid schedule"))
    runner = SchedulerRunner(app, startup_hooks=(hook,), retry_delay_seconds=0.001)
    with pytest.raises(ValueError, match="invalid schedule"):
        await asyncio.wait_for(runner.run_forever(asyncio.Event()), timeout=2)
    hook.assert_awaited_once()


async def test_runner_restores_signal_handlers() -> None:
    original = signal.getsignal(signal.SIGTERM)
    shutdown = asyncio.Event()
    with SchedulerRunner.bind_stop_signals(shutdown):
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        handler(signal.SIGTERM, None)
        await asyncio.wait_for(shutdown.wait(), timeout=2)
    assert signal.getsignal(signal.SIGTERM) == original
