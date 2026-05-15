from __future__ import annotations

import sys
from pathlib import Path

import pytest
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
            "--skip-broker-lifespan",
        ]
    )

    assert isinstance(runner, SchedulerRunner)
    assert runner.manage_broker is False
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

    async def startup_hook() -> None:
        calls.append("startup")

    scheduler_app = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(sync_interval_seconds=60),
    )
    runner = SchedulerRunner(
        scheduler_app,
        startup_hooks=(startup_hook,),
        manage_broker=True,
    )

    await runner.start()
    assert calls == ["startup"]
    assert scheduler_app.get_health_snapshot().is_running is True

    await runner.stop()
    assert scheduler_app.get_health_snapshot().is_running is False
