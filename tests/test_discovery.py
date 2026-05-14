from __future__ import annotations

import importlib
from pathlib import Path
from uuid import uuid4

import pytest

from taskiq_beat import TaskDiscovery, discover_task_modules


def write_package(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "__init__.py").write_text("", encoding="utf-8")


def make_discovery_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, object]:
    package_name = f"discovery_case_{uuid4().hex}"
    package_path = tmp_path / package_name
    modules_path = package_path / "modules"
    write_package(package_path)
    write_package(modules_path)
    write_package(package_path / "modules" / "billing")
    write_package(package_path / "modules" / "chat")
    write_package(package_path / "modules" / "chat" / "tasks")
    write_package(package_path / "modules" / "chat" / "tasks" / "nested")
    write_package(package_path / "modules" / "empty")
    (package_path / "broker.py").write_text(
        "from taskiq import InMemoryBroker\n"
        "broker = InMemoryBroker()\n",
        encoding="utf-8",
    )
    (package_path / "modules" / "billing" / "tasks.py").write_text(
        f"from {package_name}.broker import broker\n"
        "@broker.task(task_name='billing.send_invoice')\n"
        "async def send_invoice() -> None:\n"
        "    return None\n",
        encoding="utf-8",
    )
    (package_path / "modules" / "chat" / "tasks" / "email.py").write_text(
        f"from {package_name}.broker import broker\n"
        "@broker.task(task_name='chat.send_email')\n"
        "async def send_email() -> None:\n"
        "    return None\n",
        encoding="utf-8",
    )
    (package_path / "modules" / "chat" / "tasks" / "nested" / "cleanup.py").write_text(
        f"from {package_name}.broker import broker\n"
        "@broker.task(task_name='chat.cleanup')\n"
        "async def cleanup() -> None:\n"
        "    return None\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    broker_module = importlib.import_module(f"{package_name}.broker")
    return package_name, broker_module.broker


def test_task_discovery_finds_module_and_package_tasks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package_name, broker = make_discovery_app(tmp_path, monkeypatch)
    discovery = TaskDiscovery(broker=broker, packages=[f"{package_name}.modules"])

    module_names = discovery.module_names()

    assert module_names == (
        f"{package_name}.modules.billing.tasks",
        f"{package_name}.modules.chat.tasks",
        f"{package_name}.modules.chat.tasks.email",
        f"{package_name}.modules.chat.tasks.nested",
        f"{package_name}.modules.chat.tasks.nested.cleanup",
    )


def test_task_discovery_imports_modules_and_logs_tasks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    log_capture,
) -> None:
    package_name, broker = make_discovery_app(tmp_path, monkeypatch)
    log_capture.set_level("INFO", logger="taskiq_beat.discovery")
    discovery = TaskDiscovery(broker=broker, packages=[f"{package_name}.modules"])

    module_names = discovery.import_modules()

    assert f"{package_name}.modules.billing.tasks" in module_names
    assert discovery.task_names() == ("billing.send_invoice", "chat.cleanup", "chat.send_email")
    assert "Taskiq task modules discovered" in log_capture.text
    assert "modules_count=5" in log_capture.text
    assert "tasks_count=3" in log_capture.text
    assert "  - billing.send_invoice" in log_capture.text


def test_task_discovery_can_skip_nested_task_packages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package_name, broker = make_discovery_app(tmp_path, monkeypatch)
    discovery = TaskDiscovery(
        broker=broker,
        packages=[f"{package_name}.modules"],
        include_nested=False,
    )

    module_names = discovery.module_names()

    assert module_names == (
        f"{package_name}.modules.billing.tasks",
        f"{package_name}.modules.chat.tasks",
    )


def test_discover_task_modules_helper_imports_tasks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package_name, broker = make_discovery_app(tmp_path, monkeypatch)

    module_names = discover_task_modules(
        [f"{package_name}.modules"],
        broker=broker,
        log_tasks=False,
    )

    assert f"{package_name}.modules.chat.tasks.email" in module_names
    assert sorted(broker.local_task_registry) == ["billing.send_invoice", "chat.cleanup", "chat.send_email"]


def test_task_discovery_rejects_non_package() -> None:
    discovery = TaskDiscovery(packages=["taskiq_beat.discovery"])

    with pytest.raises(ValueError, match="must be a package"):
        discovery.module_names()
