from __future__ import annotations

import importlib
import importlib.util
import logging
import pkgutil
from collections.abc import Sequence
from dataclasses import dataclass

from taskiq_beat.types import TaskiqBroker

log = logging.getLogger(__name__)


@dataclass(slots=True)
class TaskDiscovery:
    """Find and import Taskiq task modules from application packages."""

    packages: Sequence[str]
    broker: TaskiqBroker | None = None
    task_module_name: str = "tasks"
    include_nested: bool = True
    log_tasks: bool = True

    def module_names(self) -> tuple[str, ...]:
        """Return import paths for discovered task modules."""
        module_names: set[str] = set()
        for package_name in self.packages:
            package = importlib.import_module(package_name)
            package_path = getattr(package, "__path__", None)
            if package_path is None:
                raise ValueError(f"Discovery package '{package_name}' must be a package.")
            self.add_task_module_names(module_names, package_name)
            for module_info in pkgutil.iter_modules(package_path, f"{package.__name__}."):
                if module_info.ispkg:
                    self.add_task_module_names(module_names, module_info.name)
        return tuple(sorted(module_names))

    def add_task_module_names(self, module_names: set[str], package_name: str) -> None:
        """Add the package task module and nested modules when they exist."""
        task_module_name = f"{package_name}.{self.task_module_name}"
        spec = importlib.util.find_spec(task_module_name)
        if spec is None:
            return
        if spec.origin is not None or spec.submodule_search_locations is not None:
            module_names.add(task_module_name)
        if not self.include_nested or spec.submodule_search_locations is None:
            return
        module_names.update(
            submodule_info.name
            for submodule_info in pkgutil.walk_packages(
                spec.submodule_search_locations,
                f"{task_module_name}.",
            )
        )

    def import_modules(self) -> tuple[str, ...]:
        """Import discovered task modules and log broker task registry contents."""
        module_names = self.module_names()
        for module_name in module_names:
            importlib.import_module(module_name)
        if self.log_tasks:
            log.info(self.log_message(module_names=module_names, task_names=self.task_names()))
        return module_names

    def task_names(self) -> tuple[str, ...]:
        """Return broker task names when the broker exposes a local registry."""
        if self.broker is None:
            return ()
        registry = getattr(self.broker, "local_task_registry", None)
        if registry is None:
            return ()
        return tuple(sorted(str(task_name) for task_name in registry))

    @classmethod
    def log_message(cls, *, module_names: Sequence[str], task_names: Sequence[str]) -> str:
        """Build a compact multiline discovery log message."""
        return (
            "Taskiq task modules discovered\n"
            f"modules_count={len(module_names)}\n"
            "modules=[\n"
            f"{cls.list_lines(module_names)}\n"
            "]\n"
            f"tasks_count={len(task_names)}\n"
            "tasks=[\n"
            f"{cls.list_lines(task_names)}\n"
            "]"
        )

    @staticmethod
    def list_lines(values: Sequence[str]) -> str:
        """Format values as stable log lines."""
        if not values:
            return "  <empty>"
        return "\n".join(f"  - {value}" for value in values)


def discover_task_modules(
        packages: Sequence[str],
        *,
        broker: TaskiqBroker | None = None,
        task_module_name: str = "tasks",
        include_nested: bool = True,
        log_tasks: bool = True,
) -> tuple[str, ...]:
    """Import Taskiq task modules without creating a TaskDiscovery instance explicitly."""
    return TaskDiscovery(
        packages=packages,
        broker=broker,
        task_module_name=task_module_name,
        include_nested=include_nested,
        log_tasks=log_tasks,
    ).import_modules()
