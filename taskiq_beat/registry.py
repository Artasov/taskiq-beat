from __future__ import annotations

import json
from collections.abc import Sequence

from taskiq_beat.types import TaskiqBroker, TaskiqTask, TaskLoader, TaskReference


class TaskRegistry:
    """Resolve task references through the broker and optional lazy loader."""

    def __init__(self, *, broker: TaskiqBroker, task_loader: TaskLoader | None = None) -> None:
        self.broker = broker
        self.task_loader = task_loader

    def load(self) -> tuple[str, ...]:
        """Trigger task registration side effects if a loader was provided."""
        if self.task_loader is None:
            return ()
        return tuple(self.task_loader())

    @staticmethod
    def get_task_name(task: TaskReference) -> str:
        """Normalize a task reference into a non-empty task name."""
        if isinstance(task, str):
            return task.strip()
        task_name = str(getattr(task, "task_name", "") or "").strip()
        if not task_name:
            raise ValueError("Scheduler task must have a task_name.")
        return task_name

    def get_task(self, task_name: str) -> TaskiqTask:
        """Resolve a task, retrying after lazy loading if needed."""
        normalized_name = task_name.strip()
        task = self.broker.find_task(normalized_name)
        if task is None:
            self.load()
            task = self.broker.find_task(normalized_name)
        if task is None:
            raise ValueError(f"Task '{normalized_name}' is not registered in Taskiq.")
        return task

    def validate_task(self, task: TaskReference) -> str:
        """Ensure the task exists in the broker and return its name."""
        task_name = self.get_task_name(task)
        self.get_task(task_name)
        return task_name

    @classmethod
    def validate_payload(
            cls,
            args: Sequence[object] | None,
            kwargs: dict[str, object] | None,
            metadata: dict[str, object] | None,
    ) -> None:
        """Fail fast if scheduler payload pieces are not JSON serializable."""
        json.dumps(list(args or []))
        json.dumps(dict(kwargs or {}))
        json.dumps(dict(metadata or {}))
