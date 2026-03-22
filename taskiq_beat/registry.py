from __future__ import annotations

import json
from collections.abc import Sequence

from taskiq_beat.types import TaskiqBroker, TaskiqTask, TaskLoader, TaskReference


class TaskRegistry:
    def __init__(self, *, broker: TaskiqBroker, task_loader: TaskLoader | None = None) -> None:
        self.broker = broker
        self.task_loader = task_loader

    def load(self) -> tuple[str, ...]:
        if self.task_loader is None:
            return ()
        return tuple(self.task_loader())

    def get_task_name(self, task: TaskReference) -> str:
        if isinstance(task, str):
            return task.strip()
        task_name = str(getattr(task, "task_name", "") or "").strip()
        if not task_name:
            raise ValueError("Scheduler task must have a task_name.")
        return task_name

    def get_task(self, task_name: str) -> TaskiqTask:
        normalized_name = task_name.strip()
        task = self.broker.find_task(normalized_name)
        if task is None:
            self.load()
            task = self.broker.find_task(normalized_name)
        if task is None:
            raise ValueError(f"Task '{normalized_name}' is not registered in Taskiq.")
        return task

    def validate_task(self, task: TaskReference) -> str:
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
        json.dumps(list(args or []))
        json.dumps(dict(kwargs or {}))
        json.dumps(dict(metadata or {}))
