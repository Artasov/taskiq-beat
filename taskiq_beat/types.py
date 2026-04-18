from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.task import AsyncTaskiqTask

type TaskiqDispatchResult = AsyncTaskiqTask[Any]


class TaskiqTask(Protocol):
    """Minimal task interface used for validation and dispatch."""

    task_name: str

    def kiq(self, *args: Any, **kwargs: Any) -> Awaitable[TaskiqDispatchResult]: ...


class TaskiqBroker(Protocol):
    """Minimal broker interface needed to resolve registered tasks."""

    def find_task(
        self, task_name: str
    ) -> AsyncTaskiqDecoratedTask[Any, Any] | None: ...


type TaskReference = TaskiqTask | str
type TaskLoader = Callable[[], Sequence[str]]
type AsyncSessionFactory = async_sessionmaker[AsyncSession]
