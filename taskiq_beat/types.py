from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class TaskiqDispatchResult(Protocol):
    task_id: str | None


class TaskiqTask(Protocol):
    task_name: str

    def kiq(self, *args: Any, **kwargs: Any) -> Awaitable[TaskiqDispatchResult]: ...


class TaskiqBroker(Protocol):
    def find_task(self, task_name: str) -> TaskiqTask | None: ...


type TaskReference = TaskiqTask | str
type TaskLoader = Callable[[], Sequence[str]]
type AsyncSessionFactory = async_sessionmaker[AsyncSession]
