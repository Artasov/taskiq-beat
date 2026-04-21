from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from taskiq_beat.registry import TaskRegistry
from taskiq_beat.types import TaskiqBroker, TaskReference

log = logging.getLogger(__name__)

CHAIN_ORCHESTRATOR_TASK_NAME = "taskiq_beat.chain_orchestrator"

ChainFailurePolicy = Literal["stop", "restart"]


class ChainStepFailedError(RuntimeError):
    """Raised by the orchestrator when a chain step cannot be completed."""

    def __init__(
            self,
            *,
            step_index: int,
            task_name: str,
            attempts: int,
            reason: str,
    ) -> None:
        super().__init__(
            f"Chain step {step_index} ({task_name}) failed after {attempts} attempt(s): {reason}",
        )
        self.step_index = step_index
        self.task_name = task_name
        self.attempts = attempts
        self.reason = reason


class ChainAbortedError(RuntimeError):
    """Raised when a chain gives up after exhausting its restart budget."""

    def __init__(self, *, chain_attempts: int, last_error: ChainStepFailedError) -> None:
        super().__init__(
            f"Chain aborted after {chain_attempts} attempt(s). Last error: {last_error}",
        )
        self.chain_attempts = chain_attempts
        self.last_error = last_error


@dataclass(slots=True, frozen=True)
class ChainStep:
    """Single step inside a task chain."""

    task: TaskReference
    args: list[object] = field(default_factory=list)
    kwargs: dict[str, object] = field(default_factory=dict)
    max_attempts: int | None = None
    retry_delay_seconds: float | None = None
    timeout_seconds: float | None = None
    post_delay_ms: int | None = None

    def __post_init__(self) -> None:
        """Fail fast on obviously wrong step configuration."""
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError("ChainStep.max_attempts must be >= 1.")
        if self.retry_delay_seconds is not None and self.retry_delay_seconds < 0:
            raise ValueError("ChainStep.retry_delay_seconds must be >= 0.")
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError("ChainStep.timeout_seconds must be > 0.")
        if self.post_delay_ms is not None and self.post_delay_ms < 0:
            raise ValueError("ChainStep.post_delay_ms must be >= 0.")

    def to_payload(self, registry: TaskRegistry) -> dict[str, Any]:
        """Serialize the step into a JSON-safe payload for the orchestrator."""
        task_name = registry.validate_task(self.task)
        TaskRegistry.validate_payload(self.args, self.kwargs, None)
        return {
            "task_name": task_name,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "max_attempts": self.max_attempts,
            "retry_delay_seconds": self.retry_delay_seconds,
            "timeout_seconds": self.timeout_seconds,
            "post_delay_ms": self.post_delay_ms,
        }


@dataclass(slots=True, frozen=True)
class TaskChain:
    """Ordered sequence of tasks executed one after another."""

    steps: Sequence[ChainStep]
    on_failure: ChainFailurePolicy = "stop"
    max_chain_attempts: int = 1
    default_step_max_attempts: int = 1
    default_step_retry_delay_seconds: float = 0.0
    default_step_timeout_seconds: float | None = None
    wait_poll_interval_seconds: float = 0.5

    def __post_init__(self) -> None:
        """Validate chain configuration."""
        if not self.steps:
            raise ValueError("TaskChain must contain at least one step.")
        if self.on_failure not in ("stop", "restart"):
            raise ValueError("TaskChain.on_failure must be 'stop' or 'restart'.")
        if self.max_chain_attempts < 1:
            raise ValueError("TaskChain.max_chain_attempts must be >= 1.")
        if self.default_step_max_attempts < 1:
            raise ValueError("TaskChain.default_step_max_attempts must be >= 1.")
        if self.default_step_retry_delay_seconds < 0:
            raise ValueError("TaskChain.default_step_retry_delay_seconds must be >= 0.")
        if self.default_step_timeout_seconds is not None and self.default_step_timeout_seconds <= 0:
            raise ValueError("TaskChain.default_step_timeout_seconds must be > 0.")
        if self.wait_poll_interval_seconds <= 0:
            raise ValueError("TaskChain.wait_poll_interval_seconds must be > 0.")

    def to_orchestrator_kwargs(self, registry: TaskRegistry) -> dict[str, Any]:
        """Build the kwargs payload that will be handed to the orchestrator task."""
        return {
            "steps": [step.to_payload(registry) for step in self.steps],
            "on_failure": self.on_failure,
            "max_chain_attempts": self.max_chain_attempts,
            "default_step_max_attempts": self.default_step_max_attempts,
            "default_step_retry_delay_seconds": self.default_step_retry_delay_seconds,
            "default_step_timeout_seconds": self.default_step_timeout_seconds,
            "wait_poll_interval_seconds": self.wait_poll_interval_seconds,
        }


@dataclass(slots=True)
class ChainRunSummary:
    """Return value of a completed chain orchestration."""

    status: Literal["completed"]
    chain_attempts: int
    completed_steps: int
    step_attempts: list[int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "chain_attempts": self.chain_attempts,
            "completed_steps": self.completed_steps,
            "step_attempts": list(self.step_attempts),
        }


class ChainOrchestrator:
    """Execute a serialized task chain by dispatching each step through the broker."""

    def __init__(self, broker: TaskiqBroker) -> None:
        self.broker = broker

    async def execute(
            self,
            *,
            steps: list[dict[str, Any]],
            on_failure: ChainFailurePolicy,
            max_chain_attempts: int,
            default_step_max_attempts: int,
            default_step_retry_delay_seconds: float,
            default_step_timeout_seconds: float | None,
            wait_poll_interval_seconds: float,
    ) -> dict[str, Any]:
        """Run the chain and return a summary dict on success."""
        chain_attempts = 0
        last_error: ChainStepFailedError | None = None
        step_attempts: list[int] = []
        while chain_attempts < max_chain_attempts:
            chain_attempts += 1
            step_attempts = []
            try:
                for index, step in enumerate(steps):
                    attempts = await self.run_step(
                        index=index,
                        step=step,
                        default_max_attempts=default_step_max_attempts,
                        default_retry_delay_seconds=default_step_retry_delay_seconds,
                        default_timeout_seconds=default_step_timeout_seconds,
                        wait_poll_interval_seconds=wait_poll_interval_seconds,
                    )
                    step_attempts.append(attempts)
                    if index < len(steps) - 1:
                        await self.sleep_after_step(step)
                log.info(
                    "Task chain completed.",
                    extra={
                        "chain_attempts": chain_attempts,
                        "completed_steps": len(steps),
                        "step_attempts": step_attempts,
                    },
                )
                return ChainRunSummary(
                    status="completed",
                    chain_attempts=chain_attempts,
                    completed_steps=len(steps),
                    step_attempts=step_attempts,
                ).to_dict()
            except ChainStepFailedError as exc:
                last_error = exc
                log.warning(
                    "Task chain step failed.",
                    extra={
                        "chain_attempts": chain_attempts,
                        "step_index": exc.step_index,
                        "task_name": exc.task_name,
                        "reason": exc.reason,
                        "on_failure": on_failure,
                    },
                )
                if on_failure == "stop":
                    break

        assert last_error is not None
        raise ChainAbortedError(chain_attempts=chain_attempts, last_error=last_error)

    async def run_step(
            self,
            *,
            index: int,
            step: dict[str, Any],
            default_max_attempts: int,
            default_retry_delay_seconds: float,
            default_timeout_seconds: float | None,
            wait_poll_interval_seconds: float,
    ) -> int:
        """Dispatch a single step, respecting its retry and timeout policy."""
        task_name = str(step["task_name"])
        args = list(step.get("args") or [])
        kwargs = dict(step.get("kwargs") or {})
        max_attempts = int(step.get("max_attempts") or default_max_attempts)
        raw_retry_delay = step.get("retry_delay_seconds")
        retry_delay_seconds = (
            float(raw_retry_delay) if raw_retry_delay is not None else float(default_retry_delay_seconds)
        )
        timeout_value = step.get("timeout_seconds")
        timeout_seconds = (
            float(timeout_value) if timeout_value is not None else default_timeout_seconds
        )

        task = self.broker.find_task(task_name)
        if task is None:
            raise ChainStepFailedError(
                step_index=index,
                task_name=task_name,
                attempts=0,
                reason="task is not registered in the broker",
            )

        attempts = 0
        while True:
            attempts += 1
            try:
                enqueued = await task.kiq(*args, **kwargs)
                wait_timeout = -1.0 if timeout_seconds is None else float(timeout_seconds)
                result = await enqueued.wait_result(
                    check_interval=wait_poll_interval_seconds,
                    timeout=wait_timeout,
                )
            except Exception as exc:
                if attempts >= max_attempts:
                    raise ChainStepFailedError(
                        step_index=index,
                        task_name=task_name,
                        attempts=attempts,
                        reason=f"{type(exc).__name__}: {exc}",
                    ) from exc
                log.warning(
                    "Task chain step raised; retrying.",
                    extra={
                        "step_index": index,
                        "task_name": task_name,
                        "attempt": attempts,
                        "max_attempts": max_attempts,
                        "reason": str(exc),
                    },
                )
                if retry_delay_seconds > 0:
                    await asyncio.sleep(retry_delay_seconds)
                continue

            if not result.is_err:
                log.info(
                    "Task chain step completed.",
                    extra={
                        "step_index": index,
                        "task_name": task_name,
                        "attempts": attempts,
                    },
                )
                return attempts

            reason = str(result.error) if result.error is not None else "task returned error"
            if attempts >= max_attempts:
                raise ChainStepFailedError(
                    step_index=index,
                    task_name=task_name,
                    attempts=attempts,
                    reason=reason,
                )
            log.warning(
                "Task chain step returned error; retrying.",
                extra={
                    "step_index": index,
                    "task_name": task_name,
                    "attempt": attempts,
                    "max_attempts": max_attempts,
                    "reason": reason,
                },
            )
            if retry_delay_seconds > 0:
                await asyncio.sleep(retry_delay_seconds)

    async def sleep_after_step(self, step: dict[str, Any]) -> None:
        """Delay before the next chain step when the current step requests it."""
        post_delay_ms = step.get("post_delay_ms")
        if post_delay_ms is None:
            return
        delay_seconds = int(post_delay_ms) / 1000
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)


def register_chain_orchestrator(broker: TaskiqBroker) -> None:
    """Attach the chain orchestrator task to the given broker, once."""
    if broker.find_task(CHAIN_ORCHESTRATOR_TASK_NAME) is not None:
        return
    orchestrator = ChainOrchestrator(broker)

    async def chain_orchestrator_task(**payload: Any) -> dict[str, Any]:
        return await orchestrator.execute(**payload)

    register = getattr(broker, "register_task", None)
    if register is None:
        raise RuntimeError("Broker does not expose register_task; cannot register chain orchestrator.")
    register(chain_orchestrator_task, task_name=CHAIN_ORCHESTRATOR_TASK_NAME)
