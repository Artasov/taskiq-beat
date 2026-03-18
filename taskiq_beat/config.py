from __future__ import annotations

from dataclasses import dataclass

DEFAULT_TIMEZONE = "UTC"


@dataclass(slots=True, frozen=True)
class SchedulerConfig:
    sync_interval_seconds: float = 1.0
    idle_sleep_seconds: float = 0.2
    dispatch_retry_seconds: int = 5
    dispatch_concurrency: int = 32
    dispatch_batch_size: int = 256
    record_runs: bool = True
    default_timezone: str = DEFAULT_TIMEZONE

    def __post_init__(self) -> None:
        if self.sync_interval_seconds <= 0:
            raise ValueError("sync_interval_seconds must be positive.")
        if self.idle_sleep_seconds <= 0:
            raise ValueError("idle_sleep_seconds must be positive.")
        if self.dispatch_retry_seconds <= 0:
            raise ValueError("dispatch_retry_seconds must be positive.")
        if self.dispatch_concurrency <= 0:
            raise ValueError("dispatch_concurrency must be positive.")
        if self.dispatch_batch_size <= 0:
            raise ValueError("dispatch_batch_size must be positive.")
