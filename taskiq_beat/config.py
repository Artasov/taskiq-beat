from __future__ import annotations

from dataclasses import dataclass

DEFAULT_TIMEZONE = "UTC"


@dataclass(slots=True, frozen=True)
class SchedulerConfig:
    sync_interval_seconds: float = 1.0
    idle_sleep_seconds: float = 0.2
    dispatch_retry_seconds: int = 5
    default_timezone: str = DEFAULT_TIMEZONE
