from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

DEFAULT_TIMEZONE = "UTC"


@dataclass(slots=True, frozen=True)
class SchedulerConfig:
    scheduler_id: str = field(default_factory=lambda: uuid4().hex)
    sync_interval_seconds: float = 1.0
    idle_sleep_seconds: float = 0.2
    claim_ttl_seconds: int = 60
    dispatch_retry_seconds: int = 5
    dispatch_concurrency: int = 32
    dispatch_batch_size: int = 256
    record_runs: bool = True
    run_history_retention_days: int | None = None
    run_cleanup_interval_seconds: float = 3600.0
    default_timezone: str = DEFAULT_TIMEZONE

    def __post_init__(self) -> None:
        if not self.scheduler_id.strip():
            raise ValueError("scheduler_id must not be empty.")
        if self.sync_interval_seconds <= 0:
            raise ValueError("sync_interval_seconds must be positive.")
        if self.idle_sleep_seconds <= 0:
            raise ValueError("idle_sleep_seconds must be positive.")
        if self.claim_ttl_seconds <= 0:
            raise ValueError("claim_ttl_seconds must be positive.")
        if self.dispatch_retry_seconds <= 0:
            raise ValueError("dispatch_retry_seconds must be positive.")
        if self.dispatch_concurrency <= 0:
            raise ValueError("dispatch_concurrency must be positive.")
        if self.dispatch_batch_size <= 0:
            raise ValueError("dispatch_batch_size must be positive.")
        if self.run_history_retention_days is not None and self.run_history_retention_days < 0:
            raise ValueError("run_history_retention_days must be greater than or equal to zero.")
        if self.run_cleanup_interval_seconds <= 0:
            raise ValueError("run_cleanup_interval_seconds must be positive.")
