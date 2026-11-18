from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from typing import Any, Self, get_type_hints
from uuid import uuid4

DEFAULT_TIMEZONE = "UTC"


@dataclass(slots=True, frozen=True)
class SchedulerConfig:
    """Runtime settings for polling, claiming, dispatch, and cleanup."""

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
    completed_job_retention_days: int | None = None
    job_cleanup_interval_seconds: float = 3600.0
    default_timezone: str = DEFAULT_TIMEZONE

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "TASKIQ_SCHEDULER_",
        environ: Mapping[str, str] | None = None,
        defaults: Self | None = None,
    ) -> Self:
        """Apply non-empty environment overrides to an existing or default config."""
        source = os.environ if environ is None else environ
        overrides: dict[str, Any] = {}
        types = get_type_hints(cls)
        for option in fields(cls):
            key = prefix + option.name.removeprefix("scheduler_").upper()
            value = source.get(key, "").strip()
            if not value:
                continue
            value_type = types[option.name]
            try:
                if value_type is bool:
                    overrides[option.name] = {
                        "true": True, "1": True, "yes": True, "on": True,
                        "false": False, "0": False, "no": False, "off": False,
                    }[value.lower()]
                elif value_type == (int | None):
                    overrides[option.name] = None if value.lower() == "none" else int(value)
                else:
                    overrides[option.name] = value_type(value)
            except (KeyError, ValueError) as exc:
                raise ValueError(f"Invalid value for {key}: {value!r}") from exc
        return replace(defaults if defaults is not None else cls(), **overrides)

    def __post_init__(self) -> None:
        """Validate numeric limits eagerly to fail fast on bad config."""
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
        if self.completed_job_retention_days is not None and self.completed_job_retention_days < 0:
            raise ValueError("completed_job_retention_days must be greater than or equal to zero.")
        if self.job_cleanup_interval_seconds <= 0:
            raise ValueError("job_cleanup_interval_seconds must be positive.")
