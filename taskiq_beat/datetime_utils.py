from __future__ import annotations

from datetime import UTC, datetime, tzinfo


def normalize_utc(value: datetime) -> datetime:
    """Return the value as a timezone-aware UTC datetime."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def normalize_timezone(value: datetime, timezone: tzinfo) -> datetime:
    """Normalize a datetime to UTC first and then convert to the target zone."""
    return normalize_utc(value).astimezone(timezone)
