from __future__ import annotations

from datetime import UTC, datetime

import pytest

from taskiq_beat.triggers import CrontabTrigger, ImmediateDispatch, IntervalTrigger, OneOffSchedule, PeriodicSchedule


def test_interval_trigger_get_next_run_at() -> None:
    interval = IntervalTrigger(seconds=5)

    result = interval.get_next_run_at(
        datetime(2026, 3, 18, 10, 0, 4, tzinfo=UTC),
        anchor=datetime(2026, 3, 18, 10, 0, 0, tzinfo=UTC),
    )

    assert result == datetime(2026, 3, 18, 10, 0, 5, tzinfo=UTC)


def test_crontab_trigger_weekday_get_next_run_at() -> None:
    crontab = CrontabTrigger.every(monday="09:30", timezone="UTC")

    result = crontab.get_next_run_at(datetime(2026, 3, 18, 10, 0, 0, tzinfo=UTC))

    assert result == datetime(2026, 3, 23, 9, 30, 0, tzinfo=UTC)


def test_crontab_every_seconds_get_next_run_at() -> None:
    crontab = CrontabTrigger.every(seconds=5, timezone="UTC")

    result = crontab.get_next_run_at(datetime(2026, 3, 18, 10, 0, 4, tzinfo=UTC))

    assert result == datetime(2026, 3, 18, 10, 0, 5, tzinfo=UTC)


def test_periodic_schedule_payload_roundtrip() -> None:
    trigger = PeriodicSchedule(interval=IntervalTrigger(minutes=15))

    restored = PeriodicSchedule.from_payload(trigger.to_payload())

    assert restored.strategy == "interval"
    assert restored.interval is not None
    assert restored.interval.minutes == 15


def test_periodic_schedule_start_at_used_as_first_run() -> None:
    start_at = datetime(2026, 3, 18, 10, 10, 0, tzinfo=UTC)
    trigger = PeriodicSchedule(
        interval=IntervalTrigger(minutes=5),
        start_at=start_at,
    )

    result = trigger.get_next_run_at(
        datetime(2026, 3, 18, 10, 0, 0, tzinfo=UTC),
        anchor=datetime(2026, 3, 18, 10, 0, 0, tzinfo=UTC),
    )

    assert result == start_at


def test_periodic_schedule_end_at_stops_next_run() -> None:
    trigger = PeriodicSchedule(
        interval=IntervalTrigger(minutes=5),
        end_at=datetime(2026, 3, 18, 10, 4, 0, tzinfo=UTC),
    )

    result = trigger.get_next_run_at(
        datetime(2026, 3, 18, 10, 0, 0, tzinfo=UTC),
        anchor=datetime(2026, 3, 18, 10, 0, 0, tzinfo=UTC),
    )

    assert result is None


def test_one_off_requires_timezone() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        OneOffSchedule(run_at=datetime.now())


def test_immediate_dispatch_payload_roundtrip() -> None:
    trigger = ImmediateDispatch()

    restored = ImmediateDispatch.from_payload(trigger.to_payload())

    assert restored == ImmediateDispatch()


def test_crontab_invalid_step_raises() -> None:
    with pytest.raises(ValueError, match="positive"):
        CrontabTrigger(second="*/0")


def test_crontab_uses_standard_day_of_month_or_day_of_week_semantics() -> None:
    crontab = CrontabTrigger(
        second="0",
        minute="0",
        hour="9",
        day_of_month="20",
        day_of_week="1",
        timezone="UTC",
    )

    result = crontab.get_next_run_at(datetime(2026, 3, 18, 10, 0, 0, tzinfo=UTC))

    assert result == datetime(2026, 3, 20, 9, 0, 0, tzinfo=UTC)


def test_crontab_supports_range_step_syntax() -> None:
    crontab = CrontabTrigger(second="0", minute="10-20/5", hour="*", timezone="UTC")

    result = crontab.get_next_run_at(datetime(2026, 3, 18, 10, 11, 0, tzinfo=UTC))

    assert result == datetime(2026, 3, 18, 10, 15, 0, tzinfo=UTC)
