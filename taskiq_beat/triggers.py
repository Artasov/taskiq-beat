from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from taskiq_beat.config import DEFAULT_TIMEZONE


class TimezoneResolver:
    @staticmethod
    def get(value: str):
        if value.upper() == "UTC":
            return UTC
        return ZoneInfo(value)


class CronFieldSet:
    @staticmethod
    def parse(expression: str, minimum: int, maximum: int) -> set[int]:
        value = expression.strip()
        if not value:
            raise ValueError("Cron expression is empty.")
        result: set[int] = set()
        for chunk in value.split(","):
            part = chunk.strip()
            if part == "*":
                result.update(range(minimum, maximum + 1))
                continue
            if part.startswith("*/"):
                step = int(part[2:])
                if step <= 0:
                    raise ValueError("Cron step must be positive.")
                result.update(range(minimum, maximum + 1, step))
                continue
            if "-" in part:
                start_text, end_text = part.split("-", 1)
                start = int(start_text)
                end = int(end_text)
                if start > end:
                    raise ValueError("Cron range start must be <= end.")
                result.update(range(start, end + 1))
                continue
            result.add(int(part))
        if not result:
            raise ValueError("Cron expression produced an empty set.")
        if min(result) < minimum or max(result) > maximum:
            raise ValueError(f"Cron values must be between {minimum} and {maximum}.")
        return result


@dataclass(slots=True, frozen=True)
class IntervalTrigger:
    seconds: int | None = None
    minutes: int | None = None
    hours: int | None = None
    days: int | None = None

    def __post_init__(self) -> None:
        values = [value for value in (self.seconds, self.minutes, self.hours, self.days) if value is not None]
        if len(values) != 1:
            raise ValueError("Interval must contain exactly one unit.")
        if any(value <= 0 for value in values):
            raise ValueError("Interval values must be positive.")

    @property
    def total_seconds(self) -> int:
        if self.seconds is not None:
            return self.seconds
        if self.minutes is not None:
            return self.minutes * 60
        if self.hours is not None:
            return self.hours * 3600
        return (self.days or 0) * 86400

    def to_payload(self) -> dict[str, int | None]:
        return {
            "seconds": self.seconds,
            "minutes": self.minutes,
            "hours": self.hours,
            "days": self.days,
        }

    @classmethod
    def from_payload(cls, payload: dict) -> "IntervalTrigger":
        return cls(
            seconds=payload.get("seconds"),
            minutes=payload.get("minutes"),
            hours=payload.get("hours"),
            days=payload.get("days"),
        )

    def get_next_run_at(self, after: datetime, *, anchor: datetime) -> datetime:
        base = anchor.astimezone(UTC).replace(microsecond=0)
        current = after.astimezone(UTC).replace(microsecond=0)
        if current < base:
            return base
        delta_seconds = int((current - base).total_seconds())
        step = (delta_seconds // self.total_seconds) + 1
        return base + timedelta(seconds=step * self.total_seconds)


@dataclass(slots=True, frozen=True)
class CrontabTrigger:
    second: str = "0"
    minute: str = "*"
    hour: str = "*"
    day_of_month: str = "*"
    month_of_year: str = "*"
    day_of_week: str = "*"
    timezone: str = DEFAULT_TIMEZONE

    def __post_init__(self) -> None:
        TimezoneResolver.get(self.timezone)
        CronFieldSet.parse(self.second, 0, 59)
        CronFieldSet.parse(self.minute, 0, 59)
        CronFieldSet.parse(self.hour, 0, 23)
        CronFieldSet.parse(self.day_of_month, 1, 31)
        CronFieldSet.parse(self.month_of_year, 1, 12)
        CronFieldSet.parse(self.day_of_week, 0, 6)

    @classmethod
    def every(
        cls,
        *,
        seconds: int | None = None,
        minutes: int | None = None,
        hours: int | None = None,
        days: int | None = None,
        monday: str | None = None,
        tuesday: str | None = None,
        wednesday: str | None = None,
        thursday: str | None = None,
        friday: str | None = None,
        saturday: str | None = None,
        sunday: str | None = None,
        timezone: str = DEFAULT_TIMEZONE,
    ) -> "CrontabTrigger":
        if seconds:
            return cls(second=f"*/{seconds}", timezone=timezone)
        if minutes:
            return cls(second="0", minute=f"*/{minutes}", timezone=timezone)
        if hours:
            return cls(second="0", minute="0", hour=f"*/{hours}", timezone=timezone)
        if days:
            return cls(second="0", minute="0", hour="0", day_of_month=f"*/{days}", timezone=timezone)
        weekday_map = {
            0: monday,
            1: tuesday,
            2: wednesday,
            3: thursday,
            4: friday,
            5: saturday,
            6: sunday,
        }
        for day_index, time_value in weekday_map.items():
            if not time_value:
                continue
            hour, minute = cls.parse_time(time_value)
            return cls(second="0", minute=str(minute), hour=str(hour), day_of_week=str(day_index), timezone=timezone)
        raise ValueError("Invalid parameters for crontab generation.")

    @staticmethod
    def parse_time(value: str) -> tuple[int, int]:
        match = re.fullmatch(r"(\d{1,2}):(\d{2})", value.strip())
        if not match:
            raise ValueError(f"Invalid time format: {value}")
        hour, minute = (int(item) for item in match.groups())
        if hour not in range(24) or minute not in range(60):
            raise ValueError(f"Invalid time format: {value}")
        return hour, minute

    def to_payload(self) -> dict[str, str]:
        return {
            "second": self.second,
            "minute": self.minute,
            "hour": self.hour,
            "day_of_month": self.day_of_month,
            "month_of_year": self.month_of_year,
            "day_of_week": self.day_of_week,
            "timezone": self.timezone,
        }

    @classmethod
    def from_payload(cls, payload: dict) -> "CrontabTrigger":
        return cls(
            second=str(payload.get("second", "0")),
            minute=str(payload.get("minute", "*")),
            hour=str(payload.get("hour", "*")),
            day_of_month=str(payload.get("day_of_month", "*")),
            month_of_year=str(payload.get("month_of_year", "*")),
            day_of_week=str(payload.get("day_of_week", "*")),
            timezone=str(payload.get("timezone", DEFAULT_TIMEZONE)),
        )

    def get_next_run_at(self, after: datetime) -> datetime:
        timezone = TimezoneResolver.get(self.timezone)
        current = after.astimezone(timezone).replace(microsecond=0) + timedelta(seconds=1)
        seconds = sorted(CronFieldSet.parse(self.second, 0, 59))
        minutes = sorted(CronFieldSet.parse(self.minute, 0, 59))
        hours = sorted(CronFieldSet.parse(self.hour, 0, 23))
        days = CronFieldSet.parse(self.day_of_month, 1, 31)
        months = CronFieldSet.parse(self.month_of_year, 1, 12)
        weekdays = CronFieldSet.parse(self.day_of_week, 0, 6)
        limit = current + timedelta(days=366 * 5)

        while current <= limit:
            if current.month not in months:
                current = self.move_month(current, months)
                continue
            if current.day not in days or current.weekday() not in weekdays:
                current = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0)
                continue
            next_hour = self.next_value(hours, current.hour)
            if next_hour != current.hour:
                if next_hour < current.hour:
                    current = (current + timedelta(days=1)).replace(hour=next_hour, minute=0, second=0)
                else:
                    current = current.replace(hour=next_hour, minute=0, second=0)
                continue
            next_minute = self.next_value(minutes, current.minute)
            if next_minute != current.minute:
                if next_minute < current.minute:
                    current = (current + timedelta(hours=1)).replace(minute=next_minute, second=0)
                else:
                    current = current.replace(minute=next_minute, second=0)
                continue
            next_second = self.next_value(seconds, current.second)
            if next_second != current.second:
                if next_second < current.second:
                    current = (current + timedelta(minutes=1)).replace(second=next_second)
                else:
                    current = current.replace(second=next_second)
                continue
            return current.astimezone(UTC)
        raise ValueError("Unable to compute next run for crontab.")

    @staticmethod
    def next_value(values: list[int], current: int) -> int:
        for value in values:
            if value >= current:
                return value
        return values[0]

    @staticmethod
    def move_month(current: datetime, allowed_months: set[int]) -> datetime:
        target_month = min((item for item in allowed_months if item > current.month), default=min(allowed_months))
        year = current.year + 1 if target_month <= current.month else current.year
        return current.replace(year=year, month=target_month, day=1, hour=0, minute=0, second=0)


@dataclass(slots=True, frozen=True)
class PeriodicSchedule:
    interval: IntervalTrigger | None = None
    crontab: CrontabTrigger | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None

    def __post_init__(self) -> None:
        if (self.interval is None) == (self.crontab is None):
            raise ValueError("Periodic schedule must contain either interval or crontab.")
        if self.start_at is not None and self.start_at.tzinfo is None:
            raise ValueError("Periodic schedule start_at must be timezone-aware.")
        if self.end_at is not None and self.end_at.tzinfo is None:
            raise ValueError("Periodic schedule end_at must be timezone-aware.")
        if self.start_at is not None and self.end_at is not None and self.start_at >= self.end_at:
            raise ValueError("Periodic schedule end_at must be greater than start_at.")

    @property
    def strategy(self) -> str:
        return "interval" if self.interval is not None else "crontab"

    def to_payload(self) -> dict:
        return {
            "strategy": self.strategy,
            "interval": self.interval.to_payload() if self.interval is not None else None,
            "crontab": self.crontab.to_payload() if self.crontab is not None else None,
            "start_at": self.start_at.astimezone(UTC).isoformat() if self.start_at is not None else None,
            "end_at": self.end_at.astimezone(UTC).isoformat() if self.end_at is not None else None,
        }

    @classmethod
    def from_payload(cls, payload: dict) -> "PeriodicSchedule":
        start_at = payload.get("start_at")
        end_at = payload.get("end_at")
        return cls(
            interval=IntervalTrigger.from_payload(payload["interval"]) if payload.get("interval") else None,
            crontab=CrontabTrigger.from_payload(payload["crontab"]) if payload.get("crontab") else None,
            start_at=datetime.fromisoformat(start_at) if start_at else None,
            end_at=datetime.fromisoformat(end_at) if end_at else None,
        )

    def get_next_run_at(self, after: datetime, *, anchor: datetime) -> datetime | None:
        current = after.astimezone(UTC)
        if self.end_at is not None and current >= self.end_at.astimezone(UTC):
            return None
        if self.interval is not None:
            interval_anchor = self.start_at.astimezone(UTC) if self.start_at is not None else anchor.astimezone(UTC)
            next_run = self.interval.get_next_run_at(current, anchor=interval_anchor)
        else:
            baseline = self.start_at.astimezone(UTC) - timedelta(seconds=1) if self.start_at and current < self.start_at else current
            next_run = self.crontab.get_next_run_at(baseline)
        if self.end_at is not None and next_run > self.end_at.astimezone(UTC):
            return None
        return next_run


@dataclass(slots=True, frozen=True)
class OneOffSchedule:
    run_at: datetime

    def __post_init__(self) -> None:
        if self.run_at.tzinfo is None:
            raise ValueError("One-off schedule run_at must be timezone-aware.")

    def to_payload(self) -> dict[str, str]:
        return {"run_at": self.run_at.astimezone(UTC).isoformat()}

    @classmethod
    def from_payload(cls, payload: dict) -> "OneOffSchedule":
        return cls(run_at=datetime.fromisoformat(str(payload["run_at"])))
