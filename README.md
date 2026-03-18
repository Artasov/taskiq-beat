# taskiq-beat

`taskiq-beat` is a database-backed scheduler for `Taskiq`.

It targets a simple deployment model:

- one active scheduler instance per schedule store
- persistent schedules in SQLAlchemy models
- one-off, interval, and crontab triggers
- in-memory heap-based dispatch loop
- run history stored in the database

## Installation

```bash
pip install taskiq-beat
```

With test dependencies:

```bash
pip install "taskiq-beat[test]"
```

For local development:

```bash
pip install "taskiq-beat[dev]"
```

## Architecture

This library is intentionally single-instance.

- the database is the source of truth for schedules and run history
- one scheduler process loads active jobs into memory and dispatches due tasks
- schedule changes made through `SchedulerApp` update the in-memory engine immediately
- the engine also periodically resyncs from the database

If you need high-availability leader election or several scheduler instances
running against the same schedule store, that is outside the scope of this
library.

## Quick start

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat import (
    CrontabTrigger,
    IntervalTrigger,
    OneOffSchedule,
    PeriodicSchedule,
    SchedulerApp,
    SchedulerConfig,
    SchedulerBase,
)

engine = create_async_engine("sqlite+aiosqlite:///scheduler.sqlite3")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
broker = InMemoryBroker()


@broker.task(task_name="demo.heartbeat")
async def heartbeat_task() -> None:
    print("tick")


scheduler_app = SchedulerApp(
    broker=broker,
    session_factory=session_factory,
    config=SchedulerConfig(),
)
```

Create tables:

```python
async with engine.begin() as connection:
    await connection.run_sync(SchedulerBase.metadata.create_all)
```

Start scheduler:

```python
await broker.startup()
await scheduler_app.start()
```

Register an interval job:

```python
async with session_factory() as session:
    scheduler = scheduler_app.create_scheduler(
        task=heartbeat_task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=5)),
        name="Heartbeat every 5 seconds",
    )
    await scheduler.schedule(session)
```

Register a one-off job:

```python
from datetime import UTC, datetime, timedelta

async with session_factory() as session:
    scheduler = scheduler_app.create_scheduler(
        task=heartbeat_task,
        trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(minutes=10)),
        name="Delayed heartbeat",
    )
    await scheduler.schedule(session)
```

Register a crontab job:

```python
async with session_factory() as session:
    scheduler = scheduler_app.create_scheduler(
        task=heartbeat_task,
        trigger=PeriodicSchedule(
            crontab=CrontabTrigger(second="0", minute="*/5", hour="*"),
        ),
        name="Every 5 minutes",
    )
    await scheduler.schedule(session)
```

Stop scheduler:

```python
await scheduler_app.stop()
await broker.shutdown()
```

## Public API

- `SchedulerConfig`
- `SchedulerApp`
- `Scheduler`
- `IntervalTrigger`
- `CrontabTrigger`
- `PeriodicSchedule`
- `OneOffSchedule`
- `SchedulerBase`
- `SchedulerJob`
- `SchedulerRun`
- `__version__`

## Main pieces

- `taskiq_beat/config.py`
  runtime configuration
- `taskiq_beat/triggers.py`
  typed trigger and schedule objects
- `taskiq_beat/models.py`
  SQLAlchemy models
- `taskiq_beat/repositories.py`
  database operations
- `taskiq_beat/registry.py`
  Taskiq task resolution and payload validation
- `taskiq_beat/engine.py`
  heap-based scheduler loop
- `taskiq_beat/app.py`
  public integration entrypoint

## Default configuration

```python
SchedulerConfig(
    sync_interval_seconds=1.0,
    idle_sleep_seconds=0.2,
    dispatch_retry_seconds=5,
    default_timezone="UTC",
)
```

## Database notes

The library provides SQLAlchemy models, but it does not generate migrations for
you. Use your own migration workflow and include:

- `SchedulerJob`
- `SchedulerRun`

## Testing

Run tests:

```bash
pytest
```

## Release

See [`release_guide`](./release_guide).
