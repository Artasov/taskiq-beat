# taskiq-beat

<div align="center">
  <p><strong>Database-backed scheduler for Taskiq with schedule and run history stored in your database</strong></p>
  <p>One active scheduler process per database</p>
</div>

<div align="center">
  <a href="./README.md">
    <img src="https://img.shields.io/badge/English-blue?style=for-the-badge" alt="English">
  </a>
  <a href="./README.ru.md">
    <img src="https://img.shields.io/badge/Русский-red?style=for-the-badge" alt="Русский">
  </a>
</div>

<div align="center">
  <a href="./scripts/README.md">Scripts README</a>
</div>

## Navigation

- [Quick start](#quick-start)
- [How to run it](#how-to-run-it)
- [Run with FastAPI](#run-with-fastapi)
- [Create jobs](#create-jobs)
- [Manage jobs](#manage-jobs)
- [Alembic](#alembic)
- [Default configuration](#default-configuration)
- [Create tables manually](#create-tables-manually)
- [Load testing](#load-testing)

## Installation

```bash
pip install taskiq-beat
```

> `pip install "taskiq-beat[test]"`</br>
> `pip install "taskiq-beat[dev]"`

`taskiq-beat` does not provide a network broker by itself. For a real multi-process setup you usually also install a broker backend from the Taskiq ecosystem.

## Quick start

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat import (
    IntervalTrigger,
    PeriodicSchedule,
    SchedulerApp,
    SchedulerConfig,
)

# SQLAlchemy engine connected to your database.
engine = create_async_engine("sqlite+aiosqlite:///scheduler.sqlite3")

# Factory that creates AsyncSession objects.
# Scheduler uses these sessions to work with the database.
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# Taskiq broker used only to demonstrate the API.
# InMemoryBroker is not suitable for a separate worker process setup.
broker = InMemoryBroker()


@broker.task(task_name="demo.heartbeat")
async def heartbeat_task() -> None:
    print("tick")


# Main taskiq-beat entry point.
scheduler_app = SchedulerApp(
    broker=broker,
    session_factory=session_factory,
    config=SchedulerConfig(
        # How often the scheduler reloads active jobs from the database.
        sync_interval_seconds=1.0,
        # Minimum sleep between loop iterations.
        idle_sleep_seconds=0.2,
        # Delay before retrying dispatch after task.kiq(...) fails.
        dispatch_retry_seconds=5,
        # Maximum number of jobs dispatched concurrently inside one batch.
        dispatch_concurrency=32,
        # Maximum number of due jobs taken from the heap in one batch.
        dispatch_batch_size=256,
        # Whether to write SchedulerRun history rows.
        record_runs=True,
        # Default timezone used by helper APIs when no timezone is specified.
        default_timezone="UTC",
    ),
)
```

This snippet only shows how to assemble `broker`, `session_factory`, and `scheduler_app`.
It does not mean jobs are already being executed in a separate process.

## How to run it

The code above only creates Python objects. That alone does not make the system start working.

In a real application you need:

1. A process that runs `scheduler_app.start()`.
2. A Taskiq worker process that consumes tasks from the broker.
3. Code that creates jobs through `scheduler_app.create_scheduler(...).schedule(session)`.

Important:

- `InMemoryBroker` is fine for API demos, tests, and local experiments inside one process.
- If you want a worker in another terminal or service, you need a real broker backend.
- The worker is usually started in a separate terminal or a separate service/process.
- The command `python -m taskiq worker app.main:broker` means: import the `broker` object from the `app.main` module and start consuming its queue.

Typical setup:

- terminal 1: FastAPI app with `scheduler_app.start()` inside lifespan
- terminal 2: Taskiq worker
- terminal 3: API calls or a separate script that creates jobs

The most common FastAPI setup looks like this:

1. `app/main.py` contains `broker`, `scheduler_app`, `app = FastAPI(...)`, and your task functions.
2. Start FastAPI in the first terminal:

```bash
uvicorn app.main:app --reload
```

3. Start the worker in the second terminal:

```bash
python -m taskiq worker app.main:broker
```

4. Then create jobs through your API, a script, or a Python shell.

What happens after everything is running:

- the scheduler watches the schedule and calls `task.kiq(...)` when a job is due
- the broker publishes the task
- the worker receives the task and executes it

If the worker is not running, the scheduler can still publish tasks to the broker, but nothing will execute them.

If you prefer, the scheduler can also run outside FastAPI as a separate process.
In that setup one process runs `scheduler_app.start()`, and another process still runs the Taskiq worker.

## Run with FastAPI

Scheduler can run:

- inside the FastAPI process
- or as a separate process/service

Both are valid. The main rule is: do not run several scheduler processes against the same database.

In most applications the simplest setup is:

- FastAPI starts `scheduler_app`
- a separate Taskiq worker process executes tasks

That is a normal and recommended setup for most projects.

FastAPI example:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat import SchedulerApp, SchedulerConfig

engine = create_async_engine("sqlite+aiosqlite:///scheduler.sqlite3")
session_factory = async_sessionmaker(engine, expire_on_commit=False)
broker = InMemoryBroker()
scheduler_app = SchedulerApp(
    broker=broker,
    session_factory=session_factory,
    config=SchedulerConfig(
        # How often the scheduler reloads active jobs from the database.
        sync_interval_seconds=1.0,
        # Minimum sleep between loop iterations.
        idle_sleep_seconds=0.2,
        # Delay before retrying dispatch after task.kiq(...) fails.
        dispatch_retry_seconds=5,
        # Maximum number of jobs dispatched concurrently inside one batch.
        dispatch_concurrency=32,
        # Maximum number of due jobs taken from the heap in one batch.
        dispatch_batch_size=256,
        # Whether to write SchedulerRun history rows.
        record_runs=True,
        # Default timezone used by helper APIs when no timezone is specified.
        default_timezone="UTC",
    ),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.startup()
    await scheduler_app.start()
    try:
        yield
    finally:
        await scheduler_app.stop()
        await broker.shutdown()


app = FastAPI(lifespan=lifespan)
```

## Create jobs

### Interval job

```python
from taskiq_beat import IntervalTrigger, PeriodicSchedule

async with session_factory() as session:
    scheduler = scheduler_app.create_scheduler(
        task=heartbeat_task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=5)),
        name="Heartbeat every 5 seconds",
    )
    job = await scheduler.schedule(session)

print(job.id)  # Example: "6c6342d8-6d74-4d16-8f7a-5d4f1b3a0b13"
```

`await scheduler.schedule(session)` returns `SchedulerJob`.
It is a SQLAlchemy model instance with fields such as:

- `job.id`
- `job.task_name`
- `job.kind`
- `job.strategy`
- `job.next_run_at`
- `job.is_enabled`

In practice, `job.id` is the field you use later for pause, resume, run-now, and delete.

### One-off job

```python
from datetime import UTC, datetime, timedelta

from taskiq_beat import OneOffSchedule

async with session_factory() as session:
    scheduler = scheduler_app.create_scheduler(
        task=heartbeat_task,
        trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(minutes=10)),
        name="Delayed heartbeat",
    )
    job = await scheduler.schedule(session)
```

### Crontab job

```python
from taskiq_beat import CrontabTrigger, PeriodicSchedule

async with session_factory() as session:
    scheduler = scheduler_app.create_scheduler(
        task=heartbeat_task,
        trigger=PeriodicSchedule(
            crontab=CrontabTrigger(second="0", minute="*/5", hour="*"),
        ),
        name="Every 5 minutes",
    )
    job = await scheduler.schedule(session)
```

## Manage jobs

What you need:

- `session_factory()` to open an `AsyncSession`
- `job.id`, usually taken from the result of `await scheduler.schedule(session)`

```python
async with session_factory() as session:
    await scheduler_app.pause(session, job.id)
    await scheduler_app.resume(session, job.id)
    await scheduler_app.run_now(session, job.id)
    await scheduler_app.delete(session, job.id)
```

## Alembic

To make Alembic detect scheduler tables, add `SchedulerBase.metadata` to `target_metadata`.

`alembic/env.py`:

```python
from myapp.db import Base
from taskiq_beat import SchedulerBase

target_metadata = [
    # Your main ORM metadata.
    Base.metadata,
    # Metadata from taskiq-beat.
    SchedulerBase.metadata,
]
```

Commands you can copy and run:

```bash
pip install alembic
alembic init alembic
alembic revision --autogenerate -m "add taskiq beat tables"
alembic upgrade head
```

Alembic should detect:

- `scheduler_job`
- `scheduler_run`

## Default configuration

```python
SchedulerConfig(
    # How often the scheduler reloads active jobs from the database.
    sync_interval_seconds=1.0,

    # Minimum sleep between loop iterations.
    idle_sleep_seconds=0.2,

    # Delay before retrying dispatch after task.kiq(...) fails.
    dispatch_retry_seconds=5,

    # Maximum number of jobs dispatched concurrently inside one batch.
    dispatch_concurrency=32,

    # Maximum number of due jobs taken from the heap in one batch.
    dispatch_batch_size=256,

    # Whether to write SchedulerRun history rows.
    record_runs=True,

    # Default timezone used by helper APIs when no timezone is specified.
    default_timezone="UTC",
)
```

## Create tables manually

Use this only for local setup, tests, and quick experiments.

```python
from taskiq_beat import SchedulerBase

async with engine.begin() as connection:
    await connection.run_sync(SchedulerBase.metadata.create_all)
```

This creates:

- `scheduler_job`
- `scheduler_run`

## Load testing

See [scripts/README.md](./scripts/README.md).

## Testing

```bash
pytest
```
