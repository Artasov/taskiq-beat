<div align="center">
  <h1>taskiq-beat</h1>
</div>

<div align="center">
  <a href="./README.md">
    <img src="https://img.shields.io/badge/English-blue?style=for-the-badge" alt="English">
  </a>
  <a href="./README.ru.md">
    <img src="https://img.shields.io/badge/Русский-red?style=for-the-badge" alt="Русский">
  </a>
</div>  

### Scheduler for Taskiq with schedules and run history stored in the database

## Navigation

- [Quick start](#quick-start)
- [How to run it](#how-to-run-it)
- [Run with FastAPI](#run-with-fastapi)
- [Create jobs](#create-jobs)
- [Task chains](#task-chains)
- [Upsert and startup sync](#upsert-and-startup-sync)
- [Manage jobs](#manage-jobs)
- [Logging](#logging)
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

`taskiq-beat` does not provide a network broker on its own. For real multi-process usage you usually also need a broker
backend from the Taskiq ecosystem.

## Quick start

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat import (
    ImmediateDispatch,
    IntervalTrigger,
    PeriodicSchedule,
    SchedulerApp,
    SchedulerConfig,
)

# SQLAlchemy engine used to connect to the database.
engine = create_async_engine("sqlite+aiosqlite:///scheduler.sqlite3")

# Factory that creates AsyncSession objects.
# Scheduler uses these sessions to work with the database.
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# Taskiq broker for API demonstration.
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
        # Minimum pause between scheduler loop iterations.
        idle_sleep_seconds=0.2,
        # How many seconds to wait before retrying dispatch after task.kiq(...) fails.
        dispatch_retry_seconds=5,
        # How many jobs can be dispatched concurrently inside one batch.
        dispatch_concurrency=32,
        # Maximum number of due jobs taken from the heap in one batch.
        dispatch_batch_size=256,
        # Whether to write history rows into SchedulerRun.
        record_runs=True,
        # Base timezone for helper APIs if another timezone is not specified explicitly.
        default_timezone="UTC",
    ),
)
```

This example only shows how to assemble `broker`, `session_factory`, and `scheduler_app`.
It does not mean tasks will already start executing in a separate process.

## How to run it

The example above only creates Python objects. That is still not enough for the system to start working by itself.

What a real application needs:

1. Start a process with `scheduler_app.start()`.
2. Start a Taskiq worker process that will consume tasks from the broker.
3. Create jobs through `scheduler_app.single(task=...).schedule(session)` or `scheduler_app.chain(steps=[...]).schedule(session)`.

Important:

- `InMemoryBroker` in the examples is only suitable for API demos, tests, and local experiments inside one process.
- If you want a separate worker in another terminal or service, you need a real broker backend.
- A worker is usually started in a separate terminal or a separate service/process.
- The command `python -m taskiq worker app.main:broker` means: import the `broker` object from the `app.main` module and
  listen to its queue.

Typical scenario:

- terminal 1: FastAPI with `scheduler_app.start()` inside lifespan
- terminal 2: Taskiq worker
- terminal 3: API requests or a separate script that creates jobs

The most common FastAPI setup looks like this:

1. `app/main.py` contains `broker`, `scheduler_app`, `app = FastAPI(...)`, and task functions.
2. In the first terminal, start FastAPI:

```bash
uvicorn app.main:app --reload
```

3. In the second terminal, start the worker:

```bash
python -m taskiq worker app.main:broker
```

4. After that, create jobs through an API, script, or Python shell.

What happens after startup:

- the scheduler watches the schedule and calls `task.kiq(...)` at the required moment
- the broker publishes the task
- the worker takes the task and executes it

If the worker is not running, the scheduler can publish tasks into the broker, but nothing will execute them.

If you want to run the scheduler separately instead of inside FastAPI, that is also possible.
Then one process keeps `scheduler_app.start()`, and another process still remains the Taskiq worker.

## Run with FastAPI

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
    config=SchedulerConfig(),  # Configuration was shown above
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

### Scheduler can run:

- #### Inside FastAPI
  As in the example above

- #### Separately
    - terminal/container 1:
      `uvicorn app.main:app --reload`
    - terminal/container 2:
      `python -m app.run_scheduler`
    - terminal/container 3:
      `python -m taskiq worker app.broker:broker`

Both approaches are valid. The main rule is: do not run several scheduler processes against the same database.

## Create jobs

The public API has two builders on `SchedulerApp`:

- `scheduler_app.single(task=...)` — schedule a single task.
- `scheduler_app.chain(steps=[...])` — schedule an ordered chain of tasks.

Both builders expose `.schedule(session, trigger=...)` and
`.upsert(session, trigger=..., job_id=...)`.
If `trigger` is omitted in `.schedule(...)`, taskiq-beat uses `ImmediateDispatch()`: the task is sent to the broker
right away and the job/run records are stored in the database.

```python
from datetime import UTC, datetime, timedelta

from taskiq_beat import ImmediateDispatch, IntervalTrigger, OneOffSchedule, PeriodicSchedule

# Default: immediate task.kiq(...) + database record.
await scheduler_app.single(task=heartbeat_task).schedule(session)

# Equivalent explicit form.
await scheduler_app.single(task=heartbeat_task).schedule(
    session,
    trigger=ImmediateDispatch(),
)

# Explicit delayed or periodic scheduling.
await scheduler_app.single(task=heartbeat_task).schedule(
    session,
    trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(minutes=10)),
)
await scheduler_app.single(task=heartbeat_task).schedule(
    session,
    trigger=PeriodicSchedule(interval=IntervalTrigger(minutes=5)),
)
```

`ImmediateDispatch` is only valid with `.schedule(...)`. Use a real schedule trigger with `.upsert(...)`.

### Interval job

```python
from taskiq_beat import IntervalTrigger, PeriodicSchedule

async with session_factory() as session:
    job = await scheduler_app.single(task=heartbeat_task).schedule(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=5)),
        name="Heartbeat every 5 seconds",
    )

print(job.id)  # Example: "6c6342d8-6d74-4d16-8f7a-5d4f1b3a0b13"
```

`.schedule(session, ...)` returns `SchedulerJob`.
This is a SQLAlchemy model instance with fields such as:

- `job.id`
- `job.task_name`
- `job.kind`
- `job.strategy`
- `job.next_run_at`
- `job.is_enabled`

Most of the time you use `job.id` later for pause, resume, run-now, and delete.

If you want a stable identifier for a system job, use `.upsert(...)` with an explicit `job_id`:

```python
async with session_factory() as session:
    job = await scheduler_app.single(task=heartbeat_task).upsert(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(minutes=5)),
        job_id="system.heartbeat",
        name="System heartbeat",
    )
```

### One-off job

```python
from datetime import UTC, datetime, timedelta

from taskiq_beat import OneOffSchedule

async with session_factory() as session:
    job = await scheduler_app.single(task=heartbeat_task).schedule(
        session,
        trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(minutes=10)),
        name="Delayed heartbeat",
    )
```

### Crontab job

```python
from taskiq_beat import CrontabTrigger, PeriodicSchedule

async with session_factory() as session:
    job = await scheduler_app.single(task=heartbeat_task).schedule(
        session,
        trigger=PeriodicSchedule(
            crontab=CrontabTrigger(second="0", minute="*/5", hour="*"),
        ),
        name="Every 5 minutes",
    )
```

### Passing args and kwargs to the task

```python
async with session_factory() as session:
    await scheduler_app.single(
        task=heartbeat_task,
        args=[42],
        kwargs={"label": "ping"},
        metadata={"scope": "system"},
    ).schedule(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(minutes=1)),
    )
```

## Task chains

Taskiq does not enforce ordered execution of multiple tasks out of the box.
`taskiq-beat` adds a small built-in orchestrator that runs tasks one after another
and persists the chain as a regular scheduler job.

The orchestrator task is registered automatically inside every `SchedulerApp`
under the name `taskiq_beat.chain_orchestrator`. When the schedule fires, the
scheduler enqueues this orchestrator; the orchestrator then dispatches every
step to the broker via `task.kiq(...)` and awaits the result before moving on
to the next one.

```python
from datetime import UTC, datetime

from taskiq_beat import ChainStep, IntervalTrigger, OneOffSchedule, PeriodicSchedule

@broker.task(task_name="reports.collect")
async def collect_report(year: int, month: int) -> dict:
    ...


@broker.task(task_name="reports.render")
async def render_report(*, format: str) -> bytes:
    ...


@broker.task(task_name="reports.upload")
async def upload_report() -> str:
    ...


async with session_factory() as session:
    # Run once, immediately.
    await scheduler_app.chain(
        steps=[
            ChainStep(task=collect_report, args=[2026, 4]),
            ChainStep(task=render_report, kwargs={"format": "pdf"}, max_attempts=3, post_delay_ms=500),
            ChainStep(task=upload_report, timeout_seconds=120.0),
        ],
        on_failure="stop",             # "stop" or "restart"
        max_chain_attempts=2,          # how many times the whole chain may run
        default_step_max_attempts=1,   # default per-step retry budget
        default_step_retry_delay_seconds=0.0,
        default_step_timeout_seconds=None,  # None = wait forever for result
        wait_poll_interval_seconds=0.5,
    ).upsert(
        session,
        trigger=OneOffSchedule(run_at=datetime.now(UTC)),
        job_id="reports.monthly.one_off",
        name="Monthly report chain",
    )

    # Or schedule it periodically like any other job.
    await scheduler_app.chain(
        steps=[
            ChainStep(task=collect_report, args=[2026, 4]),
            ChainStep(task=render_report, kwargs={"format": "pdf"}, max_attempts=3, post_delay_ms=500),
            ChainStep(task=upload_report, timeout_seconds=120.0),
        ],
    ).upsert(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=6)),
        job_id="reports.monthly",
        name="Monthly report chain",
    )
```

### Options

- `ChainStep.task` — accepts the Taskiq task object itself (result of `@broker.task(...)`) or its string name if importing the task is inconvenient.
- `ChainStep.args` / `ChainStep.kwargs` — must be JSON-serializable, same as for a regular schedule.
- `ChainStep.max_attempts` — per-step retry budget. Overrides `default_step_max_attempts`.
- `ChainStep.retry_delay_seconds` — delay between attempts of a single step.
- `ChainStep.timeout_seconds` — maximum time to wait for a single step result; `None` waits forever.
- `ChainStep.post_delay_ms` — delay in milliseconds after a successful step before the next step starts.
- `chain(..., on_failure=...)` — `"stop"` aborts the chain and raises on the worker,
  `"restart"` starts the chain over from the first step.
- `chain(..., max_chain_attempts=...)` — upper bound on chain restarts; `1` disables restart.

### Failure handling

Per-step retries run inside the orchestrator, so intermittent errors are absorbed
transparently. If a step exhausts its attempts:

- with `on_failure="stop"` the orchestrator raises `ChainAbortedError`, which the
  worker logs as a failed task.
- with `on_failure="restart"` the chain re-runs from the first step until
  `max_chain_attempts` is reached, then raises `ChainAbortedError`.

Scheduler-level retries (`dispatch_retry_seconds`) still apply to the
orchestrator itself when `task.kiq(...)` cannot be delivered to the broker.

### Requirements

`task.wait_result(...)` is used to wait for each step, so the broker must have
a result backend configured. `InMemoryBroker` has one built in; for production
brokers attach a result backend such as Redis or database-based implementations
from the Taskiq ecosystem.

Because the orchestrator runs as a regular worker task, it occupies a worker
slot for the full duration of the chain. Size your worker pool accordingly.

## Upsert and startup sync

For system schedules you usually do not want duplicate rows after every restart.

Use a stable `job_id` and call `.upsert(...)`:

```python
async with session_factory() as session:
    await scheduler_app.single(task=heartbeat_task).upsert(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        job_id="system.cleanup",
        name="Cleanup",
    )
```

`.upsert(...)` behavior:

- creates a new row if the job does not exist yet
- updates the existing row if `job_id` already exists
- does not create duplicates on restart
- preserves `next_run_at` if the schedule did not change
- recalculates `next_run_at` if trigger or enabled state changed

For several schedules at startup, just upsert them in a row:

```python
async with session_factory() as session:
    await scheduler_app.single(task=heartbeat_task).upsert(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        job_id="system.cleanup",
        name="Cleanup",
    )
    await scheduler_app.single(task=heartbeat_task).upsert(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(minutes=5)),
        job_id="system.metrics",
        name="Metrics",
    )
    await scheduler_app.chain(steps=[...]).upsert(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=6)),
        job_id="system.reports",
        name="Reports chain",
    )
```

This is the recommended way to register built-in application schedules during startup.

## Manage jobs

What you need:

- `session_factory()` to open an `AsyncSession`
- `job.id`, which is usually taken from the result of `await scheduler_app.single(task=...).schedule(session, trigger=...)`

```python
async with session_factory() as session:
    await scheduler_app.pause(session, job.id)
    await scheduler_app.resume(session, job.id)
    await scheduler_app.run_now(session, job.id)
    await scheduler_app.delete(session, job.id)
```

## Logging

`taskiq-beat` uses the standard Python `logging` module.
The library emits logs, but does not configure handlers, files, or external storage by itself.

This is intentional:

- the application decides where logs go
- local development usually sends them to stdout
- Docker setups usually read them through container logs
- production systems usually forward them to Loki, ELK, Datadog, Cloud Logging, and similar tools

Basic setup:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
```

This will show logs from `taskiq_beat.app`, `taskiq_beat.scheduler`, and `taskiq_beat.engine`.

Typical events that are logged:

- scheduler app start and stop
- scheduler engine sync from storage
- scheduler job create, upsert, pause, resume, run-now, delete
- successful dispatches
- dispatch failures with retry scheduling

If your worker and API run in containers, it is usually enough to log to stdout and collect logs from Docker or Kubernetes.

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

Commands you can copy directly:

```bash
pip install alembic
alembic init alembic
alembic revision --autogenerate -m "add taskiq beat tables"
alembic upgrade head
```

Alembic should detect:

- `scheduler_job`
- `scheduler_run`

## Create tables manually

Use this only for local runs, tests, and quick experiments.

```python
from taskiq_beat import SchedulerBase

async with engine.begin() as connection:
    await connection.run_sync(SchedulerBase.metadata.create_all)
```

These tables will be created:

- `scheduler_job`
- `scheduler_run`

## Load testing

See [scripts/README.md](./scripts/README.md).

## Testing

```bash
pytest
```
