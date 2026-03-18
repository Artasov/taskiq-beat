# taskiq-beat

<div align="center">
  <p><strong>Планировщик для Taskiq с хранением расписания и истории запусков в базе данных</strong></p>
  <p>Один активный scheduler process на одну базу данных</p>
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
  <a href="./scripts/README.md">README для scripts</a>
</div>

## Навигация

- [Быстрый старт](#быстрый-старт)
- [Запуск с FastAPI](#запуск-с-fastapi)
- [Создание job](#создание-job)
- [Управление job](#управление-job)
- [Alembic](#alembic)
- [Конфигурация по умолчанию](#конфигурация-по-умолчанию)
- [Создание таблиц вручную](#создание-таблиц-вручную)
- [Нагрузочный тест](#нагрузочный-тест)

## Установка

```bash
pip install taskiq-beat
```

> `pip install "taskiq-beat[test]"`</br>
> `pip install "taskiq-beat[dev]"`

## Быстрый старт

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat import (
    IntervalTrigger,
    PeriodicSchedule,
    SchedulerApp,
    SchedulerConfig,
)

# SQLAlchemy engine для подключения к базе.
engine = create_async_engine("sqlite+aiosqlite:///scheduler.sqlite3")

# Фабрика, которая создаёт AsyncSession.
# Через эти сессии scheduler работает с базой данных.
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# Taskiq broker. InMemoryBroker здесь только для простого примера.
broker = InMemoryBroker()


@broker.task(task_name="demo.heartbeat")
async def heartbeat_task() -> None:
    print("tick")


# Главная точка входа taskiq-beat.
scheduler_app = SchedulerApp(
    broker=broker,
    session_factory=session_factory,
    config=SchedulerConfig(),
)
```

## Запуск с FastAPI

Scheduler можно запускать:

- внутри FastAPI process
- отдельным process/service

Можно и так, и так. Главное правило: не запускать несколько scheduler processes на одну и ту же базу.

Пример c FastAPI:

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
        # Как часто scheduler перечитывает активные job из базы данных.
        sync_interval_seconds=1.0,
        # Минимальная пауза между итерациями scheduler loop.
        idle_sleep_seconds=0.2,
        # Через сколько секунд повторить dispatch после ошибки в task.kiq(...).
        dispatch_retry_seconds=5,
        # Сколько job можно dispatch'ить параллельно внутри одного batch.
        dispatch_concurrency=32,
        # Сколько due job максимум забирать из heap за один batch.
        dispatch_batch_size=256,
        # Нужно ли писать строки истории в SchedulerRun.
        record_runs=True,
        # Базовая timezone для helper API, если явно не указана другая.
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

## Создание job

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

print(job.id)  # Например: "6c6342d8-6d74-4d16-8f7a-5d4f1b3a0b13"
```

`await scheduler.schedule(session)` возвращает `SchedulerJob`.
Это SQLAlchemy model instance с полями вроде:

- `job.id`
- `job.task_name`
- `job.kind`
- `job.strategy`
- `job.next_run_at`
- `job.is_enabled`

Чаще всего дальше используется именно `job.id` для pause, resume, run-now и delete.

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

## Управление job

Что нужно:

- `session_factory()`, чтобы открыть `AsyncSession`
- `job.id`, который обычно берётся из результата `await scheduler.schedule(session)`

```python
async with session_factory() as session:
    await scheduler_app.pause(session, job.id)
    await scheduler_app.resume(session, job.id)
    await scheduler_app.run_now(session, job.id)
    await scheduler_app.delete(session, job.id)
```

## Alembic

Чтобы Alembic увидел таблицы scheduler'а, добавь `SchedulerBase.metadata` в `target_metadata`.

`alembic/env.py`:

```python
from myapp.db import Base
from taskiq_beat import SchedulerBase

target_metadata = [
    # Твоя основная ORM metadata.
    Base.metadata,
    # Metadata из taskiq-beat.
    SchedulerBase.metadata,
]
```

Команды, которые можно просто скопировать:

```bash
pip install alembic
alembic init alembic
alembic revision --autogenerate -m "add taskiq beat tables"
alembic upgrade head
```

Alembic должен обнаружить:

- `scheduler_job`
- `scheduler_run`

## Создание таблиц вручную

Используй это только для локального запуска, тестов и быстрых экспериментов.

```python
from taskiq_beat import SchedulerBase

async with engine.begin() as connection:
    await connection.run_sync(SchedulerBase.metadata.create_all)
```

Будут созданы таблицы:

- `scheduler_job`
- `scheduler_run`

## Нагрузочный тест

Смотри [scripts/README.md](./scripts/README.md).

## Тестирование

```bash
pytest
```
