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

### Планировщик для Taskiq с хранением расписания и истории запусков в базе данных

## Навигация

- [Быстрый старт](#быстрый-старт)
- [Как это запускать](#как-это-запускать)
- [Запуск с FastAPI](#запуск-с-fastapi)
- [Создание job](#создание-job)
- [Upsert и стартовый sync](#upsert-и-стартовый-sync)
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

`taskiq-beat` сам по себе не даёт сетевой broker. Для реального запуска в нескольких process обычно нужен ещё broker
backend из экосистемы Taskiq.

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

# Taskiq broker для демонстрации API.
# InMemoryBroker не подходит для сценария с отдельным worker process.
broker = InMemoryBroker()


@broker.task(task_name="demo.heartbeat")
async def heartbeat_task() -> None:
    print("tick")


# Главная точка входа taskiq-beat.
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
```

Этот пример только показывает, как собрать `broker`, `session_factory` и `scheduler_app`.
Он ещё не означает, что задачи уже начнут выполняться в отдельном process.

## Как это запускать

Из примера выше создаются только объекты Python. Этого ещё недостаточно, чтобы система начала работать сама по себе.

Что нужно в реальном приложении:

1. Поднять process с `scheduler_app.start()`.
2. Поднять Taskiq worker process, который будет забирать задачи из broker.
3. Создавать job через `scheduler_app.create_scheduler(...).schedule(session)`.

Важно:

- `InMemoryBroker` в примерах подходит только для демонстрации API, тестов и локальных экспериментов внутри одного
  process.
- Если ты хочешь отдельный worker в другом терминале или service, нужен настоящий broker backend.
- Worker обычно запускается в отдельном терминале или отдельном service/process.
- Команда `python -m taskiq worker app.main:broker` означает: импортировать объект `broker` из модуля `app.main` и
  слушать его очередь.

Типичный сценарий:

- терминал 1: FastAPI с `scheduler_app.start()` внутри lifespan
- терминал 2: Taskiq worker
- терминал 3: запросы в API или отдельный script, который создаёт job

Самый обычный сценарий для FastAPI выглядит так:

1. В `app/main.py` лежат `broker`, `scheduler_app`, `app = FastAPI(...)` и task-функции.
2. В первом терминале запускается FastAPI:

```bash
uvicorn app.main:app --reload
```

3. Во втором терминале запускается worker:

```bash
python -m taskiq worker app.main:broker
```

4. После этого ты создаёшь job через API, script или Python shell.

Что происходит после запуска:

- scheduler следит за расписанием и в нужный момент вызывает `task.kiq(...)`
- broker публикует задачу
- worker забирает задачу и выполняет её

Если worker не запущен, scheduler сможет публиковать задачи в broker, но выполнять их будет некому.

Если хочется запускать scheduler не внутри FastAPI, а отдельно, это тоже можно делать.
Тогда один process держит `scheduler_app.start()`, а второй process всё равно остаётся Taskiq worker.

## Запуск с FastAPI

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
    config=SchedulerConfig(),  # Как настраивается было показано выше
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

### Scheduler можно запускать:

- #### Внутри FastAPI
  Как в примере выше

- #### Отдельно
    - terminal/container 1:
      `uvicorn app.main:app --reload`
    - terminal/container 2:
      `python -m app.run_scheduler`
    - terminal/container 3:
      `python -m taskiq worker app.broker:broker`

Можно и так, и так. Главное правило: не запускать несколько scheduler processes на одну и ту же базу.

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

Если нужен стабильный идентификатор для системной job, передай `job_id` явно:

```python
async with session_factory() as session:
    job = await scheduler_app.create_scheduler(
        job_id="system.heartbeat",
        task=heartbeat_task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(minutes=5)),
        name="System heartbeat",
    ).upsert(session)
```

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

## Upsert и стартовый sync

Для системных расписаний обычно не нужны дубли строк после каждого рестарта.

Используй стабильный `job_id` и вызывай `upsert(...)`:

```python
async with session_factory() as session:
    await scheduler_app.create_scheduler(
        job_id="system.cleanup",
        task=heartbeat_task,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        name="Cleanup",
    ).upsert(session)
```

Что делает `upsert(...)`:

- создаёт новую строку, если job ещё нет
- обновляет существующую строку, если `job_id` уже есть
- не создаёт дубли при рестартах
- сохраняет `next_run_at`, если расписание не менялось
- пересчитывает `next_run_at`, если изменился trigger или enabled state

Также можно синкать несколько расписаний на старте:

```python
async with session_factory() as session:
    await scheduler_app.sync_schedules(
        session,
        [
            scheduler_app.create_scheduler(
                job_id="system.cleanup",
                task=heartbeat_task,
                trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
                name="Cleanup",
            ),
            scheduler_app.create_scheduler(
                job_id="system.metrics",
                task=heartbeat_task,
                trigger=PeriodicSchedule(interval=IntervalTrigger(minutes=5)),
                name="Metrics",
            ),
        ],
    )
```

Именно так лучше регистрировать встроенные расписания приложения при старте.

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
