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
- [Цепочки задач](#цепочки-задач)
- [Upsert и стартовый sync](#upsert-и-стартовый-sync)
- [Управление job](#управление-job)
- [Логирование](#логирование)
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
    ImmediateDispatch,
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
3. Создавать job через `scheduler_app.single(task=...).schedule(session)` или `scheduler_app.chain(steps=[...]).schedule(session)`.

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

Публичный API `SchedulerApp` построен на двух builder-ах:

- `scheduler_app.single(task=...)` — запланировать одну задачу.
- `scheduler_app.chain(steps=[...])` — запланировать упорядоченную цепочку задач.

У обоих есть `.schedule(session, trigger=...)` и
`.upsert(session, trigger=..., job_id=...)`.
Если в `.schedule(...)` не передать `trigger`, taskiq-beat использует `ImmediateDispatch()`: задача сразу уходит в
broker, а записи job/run сохраняются в базе.

```python
from datetime import UTC, datetime, timedelta

from taskiq_beat import ImmediateDispatch, IntervalTrigger, OneOffSchedule, PeriodicSchedule

# По умолчанию: сразу task.kiq(...) + запись в БД.
await scheduler_app.single(task=heartbeat_task).schedule(session)

# То же самое, но явно.
await scheduler_app.single(task=heartbeat_task).schedule(
    session,
    trigger=ImmediateDispatch(),
)

# Явные варианты отложенного или периодического запуска.
await scheduler_app.single(task=heartbeat_task).schedule(
    session,
    trigger=OneOffSchedule(run_at=datetime.now(UTC) + timedelta(minutes=10)),
)
await scheduler_app.single(task=heartbeat_task).schedule(
    session,
    trigger=PeriodicSchedule(interval=IntervalTrigger(minutes=5)),
)
```

`ImmediateDispatch` работает только с `.schedule(...)`. Для `.upsert(...)` используй настоящий schedule trigger.

### Interval job

```python
from taskiq_beat import IntervalTrigger, PeriodicSchedule

async with session_factory() as session:
    job = await scheduler_app.single(task=heartbeat_task).schedule(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(seconds=5)),
        name="Heartbeat every 5 seconds",
    )

print(job.id)  # Например: "6c6342d8-6d74-4d16-8f7a-5d4f1b3a0b13"
```

`.schedule(session, ...)` возвращает `SchedulerJob`.
Это SQLAlchemy model instance с полями вроде:

- `job.id`
- `job.task_name`
- `job.kind`
- `job.strategy`
- `job.next_run_at`
- `job.is_enabled`

Чаще всего дальше используется именно `job.id` для pause, resume, run-now и delete.

Если нужен стабильный идентификатор для системной job, используй `.upsert(...)` с явным `job_id`:

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

### Передача args и kwargs в задачу

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

## Цепочки задач

Сам Taskiq не гарантирует упорядоченный запуск нескольких задач друг за другом.
`taskiq-beat` добавляет встроенный orchestrator, который выполняет задачи по
очереди и хранится как обычный scheduler job.

Orchestrator автоматически регистрируется в `SchedulerApp` под именем
`taskiq_beat.chain_orchestrator`. Когда срабатывает расписание, scheduler
enqueue'ит orchestrator, а уже orchestrator поочерёдно вызывает каждый шаг через
`task.kiq(...)` и ждёт его результат перед переходом к следующему шагу.

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
    # Однократный запуск прямо сейчас.
    await scheduler_app.chain(
        steps=[
            ChainStep(task=collect_report, args=[2026, 4]),
            ChainStep(task=render_report, kwargs={"format": "pdf"}, max_attempts=3, post_delay_ms=500),
            ChainStep(task=upload_report, timeout_seconds=120.0),
        ],
        on_failure="stop",             # "stop" или "restart"
        max_chain_attempts=2,          # сколько раз можно перезапускать всю цепочку
        default_step_max_attempts=1,   # дефолтный бюджет ретраев на шаг
        default_step_retry_delay_seconds=0.0,
        default_step_timeout_seconds=None,  # None = ждать результат сколько угодно
        wait_poll_interval_seconds=0.5,
    ).upsert(
        session,
        trigger=OneOffSchedule(run_at=datetime.now(UTC)),
        job_id="reports.monthly.one_off",
        name="Monthly report chain",
    )

    # Либо периодически, как любой другой job.
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

### Параметры

- `ChainStep.task` — принимает саму Taskiq-задачу (результат `@broker.task(...)`) либо её строковое имя, если импортировать задачу неудобно.
- `ChainStep.args` / `ChainStep.kwargs` — должны быть JSON-сериализуемыми, как и для обычного расписания.
- `ChainStep.max_attempts` — бюджет ретраев для конкретного шага. Перекрывает `default_step_max_attempts`.
- `ChainStep.retry_delay_seconds` — задержка между попытками одного шага.
- `ChainStep.timeout_seconds` — максимальное время ожидания результата шага; `None` — без лимита.
- `ChainStep.post_delay_ms` — задержка в миллисекундах после успешного шага перед запуском следующего.
- `chain(..., on_failure=...)` — `"stop"` останавливает цепочку и бросает исключение на worker,
  `"restart"` запускает цепочку заново с первого шага.
- `chain(..., max_chain_attempts=...)` — максимум перезапусков цепочки; `1` отключает перезапуск.

### Обработка сбоев

Ретраи на уровне шага происходят внутри orchestrator, поэтому временные ошибки
поглощаются прозрачно. Если шаг исчерпал свои попытки:

- при `on_failure="stop"` orchestrator бросает `ChainAbortedError`, и worker
  логирует таск как упавший.
- при `on_failure="restart"` цепочка запускается с первого шага, пока не будет
  достигнут `max_chain_attempts`, затем также бросается `ChainAbortedError`.

Scheduler-уровневые ретраи (`dispatch_retry_seconds`) по-прежнему работают для
самого orchestrator, если `task.kiq(...)` не удалось отправить в broker.

### Требования

Для ожидания каждого шага используется `task.wait_result(...)`, поэтому у broker
должен быть настроен result backend. У `InMemoryBroker` он встроенный; для
продакшен-брокеров нужно подключить backend (Redis и другие) из экосистемы Taskiq.

Orchestrator выполняется как обычный worker task и занимает слот worker на всё
время работы цепочки. Учитывайте это при выборе размера worker pool.

## Upsert и стартовый sync

Для системных расписаний обычно не нужны дубли строк после каждого рестарта.

Используй стабильный `job_id` и вызывай `.upsert(...)`:

```python
async with session_factory() as session:
    await scheduler_app.single(task=heartbeat_task).upsert(
        session,
        trigger=PeriodicSchedule(interval=IntervalTrigger(hours=1)),
        job_id="system.cleanup",
        name="Cleanup",
    )
```

Что делает `.upsert(...)`:

- создаёт новую строку, если job ещё нет
- обновляет существующую строку, если `job_id` уже есть
- не создаёт дубли при рестартах
- сохраняет `next_run_at`, если расписание не менялось
- пересчитывает `next_run_at`, если изменился trigger или enabled state

Для нескольких расписаний на старте просто вызывай `.upsert(...)` подряд:

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

Именно так лучше регистрировать встроенные расписания приложения при старте.

## Управление job

Что нужно:

- `session_factory()`, чтобы открыть `AsyncSession`
- `job.id`, который обычно берётся из результата `await scheduler_app.single(task=...).schedule(session, trigger=...)`

```python
async with session_factory() as session:
    await scheduler_app.pause(session, job.id)
    await scheduler_app.resume(session, job.id)
    await scheduler_app.run_now(session, job.id)
    await scheduler_app.delete(session, job.id)
```

## Логирование

`taskiq-beat` использует стандартный Python `logging`.
Библиотека сама пишет события в логгер, но не настраивает handlers, файлы или внешнее хранилище.

Это сделано специально:

- приложение само решает, куда отправлять логи
- локально обычно достаточно stdout
- в Docker обычно читают логи контейнера
- в production их обычно отправляют в Loki, ELK, Datadog, Cloud Logging и похожие системы

Базовая настройка:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
```

Так будут видны логи из `taskiq_beat.app`, `taskiq_beat.scheduler` и `taskiq_beat.engine`.

Что логируется:

- старт и остановка scheduler app
- sync scheduler engine с хранилищем
- create, upsert, pause, resume, run-now, delete для job
- успешные dispatch'и
- ошибки dispatch'а и постановка retry

Если worker и API запущены в контейнерах, обычно достаточно писать в stdout и собирать логи уже на уровне Docker или Kubernetes.

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
