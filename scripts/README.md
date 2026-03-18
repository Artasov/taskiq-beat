# Scripts

Здесь лежат служебные скрипты для `taskiq-beat`.

## scheduler_load_test.py

`scheduler_load_test.py` запускает burst-нагрузочный тест scheduler'а.

Что делает скрипт:

- создаёт временную SQLite базу
- создаёт пачку due job
- синкает scheduler
- запускает dispatch loop
- считает throughput и lag
- показывает, где начинается saturation

Что измеряется:

- `setup time`
- `sync time`
- `dispatch time`
- `jobs/s`
- `p50 lag`
- `p95 lag`
- `max lag`
- сколько dispatch попыток упало
- сколько job осталось активными

Сценарий считается `SATURATED`, если `p95 lag` больше заданного порога.

## Быстрый запуск

```bash
.\.venv\Scripts\python.exe scripts\scheduler_load_test.py
```

## Аргументы

```bash
.\.venv\Scripts\python.exe scripts\scheduler_load_test.py --help
```

Основные аргументы:

- `--sizes`
  Размеры burst-нагрузки через запятую. Пример: `100,500,1000,2000`.
- `--dispatch-delay-ms`
  Искусственная задержка внутри `task.kiq()`. Нужна, чтобы эмулировать broker или сеть.
- `--lag-threshold-ms`
  Бюджет по `p95 lag`.
- `--sync-interval-seconds`
  Значение `sync_interval_seconds`, с которым запускается scheduler.
- `--chunk-size`
  Сколько job вставлять в базу за один batch.
- `--lead-time-ms`
  За сколько миллисекунд до `due_at` скрипт создаёт job и синкает scheduler.

## Готовые сценарии

### Baseline

Чистая стоимость самого scheduler'а почти без задержки broker.

```bash
.\.venv\Scripts\python.exe scripts\scheduler_load_test.py --sizes 100,500,1000,2000,5000 --dispatch-delay-ms 0 --lag-threshold-ms 1000
```

### Realistic

Небольшая искусственная задержка, похожая на локальный broker или лёгкую сеть.

```bash
.\.venv\Scripts\python.exe scripts\scheduler_load_test.py --sizes 100,500,1000,2000 --dispatch-delay-ms 2 --lag-threshold-ms 1000
```

### Stress

Поиск явной точки saturation на больших burst размерах.

```bash
.\.venv\Scripts\python.exe scripts\scheduler_load_test.py --sizes 1000,2000,5000,10000,20000 --dispatch-delay-ms 5 --lag-threshold-ms 2000
```

### Tight SLA

Проверка, сколько job помещается в жёсткий budget по задержке.

```bash
.\.venv\Scripts\python.exe scripts\scheduler_load_test.py --sizes 10,50,100,250,500 --dispatch-delay-ms 1 --lag-threshold-ms 100
```

## Как читать вывод

Скрипт печатает три части.

### Progress строки

Пример:

```text
Completed burst      500 jobs |      320.5 jobs/s | p95 lag      412.7 ms | OK
```

Это короткая сводка по каждому сценарию во время выполнения.

### Итоговая таблица

Колонки:

- `Jobs`
  Размер burst-а.
- `Setup s`
  Сколько заняло создание job в базе.
- `Sync s`
  Сколько заняла загрузка job в scheduler engine.
- `Dispatch s`
  Сколько занял dispatch всех due job.
- `Jobs/s`
  Фактический throughput scheduler'а.
- `p50 lag ms`
  Медианная задержка между scheduled time и реальным dispatch.
- `p95 lag ms`
  Основная метрика для saturation.
- `max lag ms`
  Худшая задержка в сценарии.
- `Fail`
  Сколько dispatch попыток завершилось ошибкой.
- `Remain`
  Сколько job осталось активными после прогона.
- `Status`
  `OK` или `SATURATED`.

### Графики и assessment

ASCII-графики дают быстрый визуальный обзор.

`Assessment` в конце показывает:

- самый большой burst без saturation
- первый burst, на котором наступил saturation
- грубую оценку burst capacity внутри выбранного lag budget

## Как использовать на практике

Нормальный порядок такой:

1. Сначала прогнать `Baseline`.
2. Потом прогнать `Realistic` с `1`, `2` или `5` ms задержки.
3. Сравнивать в первую очередь `p95 lag`, а не только `jobs/s`.
4. Выбрать реальный lag budget для продукта, например `100 ms`, `500 ms` или `2000 ms`.

Если важно, чтобы job начинались не позже чем через `500 ms`, смотреть надо прежде всего на `p95 lag`.

## Ограничения теста

Этот скрипт меряет поведение scheduler'а на этапе dispatch, а не всей системы целиком.

Сюда не входит:

- реальное выполнение задач worker'ами
- реальная очередь production broker'а
- сетевой contention production окружения
- дополнительная нагрузка приложения на базу

Это benchmark scheduler'а, а не полный benchmark всей системы.
