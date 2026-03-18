from __future__ import annotations

import argparse
import asyncio
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from taskiq import InMemoryBroker

from taskiq_beat import OneOffSchedule, SchedulerApp, SchedulerBase, SchedulerConfig
from taskiq_beat.models import SchedulerJob, SchedulerRun


@dataclass(slots=True, frozen=True)
class ScenarioResult:
    jobs: int
    setup_seconds: float
    sync_seconds: float
    dispatch_seconds: float
    dispatched_jobs: int
    failed_jobs: int
    remaining_jobs: int
    jobs_per_second: float
    lag_p50_ms: float
    lag_p95_ms: float
    lag_max_ms: float
    status: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a burst load test against taskiq-beat and estimate when it starts to fall behind.",
    )
    parser.add_argument(
        "--sizes",
        default="100,500,1000,2000,5000,10000",
        help="Comma-separated burst sizes to test.",
    )
    parser.add_argument(
        "--dispatch-delay-ms",
        type=float,
        default=0.0,
        help="Artificial delay inside task.kiq() to emulate broker or network latency.",
    )
    parser.add_argument(
        "--lag-threshold-ms",
        type=float,
        default=1000.0,
        help="Scenario is marked SATURATED when p95 dispatch lag exceeds this threshold.",
    )
    parser.add_argument(
        "--sync-interval-seconds",
        type=float,
        default=60.0,
        help="Scheduler sync interval used during the benchmark.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="How many jobs to insert per batch while preparing a scenario.",
    )
    parser.add_argument(
        "--lead-time-ms",
        type=float,
        default=250.0,
        help="How long before due_at the benchmark seeds jobs and syncs the scheduler.",
    )
    return parser.parse_args()


def parse_sizes(raw_sizes: str) -> list[int]:
    sizes: list[int] = []
    for chunk in raw_sizes.split(","):
        value = int(chunk.strip())
        if value <= 0:
            raise ValueError("All burst sizes must be positive integers.")
        sizes.append(value)
    if not sizes:
        raise ValueError("At least one burst size is required.")
    return sizes


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * ratio
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    weight = position - lower_index
    return lower_value + (upper_value - lower_value) * weight


def format_seconds(value: float) -> str:
    return f"{value:8.3f}"


def format_float(value: float) -> str:
    return f"{value:10.1f}"


def render_bar(label: str, value: float, maximum: float, width: int = 32) -> str:
    if maximum <= 0:
        bar_width = 0
    else:
        bar_width = max(1, int(round((value / maximum) * width))) if value > 0 else 0
    return f"{label:>8} | {'#' * bar_width:<{width}} | {value:10.1f}"


async def clear_database(factory: async_sessionmaker[AsyncSession]) -> None:
    async with factory() as session:
        if session.bind is not None and session.bind.dialect.name == "sqlite":
            await session.execute(text("PRAGMA foreign_keys=OFF"))
        for table in reversed(list(SchedulerBase.metadata.tables.values())):
            await session.execute(delete(table))
        if session.bind is not None and session.bind.dialect.name == "sqlite":
            await session.execute(text("PRAGMA foreign_keys=ON"))
        await session.commit()


async def seed_due_jobs(
    factory: async_sessionmaker[AsyncSession],
    *,
    task_name: str,
    jobs: int,
    due_at: datetime,
    chunk_size: int,
) -> None:
    trigger_payload = OneOffSchedule(run_at=due_at).to_payload()
    created_at = due_at - timedelta(seconds=1)

    async with factory() as session:
        for chunk_start in range(0, jobs, chunk_size):
            chunk_end = min(chunk_start + chunk_size, jobs)
            session.add_all(
                [
                    SchedulerJob(
                        name=f"benchmark-{chunk_start + index}",
                        description="scheduler-load-test",
                        task_name=task_name,
                        kind="one_off",
                        strategy="one_off",
                        trigger_payload=trigger_payload,
                        task_args=[],
                        task_kwargs={},
                        metadata_payload={"source": "scheduler-load-test"},
                        is_enabled=True,
                        next_run_at=due_at,
                        created_at=created_at,
                        updated_at=due_at,
                    )
                    for index in range(chunk_end - chunk_start)
                ]
            )
            await session.flush()
        await session.commit()


async def collect_results(factory: async_sessionmaker[AsyncSession]) -> tuple[int, int, int, list[float]]:
    async with factory() as session:
        runs = list((await session.execute(select(SchedulerRun))).scalars())
        remaining_jobs = len(
            list(
                (
                    await session.execute(
                        select(SchedulerJob).where(
                            SchedulerJob.is_enabled.is_(True),
                            SchedulerJob.next_run_at.is_not(None),
                        )
                    )
                ).scalars()
            )
        )

    dispatched = [run for run in runs if run.status == "dispatched"]
    failed = [run for run in runs if run.status == "failed"]
    lags_ms: list[float] = []
    for run in dispatched:
        if run.dispatched_at is None:
            continue
        lags_ms.append((run.dispatched_at - run.scheduled_for).total_seconds() * 1000.0)
    return len(dispatched), len(failed), remaining_jobs, lags_ms


async def benchmark_scenario(
    app: SchedulerApp,
    factory: async_sessionmaker[AsyncSession],
    *,
    task_name: str,
    jobs: int,
    chunk_size: int,
    lag_threshold_ms: float,
    lead_time_ms: float,
) -> ScenarioResult:
    await clear_database(factory)
    app.engine.jobs = {}
    app.engine.heap = []

    due_at = datetime.now(UTC) + timedelta(milliseconds=lead_time_ms)

    setup_started_at = perf_counter()
    await seed_due_jobs(
        factory,
        task_name=task_name,
        jobs=jobs,
        due_at=due_at,
        chunk_size=chunk_size,
    )
    setup_seconds = perf_counter() - setup_started_at

    sync_started_at = perf_counter()
    await app.engine.sync_all()
    sync_seconds = perf_counter() - sync_started_at

    sleep_seconds = (due_at - datetime.now(UTC)).total_seconds()
    if sleep_seconds > 0:
        await asyncio.sleep(sleep_seconds)

    dispatch_started_at = perf_counter()
    await app.engine.dispatch_due_jobs()
    dispatch_seconds = perf_counter() - dispatch_started_at

    dispatched_jobs, failed_jobs, remaining_jobs, lags_ms = await collect_results(factory)
    lag_p50_ms = percentile(lags_ms, 0.50)
    lag_p95_ms = percentile(lags_ms, 0.95)
    lag_max_ms = max(lags_ms, default=0.0)
    jobs_per_second = dispatched_jobs / dispatch_seconds if dispatch_seconds > 0 else 0.0
    status = "SATURATED" if lag_p95_ms > lag_threshold_ms else "OK"

    return ScenarioResult(
        jobs=jobs,
        setup_seconds=setup_seconds,
        sync_seconds=sync_seconds,
        dispatch_seconds=dispatch_seconds,
        dispatched_jobs=dispatched_jobs,
        failed_jobs=failed_jobs,
        remaining_jobs=remaining_jobs,
        jobs_per_second=jobs_per_second,
        lag_p50_ms=lag_p50_ms,
        lag_p95_ms=lag_p95_ms,
        lag_max_ms=lag_max_ms,
        status=status,
    )


def print_header(args: argparse.Namespace, database_url: str) -> None:
    print("Taskiq Beat Scheduler Load Test")
    print("=" * 80)
    print(f"Python:              {sys.version.split()[0]}")
    print(f"Database:            {database_url}")
    print(f"Dispatch delay:      {args.dispatch_delay_ms:.1f} ms")
    print(f"Lag threshold (p95): {args.lag_threshold_ms:.1f} ms")
    print(f"Lead time:           {args.lead_time_ms:.1f} ms")
    print(f"Sync interval:       {args.sync_interval_seconds:.1f} s")
    print()
    print("A scenario is marked SATURATED when p95 dispatch lag crosses the threshold.")
    print()


def print_results_table(results: list[ScenarioResult]) -> None:
    headers = (
        "Jobs",
        "Setup s",
        "Sync s",
        "Dispatch s",
        "Jobs/s",
        "p50 lag ms",
        "p95 lag ms",
        "max lag ms",
        "Fail",
        "Remain",
        "Status",
    )
    print(
        f"{headers[0]:>8} {headers[1]:>8} {headers[2]:>8} {headers[3]:>10} "
        f"{headers[4]:>10} {headers[5]:>11} {headers[6]:>11} {headers[7]:>11} "
        f"{headers[8]:>6} {headers[9]:>7} {headers[10]:>11}"
    )
    print("-" * 110)
    for result in results:
        print(
            f"{result.jobs:8d} {format_seconds(result.setup_seconds)} {format_seconds(result.sync_seconds)} "
            f"{format_seconds(result.dispatch_seconds):>10} {format_float(result.jobs_per_second)} "
            f"{format_float(result.lag_p50_ms):>11} {format_float(result.lag_p95_ms):>11} "
            f"{format_float(result.lag_max_ms):>11} {result.failed_jobs:6d} {result.remaining_jobs:7d} "
            f"{result.status:>11}"
        )
    print()


def print_charts(results: list[ScenarioResult]) -> None:
    max_p95 = max((result.lag_p95_ms for result in results), default=0.0)
    max_throughput = max((result.jobs_per_second for result in results), default=0.0)

    print("P95 Lag Chart (ms)")
    print("-" * 80)
    for result in results:
        print(render_bar(f"{result.jobs}", result.lag_p95_ms, max_p95))
    print()

    print("Throughput Chart (jobs/s)")
    print("-" * 80)
    for result in results:
        print(render_bar(f"{result.jobs}", result.jobs_per_second, max_throughput))
    print()


def print_assessment(results: list[ScenarioResult], lag_threshold_ms: float) -> None:
    saturated = next((result for result in results if result.status == "SATURATED"), None)
    healthy = [result for result in results if result.status == "OK"]

    print("Assessment")
    print("-" * 80)
    if healthy:
        strongest_healthy = healthy[-1]
        print(
            "Highest non-saturated burst: "
            f"{strongest_healthy.jobs} jobs "
            f"(p95 lag {strongest_healthy.lag_p95_ms:.1f} ms, "
            f"throughput {strongest_healthy.jobs_per_second:.1f} jobs/s)."
        )
    else:
        print("Every tested scenario was already above the lag threshold.")

    if saturated is None:
        heaviest = results[-1]
        print(
            "Saturation not reached in the tested range. "
            f"Heaviest scenario: {heaviest.jobs} jobs "
            f"(p95 lag {heaviest.lag_p95_ms:.1f} ms, "
            f"throughput {heaviest.jobs_per_second:.1f} jobs/s)."
        )
    else:
        safe_burst_estimate = int((saturated.jobs_per_second * lag_threshold_ms) / 1000.0)
        print(
            "First saturated burst: "
            f"{saturated.jobs} jobs "
            f"(p95 lag {saturated.lag_p95_ms:.1f} ms > {lag_threshold_ms:.1f} ms)."
        )
        print(
            "Rule-of-thumb burst capacity at this dispatch speed: "
            f"about {safe_burst_estimate} jobs within {lag_threshold_ms:.0f} ms of lag budget."
        )
    print()


async def run_benchmark(args: argparse.Namespace) -> None:
    sizes = parse_sizes(args.sizes)
    dispatch_delay_seconds = args.dispatch_delay_ms / 1000.0

    database_path = Path(tempfile.gettempdir()) / f"taskiq_beat_load_test_{uuid4().hex}.sqlite3"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    engine = create_async_engine(database_url, future=True)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False)
    broker = InMemoryBroker()

    @broker.task(task_name="benchmark.noop")
    async def noop_task() -> None:
        return None

    async def fake_kiq(*args: Any, **kwargs: Any):
        if dispatch_delay_seconds > 0:
            await asyncio.sleep(dispatch_delay_seconds)

        class Result:
            task_id = uuid4().hex

        return Result()

    noop_task.kiq = fake_kiq  # type: ignore[method-assign]

    app = SchedulerApp(
        broker=broker,
        session_factory=session_factory,
        config=SchedulerConfig(sync_interval_seconds=args.sync_interval_seconds),
    )

    try:
        async with engine.begin() as connection:
            await connection.run_sync(SchedulerBase.metadata.create_all)

        print_header(args, database_url)
        results: list[ScenarioResult] = []
        for jobs in sizes:
            result = await benchmark_scenario(
                app,
                session_factory,
                task_name="benchmark.noop",
                jobs=jobs,
                chunk_size=args.chunk_size,
                lag_threshold_ms=args.lag_threshold_ms,
                lead_time_ms=args.lead_time_ms,
            )
            results.append(result)
            print(
                f"Completed burst {jobs:>8} jobs | "
                f"{result.jobs_per_second:>10.1f} jobs/s | "
                f"p95 lag {result.lag_p95_ms:>10.1f} ms | "
                f"{result.status}"
            )

        print()
        print_results_table(results)
        print_charts(results)
        print_assessment(results, args.lag_threshold_ms)
    finally:
        await engine.dispose()
        database_path.unlink(missing_ok=True)


def main() -> None:
    args = parse_args()
    asyncio.run(run_benchmark(args))


if __name__ == "__main__":
    main()
