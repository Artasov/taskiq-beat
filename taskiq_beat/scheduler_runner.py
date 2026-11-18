from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import logging
import math
import os
import signal
import sys
import threading
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, ExitStack
from typing import Any

from sqlalchemy.exc import OperationalError

from taskiq_beat.app import SchedulerApp

log = logging.getLogger(__name__)

StartupHook = Callable[[], object | Awaitable[object]]


class ImportPathResolver:
    """Loads Python objects from module:attribute strings."""

    @staticmethod
    def load(path: str) -> Any:
        """Import an object by a module:attribute path."""
        module_path, separator, attribute_path = path.partition(":")
        if not separator or not module_path or not attribute_path:
            raise ValueError(f"Import path must look like module:attribute, got {path!r}.")

        module = importlib.import_module(module_path)
        current: Any = module
        for attribute_name in attribute_path.split("."):
            current = getattr(current, attribute_name)
        return current


class SchedulerRunner:
    """Runs a SchedulerApp as a standalone process."""

    def __init__(
        self,
        scheduler_app: SchedulerApp,
        *,
        startup_hooks: Sequence[StartupHook] = (),
        manage_broker: bool = True,
        retry_delay_seconds: float | None = None,
    ) -> None:
        if retry_delay_seconds is not None and (not math.isfinite(retry_delay_seconds) or retry_delay_seconds <= 0):
            raise ValueError("retry_delay_seconds must be a positive finite number.")
        self.scheduler_app = scheduler_app
        self.startup_hooks = tuple(startup_hooks)
        self.manage_broker = manage_broker
        self.retry_delay_seconds = retry_delay_seconds
        self._lifecycle_lock = asyncio.Lock()
        self._resources: AsyncExitStack | None = None

    async def start(self) -> None:
        """Load tasks, run hooks, then start the broker and scheduler once."""
        async with self._lifecycle_lock:
            if self._resources is not None:
                return
            self.scheduler_app.load_tasks()
            await self.run_startup_hooks()
            async with AsyncExitStack() as resources:
                if self.manage_broker:
                    resources.push_async_callback(self.call_broker_method, "shutdown")
                    await self.call_broker_method("startup")
                resources.push_async_callback(self.scheduler_app.stop)
                await self.scheduler_app.start()
                self._resources = resources.pop_all()
        log.info("Taskiq Beat scheduler runner started.")

    async def stop(self) -> None:
        """Release the runner's resources, also when its caller is cancelled."""
        async with self._lifecycle_lock:
            if self._resources is None:
                return
            cleanup = asyncio.create_task(self._resources.aclose())
            self._resources = None
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise
        log.info("Taskiq Beat scheduler runner stopped.")

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        """Run until a process signal or an externally managed event requests shutdown."""
        with ExitStack() as signals:
            if stop_event is None:
                stop_event = asyncio.Event()
                signals.enter_context(self.bind_stop_signals(stop_event))
            if stop_event.is_set():
                return
            startup = asyncio.create_task(self._start_with_retry())
            stopping = asyncio.create_task(stop_event.wait())
            try:
                done, _ = await asyncio.wait((startup, stopping), return_when=asyncio.FIRST_COMPLETED)
                if startup in done:
                    await startup
                    await stopping
            finally:
                startup.cancel()
                stopping.cancel()
                await asyncio.gather(startup, stopping, return_exceptions=True)
                await self.stop()

    async def _start_with_retry(self) -> None:
        """Repeat the startup sequence on temporary database or network failures."""
        while True:
            try:
                await self.start()
                return
            except (OSError, OperationalError):
                if self.retry_delay_seconds is None:
                    raise
                log.warning(f"Scheduler startup failed; retrying in {self.retry_delay_seconds}s.", exc_info=True)
                await asyncio.sleep(self.retry_delay_seconds)

    async def run_startup_hooks(self) -> None:
        """Run app-specific startup hooks before the scheduler loop starts."""
        for hook in self.startup_hooks:
            result = hook()
            if inspect.isawaitable(result):
                await result

    async def call_broker_method(self, method_name: str) -> None:
        """Call an optional async/sync method on the SchedulerApp broker."""
        method = getattr(self.scheduler_app.broker, method_name, None)
        if method is None:
            return
        result = method()
        if inspect.isawaitable(result):
            await result

    @staticmethod
    def bind_stop_signals(stop_event: asyncio.Event) -> ExitStack:
        """Install process handlers on the main thread and return their cleanup context."""
        handlers = ExitStack()
        if threading.current_thread() is not threading.main_thread():
            return handlers
        loop = asyncio.get_running_loop()

        def request_stop(_signum: int, _frame: object) -> None:
            loop.call_soon_threadsafe(stop_event.set)

        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            previous = signal.signal(stop_signal, request_stop)
            handlers.callback(signal.signal, stop_signal, previous if previous is not None else signal.SIG_DFL)
        return handlers


class SchedulerRunnerCli:
    """CLI parser for the standalone scheduler runner."""

    @staticmethod
    def add_current_directory_to_import_path() -> None:
        """Make app import paths work when the runner is launched as a console script."""
        current_directory = os.getcwd()
        if current_directory not in sys.path:
            sys.path.insert(0, current_directory)

    @staticmethod
    def build_parser() -> argparse.ArgumentParser:
        """Create the command line parser."""
        parser = argparse.ArgumentParser(
            prog="taskiq-beat-scheduler",
            description="Run a taskiq-beat SchedulerApp from an import path.",
        )
        parser.add_argument("scheduler_app", help="Import path to SchedulerApp, for example app.tasks:scheduler_app.")
        parser.add_argument(
            "--startup",
            action="append",
            default=[],
            help="Import path to a no-argument startup hook. Can be passed more than once.",
        )
        parser.add_argument(
            "--retry-delay",
            type=float,
            help="Retry startup after a transient database or network failure, waiting this many seconds.",
        )
        parser.add_argument(
            "--skip-broker-lifespan",
            action="store_true",
            help="Do not call scheduler_app.broker.startup()/shutdown().",
        )
        parser.add_argument("--log-level", default="INFO", help="Python logging level. Default: INFO.")
        return parser

    @classmethod
    def build_runner(cls, argv: Sequence[str] | None = None) -> SchedulerRunner:
        """Load CLI targets and create a SchedulerRunner."""
        args = cls.build_parser().parse_args(argv)
        cls.add_current_directory_to_import_path()
        logging.basicConfig(
            level=getattr(logging, args.log_level.upper(), logging.INFO),
            format="%(levelname)s:     %(message)s",
        )

        scheduler_app = ImportPathResolver.load(args.scheduler_app)
        if not isinstance(scheduler_app, SchedulerApp):
            raise TypeError(f"{args.scheduler_app!r} does not point to a SchedulerApp instance.")

        startup_hooks = tuple(ImportPathResolver.load(path) for path in args.startup)
        return SchedulerRunner(
            scheduler_app,
            startup_hooks=startup_hooks,
            manage_broker=not args.skip_broker_lifespan,
            retry_delay_seconds=args.retry_delay,
        )

    @classmethod
    def main(cls, argv: Sequence[str] | None = None) -> None:
        """Run the scheduler process from CLI arguments."""
        runner = cls.build_runner(argv)
        try:
            asyncio.run(runner.run_forever())
        except KeyboardInterrupt:
            pass


def main() -> None:
    """Module entrypoint for python -m taskiq_beat.scheduler_runner."""
    SchedulerRunnerCli.main()


if __name__ == "__main__":
    main()
