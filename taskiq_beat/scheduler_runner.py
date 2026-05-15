from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from typing import Any

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
    ) -> None:
        self.scheduler_app = scheduler_app
        self.startup_hooks = tuple(startup_hooks)
        self.manage_broker = manage_broker

    async def start(self) -> None:
        """Run startup hooks, broker startup, and scheduler startup."""
        await self.run_startup_hooks()
        if self.manage_broker:
            await self.call_broker_method("startup")
        await self.scheduler_app.start()
        log.info("Taskiq Beat scheduler runner started.")

    async def stop(self) -> None:
        """Stop scheduler and broker resources."""
        await self.scheduler_app.stop()
        if self.manage_broker:
            await self.call_broker_method("shutdown")
        log.info("Taskiq Beat scheduler runner stopped.")

    async def run_forever(self) -> None:
        """Run scheduler until SIGINT or SIGTERM is received."""
        stop_event = asyncio.Event()
        self.bind_stop_signals(stop_event)

        await self.start()
        try:
            await stop_event.wait()
        finally:
            await self.stop()

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
    def bind_stop_signals(stop_event: asyncio.Event) -> None:
        """Bind SIGINT and SIGTERM when the current event loop supports it."""
        loop = asyncio.get_running_loop()
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(stop_signal, stop_event.set)


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
