from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from .alpaca_client import AlpacaClient
from .bot import run_once
from .journal import TradeJournal


@dataclass
class RunnerConfig:
    segment: str
    budget: float
    execute: bool
    interval_seconds: int


class BotScheduler:
    def __init__(self, *, client: AlpacaClient, risk_policy: dict[str, Any], journal: TradeJournal) -> None:
        self.client = client
        self.risk_policy = risk_policy
        self.journal = journal
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._config: RunnerConfig | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None
        self._last_run_at: str | None = None

    def start(self, config: RunnerConfig) -> None:
        with self._lock:
            if self.is_running():
                raise RuntimeError("Scheduler is already running.")
            self._config = config
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True, name="revenue-generator-bot")
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
        if thread:
            thread.join(timeout=5)
        with self._lock:
            self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> dict[str, Any]:
        with self._lock:
            cfg = asdict(self._config) if self._config else None
            return {
                "running": self.is_running(),
                "config": cfg,
                "last_result": self._last_result,
                "last_error": self._last_error,
                "last_run_at": self._last_run_at,
            }

    def run_once_now(self, config: RunnerConfig) -> dict[str, Any]:
        result = run_once(
            client=self.client,
            risk_policy=self.risk_policy,
            segment=config.segment,
            budget=config.budget,
            execute=config.execute,
        )
        self.journal.log_cycle(result)
        with self._lock:
            self._last_result = result
            self._last_error = None
            self._last_run_at = datetime.now(timezone.utc).isoformat()
        return result

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            cfg = self._config
            if not cfg:
                break
            try:
                result = self.run_once_now(cfg)
                with self._lock:
                    self._last_result = result
                    self._last_error = None
            except Exception as err:  # pragma: no cover
                with self._lock:
                    self._last_error = str(err)
                    self._last_run_at = datetime.now(timezone.utc).isoformat()

            sleep_for = max(cfg.interval_seconds, 5)
            for _ in range(sleep_for):
                if self._stop_event.is_set():
                    break
                time.sleep(1)
