from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .alpaca_client import AlpacaClient
from .bot import run_once
from .journal import TradeJournal


@dataclass
class RunnerConfig:
    segment: str
    execute: bool
    interval_seconds: int
    budget: float = 0.0
    budget_mode: str = "dynamic"


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
        self._reserve_state_path = Path(__file__).resolve().parents[2] / "logs" / "reserve_state.json"

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
        effective_budget, budget_meta = self._resolve_budget(config)
        result = run_once(
            client=self.client,
            risk_policy=self.risk_policy,
            segment=config.segment,
            budget=effective_budget,
            execute=config.execute,
        )
        result["budget_mode"] = config.budget_mode
        result["budget_input"] = config.budget
        result["budget_effective"] = effective_budget
        if budget_meta:
            result["budget_meta"] = budget_meta
        self.journal.log_cycle(result)
        with self._lock:
            self._last_result = result
            self._last_error = None
            self._last_run_at = datetime.now(timezone.utc).isoformat()
        return result

    def _to_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _load_reserve_state(self) -> dict[str, float]:
        if not self._reserve_state_path.exists():
            return {"reserve_balance": 0.0, "reserve_target_request": 0.0}
        try:
            raw = json.loads(self._reserve_state_path.read_text(encoding="utf-8"))
            return {
                "reserve_balance": max(self._to_float(raw.get("reserve_balance")), 0.0),
                "reserve_target_request": max(self._to_float(raw.get("reserve_target_request")), 0.0),
            }
        except Exception:
            return {"reserve_balance": 0.0, "reserve_target_request": 0.0}

    def _save_reserve_state(self, reserve_balance: float, reserve_target_request: float) -> None:
        self._reserve_state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "reserve_balance": round(max(reserve_balance, 0.0), 6),
            "reserve_target_request": round(max(reserve_target_request, 0.0), 6),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._reserve_state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def _resolve_budget(self, config: RunnerConfig) -> tuple[float, dict[str, Any]]:
        if str(config.budget_mode).lower() == "fixed":
            return max(config.budget, 0.0), {"mode": "fixed"}

        account = self.client.get_account()
        cash = max(self._to_float(account.get("cash")), 0.0)
        equity = max(self._to_float(account.get("equity")), 0.0)
        reserve_state = self._load_reserve_state()
        reserve_balance = reserve_state["reserve_balance"]
        reserve_target_request = reserve_state["reserve_target_request"]

        # Sync reserve state with current cash so scheduler behavior matches dashboard behavior.
        changed = False
        bounded_balance = min(reserve_balance, cash)
        if bounded_balance != reserve_balance:
            reserve_balance = bounded_balance
            changed = True
        if reserve_target_request > reserve_balance:
            free_cash = max(cash - reserve_balance, 0.0)
            fill = min(reserve_target_request - reserve_balance, free_cash)
            if fill > 0:
                reserve_balance += fill
                changed = True
            if reserve_balance >= reserve_target_request:
                reserve_target_request = 0.0
                changed = True
        if changed:
            self._save_reserve_state(reserve_balance, reserve_target_request)

        deployable_cash = max(cash - reserve_balance, 0.0)
        deployable_equity = max(equity - reserve_balance, 0.0)
        # Trading cycle budget should track available deployable cash.
        return deployable_cash, {
            "mode": "dynamic",
            "cash": cash,
            "equity": equity,
            "reserve_balance": reserve_balance,
            "reserve_target_request": reserve_target_request,
            "deployable_cash": deployable_cash,
            "deployable_equity": deployable_equity,
        }

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
