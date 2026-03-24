from __future__ import annotations

import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _to_float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class TradeJournal:
    def __init__(self, folder: str = "logs") -> None:
        self.folder = Path(folder)
        self.folder.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.folder / "cycles.csv"
        self.db_path = self.folder / "trades.db"
        self._init_db()
        self._init_csv()

    def _init_csv(self) -> None:
        if self.csv_path.exists():
            pass
        else:
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "segment",
                        "budget",
                        "execute",
                        "account_status",
                        "signals_considered",
                        "orders_planned_count",
                        "orders_placed_count",
                        "predicted_edge_avg",
                        "realized_edge_avg",
                        "edge_error",
                        "notes",
                    ]
                )

        exit_csv_path = self.folder / "exit_actions.csv"
        if not exit_csv_path.exists():
            with open(exit_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "symbol",
                        "trigger",
                        "qty",
                        "price",
                        "pnl_pct",
                        "execute",
                        "result",
                    ]
                )

    def _init_db(self) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS cycles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    segment TEXT NOT NULL,
                    budget REAL NOT NULL,
                    execute INTEGER NOT NULL,
                    account_status TEXT,
                    signals_considered INTEGER NOT NULL,
                    orders_planned_json TEXT NOT NULL,
                    orders_placed_json TEXT NOT NULL,
                    notes TEXT,
                    strategy TEXT,
                    order_errors_json TEXT,
                    raw_result_json TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS exit_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    pnl_pct REAL NOT NULL,
                    execute INTEGER NOT NULL,
                    result_json TEXT NOT NULL
                )
                """
            )
            self._ensure_column(con, "cycles", "strategy", "TEXT")
            self._ensure_column(con, "cycles", "order_errors_json", "TEXT")
            self._ensure_column(con, "cycles", "raw_result_json", "TEXT")
            self._ensure_column(con, "cycles", "predicted_edge_avg", "REAL")
            self._ensure_column(con, "cycles", "realized_edge_avg", "REAL")
            self._ensure_column(con, "cycles", "edge_error", "REAL")
            con.commit()
        finally:
            con.close()

    def _ensure_column(self, con: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        cols = {str(r[1]) for r in rows}
        if column not in cols:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")

    def log_cycle(self, result: dict[str, Any]) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        segment = str(result.get("segment", "unknown"))
        budget = float(result.get("budget", 0.0))
        execute = 1 if bool(result.get("execute", False)) else 0
        account_status = str(result.get("account_status", "unknown"))
        signals_considered = int(result.get("signals_considered", 0))
        planned = result.get("orders_planned", [])
        placed = result.get("orders_placed", [])
        order_errors = result.get("order_errors", [])
        predicted_edges = [_to_float(p.get("expected_edge")) for p in planned if isinstance(p, dict)]
        predicted_edges = [v for v in predicted_edges if v is not None]
        predicted_edge_avg = (sum(predicted_edges) / len(predicted_edges)) if predicted_edges else None

        realized_edges: list[float] = []
        for p in placed:
            if not isinstance(p, dict):
                continue
            limit_v = _to_float(p.get("limit_price"))
            fill_v = _to_float(p.get("filled_avg_price"))
            side = str(p.get("side", "")).lower()
            if limit_v is None or fill_v is None or limit_v <= 0:
                continue
            if side == "sell":
                realized_edges.append((fill_v - limit_v) / limit_v)
            else:
                realized_edges.append((limit_v - fill_v) / limit_v)
        realized_edge_avg = (sum(realized_edges) / len(realized_edges)) if realized_edges else None
        edge_error = None
        if predicted_edge_avg is not None and realized_edge_avg is not None:
            edge_error = realized_edge_avg - predicted_edge_avg

        strategy = str(
            result.get("strategy")
            or (
                "ai_direct"
                if bool(result.get("ai_used"))
                else ("rule_fallback_from_ai" if bool(result.get("ai_fallback_used")) else "rule_engine")
            )
        )
        notes = str(result.get("reason", ""))

        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    ts,
                    segment,
                    budget,
                    execute,
                    account_status,
                    signals_considered,
                    len(planned),
                    len(placed),
                    "" if predicted_edge_avg is None else f"{predicted_edge_avg:.6f}",
                    "" if realized_edge_avg is None else f"{realized_edge_avg:.6f}",
                    "" if edge_error is None else f"{edge_error:.6f}",
                    notes,
                ]
            )

        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """
                INSERT INTO cycles (
                    ts, segment, budget, execute, account_status, signals_considered,
                    orders_planned_json, orders_placed_json, notes, strategy, order_errors_json, raw_result_json,
                    predicted_edge_avg, realized_edge_avg, edge_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    segment,
                    budget,
                    execute,
                    account_status,
                    signals_considered,
                    json.dumps(planned),
                    json.dumps(placed),
                    notes,
                    strategy,
                    json.dumps(order_errors),
                    json.dumps(result),
                    predicted_edge_avg,
                    realized_edge_avg,
                    edge_error,
                ),
            )
            con.commit()
        finally:
            con.close()

    def log_exit_action(
        self,
        *,
        symbol: str,
        trigger: str,
        qty: float,
        price: float,
        pnl_pct: float,
        execute: bool,
        result: dict[str, Any],
    ) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        exit_csv_path = self.folder / "exit_actions.csv"
        with open(exit_csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    ts,
                    symbol,
                    trigger,
                    f"{qty:.6f}",
                    f"{price:.6f}",
                    f"{pnl_pct:.4f}",
                    1 if execute else 0,
                    json.dumps(result, separators=(",", ":")),
                ]
            )

        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """
                INSERT INTO exit_actions (ts, symbol, trigger, qty, price, pnl_pct, execute, result_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    symbol,
                    trigger,
                    qty,
                    price,
                    pnl_pct,
                    1 if execute else 0,
                    json.dumps(result),
                ),
            )
            con.commit()
        finally:
            con.close()
