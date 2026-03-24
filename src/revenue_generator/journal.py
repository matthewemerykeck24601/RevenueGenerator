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


def _realized_edge_from_order(order: dict[str, Any]) -> float | None:
    limit_v = _to_float(order.get("limit_price"))
    fill_v = _to_float(order.get("filled_avg_price"))
    side = str(order.get("side", "")).lower()
    if limit_v is None or fill_v is None or limit_v <= 0:
        return None
    if side == "sell":
        return (fill_v - limit_v) / limit_v
    return (limit_v - fill_v) / limit_v


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

        trade_edge_csv_path = self.folder / "trade_edges.csv"
        if not trade_edge_csv_path.exists():
            with open(trade_edge_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "segment",
                        "strategy",
                        "symbol",
                        "execute",
                        "predicted_edge",
                        "predicted_edge_net",
                        "estimated_cost_pct",
                        "realized_edge",
                        "edge_error",
                        "planned_allocation",
                        "planned_qty",
                        "placed_qty",
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
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_edges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    segment TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    execute INTEGER NOT NULL,
                    predicted_edge REAL,
                    predicted_edge_net REAL,
                    estimated_cost_pct REAL,
                    realized_edge REAL,
                    edge_error REAL,
                    planned_allocation REAL,
                    planned_qty REAL,
                    placed_qty REAL
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
            r_edge = _realized_edge_from_order(p)
            if r_edge is None:
                continue
            realized_edges.append(r_edge)
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

            # Per-trade telemetry for predicted vs realized edge.
            trade_edge_csv_path = self.folder / "trade_edges.csv"
            placed_by_symbol: dict[str, list[dict[str, Any]]] = {}
            for p in placed:
                if not isinstance(p, dict):
                    continue
                sym = str(p.get("symbol") or "")
                if not sym:
                    continue
                placed_by_symbol.setdefault(sym, []).append(p)

            for p in planned:
                if not isinstance(p, dict):
                    continue
                symbol = str(p.get("symbol") or "")
                if not symbol:
                    continue
                predicted_edge = _to_float(p.get("expected_edge"))
                predicted_edge_net = _to_float(p.get("expected_edge_net"))
                estimated_cost_pct = _to_float(p.get("estimated_cost_pct"))
                planned_allocation = _to_float(p.get("allocation"))
                planned_qty = _to_float(p.get("qty"))

                placed_match = None
                if symbol in placed_by_symbol and placed_by_symbol[symbol]:
                    placed_match = placed_by_symbol[symbol].pop(0)

                placed_qty = _to_float(placed_match.get("qty")) if isinstance(placed_match, dict) else None
                realized_edge = _realized_edge_from_order(placed_match) if isinstance(placed_match, dict) else None
                edge_error_trade = None
                if predicted_edge is not None and realized_edge is not None:
                    edge_error_trade = realized_edge - predicted_edge

                with open(trade_edge_csv_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        [
                            ts,
                            segment,
                            strategy,
                            symbol,
                            execute,
                            "" if predicted_edge is None else f"{predicted_edge:.6f}",
                            "" if predicted_edge_net is None else f"{predicted_edge_net:.6f}",
                            "" if estimated_cost_pct is None else f"{estimated_cost_pct:.6f}",
                            "" if realized_edge is None else f"{realized_edge:.6f}",
                            "" if edge_error_trade is None else f"{edge_error_trade:.6f}",
                            "" if planned_allocation is None else f"{planned_allocation:.6f}",
                            "" if planned_qty is None else f"{planned_qty:.6f}",
                            "" if placed_qty is None else f"{placed_qty:.6f}",
                        ]
                    )

                con.execute(
                    """
                    INSERT INTO trade_edges (
                        ts, segment, strategy, symbol, execute,
                        predicted_edge, predicted_edge_net, estimated_cost_pct,
                        realized_edge, edge_error, planned_allocation, planned_qty, placed_qty
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ts,
                        segment,
                        strategy,
                        symbol,
                        execute,
                        predicted_edge,
                        predicted_edge_net,
                        estimated_cost_pct,
                        realized_edge,
                        edge_error_trade,
                        planned_allocation,
                        planned_qty,
                        placed_qty,
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
