"""
Journal Module - Persistent Memory for Agentic Trader
Enables short-term memory (last trades) for the AI agent + trade logging for review.
Supports daily churn attribution.
"""

import sqlite3
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

DB_PATH = Path("logs/trades.db")
CSV_PATH = Path("logs/trades.csv")
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)


class TradeJournal:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self._init_db()
        self._init_csv()

    def _init_db(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                segment TEXT,
                ticker TEXT,
                action TEXT,
                qty REAL,
                price REAL,
                size_percent REAL,
                confidence REAL,
                edge_percent REAL,
                approved BOOLEAN,
                rationale TEXT,
                outcome TEXT DEFAULT 'open',
                realized_pnl REAL DEFAULT 0.0,
                regime TEXT
            )
        """
        )
        self.conn.commit()

    def _init_csv(self):
        if not CSV_PATH.exists():
            with open(CSV_PATH, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "segment",
                        "ticker",
                        "action",
                        "qty",
                        "price",
                        "size_percent",
                        "confidence",
                        "edge_percent",
                        "approved",
                        "rationale",
                        "outcome",
                        "realized_pnl",
                        "regime",
                    ]
                )

    def log_trade_signal(
        self,
        signal: Dict[str, Any],
        approved: bool,
        rationale: str = "",
        outcome: str = "open",
        realized_pnl: float = 0.0,
        regime: str = "normal",
    ):
        """Log agent proposal + risk decision"""
        ts = datetime.utcnow().isoformat()
        ticker = signal.get("ticker", "")
        segment = signal.get("segment", "crypto")
        action = signal.get("action", "HOLD")
        qty = signal.get("qty", 0.0)
        price = signal.get("price", 0.0)
        size_pct = signal.get("size_percent", 0.0)
        conf = signal.get("confidence", 0.0)
        edge = signal.get("edge_percent", 0.0)

        # SQLite
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO trades
            (timestamp, segment, ticker, action, qty, price, size_percent, confidence,
             edge_percent, approved, rationale, outcome, realized_pnl, regime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (ts, segment, ticker, action, qty, price, size_pct, conf, edge, approved, rationale, outcome, realized_pnl, regime),
        )
        self.conn.commit()

        # CSV append
        with open(CSV_PATH, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([ts, segment, ticker, action, qty, price, size_pct, conf, edge, approved, rationale, outcome, realized_pnl, regime])

        logger.info(f"Journal: {action} {ticker} | approved={approved} | conf={conf:.2f} | regime={regime} | {rationale[:80]}...")

    def get_recent_trades(self, limit: int = 10) -> List[Dict]:
        """Get last N trades for agent memory (short-term context)"""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT timestamp, segment, ticker, action, confidence, edge_percent,
                   approved, rationale, outcome, realized_pnl, regime
            FROM trades
            ORDER BY timestamp DESC
            LIMIT ?
        """,
            (limit,),
        )
        rows = cursor.fetchall()
        return [
            {
                "timestamp": r[0],
                "segment": r[1],
                "ticker": r[2],
                "action": r[3],
                "confidence": r[4],
                "edge": r[5],
                "approved": bool(r[6]),
                "rationale": r[7],
                "outcome": r[8],
                "pnl": r[9],
                "regime": r[10],
            }
            for r in rows
        ]

    def get_recent_journal(self, limit: int = 5) -> List[Dict]:
        """Alias for agent memory calls"""
        return self.get_recent_trades(limit)

    def close(self):
        self.conn.close()

    def log_cycle(self, result: Dict[str, Any]):
        """Backward-compatible cycle logger for scheduler/UI callers."""
        planned = result.get("orders_planned", []) if isinstance(result, dict) else []
        segment = str(result.get("segment", "unknown")) if isinstance(result, dict) else "unknown"
        for order in planned if isinstance(planned, list) else []:
            if not isinstance(order, dict):
                continue
            signal = {
                "ticker": order.get("symbol", ""),
                "segment": segment,
                "action": "BUY",
                "qty": order.get("qty", 0.0),
                "price": order.get("limit_price", 0.0),
                "size_percent": order.get("size_percent", 0.0),
                "confidence": order.get("confidence", 0.0),
                "edge_percent": order.get("edge", order.get("expected_edge", 0.0)),
            }
            self.log_trade_signal(signal, approved=True, rationale=str(order.get("rationale", "cycle_order")))


# Global instance (singleton style)
journal = TradeJournal()


# Public API functions (for backward compatibility in other modules)
def log_trade_signal(signal: Dict, approved: bool, rationale: str = "", **kwargs):
    journal.log_trade_signal(signal, approved, rationale, **kwargs)


def get_recent_trades(limit: int = 10):
    return journal.get_recent_trades(limit)


def get_recent_journal(limit: int = 5):
    return journal.get_recent_journal(limit)
