"""
Bot Module - Agentic Execution for Daily Profit Churn
Integrates new AI bridge, risk validation, and higher-turnover logic.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Dict
import logging

from .alpaca_client import AlpacaClient
from .ai_bridge import analyze_segment
from .risk import validate_and_plan_signal
from .strategy import select_top_signals
from .external_research import get_segment_research
from .journal import TradeJournal, log_trade_signal
from .config import load_risk_policy
from .exit_manager import ExitManager  # keep for now

logger = logging.getLogger(__name__)  # assume logging is imported elsewhere or add


@dataclass
class PlannedOrder:
    symbol: str
    qty: float
    limit_price: float
    confidence: float
    expected_edge: float
    size_percent: float
    rationale: str


class RevenueBot:
    def __init__(self, client: AlpacaClient, crypto_client=None):
        self.client = client
        self.crypto_client = crypto_client
        self.risk_policy = load_risk_policy()
        self.exit_manager = ExitManager(client=client, risk_policy=self.risk_policy, journal=TradeJournal())  # journal later

    def run_cycle(self, segment: str = "crypto") -> List[Dict]:
        """Main agentic cycle: research → agent analyze → risk gate → execute"""
        research = get_segment_research(segment)
        regime = research.get("regime", "normal")

        # Agent proposes (now tool-calling)
        agent_result = analyze_segment(segment)
        if agent_result.get("action") == "HOLD":
            logger.info(f"HOLD in {segment} - {agent_result.get('reason')}")
            return []

        # Legacy strategy fallback + agent enhancement (for richer signals)
        bars = {}  # In full version, fetch real bars via alpaca/yfinance
        top_signals = select_top_signals(bars, segment=segment, regime=regime, top_n=6)

        planned = []
        for sig in top_signals:
            # Full risk validation
            validated = validate_and_plan_signal(
                {
                    "action": "BUY",
                    "ticker": sig.symbol,
                    "confidence": sig.confidence,
                    "edge_percent": sig.expected_edge,
                    "size_percent": 0.0,  # let risk decide
                    "rationale": sig.rationale
                },
                self.risk_policy,
                segment=segment,
                fear_climate=research.get("fear_climate")
            )

            if not validated.get("approved"):
                continue

            # Calculate actual qty (simplified)
            equity = float(self.client.get_account().get("equity", 10000) or 10000)
            alloc_dollars = equity * (validated["size_percent"] / 100.0)
            qty = alloc_dollars / sig.last_price if sig.last_price > 0 else 0

            if qty > 0:
                planned.append({
                    "symbol": sig.symbol,
                    "qty": round(qty, 6 if "USD" in sig.symbol else 4),
                    "limit_price": round(sig.last_price * 0.998, 4),  # slight limit discount
                    "confidence": validated["confidence"],
                    "edge": validated["edge"],
                    "size_percent": validated["size_percent"],
                    "rationale": validated["reason"]
                })

        # Execute approved (paper only for now)
        for order in planned[:4]:  # cap per cycle
            try:
                # self.client.submit_order(...)  # uncomment when ready
                logger.info(f"EXECUTING (paper): BUY {order['qty']} {order['symbol']} @ ~{order['limit_price']} | {order['rationale']}")
                log_trade_signal(order, True, rationale=order["rationale"])
            except Exception as e:
                logger.error(f"Order failed: {e}")

        return planned


# Backward compatibility
def run_bot_cycle(segment: str = "crypto"):
    # Instantiate with your client in scripts
    pass  # runners will adapt


def run_once(
    *,
    client: AlpacaClient,
    risk_policy: dict[str, Any],
    segment: str,
    budget: float,
    execute: bool,
) -> dict[str, Any]:
    """Compatibility API used by scheduler/web UI."""
    bot = RevenueBot(client=client)
    bot.risk_policy = risk_policy
    planned = bot.run_cycle(segment=segment)
    return {
        "strategy": "agentic",
        "account_status": "ACTIVE",
        "segment": segment,
        "budget": budget,
        "execute": execute,
        "signals_considered": len(planned),
        "orders_planned": planned,
        "orders_placed": [],
        "order_errors": [],
    }
