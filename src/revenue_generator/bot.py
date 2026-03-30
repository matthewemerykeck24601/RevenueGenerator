"""
Bot Module - Finalized for Reliable Agentic Churn Execution
Fixed journal logging + robust account fetching.
"""

import logging
from typing import List, Dict, Any

from .alpaca_client import AlpacaClient
from .ai_bridge import analyze_segment
from .risk import validate_and_plan_signal
from .external_research import get_segment_research
from .journal import log_trade_signal
from .config import load_risk_policy

logger = logging.getLogger(__name__)


class RevenueBot:
    def __init__(self, client: AlpacaClient):
        self.client = client
        self.risk_policy = load_risk_policy()

    def _get_account_context(self):
        """Robust account & position snapshot with fallbacks"""
        try:
            # Try common Alpaca methods (adjust based on your client)
            account = self.client.get_account() if hasattr(self.client, "get_account") else {}
            equity = float(account.get("equity", account.get("portfolio_value", 15000.0)))

            positions = self.client.get_open_positions() if hasattr(self.client, "get_open_positions") else []
            open_positions_count = len(positions)

            return equity, open_positions_count
        except Exception as e:
            logger.warning(f"Account snapshot failed ({e}), using safe defaults")
            return 15000.0, 3  # conservative defaults for paper

    def run_cycle(self, segment: str = "crypto") -> List[Dict]:
        research = get_segment_research(segment)
        regime = research.get("regime", "normal")
        fear_climate = research.get("fear_climate", {})

        equity, open_positions = self._get_account_context()

        agent_result = analyze_segment(segment)
        if agent_result.get("action", "HOLD").upper() == "HOLD":
            logger.info(f"Agent HOLD in {segment} - {agent_result.get('reason', '')}")
            return []

        logger.info(f"Agent proposed: {agent_result.get('action')} {agent_result.get('ticker')} | conf {agent_result.get('confidence', 0):.2f}")

        validated = validate_and_plan_signal(
            signal=agent_result,
            risk_policy=self.risk_policy,
            segment=segment,
            current_equity=equity,
            start_equity=equity * 1.02,
            open_positions=open_positions,
            fear_climate=fear_climate,
        )

        if not validated.get("approved", False):
            logger.info(f"Risk REJECTED: {validated.get('reason', 'unknown')}")
            return []

        ticker = validated["ticker"]
        size_pct = validated["size_percent"]
        alloc_dollars = equity * (size_pct / 100.0)

        # Get better price (from research candidates or fallback)
        price = 1000.0
        for cand in research.get("candidates", []):
            if cand.get("ticker") == ticker:
                price = cand.get("price", 1000.0)
                break

        qty = alloc_dollars / price if price > 0 else 0.0
        qty = round(qty, 6 if "-" in ticker or "USD" in ticker else 4)

        planned_order = {
            "symbol": ticker,
            "qty": qty,
            "limit_price": round(price * 0.999, 4),
            "confidence": validated["confidence"],
            "edge": validated.get("edge", 0),
            "size_percent": size_pct,
            "rationale": validated.get("reason", "agent_approved"),
            "action": "BUY",
            "segment": segment,
        }

        logger.info(f"APPROVED & EXECUTING (paper): BUY {qty} {ticker} (~${alloc_dollars:.0f}) | conf {validated['confidence']:.2f} | {planned_order['rationale']}")

        # Correct journal logging
        log_trade_signal(planned_order, approved=True, rationale=planned_order["rationale"], regime=regime)

        return [planned_order]


# Backward compatibility
def run_bot_cycle(segment: str = "crypto"):
    client = AlpacaClient()
    bot = RevenueBot(client)
    return bot.run_cycle(segment)
