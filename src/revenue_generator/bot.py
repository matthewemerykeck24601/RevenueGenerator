"""
Bot Module - Fixed Real Order Submission + Accurate Journal Logging
"""

import logging
from typing import List, Dict, Any

from .alpaca_client import AlpacaClient
from .ai_bridge import analyze_segment
from .risk import validate_and_plan_signal
from .external_research import get_segment_research
from .journal import log_trade_signal
from .config import load_risk_policy, build_runtime_config

logger = logging.getLogger(__name__)


class RevenueBot:
    def __init__(self, client: AlpacaClient):
        self.client = client
        self.risk_policy = load_risk_policy()

    def _get_account_context(self):
        try:
            account = self.client.get_account() if hasattr(self.client, 'get_account') else {}
            equity = float(account.get("equity", account.get("portfolio_value", 15000.0)))
            positions = self.client.get_open_positions() if hasattr(self.client, 'get_open_positions') else []
            return equity, len(positions)
        except Exception as e:
            logger.warning(f"Account snapshot failed: {e}")
            return 15000.0, 0

    def run_cycle(self, segment: str = "crypto") -> List[Dict]:
        research = get_segment_research(segment)
        regime = research.get("regime", "normal")
        fear_climate = research.get("fear_climate", {})

        equity, open_positions = self._get_account_context()

        agent_result = analyze_segment(segment)

        if agent_result.get("action", "HOLD").upper() != "BUY":
            logger.info(f"Agent did not propose BUY in {segment}")
            return []

        raw_edge = agent_result.get("edge_percent", 0.0)
        logger.info(f"RAW from agent: BUY {agent_result.get('ticker')} | conf {agent_result.get('confidence', 0):.2f} | edge {raw_edge:.2f}%")

        signal_for_risk = {
            "action": "BUY",
            "ticker": agent_result.get("ticker"),
            "confidence": float(agent_result.get("confidence", 0.0)),
            "edge_percent": float(raw_edge),
            "size_percent": float(agent_result.get("size_percent", 5.0)),
            "rationale": agent_result.get("rationale", "agent_proposal")
        }

        validated = validate_and_plan_signal(
            signal=signal_for_risk,
            risk_policy=self.risk_policy,
            segment=segment,
            current_equity=equity,
            start_equity=equity * 1.02,
            open_positions=open_positions,
            fear_climate=fear_climate
        )

        if not validated.get("approved", False):
            logger.info(f"Risk REJECTED: {validated.get('reason', 'unknown')}")
            return []

        ticker = validated["ticker"]
        size_pct = validated.get("size_percent", 5.0)
        alloc_dollars = equity * (size_pct / 100.0)

        price = 1000.0
        for cand in research.get("candidates", []):
            if cand.get("ticker") == ticker or cand.get("ticker", "").replace("-", "") == ticker.replace("-", ""):
                price = cand.get("price", 1000.0)
                break

        qty = round(alloc_dollars / price, 6 if "-" in ticker else 4) if price > 0 else 0.0

        planned_order = {
            "symbol": ticker,
            "qty": qty,
            "limit_price": round(price * 0.999, 4),
            "confidence": validated["confidence"],
            "edge": signal_for_risk["edge_percent"],
            "size_percent": size_pct,
            "rationale": validated.get("reason", "approved"),
            "action": "BUY",
            "segment": segment
        }

        # REAL PAPER ORDER SUBMISSION
        success = False
        try:
            order_resp = self.client.submit_order(
                symbol=ticker,
                qty=qty,
                side="buy",
                type="market"
            )
            broker_symbol = order_resp.get("symbol", ticker) if isinstance(order_resp, dict) else ticker
            broker_order_id = order_resp.get("id", "") if isinstance(order_resp, dict) else ""
            broker_status = order_resp.get("status", "") if isinstance(order_resp, dict) else ""
            planned_order["symbol"] = broker_symbol
            planned_order["broker_order_id"] = broker_order_id
            planned_order["broker_status"] = broker_status
            logger.info(
                f"✅ REAL PAPER BUY SUBMITTED: {qty} {broker_symbol} (~${alloc_dollars:.0f}) "
                f"| conf {validated['confidence']:.2f} | edge {planned_order['edge']:.2f}% | "
                f"order_id={broker_order_id} status={broker_status}"
            )
            success = True
        except Exception as e:
            logger.error(f"Failed to submit BUY order for {ticker}: {e}")
            planned_order["rationale"] = f"Order failed: {str(e)[:100]}"

        # Log to journal ONLY after attempting the real order
        log_trade_signal(planned_order, approved=success, rationale=planned_order['rationale'], regime=regime)

        return [planned_order] if success else []


def run_bot_cycle(segment: str = "crypto"):
    client = AlpacaClient(cfg=build_runtime_config())
    bot = RevenueBot(client)
    return bot.run_cycle(segment)
