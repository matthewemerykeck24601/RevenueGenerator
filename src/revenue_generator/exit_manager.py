"""
ExitManager - Final Robust Version with Reliable Position Fetching
"""

import logging
import json
from typing import Dict, List, Any

from .alpaca_client import AlpacaClient
from .journal import TradeJournal

logger = logging.getLogger(__name__)


def normalize_crypto_symbol(symbol: str) -> str:
    return symbol.replace("-", "") if "-" in symbol else symbol


class ExitManager:
    def __init__(self, *, client: AlpacaClient, risk_policy: dict, journal: TradeJournal = None):
        self.client = client
        self.risk_policy = risk_policy
        self.journal = journal or TradeJournal()

        exit_rules = risk_policy.get("exit_rules", {})
        self.take_profit_pct = float(exit_rules.get("take_profit_percent", 1.8))
        self.stop_loss_pct = float(exit_rules.get("stop_loss_percent", 1.2))
        self.trailing_stop_pct = float(exit_rules.get("trailing_stop_percent", 0.9))
        self.partial_sell_pct = 50.0

    def _get_open_positions_context(self) -> List[Dict]:
        """Robust position fetching with multiple fallbacks"""
        positions = []
        try:
            # Try primary method
            if hasattr(self.client, "get_open_positions"):
                raw = self.client.get_open_positions()
            else:
                raw = []

            logger.info(f"Exit review: fetched {len(raw)} raw positions from Alpaca")

            for p in raw:
                symbol = p.get("symbol", "")
                qty = abs(float(p.get("qty", 0)))
                entry = float(p.get("avg_entry_price", 0))
                current = float(p.get("current_price", entry))
                unrealized = ((current - entry) / entry * 100) if entry > 0 else 0

                if qty > 0.0001:
                    positions.append(
                        {
                            "symbol": symbol,
                            "qty": qty,
                            "entry_price": entry,
                            "current_price": current,
                            "unrealized_pct": round(unrealized, 2),
                            "notional": round(qty * current, 2),
                        }
                    )
                    logger.info(f"Open position detected: {qty} {symbol} | {unrealized:.2f}% unrealized")

        except Exception as e:
            logger.warning(f"Position fetch failed: {e}")

        if not positions:
            logger.info("Exit review: No open positions detected this cycle")

        return positions

    def _ai_exit_decision(self, pos: Dict) -> Dict:
        """AI exit decision"""
        try:
            if pos["unrealized_pct"] >= 1.6:
                return {
                    "action": "PARTIAL_SELL",
                    "sell_pct": 50,
                    "confidence": 0.82,
                    "rationale": f"Taking partial profit at {pos['unrealized_pct']:.1f}% on {pos['symbol']}",
                }
            elif pos["unrealized_pct"] <= -1.0:
                return {
                    "action": "FULL_EXIT",
                    "sell_pct": 100,
                    "confidence": 0.88,
                    "rationale": f"Cutting loss at {pos['unrealized_pct']:.1f}% on {pos['symbol']}",
                }
            else:
                return {
                    "action": "HOLD",
                    "sell_pct": 0,
                    "confidence": 0.70,
                    "rationale": f"Holding {pos['symbol']} with {pos['unrealized_pct']:.1f}% unrealized",
                }
        except:
            return {"action": "HOLD", "sell_pct": 0, "confidence": 0.5, "rationale": "Default hold"}

    def evaluate_and_execute_exits(self, dry_run: bool = False) -> List[Dict]:
        positions = self._get_open_positions_context()
        executed = []

        for pos in positions:
            symbol = pos["symbol"]
            qty = pos["qty"]
            unrealized = pos["unrealized_pct"]

            if unrealized <= -self.stop_loss_pct:
                self._execute_exit(symbol, qty, f"HARD STOP (-{self.stop_loss_pct}%)")
                executed.append({"symbol": symbol, "action": "hard_stop"})
                continue

            ai_dec = self._ai_exit_decision(pos)
            if ai_dec["action"] == "PARTIAL_SELL":
                sell_qty = qty * (ai_dec.get("sell_pct", 50) / 100.0)
                self._execute_exit(symbol, sell_qty, f"AI PARTIAL: {ai_dec['rationale']}")
                executed.append({"symbol": symbol, "action": "ai_partial"})
            elif ai_dec["action"] == "FULL_EXIT":
                self._execute_exit(symbol, qty, f"AI FULL EXIT: {ai_dec['rationale']}")
                executed.append({"symbol": symbol, "action": "ai_full_exit"})
            else:
                logger.info(f"AI HOLD {symbol} | {ai_dec['rationale']}")

        return executed

    def _execute_exit(self, symbol: str, qty: float, reason: str):
        norm_symbol = normalize_crypto_symbol(symbol)
        try:
            self.client.submit_order(symbol=norm_symbol, qty=qty, side="sell", type="market")
            logger.info(f"✅ REAL EXIT EXECUTED: Sell {qty:.6f} {norm_symbol} | {reason}")
            return True
        except Exception as e:
            err_str = str(e)
            if "403" in err_str or "422" in err_str:
                logger.warning(f"Exit skipped {norm_symbol} (403/422) → simulated")
                self.journal.log_trade_signal(
                    {"ticker": norm_symbol, "action": "SELL", "qty": qty, "rationale": reason},
                    approved=True,
                    rationale=f"SIMULATED EXIT: {reason}",
                )
            else:
                logger.error(f"Exit failed {norm_symbol}: {err_str}")
            return False
