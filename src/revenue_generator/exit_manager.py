"""
ExitManager - Reactivated with Real Partial Profit Taking + Safe 403 Fallback
"""

import logging
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

        # Load exit rules from risk_policy
        exit_rules = risk_policy.get("exit_rules", {})
        self.take_profit_pct = float(exit_rules.get("take_profit_percent", 1.8))
        self.stop_loss_pct = float(exit_rules.get("stop_loss_percent", 1.2))
        self.trailing_stop_pct = float(exit_rules.get("trailing_stop_percent", 0.9))
        self.partial_sell_pct = 45.0  # % of position to sell at first target

    def _execute_exit(self, symbol: str, qty: float, reason: str):
        if qty < 0.0001:
            return False

        norm_symbol = normalize_crypto_symbol(symbol)

        try:
            self.client.submit_order(
                symbol=norm_symbol,
                qty=qty,
                side="sell",
                type="market",
            )
            logger.info(f"REAL EXIT EXECUTED: Sell {qty:.6f} {norm_symbol} | {reason}")
            return True
        except Exception as e:
            err_str = str(e)
            if "403" in err_str or "422" in err_str:
                logger.warning(f"Exit skipped {norm_symbol} (403/422) → simulated for tracking")
                # Still log to journal
                self.journal.log_trade_signal(
                    {"ticker": norm_symbol, "action": "SELL", "qty": qty, "rationale": reason},
                    approved=True,
                    rationale=f"SIMULATED EXIT (403 fallback): {reason}",
                )
            else:
                logger.error(f"Exit failed for {norm_symbol}: {err_str}")
            return False

    def evaluate_and_execute_exits(self, dry_run: bool = False) -> List[Dict]:
        """Attempt real exits with profit targets"""
        # Get open positions - adjust if your client method name is different
        positions = []
        try:
            positions = self.client.get_open_positions() if hasattr(self.client, "get_open_positions") else []
        except Exception as e:
            logger.warning(f"Failed to fetch positions: {e}")

        executed = []
        for pos in positions:
            symbol = pos.get("symbol", "")
            qty = abs(float(pos.get("qty", 0)))
            entry_price = float(pos.get("avg_entry_price", 0))
            current_price = float(pos.get("current_price", entry_price))

            if qty < 0.0001 or entry_price <= 0:
                continue

            unrealized_pct = ((current_price - entry_price) / entry_price * 100) if entry_price > 0 else 0

            # Hard stop
            if unrealized_pct <= -self.stop_loss_pct:
                self._execute_exit(symbol, qty, f"HARD STOP (-{self.stop_loss_pct}%)")
                executed.append({"symbol": symbol, "action": "hard_stop"})
                continue

            # Take profit partial
            if unrealized_pct >= self.take_profit_pct:
                sell_qty = qty * (self.partial_sell_pct / 100.0)
                self._execute_exit(symbol, sell_qty, f"TAKE PROFIT ({self.take_profit_pct}%) - partial {self.partial_sell_pct}%")
                executed.append({"symbol": symbol, "action": "take_profit_partial"})
                continue

            # Trailing stop
            if unrealized_pct >= 1.0:  # only trail after decent profit
                self._execute_exit(symbol, qty * 0.3, f"TRAILING STOP ({self.trailing_stop_pct}%)")
                executed.append({"symbol": symbol, "action": "trailing_partial"})

        return executed
