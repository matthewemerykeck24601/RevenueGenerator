from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .alpaca_client import AlpacaClient
from .journal import TradeJournal


@dataclass
class ExitConfig:
    first_target_pct: float = 3.0
    first_sell_pct: float = 40.0
    second_target_pct: float = 6.0
    second_sell_pct: float = 30.0
    trailing_stop_pct: float = 2.0
    break_even_buffer_pct: float = 0.1
    hard_stop_loss_pct: float = 2.4
    min_notional_exit_usd: float = 5.0


def _is_crypto_symbol(symbol: str) -> bool:
    return "/" in symbol


def _position_side_qty(position: dict[str, Any]) -> float:
    return abs(float(position.get("qty", "0") or 0))


def _parse_price(snapshot: dict[str, Any]) -> float | None:
    latest_trade = snapshot.get("latestTrade", {})
    p = latest_trade.get("p")
    if p is not None:
        return float(p)
    daily_bar = snapshot.get("dailyBar", {})
    if daily_bar.get("c") is not None:
        return float(daily_bar["c"])
    return None


class ExitManager:
    def __init__(self, *, client: AlpacaClient, risk_policy: dict[str, Any], journal: TradeJournal, state_path: str = "logs/exit_state.json") -> None:
        self.client = client
        self.risk_policy = risk_policy
        self.journal = journal
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state: dict[str, Any] = self._load_state()
        hooks = risk_policy.get("exitHooks", {})
        self.cfg = ExitConfig(
            first_target_pct=float(hooks.get("firstTargetPct", 3.0)),
            first_sell_pct=float(hooks.get("firstTargetSellPct", 40.0)),
            second_target_pct=float(hooks.get("secondTargetPct", 6.0)),
            second_sell_pct=float(hooks.get("secondTargetSellPct", 30.0)),
            trailing_stop_pct=float(hooks.get("trailingStopPct", 2.0)),
            break_even_buffer_pct=float(hooks.get("breakEvenBufferPct", 0.1)),
            hard_stop_loss_pct=float(hooks.get("hardStopLossPct", risk_policy.get("stopLossPct", 2.4))),
            min_notional_exit_usd=float(hooks.get("minNotionalExitUsd", 5.0)),
        )

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"symbols": {}}
        with open(self.state_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_state(self) -> None:
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)

    def _price_map_for_positions(self, positions: list[dict[str, Any]]) -> dict[str, float]:
        stock_symbols = [p["symbol"] for p in positions if not _is_crypto_symbol(p["symbol"])]
        crypto_symbols = [p["symbol"] for p in positions if _is_crypto_symbol(p["symbol"])]
        prices: dict[str, float] = {}
        if stock_symbols:
            snapshots = self.client.get_stock_snapshots(stock_symbols)
            for sym in stock_symbols:
                snap = snapshots.get(sym, {})
                p = _parse_price(snap)
                if p is not None:
                    prices[sym] = p
        if crypto_symbols:
            prices.update(self.client.get_crypto_latest_prices(crypto_symbols))
        return prices

    def _sell_qty(self, symbol: str, total_qty: float, sell_pct: float) -> float:
        raw = total_qty * (sell_pct / 100.0)
        if _is_crypto_symbol(symbol):
            qty = round(raw, 6)
            return max(qty, 0.0)
        qty_int = int(raw)
        if qty_int < 1 and total_qty >= 1:
            qty_int = 1
        return float(qty_int)

    def _remaining_qty(self, symbol: str, total_qty: float, planned_qty: float) -> float:
        remaining = total_qty - planned_qty
        if _is_crypto_symbol(symbol):
            return max(round(remaining, 6), 0.0)
        return float(max(int(remaining), 0))

    def run_cycle(self, *, execute: bool) -> dict[str, Any]:
        positions = self.client.get_open_positions()
        if not positions:
            return {"positions": 0, "actions": [], "execute": execute}

        pdt_cfg = (self.risk_policy.get("pdtGuard") or {})
        pdt_guard_enabled = bool(pdt_cfg.get("enabled", True))
        pdt_threshold = int(pdt_cfg.get("thresholdDayTrades", 3))
        pdt_allow_only_hard_stop = bool(pdt_cfg.get("allowOnlyHardStopAtThreshold", True))
        account = self.client.get_account() if pdt_guard_enabled else {}
        daytrade_count = int(float(account.get("daytrade_count", 0) or 0)) if pdt_guard_enabled else 0
        bought_today_equities: set[str] = set()
        if pdt_guard_enabled:
            try:
                today_utc = datetime.now(timezone.utc).date()
                recent_orders = self.client.get_orders(status="all", limit=500, direction="desc")
                for order in recent_orders:
                    if str(order.get("side", "")).lower() != "buy":
                        continue
                    symbol = str(order.get("symbol") or "")
                    if not symbol or _is_crypto_symbol(symbol):
                        continue
                    ts = order.get("filled_at") or order.get("submitted_at")
                    if not ts:
                        continue
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    if dt.date() == today_utc:
                        bought_today_equities.add(symbol)
            except Exception:
                bought_today_equities = set()

        prices = self._price_map_for_positions(positions)
        symbols_state = self.state.setdefault("symbols", {})
        actions: list[dict[str, Any]] = []
        pdt_blocked_exits = 0

        for pos in positions:
            symbol = pos["symbol"]
            if symbol not in prices:
                continue
            entry = float(pos.get("avg_entry_price", "0") or 0)
            if entry <= 0:
                continue

            qty = _position_side_qty(pos)
            current = prices[symbol]
            pnl_pct = ((current - entry) / entry) * 100.0
            symbol_state = symbols_state.setdefault(
                symbol,
                {
                    "peak_price": current,
                    "first_target_hit": False,
                    "second_target_hit": False,
                },
            )
            symbol_state["peak_price"] = max(float(symbol_state.get("peak_price", current)), current)
            peak = float(symbol_state["peak_price"])
            peak_drop_pct = ((peak - current) / peak * 100.0) if peak > 0 else 0.0

            trigger: str | None = None
            sell_qty = 0.0

            if pnl_pct <= -self.cfg.hard_stop_loss_pct:
                trigger = "hard_stop_loss"
                sell_qty = qty
            elif symbol_state.get("first_target_hit") and pnl_pct <= self.cfg.break_even_buffer_pct:
                trigger = "break_even_exit"
                sell_qty = qty
            elif peak_drop_pct >= self.cfg.trailing_stop_pct and pnl_pct > 0:
                trigger = "trailing_stop_exit"
                sell_qty = qty
            elif not symbol_state.get("second_target_hit") and pnl_pct >= self.cfg.second_target_pct:
                trigger = "second_target_partial"
                sell_qty = self._sell_qty(symbol, qty, self.cfg.second_sell_pct)
                symbol_state["second_target_hit"] = True
            elif not symbol_state.get("first_target_hit") and pnl_pct >= self.cfg.first_target_pct:
                trigger = "first_target_partial"
                sell_qty = self._sell_qty(symbol, qty, self.cfg.first_sell_pct)
                symbol_state["first_target_hit"] = True

            if not trigger or sell_qty <= 0:
                continue

            # At PDT threshold, avoid same-day equity profit-taking exits.
            if (
                pdt_guard_enabled
                and pdt_allow_only_hard_stop
                and daytrade_count >= pdt_threshold
                and not _is_crypto_symbol(symbol)
                and symbol in bought_today_equities
                and trigger != "hard_stop_loss"
            ):
                pdt_blocked_exits += 1
                continue

            notional = sell_qty * current
            if notional < self.cfg.min_notional_exit_usd:
                continue

            tif = "gtc" if _is_crypto_symbol(symbol) else "day"
            if execute:
                order_result = self.client.place_order(
                    symbol=symbol,
                    qty=sell_qty,
                    side="sell",
                    order_type="market",
                    tif=tif,
                )
            else:
                order_result = {"dry_run": True, "symbol": symbol, "qty": sell_qty, "side": "sell", "type": "market"}

            if trigger == "first_target_partial":
                symbol_state["first_target_hit"] = True
            if trigger == "second_target_partial":
                symbol_state["second_target_hit"] = True
            if trigger.endswith("_exit"):
                symbol_state["peak_price"] = current
                symbol_state["first_target_hit"] = False
                symbol_state["second_target_hit"] = False

            action = {
                "symbol": symbol,
                "trigger": trigger,
                "qty": sell_qty,
                "price": current,
                "pnl_pct": pnl_pct,
                "peak_drop_pct": peak_drop_pct,
                "execute": execute,
                "order_result": order_result,
            }
            actions.append(action)
            self.journal.log_exit_action(
                symbol=symbol,
                trigger=trigger,
                qty=sell_qty,
                price=current,
                pnl_pct=pnl_pct,
                execute=execute,
                result=order_result,
            )

            if trigger.endswith("_partial"):
                remaining = self._remaining_qty(symbol, qty, sell_qty)
                if remaining <= 0:
                    symbol_state["first_target_hit"] = False
                    symbol_state["second_target_hit"] = False
                    symbol_state["peak_price"] = current

        self._save_state()
        return {
            "positions": len(positions),
            "tracked_prices": len(prices),
            "actions": actions,
            "pdt_daytrade_count": daytrade_count,
            "pdt_blocked_exits": pdt_blocked_exits,
            "execute": execute,
            "exit_config": self.cfg.__dict__,
        }
