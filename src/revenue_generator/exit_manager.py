from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .alpaca_client import AlpacaClient
from .equity_mode import apply_equity_mode_switch
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
    if "/" in symbol:
        return True
    # Alpaca positions drop the slash: BTC/USD -> BTCUSD
    return len(symbol) >= 6 and symbol.endswith("USD")


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


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _floor_decimals(value: float, decimals: int) -> float:
    if value <= 0:
        return 0.0
    scale = 10 ** max(decimals, 0)
    return math.floor(value * scale) / scale


class ExitManager:
    def __init__(self, *, client: AlpacaClient, risk_policy: dict[str, Any], journal: TradeJournal, state_path: str = "logs/exit_state.json", crypto_client: Any | None = None) -> None:
        self.client = client
        self.crypto_client = crypto_client
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
        reliability = risk_policy.get("exitReliability", {})
        self.trigger_cooldown_seconds = int(reliability.get("triggerCooldownSeconds", 45))
        self.insufficient_funds_backoff_seconds = int(reliability.get("insufficientFundsBackoffSeconds", 90))
        self.generic_error_backoff_seconds = int(reliability.get("genericErrorBackoffSeconds", 45))
        self.max_backoff_seconds = int(reliability.get("maxBackoffSeconds", 900))
        self.crypto_min_sell_qty = float(reliability.get("cryptoMinSellQty", 0.00001))
        ai_exit = risk_policy.get("aiExitAdvisor", {})
        self.ai_exit_enabled = bool(ai_exit.get("enabled", False))
        self.ai_exit_segments = {str(s) for s in (ai_exit.get("segments") or []) if str(s)}
        self.ai_exit_mode = str(ai_exit.get("mode", "assist")).strip().lower()
        self.ai_exit_evaluate_every_position = bool(ai_exit.get("evaluateEveryPosition", False))
        self.ai_exit_timeout_seconds = int(ai_exit.get("timeoutSeconds", 8))
        self.ai_exit_near_target_buffer_pct = float(ai_exit.get("nearTargetBufferPct", 0.20))
        self.ai_exit_near_stop_buffer_pct = float(ai_exit.get("nearStopBufferPct", 0.20))
        self.ai_exit_near_trailing_buffer_pct = float(ai_exit.get("nearTrailingBufferPct", 0.20))
        self.ai_exit_min_confidence = float(ai_exit.get("minConfidence", 0.62))
        self.ai_exit_min_sell_confidence = float(ai_exit.get("minSellConfidence", self.ai_exit_min_confidence))
        self.ai_exit_min_sell_pct = float(ai_exit.get("minSellPct", 20.0))
        self.ai_exit_default_sell_pct = float(ai_exit.get("defaultSellPct", 100.0))
        self.ai_exit_min_rebound_pct = float(ai_exit.get("minExpectedReboundPct", 0.15))
        self.ai_exit_max_defer_cycles = int(ai_exit.get("maxDeferralsPerTrigger", 2))
        self.ai_exit_emergency_stop_loss_pct = float(ai_exit.get("emergencyStopLossPct", 6.0))
        self.ai_exit_log_path = Path(ai_exit.get("logPath", "logs/ai_exit_advisor.jsonl"))
        self.ai_exit_log_path.parent.mkdir(parents=True, exist_ok=True)

    def _cfg_for_symbol(self, symbol: str) -> ExitConfig:
        hooks = dict(self.risk_policy.get("exitHooks", {}))
        per_segment = self.risk_policy.get("exitHooksBySegment", {})
        seg_key = "crypto" if _is_crypto_symbol(symbol) else "stocks"
        if isinstance(per_segment, dict):
            seg_hooks = per_segment.get(seg_key, {})
            if isinstance(seg_hooks, dict):
                hooks.update(seg_hooks)
        return ExitConfig(
            first_target_pct=float(hooks.get("firstTargetPct", self.cfg.first_target_pct)),
            first_sell_pct=float(hooks.get("firstTargetSellPct", self.cfg.first_sell_pct)),
            second_target_pct=float(hooks.get("secondTargetPct", self.cfg.second_target_pct)),
            second_sell_pct=float(hooks.get("secondTargetSellPct", self.cfg.second_sell_pct)),
            trailing_stop_pct=float(hooks.get("trailingStopPct", self.cfg.trailing_stop_pct)),
            break_even_buffer_pct=float(hooks.get("breakEvenBufferPct", self.cfg.break_even_buffer_pct)),
            hard_stop_loss_pct=float(hooks.get("hardStopLossPct", self.cfg.hard_stop_loss_pct)),
            min_notional_exit_usd=float(hooks.get("minNotionalExitUsd", self.cfg.min_notional_exit_usd)),
        )

    def _all_open_positions(self) -> list[dict[str, Any]]:
        positions_by_symbol: dict[str, dict[str, Any]] = {}
        try:
            for pos in self.client.get_open_positions():
                symbol = str(pos.get("symbol") or "")
                if symbol:
                    # When a dedicated crypto client is configured, treat it as source of truth
                    # for crypto balances to avoid duplicate symbol-format positions.
                    if self.crypto_client and _is_crypto_symbol(symbol):
                        continue
                    positions_by_symbol[symbol] = pos
        except Exception:
            pass
        if self.crypto_client:
            try:
                for pos in self.crypto_client.get_open_positions():
                    symbol = self._normalize_crypto_symbol(str(pos.get("symbol") or ""))
                    if not symbol:
                        continue
                    pos = dict(pos)
                    pos["symbol"] = symbol
                    existing = positions_by_symbol.get(symbol)
                    if not existing:
                        positions_by_symbol[symbol] = pos
                        continue
                    existing_entry = float(existing.get("avg_entry_price", "0") or 0)
                    new_entry = float(pos.get("avg_entry_price", "0") or 0)
                    if existing_entry <= 0 and new_entry > 0:
                        positions_by_symbol[symbol] = pos
            except Exception:
                pass
        return list(positions_by_symbol.values())

    def _load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"symbols": {}}
        with open(self.state_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_state(self) -> None:
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)

    @staticmethod
    def _normalize_crypto_symbol(symbol: str) -> str:
        """Convert Alpaca position format (BTCUSD) to API format (BTC/USD)."""
        if "/" in symbol:
            return symbol
        if symbol.endswith("USD") and len(symbol) >= 6:
            return symbol[:-3] + "/USD"
        return symbol

    def _price_map_for_positions(self, positions: list[dict[str, Any]]) -> dict[str, float]:
        stock_symbols = [p["symbol"] for p in positions if not _is_crypto_symbol(p["symbol"])]
        crypto_raw = [p["symbol"] for p in positions if _is_crypto_symbol(p["symbol"])]
        prices: dict[str, float] = {}
        if stock_symbols:
            try:
                snapshots = self.client.get_stock_snapshots(stock_symbols)
                for sym in stock_symbols:
                    snap = snapshots.get(sym, {})
                    p = _parse_price(snap)
                    if p is not None:
                        prices[sym] = p
            except Exception:
                pass
        if crypto_raw:
            api_syms = [self._normalize_crypto_symbol(s) for s in crypto_raw]
            price_client = self.crypto_client if self.crypto_client else self.client
            try:
                crypto_prices = price_client.get_crypto_latest_prices(api_syms)
                for raw, api in zip(crypto_raw, api_syms):
                    if api in crypto_prices:
                        prices[raw] = crypto_prices[api]
            except Exception:
                pass
        return prices

    def _sell_qty(self, symbol: str, total_qty: float, sell_pct: float) -> float:
        raw = total_qty * (sell_pct / 100.0)
        if _is_crypto_symbol(symbol):
            qty = _floor_decimals(raw, 6)
            return max(qty, 0.0)
        qty_int = int(raw)
        if qty_int < 1 and total_qty >= 1:
            qty_int = 1
        return float(qty_int)

    def _remaining_qty(self, symbol: str, total_qty: float, planned_qty: float) -> float:
        remaining = total_qty - planned_qty
        if _is_crypto_symbol(symbol):
            return max(_floor_decimals(remaining, 6), 0.0)
        return float(max(int(remaining), 0))

    def _segment_value(self, key: str, symbol: str, default: float) -> float:
        per_segment = self.risk_policy.get(f"{key}BySegment", {})
        seg_key = "crypto" if _is_crypto_symbol(symbol) else "stocks"
        if isinstance(per_segment, dict) and seg_key in per_segment:
            try:
                return float(per_segment.get(seg_key))
            except (TypeError, ValueError):
                return default
        try:
            return float(self.risk_policy.get(key, default))
        except (TypeError, ValueError):
            return default

    def _segment_for_symbol(self, symbol: str) -> str:
        if _is_crypto_symbol(symbol):
            return "crypto"
        allowed = self.risk_policy.get("allowedSegments", {})
        if not isinstance(allowed, dict):
            return "other"
        for seg in ("largeCapStocks", "indexFunds", "pennyStocks"):
            seg_cfg = allowed.get(seg, {})
            if not isinstance(seg_cfg, dict):
                continue
            allowlist = seg_cfg.get("symbolsAllowlist", [])
            if isinstance(allowlist, list) and symbol in allowlist:
                return seg
        return "other"

    @staticmethod
    def _trigger_near_threshold(
        *,
        trigger: str,
        pnl_pct: float,
        peak_drop_pct: float,
        held_minutes: float,
        max_hold_minutes: float,
        cfg: ExitConfig,
        target_buffer: float,
        stop_buffer: float,
        trailing_buffer: float,
    ) -> bool:
        if trigger == "first_target_partial":
            return abs(pnl_pct - cfg.first_target_pct) <= target_buffer
        if trigger == "second_target_partial":
            return abs(pnl_pct - cfg.second_target_pct) <= target_buffer
        if trigger == "break_even_exit":
            return abs(pnl_pct - cfg.break_even_buffer_pct) <= target_buffer
        if trigger == "trailing_stop_exit":
            return abs(peak_drop_pct - cfg.trailing_stop_pct) <= trailing_buffer
        if trigger == "max_hold_time_exit":
            if max_hold_minutes <= 0:
                return False
            return held_minutes <= (max_hold_minutes + max(1.0, stop_buffer))
        return False

    def _log_ai_exit_advisor(self, payload: dict[str, Any]) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        with open(self.ai_exit_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def run_cycle(self, *, execute: bool) -> dict[str, Any]:
        positions = self._all_open_positions()
        if not positions:
            return {"positions": 0, "actions": [], "execute": execute}
        available_crypto_qty: dict[str, float] = {
            self._normalize_crypto_symbol(str(p.get("symbol") or "")): _position_side_qty(p)
            for p in positions
            if _is_crypto_symbol(str(p.get("symbol") or ""))
        }

        account = self.client.get_account()
        equity_mode = apply_equity_mode_switch(self.risk_policy, account=account)
        pdt_cfg = (self.risk_policy.get("pdtGuard") or {})
        pdt_guard_enabled = bool(pdt_cfg.get("enabled", True))
        pdt_threshold = int(pdt_cfg.get("thresholdDayTrades", 3))
        pdt_allow_only_hard_stop = bool(pdt_cfg.get("allowOnlyHardStopAtThreshold", True))
        pdt_near_limit_buffer = int(pdt_cfg.get("nearLimitBuffer", 1))
        pdt_account = account if pdt_guard_enabled else {}
        daytrade_count = int(float(pdt_account.get("daytrade_count", 0) or 0)) if pdt_guard_enabled else 0
        pdt_near_limit = daytrade_count >= max(0, pdt_threshold - max(0, pdt_near_limit_buffer))
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
        reliability_suppressed_backoff = 0
        reliability_suppressed_cooldown = 0
        reliability_suppressed_min_qty = 0
        ai_advisor_calls = 0
        ai_advisor_deferrals = 0
        now_utc = datetime.now(timezone.utc)
        active_symbols = {str(p.get("symbol") or "") for p in positions if p.get("symbol")}
        ai_sell_map: dict[str, dict[str, Any]] = {}

        if self.ai_exit_enabled and self.ai_exit_evaluate_every_position and positions:
            try:
                from .ai_bridge import run_ai_exit_portfolio_analysis

                contexts: list[dict[str, Any]] = []
                for pos in positions:
                    symbol = str(pos.get("symbol") or "")
                    if not symbol:
                        continue
                    if symbol not in prices:
                        continue
                    segment = self._segment_for_symbol(symbol)
                    if self.ai_exit_segments and segment not in self.ai_exit_segments:
                        continue
                    entry = float(pos.get("avg_entry_price", "0") or 0)
                    qty = _position_side_qty(pos)
                    if entry <= 0 or qty <= 0:
                        continue
                    current = prices[symbol]
                    pnl_pct = ((current - entry) / entry) * 100.0
                    symbol_state = symbols_state.setdefault(
                        symbol,
                        {
                            "peak_price": current,
                            "first_target_hit": False,
                            "second_target_hit": False,
                            "first_seen_at": now_utc.isoformat(),
                        },
                    )
                    peak = max(float(symbol_state.get("peak_price", current)), current)
                    symbol_state["peak_price"] = peak
                    first_seen_at = _parse_iso_utc(str(symbol_state.get("first_seen_at") or "")) or now_utc
                    held_minutes = max((now_utc - first_seen_at).total_seconds() / 60.0, 0.0)
                    peak_drop_pct = ((peak - current) / peak * 100.0) if peak > 0 else 0.0
                    # Rule candidate is provided as context only; execution can remain AI-dependent.
                    rule_candidate_trigger = None
                    cfg = self._cfg_for_symbol(symbol)
                    max_hold_minutes = self._segment_value("maxHoldMinutes", symbol, 0.0)
                    max_hold_exit_min_pnl_pct = self._segment_value("maxHoldExitMinPnlPct", symbol, 0.05)
                    if pnl_pct <= -cfg.hard_stop_loss_pct:
                        rule_candidate_trigger = "hard_stop_loss"
                    elif max_hold_minutes > 0 and held_minutes >= max_hold_minutes and pnl_pct >= max_hold_exit_min_pnl_pct:
                        rule_candidate_trigger = "max_hold_time_exit"
                    elif symbol_state.get("first_target_hit") and pnl_pct <= cfg.break_even_buffer_pct:
                        rule_candidate_trigger = "break_even_exit"
                    elif peak_drop_pct >= cfg.trailing_stop_pct and pnl_pct > 0:
                        rule_candidate_trigger = "trailing_stop_exit"
                    elif not symbol_state.get("second_target_hit") and pnl_pct >= cfg.second_target_pct:
                        rule_candidate_trigger = "second_target_partial"
                    elif not symbol_state.get("first_target_hit") and pnl_pct >= cfg.first_target_pct:
                        rule_candidate_trigger = "first_target_partial"

                    contexts.append(
                        {
                            "symbol": symbol,
                            "segment": segment,
                            "pnl_pct": round(pnl_pct, 6),
                            "held_minutes": round(held_minutes, 2),
                            "peak_drop_pct": round(peak_drop_pct, 6),
                            "entry_price": entry,
                            "current_price": current,
                            "qty": qty,
                            "rule_candidate_trigger": rule_candidate_trigger,
                        }
                    )
                if contexts:
                    portfolio_ai = run_ai_exit_portfolio_analysis(
                        positions=contexts,
                        timeout_seconds=self.ai_exit_timeout_seconds,
                        default_sell_pct=self.ai_exit_default_sell_pct,
                    )
                    for item in portfolio_ai.get("actions", []):
                        if not isinstance(item, dict):
                            continue
                        sym = str(item.get("symbol") or "")
                        if not sym:
                            continue
                        ai_sell_map[sym] = item
                    ai_advisor_calls += 1
            except Exception as err:
                self._log_ai_exit_advisor({"portfolio_error": str(err)})

        for pos in positions:
            symbol = pos["symbol"]
            if symbol not in prices:
                continue
            cfg = self._cfg_for_symbol(symbol)
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
                    "first_seen_at": now_utc.isoformat(),
                },
            )
            symbol_state["peak_price"] = max(float(symbol_state.get("peak_price", current)), current)
            first_seen_at = _parse_iso_utc(str(symbol_state.get("first_seen_at") or "")) or now_utc
            symbol_state["first_seen_at"] = first_seen_at.isoformat()
            held_minutes = max((now_utc - first_seen_at).total_seconds() / 60.0, 0.0)
            peak = float(symbol_state["peak_price"])
            peak_drop_pct = ((peak - current) / peak * 100.0) if peak > 0 else 0.0

            trigger: str | None = None
            sell_qty = 0.0
            max_hold_minutes = self._segment_value("maxHoldMinutes", symbol, 0.0)
            max_hold_exit_min_pnl_pct = self._segment_value("maxHoldExitMinPnlPct", symbol, 0.05)

            if pnl_pct <= -cfg.hard_stop_loss_pct:
                trigger = "hard_stop_loss"
                sell_qty = qty
            elif max_hold_minutes > 0 and held_minutes >= max_hold_minutes and pnl_pct >= max_hold_exit_min_pnl_pct:
                trigger = "max_hold_time_exit"
                sell_qty = qty
            elif symbol_state.get("first_target_hit") and pnl_pct <= cfg.break_even_buffer_pct:
                trigger = "break_even_exit"
                sell_qty = qty
            elif peak_drop_pct >= cfg.trailing_stop_pct and pnl_pct > 0:
                trigger = "trailing_stop_exit"
                sell_qty = qty
            elif not symbol_state.get("second_target_hit") and pnl_pct >= cfg.second_target_pct:
                trigger = "second_target_partial"
                sell_qty = self._sell_qty(symbol, qty, cfg.second_sell_pct)
                symbol_state["second_target_hit"] = True
            elif not symbol_state.get("first_target_hit") and pnl_pct >= cfg.first_target_pct:
                trigger = "first_target_partial"
                sell_qty = self._sell_qty(symbol, qty, cfg.first_sell_pct)
                symbol_state["first_target_hit"] = True

            if not trigger or sell_qty <= 0:
                trigger = None
                sell_qty = 0.0

            # Optional AI-dependent mode: AI decides HOLD/SELL on every position,
            # with an emergency deep-loss stop as catastrophic protection.
            if self.ai_exit_enabled and self.ai_exit_mode == "signal_dependent":
                ai_item = ai_sell_map.get(symbol)
                ai_decision = str((ai_item or {}).get("decision", "")).upper()
                ai_conf = float((ai_item or {}).get("confidence", 0.0) or 0.0)
                ai_sell_pct = float((ai_item or {}).get("sell_pct", self.ai_exit_default_sell_pct) or self.ai_exit_default_sell_pct)
                if ai_decision == "SELL" and ai_conf >= self.ai_exit_min_sell_confidence and ai_sell_pct >= self.ai_exit_min_sell_pct:
                    trigger = "ai_signal_exit"
                    sell_qty = self._sell_qty(symbol, qty, min(max(ai_sell_pct, 0.0), 100.0))
                elif self.ai_exit_emergency_stop_loss_pct > 0 and pnl_pct <= -abs(self.ai_exit_emergency_stop_loss_pct):
                    trigger = "emergency_hard_stop"
                    sell_qty = qty
                else:
                    trigger = None
                    sell_qty = 0.0

            if not trigger or sell_qty <= 0:
                continue

            advisor_recommendation: dict[str, Any] | None = None
            ai_advisor_deferred = False
            symbol_segment = self._segment_for_symbol(symbol)
            should_consider_ai = (
                self.ai_exit_enabled
                and trigger != "hard_stop_loss"
                and trigger in {"first_target_partial", "second_target_partial", "trailing_stop_exit", "break_even_exit", "max_hold_time_exit"}
                and (not self.ai_exit_segments or symbol_segment in self.ai_exit_segments)
                and self._trigger_near_threshold(
                    trigger=trigger,
                    pnl_pct=pnl_pct,
                    peak_drop_pct=peak_drop_pct,
                    held_minutes=held_minutes,
                    max_hold_minutes=max_hold_minutes,
                    cfg=cfg,
                    target_buffer=self.ai_exit_near_target_buffer_pct,
                    stop_buffer=self.ai_exit_near_stop_buffer_pct,
                    trailing_buffer=self.ai_exit_near_trailing_buffer_pct,
                )
            )
            if should_consider_ai:
                try:
                    from .ai_bridge import run_ai_exit_advisor

                    ai_advisor_calls += 1
                    advisor_recommendation = run_ai_exit_advisor(
                        symbol=symbol,
                        segment=symbol_segment,
                        trigger=trigger,
                        entry_price=entry,
                        current_price=current,
                        pnl_pct=pnl_pct,
                        peak_price=peak,
                        peak_drop_pct=peak_drop_pct,
                        held_minutes=held_minutes,
                        thresholds={
                            "first_target_pct": cfg.first_target_pct,
                            "second_target_pct": cfg.second_target_pct,
                            "trailing_stop_pct": cfg.trailing_stop_pct,
                            "break_even_buffer_pct": cfg.break_even_buffer_pct,
                            "hard_stop_loss_pct": cfg.hard_stop_loss_pct,
                        },
                        timeout_seconds=self.ai_exit_timeout_seconds,
                    )
                    decision = str((advisor_recommendation or {}).get("decision", "EXIT")).upper()
                    confidence = float((advisor_recommendation or {}).get("confidence", 0.0) or 0.0)
                    expected_rebound_pct = float((advisor_recommendation or {}).get("expected_rebound_pct", 0.0) or 0.0)
                    deferred_count = int(symbol_state.get("ai_defer_count", 0) or 0)
                    if (
                        decision == "HOLD"
                        and confidence >= self.ai_exit_min_confidence
                        and expected_rebound_pct >= self.ai_exit_min_rebound_pct
                        and deferred_count < max(self.ai_exit_max_defer_cycles, 0)
                    ):
                        symbol_state["ai_defer_count"] = deferred_count + 1
                        ai_advisor_deferrals += 1
                        ai_advisor_deferred = True
                    else:
                        symbol_state["ai_defer_count"] = 0
                except Exception as err:
                    advisor_recommendation = {"error": str(err)}

                self._log_ai_exit_advisor(
                    {
                        "symbol": symbol,
                        "segment": symbol_segment,
                        "trigger": trigger,
                        "pnl_pct": pnl_pct,
                        "peak_drop_pct": peak_drop_pct,
                        "held_minutes": held_minutes,
                        "recommendation": advisor_recommendation,
                        "executed_action": "defer_hold" if ai_advisor_deferred else "proceed_exit",
                    }
                )
            if ai_advisor_deferred:
                continue

            retry_after = _parse_iso_utc(str(symbol_state.get("retry_after") or ""))
            if retry_after and retry_after > now_utc:
                reliability_suppressed_backoff += 1
                continue
            if _is_crypto_symbol(symbol):
                symbol = self._normalize_crypto_symbol(symbol)
                available_qty = max(float(available_crypto_qty.get(symbol, 0.0)), 0.0)
                sell_qty = _floor_decimals(min(sell_qty, available_qty), 6)
                if sell_qty <= 0 or sell_qty < self.crypto_min_sell_qty:
                    symbol_state["retry_after"] = (now_utc + timedelta(seconds=self.insufficient_funds_backoff_seconds)).isoformat()
                    symbol_state["last_error"] = "crypto_sell_qty_below_min_or_unavailable"
                    reliability_suppressed_min_qty += 1
                    continue

            last_trigger = str(symbol_state.get("last_trigger") or "")
            last_attempt_at = _parse_iso_utc(str(symbol_state.get("last_attempt_at") or ""))
            last_attempt_qty = float(symbol_state.get("last_attempt_qty") or 0.0)
            if (
                trigger == last_trigger
                and last_attempt_at is not None
                and (now_utc - last_attempt_at).total_seconds() < max(0, self.trigger_cooldown_seconds)
                and abs(last_attempt_qty - sell_qty) <= 1e-6
            ):
                reliability_suppressed_cooldown += 1
                continue

            # Near PDT limit, avoid same-day non-hard-stop exits for equities bought today.
            if (
                pdt_guard_enabled
                and pdt_allow_only_hard_stop
                and pdt_near_limit
                and not _is_crypto_symbol(symbol)
                and symbol in bought_today_equities
                and trigger != "hard_stop_loss"
            ):
                pdt_blocked_exits += 1
                continue

            notional = sell_qty * current
            if notional < cfg.min_notional_exit_usd:
                continue

            tif = "gtc" if _is_crypto_symbol(symbol) else "day"
            sell_client = (self.crypto_client or self.client) if _is_crypto_symbol(symbol) else self.client
            if execute:
                try:
                    order_result = sell_client.place_order(
                        symbol=self._normalize_crypto_symbol(symbol) if _is_crypto_symbol(symbol) else symbol,
                        qty=sell_qty,
                        side="sell",
                        order_type="market",
                        tif=tif,
                    )
                    if _is_crypto_symbol(symbol):
                        available_crypto_qty[symbol] = max(float(available_crypto_qty.get(symbol, 0.0)) - sell_qty, 0.0)
                    symbol_state["retry_after"] = ""
                    symbol_state["consecutive_exit_failures"] = 0
                    symbol_state["last_error"] = ""
                except Exception as err:
                    order_result = {
                        "ok": False,
                        "symbol": symbol,
                        "qty": sell_qty,
                        "side": "sell",
                        "type": "market",
                        "error": str(err),
                    }
                    error_text = str(err).lower()
                    prev_failures = int(symbol_state.get("consecutive_exit_failures", 0)) + 1
                    symbol_state["consecutive_exit_failures"] = prev_failures
                    if "insufficient funds" in error_text:
                        backoff = min(self.insufficient_funds_backoff_seconds * (2 ** max(prev_failures - 1, 0)), self.max_backoff_seconds)
                    else:
                        backoff = min(self.generic_error_backoff_seconds * (2 ** max(prev_failures - 1, 0)), self.max_backoff_seconds)
                    symbol_state["retry_after"] = now_utc.isoformat() if backoff <= 0 else (now_utc + timedelta(seconds=backoff)).isoformat()
                    symbol_state["last_error"] = str(err)
            else:
                order_result = {"dry_run": True, "symbol": symbol, "qty": sell_qty, "side": "sell", "type": "market"}

            symbol_state["last_trigger"] = trigger
            symbol_state["last_attempt_at"] = now_utc.isoformat()
            symbol_state["last_attempt_qty"] = sell_qty

            if trigger == "first_target_partial":
                symbol_state["first_target_hit"] = True
            if trigger == "second_target_partial":
                symbol_state["second_target_hit"] = True
            if trigger.endswith("_exit"):
                symbol_state["peak_price"] = current
                symbol_state["first_target_hit"] = False
                symbol_state["second_target_hit"] = False
                symbol_state["first_seen_at"] = now_utc.isoformat()

            action = {
                "symbol": symbol,
                "trigger": trigger,
                "held_minutes": round(held_minutes, 2),
                "qty": sell_qty,
                "price": current,
                "pnl_pct": pnl_pct,
                "peak_drop_pct": peak_drop_pct,
                "execute": execute,
                "ai_exit_advisor": advisor_recommendation,
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
                    symbol_state["first_seen_at"] = now_utc.isoformat()

        for sym in list(symbols_state.keys()):
            if sym not in active_symbols:
                symbols_state.pop(sym, None)

        self._save_state()
        return {
            "positions": len(positions),
            "tracked_prices": len(prices),
            "actions": actions,
            "reliability_suppressed_backoff": reliability_suppressed_backoff,
            "reliability_suppressed_cooldown": reliability_suppressed_cooldown,
            "reliability_suppressed_min_qty": reliability_suppressed_min_qty,
            "ai_advisor_calls": ai_advisor_calls,
            "ai_advisor_deferrals": ai_advisor_deferrals,
            "pdt_daytrade_count": daytrade_count,
            "pdt_threshold": pdt_threshold,
            "pdt_near_limit": pdt_near_limit,
            "pdt_blocked_exits": pdt_blocked_exits,
            "equity_mode": equity_mode,
            "execute": execute,
            "exit_config": self.cfg.__dict__,
        }
