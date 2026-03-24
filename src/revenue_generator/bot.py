from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .alpaca_client import AlpacaClient
from .external_research import select_external_candidates, should_skip_cycle_for_vix
from .risk import evaluate_risk
from .strategy import Signal, select_top_signals


SEGMENT_UNIVERSE: dict[str, list[str]] = {
    "largeCapStocks": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA"],
    "pennyStocks": ["SNDL", "MULN", "XELA", "HSDT", "AEMD", "TNXP", "CTRM"],
    "crypto": ["BTC/USD", "ETH/USD", "SOL/USD", "AVAX/USD", "DOGE/USD"],
    "indexFunds": ["SPY", "QQQ", "DIA", "IWM", "VTI", "VOO", "IVV"],
}


@dataclass
class PlannedOrder:
    symbol: str
    qty: int
    limit_price: float
    confidence: float
    expected_edge: float
    allocation: float


def _round_limit(price: float) -> float:
    if price < 1:
        return round(price, 4)
    return round(price, 2)


def _price_targets(limit_price: float, tp_pct: float, sl_pct: float) -> tuple[float, float]:
    tp = _round_limit(limit_price * (1.0 + tp_pct / 100.0))
    sl = _round_limit(limit_price * (1.0 - sl_pct / 100.0))
    return tp, sl


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _bars_from_stock_snapshots(snapshots: dict[str, Any]) -> dict[str, list[dict[str, float]]]:
    bars_by_symbol: dict[str, list[dict[str, float]]] = {}
    for symbol, snap in snapshots.items():
        prev_bar = snap.get("prevDailyBar")
        day_bar = snap.get("dailyBar")
        if not prev_bar or not day_bar:
            continue
        bars_by_symbol[symbol] = [
            {"c": float(prev_bar["c"]), "h": float(prev_bar["h"]), "l": float(prev_bar["l"]), "v": float(prev_bar.get("v", 0))},
            {"c": float(day_bar["c"]), "h": float(day_bar["h"]), "l": float(day_bar["l"]), "v": float(day_bar.get("v", 0))},
        ]
    return bars_by_symbol


def build_orders(
    *,
    signals: list[Signal],
    budget: float,
    start_equity: float,
    current_equity: float,
    open_positions: int,
    risk_policy: dict[str, Any],
) -> list[PlannedOrder]:
    max_open_positions = int(risk_policy.get("maxOpenPositions", 5))
    max_daily_loss_pct = float(risk_policy.get("maxDailyLossPct", 2.0))
    max_position_size_pct = float(risk_policy.get("maxPositionSizePct", 10.0))

    orders: list[PlannedOrder] = []
    remaining_budget = budget
    for sig in signals:
        decision = evaluate_risk(
            start_equity=start_equity,
            current_equity=current_equity,
            open_positions=open_positions + len(orders),
            max_open_positions=max_open_positions,
            max_daily_loss_pct=max_daily_loss_pct,
            budget=budget,
            max_position_size_pct=max_position_size_pct,
            confidence=sig.confidence,
        )
        if not decision.allowed:
            break

        allowed_alloc = min(decision.max_alloc_dollars, remaining_budget)
        qty = int(allowed_alloc // sig.last_price)
        if qty < 1 and remaining_budget >= sig.last_price:
            qty = 1
        if qty < 1:
            continue

        # Slight discount below last for better fill quality.
        limit_price = _round_limit(sig.last_price * 0.998)
        used_alloc = qty * limit_price
        remaining_budget -= used_alloc
        if remaining_budget <= 0:
            break
        orders.append(
            PlannedOrder(
                symbol=sig.symbol,
                qty=qty,
                limit_price=limit_price,
                confidence=sig.confidence,
                expected_edge=sig.expected_edge,
                allocation=used_alloc,
            )
        )
    return orders


def run_once(
    *,
    client: AlpacaClient,
    risk_policy: dict[str, Any],
    segment: str,
    budget: float,
    execute: bool,
) -> dict[str, Any]:
    if segment not in SEGMENT_UNIVERSE:
        raise ValueError(f"Unsupported segment '{segment}'. Choose from: {', '.join(SEGMENT_UNIVERSE)}")

    account = client.get_account()
    positions = client.get_open_positions()
    start_equity = float(account.get("last_equity", account.get("equity", "0")))
    current_equity = float(account.get("equity", "0"))

    segment_cfg = risk_policy.get("allowedSegments", {}).get(segment, {})
    if not segment_cfg.get("enabled", True):
        return {"orders": [], "reason": f"Segment '{segment}' disabled in risk policy."}

    universe = segment_cfg.get("symbolsAllowlist") or SEGMENT_UNIVERSE[segment]
    external_cfg = risk_policy.get("externalResearch", {})
    if external_cfg.get("enabled", True):
        regime_vix_ceiling = float(external_cfg.get("riskOffVixCeiling", 25.0))
        risk_off_segments = external_cfg.get("riskOffSegments", ["pennyStocks"])
        skip_for_vix, vix_now = should_skip_cycle_for_vix(
            segment=segment,
            risk_off_vix_ceiling=regime_vix_ceiling,
            risk_off_segments=[str(s) for s in risk_off_segments],
        )
        if skip_for_vix:
            return {
                "strategy": "rule_engine",
                "account_status": account.get("status"),
                "segment": segment,
                "budget": budget,
                "execute": execute,
                "vix_risk_off_skip": True,
                "vix_value": vix_now,
                "vix_ceiling": regime_vix_ceiling,
                "reason": f"Skipped cycle due to elevated VIX ({vix_now:.2f} > {regime_vix_ceiling:.2f}).",
                "orders_planned": [],
                "orders_placed": [],
                "order_errors": [],
            }
    if external_cfg.get("enabled", True):
        top_n = int(external_cfg.get("topCandidatesPerSegment", 12))
        regime_vix_ceiling = float(external_cfg.get("riskOffVixCeiling", 25.0))
        universe = select_external_candidates(
            segment=segment,
            symbols=list(universe),
            top_n=top_n,
            regime_vix_ceiling=regime_vix_ceiling,
        )
    max_spread_bps = float(segment_cfg.get("maxSpreadBps", 40))
    min_confidence = float(risk_policy.get("minSignalConfidence", 0.5))
    min_expected_edge = float(risk_policy.get("minExpectedEdge", 0.0))
    max_signals = int(segment_cfg.get("maxSignals", 3))

    stale_minutes = int(risk_policy.get("cancelOpenOrdersAfterMinutes", 20))
    cancel_open_before_run = bool(risk_policy.get("cancelOpenOrdersBeforeRun", True))
    cancelled_order_ids: list[str] = []
    if execute and cancel_open_before_run and stale_minutes > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
        try:
            open_orders = client.get_orders(status="open", limit=200, direction="desc")
            for order in open_orders:
                if str(order.get("side", "")).lower() != "buy":
                    continue
                order_id = order.get("id")
                if not order_id:
                    continue
                created_at = _parse_iso_utc(order.get("created_at") or order.get("submitted_at"))
                if not created_at:
                    continue
                if created_at <= cutoff:
                    try:
                        client.cancel_order(str(order_id))
                        cancelled_order_ids.append(str(order_id))
                    except Exception:
                        # Keep cycle alive if one cancel fails.
                        pass
        except Exception:
            # Keep cycle alive if orders lookup fails.
            pass

    if segment == "crypto":
        bars = client.get_crypto_bars(universe, timeframe="1Hour", limit=80)
    else:
        snapshots = client.get_stock_snapshots(universe)
        bars = _bars_from_stock_snapshots(snapshots)

    bars_with_data = {sym: v for sym, v in bars.items() if v}

    # Duplicate/add-on controls:
    # - default mode: skip held + recently bought symbols
    # - aggressive mode: allow add-on buys with per-symbol cap and short cooldown
    held_symbols = {p.get("symbol") for p in positions if p.get("symbol")}
    cooldown_minutes = int(risk_policy.get("buyCooldownMinutes", 180))
    cooldown_cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
    allow_add_on_buys = bool(risk_policy.get("allowAddOnBuys", False))
    max_entries_per_symbol = int(risk_policy.get("maxEntriesPerSymbol", 3))
    add_on_cooldown_minutes = int(risk_policy.get("addOnBuyCooldownMinutes", 8))
    entry_lookback_hours = int(risk_policy.get("entryCountLookbackHours", 48))
    add_on_cooldown_cutoff = datetime.now(timezone.utc) - timedelta(minutes=add_on_cooldown_minutes)
    entry_lookback_cutoff = datetime.now(timezone.utc) - timedelta(hours=entry_lookback_hours)
    recently_bought: set[str] = set()
    recently_bought_add_on: set[str] = set()
    buy_entry_count: dict[str, int] = {}
    try:
        recent_orders = client.get_orders(status="all", limit=100, direction="desc")
        for order in recent_orders:
            if str(order.get("side", "")).lower() != "buy":
                continue
            symbol = order.get("symbol")
            if not symbol:
                continue
            ts = order.get("filled_at") or order.get("submitted_at")
            if not ts:
                continue
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if dt >= cooldown_cutoff:
                recently_bought.add(symbol)
            if dt >= add_on_cooldown_cutoff:
                recently_bought_add_on.add(symbol)
            if dt >= entry_lookback_cutoff:
                buy_entry_count[symbol] = buy_entry_count.get(symbol, 0) + 1
    except Exception:
        # If orders lookup fails, continue with held-symbol filtering only.
        pass

    if allow_add_on_buys:
        blocked_symbols = {
            sym
            for sym in bars_with_data
            if sym in recently_bought_add_on or buy_entry_count.get(sym, 0) >= max_entries_per_symbol
        }
    else:
        blocked_symbols = held_symbols | recently_bought
    bars_with_data = {sym: b for sym, b in bars_with_data.items() if sym not in blocked_symbols}

    signals = select_top_signals(
        bars_by_symbol=bars_with_data,
        max_spread_bps=max_spread_bps,
        top_n=max_signals,
        min_confidence=min_confidence,
        min_expected_edge=min_expected_edge,
        segment=segment,
    )
    orders = build_orders(
        signals=signals,
        budget=budget,
        start_equity=start_equity,
        current_equity=current_equity,
        open_positions=len(positions),
        risk_policy=risk_policy,
    )

    placed: list[dict[str, Any]] = []
    order_errors: list[dict[str, Any]] = []
    if execute:
        tp_pct = float(risk_policy.get("takeProfitPct", 4.5))
        sl_pct = float(risk_policy.get("stopLossPct", 2.2))
        equity_brackets_enabled = bool(risk_policy.get("equityBracketsEnabled", False))
        for order in orders:
            side = "buy"
            tif = "gtc" if segment == "crypto" else "day"
            tp, sl = _price_targets(order.limit_price, tp_pct, sl_pct)
            use_bracket = segment != "crypto" and equity_brackets_enabled
            try:
                placed.append(
                    client.place_order(
                        symbol=order.symbol,
                        qty=order.qty,
                        side=side,
                        order_type="limit",
                        tif=tif,
                        limit_price=order.limit_price,
                        take_profit_price=tp if use_bracket else None,
                        stop_loss_price=sl if use_bracket else None,
                    )
                )
            except Exception as err:
                order_errors.append({"symbol": order.symbol, "error": str(err)})

    return {
        "strategy": "rule_engine",
        "account_status": account.get("status"),
        "segment": segment,
        "budget": budget,
        "stale_orders_cancelled": len(cancelled_order_ids),
        "universe_size": len(universe),
        "symbols_with_data": len(bars_with_data),
        "signals_considered": len(signals),
        "orders_planned": [o.__dict__ for o in orders],
        "orders_placed": placed,
        "order_errors": order_errors,
        "execute": execute,
    }
