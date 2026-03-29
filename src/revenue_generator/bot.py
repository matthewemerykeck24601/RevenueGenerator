from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .alpaca_client import AlpacaClient
from .equity_mode import apply_equity_mode_switch
from .external_research import discover_segment_candidates, select_external_candidates, should_skip_cycle_for_vix
from .fear_climate import apply_fear_climate_overrides, load_fear_climate_state
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
    qty: float | int
    limit_price: float
    confidence: float
    expected_edge: float
    expected_edge_net: float
    estimated_cost_pct: float
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


def _yahoo_symbol(symbol: str) -> str:
    if "/" in symbol:
        return symbol.replace("/", "-")
    return symbol


def _fetch_yfinance_daily_bars(symbols: list[str], limit: int = 40) -> dict[str, list[dict[str, float]]]:
    try:
        import yfinance as yf
    except Exception:
        return {}

    out: dict[str, list[dict[str, float]]] = {}
    for sym in symbols:
        try:
            ticker = yf.Ticker(_yahoo_symbol(sym))
            hist = ticker.history(period="3mo", interval="1d", auto_adjust=False)
            if hist is None or len(hist) < 2:
                continue
            rows = hist.tail(max(2, limit))
            bars: list[dict[str, float]] = []
            for _idx, row in rows.iterrows():
                c = float(row.get("Close", 0.0) or 0.0)
                h = float(row.get("High", 0.0) or 0.0)
                l = float(row.get("Low", 0.0) or 0.0)
                v = float(row.get("Volume", 0.0) or 0.0)
                if c <= 0 or h <= 0 or l <= 0:
                    continue
                bars.append({"c": c, "h": h, "l": l, "v": v})
            if len(bars) >= 2:
                out[sym] = bars
        except Exception:
            continue
    return out


def _fear_benchmark_risk_off(
    bars_by_symbol: dict[str, list[dict[str, Any]]],
    *,
    required_symbols: tuple[str, str] = ("BTC/USD", "ETH/USD"),
    lookback_bars: int = 3,
    min_return_pct: float = -0.15,
) -> tuple[bool, dict[str, float]]:
    rets: dict[str, float] = {}
    for symbol in required_symbols:
        bars = bars_by_symbol.get(symbol) or []
        if len(bars) <= lookback_bars:
            continue
        now_c = float(bars[-1].get("c", 0.0) or 0.0)
        prev_c = float(bars[-(lookback_bars + 1)].get("c", 0.0) or 0.0)
        if now_c <= 0 or prev_c <= 0:
            continue
        rets[symbol] = (now_c - prev_c) / prev_c * 100.0
    if len(rets) < len(required_symbols):
        return False, rets
    risk_off = all(ret <= min_return_pct for ret in rets.values())
    return risk_off, rets


def build_orders(
    *,
    signals: list[Signal],
    budget: float,
    start_equity: float,
    current_equity: float,
    open_positions: int,
    risk_policy: dict[str, Any],
    segment: str = "",
    available_cash_cap: float | None = None,
) -> list[PlannedOrder]:
    max_open_positions = int(risk_policy.get("maxOpenPositions", 5))
    max_daily_loss_pct = float(risk_policy.get("maxDailyLossPct", 2.0))
    max_pos_by_segment = risk_policy.get("maxPositionSizePctBySegment", {})
    max_position_size_pct = float(max_pos_by_segment.get(segment, risk_policy.get("maxPositionSizePct", 10.0))) if isinstance(max_pos_by_segment, dict) else float(risk_policy.get("maxPositionSizePct", 10.0))
    allow_fractional_stocks = bool(risk_policy.get("allowFractionalStocks", True))
    min_edge_net_by_segment = risk_policy.get("minExpectedEdgeNetBySegment", {})
    min_expected_edge_net = float(min_edge_net_by_segment.get(segment, risk_policy.get("minExpectedEdgeNet", 0.0))) if isinstance(min_edge_net_by_segment, dict) else float(risk_policy.get("minExpectedEdgeNet", 0.0))
    ai_cfg = risk_policy.get("aiScheduler", {})
    stock_slippage_bps = float(ai_cfg.get("slippageBufferBpsStocks", 6.0))
    crypto_slippage_bps = float(ai_cfg.get("slippageBufferBpsCrypto", 12.0))
    global_discount_pct = float(risk_policy.get("limitPriceDiscountPct", 0.2))
    per_segment_discount = risk_policy.get("limitPriceDiscountPctBySegment", {})
    discount_pct = float(per_segment_discount.get(segment, global_discount_pct)) if isinstance(per_segment_discount, dict) else global_discount_pct
    limit_multiplier = 1.0 - (discount_pct / 100.0)
    min_notional_by_segment = risk_policy.get("minOrderNotionalUsdBySegment", {})
    max_notional_by_segment = risk_policy.get("maxOrderNotionalUsdBySegment", {})
    min_notional_global = float(risk_policy.get("minOrderNotionalUsd", 0.0))
    max_notional_global = float(risk_policy.get("maxOrderNotionalUsd", 0.0))
    min_order_notional = float(min_notional_by_segment.get(segment, min_notional_global)) if isinstance(min_notional_by_segment, dict) else min_notional_global
    max_order_notional = float(max_notional_by_segment.get(segment, max_notional_global)) if isinstance(max_notional_by_segment, dict) else max_notional_global

    orders: list[PlannedOrder] = []
    remaining_budget = budget
    if available_cash_cap is not None:
        remaining_budget = min(remaining_budget, max(float(available_cash_cap), 0.0))
    for sig in signals:
        spread_cost_pct = max(sig.spread_bps, 0.0) / 10000.0
        slippage_cost_pct = (crypto_slippage_bps if "/" in sig.symbol else stock_slippage_bps) / 10000.0
        estimated_cost_pct = spread_cost_pct + slippage_cost_pct
        expected_edge_net = sig.expected_edge - estimated_cost_pct
        if expected_edge_net < min_expected_edge_net:
            continue

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
        if max_order_notional > 0:
            allowed_alloc = min(allowed_alloc, max_order_notional)
        if allowed_alloc <= 0:
            continue
        is_crypto = "/" in sig.symbol
        qty: float | int
        if is_crypto:
            qty = round(allowed_alloc / sig.last_price, 6)
            if qty <= 0:
                continue
        elif allow_fractional_stocks:
            qty = round(allowed_alloc / sig.last_price, 6)
            if qty <= 0:
                continue
        else:
            qty = int(allowed_alloc // sig.last_price)
            if qty < 1 and remaining_budget >= sig.last_price:
                qty = 1
            if qty < 1:
                continue

        limit_price = _round_limit(sig.last_price * limit_multiplier)
        used_alloc = qty * limit_price
        if used_alloc < min_order_notional:
            continue
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
                expected_edge_net=expected_edge_net,
                estimated_cost_pct=estimated_cost_pct,
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

    policy_effective = deepcopy(risk_policy)
    fear_state = load_fear_climate_state()
    fear_active = segment == "crypto" and bool(fear_state.get("enabled", False))
    fear_meta: dict[str, Any] = {"enabled": fear_active}
    if fear_active:
        fear_meta = apply_fear_climate_overrides(policy_effective, segment="crypto")

    account = client.get_account()
    positions = client.get_open_positions()
    equity_mode = apply_equity_mode_switch(policy_effective, account=account)
    start_equity = float(account.get("last_equity", account.get("equity", "0")))
    current_equity = float(account.get("equity", "0"))

    segment_cfg = policy_effective.get("allowedSegments", {}).get(segment, {})
    if not segment_cfg.get("enabled", True):
        return {"orders": [], "reason": f"Segment '{segment}' disabled in risk policy.", "equity_mode": equity_mode, "fear_climate": fear_meta}
    if fear_active and segment == "crypto" and bool(policy_effective.get("fearPauseNewBuysCrypto", False)):
        return {
            "strategy": "rule_engine",
            "account_status": account.get("status"),
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "reason": "Fear climate mode is ON: new crypto buys are paused.",
            "orders_planned": [],
            "orders_placed": [],
            "order_errors": [],
            "equity_mode": equity_mode,
            "fear_climate": fear_meta,
        }

    universe = segment_cfg.get("symbolsAllowlist") or SEGMENT_UNIVERSE[segment]
    external_cfg = policy_effective.get("externalResearch", {})
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
                "equity_mode": equity_mode,
                "fear_climate": fear_meta,
            }
    discovery_cfg = policy_effective.get("marketDiscovery", {})
    if bool(discovery_cfg.get("enabled", False)):
        top_n = int(discovery_cfg.get("topCandidatesPerSegment", external_cfg.get("topCandidatesPerSegment", 20)))
        from .kraken_client import KrakenClient as _KC
        _kc = client if isinstance(client, _KC) else None
        universe = discover_segment_candidates(
            segment=segment,
            base_symbols=list(universe),
            top_n=top_n,
            discovery_cfg=discovery_cfg,
            kraken_client=_kc,
        )
    elif external_cfg.get("enabled", True):
        top_n = int(external_cfg.get("topCandidatesPerSegment", 12))
        regime_vix_ceiling = float(external_cfg.get("riskOffVixCeiling", 25.0))
        universe = select_external_candidates(
            segment=segment,
            symbols=list(universe),
            top_n=top_n,
            regime_vix_ceiling=regime_vix_ceiling,
        )
    max_spread_bps = float(segment_cfg.get("maxSpreadBps", 40))
    min_conf_by_segment = policy_effective.get("minSignalConfidenceBySegment", {})
    min_edge_by_segment = policy_effective.get("minExpectedEdgeBySegment", {})
    min_confidence = float(min_conf_by_segment.get(segment, policy_effective.get("minSignalConfidence", 0.5))) if isinstance(min_conf_by_segment, dict) else float(policy_effective.get("minSignalConfidence", 0.5))
    min_expected_edge = float(min_edge_by_segment.get(segment, policy_effective.get("minExpectedEdge", 0.0))) if isinstance(min_edge_by_segment, dict) else float(policy_effective.get("minExpectedEdge", 0.0))
    max_signals = int(segment_cfg.get("maxSignals", 3))

    available_cash_cap: float | None = None
    if segment == "crypto":
        try:
            from .kraken_client import KrakenClient as _KC

            if isinstance(client, _KC):
                guards = policy_effective.get("orderSizingGuards", {})
                cash_buffer_pct = float(guards.get("cryptoAvailableCashBufferPct", 2.0)) if isinstance(guards, dict) else 2.0
                use_buying_power = bool(guards.get("cryptoAvailableCashUseBuyingPower", True)) if isinstance(guards, dict) else True
                cash_raw = float(account.get("cash", "0") or 0.0)
                bp_raw = float(account.get("buying_power", "0") or 0.0)
                spendable = max(bp_raw if use_buying_power else cash_raw, 0.0)
                spendable = spendable * max(0.0, 1.0 - (cash_buffer_pct / 100.0))
                available_cash_cap = spendable
        except Exception:
            available_cash_cap = None

    stale_minutes = int(policy_effective.get("cancelOpenOrdersAfterMinutes", 20))
    cancel_open_before_run = bool(policy_effective.get("cancelOpenOrdersBeforeRun", True))
    cancelled_order_ids: list[str] = []
    if execute and cancel_open_before_run and stale_minutes > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
        try:
            open_orders = client.get_orders(status="open", limit=200, direction="desc")
            for order in open_orders:
                if str(order.get("side", "")).lower() != "buy":
                    continue
                order_sym = str(order.get("symbol", ""))
                order_is_crypto = "/" in order_sym
                running_is_crypto = segment == "crypto"
                if order_is_crypto != running_is_crypto:
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
                        pass
        except Exception:
            pass

    if segment == "crypto":
        crypto_tf = str(segment_cfg.get("timeframe", policy_effective.get("cryptoSignalTimeframe", "5Min")))
        crypto_limit = int(segment_cfg.get("barsLimit", policy_effective.get("cryptoBarsLimit", 120)))
        bars = client.get_crypto_bars(universe, timeframe=crypto_tf, limit=crypto_limit)
    elif segment in ("largeCapStocks", "indexFunds"):
        bars = client.get_stock_bars(universe, timeframe="1Day", limit=40)
        # Alpaca can occasionally return only the latest bar for some symbols/feeds.
        # Backfill from yfinance so signal quality doesn't collapse to zero.
        shallow = [sym for sym in universe if len(bars.get(sym, [])) < 10]
        if shallow:
            yf_bars = _fetch_yfinance_daily_bars(shallow, limit=40)
            for sym in shallow:
                if len(bars.get(sym, [])) < 10 and len(yf_bars.get(sym, [])) >= 2:
                    bars[sym] = yf_bars[sym]
    else:
        snapshots = client.get_stock_snapshots(universe)
        bars = _bars_from_stock_snapshots(snapshots)

    bars_with_data = {sym: v for sym, v in bars.items() if v}
    if fear_active and segment == "crypto":
        fear_cfg = policy_effective.get("fearClimateMode", {})
        crypto_fear_cfg = fear_cfg.get("crypto", {}) if isinstance(fear_cfg, dict) else {}
        if bool(crypto_fear_cfg.get("benchmarkGateEnabled", True)):
            lookback_bars = int(crypto_fear_cfg.get("benchmarkLookbackBars", 3))
            min_ret_pct = float(crypto_fear_cfg.get("benchmarkMinReturnPct", -0.15))
            risk_off, benchmark_returns = _fear_benchmark_risk_off(
                bars_with_data,
                lookback_bars=lookback_bars,
                min_return_pct=min_ret_pct,
            )
            fear_meta["benchmark_returns_pct"] = benchmark_returns
            fear_meta["benchmark_risk_off"] = risk_off
            if risk_off:
                return {
                    "strategy": "rule_engine",
                    "account_status": account.get("status"),
                    "segment": segment,
                    "budget": budget,
                    "execute": execute,
                    "reason": "Fear climate benchmark gate blocked crypto entries.",
                    "orders_planned": [],
                    "orders_placed": [],
                    "order_errors": [],
                    "equity_mode": equity_mode,
                    "fear_climate": fear_meta,
                }

    # Duplicate/add-on controls:
    # - default mode: skip held + recently bought symbols
    # - aggressive mode: allow add-on buys with per-symbol cap and short cooldown
    held_symbols = {p.get("symbol") for p in positions if p.get("symbol")}
    cooldown_by_segment = policy_effective.get("buyCooldownMinutesBySegment", {})
    cooldown_minutes = int(cooldown_by_segment.get(segment, policy_effective.get("buyCooldownMinutes", 180))) if isinstance(cooldown_by_segment, dict) else int(policy_effective.get("buyCooldownMinutes", 180))
    cooldown_cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
    allow_add_on_buys = bool(policy_effective.get("allowAddOnBuys", False))
    max_entries_by_segment = policy_effective.get("maxEntriesPerSymbolBySegment", {})
    max_entries_per_symbol = int(max_entries_by_segment.get(segment, policy_effective.get("maxEntriesPerSymbol", 3))) if isinstance(max_entries_by_segment, dict) else int(policy_effective.get("maxEntriesPerSymbol", 3))
    add_on_cd_by_segment = policy_effective.get("addOnBuyCooldownMinutesBySegment", {})
    add_on_cooldown_minutes = int(add_on_cd_by_segment.get(segment, policy_effective.get("addOnBuyCooldownMinutes", 8))) if isinstance(add_on_cd_by_segment, dict) else int(policy_effective.get("addOnBuyCooldownMinutes", 8))
    entry_lookback_by_segment = policy_effective.get("entryCountLookbackHoursBySegment", {})
    entry_lookback_hours = int(entry_lookback_by_segment.get(segment, policy_effective.get("entryCountLookbackHours", 48))) if isinstance(entry_lookback_by_segment, dict) else int(policy_effective.get("entryCountLookbackHours", 48))
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
        risk_policy=policy_effective,
        segment=segment,
        available_cash_cap=available_cash_cap,
    )

    placed: list[dict[str, Any]] = []
    order_errors: list[dict[str, Any]] = []
    if execute:
        tp_by_segment = policy_effective.get("takeProfitPctBySegment", {})
        sl_by_segment = policy_effective.get("stopLossPctBySegment", {})
        tp_pct = float(tp_by_segment.get(segment, policy_effective.get("takeProfitPct", 4.5))) if isinstance(tp_by_segment, dict) else float(policy_effective.get("takeProfitPct", 4.5))
        sl_pct = float(sl_by_segment.get(segment, policy_effective.get("stopLossPct", 2.2))) if isinstance(sl_by_segment, dict) else float(policy_effective.get("stopLossPct", 2.2))
        equity_brackets_enabled = bool(policy_effective.get("equityBracketsEnabled", False))
        order_defaults = (policy_effective.get("orderDefaults") or {})
        segment_order_defaults = order_defaults.get("crypto" if segment == "crypto" else "stocks", {})
        default_order_type = str(segment_order_defaults.get("type", "limit")).lower()
        if default_order_type not in {"limit", "market"}:
            default_order_type = "limit"
        default_tif = str(segment_order_defaults.get("timeInForce", "gtc" if segment == "crypto" else "day")).lower()
        for order in orders:
            side = "buy"
            tif = default_tif
            tp, sl = _price_targets(order.limit_price, tp_pct, sl_pct)
            use_bracket = segment != "crypto" and equity_brackets_enabled
            try:
                placed.append(
                    client.place_order(
                        symbol=order.symbol,
                        qty=order.qty,
                        side=side,
                        order_type=default_order_type,
                        tif=tif,
                        limit_price=order.limit_price if default_order_type == "limit" else None,
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
        "equity_mode": equity_mode,
        "fear_climate": fear_meta,
    }
