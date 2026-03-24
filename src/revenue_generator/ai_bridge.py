from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .alpaca_client import AlpacaClient
from .bot import SEGMENT_UNIVERSE, run_once


@dataclass
class AiSignalDecision:
    allowed: bool
    reason: str
    normalized_signal: dict[str, Any]
    planned_order: dict[str, Any] | None


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for idx in range(len(text)):
            if text[idx] != "{":
                continue
            try:
                obj, _end = decoder.raw_decode(text[idx:])
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                continue
    raise ValueError("No valid JSON object found in model output.")


def _best_rule_prefilter_score(candidates: list[dict[str, Any]]) -> float:
    best = 0.0
    for item in candidates:
        conf = _to_float(item.get("confidence"))
        edge = _to_float(item.get("expected_edge"))
        score = max(0.0, conf) * max(0.0, edge)
        if score > best:
            best = score
    return best


def build_openclaw_prompt(
    *,
    segment: str,
    budget: float,
    allowed_symbols: list[str],
    risk_policy: dict[str, Any],
    rule_based_signals: list[dict[str, Any]] | None = None,
    market_context: str = "",
) -> str:
    max_pos = float(risk_policy.get("maxPositionSizePct", 8.0))
    max_open = int(risk_policy.get("maxOpenPositions", 15))
    stop_loss = float(risk_policy.get("stopLossPct", 2.2))
    tp1 = float((risk_policy.get("exitHooks") or {}).get("firstTargetPct", 3.0))
    tp2 = float((risk_policy.get("exitHooks") or {}).get("secondTargetPct", 6.0))
    ai_cfg = risk_policy.get("aiScheduler", {})
    min_conf = float(ai_cfg.get("minConfidenceForBuy", 0.70))
    min_edge = float(ai_cfg.get("minExpectedEdgeForBuy", 0.05))

    rule_context = ""
    if rule_based_signals:
        rule_context = (
            "\nRule-based preliminary signals (use as strong prior, but override if you see better edge):\n"
            + json.dumps(rule_based_signals, indent=2)
        )

    return f"""
You are a conservative, risk-aware trading signal generator for live execution on Alpaca.

Segment: {segment} (focus especially on penny stocks if applicable — they are volatile, illiquid, prone to pumps/dumps and manipulation).

Current budget: ${budget:.2f}
Max position size % of budget: {max_pos:.2f}%
Max open positions: {max_open}
Allowed symbols: {", ".join(allowed_symbols)}
Baseline hard stop-loss: -{stop_loss:.2f}%
First target: +{tp1:.2f}% -> scale out 40%
Second target: +{tp2:.2f}% -> scale out another 30%
Trailing stop: 1.8%

{rule_context}

{market_context}

Strict rules for LIVE trading:
- Only BUY if you see a clear, realistic positive edge AFTER accounting for spreads/slippage (pennies often have 5-20% effective spreads).
- Confidence must be >= {min_conf:.2f} and expected_edge >= {min_edge:.2f} for any BUY.
- If edge is marginal or market looks choppy/manipulated, return HOLD.
- Never exceed position limits or use symbols outside allowlist.
- For penny stocks, be extra skeptical of volume spikes without fundamental/news catalyst.
- For HOLD, set "symbol" to null and position_size_pct_of_budget to 0.
- Use exact symbol formatting from allowlist (for example BTC/USD, not BTCUSDT).
- Output ONLY valid JSON, no extra text.

Schema (exact):
{{
  "decision": "BUY|HOLD",
  "symbol": "SYMBOL_OR_NULL",
  "segment": "{segment}",
  "confidence": 0.0,
  "expected_edge": 0.0,
  "position_size_pct_of_budget": 0.0,
  "entry": {{
    "order_type": "limit|market",
    "limit_price": 0.0,
    "time_in_force": "gtc|day"
  }},
  "reasoning": ["bullet 1", "bullet 2", "bullet 3"]
}}
""".strip()


def run_openclaw_analysis(
    *,
    segment: str,
    budget: float,
    allowed_symbols: list[str],
    risk_policy: dict[str, Any],
    timeout_seconds: int = 35,
    rule_based_signals: list[dict[str, Any]] | None = None,
    market_context: str = "",
) -> dict[str, Any]:
    prompt = build_openclaw_prompt(
        segment=segment,
        budget=budget,
        allowed_symbols=allowed_symbols,
        risk_policy=risk_policy,
        rule_based_signals=rule_based_signals,
        market_context=market_context,
    )
    openclaw_bin = shutil.which("openclaw")
    if not openclaw_bin:
        fallback = os.path.expanduser(r"~\AppData\Roaming\npm\openclaw.cmd")
        openclaw_bin = fallback if os.path.exists(fallback) else "openclaw"

    cmd = [
        openclaw_bin,
        "agent",
        "--local",
        "--agent",
        "main",
        "--message",
        prompt,
        "--json",
    ]
    if os.name == "nt":
        command_str = subprocess.list2cmdline(cmd)
        proc = subprocess.run(
            command_str,
            capture_output=True,
            text=True,
            check=False,
            shell=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(5, timeout_seconds),
        )
    else:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            encoding="utf-8",
            errors="replace",
            timeout=max(5, timeout_seconds),
        )
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    if proc.returncode != 0:
        raise RuntimeError(stderr.strip() or stdout.strip() or "openclaw agent failed")
    return _extract_json_object(stdout)


def validate_and_plan_signal(
    *,
    signal: dict[str, Any],
    budget: float,
    segment: str,
    risk_policy: dict[str, Any],
    account: dict[str, Any],
    open_positions: list[dict[str, Any]],
    client: AlpacaClient,
) -> AiSignalDecision:
    decision = str(signal.get("decision", "HOLD")).upper()
    symbol = str(signal.get("symbol") or "")
    confidence = _to_float(signal.get("confidence"))
    expected_edge = _to_float(signal.get("expected_edge"))
    size_pct = _to_float(signal.get("position_size_pct_of_budget"))

    normalized = {
        "decision": decision,
        "symbol": symbol,
        "segment": segment,
        "confidence": confidence,
        "expected_edge": expected_edge,
        "position_size_pct_of_budget": size_pct,
        "entry": signal.get("entry", {}),
        "reasoning": signal.get("reasoning", []),
    }

    segment_cfg = (risk_policy.get("allowedSegments") or {}).get(segment, {})
    if not segment_cfg.get("enabled", True):
        return AiSignalDecision(False, f"Segment '{segment}' disabled in risk policy.", normalized, None)

    if decision == "HOLD":
        return AiSignalDecision(False, "Model returned HOLD.", normalized, None)
    if decision not in {"BUY", "SELL"}:
        return AiSignalDecision(False, "Invalid decision in signal.", normalized, None)

    allowed_symbols = segment_cfg.get("symbolsAllowlist") or SEGMENT_UNIVERSE.get(segment, [])
    if symbol not in allowed_symbols:
        return AiSignalDecision(False, f"Symbol '{symbol}' not allowed for segment '{segment}'.", normalized, None)

    ai_cfg = risk_policy.get("aiScheduler", {})
    min_conf = float(ai_cfg.get("minConfidenceForBuy", 0.70))
    min_edge = float(ai_cfg.get("minExpectedEdgeForBuy", 0.05))
    if confidence < min_conf:
        return AiSignalDecision(False, "Signal does not pass confidence threshold.", normalized, None)

    max_open = int(risk_policy.get("maxOpenPositions", 3))
    if len(open_positions) >= max_open and decision == "BUY":
        return AiSignalDecision(False, "Max open positions reached.", normalized, None)

    max_size_pct = float(risk_policy.get("maxPositionSizePct", 12.0))
    if size_pct <= 0 or size_pct > max_size_pct:
        return AiSignalDecision(False, f"Position size pct {size_pct:.2f} exceeds policy max {max_size_pct:.2f}.", normalized, None)

    equity = _to_float(account.get("equity"), budget)
    if equity <= 0:
        equity = budget
    alloc = min(budget * (size_pct / 100.0), equity * (size_pct / 100.0))
    if alloc <= 0:
        return AiSignalDecision(False, "Calculated allocation is zero.", normalized, None)

    latest_price = client.get_latest_price(symbol)
    if latest_price is None or latest_price <= 0:
        return AiSignalDecision(False, f"Could not fetch latest price for {symbol}.", normalized, None)

    entry = signal.get("entry", {}) or {}
    order_type = str(entry.get("order_type", "limit")).lower()
    if order_type not in {"limit", "market"}:
        order_type = "limit"
    tif = str(entry.get("time_in_force", "gtc" if "/" in symbol else "day")).lower()
    limit_price = _to_float(entry.get("limit_price"), latest_price)
    if order_type == "limit" and limit_price <= 0:
        limit_price = latest_price

    # Subtract estimated spread/slippage cost before approving edge.
    max_spread_bps = float(segment_cfg.get("maxSpreadBps", 40.0))
    quoted_spread_bps = (
        abs(limit_price - latest_price) / latest_price * 10000.0 if latest_price > 0 else max_spread_bps
    )
    baseline_spread_bps = max_spread_bps * 0.35
    effective_spread_bps = min(max_spread_bps, max(quoted_spread_bps, baseline_spread_bps))
    slippage_buffer_bps = float(
        ai_cfg.get("slippageBufferBpsCrypto", 12.0) if "/" in symbol else ai_cfg.get("slippageBufferBpsStocks", 6.0)
    )
    estimated_cost_pct = (effective_spread_bps + slippage_buffer_bps) / 10000.0
    net_expected_edge = expected_edge - estimated_cost_pct
    normalized["estimated_cost_pct"] = estimated_cost_pct
    normalized["expected_edge_net"] = net_expected_edge

    if decision == "BUY" and net_expected_edge <= min_edge:
        return AiSignalDecision(False, "Signal does not pass net edge threshold after execution costs.", normalized, None)

    raw_qty = alloc / latest_price
    is_crypto = "/" in symbol
    qty: float | int
    if is_crypto:
        qty = round(raw_qty, 6)
        if qty <= 0:
            return AiSignalDecision(False, "Calculated crypto quantity is zero.", normalized, None)
    else:
        qty = int(raw_qty)
        if qty < 1:
            return AiSignalDecision(False, "Calculated stock quantity is less than one share.", normalized, None)

    planned = {
        "symbol": symbol,
        "side": "buy" if decision == "BUY" else "sell",
        "qty": qty,
        "order_type": order_type,
        "time_in_force": tif,
        "limit_price": round(limit_price, 6),
        "latest_price": latest_price,
        "allocation": round(alloc, 2),
        "expected_edge": expected_edge,
        "expected_edge_net": round(net_expected_edge, 6),
        "estimated_cost_pct": round(estimated_cost_pct, 6),
    }
    return AiSignalDecision(True, "Signal approved by risk gate.", normalized, planned)


def run_ai_cycle(
    *,
    client: AlpacaClient,
    risk_policy: dict[str, Any],
    segment: str,
    budget: float,
    execute: bool,
) -> dict[str, Any]:
    ai_cfg = risk_policy.get("aiScheduler", {})
    timeout_seconds = int(ai_cfg.get("openclawTimeoutSeconds", 35))
    segment_cfg = (risk_policy.get("allowedSegments") or {}).get(segment, {})
    allowed_symbols = segment_cfg.get("symbolsAllowlist") or SEGMENT_UNIVERSE.get(segment, [])
    min_budget = float(ai_cfg.get("minBudgetUsd", 25.0))
    if budget < min_budget:
        return {
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "ai_used": False,
            "reason": f"Budget below AI min threshold ({min_budget}).",
        }
    if not allowed_symbols:
        return {
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "ai_used": False,
            "reason": "No symbols configured for segment.",
        }

    # Optional pre-run cleanup to avoid cash lockup from stale buy orders.
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
                created_at = order.get("created_at") or order.get("submitted_at")
                if not created_at:
                    continue
                dt = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
                if dt <= cutoff:
                    try:
                        client.cancel_order(str(order_id))
                        cancelled_order_ids.append(str(order_id))
                    except Exception:
                        pass
        except Exception:
            pass

    rule_based_signals: list[dict[str, Any]] = []
    market_context = ""
    preview: dict[str, Any] | None = None
    try:
        preview = run_once(
            client=client,
            risk_policy=risk_policy,
            segment=segment,
            budget=budget,
            execute=False,
        )
        for item in (preview.get("orders_planned") or [])[:5]:
            rule_based_signals.append(
                {
                    "symbol": item.get("symbol"),
                    "confidence": _to_float(item.get("confidence")),
                    "expected_edge": _to_float(item.get("expected_edge")),
                    "limit_price": _to_float(item.get("limit_price")),
                    "allocation": _to_float(item.get("allocation")),
                }
            )
        market_context = (
            f"Rule engine context: universe_size={preview.get('universe_size', 0)}, "
            f"symbols_with_data={preview.get('symbols_with_data', 0)}, "
            f"signals_considered={preview.get('signals_considered', 0)}."
        )
    except Exception:
        rule_based_signals = []
        market_context = ""

    prefilter_threshold = float(ai_cfg.get("prefilterScoreThreshold", 0.035))
    best_prefilter_score = _best_rule_prefilter_score(rule_based_signals)
    if best_prefilter_score < prefilter_threshold:
        if preview is None:
            preview = {
                "strategy": "rule_engine",
                "segment": segment,
                "budget": budget,
                "execute": execute,
                "orders_planned": [],
                "orders_placed": [],
                "order_errors": [],
            }
        preview = dict(preview)
        preview["ai_used"] = False
        preview["ai_prefilter_source"] = "rule_engine"
        preview["ai_prefilter_threshold"] = prefilter_threshold
        preview["ai_prefilter_best_score"] = round(best_prefilter_score, 6)
        preview["ai_skipped_reason"] = "prefilter_below_threshold"
        return preview

    signal = run_openclaw_analysis(
        segment=segment,
        budget=budget,
        allowed_symbols=allowed_symbols,
        risk_policy=risk_policy,
        timeout_seconds=timeout_seconds,
        rule_based_signals=rule_based_signals,
        market_context=market_context,
    )
    account = client.get_account()
    positions = client.get_open_positions()
    decision = validate_and_plan_signal(
        signal=signal,
        budget=budget,
        segment=segment,
        risk_policy=risk_policy,
        account=account,
        open_positions=positions,
        client=client,
    )

    placed = None
    place_error = None
    if decision.allowed and decision.planned_order and execute:
        planned = decision.planned_order
        try:
            placed = client.place_order(
                symbol=planned["symbol"],
                qty=planned["qty"],
                side=planned["side"],
                order_type=planned["order_type"],
                tif=planned["time_in_force"],
                limit_price=planned["limit_price"] if planned["order_type"] == "limit" else None,
            )
        except Exception as err:
            place_error = str(err)

    return {
        "strategy": "ai_direct",
        "account_status": account.get("status"),
        "segment": segment,
        "budget": budget,
        "execute": execute,
        "ai_used": True,
        "stale_orders_cancelled": len(cancelled_order_ids),
        "ai_signal": decision.normalized_signal,
        "ai_allowed": decision.allowed,
        "ai_reason": decision.reason,
        "orders_planned": [decision.planned_order] if decision.planned_order else [],
        "orders_placed": [placed] if placed else [],
        "order_errors": ([{"symbol": decision.planned_order.get("symbol"), "error": place_error}] if place_error and decision.planned_order else []),
    }

