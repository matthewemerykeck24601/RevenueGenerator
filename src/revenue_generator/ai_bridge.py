from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .alpaca_client import AlpacaClient
from .bot import SEGMENT_UNIVERSE, run_once
from .external_research import discover_segment_candidates
from .equity_mode import apply_equity_mode_switch


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
        score = max(0.0, conf) * max(0.02, edge)
        if score > best:
            best = score
    return best


def _log_ai_call(
    *,
    provider: str,
    model: str,
    segment: str,
    budget: float,
    prompt: str,
    status: str,
    response_text: str = "",
    error: str = "",
    elapsed_ms: int | None = None,
) -> None:
    path = Path("logs") / "ai_calls.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "provider": provider,
        "model": model,
        "segment": segment,
        "budget": budget,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8", errors="replace")).hexdigest(),
        "prompt": prompt,
        "response_text": response_text,
        "error": error,
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def fetch_market_context(symbols: list[str], segment: str) -> str:
    """Fetch recent price/volume context for AI prompt enrichment using yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        return ""

    lines: list[str] = []
    lines.append(f"Live market snapshot ({date.today().isoformat()}) for {segment}:")

    # Map crypto to Yahoo format
    yahoo_map = {
        "BTC/USD": "BTC-USD",
        "ETH/USD": "ETH-USD",
        "SOL/USD": "SOL-USD",
        "AVAX/USD": "AVAX-USD",
        "LTC/USD": "LTC-USD",
        "LINK/USD": "LINK-USD",
        "BCH/USD": "BCH-USD",
        "UNI/USD": "UNI-USD",
        "AAVE/USD": "AAVE-USD",
    }

    summary_rows: list[str] = []
    for sym in symbols[:8]:  # cap to avoid slow calls
        yahoo_sym = yahoo_map.get(sym, sym)
        try:
            ticker = yf.Ticker(yahoo_sym)
            hist = ticker.history(period="5d", interval="1d", auto_adjust=False)
            if hist is None or len(hist) < 2:
                continue
            closes = hist["Close"].dropna().tolist()
            vols = hist["Volume"].fillna(0).tolist()
            if len(closes) < 2:
                continue
            last = closes[-1]
            prev = closes[-2]
            ret_1d = ((last - prev) / prev * 100.0) if prev > 0 else 0.0
            ret_5d = ((last - closes[0]) / closes[0] * 100.0) if closes[0] > 0 else 0.0
            vol_today = vols[-1] if vols else 0
            vol_avg = sum(vols) / len(vols) if vols else 1
            vol_ratio = vol_today / max(vol_avg, 1)
            summary_rows.append(
                f"  {sym}: price={last:.4f}, 1d={ret_1d:+.2f}%, 5d={ret_5d:+.2f}%, vol_ratio={vol_ratio:.2f}x"
            )
        except Exception:
            continue

    if not summary_rows:
        return ""

    lines.extend(summary_rows)
    lines.append("Use this data to assess momentum quality before deciding.")
    return "\n".join(lines)


def build_ai_prompt(
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
    min_net_edge = float(ai_cfg.get("minExpectedEdgeNetForBuy", max(min_edge, 0.0)))

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


def run_ai_analysis(
    *,
    segment: str,
    budget: float,
    allowed_symbols: list[str],
    risk_policy: dict[str, Any],
    timeout_seconds: int = 35,
    rule_based_signals: list[dict[str, Any]] | None = None,
    market_context: str = "",
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    prompt = build_ai_prompt(
        segment=segment,
        budget=budget,
        allowed_symbols=allowed_symbols,
        risk_policy=risk_policy,
        rule_based_signals=rule_based_signals,
        market_context=market_context,
    )
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()

    if anthropic_key:
        provider = "anthropic"
        model = os.getenv("AI_MODEL", "claude-sonnet-4-6").strip() or "claude-sonnet-4-6"
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": model,
            "max_tokens": 900,
            "temperature": 0.2,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=max(5, timeout_seconds))
            resp.raise_for_status()
            data = resp.json()
            content = data.get("content", [])
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(str(block.get("text", "")))
            response_text = "\n".join([t for t in text_parts if t]).strip()
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment=segment,
                budget=budget,
                prompt=prompt,
                status="ok",
                response_text=response_text,
                elapsed_ms=elapsed_ms,
            )
            return _extract_json_object(response_text)
        except Exception as err:
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment=segment,
                budget=budget,
                prompt=prompt,
                status="error",
                error=str(err),
                elapsed_ms=elapsed_ms,
            )
            raise RuntimeError(f"anthropic call failed: {err}") from err

    if openai_key:
        provider = "openai"
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "temperature": 0.2,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=max(5, timeout_seconds))
            resp.raise_for_status()
            data = resp.json()
            choices = data.get("choices", [])
            response_text = ""
            if choices:
                response_text = str(((choices[0] or {}).get("message") or {}).get("content") or "").strip()
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment=segment,
                budget=budget,
                prompt=prompt,
                status="ok",
                response_text=response_text,
                elapsed_ms=elapsed_ms,
            )
            return _extract_json_object(response_text)
        except Exception as err:
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment=segment,
                budget=budget,
                prompt=prompt,
                status="error",
                error=str(err),
                elapsed_ms=elapsed_ms,
            )
            raise RuntimeError(f"openai call failed: {err}") from err

    raise RuntimeError("No AI provider key configured. Set ANTHROPIC_API_KEY or OPENAI_API_KEY in .env.")


def validate_and_plan_signal(
    *,
    signal: dict[str, Any],
    budget: float,
    segment: str,
    risk_policy: dict[str, Any],
    account: dict[str, Any],
    open_positions: list[dict[str, Any]],
    client: AlpacaClient,
    allowed_symbols: list[str] | None = None,
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

    allowed_symbols_effective = allowed_symbols or (segment_cfg.get("symbolsAllowlist") or SEGMENT_UNIVERSE.get(segment, []))
    if symbol not in allowed_symbols_effective:
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

    if decision == "BUY" and expected_edge < min_edge:
        return AiSignalDecision(False, "Signal does not pass gross edge threshold.", normalized, None)
    if decision == "BUY" and net_expected_edge < min_net_edge:
        return AiSignalDecision(False, "Signal does not pass net edge threshold after execution costs.", normalized, None)

    raw_qty = alloc / latest_price
    is_crypto = "/" in symbol
    allow_fractional_stocks = bool(risk_policy.get("allowFractionalStocks", True))
    qty: float | int
    if is_crypto:
        qty = round(raw_qty, 6)
        if qty <= 0:
            return AiSignalDecision(False, "Calculated crypto quantity is zero.", normalized, None)
    elif allow_fractional_stocks:
        qty = round(raw_qty, 6)
        if qty <= 0:
            return AiSignalDecision(False, "Calculated stock fractional quantity is zero.", normalized, None)
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
    account_snapshot = client.get_account()
    equity_mode = apply_equity_mode_switch(risk_policy, account=account_snapshot)
    ai_cfg = risk_policy.get("aiScheduler", {})
    timeout_seconds = int(ai_cfg.get("aiTimeoutSeconds", ai_cfg.get("openclawTimeoutSeconds", 35)))
    segment_cfg = (risk_policy.get("allowedSegments") or {}).get(segment, {})
    base_symbols = segment_cfg.get("symbolsAllowlist") or SEGMENT_UNIVERSE.get(segment, [])
    discovery_cfg = risk_policy.get("marketDiscovery", {})
    if bool(discovery_cfg.get("enabled", False)):
        top_n = int(discovery_cfg.get("topCandidatesPerSegment", 20))
        allowed_symbols = discover_segment_candidates(
            segment=segment,
            base_symbols=list(base_symbols),
            top_n=top_n,
            discovery_cfg=discovery_cfg,
        )
    else:
        allowed_symbols = list(base_symbols)
    min_budget = float(ai_cfg.get("minBudgetUsd", 25.0))
    if budget < min_budget:
        return {
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "ai_used": False,
            "reason": f"Budget below AI min threshold ({min_budget}).",
            "equity_mode": equity_mode,
        }
    if not allowed_symbols:
        return {
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "ai_used": False,
            "reason": "No symbols configured for segment.",
            "equity_mode": equity_mode,
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
            f"signals_considered={preview.get('signals_considered', 0)}.\n" + fetch_market_context(allowed_symbols, segment)
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

    signal = run_ai_analysis(
        segment=segment,
        budget=budget,
        allowed_symbols=allowed_symbols,
        risk_policy=risk_policy,
        timeout_seconds=timeout_seconds,
        rule_based_signals=rule_based_signals,
        market_context=market_context,
    )
    account = account_snapshot
    positions = client.get_open_positions()
    decision = validate_and_plan_signal(
        signal=signal,
        budget=budget,
        segment=segment,
        risk_policy=risk_policy,
        account=account,
        open_positions=positions,
        client=client,
        allowed_symbols=allowed_symbols,
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
        "equity_mode": equity_mode,
    }

