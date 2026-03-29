from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .alpaca_client import AlpacaClient
from .bot import SEGMENT_UNIVERSE, run_once
from .external_research import discover_segment_candidates
from .equity_mode import apply_equity_mode_switch
from .fear_climate import apply_fear_climate_overrides, load_fear_climate_state


@dataclass
class AiSignalDecision:
    allowed: bool
    reason: str
    normalized_signal: dict[str, Any]
    planned_order: dict[str, Any] | None


_AI_COOLDOWN_UNTIL: dict[str, datetime] = {}
_AI_CONSEC_ERRORS: dict[str, int] = {}


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
    prompt_version: str = "",
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
        "prompt_version": prompt_version,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8", errors="replace")).hexdigest(),
        "prompt": prompt,
        "response_text": response_text,
        "error": error,
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def fetch_market_context(symbols: list[str], segment: str, client: Any = None, risk_policy: dict[str, Any] | None = None) -> str:
    """Fetch recent price/volume context for AI prompt. Uses Kraken data for crypto if available."""
    lines: list[str] = []
    lines.append(f"Live market snapshot ({date.today().isoformat()}) for {segment}:")
    summary_rows: list[str] = []
    covered: set[str] = set()

    from .kraken_client import KrakenClient as _KC
    if segment == "crypto" and isinstance(client, _KC):
        try:
            ai_cfg = (risk_policy or {}).get("aiScheduler", {})
            ctx_tf = str(ai_cfg.get("cryptoContextTimeframe", "15Min"))
            ctx_limit = int(ai_cfg.get("cryptoContextBars", 16))
            bars_map = client.get_crypto_bars(symbols[:12], timeframe=ctx_tf, limit=ctx_limit)
            for sym in symbols[:12]:
                bars = bars_map.get(sym, [])
                if len(bars) < 2:
                    continue
                last = bars[-1]["c"]
                prev = bars[-2]["c"]
                first = bars[0]["c"]
                ret_1d = ((last - prev) / prev * 100) if prev > 0 else 0
                ret_5d = ((last - first) / first * 100) if first > 0 else 0
                vols = [b["v"] for b in bars]
                vol_avg = sum(vols) / len(vols) if vols else 1
                vol_ratio = vols[-1] / max(vol_avg, 1) if vols else 0
                summary_rows.append(
                    f"  {sym}: price={last:.4f}, 1d={ret_1d:+.2f}%, 5d={ret_5d:+.2f}%, vol_ratio={vol_ratio:.2f}x"
                )
                covered.add(sym)
        except Exception:
            pass

    try:
        import yfinance as yf
    except ImportError:
        yf = None

    if yf:
        from .external_research import CRYPTO_TO_YAHOO
        yahoo_map = dict(CRYPTO_TO_YAHOO)
        for sym in symbols[:12]:
            if sym in covered:
                continue
            if "/" in sym and sym not in yahoo_map:
                yahoo_sym = sym.replace("/", "-")
            else:
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
    # Extract risk parameters (same as current)
    max_pos_by_segment = risk_policy.get("maxPositionSizePctBySegment", {})
    max_pos = float(max_pos_by_segment.get(segment, risk_policy.get("maxPositionSizePct", 8.0)))
    max_open = int(risk_policy.get("maxOpenPositions", 15))
    stop_loss = float(risk_policy.get("stopLossPct", 2.2))
    exit_hooks = risk_policy.get("exitHooks") or {}
    tp1 = float(exit_hooks.get("firstTargetPct", 3.0))
    tp2 = float(exit_hooks.get("secondTargetPct", 6.0))
    trailing_stop = float(exit_hooks.get("trailingStopPct", 2.0))

    ai_cfg = risk_policy.get("aiScheduler", {})
    min_conf_by_segment = ai_cfg.get("minConfidenceForBuyBySegment", {})
    min_edge_by_segment = ai_cfg.get("minExpectedEdgeForBuyBySegment", {})
    min_net_edge_by_segment = ai_cfg.get("minExpectedEdgeNetForBuyBySegment", {})

    min_conf = float(min_conf_by_segment.get(segment, ai_cfg.get("minConfidenceForBuy", 0.75)))
    min_edge = float(min_edge_by_segment.get(segment, ai_cfg.get("minExpectedEdgeForBuy", 0.03)))
    min_net_edge = float(min_net_edge_by_segment.get(segment, ai_cfg.get("minExpectedEdgeNetForBuy", 0.015)))

    est_cost_bps = 25 if segment == "penny" else 8  # realistic slippage/spread estimate

    # Build rule context
    rule_context = ""
    if rule_based_signals:
        rule_context = "Rule-based signals (use as strong prior):\n"
        for sig in rule_based_signals[:8]:  # limit to avoid token bloat
            conf = _to_float(sig.get("confidence"))
            edge = _to_float(sig.get("expected_edge"))
            rule_context += f"- {sig.get('symbol')}: conf={conf:.2f}, edge={edge:.3f}, reason={sig.get('reason','')}\n"

    # Market context is passed in (already rich)

    prompt = f"""You are an elite, conservative-yet-opportunistic trading signal generator for live Alpaca execution. Your goal: maximize positive-expectancy trades while strictly respecting all risk limits.

Segment: {segment} (be extra skeptical on penny stocks — require real catalyst + volume confirmation, ignore random pumps).

Current budget: ${budget:.2f}
Max position size: {max_pos:.2f}% of budget
Max open positions: {max_open}
Allowed symbols: {", ".join(allowed_symbols)}
Hard stop-loss: -{stop_loss:.2f}%
First target: +{tp1:.2f}% (scale out 40%)
Second target: +{tp2:.2f}% (scale out 30%)
Trailing stop: {trailing_stop:.1f}%

{rule_context}

{market_context}

=== STRICT LIVE RULES ===
- ONLY output BUY if there is a **clear realistic edge** AFTER spreads, slippage, and commissions.
- Requirements: confidence ≥ {min_conf:.2f}, gross expected_edge ≥ {min_edge:.2f}, net expected_edge (after ~{est_cost_bps} bps costs) ≥ {min_net_edge:.4f}.
- Setup must show favorable risk/reward aligned with your stop-loss and targets.
- If multiple candidates qualify, select ONLY the single highest-conviction one.
- For penny stocks: demand strong volume surge + catalyst. Low-volume spikes = HOLD.
- Marginal, choppy, or manipulated setups → HOLD.
- Never exceed position limits or use unlisted symbols.

=== OUTPUT ONLY VALID JSON (nothing else) ===
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
  "reasoning": ["short bullet 1", "short bullet 2", "short bullet 3"]
}}

=== FEW-SHOT EXAMPLES ===

Strong BUY example:
{{
  "decision": "BUY",
  "symbol": "AAPL",
  "segment": "stocks",
  "confidence": 0.87,
  "expected_edge": 0.058,
  "position_size_pct_of_budget": 5.2,
  "entry": {{"order_type": "limit", "limit_price": 0.0, "time_in_force": "gtc"}},
  "reasoning": [
    "Strong momentum: +4.8% 1d on 2.3x avg volume, breakout above resistance",
    "Rule signal + market context align, risk/reward ~1:2.9 vs stop and targets",
    "Net edge after costs = +5.4% — highest conviction setup"
  ]
}}

Safe HOLD example:
{{
  "decision": "HOLD",
  "symbol": null,
  "segment": "crypto",
  "confidence": 0.48,
  "expected_edge": 0.009,
  "position_size_pct_of_budget": 0.0,
  "entry": {{"order_type": "limit", "limit_price": 0.0, "time_in_force": "gtc"}},
  "reasoning": [
    "Choppy price action, volume not confirming",
    "Edge fails to clear cost hurdle after slippage",
    "No catalyst — better to wait"
  ]
}}

Now analyze the rule signals and market data above. Output ONLY the JSON.
"""
    return prompt.strip()


def build_ai_exit_prompt(
    *,
    symbol: str,
    segment: str,
    trigger: str,
    entry_price: float,
    current_price: float,
    pnl_pct: float,
    peak_price: float,
    peak_drop_pct: float,
    held_minutes: float,
    thresholds: dict[str, float],
) -> str:
    return f"""
You are an EXIT advisor for a live trading bot. Return HOLD only when there is a strong, near-term rebound case.

Symbol: {symbol}
Segment: {segment}
Current trigger candidate: {trigger}
Entry price: {entry_price:.8f}
Current price: {current_price:.8f}
Unrealized pnl_pct: {pnl_pct:.4f}
Peak price since tracked: {peak_price:.8f}
Drawdown from peak_pct: {peak_drop_pct:.4f}
Held minutes: {held_minutes:.2f}

Active thresholds:
- first_target_pct: {thresholds.get("first_target_pct", 0.0):.4f}
- second_target_pct: {thresholds.get("second_target_pct", 0.0):.4f}
- trailing_stop_pct: {thresholds.get("trailing_stop_pct", 0.0):.4f}
- break_even_buffer_pct: {thresholds.get("break_even_buffer_pct", 0.0):.4f}
- hard_stop_loss_pct: {thresholds.get("hard_stop_loss_pct", 0.0):.4f}

Hard risk constraints:
- hard_stop_loss is always absolute and cannot be deferred.
- Be conservative: if uncertainty is high, prefer EXIT.
- Output JSON only, no extra text.

Schema (exact):
{{
  "decision": "EXIT|HOLD",
  "confidence": 0.0,
  "expected_rebound_pct": 0.0,
  "reasoning": ["bullet 1", "bullet 2"]
}}
""".strip()


def run_ai_exit_advisor(
    *,
    symbol: str,
    segment: str,
    trigger: str,
    entry_price: float,
    current_price: float,
    pnl_pct: float,
    peak_price: float,
    peak_drop_pct: float,
    held_minutes: float,
    thresholds: dict[str, float],
    timeout_seconds: int = 8,
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    prompt = build_ai_exit_prompt(
        symbol=symbol,
        segment=segment,
        trigger=trigger,
        entry_price=entry_price,
        current_price=current_price,
        pnl_pct=pnl_pct,
        peak_price=peak_price,
        peak_drop_pct=peak_drop_pct,
        held_minutes=held_minutes,
        thresholds=thresholds,
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
            "max_tokens": 250,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=max(4, timeout_seconds))
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
                segment=f"exit:{segment}",
                budget=0.0,
                prompt=prompt,
                status="ok",
                response_text=response_text,
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PROMPT_VERSION", "exit-single-v1"),
            )
            return _extract_json_object(response_text)
        except Exception as err:
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment=f"exit:{segment}",
                budget=0.0,
                prompt=prompt,
                status="error",
                error=str(err),
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PROMPT_VERSION", "exit-single-v1"),
            )
            raise RuntimeError(f"anthropic exit advisor call failed: {err}") from err

    if openai_key:
        provider = "openai"
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 220,
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=max(4, timeout_seconds))
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
                segment=f"exit:{segment}",
                budget=0.0,
                prompt=prompt,
                status="ok",
                response_text=response_text,
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PROMPT_VERSION", "exit-single-v1"),
            )
            return _extract_json_object(response_text)
        except Exception as err:
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment=f"exit:{segment}",
                budget=0.0,
                prompt=prompt,
                status="error",
                error=str(err),
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PROMPT_VERSION", "exit-single-v1"),
            )
            raise RuntimeError(f"openai exit advisor call failed: {err}") from err

    raise RuntimeError("No AI provider key configured for exit advisor.")


def build_ai_exit_portfolio_prompt(
    *,
    positions: list[dict[str, Any]],
    default_sell_pct: float,
) -> str:
    compact = []
    for p in positions[:60]:
        compact.append(
            {
                "symbol": p.get("symbol"),
                "segment": p.get("segment"),
                "pnl_pct": p.get("pnl_pct"),
                "held_minutes": p.get("held_minutes"),
                "peak_drop_pct": p.get("peak_drop_pct"),
                "entry_price": p.get("entry_price"),
                "current_price": p.get("current_price"),
                "qty": p.get("qty"),
                "rule_candidate_trigger": p.get("rule_candidate_trigger"),
            }
        )
    return f"""
You are a conservative EXIT decision model for a live trading bot.
For each position below, decide SELL or HOLD.

Rules:
- SELL only when downside risk is likely to dominate near-term rebound probability.
- HOLD when trend/momentum likely supports recovery.
- Output JSON only.
- Use symbols exactly as provided.
- If SELL, choose sell_pct 1-100. Use {default_sell_pct:.1f} as default unless strong reason otherwise.

Schema:
{{
  "actions": [
    {{
      "symbol": "SYMBOL",
      "decision": "SELL|HOLD",
      "sell_pct": 0.0,
      "confidence": 0.0,
      "reason": "short reason"
    }}
  ]
}}

Positions:
{json.dumps(compact, ensure_ascii=False)}
""".strip()


def run_ai_exit_portfolio_analysis(
    *,
    positions: list[dict[str, Any]],
    timeout_seconds: int = 15,
    default_sell_pct: float = 100.0,
) -> dict[str, Any]:
    started = datetime.now(timezone.utc)
    prompt = build_ai_exit_portfolio_prompt(
        positions=positions,
        default_sell_pct=default_sell_pct,
    )
    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()

    anthropic_err: RuntimeError | None = None
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
            "max_tokens": 1200,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": prompt}],
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=max(6, timeout_seconds))
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
                segment="exit:portfolio",
                budget=0.0,
                prompt=prompt,
                status="ok",
                response_text=response_text,
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PORTFOLIO_PROMPT_VERSION", "exit-portfolio-v1"),
            )
            obj = _extract_json_object(response_text)
            actions = obj.get("actions", [])
            return {"actions": actions if isinstance(actions, list) else []}
        except Exception as err:
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment="exit:portfolio",
                budget=0.0,
                prompt=prompt,
                status="error",
                error=str(err),
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PORTFOLIO_PROMPT_VERSION", "exit-portfolio-v1"),
            )
            anthropic_err = RuntimeError(f"anthropic exit portfolio call failed: {err}")
            if not openai_key:
                raise anthropic_err from err

    if openai_key:
        provider = "openai"
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1200,
        }
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=max(6, timeout_seconds))
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
                segment="exit:portfolio",
                budget=0.0,
                prompt=prompt,
                status="ok",
                response_text=response_text,
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PORTFOLIO_PROMPT_VERSION", "exit-portfolio-v1"),
            )
            obj = _extract_json_object(response_text)
            actions = obj.get("actions", [])
            return {"actions": actions if isinstance(actions, list) else []}
        except Exception as err:
            elapsed_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            _log_ai_call(
                provider=provider,
                model=model,
                segment="exit:portfolio",
                budget=0.0,
                prompt=prompt,
                status="error",
                error=str(err),
                elapsed_ms=elapsed_ms,
                prompt_version=os.getenv("AI_EXIT_PORTFOLIO_PROMPT_VERSION", "exit-portfolio-v1"),
            )
            if anthropic_err is not None:
                raise RuntimeError(f"{anthropic_err}; openai exit portfolio call failed: {err}") from err
            raise RuntimeError(f"openai exit portfolio call failed: {err}") from err

    if anthropic_err is not None:
        raise anthropic_err
    raise RuntimeError("No AI provider key configured for exit portfolio analysis.")


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

    anthropic_err: RuntimeError | None = None
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
                prompt_version=os.getenv("AI_BUY_PROMPT_VERSION", "buy-elite-v1"),
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
                prompt_version=os.getenv("AI_BUY_PROMPT_VERSION", "buy-elite-v1"),
            )
            anthropic_err = RuntimeError(f"anthropic call failed: {err}")
            if not openai_key:
                raise anthropic_err from err

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
                prompt_version=os.getenv("AI_BUY_PROMPT_VERSION", "buy-elite-v1"),
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
                prompt_version=os.getenv("AI_BUY_PROMPT_VERSION", "buy-elite-v1"),
            )
            if anthropic_err is not None:
                raise RuntimeError(f"{anthropic_err}; openai call failed: {err}") from err
            raise RuntimeError(f"openai call failed: {err}") from err

    if anthropic_err is not None:
        raise anthropic_err
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
    min_conf_by_segment = ai_cfg.get("minConfidenceForBuyBySegment", {})
    min_edge_by_segment = ai_cfg.get("minExpectedEdgeForBuyBySegment", {})
    min_net_edge_by_segment = ai_cfg.get("minExpectedEdgeNetForBuyBySegment", {})
    min_conf = float(min_conf_by_segment.get(segment, ai_cfg.get("minConfidenceForBuy", 0.70))) if isinstance(min_conf_by_segment, dict) else float(ai_cfg.get("minConfidenceForBuy", 0.70))
    min_edge = float(min_edge_by_segment.get(segment, ai_cfg.get("minExpectedEdgeForBuy", 0.05))) if isinstance(min_edge_by_segment, dict) else float(ai_cfg.get("minExpectedEdgeForBuy", 0.05))
    min_net_edge = float(min_net_edge_by_segment.get(segment, ai_cfg.get("minExpectedEdgeNetForBuy", max(min_edge, 0.0)))) if isinstance(min_net_edge_by_segment, dict) else float(ai_cfg.get("minExpectedEdgeNetForBuy", max(min_edge, 0.0)))
    if confidence < min_conf:
        return AiSignalDecision(False, "Signal does not pass confidence threshold.", normalized, None)

    max_open = int(risk_policy.get("maxOpenPositions", 3))
    if len(open_positions) >= max_open and decision == "BUY":
        return AiSignalDecision(False, "Max open positions reached.", normalized, None)

    max_size_by_segment = risk_policy.get("maxPositionSizePctBySegment", {})
    max_size_pct = float(max_size_by_segment.get(segment, risk_policy.get("maxPositionSizePct", 12.0))) if isinstance(max_size_by_segment, dict) else float(risk_policy.get("maxPositionSizePct", 12.0))
    if size_pct <= 0 or size_pct > max_size_pct:
        return AiSignalDecision(False, f"Position size pct {size_pct:.2f} exceeds policy max {max_size_pct:.2f}.", normalized, None)

    equity = _to_float(account.get("equity"), budget)
    if equity <= 0:
        equity = budget
    alloc = min(budget * (size_pct / 100.0), equity * (size_pct / 100.0))
    if decision == "BUY" and "/" in symbol:
        guards = risk_policy.get("orderSizingGuards", {})
        cash_buffer_pct = float(guards.get("cryptoAvailableCashBufferPct", 2.0)) if isinstance(guards, dict) else 2.0
        use_buying_power = bool(guards.get("cryptoAvailableCashUseBuyingPower", True)) if isinstance(guards, dict) else True
        cash_raw = _to_float(account.get("cash"))
        bp_raw = _to_float(account.get("buying_power"))
        spendable = max(bp_raw if use_buying_power else cash_raw, 0.0)
        spendable = spendable * max(0.0, 1.0 - (cash_buffer_pct / 100.0))
        alloc = min(alloc, spendable)
    max_notional_by_segment = risk_policy.get("maxOrderNotionalUsdBySegment", {})
    min_notional_by_segment = risk_policy.get("minOrderNotionalUsdBySegment", {})
    max_notional_global = float(risk_policy.get("maxOrderNotionalUsd", 0.0))
    min_notional_global = float(risk_policy.get("minOrderNotionalUsd", 0.0))
    max_notional = float(max_notional_by_segment.get(segment, max_notional_global)) if isinstance(max_notional_by_segment, dict) else max_notional_global
    min_notional = float(min_notional_by_segment.get(segment, min_notional_global)) if isinstance(min_notional_by_segment, dict) else min_notional_global
    if max_notional > 0:
        alloc = min(alloc, max_notional)
    if alloc <= 0:
        return AiSignalDecision(False, "Calculated allocation is zero.", normalized, None)
    if min_notional > 0 and alloc < min_notional:
        return AiSignalDecision(False, f"Allocation ${alloc:.2f} below min notional ${min_notional:.2f}.", normalized, None)

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
    combined_equity: float | None = None,
) -> dict[str, Any]:
    policy_effective = deepcopy(risk_policy)
    fear_state = load_fear_climate_state()
    fear_active = segment == "crypto" and bool(fear_state.get("enabled", False))
    fear_meta: dict[str, Any] = {"enabled": fear_active}
    if fear_active:
        fear_meta = apply_fear_climate_overrides(policy_effective, segment="crypto")

    account_snapshot = client.get_account()
    if combined_equity is not None:
        account_snapshot = dict(account_snapshot)
        account_snapshot["equity"] = combined_equity
    equity_mode = apply_equity_mode_switch(policy_effective, account=account_snapshot)
    ai_cfg = policy_effective.get("aiScheduler", {})
    now_utc = datetime.now(timezone.utc)
    cooldown_until = _AI_COOLDOWN_UNTIL.get(segment)
    if cooldown_until is not None and cooldown_until > now_utc:
        preview = {
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "ai_used": False,
            "ai_skipped_reason": "ai_provider_error_cooldown",
            "ai_error_cooldown_remaining_seconds": int(max((cooldown_until - now_utc).total_seconds(), 0)),
            "equity_mode": equity_mode,
            "fear_climate": fear_meta,
        }
        return preview
    timeout_seconds = int(ai_cfg.get("aiTimeoutSeconds", ai_cfg.get("openclawTimeoutSeconds", 35)))
    segment_cfg = (policy_effective.get("allowedSegments") or {}).get(segment, {})
    base_symbols = segment_cfg.get("symbolsAllowlist") or SEGMENT_UNIVERSE.get(segment, [])
    discovery_cfg = policy_effective.get("marketDiscovery", {})
    from .kraken_client import KrakenClient as _KC
    _kc = client if isinstance(client, _KC) else None
    if bool(discovery_cfg.get("enabled", False)):
        top_n = int(discovery_cfg.get("topCandidatesPerSegment", 20))
        allowed_symbols = discover_segment_candidates(
            segment=segment,
            base_symbols=list(base_symbols),
            top_n=top_n,
            discovery_cfg=discovery_cfg,
            kraken_client=_kc,
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
            "fear_climate": fear_meta,
        }
    if not allowed_symbols:
        return {
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "ai_used": False,
            "reason": "No symbols configured for segment.",
            "equity_mode": equity_mode,
            "fear_climate": fear_meta,
        }

    # Optional pre-run cleanup — only cancel stale buys from the *current* segment
    # so that, e.g., an indexFunds cycle doesn't kill a crypto limit buy.
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
            risk_policy=policy_effective,
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
            f"signals_considered={preview.get('signals_considered', 0)}.\n" + fetch_market_context(allowed_symbols, segment, client=client, risk_policy=risk_policy)
        )
    except Exception:
        rule_based_signals = []
        market_context = ""

    global_prefilter = float(ai_cfg.get("prefilterScoreThreshold", 0.035))
    per_segment_pf = ai_cfg.get("prefilterScoreThresholdBySegment", {})
    prefilter_threshold = float(per_segment_pf.get(segment, global_prefilter)) if isinstance(per_segment_pf, dict) else global_prefilter
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

    try:
        signal = run_ai_analysis(
            segment=segment,
            budget=budget,
            allowed_symbols=allowed_symbols,
            risk_policy=risk_policy,
            timeout_seconds=timeout_seconds,
            rule_based_signals=rule_based_signals,
            market_context=market_context,
        )
        _AI_CONSEC_ERRORS[segment] = 0
        _AI_COOLDOWN_UNTIL.pop(segment, None)
    except Exception as err:
        error_cooldown = int(ai_cfg.get("aiErrorCooldownSeconds", 300))
        max_error_cooldown = int(ai_cfg.get("aiMaxErrorCooldownSeconds", 1800))
        failures = int(_AI_CONSEC_ERRORS.get(segment, 0)) + 1
        _AI_CONSEC_ERRORS[segment] = failures
        backoff_seconds = max(1, min(error_cooldown * (2 ** max(failures - 1, 0)), max_error_cooldown))
        _AI_COOLDOWN_UNTIL[segment] = now_utc + timedelta(seconds=backoff_seconds)
        return {
            "segment": segment,
            "budget": budget,
            "execute": execute,
            "ai_used": False,
            "ai_skipped_reason": "ai_provider_error_cooldown",
            "ai_error": str(err),
            "ai_error_backoff_seconds": backoff_seconds,
            "equity_mode": equity_mode,
            "fear_climate": fear_meta,
        }
    account = account_snapshot
    positions = client.get_open_positions()
    decision = validate_and_plan_signal(
        signal=signal,
        budget=budget,
        segment=segment,
        risk_policy=policy_effective,
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
        "fear_climate": fear_meta,
    }

