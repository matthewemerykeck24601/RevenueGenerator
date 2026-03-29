"""
Agentic AI Bridge for RevenueGenerator
- Tool-calling ReAct-style skeleton for dynamic analysis
- Native structured outputs
- Memory via journal
- Regime-aware using risk_policy
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from .alpaca_client import AlpacaClient
from .config import build_runtime_config, ensure_risk_policy
from .external_research import discover_segment_candidates, get_current_vix
from .fear_climate import load_fear_climate_state
from .journal import TradeJournal
from .risk import evaluate_risk

try:
    from .kraken_client import KrakenClient
except Exception:  # pragma: no cover
    KrakenClient = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_CLIENTS: dict[str, Any] = {}
_JOURNAL: TradeJournal | None = None


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _get_alpaca_client() -> AlpacaClient:
    client = _CLIENTS.get("alpaca")
    if client is not None:
        return client
    cfg = build_runtime_config()
    client = AlpacaClient(cfg=cfg)
    _CLIENTS["alpaca"] = client
    return client


def _get_kraken_client() -> Any | None:
    if KrakenClient is None:
        return None
    if "kraken" in _CLIENTS:
        return _CLIENTS["kraken"]
    try:
        kc = KrakenClient()
        _CLIENTS["kraken"] = kc
        return kc
    except Exception:
        _CLIENTS["kraken"] = None
        return None


def _get_journal() -> TradeJournal:
    global _JOURNAL
    if _JOURNAL is None:
        _JOURNAL = TradeJournal()
    return _JOURNAL


# Tool definitions (simple functions the LLM can call)
TOOLS = [
    {
        "name": "get_account_summary",
        "description": "Get current buying power, equity, and portfolio value",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_current_positions",
        "description": "List all open positions with P&L",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_segment_research",
        "description": "Get latest market research and candidates for a segment (stocks/crypto)",
        "parameters": {
            "type": "object",
            "properties": {"segment": {"type": "string", "enum": ["stocks", "crypto"]}},
            "required": ["segment"],
        },
    },
    {
        "name": "get_fear_climate",
        "description": "Get current fear/greed climate and VIX regime",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "get_recent_journal",
        "description": "Get last 5 trade signals and outcomes for memory",
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
]

# Structured output schema for final signal
SIGNAL_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": ["BUY", "HOLD", "SELL"]},
        "ticker": {"type": "string"},
        "segment": {"type": "string", "enum": ["stocks", "crypto"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "edge_percent": {"type": "number"},
        "size_percent": {"type": "number", "minimum": 0.0},
        "rationale": {"type": "string"},
        "suggested_timeframe": {"type": "string"},
    },
    "required": ["action", "ticker", "segment", "confidence", "edge_percent", "rationale"],
}


def get_account_summary() -> dict[str, Any]:
    alp = _get_alpaca_client()
    acct = alp.get_account()
    out = {
        "provider": "alpaca",
        "equity": _to_float(acct.get("equity")),
        "cash": _to_float(acct.get("cash")),
        "buying_power": _to_float(acct.get("buying_power")),
    }
    kc = _get_kraken_client()
    if kc is not None:
        try:
            k = kc.get_account()
            out["kraken"] = {
                "equity": _to_float(k.get("equity")),
                "cash": _to_float(k.get("cash")),
                "buying_power": _to_float(k.get("buying_power")),
            }
        except Exception:
            pass
    return out


def get_current_positions() -> list[dict[str, Any]]:
    alp = _get_alpaca_client()
    positions = alp.get_open_positions()
    out: list[dict[str, Any]] = []
    for p in positions:
        out.append(
            {
                "provider": "alpaca",
                "symbol": p.get("symbol"),
                "qty": _to_float(p.get("qty")),
                "market_value": _to_float(p.get("market_value")),
                "unrealized_pl": _to_float(p.get("unrealized_pl")),
                "unrealized_plpc": _to_float(p.get("unrealized_plpc")) * 100.0,
            }
        )
    kc = _get_kraken_client()
    if kc is not None:
        try:
            for p in kc.get_open_positions():
                qty = _to_float(p.get("qty"))
                cur = _to_float(p.get("current_price"))
                ent = _to_float(p.get("avg_entry_price"))
                out.append(
                    {
                        "provider": "kraken",
                        "symbol": p.get("symbol"),
                        "qty": qty,
                        "market_value": qty * cur,
                        "unrealized_pl": qty * (cur - ent) if ent > 0 else 0.0,
                        "unrealized_plpc": ((cur - ent) / ent * 100.0) if ent > 0 else 0.0,
                    }
                )
        except Exception:
            pass
    return out


def get_kraken_positions() -> list[dict[str, Any]]:
    kc = _get_kraken_client()
    if kc is None:
        return []
    try:
        return kc.get_open_positions()
    except Exception:
        return []


def get_segment_research(segment: str) -> dict[str, Any]:
    policy = ensure_risk_policy()
    ds_cfg = (policy.get("marketDiscovery") or {}) if isinstance(policy.get("marketDiscovery"), dict) else {}
    if segment == "crypto":
        base = list((policy.get("allowedSegments") or {}).get("crypto", {}).get("symbolsAllowlist", []))
        candidates = discover_segment_candidates(
            segment="crypto",
            base_symbols=base,
            top_n=int(ds_cfg.get("topCandidatesPerSegment", 20)),
            discovery_cfg=ds_cfg,
            kraken_client=_get_kraken_client(),
        )
        return {"segment": "crypto", "candidates": candidates}
    base = list((policy.get("allowedSegments") or {}).get("largeCapStocks", {}).get("symbolsAllowlist", []))
    candidates = discover_segment_candidates(
        segment="largeCapStocks",
        base_symbols=base,
        top_n=int(ds_cfg.get("topCandidatesPerSegment", 20)),
        discovery_cfg=ds_cfg,
        kraken_client=None,
    )
    return {"segment": "stocks", "candidates": candidates}


def get_fear_climate() -> dict[str, Any]:
    st = load_fear_climate_state()
    vix = get_current_vix()
    bullish = not bool(st.get("enabled", False)) and (vix is not None and vix < 18.0)
    return {"fear_state": st, "vix": vix, "bullish": bullish}


def get_recent_trades(limit: int = 5) -> list[dict[str, Any]]:
    db = Path("logs") / "trades.db"
    if not db.exists():
        return []
    try:
        import sqlite3

        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT ts, segment, strategy, orders_planned_json, orders_placed_json, order_errors_json "
            "FROM cycles ORDER BY ts DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
        con.close()
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "ts": r["ts"],
                "segment": r["segment"],
                "strategy": r["strategy"],
                "orders_planned_count": len(json.loads(r["orders_planned_json"] or "[]")),
                "orders_placed_count": len(json.loads(r["orders_placed_json"] or "[]")),
                "order_errors_count": len(json.loads(r["order_errors_json"] or "[]")),
            }
        )
    return out


def get_recent_journal() -> list[dict[str, Any]]:
    return get_recent_trades(limit=5)


def log_trade_signal(signal: dict[str, Any], approved: bool, rationale: str = "") -> None:
    path = Path("logs") / "agent_signals.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "signal": signal,
        "approved": bool(approved),
        "rationale": rationale,
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def validate_and_plan_signal(signal: dict[str, Any], risk_policy: dict[str, Any], segment: str) -> dict[str, Any]:
    action = str(signal.get("action", "HOLD")).upper()
    ticker = str(signal.get("ticker") or "")
    confidence = _to_float(signal.get("confidence"))
    edge = _to_float(signal.get("edge_percent"))
    size_pct = _to_float(signal.get("size_percent"))
    defaults = risk_policy.get("default", {})
    min_conf = _to_float(defaults.get("min_confidence"), 0.65)
    min_edge = _to_float(defaults.get("min_edge"), 1.0)
    max_pos_pct = _to_float(defaults.get("max_position_percent_of_equity"), 8.0)
    approved = (
        action in {"BUY", "SELL", "HOLD"}
        and (action == "HOLD" or (confidence >= min_conf and edge >= min_edge and size_pct <= max_pos_pct and bool(ticker)))
    )
    return {
        "approved": approved,
        "action": action,
        "ticker": ticker,
        "segment": segment,
        "confidence": confidence,
        "edge_percent": edge,
        "size_percent": size_pct,
        "rationale": str(signal.get("rationale", "")),
    }


class AgenticAIBridge:
    def __init__(self) -> None:
        self.risk_policy = ensure_risk_policy()
        self.last_call_time = 0.0
        self.cooldown_seconds = 30
        self.prompt_version = "agentic_v2_20260328"

    def _get_llm_client(self) -> tuple[str, str]:
        """Prefer Anthropic, fallback to OpenAI."""
        anth_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        openai_key = os.getenv("OPENAI_API_KEY", "").strip()
        if anth_key:
            return "anthropic", os.getenv("AI_MODEL", "claude-3-5-sonnet-20240620").strip() or "claude-3-5-sonnet-20240620"
        if openai_key:
            return "openai", os.getenv("OPENAI_MODEL", "gpt-4o").strip() or "gpt-4o"
        raise RuntimeError("No AI provider key configured.")

    def _build_system_prompt(self, segment: str, regime: str) -> str:
        policy = self.risk_policy.get("regime_overrides", {}).get(regime, self.risk_policy.get("default", {}))
        return f"""You are an expert agentic momentum/scalping trader.
Your goal is daily profit churn targeting $100+ net after fees/slippage.

Current regime: {regime} (use aggressive thresholds when enabled).
Risk Policy Highlights:
- Min confidence: {policy.get('min_confidence', 0.65)}
- Min edge: {policy.get('min_edge', 1.0)}%
- Max position % equity: {policy.get('max_position_percent_of_equity', 8.0)}%
- Max signals per cycle: {policy.get('max_signals_per_cycle', 5)}

You have tools to gather real-time data. Use them before deciding.
Think step-by-step: Research -> Analyze momentum/volume -> Check risk -> Propose signal.

Only output a valid JSON matching the schema after using tools if needed.
Focus on high-turnover opportunities in liquid names during green tape."""

    @staticmethod
    def _extract_json(text: str) -> dict[str, Any]:
        text = str(text or "").strip()
        try:
            return json.loads(text)
        except Exception:
            # best effort extraction of first json object
            decoder = json.JSONDecoder()
            for i, ch in enumerate(text):
                if ch != "{":
                    continue
                try:
                    obj, _ = decoder.raw_decode(text[i:])
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    continue
        raise ValueError("No valid JSON object in LLM output.")

    def _llm_json_call(self, *, provider: str, model: str, system_prompt: str, user_message: str) -> dict[str, Any]:
        if provider == "anthropic":
            key = os.getenv("ANTHROPIC_API_KEY", "").strip()
            headers = {
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }
            payload = {
                "model": model,
                "max_tokens": 1200,
                "temperature": 0.2,
                "messages": [
                    {"role": "user", "content": f"{system_prompt}\n\n{user_message}\n\nOutput only valid JSON."}
                ],
            }
            resp = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=45)
            resp.raise_for_status()
            data = resp.json()
            text_parts: list[str] = []
            for block in data.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(str(block.get("text", "")))
            return self._extract_json("\n".join(text_parts))

        key = os.getenv("OPENAI_API_KEY", "").strip()
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        payload = {
            "model": model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "response_format": {"type": "json_object"},
        }
        resp = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=45)
        resp.raise_for_status()
        data = resp.json()
        content = str((((data.get("choices") or [{}])[0].get("message") or {}).get("content")) or "")
        return self._extract_json(content)

    def analyze_segment(self, segment: str = "crypto") -> dict[str, Any]:
        """Main agentic analysis entry point."""
        if time.time() - self.last_call_time < self.cooldown_seconds:
            logger.info("AI cooldown active")
            return {"action": "HOLD", "reason": "cooldown", "confidence": 0.0}
        self.last_call_time = time.time()

        try:
            provider, model = self._get_llm_client()
            fear = get_fear_climate()
            regime = "aggressive_mode" if fear.get("bullish", False) or _to_float(fear.get("vix"), 20.0) < 18 else "normal_mode"
            system_prompt = self._build_system_prompt(segment, regime)
            research = get_segment_research(segment)
            recent_trades = get_recent_journal()

            user_message = f"""
Segment: {segment}
Current Fear Climate: {json.dumps(fear, default=str)}
Recent Research: {json.dumps(research, default=str)[:2000]}
Recent Trades Memory: {json.dumps(recent_trades, default=str)[:1500]}
Account Snapshot: {json.dumps(get_account_summary(), default=str)[:1200]}
Current Positions: {json.dumps(get_current_positions(), default=str)[:1200]}

Propose the best actionable signal for profit churn right now.

Return JSON using schema keys:
action, ticker, segment, confidence, edge_percent, size_percent, rationale, suggested_timeframe
"""
            signal = self._llm_json_call(provider=provider, model=model, system_prompt=system_prompt, user_message=user_message)
            validated = validate_and_plan_signal(signal, self.risk_policy, segment)
            log_trade_signal(signal, validated.get("approved", False), rationale=str(signal.get("rationale", "")))
            logger.info(
                "Agentic signal generated: %s %s | confidence %.2f | regime %s",
                str(signal.get("action")),
                str(signal.get("ticker")),
                _to_float(signal.get("confidence")),
                regime,
            )
            return validated
        except Exception as e:
            logger.error("AI Bridge error: %s", e)
            return {"action": "HOLD", "reason": f"error: {str(e)}", "confidence": 0.0}


# Singleton
ai_bridge = AgenticAIBridge()


def analyze_segment(segment: str = "crypto") -> dict[str, Any]:
    """Public API - keep existing function signature for backward compatibility."""
    return ai_bridge.analyze_segment(segment)


# ---- Compatibility wrappers for existing runners ----
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
    _ = (budget, allowed_symbols, risk_policy, timeout_seconds, rule_based_signals, market_context)
    out = ai_bridge.analyze_segment("crypto" if segment == "crypto" else "stocks")
    action = str(out.get("action", "HOLD")).upper()
    return {
        "decision": "BUY" if action == "BUY" else ("SELL" if action == "SELL" else "HOLD"),
        "symbol": str(out.get("ticker") or ""),
        "segment": segment,
        "confidence": _to_float(out.get("confidence")),
        "expected_edge": _to_float(out.get("edge_percent")) / 100.0,
        "position_size_pct_of_budget": _to_float(out.get("size_percent")),
        "entry": {"order_type": "limit", "limit_price": 0.0, "time_in_force": "gtc" if segment == "crypto" else "day"},
        "reasoning": [str(out.get("rationale") or "agentic output")],
    }


def run_ai_cycle(
    *,
    client: Any,
    risk_policy: dict[str, Any],
    segment: str,
    budget: float,
    execute: bool,
    combined_equity: Optional[float] = None,
) -> dict[str, Any]:
    _ = (client, risk_policy, execute, combined_equity)
    signal = run_ai_analysis(
        segment=segment,
        budget=budget,
        allowed_symbols=[],
        risk_policy=ai_bridge.risk_policy,
    )
    decision = validate_and_plan_signal(
        {
            "action": signal.get("decision"),
            "ticker": signal.get("symbol"),
            "segment": segment,
            "confidence": signal.get("confidence", 0.0),
            "edge_percent": _to_float(signal.get("expected_edge")) * 100.0,
            "size_percent": signal.get("position_size_pct_of_budget", 0.0),
            "rationale": "; ".join(signal.get("reasoning") or []),
        },
        ai_bridge.risk_policy,
        segment,
    )
    approved = bool(decision.get("approved", False))
    planned: list[dict[str, Any]] = []
    if approved and str(signal.get("decision")) == "BUY":
        planned.append(
            {
                "symbol": signal.get("symbol"),
                "side": "buy",
                "qty": 0,
                "order_type": "limit",
                "time_in_force": "gtc" if segment == "crypto" else "day",
                "limit_price": 0.0,
                "latest_price": 0.0,
                "allocation": 0.0,
                "expected_edge": signal.get("expected_edge", 0.0),
                "expected_edge_net": signal.get("expected_edge", 0.0),
                "estimated_cost_pct": 0.0,
            }
        )
    return {
        "strategy": "ai_direct",
        "account_status": "ACTIVE",
        "segment": segment,
        "budget": budget,
        "execute": execute,
        "ai_used": True,
        "ai_signal": signal,
        "ai_allowed": approved and str(signal.get("decision")) != "HOLD",
        "ai_reason": "Signal approved by risk gate." if approved else "Model returned HOLD." if str(signal.get("decision")) == "HOLD" else "Signal rejected by risk gate.",
        "orders_planned": planned,
        "orders_placed": [],
        "order_errors": [],
    }
