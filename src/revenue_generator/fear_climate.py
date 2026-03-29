from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_STATE_PATH = Path(__file__).resolve().parents[2] / "logs" / "fear_climate_state.json"


def _state_path(path: str | Path | None = None) -> Path:
    if path is None:
        return _DEFAULT_STATE_PATH
    return Path(path)


def load_fear_climate_state(path: str | Path | None = None) -> dict[str, Any]:
    state_path = _state_path(path)
    if not state_path.exists():
        return {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
        return {
            "enabled": bool(raw.get("enabled", False)),
            "updated_at": str(raw.get("updated_at") or datetime.now(timezone.utc).isoformat()),
        }
    except Exception:
        return {"enabled": False, "updated_at": datetime.now(timezone.utc).isoformat()}


def set_fear_climate_enabled(enabled: bool, path: str | Path | None = None) -> dict[str, Any]:
    state_path = _state_path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "enabled": bool(enabled),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    return state


def _set_by_segment(policy: dict[str, Any], key: str, segment: str, value: Any) -> None:
    bucket = policy.setdefault(key, {})
    if not isinstance(bucket, dict):
        bucket = {}
        policy[key] = bucket
    bucket[segment] = value


def apply_fear_climate_overrides(risk_policy: dict[str, Any], *, segment: str = "crypto") -> dict[str, Any]:
    cfg = risk_policy.get("fearClimateMode", {}) if isinstance(risk_policy.get("fearClimateMode", {}), dict) else {}
    seg_cfg = cfg.get(segment, {}) if isinstance(cfg.get(segment, {}), dict) else {}

    min_conf = float(seg_cfg.get("minSignalConfidence", 0.52))
    min_edge_net = float(seg_cfg.get("minExpectedEdgeNet", 0.0030))
    max_signals = int(seg_cfg.get("maxSignals", 4))
    max_entries = int(seg_cfg.get("maxEntriesPerSymbol", 12))
    buy_cooldown = int(seg_cfg.get("buyCooldownMinutes", 5))
    add_on_cooldown = int(seg_cfg.get("addOnBuyCooldownMinutes", 3))
    max_position_pct = float(seg_cfg.get("maxPositionSizePct", 8.0))
    pause_new_buys = bool(seg_cfg.get("pauseNewBuys", True))

    _set_by_segment(risk_policy, "minSignalConfidenceBySegment", segment, min_conf)
    _set_by_segment(risk_policy, "minExpectedEdgeNetBySegment", segment, min_edge_net)
    _set_by_segment(risk_policy, "maxEntriesPerSymbolBySegment", segment, max_entries)
    _set_by_segment(risk_policy, "buyCooldownMinutesBySegment", segment, buy_cooldown)
    _set_by_segment(risk_policy, "addOnBuyCooldownMinutesBySegment", segment, add_on_cooldown)
    _set_by_segment(risk_policy, "maxPositionSizePctBySegment", segment, max_position_pct)

    allowed = risk_policy.setdefault("allowedSegments", {})
    seg_policy = allowed.setdefault(segment, {})
    if isinstance(seg_policy, dict):
        seg_policy["maxSignals"] = max_signals

    order_defaults = risk_policy.setdefault("orderDefaults", {})
    crypto_defaults = order_defaults.setdefault("crypto", {})
    if isinstance(crypto_defaults, dict):
        crypto_defaults["type"] = "limit"

    ai_cfg = risk_policy.setdefault("aiScheduler", {})
    _set_by_segment(ai_cfg, "minConfidenceForBuyBySegment", segment, max(min_conf, 0.5))
    _set_by_segment(ai_cfg, "minExpectedEdgeNetForBuyBySegment", segment, max(min_edge_net, 0.0030))
    risk_policy["fearPauseNewBuysCrypto"] = pause_new_buys

    return {
        "enabled": True,
        "segment": segment,
        "min_signal_confidence": min_conf,
        "min_expected_edge_net": min_edge_net,
        "max_signals": max_signals,
        "max_entries_per_symbol": max_entries,
        "buy_cooldown_minutes": buy_cooldown,
        "add_on_buy_cooldown_minutes": add_on_cooldown,
        "max_position_size_pct": max_position_pct,
        "pause_new_buys": pause_new_buys,
        "order_type": "limit",
    }


def get_fear_climate(path: str | Path | None = None) -> dict[str, Any]:
    """Compatibility helper expected by newer research module."""
    state = load_fear_climate_state(path)
    enabled = bool(state.get("enabled", False))
    return {
        "enabled": enabled,
        "bullish": not enabled,
        "vix_level": 20.0,
        "vix": 20.0,
        "fear_greed_index": 65 if not enabled else 40,
        "updated_at": state.get("updated_at"),
    }
