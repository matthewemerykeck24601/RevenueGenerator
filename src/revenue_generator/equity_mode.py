from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"mode": "conservative", "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"mode": "conservative", "updated_at": datetime.now(timezone.utc).isoformat()}


def _save_state(path: Path, mode: str, equity: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "mode": mode,
                "equity": round(equity, 6),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _apply_mode_overrides(risk_policy: dict[str, Any], mode_overrides: dict[str, Any]) -> None:
    if "pdtGuardEnabled" in mode_overrides:
        pdt = risk_policy.setdefault("pdtGuard", {})
        pdt["enabled"] = bool(mode_overrides.get("pdtGuardEnabled"))
    if "maxPositionSizePct" in mode_overrides:
        risk_policy["maxPositionSizePct"] = float(mode_overrides.get("maxPositionSizePct"))
    if "maxOpenPositions" in mode_overrides:
        risk_policy["maxOpenPositions"] = int(mode_overrides.get("maxOpenPositions"))
    if "allowAddOnBuys" in mode_overrides:
        risk_policy["allowAddOnBuys"] = bool(mode_overrides.get("allowAddOnBuys"))
    if "minExpectedEdgeNet" in mode_overrides:
        risk_policy["minExpectedEdgeNet"] = float(mode_overrides.get("minExpectedEdgeNet"))

    ai_cfg = risk_policy.setdefault("aiScheduler", {})
    if "aiMinConfidenceForBuy" in mode_overrides:
        ai_cfg["minConfidenceForBuy"] = float(mode_overrides.get("aiMinConfidenceForBuy"))
    if "aiMinExpectedEdgeForBuy" in mode_overrides:
        ai_cfg["minExpectedEdgeForBuy"] = float(mode_overrides.get("aiMinExpectedEdgeForBuy"))
    if "aiMinExpectedEdgeNetForBuy" in mode_overrides:
        ai_cfg["minExpectedEdgeNetForBuy"] = float(mode_overrides.get("aiMinExpectedEdgeNetForBuy"))

    cadence_overrides = mode_overrides.get("sectorBudgetPct")
    if isinstance(cadence_overrides, dict):
        sector_cadence = risk_policy.setdefault("sectorCadence", {})
        for segment, budget_pct in cadence_overrides.items():
            segment_cfg = sector_cadence.setdefault(str(segment), {})
            segment_cfg["budgetPct"] = float(budget_pct)

    exit_overrides = mode_overrides.get("exitHooks")
    if isinstance(exit_overrides, dict):
        exit_hooks = risk_policy.setdefault("exitHooks", {})
        for key, value in exit_overrides.items():
            if key in {"firstTargetPct", "firstTargetSellPct", "secondTargetPct", "secondTargetSellPct", "trailingStopPct", "breakEvenBufferPct", "hardStopLossPct"}:
                exit_hooks[key] = float(value)


def apply_equity_mode_switch(
    risk_policy: dict[str, Any],
    *,
    account: dict[str, Any] | None = None,
    state_path: str = "logs/equity_mode_state.json",
) -> dict[str, Any]:
    cfg = risk_policy.get("equityModeSwitch") or {}
    if not bool(cfg.get("enabled", False)):
        return {"enabled": False, "mode": "conservative", "equity": _to_float((account or {}).get("equity"))}

    threshold = float(cfg.get("thresholdUsd", 25000.0))
    reenable_buffer = float(cfg.get("reenableBufferUsd", 1500.0))
    reenable_floor = max(0.0, threshold - max(0.0, reenable_buffer))
    equity = _to_float((account or {}).get("equity"))

    path = Path(state_path)
    state = _load_state(path)
    previous_mode = str(state.get("mode") or "conservative").lower()
    if previous_mode not in {"conservative", "aggressive"}:
        previous_mode = "conservative"

    if previous_mode == "conservative" and equity >= threshold:
        mode = "aggressive"
    elif previous_mode == "aggressive" and equity <= reenable_floor:
        mode = "conservative"
    else:
        mode = previous_mode

    mode_overrides = cfg.get(mode) or {}
    if isinstance(mode_overrides, dict):
        _apply_mode_overrides(risk_policy, mode_overrides)

    _save_state(path, mode, equity)
    return {
        "enabled": True,
        "mode": mode,
        "previous_mode": previous_mode,
        "threshold_usd": threshold,
        "reenable_floor_usd": reenable_floor,
        "equity": equity,
        "switched": mode != previous_mode,
    }
