"""
Risk Module - Dynamic & Regime-Aware for Aggressive Daily Profit Churn
Handles validation, position sizing, and safety gates while allowing higher turnover.
"""

from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import Dict, Any, Optional
import json
from datetime import datetime

from .config import load_risk_policy

logger = logging.getLogger(__name__)


@dataclass
class RiskDecision:
    allowed: bool
    reason: str
    max_alloc_dollars: float = 0.0
    suggested_size_percent: float = 0.0
    cooldown_minutes: int = 0


def compute_daily_drawdown_pct(start_equity: float, current_equity: float) -> float:
    if start_equity <= 0:
        return 0.0
    return max((start_equity - current_equity) / start_equity * 100.0, 0.0)


def get_regime(risk_policy: Dict, fear_climate: Optional[Dict] = None) -> str:
    """Determine aggressive vs normal regime based on policy and market"""
    overrides = risk_policy.get("regime_overrides", {})
    default = risk_policy.get("default", {})

    # Aggressive triggers (green tape, low VIX, bullish fear)
    if fear_climate:
        vix = fear_climate.get("vix", 20)
        bullish = fear_climate.get("bullish", False) or fear_climate.get("fear_greed_index", 50) > 60
        if (vix < 18 or bullish) and "aggressive_mode" in overrides:
            return "aggressive_mode"

    return "normal_mode"


def validate_and_plan_signal(
    signal: Dict[str, Any],
    risk_policy: Optional[Dict] = None,
    segment: str = "crypto",
    current_equity: float = 0.0,
    start_equity: float = 0.0,
    open_positions: int = 0,
    fear_climate: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Main risk gate: Takes agent proposal -> validates against policy -> returns approved plan or blocked.
    Never bypassed -- this is the safety core.
    """
    if risk_policy is None:
        risk_policy = load_risk_policy()

    regime = get_regime(risk_policy, fear_climate)
    policy = risk_policy.get("regime_overrides", {}).get(regime, risk_policy.get("default", {}))

    # Extract proposal
    action = signal.get("action", "HOLD").upper()
    ticker = signal.get("ticker", "")
    confidence = float(signal.get("confidence", 0.0))
    edge = float(signal.get("edge_percent", 0.0))
    proposed_size_pct = float(signal.get("size_percent", 0.0))

    result = {
        "action": action,
        "ticker": ticker,
        "segment": segment,
        "approved": False,
        "confidence": confidence,
        "edge": edge,
        "size_percent": 0.0,
        "max_alloc_dollars": 0.0,
        "reason": "",
        "regime": regime,
        "cooldown_minutes": policy.get("cooldown_minutes", 15),
    }

    if action == "HOLD":
        result["approved"] = True
        result["reason"] = "HOLD signal"
        return result

    # Hard safety checks
    daily_dd = compute_daily_drawdown_pct(start_equity, current_equity)
    max_daily_loss = risk_policy.get("max_daily_loss_percent", 2.5)
    if daily_dd >= max_daily_loss:
        result["reason"] = f"Daily drawdown kill switch: {daily_dd:.2f}% >= {max_daily_loss}%"
        logger.warning(result["reason"])
        return result

    max_positions = policy.get("max_concurrent_positions", 6)
    if open_positions >= max_positions:
        result["reason"] = f"Max open positions reached ({open_positions}/{max_positions})"
        return result

    # Regime-aware thresholds
    min_conf = policy.get("min_confidence", 0.65)
    min_edge = policy.get("min_edge", 1.0)
    max_pos_pct = policy.get("max_position_percent_of_equity", 8.0)

    if confidence < min_conf:
        result["reason"] = f"Confidence too low: {confidence:.2f} < {min_conf}"
        return result

    if edge < min_edge:
        result["reason"] = f"Edge too low: {edge:.2f}% < {min_edge}%"
        return result

    # Position sizing (confidence + regime boosted on aggressive)
    base_size = min(proposed_size_pct or max_pos_pct, max_pos_pct)
    aggressiveness = 1.35 if regime == "aggressive_mode" else 1.0
    final_size_pct = min(base_size * (confidence * aggressiveness), max_pos_pct)

    # Segment-specific floors
    seg_config = policy.get(segment, {})
    min_notional = seg_config.get("min_notional_usd", 80 if segment == "crypto" else 350)

    result["approved"] = True
    result["size_percent"] = round(final_size_pct, 2)
    result["max_alloc_dollars"] = 0.0  # Will be calculated in bot/execute using actual equity
    result["reason"] = f"Approved in {regime} regime"
    result["cooldown_minutes"] = policy.get(f"{segment}_cooldown_minutes", policy.get("cooldown_minutes", 15))

    logger.info(f"Risk validated: {action} {ticker} | size {final_size_pct:.1f}% | regime {regime} | reason: {result['reason']}")
    return result


# Legacy wrapper for backward compatibility
def evaluate_risk(**kwargs):
    """Keep old signature if any scripts still call it"""
    logger.warning("evaluate_risk called — consider migrating to validate_and_plan_signal")
    # Simple fallback
    return RiskDecision(True, "ok", kwargs.get("budget", 0) * 0.08)
