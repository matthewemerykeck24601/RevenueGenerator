from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RiskDecision:
    allowed: bool
    reason: str
    max_alloc_dollars: float


def compute_daily_drawdown_pct(start_equity: float, current_equity: float) -> float:
    if start_equity <= 0:
        return 0.0
    return max((start_equity - current_equity) / start_equity * 100.0, 0.0)


def evaluate_risk(
    *,
    start_equity: float,
    current_equity: float,
    open_positions: int,
    max_open_positions: int,
    max_daily_loss_pct: float,
    budget: float,
    max_position_size_pct: float,
    confidence: float,
    aggressiveness_multiplier: float = 1.35,
) -> RiskDecision:
    daily_dd = compute_daily_drawdown_pct(start_equity, current_equity)
    if daily_dd >= max_daily_loss_pct:
        return RiskDecision(False, f"Kill switch: daily drawdown {daily_dd:.2f}% >= {max_daily_loss_pct:.2f}%", 0.0)
    if open_positions >= max_open_positions:
        return RiskDecision(False, "Max open positions reached", 0.0)

    base_alloc = budget * (max_position_size_pct / 100.0)
    dynamic_weight = max(min(confidence * aggressiveness_multiplier, 1.0), 0.1)
    alloc = base_alloc * dynamic_weight
    return RiskDecision(True, "ok", alloc)
