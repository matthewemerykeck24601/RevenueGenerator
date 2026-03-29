"""
Strategy Module - Tuned for Agentic Momentum/Scalping Churn
Looser thresholds + regime boost for higher signal throughput on green days.
"""

from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class Signal:
    symbol: str
    confidence: float
    expected_edge: float
    last_price: float
    spread_bps: float
    rationale: str = ""  # Added for agent trace


def pct_change(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b


def compute_signal(symbol: str, bars: List[Dict], max_spread_bps: float, *, segment: str = "", regime: str = "normal") -> Signal | None:
    if len(bars) < 2:
        return None

    closes = [float(b.get("c", 0)) for b in bars]
    highs = [float(b.get("h", 0)) for b in bars]
    lows = [float(b.get("l", 0)) for b in bars]
    vols = [float(b.get("v", 0)) for b in bars]

    p_now = closes[-1]
    if p_now <= 0:
        return None

    # Momentum (more sensitive for churn)
    if len(closes) >= 21:
        ret_5 = pct_change(closes[-1], closes[-6])
        ret_10 = pct_change(closes[-1], closes[-11])
        ret_20 = pct_change(closes[-1], closes[-21])
        momentum_score = max(min((0.6 * ret_5 + 0.25 * ret_10 + 0.15 * ret_20) * 12.0, 1.2), -1.0)
    else:
        ret_short = pct_change(closes[-1], closes[-min(6, len(closes))])
        momentum_score = max(min(ret_short * 10.0, 1.2), -1.0)

    # RSI (wider profitable range for scalping)
    rsi_window = min(14, len(closes) - 1)
    gains = [max(closes[i] - closes[i - 1], 0) for i in range(-rsi_window, 0)]
    losses = [max(closes[i - 1] - closes[i], 0) for i in range(-rsi_window, 0)]
    avg_gain = sum(gains) / max(len(gains), 1)
    avg_loss = sum(losses) / max(len(losses), 1)
    rsi = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))

    rsi_boost = 0.12 if 35 <= rsi <= 72 else (-0.10 if rsi > 78 or rsi < 28 else 0.0)

    # Volume surge (key for churn)
    volume_boost = 0.0
    if len(vols) >= 5:
        avg_vol = sum(vols[-min(20, len(vols)):]) / min(20, len(vols))
        surge = vols[-1] / max(avg_vol, 1)
        volume_boost = min((surge - 1.0) * 0.25, 0.35) if surge >= 1.35 else (-0.08 if surge < 0.6 else 0.0)

    # Volatility & spread penalty (lighter in aggressive regime)
    vol_penalty = min((max(highs[-5:]) - min(lows[-5:])) / p_now * 1.4, 0.6)
    spread_penalty = min(((highs[-1] - lows[-1]) / p_now) * 80, 0.7)  # loose proxy

    aggressiveness = 1.25 if regime == "aggressive_mode" else 1.0

    edge = aggressiveness * (
        1.05 * momentum_score
        + rsi_boost
        + 1.25 * volume_boost
        - 0.09 * vol_penalty
        - 0.07 * spread_penalty
    )

    confidence = max(min(
        0.45
        + max(momentum_score, 0) * 0.38
        + max(volume_boost, 0) * 0.48
        + (0.08 if 35 <= rsi <= 72 else 0)
        - 0.09 * vol_penalty
        - 0.06 * spread_penalty
        + max(edge, 0) * 0.22,
        1.0
    ), 0.0)

    rationale = f"Mom:{momentum_score:.2f} RSI:{rsi:.1f} Vol:{volume_boost:.2f} Regime:{regime}"

    return Signal(
        symbol=symbol,
        confidence=confidence,
        expected_edge=edge,
        last_price=p_now,
        spread_bps=spread_penalty * 100,
        rationale=rationale
    )


def select_top_signals(
    bars_by_symbol: Dict[str, List[Dict]],
    max_spread_bps: float = 0.35,
    top_n: int = 6,                    # increased for churn
    min_confidence: float = 0.55,      # lowered
    min_expected_edge: float = 0.8,    # lowered
    *,
    segment: str = "",
    regime: str = "normal"
) -> List[Signal]:
    signals: List[Signal] = []
    for symbol, bars in bars_by_symbol.items():
        sig = compute_signal(symbol, bars, max_spread_bps, segment=segment, regime=regime)
        if sig and sig.expected_edge >= min_expected_edge and sig.confidence >= min_confidence:
            signals.append(sig)
    signals.sort(key=lambda s: (s.expected_edge, s.confidence), reverse=True)
    return signals[:top_n]
