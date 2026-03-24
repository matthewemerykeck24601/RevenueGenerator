from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Signal:
    symbol: str
    confidence: float
    expected_edge: float
    last_price: float
    spread_bps: float


def pct_change(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b


def compute_signal(symbol: str, bars: list[dict], max_spread_bps: float, *, segment: str = "") -> Signal | None:
    if len(bars) < 2:
        return None

    closes = [float(b["c"]) for b in bars]
    highs = [float(b["h"]) for b in bars]
    lows = [float(b["l"]) for b in bars]
    vols = [float(b.get("v", 0.0)) for b in bars]

    p_now = closes[-1]
    if p_now <= 0:
        return None

    # Multi-timeframe momentum
    if len(closes) >= 21:
        ret_5 = pct_change(closes[-1], closes[-6])
        ret_10 = pct_change(closes[-1], closes[-11])
        ret_20 = pct_change(closes[-1], closes[-21])
        momentum_score = max(min((0.5 * ret_5 + 0.3 * ret_10 + 0.2 * ret_20) * 10.0, 1.0), -1.0)
    elif len(closes) >= 6:
        ret_5 = pct_change(closes[-1], closes[-6])
        ret_all = pct_change(closes[-1], closes[0])
        momentum_score = max(min((0.7 * ret_5 + 0.3 * ret_all) * 10.0, 1.0), -1.0)
    else:
        ret_1 = pct_change(closes[-1], closes[0])
        momentum_score = max(min(ret_1 * 8.0, 1.0), -1.0)

    # Approximate RSI (14-period) using available bars
    rsi_window = min(14, len(closes) - 1)
    gains = [max(closes[i] - closes[i - 1], 0.0) for i in range(-rsi_window, 0)]
    losses = [max(closes[i - 1] - closes[i], 0.0) for i in range(-rsi_window, 0)]
    avg_gain = sum(gains) / max(len(gains), 1)
    avg_loss = sum(losses) / max(len(losses), 1)
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
    # RSI signal: reward 40-65 range (momentum without overbought), penalize extremes
    if 40 <= rsi <= 65:
        rsi_boost = 0.10
    elif rsi > 75 or rsi < 30:
        rsi_boost = -0.15
    else:
        rsi_boost = 0.0

    # Volatility penalty
    window = min(20, len(closes))
    ranges = [(h - l) / c if c else 0.0 for h, l, c in zip(highs[-window:], lows[-window:], closes[-window:])]
    avg_range = sum(ranges) / len(ranges)
    vol_penalty = min(avg_range * 2.5, 0.8)

    # Spread estimate
    if len(closes) <= 2:
        spread_bps = abs(pct_change(closes[-1], closes[0])) * 500
    else:
        spread_bps = ((highs[-1] - lows[-1]) / p_now) * 10000 if p_now else 9999
    spread_penalty = min(spread_bps / max(max_spread_bps, 1.0), 0.9)

    # Volume confirmation
    volume_boost = 0.0
    if len(vols) >= 5:
        avg_n = min(20, len(vols))
        avg_vol = max(sum(vols[-avg_n:]) / avg_n, 1.0)
        recent_surge = vols[-1] / avg_vol
        if recent_surge >= 1.5:
            volume_boost = min((recent_surge - 1.0) * 0.12, 0.18)
        elif recent_surge < 0.5:
            volume_boost = -0.10  # low participation is a warning sign

    edge = (
        (0.75 * momentum_score)
        + rsi_boost
        + volume_boost
        - (0.20 * vol_penalty)
        - (0.18 * spread_penalty)
    )
    confidence = max(min((edge + 1.0) / 2.0, 1.0), 0.0)

    return Signal(
        symbol=symbol,
        confidence=confidence,
        expected_edge=edge,
        last_price=p_now,
        spread_bps=spread_bps,
    )


def select_top_signals(
    bars_by_symbol: dict[str, list[dict]],
    max_spread_bps: float,
    top_n: int = 3,
    min_confidence: float = 0.5,
    min_expected_edge: float = 0.0,
    *,
    segment: str = "",
) -> list[Signal]:
    signals: list[Signal] = []
    for symbol, bars in bars_by_symbol.items():
        sig = compute_signal(symbol=symbol, bars=bars, max_spread_bps=max_spread_bps, segment=segment)
        if sig and sig.expected_edge >= min_expected_edge and sig.confidence >= min_confidence:
            signals.append(sig)
    signals.sort(key=lambda s: (s.expected_edge, s.confidence), reverse=True)
    return signals[:top_n]
