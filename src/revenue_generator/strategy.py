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


def compute_signal(symbol: str, bars: list[dict], max_spread_bps: float) -> Signal | None:
    if len(bars) < 2:
        return None
    closes = [float(b["c"]) for b in bars]
    highs = [float(b["h"]) for b in bars]
    lows = [float(b["l"]) for b in bars]
    vols = [float(b.get("v", 0.0)) for b in bars]

    p_now = closes[-1]
    if len(closes) >= 21:
        ret_5 = pct_change(closes[-1], closes[-6])
        ret_20 = pct_change(closes[-1], closes[-21])
        momentum_score = max(min((0.6 * ret_5 + 0.4 * ret_20) * 12.0, 1.0), -1.0)
    else:
        ret_1 = pct_change(closes[-1], closes[0])
        momentum_score = max(min(ret_1 * 10.0, 1.0), -1.0)

    window = min(20, len(closes))
    ranges = [(h - l) / c if c else 0.0 for h, l, c in zip(highs[-window:], lows[-window:], closes[-window:])]
    avg_range = sum(ranges) / len(ranges)
    vol_penalty = min(avg_range * 3.0, 1.0)

    # Proxy spread estimate from last candle range when quote spread isn't fetched.
    if len(closes) <= 2:
        spread_bps = abs(pct_change(closes[-1], closes[0])) * 500
    else:
        spread_bps = ((highs[-1] - lows[-1]) / p_now) * 10000 if p_now else 9999
    spread_penalty = min(spread_bps / max(max_spread_bps, 1.0), 0.9)

    volume_boost = 0.0
    if len(vols) >= 2:
        avg_n = min(20, len(vols))
        avg_vol = max(sum(vols[-avg_n:]) / avg_n, 1.0)
        volume_boost = min((vols[-1] / avg_vol - 1.0) * 0.15, 0.2)

    edge = (0.8 * momentum_score) + volume_boost - (0.25 * vol_penalty) - (0.2 * spread_penalty)
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
) -> list[Signal]:
    signals: list[Signal] = []
    for symbol, bars in bars_by_symbol.items():
        sig = compute_signal(symbol=symbol, bars=bars, max_spread_bps=max_spread_bps)
        if sig and sig.expected_edge >= min_expected_edge and sig.confidence >= min_confidence:
            signals.append(sig)
    signals.sort(key=lambda s: (s.expected_edge, s.confidence), reverse=True)
    return signals[:top_n]
