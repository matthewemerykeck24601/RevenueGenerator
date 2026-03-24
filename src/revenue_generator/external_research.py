from __future__ import annotations

from typing import Any

try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None


CRYPTO_TO_YAHOO = {
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


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _rank_symbol(symbol: str, yahoo_symbol: str, *, period: str, interval: str) -> tuple[str, float]:
    if yf is None:
        return symbol, -999.0
    ticker = yf.Ticker(yahoo_symbol)
    hist = ticker.history(period=period, interval=interval, auto_adjust=False)
    if hist is None or len(hist) < 3:
        return symbol, -999.0
    closes = hist["Close"].dropna()
    vols = hist["Volume"].fillna(0)
    if len(closes) < 3:
        return symbol, -999.0
    last = _to_float(closes.iloc[-1])
    prev = _to_float(closes.iloc[-2], last)
    base = _to_float(closes.iloc[-3], prev)
    if base <= 0 or prev <= 0 or last <= 0:
        return symbol, -999.0
    ret1 = (last - prev) / prev
    ret2 = (last - base) / base
    vol_now = _to_float(vols.iloc[-1], 0)
    vol_avg = _to_float(vols.tail(min(len(vols), 10)).mean(), 1.0)
    vol_boost = min(max((vol_now / max(vol_avg, 1.0) - 1.0) * 0.1, -0.3), 0.3)
    score = (ret1 * 0.65) + (ret2 * 0.35) + vol_boost
    return symbol, score


def _current_vix() -> float | None:
    if yf is None:
        return None
    ticker = yf.Ticker("^VIX")
    hist = ticker.history(period="5d", interval="1d", auto_adjust=False)
    if hist is None or len(hist) < 1:
        return None
    closes = hist["Close"].dropna()
    if len(closes) < 1:
        return None
    return _to_float(closes.iloc[-1], 0.0)


def select_external_candidates(
    *,
    segment: str,
    symbols: list[str],
    top_n: int,
    regime_vix_ceiling: float = 25.0,
) -> list[str]:
    if not symbols:
        return []
    if segment == "pennyStocks":
        vix = _current_vix()
        if vix is not None and vix > regime_vix_ceiling:
            # Risk-off regime: skip penny candidates until volatility cools.
            return []
    scored: list[tuple[str, float]] = []
    if segment == "crypto":
        for sym in symbols:
            yahoo_sym = CRYPTO_TO_YAHOO.get(sym)
            if not yahoo_sym:
                continue
            scored.append(_rank_symbol(sym, yahoo_sym, period="7d", interval="1h"))
    else:
        # Equities and ETFs
        for sym in symbols:
            scored.append(_rank_symbol(sym, sym, period="1mo", interval="1d"))

    if not scored:
        return symbols[:top_n]
    scored.sort(key=lambda x: x[1], reverse=True)
    selected = [sym for sym, score in scored if score > -999.0]
    if not selected:
        return symbols[:top_n]
    return selected[:top_n]
