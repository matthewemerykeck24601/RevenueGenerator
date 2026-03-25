from __future__ import annotations

from datetime import datetime, timedelta, timezone
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

# Broader discovery seed universe. This is intentionally larger than static allowlists
# but still constrained to liquid, recognizable names to avoid garbage scans.
DISCOVERY_SEED_UNIVERSE: dict[str, list[str]] = {
    "indexFunds": [
        "SPY",
        "QQQ",
        "DIA",
        "IWM",
        "VTI",
        "VOO",
        "IVV",
        "XLF",
        "XLK",
        "XLE",
        "XLI",
        "XLY",
        "XLP",
        "XLV",
        "XLU",
        "XLC",
        "XLB",
        "XLRE",
        "SMH",
        "SOXX",
        "ARKK",
        "SCHD",
        "VUG",
        "VTV",
    ],
    "largeCapStocks": [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "GOOGL",
        "META",
        "TSLA",
        "AVGO",
        "AMD",
        "NFLX",
        "ORCL",
        "CRM",
        "ADBE",
        "QCOM",
        "CSCO",
        "INTC",
        "MU",
        "PLTR",
        "UBER",
        "SHOP",
        "PANW",
        "CRWD",
        "SNOW",
        "ANET",
        "MELI",
        "JPM",
        "BAC",
        "WFC",
        "GS",
        "MS",
        "V",
        "MA",
        "PYPL",
        "BRK.B",
        "UNH",
        "LLY",
        "JNJ",
        "PFE",
        "MRK",
        "ABBV",
        "XOM",
        "CVX",
        "COP",
        "CAT",
        "DE",
        "HON",
        "GE",
        "NOC",
        "BA",
        "LMT",
        "DIS",
        "CMCSA",
        "TMUS",
        "VZ",
        "T",
        "COST",
        "WMT",
        "HD",
        "LOW",
        "MCD",
        "SBUX",
        "NKE",
        "KO",
        "PEP",
        "PG",
        "TMO",
        "ABT",
        "DHR",
    ],
    "pennyStocks": [
        "SNDL",
        "TNXP",
        "AEMD",
        "CTRM",
        "XELA",
        "HSDT",
        "MARA",
        "RIOT",
        "HUT",
        "BITF",
        "CIFR",
        "WULF",
        "IONQ",
        "SOUN",
        "MULN",
        "BKKT",
        "CLOV",
        "SOFI",
        "JOBY",
        "ACHR",
        "RKLB",
        "ASTS",
        "PLUG",
        "LCID",
        "FUBO",
        "OPEN",
        "RIVN",
    ],
    "crypto": [
        "BTC/USD",
        "ETH/USD",
        "SOL/USD",
        "AVAX/USD",
        "LTC/USD",
        "LINK/USD",
        "BCH/USD",
        "UNI/USD",
        "AAVE/USD",
    ],
}

_DISCOVERY_CACHE: dict[str, tuple[datetime, list[str]]] = {}


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


def _score_and_filter_symbol(
    symbol: str,
    yahoo_symbol: str,
    *,
    period: str,
    interval: str,
    min_price: float,
    min_dollar_volume: float,
) -> tuple[str, float]:
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

    last = _to_float(closes.iloc[-1], 0.0)
    if last < min_price:
        return symbol, -999.0
    recent_n = min(len(closes), 10)
    avg_close = _to_float(closes.tail(recent_n).mean(), 0.0)
    avg_vol = _to_float(vols.tail(recent_n).mean(), 0.0)
    avg_dollar_vol = avg_close * avg_vol
    if avg_dollar_vol < min_dollar_volume:
        return symbol, -999.0

    prev = _to_float(closes.iloc[-2], last)
    base = _to_float(closes.iloc[-3], prev)
    if base <= 0 or prev <= 0 or last <= 0:
        return symbol, -999.0
    ret1 = (last - prev) / prev
    ret2 = (last - base) / base
    vol_now = _to_float(vols.iloc[-1], 0.0)
    vol_boost = min(max((vol_now / max(avg_vol, 1.0) - 1.0) * 0.1, -0.3), 0.3)
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


def get_current_vix() -> float | None:
    return _current_vix()


def should_skip_cycle_for_vix(
    *,
    segment: str,
    risk_off_vix_ceiling: float,
    risk_off_segments: list[str],
    vix_value: float | None = None,
) -> tuple[bool, float | None]:
    segment_set = {str(s) for s in risk_off_segments}
    if segment not in segment_set:
        return False, vix_value
    vix = _to_float(vix_value, 0.0) if vix_value is not None else _current_vix()
    if vix is None:
        return False, None
    return vix > float(risk_off_vix_ceiling), vix


def select_external_candidates(
    *,
    segment: str,
    symbols: list[str],
    top_n: int,
    regime_vix_ceiling: float = 25.0,
) -> list[str]:
    if not symbols:
        return []
    skip, _vix = should_skip_cycle_for_vix(
        segment=segment,
        risk_off_vix_ceiling=regime_vix_ceiling,
        risk_off_segments=["pennyStocks"],
    )
    if skip:
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


def discover_segment_candidates(
    *,
    segment: str,
    base_symbols: list[str],
    top_n: int,
    discovery_cfg: dict[str, Any] | None = None,
) -> list[str]:
    cfg = discovery_cfg or {}
    if not bool(cfg.get("enabled", False)):
        return base_symbols[:top_n]

    cache_minutes = int(cfg.get("cacheMinutes", 10))
    cache_key = f"{segment}:{top_n}"
    now = datetime.now(timezone.utc)
    cached = _DISCOVERY_CACHE.get(cache_key)
    if cached and (now - cached[0]) <= timedelta(minutes=max(cache_minutes, 1)):
        return cached[1]

    seed = DISCOVERY_SEED_UNIVERSE.get(segment, [])
    # Keep deterministic order while merging.
    merged_symbols: list[str] = []
    seen: set[str] = set()
    for symbol in [*base_symbols, *seed]:
        sym = str(symbol or "").strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        merged_symbols.append(sym)

    max_scan = int(cfg.get("maxSymbolsToScan", 160))
    scan_symbols = merged_symbols[: max(max_scan, top_n)]
    min_price_by_segment = cfg.get("minPriceUsdBySegment", {})
    min_dollar_by_segment = cfg.get("minDollarVolumeUsdBySegment", {})
    default_min_price = 2.0 if segment == "pennyStocks" else 10.0
    default_min_dollar = 2_000_000.0 if segment == "pennyStocks" else 20_000_000.0
    min_price = float(
        (min_price_by_segment.get(segment) if isinstance(min_price_by_segment, dict) else None)
        or cfg.get("minPriceUsd", default_min_price)
    )
    min_dollar_volume = float(
        (min_dollar_by_segment.get(segment) if isinstance(min_dollar_by_segment, dict) else None)
        or cfg.get("minDollarVolumeUsd", default_min_dollar)
    )

    scored: list[tuple[str, float]] = []
    if segment == "crypto":
        for sym in scan_symbols:
            yahoo_sym = CRYPTO_TO_YAHOO.get(sym)
            if not yahoo_sym:
                continue
            scored.append(
                _score_and_filter_symbol(
                    sym,
                    yahoo_sym,
                    period="7d",
                    interval="1h",
                    min_price=min_price,
                    min_dollar_volume=min_dollar_volume,
                )
            )
    else:
        for sym in scan_symbols:
            scored.append(
                _score_and_filter_symbol(
                    sym,
                    sym,
                    period="1mo",
                    interval="1d",
                    min_price=min_price,
                    min_dollar_volume=min_dollar_volume,
                )
            )

    scored = [row for row in scored if row[1] > -999.0]
    if not scored:
        fallback = base_symbols[:top_n]
        _DISCOVERY_CACHE[cache_key] = (now, fallback)
        return fallback

    scored.sort(key=lambda x: x[1], reverse=True)
    selected = [sym for sym, _score in scored[:top_n]]
    _DISCOVERY_CACHE[cache_key] = (now, selected)
    return selected
