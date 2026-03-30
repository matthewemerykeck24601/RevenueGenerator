"""
External Research Module - Final Tuned Version for Consistent Churn
"""

import logging
import yfinance as yf
from datetime import datetime
from typing import Dict, Any

from .fear_climate import get_fear_climate
from .config import load_risk_policy

logger = logging.getLogger(__name__)

DEFAULT_CRYPTO_CANDIDATES = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD", "AVAX-USD", "DOGE-USD"]


def safe_float(value) -> float:
    if value is None:
        return 0.0
    try:
        if hasattr(value, "item"):
            return float(value.item())
        if hasattr(value, "iloc"):
            return float(value.iloc[0])
        return float(value)
    except:
        return 0.0


def get_technical_signals(ticker: str, period: str = "2d", interval: str = "5m") -> Dict:
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False, timeout=10)
        if data.empty or len(data) < 5:
            return {"ticker": ticker, "error": "no_data"}

        latest = data.iloc[-1]
        prev = data.iloc[-2] if len(data) > 1 else latest

        current_price = safe_float(latest.get("Close"))
        prev_price = safe_float(prev.get("Close"))

        price_change = 0.0
        if prev_price > 0:
            price_change = (current_price - prev_price) / prev_price * 100

        volume_surge = 1.0
        try:
            vol_series = data["Volume"]
            avg_vol = safe_float(vol_series.rolling(20).mean().iloc[-1])
            current_vol = safe_float(latest.get("Volume"))
            volume_surge = current_vol / avg_vol if avg_vol > 0 else 1.0
        except:
            pass

        return {
            "ticker": ticker,
            "current_price": current_price,
            "price_change_5m": price_change,
            "volume_surge_ratio": volume_surge,
            "is_momentum": price_change > 0.18,   # even looser
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.warning(f"Technical signals failed for {ticker}: {e}")
        return {"ticker": ticker, "error": str(e)}


def get_segment_research(segment: str = "crypto", limit: int = 8) -> Dict[str, Any]:
    risk_policy = load_risk_policy()
    fear = get_fear_climate()
    regime = "aggressive" if fear.get("bullish", False) or fear.get("vix_level", 20) < 18 else "normal"

    candidates = DEFAULT_CRYPTO_CANDIDATES[:limit]

    research = {
        "segment": segment,
        "regime": regime,
        "fear_climate": fear,
        "timestamp": datetime.utcnow().isoformat(),
        "candidates": [],
    }

    for ticker in candidates:
        tech = get_technical_signals(ticker)
        if "error" in tech:
            continue

        volume_ok = tech.get("volume_surge_ratio", 1.0) > 1.1
        momentum_ok = tech.get("is_momentum", False) or abs(tech.get("price_change_5m", 0)) > 0.12

        if volume_ok or momentum_ok:
            research["candidates"].append(
                {
                    "ticker": ticker,
                    "price": tech["current_price"],
                    "momentum_score": round(tech.get("price_change_5m", 0) * tech.get("volume_surge_ratio", 1.0), 2),
                    "volume_surge": round(tech.get("volume_surge_ratio", 1.0), 2),
                    "short_term_bias": "bullish" if tech.get("price_change_5m", 0) > 0 else "bearish",
                }
            )

    research["candidates"].sort(key=lambda x: x.get("momentum_score", 0), reverse=True)
    logger.info(f"Research completed for {segment}: {len(research['candidates'])} candidates in {regime} regime")
    return research
