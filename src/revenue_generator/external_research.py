"""
External Research Module - Tuned for Agentic Momentum/Scalping Churn
Provides dynamic candidates, technical signals, and market context.
"""

import logging
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import json
from typing import Dict, List, Any

from .fear_climate import get_fear_climate
from .config import load_risk_policy

logger = logging.getLogger(__name__)

# High-liquidity default watchlists (expand as needed)
DEFAULT_STOCK_CANDIDATES = ["SPY", "QQQ", "IWM", "TSLA", "NVDA", "AAPL", "AMD", "META", "AMZN", "GOOGL", "MSFT", "SMCI"]
DEFAULT_CRYPTO_CANDIDATES = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "BNB-USD", "DOGE-USD", "ADA-USD"]


def get_technical_signals(ticker: str, period: str = "5d", interval: str = "5m") -> Dict:
    """Fetch short-term technicals for scalping/momentum decisions"""
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False)
        if data.empty:
            return {"error": "no_data"}

        # Basic momentum indicators
        data["returns"] = data["Close"].pct_change()
        data["volume_surge"] = data["Volume"] / data["Volume"].rolling(20).mean()

        latest = data.iloc[-1]
        prev = data.iloc[-2] if len(data) > 1 else latest

        rsi = None
        try:
            delta = data["Close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
        except Exception:
            pass

        signal = {
            "ticker": ticker,
            "current_price": float(latest["Close"]),
            "price_change_5m": float((latest["Close"] - prev["Close"]) / prev["Close"] * 100) if len(data) > 1 else 0,
            "volume_surge_ratio": float(latest["volume_surge"]) if "volume_surge" in latest else 1.0,
            "rsi": float(rsi) if rsi is not None else None,
            "is_momentum": latest["Close"] > data["Close"].rolling(20).mean().iloc[-1],
            "timestamp": datetime.utcnow().isoformat(),
        }
        return signal
    except Exception as e:
        logger.warning(f"Technical signals failed for {ticker}: {e}")
        return {"ticker": ticker, "error": str(e)}


def get_segment_research(segment: str = "crypto", limit: int = 12) -> Dict[str, Any]:
    """Main research function - returns rich candidates + signals for the agent"""
    risk_policy = load_risk_policy()
    fear = get_fear_climate()

    regime = "aggressive" if fear.get("bullish", False) or fear.get("vix_level", 20) < 18 else "normal"

    if segment == "crypto":
        candidates = DEFAULT_CRYPTO_CANDIDATES[:limit]
        min_liquidity = risk_policy.get("default", {}).get("crypto", {}).get("min_notional_usd", 80)
    else:
        candidates = DEFAULT_STOCK_CANDIDATES[:limit]
        min_liquidity = risk_policy.get("default", {}).get("stocks", {}).get("min_notional_usd", 350)

    research = {
        "segment": segment,
        "regime": regime,
        "fear_climate": fear,
        "timestamp": datetime.utcnow().isoformat(),
        "candidates": [],
        "market_context": "Green tape momentum mode active" if regime == "aggressive" else "Neutral regime",
    }

    for ticker in candidates:
        tech = get_technical_signals(ticker, period="2d", interval="5m" if segment == "crypto" else "15m")

        if "error" in tech:
            continue

        # Filter for momentum/scalping opportunities
        volume_ok = tech.get("volume_surge_ratio", 1.0) > 1.3
        momentum_ok = tech.get("is_momentum", False) or (tech.get("price_change_5m", 0) > 0.3)
        rsi_ok = (tech.get("rsi") is None) or (30 < tech.get("rsi", 50) < 75)  # Avoid extremes but allow momentum

        if regime == "aggressive" or (volume_ok and momentum_ok and rsi_ok):
            research["candidates"].append(
                {
                    "ticker": ticker,
                    "price": tech["current_price"],
                    "momentum_score": round(tech.get("price_change_5m", 0) * tech.get("volume_surge_ratio", 1.0), 2),
                    "rsi": tech.get("rsi"),
                    "volume_surge": round(tech.get("volume_surge_ratio", 1.0), 2),
                    "short_term_bias": "bullish" if tech.get("price_change_5m", 0) > 0 else "bearish",
                }
            )

    # Sort by momentum score for agent priority
    research["candidates"].sort(key=lambda x: x.get("momentum_score", 0), reverse=True)

    logger.info(f"Research completed for {segment}: {len(research['candidates'])} momentum candidates in {regime} regime")
    return research


def get_latest_news(ticker: str) -> List[Dict]:
    """Basic news fetch (expand with paid API if needed later)"""
    try:
        ticker_obj = yf.Ticker(ticker)
        news = ticker_obj.news[:5]  # yfinance news
        return [{"title": item.get("title"), "publisher": item.get("publisher"), "time": item.get("providerPublishTime")} for item in news]
    except Exception:
        return []


# For direct tool use in agent
def get_fear_climate_wrapper():
    return get_fear_climate()


# Keep backward compatibility
def get_research_for_segment(segment: str = "crypto"):
    return get_segment_research(segment)
