"""
Agentic AI Bridge - Robust Version with Dependency Handling for Daily Churn
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, Any

from .config import load_risk_policy
from .risk import validate_and_plan_signal
from .journal import get_recent_journal
from .external_research import get_segment_research, get_fear_climate

logger = logging.getLogger(__name__)


class AgenticAIBridge:
    def __init__(self):
        self.risk_policy = load_risk_policy()
        self.last_call_time = 0
        self.cooldown_seconds = 25
        self.prompt_version = "agentic_churn_v13_20260329"

    def _get_llm_client(self):
        """Robust client loading with clear fallbacks and warnings"""
        try:
            from anthropic import Anthropic

            logger.info("Using Anthropic client")
            return Anthropic(), "anthropic"
        except ImportError:
            logger.warning("Anthropic not installed. Trying OpenAI...")
            try:
                from openai import OpenAI

                logger.info("Using OpenAI client")
                return OpenAI(), "openai"
            except ImportError:
                logger.error("Neither 'anthropic' nor 'openai' package is installed!")
                logger.error("Run: pip install anthropic openai")
                raise ImportError("No LLM client available. Install anthropic or openai.")

    def analyze_segment(self, segment: str = "crypto") -> Dict[str, Any]:
        if time.time() - self.last_call_time < self.cooldown_seconds:
            return {"action": "HOLD", "reason": "cooldown"}

        self.last_call_time = time.time()

        try:
            client, provider = self._get_llm_client()

            fear = get_fear_climate()
            regime = "aggressive_mode" if fear.get("bullish", False) or fear.get("vix", 20) < 18 else "normal_mode"

            research = get_segment_research(segment)
            recent_trades = get_recent_journal(limit=5)

            system_prompt = f"""You are an expert momentum/scalping trader focused on daily crypto profit churn.
Current regime: {regime} (aggressive = more decisive buys on momentum).
Recent trades memory: {json.dumps(recent_trades, default=str)}

Analyze and output ONLY a valid JSON object:
{{
  "action": "BUY" or "HOLD",
  "ticker": "SYMBOL",
  "confidence": 0.0 to 1.0,
  "edge_percent": float,
  "size_percent": float,
  "rationale": "short explanation"
}}
Never output WAIT or SELL for new entries. Be action-oriented in aggressive regime."""

            # TODO: Replace this placeholder with your actual LLM call (messages + tools or structured output)
            # For now we simulate a strong signal to unblock testing
            signal = {
                "action": "BUY",
                "ticker": "SOL-USD",
                "confidence": 0.68,
                "edge_percent": 1.45,
                "size_percent": 5.5,
                "rationale": "Strong volume surge + momentum in aggressive regime",
            }

            # Normalize action
            if signal.get("action", "").upper() not in ["BUY", "HOLD"]:
                signal["action"] = "HOLD"

            validated = validate_and_plan_signal(
                signal=signal,
                risk_policy=self.risk_policy,
                segment=segment,
                fear_climate=fear,
            )

            logger.info(f"Agentic signal generated: {signal.get('action')} {signal.get('ticker')} | confidence {signal.get('confidence'):.2f} | regime {regime}")
            return validated

        except Exception as e:
            logger.error(f"AI Bridge error: {e}")
            return {"action": "HOLD", "reason": f"error: {str(e)}"}


# Singleton
ai_bridge = AgenticAIBridge()


def analyze_segment(segment: str = "crypto"):
    return ai_bridge.analyze_segment(segment)
