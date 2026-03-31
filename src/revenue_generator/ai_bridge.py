"""
Agentic AI Bridge - Robust with Backoff + OpenAI gpt-4o Fallback
"""

import json
import logging
import time
import random
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
        self.prompt_version = "agentic_churn_v36_20260331"

    def _get_llm_client(self):
        # Prefer Anthropic
        try:
            from anthropic import Anthropic
            logger.info("Using Anthropic client")
            return Anthropic(), "anthropic"
        except Exception as e:
            logger.warning(f"Anthropic unavailable: {e}. Trying OpenAI fallback.")

        # Fallback to OpenAI gpt-4o
        try:
            from openai import OpenAI
            logger.info("Using OpenAI gpt-4o fallback")
            return OpenAI(), "openai"
        except Exception as e:
            logger.error(f"OpenAI fallback failed: {e}")
            raise

    def analyze_segment(self, segment: str = "crypto") -> Dict[str, Any]:
        if time.time() - self.last_call_time < self.cooldown_seconds:
            return {"action": "HOLD", "reason": "cooldown"}

        self.last_call_time = time.time()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                client, provider = self._get_llm_client()

                fear = get_fear_climate()
                regime = "aggressive_mode" if fear.get("bullish", False) or fear.get("vix", 20) < 18 else "normal_mode"

                research = get_segment_research(segment)
                recent_trades = get_recent_journal(limit=6)

                system_prompt = f"""You are an expert momentum/scalping crypto trader for daily profit churn.
Current regime: {regime}. Be decisive on strong momentum but respect risk.
Recent trades: {json.dumps(recent_trades, default=str)}

Return ONLY valid JSON:
{{
  "action": "BUY" or "HOLD",
  "ticker": "e.g. SOL-USD",
  "confidence": 0.58-0.95,
  "edge_percent": number (>=0.75),
  "size_percent": number,
  "rationale": "short reason"
}}"""

                user_message = f"Segment: {segment}\nFear Climate: {json.dumps(fear)}\nResearch: {json.dumps(research.get('candidates', []), default=str)[:2500]}"

                if provider == "anthropic":
                    response = client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=800,
                        temperature=0.3,
                        system=system_prompt,
                        messages=[{"role": "user", "content": user_message}]
                    )
                    content = response.content[0].text
                else:
                    response = client.chat.completions.create(
                        model="gpt-4o",
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_message}
                        ],
                        temperature=0.3,
                        response_format={"type": "json_object"}
                    )
                    content = response.choices[0].message.content

                # Parse JSON
                import re
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                signal = json.loads(json_match.group(0)) if json_match else {"action": "HOLD"}

                if signal.get("action", "").upper() not in ["BUY", "HOLD"]:
                    signal["action"] = "HOLD"

                logger.info(f"Agentic signal generated: {signal.get('action')} {signal.get('ticker')} | conf {signal.get('confidence', 0):.2f} | edge {signal.get('edge_percent', 0):.2f}%")

                return {
                    "action": signal.get("action", "HOLD"),
                    "ticker": signal.get("ticker", ""),
                    "confidence": float(signal.get("confidence", 0.0)),
                    "edge_percent": float(signal.get("edge_percent", 0.0)),
                    "size_percent": float(signal.get("size_percent", 5.0)),
                    "rationale": signal.get("rationale", "AI decision")
                }

            except Exception as e:
                if "529" in str(e) or "overloaded" in str(e).lower():
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Anthropic overloaded. Retrying in {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait)
                else:
                    logger.error(f"AI Bridge error: {e}")
                    break

        return {"action": "HOLD", "reason": "max_retries_reached", "ticker": "", "confidence": 0.0, "edge_percent": 0.0}


ai_bridge = AgenticAIBridge()


def analyze_segment(segment: str = "crypto"):
    return ai_bridge.analyze_segment(segment)
