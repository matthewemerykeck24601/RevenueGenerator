"""
Agentic AI Bridge - Real Anthropic Integration with Memory & Structured Output
"""

import json
import logging
import time
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
        self.prompt_version = "agentic_churn_v25_20260330"

    def _get_llm_client(self):
        try:
            from anthropic import Anthropic

            logger.info("Using Anthropic client (real call)")
            return Anthropic(), "anthropic"
        except ImportError:
            logger.error("Anthropic package not installed. Run: pip install anthropic")
            raise

    def analyze_segment(self, segment: str = "crypto") -> Dict[str, Any]:
        if time.time() - self.last_call_time < self.cooldown_seconds:
            return {"action": "HOLD", "reason": "cooldown"}

        self.last_call_time = time.time()

        try:
            client, _ = self._get_llm_client()

            fear = get_fear_climate()
            regime = "aggressive_mode" if fear.get("bullish", False) or fear.get("vix", 20) < 18 else "normal_mode"

            research = get_segment_research(segment)
            recent_trades = get_recent_journal(limit=6)

            system_prompt = f"""You are an expert momentum/scalping crypto trader targeting consistent daily profit churn ($100+ net).

Current regime: {regime} — be decisive on strong momentum but respect risk gates.
Recent trades for context: {json.dumps(recent_trades, default=str)}

Analyze the latest research and return ONLY valid JSON:
{{
  "action": "BUY" or "HOLD",
  "ticker": "e.g. SOL-USD",
  "confidence": number (0.58 to 0.95),
  "edge_percent": number (estimated edge, minimum 0.75),
  "size_percent": number (suggested % of equity),
  "rationale": "short clear reason"
}}"""

            user_message = f"""
Segment: {segment}
Fear Climate: {json.dumps(fear)}
Research candidates: {json.dumps(research.get('candidates', []), default=str)[:3000]}
"""

            # Real Anthropic call with structured output guidance
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=800,
                temperature=0.3,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            # Extract and parse JSON from response
            content = response.content[0].text
            # Simple JSON extraction (can be improved with regex if needed)
            import re

            json_match = re.search(r"\{.*\}", content, re.DOTALL)
            if json_match:
                signal = json.loads(json_match.group(0))
            else:
                signal = {"action": "HOLD", "reason": "parse_failed"}

            # Normalize
            if signal.get("action", "").upper() not in ["BUY", "HOLD"]:
                signal["action"] = "HOLD"

            logger.info(
                f"Agentic signal generated: {signal.get('action')} {signal.get('ticker')} | conf {signal.get('confidence', 0):.2f} | edge {signal.get('edge_percent', 0):.2f}%"
            )

            # Final safety: return clean dict
            clean_signal = {
                "action": signal.get("action", "HOLD"),
                "ticker": signal.get("ticker", ""),
                "confidence": float(signal.get("confidence", 0.62)),
                "edge_percent": float(signal.get("edge_percent", 0.0)),
                "size_percent": float(signal.get("size_percent", 5.0)),
                "rationale": signal.get("rationale", "AI decision"),
            }

            return clean_signal

        except Exception as e:
            logger.error(f"AI Bridge error: {e}")
            return {"action": "HOLD", "reason": f"error: {str(e)}", "ticker": "", "confidence": 0.0, "edge_percent": 0.0}


ai_bridge = AgenticAIBridge()


def analyze_segment(segment: str = "crypto"):
    return ai_bridge.analyze_segment(segment)
