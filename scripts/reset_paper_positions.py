"""
Final Paper Position Reset Helper
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.revenue_generator.alpaca_client import AlpacaClient
from src.revenue_generator.config import build_runtime_config
from src.revenue_generator.exit_manager import normalize_crypto_symbol

client = AlpacaClient(cfg=build_runtime_config())

print("=== FINAL PAPER POSITION RESET ===")

try:
    positions = client.get_open_positions()
    print(f"Found {len(positions)} open positions.")

    if len(positions) == 0:
        print("No open positions. Ready for clean testing!")
    else:
        print("API sells are still failing with 403. Please manually close them:")
        print("   -> Log into Alpaca web UI -> Positions -> Close all remaining positions manually")
        for pos in positions:
            symbol = pos.get("symbol", "")
            qty = abs(float(pos.get("qty", 0)))
            norm = normalize_crypto_symbol(symbol)
            print(f"   - {qty} {symbol} (normalized: {norm})")

except Exception as e:
    print(f"Error: {e}")

print("\nAfter manual cleanup, run the paper bot again.")
