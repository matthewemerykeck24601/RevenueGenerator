"""
Multi-Sector Scheduler - High-Frequency for Agentic Daily Profit Churn
Runs frequent cycles (5-12 min) with regime-aware speed for higher turnover.
"""

import time
import logging
import sys
from datetime import datetime
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.bot import RevenueBot
from revenue_generator.exit_manager import ExitManager
from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.config import build_runtime_config, load_risk_policy
from revenue_generator.journal import TradeJournal  # assume exists or add stub

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def is_market_open() -> bool:
    """Simple market hours check (extend with proper calendar if needed)"""
    now = datetime.now().time()
    # US equity hours approx (crypto always open)
    return (9 <= now.hour <= 16) or True  # crypto bias: always allow


def run_multi_sector_scheduler(dry_run: bool = True, base_interval_min: int = 8):
    """Main high-frequency scheduler loop"""
    risk_policy = load_risk_policy()
    client = AlpacaClient(cfg=build_runtime_config())  # your existing init
    journal = TradeJournal()  # or your journal instance
    exit_manager = ExitManager(client=client, risk_policy=risk_policy, journal=journal)

    bot = RevenueBot(client=client)  # crypto_client if needed

    logger.info("🚀 Starting Agentic Multi-Sector High-Frequency Scheduler (target $100+/day churn)")
    logger.info(f"Base cycle interval: {base_interval_min} min | Dry-run: {dry_run}")

    cycle_count = 0

    while True:
        try:
            cycle_count += 1
            now = datetime.now()
            regime = "aggressive_mode" if "aggressive" in str(risk_policy.get("profile", "")) else "normal"  # improve with fear_climate later

            # Dynamic interval (faster in aggressive/green)
            interval_min = 5 if regime == "aggressive_mode" else base_interval_min
            logger.info(f"Cycle {cycle_count} @ {now} | Regime: {regime} | Next in ~{interval_min} min")

            # Run crypto (24/7 priority for churn)
            logger.info("--- CRYPTO CYCLE ---")
            crypto_signals = bot.run_cycle(segment="crypto")
            logger.info(f"Executed {len(crypto_signals)} crypto signals")

            # Run stocks only during market hours
            if is_market_open():
                logger.info("--- STOCKS CYCLE ---")
                stock_signals = bot.run_cycle(segment="stocks")
                logger.info(f"Executed {len(stock_signals)} stock signals")
            else:
                logger.info("Stocks skipped (outside market hours)")

            # Run exits frequently (partial profits = capital for re-churn)
            exits = exit_manager.evaluate_and_execute_exits(dry_run=dry_run)
            logger.info(f"Processed {len(exits)} exits/partials")

            # Sleep with dynamic adjustment
            sleep_seconds = interval_min * 60
            time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user.")
            break
        except Exception as e:
            logger.error(f"Cycle error: {e}")
            time.sleep(60)  # short backoff


if __name__ == "__main__":
    # Default: aggressive frequency for churn testing
    run_multi_sector_scheduler(dry_run=True, base_interval_min=8)
