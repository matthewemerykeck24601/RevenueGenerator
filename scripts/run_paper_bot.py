"""
run_paper_bot.py - Clean Paper Mode Runner for Agentic Crypto Churn
"""

import argparse
import time
import logging
import sys
from pathlib import Path
from datetime import datetime

# Ensure "src" package imports resolve when running as a script.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.revenue_generator.bot import RevenueBot
from src.revenue_generator.alpaca_client import AlpacaClient
from src.revenue_generator.exit_manager import ExitManager
from src.revenue_generator.config import load_risk_policy, build_runtime_config
from src.revenue_generator.journal import TradeJournal

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    parser = argparse.ArgumentParser(description="Clean Paper Bot for Daily Churn")
    parser.add_argument("--segment", type=str, default="crypto", choices=["crypto", "stocks"])
    parser.add_argument("--cycles", type=int, default=12, help="Number of cycles")
    parser.add_argument("--interval", type=int, default=240, help="Seconds between cycles")
    args = parser.parse_args()

    logger.info("Starting CLEAN PAPER MODE Agentic Revenue Churn Bot")
    logger.info(f"Segment: {args.segment} | Target: $100+ daily net churn")

    risk_policy = load_risk_policy()
    client = AlpacaClient(cfg=build_runtime_config())
    journal = TradeJournal()

    exit_manager = ExitManager(client=client, risk_policy=risk_policy, journal=journal)
    bot = RevenueBot(client=client)

    logger.info(
        f"Risk Profile: {risk_policy.get('profile')} | Max concurrent positions: "
        f"{risk_policy.get('default', {}).get('max_concurrent_positions', 10)}"
    )

    for cycle in range(1, args.cycles + 1):
        try:
            now = datetime.now()
            logger.info(f"--- CYCLE {cycle}/{args.cycles} @ {now.strftime('%H:%M:%S')} ---")

            executed = bot.run_cycle(segment=args.segment)
            if executed:
                logger.info(f"Executed {len(executed)} BUY signals this cycle")

            # Clean exits - no force clear
            exits = exit_manager.evaluate_and_execute_exits(dry_run=False)
            if exits:
                logger.info(f"Processed {len(exits)} exits/partials")

            if cycle < args.cycles:
                time.sleep(args.interval)

        except KeyboardInterrupt:
            logger.info("Stopped by user.")
            break
        except Exception as e:
            logger.error(f"Cycle error: {e}")
            time.sleep(30)

    logger.info("Paper run completed. Check logs/trades.db for journal and weekly_review.py for P&L.")


if __name__ == "__main__":
    main()
