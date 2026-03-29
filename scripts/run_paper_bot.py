"""
run_paper_bot.py - Paper Mode Runner for Agentic Daily Profit Churn
Test the full agentic loop (research -> AI agent -> risk gate -> execute -> exits) in paper mode.
"""

import argparse
import time
import logging
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.bot import RevenueBot
from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.exit_manager import ExitManager
from revenue_generator.config import build_runtime_config, load_risk_policy
from revenue_generator.journal import TradeJournal

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    parser = argparse.ArgumentParser(description="Run Paper Bot for Agentic Churn Testing")
    parser.add_argument(
        "--segment",
        type=str,
        default="crypto",
        choices=["crypto", "stocks"],
        help="Segment to run (crypto for 24/7 churn)",
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=10,
        help="Number of cycles to run (default 10 for quick testing)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between cycles (default 5 min for paper testing)",
    )
    args = parser.parse_args()

    logger.info("Starting PAPER MODE Agentic Revenue Bot")
    logger.info(f"Segment: {args.segment} | Cycles: {args.cycles} | Interval: {args.interval}s")

    risk_policy = load_risk_policy()
    cfg = build_runtime_config()
    client = AlpacaClient(cfg=cfg)  # Ensure your .env is loaded with paper keys
    journal = TradeJournal()

    # Exit manager (dry_run=True for paper)
    exit_manager = ExitManager(
        client=client,
        risk_policy=risk_policy,
        journal=journal,
    )

    # Core agentic bot
    bot = RevenueBot(client=client)  # crypto_client if Kraken enabled

    logger.info(
        f"Risk Profile: {risk_policy.get('profile', 'unknown')} | "
        f"Paper trading enforced: {risk_policy.get('enforce_paper_trading_only', True)}"
    )

    for cycle in range(1, args.cycles + 1):
        try:
            now = datetime.now()
            logger.info(f"--- CYCLE {cycle}/{args.cycles} @ {now.strftime('%H:%M:%S')} ---")

            # Agentic cycle: research + AI + risk + execute
            executed = bot.run_cycle(segment=args.segment)

            logger.info(f"Cycle {cycle} completed: {len(executed)} signals executed in paper mode")

            # Frequent exits for profit realization & capital churn
            exits = exit_manager.evaluate_and_execute_exits(dry_run=True)
            if exits:
                logger.info(f"Exits/partials processed this cycle: {len(exits)}")

            # Short sleep for paper testing (increase for real run)
            if cycle < args.cycles:
                time.sleep(args.interval)

        except KeyboardInterrupt:
            logger.info("Paper bot stopped by user.")
            break
        except Exception as e:
            logger.error(f"Cycle {cycle} error: {e}")
            time.sleep(30)  # backoff

    logger.info("Paper bot run completed. Check logs/trades.db and logs/trades.csv for journal.")
    logger.info("Review with: python scripts/weekly_review.py or live_dashboard.py")


if __name__ == "__main__":
    main()
