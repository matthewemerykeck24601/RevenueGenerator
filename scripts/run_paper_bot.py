"""
run_paper_bot.py - Final Polished Daily Churn Engine
"""

import argparse
import time
import logging
import sys
from pathlib import Path
from datetime import datetime

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
    parser = argparse.ArgumentParser(description="Final Agentic Churn Engine")
    parser.add_argument("--segment", type=str, default="crypto")
    parser.add_argument("--cycles", type=int, default=12)
    parser.add_argument("--interval", type=int, default=240)
    args = parser.parse_args()

    logger.info("FINAL AGENTIC CHURN ENGINE - Paper Mode")
    logger.info(f"Target: $100–$300+ net daily | Segment: {args.segment}")

    risk_policy = load_risk_policy()
    client = AlpacaClient(cfg=build_runtime_config())  # your repo's config style
    journal = TradeJournal()

    exit_manager = ExitManager(client=client, risk_policy=risk_policy, journal=journal)
    bot = RevenueBot(client=client)

    total_buys = 0
    total_value_traded = 0

    for cycle in range(1, args.cycles + 1):
        try:
            now = datetime.now()
            logger.info(f"--- CYCLE {cycle}/{args.cycles} @ {now.strftime('%H:%M:%S')} ---")

            executed = bot.run_cycle(segment=args.segment)
            if executed:
                total_buys += len(executed)
                for trade in executed:
                    value = trade.get("qty", 0) * trade.get("limit_price", 1000)
                    total_value_traded += value
                    logger.info(f"BUY EXECUTED: {trade.get('qty')} {trade.get('symbol')} | Rationale: {trade.get('rationale', 'N/A')}")

            exits = exit_manager.evaluate_and_execute_exits(dry_run=True)
            if exits:
                logger.info(f"Processed {len(exits)} simulated exits")

            if cycle < args.cycles:
                time.sleep(args.interval)

        except KeyboardInterrupt:
            logger.info("Stopped by user.")
            break
        except Exception as e:
            logger.error(f"Cycle error: {e}")
            time.sleep(30)

    logger.info("=== RUN SUMMARY ===")
    logger.info(f"Total BUY executions: {total_buys}")
    logger.info(f"Approximate value traded: ${total_value_traded:,.0f}")
    logger.info("Target daily net: $100–$300+")
    logger.info("Review full journal in logs/trades.db or weekly_review.py")
    logger.info("Ready for live when: enforce_paper_trading_only = false")


if __name__ == "__main__":
    main()
