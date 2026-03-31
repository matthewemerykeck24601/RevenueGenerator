"""
run_paper_bot.py - 60-Second Cycle Version with Clean Summary
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
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)


def main():
    parser = argparse.ArgumentParser(description="Agentic Churn Engine - 60s Cycles")
    parser.add_argument("--segment", type=str, default="crypto")
    parser.add_argument("--cycles", type=int, default=20)   # ~20 minutes at 60s
    parser.add_argument("--interval", type=int, default=60) # 60 seconds
    args = parser.parse_args()

    logger.info("🚀 AGENTIC CHURN ENGINE - 60-Second Cycles")
    logger.info(f"Target: $150–$300+ net daily | Segment: {args.segment}")

    risk_policy = load_risk_policy()
    client = AlpacaClient(cfg=build_runtime_config())   # repo config handles paper/live
    journal = TradeJournal()

    exit_manager = ExitManager(client=client, risk_policy=risk_policy, journal=journal)
    bot = RevenueBot(client=client)

    total_buys = 0
    total_value = 0

    for cycle in range(1, args.cycles + 1):
        try:
            now = datetime.now()
            logger.info(f"--- CYCLE {cycle}/{args.cycles} @ {now.strftime('%H:%M:%S')} ---")

            executed = bot.run_cycle(segment=args.segment)
            if executed:
                total_buys += len(executed)
                for trade in executed:
                    value = trade.get("qty", 0) * trade.get("limit_price", 1000)
                    total_value += value
                    logger.info(f"BUY EXECUTED: {trade.get('qty')} {trade.get('symbol')} | Rationale: {trade.get('rationale', 'N/A')}")

            exits = exit_manager.evaluate_and_execute_exits(dry_run=True)
            if exits:
                logger.info(f"Processed {len(exits)} exit actions")

            if cycle < args.cycles:
                time.sleep(args.interval)

        except KeyboardInterrupt:
            logger.info("Stopped by user.")
            break
        except Exception as e:
            logger.error(f"Cycle error: {e}")
            time.sleep(10)

    logger.info("=== RUN SUMMARY ===")
    logger.info(f"Total BUY executions: {total_buys}")
    logger.info(f"Approximate value traded: ${total_value:,.0f}")
    logger.info(f"Target daily net: $150–$300+")
    logger.info("Review logs/trades.db or weekly_review.py for details")
    logger.info("Ready for live when enforce_paper_trading_only = false")


if __name__ == "__main__":
    main()
