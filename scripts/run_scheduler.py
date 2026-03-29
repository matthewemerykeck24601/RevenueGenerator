"""
Simple Scheduler Wrapper - For single-segment testing with high frequency
"""

import time
import logging
from src.revenue_generator.bot import RevenueBot
from src.revenue_generator.alpaca_client import AlpacaClient

logger = logging.getLogger(__name__)


def run_scheduler(segment: str = "crypto", interval_seconds: int = 480):  # 8 min default
    client = AlpacaClient()
    bot = RevenueBot(client=client)

    logger.info(f"Starting single-segment scheduler for {segment} every {interval_seconds//60} min")

    while True:
        try:
            bot.run_cycle(segment=segment)
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    run_scheduler(segment="crypto", interval_seconds=480)  # tune as needed
