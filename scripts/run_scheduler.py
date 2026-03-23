import argparse
import json
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.config import build_runtime_config, ensure_risk_policy
from revenue_generator.journal import TradeJournal
from revenue_generator.scheduler import BotScheduler, RunnerConfig


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run recurring paper-trading scheduler.")
    p.add_argument("--segment", required=True, choices=["largeCapStocks", "pennyStocks", "crypto", "indexFunds"])
    p.add_argument("--budget", required=True, type=float)
    p.add_argument("--interval", default=300, type=int, help="Seconds between runs.")
    p.add_argument("--execute", action="store_true", help="Actually place paper orders.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = build_runtime_config()
    policy = ensure_risk_policy()
    client = AlpacaClient(cfg=cfg)
    journal = TradeJournal()
    scheduler = BotScheduler(client=client, risk_policy=policy, journal=journal)

    run_cfg = RunnerConfig(
        segment=args.segment,
        budget=args.budget,
        execute=args.execute,
        interval_seconds=args.interval,
    )
    scheduler.start(run_cfg)
    print("Scheduler started. Press Ctrl+C to stop.")

    def _stop_handler(_signum, _frame) -> None:
        scheduler.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    while True:
        print(json.dumps(scheduler.status(), indent=2))
        time.sleep(10)


if __name__ == "__main__":
    raise SystemExit(main())
