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
from revenue_generator.exit_manager import ExitManager
from revenue_generator.journal import TradeJournal
from revenue_generator.kraken_client import KrakenClient


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Monitor open positions for dynamic sell triggers.")
    p.add_argument("--interval", type=int, default=20, help="Seconds between monitoring cycles.")
    p.add_argument("--execute", action="store_true", help="Place sell orders. Default is dry-run.")
    p.add_argument("--once", action="store_true", help="Run a single monitoring cycle then exit.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = build_runtime_config()
    policy = ensure_risk_policy()
    alpaca_client = AlpacaClient(cfg=cfg)
    kraken_client: KrakenClient | None = None
    crypto_broker = str(policy.get("cryptoBroker", "alpaca")).lower()
    if crypto_broker == "kraken":
        try:
            kraken_client = KrakenClient()
        except ValueError as err:
            print(f"Kraken init failed ({err}), falling back to Alpaca for crypto exits.")
            crypto_broker = "alpaca"
    journal = TradeJournal()
    manager = ExitManager(
        client=alpaca_client,
        risk_policy=policy,
        journal=journal,
        crypto_client=kraken_client if crypto_broker == "kraken" else None,
    )

    stop_flag = {"stop": False}

    def _stop_handler(_signum, _frame) -> None:
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    print(f"Exit manager started. execute={args.execute}. Ctrl+C to stop.")
    if args.once:
        print(json.dumps(manager.run_cycle(execute=args.execute), indent=2))
        return 0

    while not stop_flag["stop"]:
        result = manager.run_cycle(execute=args.execute)
        print(json.dumps(result, indent=2))
        time.sleep(max(5, args.interval))
    print("Exit manager stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
