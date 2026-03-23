import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.bot import run_once
from revenue_generator.config import build_runtime_config, ensure_risk_policy
from revenue_generator.journal import TradeJournal


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run one paper-trading cycle.")
    p.add_argument("--segment", required=True, choices=["largeCapStocks", "pennyStocks", "crypto", "indexFunds"])
    p.add_argument("--budget", required=True, type=float)
    p.add_argument("--execute", action="store_true", help="Actually place paper orders. Default is dry-run.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = build_runtime_config()
    policy = ensure_risk_policy()
    client = AlpacaClient(cfg=cfg)
    journal = TradeJournal()
    result = run_once(
        client=client,
        risk_policy=policy,
        segment=args.segment,
        budget=args.budget,
        execute=args.execute,
    )
    journal.log_cycle(result)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
