import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
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


HEARTBEAT_PATH = ROOT / "logs" / "exit_manager_heartbeat.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Monitor open positions for dynamic sell triggers.")
    p.add_argument("--interval", type=int, default=20, help="Seconds between monitoring cycles.")
    p.add_argument("--execute", action="store_true", help="Place sell orders. Default is dry-run.")
    p.add_argument("--once", action="store_true", help="Run a single monitoring cycle then exit.")
    return p.parse_args()


def _write_heartbeat(payload: dict[str, object]) -> None:
    HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    HEARTBEAT_PATH.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _build_exit_manager(cfg: dict[str, object], policy: dict[str, object], journal: TradeJournal) -> tuple[ExitManager, str]:
    alpaca_client = AlpacaClient(cfg=cfg)
    kraken_client: KrakenClient | None = None
    crypto_broker = str(policy.get("cryptoBroker", "alpaca")).lower()
    if crypto_broker == "kraken":
        try:
            kraken_client = KrakenClient()
        except ValueError as err:
            print(f"Kraken init failed ({err}), falling back to Alpaca for crypto exits.")
            crypto_broker = "alpaca"
    manager = ExitManager(
        client=alpaca_client,
        risk_policy=policy,
        journal=journal,
        crypto_client=kraken_client if crypto_broker == "kraken" else None,
    )
    return manager, crypto_broker


def main() -> int:
    args = parse_args()
    journal = TradeJournal()
    cfg = build_runtime_config()
    policy = ensure_risk_policy()
    manager, crypto_broker = _build_exit_manager(cfg, policy, journal)

    stop_flag = {"stop": False}

    def _stop_handler(_signum, _frame) -> None:
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    print(f"Exit manager started. execute={args.execute}. Ctrl+C to stop.")
    _write_heartbeat(
        {
            "pid": os.getpid(),
            "status": "started",
            "execute": bool(args.execute),
            "interval_seconds": max(5, args.interval),
            "crypto_broker": crypto_broker,
            "consecutive_errors": 0,
        }
    )
    if args.once:
        try:
            result = manager.run_cycle(execute=args.execute)
            print(json.dumps(result, indent=2))
            _write_heartbeat(
                {
                    "pid": os.getpid(),
                    "status": "ok",
                    "mode": "once",
                    "result": {
                        "positions": int(result.get("positions", 0)),
                        "actions": len(result.get("actions", [])),
                    },
                    "consecutive_errors": 0,
                }
            )
            return 0
        except Exception as err:
            _write_heartbeat(
                {
                    "pid": os.getpid(),
                    "status": "error",
                    "mode": "once",
                    "error": str(err),
                    "consecutive_errors": 1,
                }
            )
            raise

    consecutive_errors = 0
    cycle_count = 0
    while not stop_flag["stop"]:
        loop_started = datetime.now(timezone.utc)
        try:
            result = manager.run_cycle(execute=args.execute)
            cycle_count += 1
            consecutive_errors = 0
            print(json.dumps(result, indent=2))
            _write_heartbeat(
                {
                    "pid": os.getpid(),
                    "status": "ok",
                    "execute": bool(args.execute),
                    "cycle_count": cycle_count,
                    "loop_started_at": loop_started.isoformat(),
                    "positions": int(result.get("positions", 0)),
                    "actions_count": len(result.get("actions", [])),
                    "suppressed": {
                        "backoff": int(result.get("reliability_suppressed_backoff", 0)),
                        "cooldown": int(result.get("reliability_suppressed_cooldown", 0)),
                        "min_qty": int(result.get("reliability_suppressed_min_qty", 0)),
                    },
                    "ai_exit_advisor": {
                        "calls": int(result.get("ai_advisor_calls", 0)),
                        "deferrals": int(result.get("ai_advisor_deferrals", 0)),
                    },
                    "consecutive_errors": consecutive_errors,
                }
            )
        except Exception as err:
            consecutive_errors += 1
            print(
                json.dumps(
                    {
                        "watchdog_error": str(err),
                        "at": datetime.now(timezone.utc).isoformat(),
                        "consecutive_errors": consecutive_errors,
                    },
                    indent=2,
                )
            )
            _write_heartbeat(
                {
                    "pid": os.getpid(),
                    "status": "error",
                    "execute": bool(args.execute),
                    "cycle_count": cycle_count,
                    "loop_started_at": loop_started.isoformat(),
                    "error": str(err),
                    "consecutive_errors": consecutive_errors,
                }
            )
            if consecutive_errors >= 3:
                try:
                    cfg = build_runtime_config()
                    policy = ensure_risk_policy()
                    manager, crypto_broker = _build_exit_manager(cfg, policy, journal)
                    consecutive_errors = 0
                    print(json.dumps({"watchdog_recovery": "manager_reinitialized", "crypto_broker": crypto_broker}, indent=2))
                    _write_heartbeat(
                        {
                            "pid": os.getpid(),
                            "status": "recovered",
                            "execute": bool(args.execute),
                            "cycle_count": cycle_count,
                            "crypto_broker": crypto_broker,
                            "consecutive_errors": consecutive_errors,
                        }
                    )
                except Exception as recover_err:
                    print(json.dumps({"watchdog_recovery_failed": str(recover_err)}, indent=2))
                    _write_heartbeat(
                        {
                            "pid": os.getpid(),
                            "status": "recovery_failed",
                            "execute": bool(args.execute),
                            "cycle_count": cycle_count,
                            "error": str(recover_err),
                            "consecutive_errors": consecutive_errors,
                        }
                    )
        time.sleep(max(5, args.interval))
    _write_heartbeat(
        {
            "pid": os.getpid(),
            "status": "stopped",
            "execute": bool(args.execute),
            "cycle_count": cycle_count,
            "consecutive_errors": consecutive_errors,
        }
    )
    print("Exit manager stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
