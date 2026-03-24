import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.ai_bridge import run_ai_cycle
from revenue_generator.bot import run_once
from revenue_generator.config import build_runtime_config, ensure_risk_policy
from revenue_generator.journal import TradeJournal


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run multi-sector recurring trading loop.")
    p.add_argument("--budget", required=True, type=float, help="Total budget across sectors.")
    p.add_argument("--execute", action="store_true", help="Place real orders.")
    p.add_argument("--tick", type=int, default=10, help="Scheduler heartbeat seconds.")
    p.add_argument("--once", action="store_true", help="Run one cycle for each sector then exit.")
    return p.parse_args()


def _default_profiles() -> dict:
    return {
        "pennyStocks": {"enabled": True, "intervalSeconds": 120, "budgetPct": 45},
        "crypto": {"enabled": True, "intervalSeconds": 300, "budgetPct": 35},
        "indexFunds": {"enabled": True, "intervalSeconds": 900, "budgetPct": 20},
    }


def _parse_hhmm(value: str, default_hour: int, default_minute: int) -> tuple[int, int]:
    try:
        h, m = str(value).split(":", 1)
        return int(h), int(m)
    except Exception:
        return default_hour, default_minute


def _is_in_market_window(ai_cfg: dict) -> tuple[bool, str]:
    if not bool(ai_cfg.get("marketHoursOnly", False)):
        return True, "marketHoursOnly disabled"
    tz_name = str(ai_cfg.get("timezone", "America/New_York"))
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    weekdays = ai_cfg.get("activeWeekdays", [0, 1, 2, 3, 4])
    if now_local.weekday() not in [int(x) for x in weekdays]:
        return False, "outside active weekdays"
    open_h, open_m = _parse_hhmm(str(ai_cfg.get("marketOpen", "09:30")), 9, 30)
    close_h, close_m = _parse_hhmm(str(ai_cfg.get("marketClose", "16:00")), 16, 0)
    current_mins = now_local.hour * 60 + now_local.minute
    open_mins = open_h * 60 + open_m
    close_mins = close_h * 60 + close_m
    in_window = open_mins <= current_mins < close_mins
    return in_window, f"{now_local.isoformat()} in {tz_name}"


def main() -> int:
    args = parse_args()
    cfg = build_runtime_config()
    policy = ensure_risk_policy()
    client = AlpacaClient(cfg=cfg)
    journal = TradeJournal()

    profiles = policy.get("sectorCadence") or _default_profiles()
    last_run: dict[str, datetime] = {}
    stop = {"flag": False}

    def _stop_handler(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    def _run_segment(segment: str, profile: dict) -> dict:
        budget = args.budget * (float(profile.get("budgetPct", 0)) / 100.0)
        ai_cfg = policy.get("aiScheduler", {})
        ai_enabled = bool(ai_cfg.get("enabled", False))
        ai_segments = ai_cfg.get("segments", ["pennyStocks", "crypto", "indexFunds"])
        market_window_ok, market_window_note = _is_in_market_window(ai_cfg)
        use_ai = ai_enabled and segment in ai_segments and market_window_ok
        try:
            if use_ai:
                result = run_ai_cycle(
                    client=client,
                    risk_policy=policy,
                    segment=segment,
                    budget=budget,
                    execute=args.execute,
                )
                if not result.get("ai_allowed", False) and bool(ai_cfg.get("fallbackToRuleEngine", True)):
                    fallback = run_once(
                        client=client,
                        risk_policy=policy,
                        segment=segment,
                        budget=budget,
                        execute=args.execute,
                    )
                    fallback["ai_fallback_used"] = True
                    fallback["ai_summary"] = {
                        "ai_allowed": result.get("ai_allowed"),
                        "ai_reason": result.get("ai_reason"),
                        "ai_signal": result.get("ai_signal"),
                    }
                    result = fallback
            else:
                result = run_once(
                    client=client,
                    risk_policy=policy,
                    segment=segment,
                    budget=budget,
                    execute=args.execute,
                )
                if ai_enabled and segment in ai_segments and not market_window_ok:
                    result["ai_skipped_reason"] = "outside_market_hours_window"
                    result["ai_skipped_detail"] = market_window_note
        except Exception as err:
            result = {
                "segment": segment,
                "budget": budget,
                "execute": args.execute,
                "error": str(err),
            }
        journal.log_cycle(result)
        print(json.dumps({"segment": segment, "at": datetime.now(timezone.utc).isoformat(), "result": result}, indent=2))
        return result

    if args.once:
        for segment, profile in profiles.items():
            if not profile.get("enabled", True):
                continue
            _run_segment(segment, profile)
        return 0

    print("Multi-sector scheduler started. Press Ctrl+C to stop.")
    while not stop["flag"]:
        now = datetime.now(timezone.utc)
        for segment, profile in profiles.items():
            if not profile.get("enabled", True):
                continue
            interval = int(profile.get("intervalSeconds", 300))
            previous = last_run.get(segment)
            if previous is None or (now - previous).total_seconds() >= interval:
                _run_segment(segment, profile)
                last_run[segment] = now
        time.sleep(max(2, args.tick))

    print("Stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
