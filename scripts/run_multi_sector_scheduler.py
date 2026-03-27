import argparse
import json
import os
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
from revenue_generator.fear_climate import load_fear_climate_state
from revenue_generator.journal import TradeJournal
from revenue_generator.kraken_client import KrakenClient

SCHEDULER_HEARTBEAT_PATH = ROOT / "logs" / "scheduler_heartbeat.json"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run multi-sector recurring trading loop.")
    p.add_argument("--budget", required=True, type=float, help="Total budget across sectors.")
    p.add_argument("--execute", action="store_true", help="Place real orders.")
    p.add_argument("--tick", type=int, default=10, help="Scheduler heartbeat seconds.")
    p.add_argument("--once", action="store_true", help="Run one cycle for each sector then exit.")
    return p.parse_args()


def _write_scheduler_heartbeat(payload: dict[str, object]) -> None:
    SCHEDULER_HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    SCHEDULER_HEARTBEAT_PATH.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


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


def _is_us_session_open(ai_cfg: dict) -> tuple[bool, str]:
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


def _effective_profiles_for_clock(profiles: dict[str, dict], ai_cfg: dict) -> tuple[dict[str, dict], str]:
    in_window, _note = _is_us_session_open(ai_cfg)
    if in_window:
        return profiles, "normal"

    # Outside US market window: run crypto only and route full cycle budget there.
    crypto_profile = dict(profiles.get("crypto", {"enabled": True, "intervalSeconds": 300, "budgetPct": 100}))
    crypto_profile["enabled"] = True
    crypto_profile["budgetPct"] = 100.0
    return {"crypto": crypto_profile}, "crypto_only_after_hours"


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
            print(f"Kraken init failed ({err}), falling back to Alpaca for crypto.")
            crypto_broker = "alpaca"
    journal = TradeJournal()

    def _client_for_segment(segment: str) -> AlpacaClient | KrakenClient:
        if segment == "crypto" and crypto_broker == "kraken" and kraken_client is not None:
            return kraken_client
        return alpaca_client

    profiles = policy.get("sectorCadence") or _default_profiles()
    last_run: dict[str, datetime] = {}
    stop = {"flag": False}
    last_schedule_mode: str | None = None
    scheduler_cycles = 0
    consecutive_errors = 0

    def _stop_handler(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)
    _write_scheduler_heartbeat(
        {
            "pid": os.getpid(),
            "status": "started",
            "execute": bool(args.execute),
            "tick_seconds": max(2, args.tick),
            "consecutive_errors": consecutive_errors,
        }
    )

    def _combined_equity() -> float:
        try:
            alpaca_eq = float(alpaca_client.get_account().get("equity", 0))
        except Exception:
            alpaca_eq = 0.0
        kraken_eq = 0.0
        if kraken_client:
            try:
                kraken_eq = float(kraken_client.get_account().get("equity", 0))
            except Exception:
                pass
        return alpaca_eq + kraken_eq

    def _run_segment(segment: str, profile: dict) -> dict:
        client = _client_for_segment(segment)
        budget = args.budget * (float(profile.get("budgetPct", 0)) / 100.0)
        fear_state = load_fear_climate_state()
        if segment == "crypto" and bool(fear_state.get("enabled", False)):
            result = {
                "strategy": "rule_engine",
                "account_status": "ACTIVE",
                "segment": segment,
                "budget": budget,
                "execute": args.execute,
                "reason": "Fear climate mode ON: scheduler paused for new crypto entries.",
                "orders_planned": [],
                "orders_placed": [],
                "order_errors": [],
                "fear_climate": fear_state,
                "broker": "kraken" if (crypto_broker == "kraken" and kraken_client is not None) else "alpaca",
            }
            journal.log_cycle(result)
            print(json.dumps({"segment": segment, "at": datetime.now(timezone.utc).isoformat(), "result": result}, indent=2))
            return result
        ai_cfg = policy.get("aiScheduler", {})
        ai_enabled = bool(ai_cfg.get("enabled", False))
        ai_segments = ai_cfg.get("segments", ["pennyStocks", "crypto", "indexFunds"])
        use_ai = ai_enabled and segment in ai_segments
        advisory_only_segments = {str(s) for s in (ai_cfg.get("advisoryOnlySegments", []) or [])}
        advisory_only = segment in advisory_only_segments
        total_equity = _combined_equity() if kraken_client else None
        try:
            if use_ai:
                ai_error = None
                try:
                    result = run_ai_cycle(
                        client=client,
                        risk_policy=policy,
                        segment=segment,
                        budget=budget,
                        execute=args.execute,
                        combined_equity=total_equity,
                    )
                except Exception as err:
                    ai_error = str(err)
                    result = {
                        "segment": segment,
                        "budget": budget,
                        "execute": args.execute,
                        "ai_used": True,
                        "ai_allowed": False,
                        "ai_reason": f"AI cycle failed: {ai_error}",
                        "orders_planned": [],
                        "orders_placed": [],
                        "order_errors": [],
                    }
                ai_used = bool(result.get("ai_used", False))
                ai_skipped = not ai_used and result.get("ai_skipped_reason")
                ai_vetoed = ai_used and not result.get("ai_allowed", False)
                should_fallback = (ai_skipped or ai_vetoed) and bool(ai_cfg.get("fallbackToRuleEngine", True))
                if advisory_only:
                    fallback = run_once(
                        client=client,
                        risk_policy=policy,
                        segment=segment,
                        budget=budget,
                        execute=args.execute,
                    )
                    fallback["ai_advisory_only"] = True
                    fallback["ai_summary"] = {
                        "ai_used": ai_used,
                        "ai_allowed": result.get("ai_allowed"),
                        "ai_reason": result.get("ai_reason") or result.get("ai_skipped_reason"),
                        "ai_signal": result.get("ai_signal"),
                    }
                    if ai_error:
                        fallback["ai_error"] = ai_error
                    result = fallback
                elif should_fallback:
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
                        "ai_reason": result.get("ai_reason") or result.get("ai_skipped_reason"),
                        "ai_signal": result.get("ai_signal"),
                    }
                    if ai_error:
                        fallback["ai_error"] = ai_error
                    result = fallback
            else:
                result = run_once(
                    client=client,
                    risk_policy=policy,
                    segment=segment,
                    budget=budget,
                    execute=args.execute,
                )
            result["broker"] = "kraken" if (segment == "crypto" and crypto_broker == "kraken") else "alpaca"
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
        effective_profiles, schedule_mode = _effective_profiles_for_clock(profiles, policy.get("aiScheduler", {}))
        print(f"Scheduler mode: {schedule_mode}")
        for segment, profile in effective_profiles.items():
            if not profile.get("enabled", True):
                continue
            _run_segment(segment, profile)
        _write_scheduler_heartbeat(
            {
                "pid": os.getpid(),
                "status": "ok",
                "mode": "once",
                "schedule_mode": schedule_mode,
                "cycle_count": 1,
                "consecutive_errors": 0,
            }
        )
        return 0

    print("Multi-sector scheduler started. Press Ctrl+C to stop.")
    while not stop["flag"]:
        loop_started = datetime.now(timezone.utc)
        try:
            effective_profiles, schedule_mode = _effective_profiles_for_clock(profiles, policy.get("aiScheduler", {}))
            if schedule_mode != last_schedule_mode:
                print(f"Scheduler mode switched: {schedule_mode}")
                last_schedule_mode = schedule_mode
            now = datetime.now(timezone.utc)
            ran_segments: list[str] = []
            for segment, profile in effective_profiles.items():
                if not profile.get("enabled", True):
                    continue
                interval = int(profile.get("intervalSeconds", 300))
                previous = last_run.get(segment)
                if previous is None or (now - previous).total_seconds() >= interval:
                    _run_segment(segment, profile)
                    last_run[segment] = now
                    ran_segments.append(segment)
            scheduler_cycles += 1
            consecutive_errors = 0
            max_segment_interval = max([int(p.get("intervalSeconds", 300)) for p in effective_profiles.values()] or [300])
            _write_scheduler_heartbeat(
                {
                    "pid": os.getpid(),
                    "status": "ok",
                    "execute": bool(args.execute),
                    "schedule_mode": schedule_mode,
                    "cycle_count": scheduler_cycles,
                    "loop_started_at": loop_started.isoformat(),
                    "ran_segments": ran_segments,
                    "active_segments": [s for s, p in effective_profiles.items() if p.get("enabled", True)],
                    "max_segment_interval_seconds": max_segment_interval,
                    "stale_after_seconds": max(max_segment_interval * 2, max(2, args.tick) * 3),
                    "consecutive_errors": consecutive_errors,
                }
            )
        except Exception as err:
            consecutive_errors += 1
            print(
                json.dumps(
                    {
                        "scheduler_watchdog_error": str(err),
                        "at": datetime.now(timezone.utc).isoformat(),
                        "consecutive_errors": consecutive_errors,
                    },
                    indent=2,
                )
            )
            _write_scheduler_heartbeat(
                {
                    "pid": os.getpid(),
                    "status": "error",
                    "execute": bool(args.execute),
                    "cycle_count": scheduler_cycles,
                    "loop_started_at": loop_started.isoformat(),
                    "error": str(err),
                    "consecutive_errors": consecutive_errors,
                }
            )
            if consecutive_errors >= 3:
                try:
                    cfg = build_runtime_config()
                    policy = ensure_risk_policy()
                    alpaca_client = AlpacaClient(cfg=cfg)
                    kraken_client = None
                    crypto_broker = str(policy.get("cryptoBroker", "alpaca")).lower()
                    if crypto_broker == "kraken":
                        try:
                            kraken_client = KrakenClient()
                        except ValueError as inner_err:
                            print(f"Kraken re-init failed ({inner_err}), falling back to Alpaca for crypto.")
                            crypto_broker = "alpaca"
                    profiles = policy.get("sectorCadence") or _default_profiles()
                    consecutive_errors = 0
                    print(json.dumps({"scheduler_watchdog_recovery": "runtime_reinitialized", "crypto_broker": crypto_broker}, indent=2))
                    _write_scheduler_heartbeat(
                        {
                            "pid": os.getpid(),
                            "status": "recovered",
                            "execute": bool(args.execute),
                            "cycle_count": scheduler_cycles,
                            "crypto_broker": crypto_broker,
                            "consecutive_errors": consecutive_errors,
                        }
                    )
                except Exception as recover_err:
                    print(json.dumps({"scheduler_watchdog_recovery_failed": str(recover_err)}, indent=2))
                    _write_scheduler_heartbeat(
                        {
                            "pid": os.getpid(),
                            "status": "recovery_failed",
                            "execute": bool(args.execute),
                            "cycle_count": scheduler_cycles,
                            "error": str(recover_err),
                            "consecutive_errors": consecutive_errors,
                        }
                    )
        time.sleep(max(2, args.tick))

    _write_scheduler_heartbeat(
        {
            "pid": os.getpid(),
            "status": "stopped",
            "execute": bool(args.execute),
            "cycle_count": scheduler_cycles,
            "consecutive_errors": consecutive_errors,
        }
    )
    print("Stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
