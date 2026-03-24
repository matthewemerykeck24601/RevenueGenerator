from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.config import build_runtime_config, ensure_risk_policy
from revenue_generator.journal import TradeJournal


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Weekly strategy attribution and execution-quality review.")
    p.add_argument("--days", type=int, default=7, help="Lookback window in days.")
    p.add_argument("--apply", action="store_true", help="Apply bounded config tuning changes.")
    return p.parse_args()


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def _load_cycle_rows(db_path: Path, since_iso: str) -> list[sqlite3.Row]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT ts, segment, execute, strategy, orders_planned_json, orders_placed_json, order_errors_json, raw_result_json
            FROM cycles
            WHERE ts >= ?
            ORDER BY ts ASC
            """,
            (since_iso,),
        ).fetchall()
        return rows
    finally:
        con.close()


def _review_cycles(rows: list[sqlite3.Row]) -> dict[str, Any]:
    strategy_counts: dict[str, int] = defaultdict(int)
    strategy_placed: dict[str, int] = defaultdict(int)
    segment_counts: dict[str, int] = defaultdict(int)
    segment_planned: dict[str, int] = defaultdict(int)
    segment_placed: dict[str, int] = defaultdict(int)
    order_error_count = 0
    planned_total = 0
    placed_total = 0

    for row in rows:
        strategy = str(row["strategy"] or "unknown")
        segment = str(row["segment"] or "unknown")
        try:
            planned = json.loads(row["orders_planned_json"] or "[]")
        except json.JSONDecodeError:
            planned = []
        try:
            placed = json.loads(row["orders_placed_json"] or "[]")
        except json.JSONDecodeError:
            placed = []
        try:
            errs = json.loads(row["order_errors_json"] or "[]")
        except json.JSONDecodeError:
            errs = []

        strategy_counts[strategy] += 1
        strategy_placed[strategy] += len(placed)
        segment_counts[segment] += 1
        segment_planned[segment] += len(planned)
        segment_placed[segment] += len(placed)
        planned_total += len(planned)
        placed_total += len(placed)
        order_error_count += len(errs)

    strategy_attr = []
    for k in sorted(strategy_counts.keys()):
        strategy_attr.append(
            {
                "strategy": k,
                "cycles": strategy_counts[k],
                "orders_placed": strategy_placed[k],
                "placed_per_cycle": round(_safe_div(strategy_placed[k], strategy_counts[k]), 3),
            }
        )

    segment_attr = []
    for k in sorted(segment_counts.keys()):
        ratio = _safe_div(segment_placed[k], segment_planned[k])
        segment_attr.append(
            {
                "segment": k,
                "cycles": segment_counts[k],
                "orders_planned": segment_planned[k],
                "orders_placed": segment_placed[k],
                "placement_ratio": round(ratio, 3),
            }
        )

    placement_ratio = _safe_div(placed_total, planned_total)
    return {
        "cycle_count": len(rows),
        "orders_planned_total": planned_total,
        "orders_placed_total": placed_total,
        "placement_ratio": placement_ratio,
        "order_error_count": order_error_count,
        "strategy_attribution": strategy_attr,
        "segment_attribution": segment_attr,
    }


def _review_execution_quality(client: AlpacaClient, since_iso: str) -> dict[str, Any]:
    orders = client.get_orders(status="all", limit=500, direction="desc", after=since_iso)
    submitted = len(orders)
    filled = 0
    rejected = 0
    canceled = 0
    expired = 0
    other_openish = 0
    buy_slippage_bps: list[float] = []
    sell_slippage_bps: list[float] = []

    for o in orders:
        status = str(o.get("status", "")).lower()
        side = str(o.get("side", "")).lower()
        if status in {"filled", "partially_filled"}:
            filled += 1
        elif status in {"rejected"}:
            rejected += 1
        elif status in {"canceled", "cancelled"}:
            canceled += 1
        elif status in {"expired"}:
            expired += 1
        else:
            other_openish += 1

        limit_price = o.get("limit_price")
        filled_avg = o.get("filled_avg_price")
        if limit_price is None or filled_avg is None:
            continue
        try:
            limit_v = float(limit_price)
            fill_v = float(filled_avg)
        except (TypeError, ValueError):
            continue
        if limit_v <= 0:
            continue
        # Positive means favorable execution versus the limit.
        if side == "buy":
            buy_slippage_bps.append(((limit_v - fill_v) / limit_v) * 10000.0)
        elif side == "sell":
            sell_slippage_bps.append(((fill_v - limit_v) / limit_v) * 10000.0)

    fill_rate = _safe_div(filled, submitted)
    missed = rejected + canceled + expired
    missed_rate = _safe_div(missed, submitted)
    return {
        "orders_submitted": submitted,
        "orders_filled": filled,
        "orders_rejected": rejected,
        "orders_canceled": canceled,
        "orders_expired": expired,
        "orders_pending_or_other": other_openish,
        "fill_rate": fill_rate,
        "missed_fill_rate": missed_rate,
        "avg_buy_slippage_bps": round(_safe_div(sum(buy_slippage_bps), len(buy_slippage_bps)), 3),
        "avg_sell_slippage_bps": round(_safe_div(sum(sell_slippage_bps), len(sell_slippage_bps)), 3),
    }


def _bounded_tuning(policy: dict[str, Any], cycle_review: dict[str, Any], exec_review: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    changes: list[str] = []
    tuned = json.loads(json.dumps(policy))

    overall_place_ratio = float(cycle_review.get("placement_ratio", 0.0))
    err_rate = _safe_div(float(cycle_review.get("order_error_count", 0)), max(float(cycle_review.get("orders_planned_total", 1)), 1.0))
    min_conf = float(tuned.get("minSignalConfidence", 0.35))
    if err_rate > 0.2:
        new_conf = min(0.7, min_conf + 0.02)
        if new_conf != min_conf:
            tuned["minSignalConfidence"] = round(new_conf, 3)
            changes.append(f"Increase minSignalConfidence {min_conf:.3f} -> {new_conf:.3f} (high error rate).")
    elif err_rate < 0.05 and overall_place_ratio > 0.6:
        new_conf = max(0.25, min_conf - 0.01)
        if new_conf != min_conf:
            tuned["minSignalConfidence"] = round(new_conf, 3)
            changes.append(f"Decrease minSignalConfidence {min_conf:.3f} -> {new_conf:.3f} (healthy fill profile).")

    segment_rows = {r["segment"]: r for r in cycle_review.get("segment_attribution", [])}
    allowed = tuned.get("allowedSegments", {})
    for segment, cfg in allowed.items():
        if not isinstance(cfg, dict):
            continue
        current = int(cfg.get("maxSignals", 3))
        ratio = float(segment_rows.get(segment, {}).get("placement_ratio", overall_place_ratio))
        new_val = current
        if ratio < 0.25:
            new_val = max(1, current - 1)
        elif ratio > 0.65:
            new_val = min(10, current + 1)
        if new_val != current:
            cfg["maxSignals"] = new_val
            changes.append(f"{segment}.maxSignals {current} -> {new_val} (placement ratio {ratio:.2f}).")

    fill_rate = float(exec_review.get("fill_rate", 0.0))
    cancel_after = int(tuned.get("cancelOpenOrdersAfterMinutes", 20))
    if fill_rate < 0.35:
        new_cancel = max(8, cancel_after - 2)
        if new_cancel != cancel_after:
            tuned["cancelOpenOrdersAfterMinutes"] = new_cancel
            changes.append(f"Shorten cancelOpenOrdersAfterMinutes {cancel_after} -> {new_cancel} (low fill rate).")

    return tuned, changes


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    cycle_review = payload["cycle_review"]
    exec_review = payload["execution_quality"]
    tuning = payload["tuning"]
    lines: list[str] = []
    lines.append(f"# Weekly Review ({payload['window']['since']} to {payload['window']['until']})")
    lines.append("")
    lines.append("## Topline")
    lines.append(f"- Cycles reviewed: {cycle_review['cycle_count']}")
    lines.append(f"- Orders planned/placed: {cycle_review['orders_planned_total']} / {cycle_review['orders_placed_total']} ({cycle_review['placement_ratio']:.2%})")
    lines.append(f"- Fill rate: {exec_review['fill_rate']:.2%}")
    lines.append(f"- Missed fill rate: {exec_review['missed_fill_rate']:.2%}")
    lines.append(f"- Avg buy slippage (favorable +): {exec_review['avg_buy_slippage_bps']} bps")
    lines.append(f"- Avg sell slippage (favorable +): {exec_review['avg_sell_slippage_bps']} bps")
    lines.append("")
    lines.append("## Strategy Attribution")
    for row in cycle_review["strategy_attribution"]:
        lines.append(f"- {row['strategy']}: cycles={row['cycles']}, orders_placed={row['orders_placed']}, placed/cycle={row['placed_per_cycle']}")
    lines.append("")
    lines.append("## Segment Attribution")
    for row in cycle_review["segment_attribution"]:
        lines.append(
            f"- {row['segment']}: cycles={row['cycles']}, planned={row['orders_planned']}, placed={row['orders_placed']}, placement_ratio={row['placement_ratio']:.2%}"
        )
    lines.append("")
    lines.append("## Bounded Tuning Suggestions")
    if tuning["suggested_changes"]:
        for c in tuning["suggested_changes"]:
            lines.append(f"- {c}")
    else:
        lines.append("- No bounded tuning changes suggested this cycle.")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=max(1, args.days))
    since_iso = since.isoformat()

    journal = TradeJournal()
    cfg = build_runtime_config()
    policy = ensure_risk_policy()
    client = AlpacaClient(cfg=cfg)

    cycle_rows = _load_cycle_rows(journal.db_path, since_iso)
    cycle_review = _review_cycles(cycle_rows)
    execution_quality = _review_execution_quality(client, since_iso)
    tuned_policy, suggested_changes = _bounded_tuning(policy, cycle_review, execution_quality)

    payload = {
        "window": {"since": since.isoformat(), "until": now.isoformat(), "days": args.days},
        "cycle_review": cycle_review,
        "execution_quality": execution_quality,
        "tuning": {
            "suggested_changes": suggested_changes,
            "applied": False,
        },
    }

    if args.apply and suggested_changes:
        config_path = ROOT / "config" / "risk_policy.json"
        config_path.write_text(json.dumps(tuned_policy, indent=2) + "\n", encoding="utf-8")
        payload["tuning"]["applied"] = True

    stamp = now.strftime("%Y%m%d-%H%M%S")
    report_dir = ROOT / "logs" / "reviews"
    md_path = report_dir / f"weekly-review-{stamp}.md"
    json_path = report_dir / f"weekly-review-{stamp}.json"
    _write_report(md_path, payload)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({"report_markdown": str(md_path), "report_json": str(json_path), "applied": payload["tuning"]["applied"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
