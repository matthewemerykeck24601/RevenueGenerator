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


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _realized_edge_from_order(order: dict[str, Any]) -> float | None:
    limit_v = _to_float(order.get("limit_price"), 0.0)
    fill_v = _to_float(order.get("filled_avg_price"), 0.0)
    side = str(order.get("side", "")).lower()
    if limit_v <= 0 or fill_v <= 0:
        return None
    if side == "sell":
        return (fill_v - limit_v) / limit_v
    return (limit_v - fill_v) / limit_v


def _build_order_lookup(client: AlpacaClient, since_iso: str) -> dict[str, dict[str, Any]]:
    # Pull a larger window than default so edge calibration is less likely to truncate in active sessions.
    orders = client.get_orders(status="all", limit=5000, direction="desc", after=since_iso)
    out: dict[str, dict[str, Any]] = {}
    for order in orders:
        order_id = str(order.get("id") or "")
        if order_id:
            out[order_id] = order
    return out


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


def _review_trade_edge_efficiency(rows: list[sqlite3.Row], orders_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    gross_sum = 0.0
    gross_n = 0
    cost_sum = 0.0
    cost_n = 0
    net_sum = 0.0
    net_n = 0
    realized_sum = 0.0
    realized_n = 0
    capture_num = 0.0
    capture_den = 0.0
    by_segment: dict[str, dict[str, float]] = {}

    for row in rows:
        if not bool(row["execute"]):
            continue
        segment = str(row["segment"] or "unknown")
        segment_stats = by_segment.setdefault(
            segment,
            {
                "predicted_gross_sum": 0.0,
                "predicted_gross_n": 0.0,
                "estimated_cost_sum": 0.0,
                "estimated_cost_n": 0.0,
                "predicted_net_sum": 0.0,
                "predicted_net_n": 0.0,
                "realized_sum": 0.0,
                "realized_n": 0.0,
                "capture_num": 0.0,
                "capture_den": 0.0,
            },
        )

        try:
            planned = json.loads(row["orders_planned_json"] or "[]")
        except json.JSONDecodeError:
            planned = []
        try:
            placed = json.loads(row["orders_placed_json"] or "[]")
        except json.JSONDecodeError:
            placed = []

        placed_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for p in placed:
            if not isinstance(p, dict):
                continue
            sym = str(p.get("symbol") or "")
            if not sym:
                continue
            placed_by_symbol.setdefault(sym, []).append(p)

        for p in planned:
            if not isinstance(p, dict):
                continue
            symbol = str(p.get("symbol") or "")
            predicted_gross = _to_float(p.get("expected_edge"), default=0.0)
            estimated_cost = _to_float(p.get("estimated_cost_pct"), default=0.0)
            predicted_net = _to_float(p.get("expected_edge_net"), default=predicted_gross - estimated_cost)

            gross_sum += predicted_gross
            gross_n += 1
            cost_sum += estimated_cost
            cost_n += 1
            net_sum += predicted_net
            net_n += 1
            segment_stats["predicted_gross_sum"] += predicted_gross
            segment_stats["predicted_gross_n"] += 1
            segment_stats["estimated_cost_sum"] += estimated_cost
            segment_stats["estimated_cost_n"] += 1
            segment_stats["predicted_net_sum"] += predicted_net
            segment_stats["predicted_net_n"] += 1

            placed_match = None
            if symbol and symbol in placed_by_symbol and placed_by_symbol[symbol]:
                placed_match = placed_by_symbol[symbol].pop(0)
            if not isinstance(placed_match, dict):
                continue

            order_id = str(placed_match.get("id") or "")
            enriched = orders_by_id.get(order_id, placed_match) if order_id else placed_match
            realized_f = _realized_edge_from_order(enriched)
            if realized_f is None:
                continue
            realized_sum += realized_f
            realized_n += 1
            segment_stats["realized_sum"] += realized_f
            segment_stats["realized_n"] += 1
            if predicted_net > 0:
                capture_num += realized_f
                capture_den += predicted_net
                segment_stats["capture_num"] += realized_f
                segment_stats["capture_den"] += predicted_net

    by_segment_out: list[dict[str, Any]] = []
    for segment, stats in sorted(by_segment.items()):
        by_segment_out.append(
            {
                "segment": segment,
                "predicted_gross_avg": _safe_div(stats["predicted_gross_sum"], stats["predicted_gross_n"]),
                "estimated_cost_avg": _safe_div(stats["estimated_cost_sum"], stats["estimated_cost_n"]),
                "predicted_net_avg": _safe_div(stats["predicted_net_sum"], stats["predicted_net_n"]),
                "realized_avg": _safe_div(stats["realized_sum"], stats["realized_n"]),
                "realized_samples": int(stats["realized_n"]),
                "net_edge_capture_ratio": _safe_div(stats["capture_num"], stats["capture_den"]),
            }
        )

    return {
        "predicted_gross_avg": _safe_div(gross_sum, gross_n),
        "estimated_cost_avg": _safe_div(cost_sum, cost_n),
        "predicted_net_avg": _safe_div(net_sum, net_n),
        "realized_avg": _safe_div(realized_sum, realized_n),
        "realized_samples": realized_n,
        "net_edge_capture_ratio": _safe_div(capture_num, capture_den),
        "by_segment": by_segment_out,
    }


def _review_cycles(rows: list[sqlite3.Row], orders_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    strategy_counts: dict[str, int] = defaultdict(int)
    strategy_placed: dict[str, int] = defaultdict(int)
    segment_counts: dict[str, int] = defaultdict(int)
    segment_planned: dict[str, int] = defaultdict(int)
    segment_placed: dict[str, int] = defaultdict(int)
    cycle_count_all = len(rows)
    cycle_count_execute = 0
    order_error_count = 0
    planned_total = 0
    placed_total = 0
    predicted_edge_sum = 0.0
    predicted_edge_count = 0
    realized_edge_sum = 0.0
    realized_edge_count = 0

    for row in rows:
        execute_flag = bool(row["execute"])
        if not execute_flag:
            continue

        cycle_count_execute += 1
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

        for p in planned:
            if not isinstance(p, dict):
                continue
            if "expected_edge" in p:
                predicted_edge_sum += _to_float(p.get("expected_edge"), 0.0)
                predicted_edge_count += 1
        for p in placed:
            if not isinstance(p, dict):
                continue
            enriched = p
            order_id = str(p.get("id") or "")
            if order_id and order_id in orders_by_id:
                enriched = orders_by_id[order_id]
            realized = _realized_edge_from_order(enriched)
            if realized is None:
                continue
            realized_edge_sum += realized
            realized_edge_count += 1

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
    predicted_edge_avg = _safe_div(predicted_edge_sum, predicted_edge_count)
    realized_edge_avg = _safe_div(realized_edge_sum, realized_edge_count)
    edge_error = realized_edge_avg - predicted_edge_avg
    return {
        "cycle_count": cycle_count_execute,
        "cycle_count_all": cycle_count_all,
        "orders_planned_total": planned_total,
        "orders_placed_total": placed_total,
        "placement_ratio": placement_ratio,
        "order_error_count": order_error_count,
        "predicted_edge_avg": predicted_edge_avg,
        "realized_edge_avg": realized_edge_avg,
        "edge_error": edge_error,
        "predicted_edge_samples": predicted_edge_count,
        "realized_edge_samples": realized_edge_count,
        "strategy_attribution": strategy_attr,
        "segment_attribution": segment_attr,
    }


def _run_replay_scenario(
    rows: list[sqlite3.Row],
    risk_policy: dict[str, Any],
    assumptions: dict[str, float],
    *,
    include_dry_run: bool,
) -> dict[str, Any]:
    by_segment: dict[str, dict[str, Any]] = {}
    equity = assumptions["starting_equity"]
    max_equity = equity
    max_drawdown = 0.0
    wins = 0
    losses = 0
    trades = 0
    net_pnl = 0.0
    notional_total = 0.0

    for row in rows:
        if not include_dry_run and not bool(row["execute"]):
            continue
        segment = str(row["segment"] or "unknown")
        if segment not in by_segment:
            by_segment[segment] = {"trades": 0, "net_pnl": 0.0, "notional": 0.0, "wins": 0}
        try:
            planned = json.loads(row["orders_planned_json"] or "[]")
        except json.JSONDecodeError:
            planned = []

        spread_bps = float(((risk_policy.get("allowedSegments") or {}).get(segment, {}) or {}).get("maxSpreadBps", 40.0))
        spread_cost = (spread_bps * assumptions["spread_haircut_share"]) / 10000.0
        for p in planned:
            if not isinstance(p, dict):
                continue
            alloc = _to_float(p.get("allocation"), 0.0)
            if alloc <= 0:
                continue
            symbol = str(p.get("symbol", ""))
            expected_edge = _to_float(p.get("expected_edge"), 0.0)
            if "/" in symbol:
                slip = (assumptions["entry_slippage_bps_crypto"] + assumptions["exit_slippage_bps_crypto"]) / 10000.0
            else:
                slip = (assumptions["entry_slippage_bps_stocks"] + assumptions["exit_slippage_bps_stocks"]) / 10000.0
            fee = (2.0 * assumptions["fee_bps_per_side"]) / 10000.0
            net_edge = expected_edge - spread_cost - slip - fee
            net_edge = min(assumptions["edge_clip_max"], max(assumptions["edge_clip_min"], net_edge))
            trade_pnl = alloc * net_edge

            trades += 1
            notional_total += alloc
            net_pnl += trade_pnl
            by_segment[segment]["trades"] += 1
            by_segment[segment]["net_pnl"] += trade_pnl
            by_segment[segment]["notional"] += alloc
            if trade_pnl >= 0:
                wins += 1
                by_segment[segment]["wins"] += 1
            else:
                losses += 1

            equity += trade_pnl
            max_equity = max(max_equity, equity)
            max_drawdown = max(max_drawdown, _safe_div(max_equity - equity, max_equity))

    for segment, stats in by_segment.items():
        s_trades = float(stats["trades"])
        stats["win_rate"] = _safe_div(float(stats["wins"]), s_trades)
        stats["avg_net_edge"] = _safe_div(float(stats["net_pnl"]), float(stats["notional"]))
        stats["net_pnl"] = round(float(stats["net_pnl"]), 2)
        stats["notional"] = round(float(stats["notional"]), 2)
        stats.pop("wins", None)

    return {
        "summary": {
            "trades_replayed": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": _safe_div(float(wins), float(trades)),
            "starting_equity": assumptions["starting_equity"],
            "ending_equity": equity,
            "net_return_pct": _safe_div(equity - assumptions["starting_equity"], assumptions["starting_equity"]) * 100.0,
            "net_pnl": net_pnl,
            "total_notional": notional_total,
            "max_drawdown_pct": max_drawdown * 100.0,
        },
        "by_segment": by_segment,
    }


def _replay_backtest(rows: list[sqlite3.Row], risk_policy: dict[str, Any], *, include_dry_run: bool) -> dict[str, Any]:
    scenarios: dict[str, dict[str, float]] = {
        "base": {
            "starting_equity": 10000.0,
            "fee_bps_per_side": 0.0,
            "entry_slippage_bps_stocks": 6.0,
            "exit_slippage_bps_stocks": 6.0,
            "entry_slippage_bps_crypto": 10.0,
            "exit_slippage_bps_crypto": 10.0,
            "spread_haircut_share": 0.6,
            "edge_clip_min": -0.2,
            "edge_clip_max": 0.3,
        },
        "conservative": {
            "starting_equity": 10000.0,
            "fee_bps_per_side": 2.0,
            "entry_slippage_bps_stocks": 10.0,
            "exit_slippage_bps_stocks": 10.0,
            "entry_slippage_bps_crypto": 15.0,
            "exit_slippage_bps_crypto": 15.0,
            "spread_haircut_share": 0.8,
            "edge_clip_min": -0.2,
            "edge_clip_max": 0.3,
        },
        "stress": {
            "starting_equity": 10000.0,
            "fee_bps_per_side": 4.0,
            "entry_slippage_bps_stocks": 15.0,
            "exit_slippage_bps_stocks": 15.0,
            "entry_slippage_bps_crypto": 24.0,
            "exit_slippage_bps_crypto": 24.0,
            "spread_haircut_share": 1.0,
            "edge_clip_min": -0.25,
            "edge_clip_max": 0.25,
        },
    }
    results: dict[str, Any] = {}
    for name, assumptions in scenarios.items():
        scenario_result = _run_replay_scenario(
            rows=rows,
            risk_policy=risk_policy,
            assumptions=assumptions,
            include_dry_run=include_dry_run,
        )
        results[name] = {
            "assumptions": assumptions,
            "summary": scenario_result["summary"],
            "by_segment": scenario_result["by_segment"],
        }

    base_return = float(results["base"]["summary"]["net_return_pct"])
    conservative_return = float(results["conservative"]["summary"]["net_return_pct"])
    stress_return = float(results["stress"]["summary"]["net_return_pct"])
    return_band_low = min(base_return, conservative_return, stress_return)
    return_band_high = max(base_return, conservative_return, stress_return)

    return {
        "include_dry_run": include_dry_run,
        "scenarios": results,
        "band": {
            "return_low_pct": return_band_low,
            "return_high_pct": return_band_high,
            "base_return_pct": base_return,
            "conservative_return_pct": conservative_return,
            "stress_return_pct": stress_return,
        },
    }


def _review_execution_quality(client: AlpacaClient, since_iso: str) -> dict[str, Any]:
    orders = client.get_orders(status="all", limit=5000, direction="desc", after=since_iso)
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
    replay = payload["replay_backtest"]["executed_only"]
    replay_planned = payload["replay_backtest"]["planned_inclusive"]
    edge_eff = payload["edge_efficiency"]
    tuning = payload["tuning"]
    lines: list[str] = []
    lines.append(f"# Weekly Review ({payload['window']['since']} to {payload['window']['until']})")
    lines.append("")
    lines.append("## Topline")
    lines.append(f"- Cycles reviewed (executed / all): {cycle_review['cycle_count']} / {cycle_review['cycle_count_all']}")
    lines.append(f"- Orders planned/placed: {cycle_review['orders_planned_total']} / {cycle_review['orders_placed_total']} ({cycle_review['placement_ratio']:.2%})")
    lines.append(f"- Fill rate: {exec_review['fill_rate']:.2%}")
    lines.append(f"- Missed fill rate: {exec_review['missed_fill_rate']:.2%}")
    lines.append(f"- Avg buy slippage (favorable +): {exec_review['avg_buy_slippage_bps']} bps")
    lines.append(f"- Avg sell slippage (favorable +): {exec_review['avg_sell_slippage_bps']} bps")
    lines.append(
        f"- Predicted vs realized edge: {cycle_review['predicted_edge_avg']:.4f} vs {cycle_review['realized_edge_avg']:.4f} "
        f"(error {cycle_review['edge_error']:+.4f})"
    )
    lines.append("")
    lines.append("## Strategy Attribution")
    lines.append("- Attribution below uses executed cycles only.")
    for row in cycle_review["strategy_attribution"]:
        lines.append(f"- {row['strategy']}: cycles={row['cycles']}, orders_placed={row['orders_placed']}, placed/cycle={row['placed_per_cycle']}")
    lines.append("")
    lines.append("## Segment Attribution")
    lines.append("- Attribution below uses executed cycles only.")
    for row in cycle_review["segment_attribution"]:
        lines.append(
            f"- {row['segment']}: cycles={row['cycles']}, planned={row['orders_planned']}, placed={row['orders_placed']}, placement_ratio={row['placement_ratio']:.2%}"
        )
    lines.append("")
    lines.append("## Replay Backtest (Bounded Return Band)")
    replay_band = replay["band"]
    lines.append(
        f"- Return band (stress -> base): {replay_band['return_low_pct']:.2f}% to {replay_band['return_high_pct']:.2f}%"
    )
    lines.append(
        f"- Scenario returns: base={replay_band['base_return_pct']:.2f}%, "
        f"conservative={replay_band['conservative_return_pct']:.2f}%, stress={replay_band['stress_return_pct']:.2f}%"
    )
    lines.append("")
    lines.append("## Replay Mode Comparison")
    lines.append(
        f"- Executed-only base return: {replay['band']['base_return_pct']:.2f}% "
        f"(band {replay['band']['return_low_pct']:.2f}% -> {replay['band']['return_high_pct']:.2f}%)"
    )
    lines.append(
        f"- Planned-inclusive base return: {replay_planned['band']['base_return_pct']:.2f}% "
        f"(band {replay_planned['band']['return_low_pct']:.2f}% -> {replay_planned['band']['return_high_pct']:.2f}%)"
    )
    lines.append(
        f"- Base return drift (planned - executed): "
        f"{(replay_planned['band']['base_return_pct'] - replay['band']['base_return_pct']):+.2f}%"
    )
    lines.append("")
    lines.append("## Replay Scenario Detail")
    for scenario_name in ["base", "conservative", "stress"]:
        scenario = replay["scenarios"][scenario_name]
        replay_summary = scenario["summary"]
        lines.append(f"- [{scenario_name}] assumptions: {json.dumps(scenario['assumptions'], separators=(',', ':'))}")
        lines.append(
            f"  trades={replay_summary['trades_replayed']}, win_rate={replay_summary['win_rate']:.2%}, "
            f"net_return={replay_summary['net_return_pct']:.2f}%, max_drawdown={replay_summary['max_drawdown_pct']:.2f}%"
        )
        lines.append(
            f"  start/end=${replay_summary['starting_equity']:.2f}->${replay_summary['ending_equity']:.2f}, "
            f"net_pnl=${replay_summary['net_pnl']:.2f}"
        )
    lines.append("")
    lines.append("## Replay Segment Breakdown (Base)")
    replay_summary = replay["scenarios"]["base"]["summary"]
    lines.append(
        f"- Trades replayed: {replay_summary['trades_replayed']}, win_rate={replay_summary['win_rate']:.2%}, "
        f"net_return={replay_summary['net_return_pct']:.2f}%, max_drawdown={replay_summary['max_drawdown_pct']:.2f}%"
    )
    lines.append(
        f"- Start/End equity: ${replay_summary['starting_equity']:.2f} -> ${replay_summary['ending_equity']:.2f}; "
        f"net_pnl=${replay_summary['net_pnl']:.2f}"
    )
    for segment, stats in replay["scenarios"]["base"]["by_segment"].items():
        lines.append(
            f"- {segment}: trades={stats['trades']}, win_rate={stats['win_rate']:.2%}, "
            f"net_pnl=${stats['net_pnl']:.2f}, avg_net_edge={stats['avg_net_edge']:.4f}"
        )
    lines.append("")
    lines.append("## Edge Calibration")
    if cycle_review["predicted_edge_samples"] == 0:
        lines.append("- No expected-edge samples were recorded in this window.")
    elif cycle_review["realized_edge_samples"] == 0:
        lines.append("- Expected-edge samples exist, but no realized fill-edge samples were available yet.")
    elif cycle_review["edge_error"] < -0.01:
        lines.append("- Model/rules are likely overestimating edge. Consider raising minExpectedEdge and reducing maxSignals.")
    elif cycle_review["edge_error"] > 0.01:
        lines.append("- Realized edge is beating predictions. You may be able to carefully loosen confidence/edge gates.")
    else:
        lines.append("- Predicted and realized edges are fairly aligned.")
    lines.append("")
    lines.append("## Net Edge Efficiency")
    lines.append(
        f"- Gross edge avg={edge_eff['predicted_gross_avg']:.4f}, est cost avg={edge_eff['estimated_cost_avg']:.4f}, "
        f"predicted net avg={edge_eff['predicted_net_avg']:.4f}"
    )
    lines.append(
        f"- Realized edge avg={edge_eff['realized_avg']:.4f} across {edge_eff['realized_samples']} samples; "
        f"net-edge capture ratio={edge_eff['net_edge_capture_ratio']:.2f}x"
    )
    for row in edge_eff["by_segment"]:
        lines.append(
            f"- {row['segment']}: gross={row['predicted_gross_avg']:.4f}, cost={row['estimated_cost_avg']:.4f}, "
            f"net={row['predicted_net_avg']:.4f}, realized={row['realized_avg']:.4f}, "
            f"capture={row['net_edge_capture_ratio']:.2f}x, samples={row['realized_samples']}"
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
    orders_by_id = _build_order_lookup(client, since_iso)
    cycle_review = _review_cycles(cycle_rows, orders_by_id)
    execution_quality = _review_execution_quality(client, since_iso)
    edge_efficiency = _review_trade_edge_efficiency(cycle_rows, orders_by_id)
    replay_backtest = {
        "executed_only": _replay_backtest(cycle_rows, policy, include_dry_run=False),
        "planned_inclusive": _replay_backtest(cycle_rows, policy, include_dry_run=True),
    }
    tuned_policy, suggested_changes = _bounded_tuning(policy, cycle_review, execution_quality)

    payload = {
        "window": {"since": since.isoformat(), "until": now.isoformat(), "days": args.days},
        "cycle_review": cycle_review,
        "execution_quality": execution_quality,
        "edge_efficiency": edge_efficiency,
        "replay_backtest": replay_backtest,
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
