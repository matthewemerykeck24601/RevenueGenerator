from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.config import ensure_risk_policy
from revenue_generator.journal import TradeJournal


@dataclass
class ReplayAssumptions:
    starting_equity: float
    fee_bps_per_side: float
    entry_slippage_bps_stocks: float
    exit_slippage_bps_stocks: float
    entry_slippage_bps_crypto: float
    exit_slippage_bps_crypto: float
    spread_haircut_share: float
    edge_clip_min: float
    edge_clip_max: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay logged planned orders with configurable execution-cost assumptions.")
    p.add_argument("--days", type=int, default=30, help="Lookback window in days.")
    p.add_argument("--starting-equity", type=float, default=10000.0, help="Starting paper equity for replay.")
    p.add_argument("--fee-bps-per-side", type=float, default=0.0, help="Fee in bps applied on entry and exit.")
    p.add_argument("--entry-slippage-bps-stocks", type=float, default=5.0, help="Entry slippage bps for stocks/ETFs.")
    p.add_argument("--exit-slippage-bps-stocks", type=float, default=5.0, help="Exit slippage bps for stocks/ETFs.")
    p.add_argument("--entry-slippage-bps-crypto", type=float, default=8.0, help="Entry slippage bps for crypto.")
    p.add_argument("--exit-slippage-bps-crypto", type=float, default=8.0, help="Exit slippage bps for crypto.")
    p.add_argument(
        "--spread-haircut-share",
        type=float,
        default=0.5,
        help="Fraction of configured max spread bps charged on each round trip (0-1).",
    )
    p.add_argument(
        "--edge-clip-min",
        type=float,
        default=-0.2,
        help="Minimum per-trade net edge (decimal) after costs.",
    )
    p.add_argument(
        "--edge-clip-max",
        type=float,
        default=0.3,
        help="Maximum per-trade net edge (decimal) after costs.",
    )
    p.add_argument(
        "--include-dry-run",
        action="store_true",
        help="Include non-executed cycles in replay (default: executed cycles only).",
    )
    return p.parse_args()


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def _is_crypto_symbol(symbol: str) -> bool:
    return "/" in symbol


def _load_cycle_rows(db_path: Path, since_iso: str) -> list[sqlite3.Row]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT ts, segment, execute, orders_planned_json
            FROM cycles
            WHERE ts >= ?
            ORDER BY ts ASC
            """,
            (since_iso,),
        ).fetchall()
        return rows
    finally:
        con.close()


def _segment_spread_bps(policy: dict[str, Any], segment: str) -> float:
    return float(((policy.get("allowedSegments") or {}).get(segment, {}) or {}).get("maxSpreadBps", 40.0))


def _run_replay(
    rows: list[sqlite3.Row],
    policy: dict[str, Any],
    assumptions: ReplayAssumptions,
    *,
    include_dry_run: bool,
) -> dict[str, Any]:
    equity = assumptions.starting_equity
    max_equity = equity
    max_drawdown = 0.0
    trade_count = 0
    wins = 0
    losses = 0
    gross_pnl = 0.0
    net_pnl = 0.0
    total_notional = 0.0

    by_segment: dict[str, dict[str, Any]] = {}
    equity_curve: list[dict[str, Any]] = []
    replayed_cycle_count = 0

    for row in rows:
        if not include_dry_run and not bool(row["execute"]):
            continue
        replayed_cycle_count += 1
        segment = str(row["segment"] or "unknown")
        if segment not in by_segment:
            by_segment[segment] = {
                "trades": 0,
                "gross_pnl": 0.0,
                "net_pnl": 0.0,
                "notional": 0.0,
                "win_rate": 0.0,
                "avg_net_edge": 0.0,
                "wins": 0,
            }
        try:
            planned = json.loads(row["orders_planned_json"] or "[]")
        except json.JSONDecodeError:
            planned = []

        for order in planned:
            if not isinstance(order, dict):
                continue
            symbol = str(order.get("symbol") or "")
            alloc = _to_float(order.get("allocation"), 0.0)
            if alloc <= 0:
                continue
            expected_edge = _to_float(order.get("expected_edge"), 0.0)
            spread_bps = _segment_spread_bps(policy, segment)
            spread_cost = (spread_bps * max(0.0, min(1.0, assumptions.spread_haircut_share))) / 10000.0

            if _is_crypto_symbol(symbol):
                slip_cost = (assumptions.entry_slippage_bps_crypto + assumptions.exit_slippage_bps_crypto) / 10000.0
            else:
                slip_cost = (assumptions.entry_slippage_bps_stocks + assumptions.exit_slippage_bps_stocks) / 10000.0
            fee_cost = (2.0 * assumptions.fee_bps_per_side) / 10000.0
            total_cost = spread_cost + slip_cost + fee_cost

            gross_edge = expected_edge
            net_edge = gross_edge - total_cost
            net_edge = max(assumptions.edge_clip_min, min(assumptions.edge_clip_max, net_edge))

            trade_gross = alloc * gross_edge
            trade_net = alloc * net_edge

            trade_count += 1
            total_notional += alloc
            gross_pnl += trade_gross
            net_pnl += trade_net
            by_segment[segment]["trades"] += 1
            by_segment[segment]["gross_pnl"] += trade_gross
            by_segment[segment]["net_pnl"] += trade_net
            by_segment[segment]["notional"] += alloc
            if trade_net >= 0:
                wins += 1
                by_segment[segment]["wins"] += 1
            else:
                losses += 1

            equity += trade_net
            max_equity = max(max_equity, equity)
            drawdown = _safe_div(max_equity - equity, max_equity)
            max_drawdown = max(max_drawdown, drawdown)

        equity_curve.append({"ts": row["ts"], "equity": round(equity, 2)})

    for segment, stats in by_segment.items():
        trades = int(stats["trades"])
        stats["win_rate"] = round(_safe_div(float(stats["wins"]), float(trades)), 4)
        stats["avg_net_edge"] = round(_safe_div(float(stats["net_pnl"]), float(stats["notional"])), 6)
        stats["gross_pnl"] = round(float(stats["gross_pnl"]), 2)
        stats["net_pnl"] = round(float(stats["net_pnl"]), 2)
        stats["notional"] = round(float(stats["notional"]), 2)
        stats.pop("wins", None)
        by_segment[segment] = stats

    return {
        "summary": {
            "cycles_replayed": replayed_cycle_count,
            "trades_replayed": trade_count,
            "wins": wins,
            "losses": losses,
            "win_rate": round(_safe_div(float(wins), float(trade_count)), 4),
            "starting_equity": round(assumptions.starting_equity, 2),
            "ending_equity": round(equity, 2),
            "net_return_pct": round(_safe_div(equity - assumptions.starting_equity, assumptions.starting_equity) * 100.0, 3),
            "gross_pnl": round(gross_pnl, 2),
            "net_pnl": round(net_pnl, 2),
            "total_notional": round(total_notional, 2),
            "max_drawdown_pct": round(max_drawdown * 100.0, 3),
        },
        "assumptions": assumptions.__dict__,
        "by_segment": by_segment,
        "equity_curve": equity_curve,
    }


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    lines: list[str] = []
    lines.append("# Replay Backtest Report")
    lines.append("")
    lines.append("## Summary")
    lines.append(
        f"- Input mode: {'executed + dry-run cycles' if payload.get('include_dry_run') else 'executed cycles only'}"
    )
    lines.append(f"- Cycles replayed: {summary['cycles_replayed']}")
    lines.append(f"- Trades replayed: {summary['trades_replayed']}")
    lines.append(f"- Win rate: {summary['win_rate']:.2%}")
    lines.append(f"- Start/End equity: ${summary['starting_equity']:.2f} -> ${summary['ending_equity']:.2f}")
    lines.append(f"- Net return: {summary['net_return_pct']:.2f}%")
    lines.append(f"- Max drawdown: {summary['max_drawdown_pct']:.2f}%")
    lines.append("")
    lines.append("## Segment Breakdown")
    for segment, stats in payload["by_segment"].items():
        lines.append(
            f"- {segment}: trades={stats['trades']}, win_rate={stats['win_rate']:.2%}, "
            f"net_pnl=${stats['net_pnl']:.2f}, avg_net_edge={stats['avg_net_edge']:.4f}"
        )
    lines.append("")
    lines.append("## Assumptions")
    for k, v in payload["assumptions"].items():
        lines.append(f"- {k}: {v}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=max(1, args.days))
    since_iso = since.isoformat()

    journal = TradeJournal()
    policy = ensure_risk_policy()
    rows = _load_cycle_rows(journal.db_path, since_iso)

    assumptions = ReplayAssumptions(
        starting_equity=args.starting_equity,
        fee_bps_per_side=args.fee_bps_per_side,
        entry_slippage_bps_stocks=args.entry_slippage_bps_stocks,
        exit_slippage_bps_stocks=args.exit_slippage_bps_stocks,
        entry_slippage_bps_crypto=args.entry_slippage_bps_crypto,
        exit_slippage_bps_crypto=args.exit_slippage_bps_crypto,
        spread_haircut_share=args.spread_haircut_share,
        edge_clip_min=args.edge_clip_min,
        edge_clip_max=args.edge_clip_max,
    )
    payload = _run_replay(rows=rows, policy=policy, assumptions=assumptions, include_dry_run=args.include_dry_run)
    payload["window"] = {"since": since.isoformat(), "until": now.isoformat(), "days": args.days}
    payload["include_dry_run"] = bool(args.include_dry_run)

    stamp = now.strftime("%Y%m%d-%H%M%S")
    out_dir = ROOT / "logs" / "reviews"
    md_path = out_dir / f"replay-backtest-{stamp}.md"
    json_path = out_dir / f"replay-backtest-{stamp}.json"
    _write_report(md_path, payload)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "report_markdown": str(md_path),
                "report_json": str(json_path),
                "trades_replayed": payload["summary"]["trades_replayed"],
                "net_return_pct": payload["summary"]["net_return_pct"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
