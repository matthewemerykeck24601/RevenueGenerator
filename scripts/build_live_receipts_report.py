from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.config import build_runtime_config


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _fmt_money(v: float) -> str:
    return f"${v:,.2f}"


def _fmt_pct(v: float) -> str:
    return f"{v:.2f}%"


@dataclass
class CycleRow:
    ts: str
    segment: str
    execute: int
    signals_considered: int
    orders_planned_json: str
    orders_placed_json: str
    notes: str | None
    predicted_edge_avg: float | None


def _load_cycles(hours: int = 24) -> list[CycleRow]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    con = sqlite3.connect(str(ROOT / "logs" / "trades.db"))
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT ts, segment, execute, signals_considered, orders_planned_json, orders_placed_json, notes, predicted_edge_avg
        FROM cycles
        ORDER BY ts DESC
        """
    ).fetchall()
    con.close()

    out: list[CycleRow] = []
    for r in rows:
        ts = str(r[0] or "")
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt < cutoff:
            continue
        out.append(
            CycleRow(
                ts=ts,
                segment=str(r[1] or ""),
                execute=int(r[2] or 0),
                signals_considered=int(r[3] or 0),
                orders_planned_json=str(r[4] or "[]"),
                orders_placed_json=str(r[5] or "[]"),
                notes=str(r[6] or ""),
                predicted_edge_avg=_to_float(r[7], default=0.0) if r[7] is not None else None,
            )
        )
    return out


def _load_recent_exit_actions(hours: int = 24) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    con = sqlite3.connect(str(ROOT / "logs" / "trades.db"))
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT ts, symbol, trigger, qty, price, pnl_pct, execute
        FROM exit_actions
        ORDER BY ts DESC
        """
    ).fetchall()
    con.close()
    out: list[dict[str, Any]] = []
    for r in rows:
        ts = str(r[0] or "")
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt < cutoff:
            continue
        out.append(
            {
                "ts": ts,
                "symbol": str(r[1] or ""),
                "trigger": str(r[2] or ""),
                "qty": _to_float(r[3]),
                "price": _to_float(r[4]),
                "pnl_pct": _to_float(r[5]),
                "execute": int(r[6] or 0),
            }
        )
    return out


def _build_report() -> str:
    cfg = build_runtime_config()
    client = AlpacaClient(cfg=cfg)
    account = client.get_account()
    orders = client.get_orders(status="all", limit=300, direction="desc")

    now_utc = datetime.now(timezone.utc)
    cutoff_24h = now_utc - timedelta(hours=24)

    recent_filled: list[dict[str, Any]] = []
    for o in orders:
        status = str(o.get("status") or "")
        if status not in {"filled", "partially_filled"}:
            continue
        ts = str(o.get("filled_at") or o.get("submitted_at") or "")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt < cutoff_24h:
            continue
        recent_filled.append(
            {
                "ts": ts,
                "symbol": str(o.get("symbol") or ""),
                "side": str(o.get("side") or ""),
                "qty": _to_float(o.get("filled_qty")),
                "price": _to_float(o.get("filled_avg_price")),
                "status": status,
            }
        )
    recent_filled.sort(key=lambda r: r["ts"], reverse=True)

    cycles = _load_cycles(hours=24)
    exec_cycles = [c for c in cycles if c.execute == 1]
    placed_cycles = []
    for c in exec_cycles:
        try:
            placed = json.loads(c.orders_placed_json or "[]")
        except Exception:
            placed = []
        if isinstance(placed, list) and len(placed) > 0:
            placed_cycles.append((c, len(placed)))

    exits = _load_recent_exit_actions(hours=24)
    exits_exec = [e for e in exits if e["execute"] == 1]

    seg_counts: dict[str, int] = {}
    for c in exec_cycles:
        seg_counts[c.segment] = seg_counts.get(c.segment, 0) + 1

    lines: list[str] = []
    lines.append("# Live Trading Receipts Report")
    lines.append("")
    lines.append(f"_Generated (UTC): {now_utc.isoformat()}_")
    lines.append("")
    lines.append("## 1) Environment and Account Proof")
    lines.append("")
    lines.append(f"- Trading base URL: `{cfg.trading_base_url}`")
    lines.append(f"- Data base URL: `{cfg.data_base_url}`")
    lines.append(f"- Account status: `{account.get('status')}`")
    lines.append(f"- Equity: {_fmt_money(_to_float(account.get('equity')))}")
    lines.append(f"- Cash: {_fmt_money(_to_float(account.get('cash')))}")
    lines.append(f"- Buying Power: {_fmt_money(_to_float(account.get('buying_power')))}")
    lines.append(f"- Day-trade count: `{account.get('daytrade_count')}`")
    lines.append(f"- Pattern day trader: `{account.get('pattern_day_trader')}`")
    lines.append("")
    lines.append("## 2) Last 24h Execution Summary")
    lines.append("")
    lines.append(f"- Total cycle rows (24h): `{len(cycles)}`")
    lines.append(f"- Execute=true cycles (24h): `{len(exec_cycles)}`")
    lines.append(f"- Execute=true cycles with order placement: `{len(placed_cycles)}`")
    if seg_counts:
        lines.append(f"- Execute cycle counts by segment: `{seg_counts}`")
    lines.append(f"- Filled broker orders in last 24h: `{len(recent_filled)}`")
    lines.append(f"- Exit actions execute=true in last 24h: `{len(exits_exec)}`")
    lines.append("")
    lines.append("## 3) Recent Execute Cycles With Placements")
    lines.append("")
    if not placed_cycles:
        lines.append("- No execute cycle with placed orders in the lookback window.")
    else:
        lines.append("| ts (UTC) | segment | signals_considered | placed_count | predicted_edge_avg | notes |")
        lines.append("|---|---:|---:|---:|---:|---|")
        for c, placed_count in placed_cycles[:20]:
            edge = "" if c.predicted_edge_avg is None else f"{c.predicted_edge_avg:.6f}"
            note = (c.notes or "").replace("|", "/")
            lines.append(
                f"| {c.ts} | {c.segment} | {c.signals_considered} | {placed_count} | {edge} | {note} |"
            )
    lines.append("")
    lines.append("## 4) Recent Filled Orders (Broker)")
    lines.append("")
    if not recent_filled:
        lines.append("- No filled orders in the last 24h.")
    else:
        lines.append("| filled_at (UTC) | symbol | side | qty | avg_price | status |")
        lines.append("|---|---|---|---:|---:|---|")
        for o in recent_filled[:25]:
            lines.append(
                f"| {o['ts']} | {o['symbol']} | {o['side']} | {o['qty']:.6f} | {o['price']:.4f} | {o['status']} |"
            )
    lines.append("")
    lines.append("## 5) Recent Exit Actions (Local Log)")
    lines.append("")
    if not exits:
        lines.append("- No exit actions in the last 24h.")
    else:
        lines.append("| ts (UTC) | symbol | trigger | qty | px | pnl_pct | execute |")
        lines.append("|---|---|---|---:|---:|---:|---:|")
        for e in exits[:25]:
            lines.append(
                f"| {e['ts']} | {e['symbol']} | {e['trigger']} | {e['qty']:.6f} | {e['price']:.4f} | {e['pnl_pct']:.4f} | {e['execute']} |"
            )
    lines.append("")
    lines.append("## 6) Notes")
    lines.append("")
    lines.append("- This report reflects *actual broker/account data + local cycle/exit logs* at generation time.")
    lines.append("- Filled order counts are broker-side truth; cycle logs show strategy decisions and execution attempts.")
    lines.append("- Use this alongside `logs/ai_calls.jsonl` and weekly review outputs for model-quality attribution.")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    report = _build_report()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out = ROOT / "logs" / "reviews" / f"live-receipts-{ts}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report + "\n", encoding="utf-8")
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
