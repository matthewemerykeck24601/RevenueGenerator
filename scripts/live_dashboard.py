from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template_string, request

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.config import build_runtime_config, ensure_risk_policy
from revenue_generator.equity_mode import apply_equity_mode_switch
from revenue_generator.fear_climate import load_fear_climate_state, set_fear_climate_enabled
from revenue_generator.kraken_client import KrakenClient

app = Flask(__name__)
cfg = build_runtime_config()
client = AlpacaClient(cfg=cfg)
risk_policy = ensure_risk_policy()
kraken_client: KrakenClient | None = None
if str(risk_policy.get("cryptoBroker", "")).lower() == "kraken":
    try:
        kraken_client = KrakenClient()
    except Exception:
        pass
RESERVE_STATE_PATH = ROOT / "logs" / "reserve_state.json"
EXIT_MANAGER_HEARTBEAT_PATH = ROOT / "logs" / "exit_manager_heartbeat.json"
EXIT_MANAGER_STALE_SECONDS = 130
SCHEDULER_HEARTBEAT_PATH = ROOT / "logs" / "scheduler_heartbeat.json"
SCHEDULER_STALE_SECONDS = 150
TRADES_DB_PATH = ROOT / "logs" / "trades.db"
SEGMENT_OVERRIDES_PATH = ROOT / "config" / "segment_symbol_overrides.json"
_SEGMENT_HINT_CACHE: dict[str, Any] = {"ts": 0.0, "hints": {}}
_SEGMENT_HINT_TTL_SECONDS = 20.0
_SEGMENT_OVERRIDE_CACHE: dict[str, Any] = {"ts": 0.0, "symbols": {}}
_SEGMENT_OVERRIDE_TTL_SECONDS = 60.0


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


class ReserveStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "reserve_balance": 0.0,
                "reserve_target_request": 0.0,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return {
                "reserve_balance": max(_to_float(raw.get("reserve_balance")), 0.0),
                "reserve_target_request": max(_to_float(raw.get("reserve_target_request")), 0.0),
                "updated_at": str(raw.get("updated_at") or datetime.now(timezone.utc).isoformat()),
            }
        except Exception:
            return {
                "reserve_balance": 0.0,
                "reserve_target_request": 0.0,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

    def _save(self) -> None:
        self._state["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(self._state, indent=2) + "\n", encoding="utf-8")

    def _refresh(self) -> None:
        self._state = self._load()

    def set_target(self, target_amount: float) -> dict[str, Any]:
        self._refresh()
        # Treat incoming target as an additional reserve request (incremental),
        # not an absolute reserve total.
        current_pending = max(_to_float(self._state.get("reserve_target_request")), 0.0)
        self._state["reserve_target_request"] = current_pending + max(target_amount, 0.0)
        self._save()
        return dict(self._state)

    def recirculate(self) -> dict[str, Any]:
        self._refresh()
        # Release all reserved cash back to deployable capital.
        self._state["reserve_balance"] = 0.0
        self._state["reserve_target_request"] = 0.0
        self._save()
        return dict(self._state)

    def sync_with_cash(self, cash: float) -> dict[str, Any]:
        self._refresh()
        changed = False
        reserve_balance = max(_to_float(self._state.get("reserve_balance")), 0.0)
        reserve_target_request = max(_to_float(self._state.get("reserve_target_request")), 0.0)

        # Keep reserve bounded by real available cash in account.
        bounded_balance = min(reserve_balance, max(cash, 0.0))
        if bounded_balance != reserve_balance:
            reserve_balance = bounded_balance
            changed = True

        # Fill pending reserve request only from currently unreserved cash.
        if reserve_target_request > 0:
            free_cash = max(cash - reserve_balance, 0.0)
            fill = min(reserve_target_request, free_cash)
            if fill > 0:
                reserve_balance += fill
                reserve_target_request -= fill
                changed = True

        self._state["reserve_balance"] = round(reserve_balance, 6)
        self._state["reserve_target_request"] = round(reserve_target_request, 6)
        if changed:
            self._save()
        return dict(self._state)


reserve_store = ReserveStateStore(RESERVE_STATE_PATH)


def _kill_scheduler_processes() -> dict[str, Any]:
    # Stops any running multi-sector scheduler Python process.
    ps_script = r"""
$procs = Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" | Where-Object {
  $_.CommandLine -like '*run_multi_sector_scheduler.py*'
}
$killed = @()
foreach ($p in $procs) {
  try {
    Stop-Process -Id $p.ProcessId -Force -ErrorAction Stop
    $killed += $p.ProcessId
  } catch {}
}
$result = @{
  killed = $killed
  count = $killed.Count
}
$result | ConvertTo-Json -Compress
"""
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
        if proc.returncode != 0:
            return {"ok": False, "count": 0, "killed": [], "error": proc.stderr.strip() or "kill command failed"}
        data = json.loads((proc.stdout or "{}").strip() or "{}")
        killed = data.get("killed") or []
        if isinstance(killed, int):
            killed = [killed]
        return {"ok": True, "count": int(data.get("count", len(killed))), "killed": killed}
    except Exception as err:
        return {"ok": False, "count": 0, "killed": [], "error": str(err)}


def _load_exit_manager_health() -> dict[str, Any]:
    if not EXIT_MANAGER_HEARTBEAT_PATH.exists():
        return {
            "available": False,
            "status": "missing",
            "healthy": False,
            "stale": True,
            "reason": "Heartbeat file not found.",
        }
    try:
        raw = json.loads(EXIT_MANAGER_HEARTBEAT_PATH.read_text(encoding="utf-8"))
    except Exception as err:
        return {
            "available": False,
            "status": "unreadable",
            "healthy": False,
            "stale": True,
            "reason": f"Heartbeat read failed: {err}",
        }

    updated_at = _parse_iso(raw.get("updated_at"))
    age_seconds: int | None = None
    if updated_at is not None:
        age_seconds = max(int((datetime.now(timezone.utc) - updated_at).total_seconds()), 0)
    stale = age_seconds is None or age_seconds > EXIT_MANAGER_STALE_SECONDS
    status = str(raw.get("status") or "unknown")
    consecutive_errors = int(raw.get("consecutive_errors") or 0)
    healthy = (status in {"ok", "started", "recovered"}) and not stale and consecutive_errors == 0
    if stale and status == "ok":
        status = "stale"
    return {
        "available": True,
        "healthy": healthy,
        "stale": stale,
        "status": status,
        "updated_at": raw.get("updated_at"),
        "age_seconds": age_seconds,
        "pid": raw.get("pid"),
        "execute": bool(raw.get("execute", False)),
        "cycle_count": int(raw.get("cycle_count") or 0),
        "consecutive_errors": consecutive_errors,
        "suppressed": raw.get("suppressed") if isinstance(raw.get("suppressed"), dict) else {},
        "error": str(raw.get("error") or ""),
    }


def _load_scheduler_health() -> dict[str, Any]:
    if not SCHEDULER_HEARTBEAT_PATH.exists():
        return {
            "available": False,
            "status": "missing",
            "healthy": False,
            "stale": True,
            "reason": "Scheduler heartbeat file not found.",
        }
    try:
        raw = json.loads(SCHEDULER_HEARTBEAT_PATH.read_text(encoding="utf-8"))
    except Exception as err:
        return {
            "available": False,
            "status": "unreadable",
            "healthy": False,
            "stale": True,
            "reason": f"Scheduler heartbeat read failed: {err}",
        }
    updated_at = _parse_iso(raw.get("updated_at"))
    age_seconds: int | None = None
    if updated_at is not None:
        age_seconds = max(int((datetime.now(timezone.utc) - updated_at).total_seconds()), 0)
    stale_after_seconds = int(raw.get("stale_after_seconds") or SCHEDULER_STALE_SECONDS)
    stale = age_seconds is None or age_seconds > stale_after_seconds
    status = str(raw.get("status") or "unknown")
    consecutive_errors = int(raw.get("consecutive_errors") or 0)
    healthy = (status in {"ok", "started", "recovered"}) and not stale and consecutive_errors == 0
    if stale and status == "ok":
        status = "stale"
    return {
        "available": True,
        "healthy": healthy,
        "stale": stale,
        "status": status,
        "updated_at": raw.get("updated_at"),
        "age_seconds": age_seconds,
        "stale_after_seconds": stale_after_seconds,
        "pid": raw.get("pid"),
        "execute": bool(raw.get("execute", False)),
        "cycle_count": int(raw.get("cycle_count") or 0),
        "consecutive_errors": consecutive_errors,
        "schedule_mode": str(raw.get("schedule_mode") or ""),
        "ran_segments": raw.get("ran_segments") if isinstance(raw.get("ran_segments"), list) else [],
        "active_segments": raw.get("active_segments") if isinstance(raw.get("active_segments"), list) else [],
        "error": str(raw.get("error") or ""),
    }


def _load_symbol_segment_hints() -> dict[str, str]:
    now = time.time()
    cached_ts = float(_SEGMENT_HINT_CACHE.get("ts", 0.0) or 0.0)
    cached_hints = _SEGMENT_HINT_CACHE.get("hints")
    if isinstance(cached_hints, dict) and (now - cached_ts) < _SEGMENT_HINT_TTL_SECONDS:
        return dict(cached_hints)
    if not TRADES_DB_PATH.exists():
        _SEGMENT_HINT_CACHE["ts"] = now
        _SEGMENT_HINT_CACHE["hints"] = {}
        return {}

    hints: dict[str, str] = {}
    try:
        con = sqlite3.connect(TRADES_DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT ts, segment, orders_planned_json, orders_placed_json
            FROM cycles
            ORDER BY ts DESC
            LIMIT 1200
            """
        ).fetchall()
    except Exception:
        rows = []
    finally:
        try:
            con.close()  # type: ignore[name-defined]
        except Exception:
            pass

    for row in rows:
        segment = str(row["segment"] or "").strip()
        if not segment:
            continue
        for field in ("orders_placed_json", "orders_planned_json"):
            raw = row[field]
            try:
                orders = json.loads(raw or "[]")
            except Exception:
                orders = []
            if not isinstance(orders, list):
                continue
            for order in orders:
                if not isinstance(order, dict):
                    continue
                symbol = str(order.get("symbol") or "").strip()
                if not symbol:
                    continue
                # DESC query order means first write wins (most recent segment)
                if symbol not in hints:
                    hints[symbol] = segment

    _SEGMENT_HINT_CACHE["ts"] = now
    _SEGMENT_HINT_CACHE["hints"] = dict(hints)
    return hints


def _load_segment_overrides() -> dict[str, str]:
    now = time.time()
    cached_ts = float(_SEGMENT_OVERRIDE_CACHE.get("ts", 0.0) or 0.0)
    cached_symbols = _SEGMENT_OVERRIDE_CACHE.get("symbols")
    if isinstance(cached_symbols, dict) and (now - cached_ts) < _SEGMENT_OVERRIDE_TTL_SECONDS:
        return dict(cached_symbols)
    if not SEGMENT_OVERRIDES_PATH.exists():
        _SEGMENT_OVERRIDE_CACHE["ts"] = now
        _SEGMENT_OVERRIDE_CACHE["symbols"] = {}
        return {}
    try:
        raw = json.loads(SEGMENT_OVERRIDES_PATH.read_text(encoding="utf-8"))
        symbols = raw.get("symbols", {}) if isinstance(raw, dict) else {}
        if not isinstance(symbols, dict):
            symbols = {}
    except Exception:
        symbols = {}
    normalized = {
        str(sym).strip(): str(seg).strip()
        for sym, seg in symbols.items()
        if str(sym).strip() and str(seg).strip() in {"indexFunds", "largeCapStocks", "crypto", "pennyStocks"}
    }
    _SEGMENT_OVERRIDE_CACHE["ts"] = now
    _SEGMENT_OVERRIDE_CACHE["symbols"] = dict(normalized)
    return normalized


def _save_segment_overrides(symbols: dict[str, str]) -> None:
    SEGMENT_OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": dict(sorted(symbols.items())),
    }
    SEGMENT_OVERRIDES_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    _SEGMENT_OVERRIDE_CACHE["ts"] = time.time()
    _SEGMENT_OVERRIDE_CACHE["symbols"] = dict(symbols)


def _auto_promote_segment_overrides(
    *,
    index_symbols: set[str],
    large_cap_symbols: set[str],
    crypto_symbols: set[str],
    segment_hints: dict[str, str],
) -> dict[str, str]:
    overrides = _load_segment_overrides()
    if not TRADES_DB_PATH.exists():
        return overrides
    try:
        con = sqlite3.connect(TRADES_DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT symbol, price, result_json
            FROM exit_actions
            WHERE execute = 1
            ORDER BY ts DESC
            LIMIT 3000
            """
        ).fetchall()
    except Exception:
        rows = []
    finally:
        try:
            con.close()  # type: ignore[name-defined]
        except Exception:
            pass

    md_cfg = risk_policy.get("marketDiscovery", {}) if isinstance(risk_policy.get("marketDiscovery", {}), dict) else {}
    penny_cap = float(md_cfg.get("minPriceUsd", 10.0) or 10.0)
    pending: dict[str, list[float]] = {}
    for row in rows:
        symbol = str(row["symbol"] or "").strip()
        if not symbol or symbol in overrides:
            continue
        sym_u = symbol.upper()
        if symbol in crypto_symbols or "/" in symbol or (sym_u.endswith("USD") and len(sym_u) >= 6):
            continue
        # Ignore known failed attempts.
        result_raw = str(row["result_json"] or "").strip()
        if result_raw:
            try:
                result = json.loads(result_raw)
            except Exception:
                result = {}
            if isinstance(result, dict) and (result.get("ok") is False or (result.get("error") and result.get("ok") is not True)):
                continue
        try:
            px = float(row["price"] or 0.0)
        except (TypeError, ValueError):
            px = 0.0
        bucket = pending.setdefault(symbol, [])
        if px > 0:
            bucket.append(px)

    changed = False
    for symbol, prices in pending.items():
        hinted = str(segment_hints.get(symbol) or "").strip()
        if hinted in {"indexFunds", "largeCapStocks", "pennyStocks"}:
            target = hinted
        elif symbol in index_symbols:
            target = "indexFunds"
        elif symbol in large_cap_symbols:
            target = "largeCapStocks"
        else:
            avg_px = (sum(prices) / len(prices)) if prices else 0.0
            target = "pennyStocks" if (avg_px > 0 and avg_px <= penny_cap) else "largeCapStocks"
        if overrides.get(symbol) != target:
            overrides[symbol] = target
            changed = True

    if changed:
        _save_segment_overrides(overrides)
    return overrides


def _segment_for_symbol(
    symbol: str,
    *,
    index_symbols: set[str],
    large_cap_symbols: set[str],
    crypto_symbols: set[str],
    segment_hints: dict[str, str],
    segment_overrides: dict[str, str],
) -> str:
    sym = str(symbol or "").strip()
    sym_u = sym.upper()
    if sym in crypto_symbols or "/" in sym or (sym_u.endswith("USD") and len(sym_u) >= 6):
        return "crypto"
    override = str(segment_overrides.get(sym) or "").strip()
    if override in {"indexFunds", "largeCapStocks", "crypto", "pennyStocks"}:
        return override
    hinted = str(segment_hints.get(sym) or "").strip()
    if hinted in {"indexFunds", "largeCapStocks", "crypto", "pennyStocks"}:
        return hinted
    if sym in index_symbols:
        return "indexFunds"
    if sym in large_cap_symbols:
        return "largeCapStocks"
    return "other"


def _compute_overall_sell_stats(
    *,
    index_symbols: set[str],
    large_cap_symbols: set[str],
    crypto_symbols: set[str],
    segment_hints: dict[str, str],
    segment_overrides: dict[str, str],
    roundtrip_cost_pct_stocks: float,
    roundtrip_cost_pct_crypto: float,
) -> dict[str, Any]:
    empty_segments = {
        "indexFunds": {"evaluated": 0, "wins": 0, "win_rate_pct": 0.0},
        "largeCapStocks": {"evaluated": 0, "wins": 0, "win_rate_pct": 0.0},
        "crypto": {"evaluated": 0, "wins": 0, "win_rate_pct": 0.0},
        "pennyStocks": {"evaluated": 0, "wins": 0, "win_rate_pct": 0.0},
        "other": {"evaluated": 0, "wins": 0, "win_rate_pct": 0.0},
    }
    if not TRADES_DB_PATH.exists():
        return {
            "evaluated": 0,
            "wins": 0,
            "win_rate_pct": 0.0,
            "net_wins": 0,
            "net_win_rate_pct": 0.0,
            "avg_win_pct": 0.0,
            "avg_loss_pct": 0.0,
            "payoff_ratio": 0.0,
            "profit_factor": 0.0,
            "by_segment": empty_segments,
        }

    try:
        con = sqlite3.connect(TRADES_DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """
            SELECT symbol, pnl_pct, qty, price, result_json
            FROM exit_actions
            WHERE execute = 1
            ORDER BY ts ASC
            """
        ).fetchall()
    except Exception:
        rows = []
    finally:
        try:
            con.close()  # type: ignore[name-defined]
        except Exception:
            pass

    sell_trades_evaluated = 0
    sell_wins = 0
    net_sell_wins = 0
    realized_win_pcts: list[float] = []
    realized_loss_pcts: list[float] = []
    gross_profit_usd = 0.0
    gross_loss_usd = 0.0
    segment_counts: dict[str, dict[str, int]] = {
        "indexFunds": {"evaluated": 0, "wins": 0},
        "largeCapStocks": {"evaluated": 0, "wins": 0},
        "crypto": {"evaluated": 0, "wins": 0},
        "pennyStocks": {"evaluated": 0, "wins": 0},
        "other": {"evaluated": 0, "wins": 0},
    }

    for row in rows:
        symbol = str(row["symbol"] or "")
        try:
            pnl_pct = float(row["pnl_pct"] or 0.0)
        except (TypeError, ValueError):
            continue

        # Ignore known failed order attempts from reliability retries.
        result_raw = str(row["result_json"] or "").strip()
        if result_raw:
            try:
                result = json.loads(result_raw)
            except Exception:
                result = {}
            if isinstance(result, dict):
                if result.get("ok") is False:
                    continue
                if result.get("error") and result.get("ok") is not True:
                    continue

        seg = _segment_for_symbol(
            symbol,
            index_symbols=index_symbols,
            large_cap_symbols=large_cap_symbols,
            crypto_symbols=crypto_symbols,
            segment_hints=segment_hints,
            segment_overrides=segment_overrides,
        )
        if seg not in segment_counts:
            seg = "other"

        sell_trades_evaluated += 1
        segment_counts[seg]["evaluated"] += 1

        if pnl_pct > 0:
            sell_wins += 1
            segment_counts[seg]["wins"] += 1
            realized_win_pcts.append(pnl_pct)
        elif pnl_pct < 0:
            realized_loss_pcts.append(abs(pnl_pct))

        cost_pct = roundtrip_cost_pct_crypto if seg == "crypto" else roundtrip_cost_pct_stocks
        net_realized_pl_pct = pnl_pct - (cost_pct * 100.0)
        if net_realized_pl_pct > 0:
            net_sell_wins += 1

        # Proxy USD P/L from current notional and pnl_pct.
        try:
            qty = float(row["qty"] or 0.0)
            price = float(row["price"] or 0.0)
        except (TypeError, ValueError):
            qty = 0.0
            price = 0.0
        notional = max(qty * price, 0.0)
        pnl_usd = notional * (pnl_pct / 100.0)
        if pnl_usd > 0:
            gross_profit_usd += pnl_usd
        elif pnl_usd < 0:
            gross_loss_usd += abs(pnl_usd)

    sell_win_rate_pct = (sell_wins / sell_trades_evaluated * 100.0) if sell_trades_evaluated > 0 else 0.0
    net_sell_win_rate_pct = (net_sell_wins / sell_trades_evaluated * 100.0) if sell_trades_evaluated > 0 else 0.0
    avg_win_pct = (sum(realized_win_pcts) / len(realized_win_pcts)) if realized_win_pcts else 0.0
    avg_loss_pct = (sum(realized_loss_pcts) / len(realized_loss_pcts)) if realized_loss_pcts else 0.0
    payoff_ratio = (avg_win_pct / avg_loss_pct) if avg_loss_pct > 0 else 0.0
    profit_factor = (gross_profit_usd / gross_loss_usd) if gross_loss_usd > 0 else 0.0

    by_segment: dict[str, dict[str, float]] = {}
    for seg_name, counts in segment_counts.items():
        evaluated = int(counts["evaluated"])
        wins = int(counts["wins"])
        by_segment[seg_name] = {
            "evaluated": evaluated,
            "wins": wins,
            "win_rate_pct": (wins / evaluated * 100.0) if evaluated > 0 else 0.0,
        }

    return {
        "evaluated": sell_trades_evaluated,
        "wins": sell_wins,
        "win_rate_pct": sell_win_rate_pct,
        "net_wins": net_sell_wins,
        "net_win_rate_pct": net_sell_win_rate_pct,
        "avg_win_pct": avg_win_pct,
        "avg_loss_pct": avg_loss_pct,
        "payoff_ratio": payoff_ratio,
        "profit_factor": profit_factor,
        "by_segment": by_segment,
    }


def _build_dashboard_payload() -> dict[str, Any]:
    account = client.get_account()
    positions = client.get_open_positions()
    orders = client.get_orders(status="all", limit=200, direction="desc")
    portfolio = client.get_portfolio_history(period="1D", timeframe="1Min", extended_hours=True)

    equity = _to_float(account.get("equity"))
    cash = _to_float(account.get("cash"))
    buying_power = _to_float(account.get("buying_power"))
    day_start_equity = _to_float(account.get("last_equity"), equity)
    if day_start_equity <= 0:
        day_start_equity = equity
    day_pnl = equity - day_start_equity
    day_pnl_pct = (day_pnl / day_start_equity * 100.0) if day_start_equity else 0.0
    reserve_state = reserve_store.sync_with_cash(cash=cash)
    reserve_balance = _to_float(reserve_state.get("reserve_balance"))
    reserve_target_request = _to_float(reserve_state.get("reserve_target_request"))
    # Deployable budget = current equity minus reserved cash.
    working_budget = max(equity - reserve_balance, 0.0)
    deployable_cash = max(cash - reserve_balance, 0.0)
    # Apply equity mode overrides to derive the active max open positions cap.
    policy_effective = deepcopy(risk_policy)
    apply_equity_mode_switch(policy_effective, account=account)
    max_open_positions = int(policy_effective.get("maxOpenPositions", 0))
    fear_state = load_fear_climate_state()

    open_positions: list[dict[str, Any]] = []
    total_market_value = 0.0
    total_unrealized_pl = 0.0
    for p in positions:
        market_value = _to_float(p.get("market_value"))
        unrealized_pl = _to_float(p.get("unrealized_pl"))
        open_positions.append(
            {
                "symbol": p.get("symbol"),
                "qty": p.get("qty"),
                "avg_entry_price": _to_float(p.get("avg_entry_price")),
                "current_price": _to_float(p.get("current_price")),
                "market_value": market_value,
                "unrealized_pl": unrealized_pl,
                "unrealized_plpc": _to_float(p.get("unrealized_plpc")) * 100.0,
                "side": p.get("side", "long"),
                "asset_class": p.get("asset_class"),
            }
        )
        total_market_value += market_value
        total_unrealized_pl += unrealized_pl

    filled_orders: list[dict[str, Any]] = []
    for o in orders:
        status = str(o.get("status", ""))
        filled_at = o.get("filled_at")
        filled_qty = _to_float(o.get("filled_qty"))
        filled_avg_price = _to_float(o.get("filled_avg_price"))
        if status not in {"filled", "partially_filled"} or not filled_at or filled_qty <= 0:
            continue
        filled_orders.append(
            {
                "filled_at": filled_at,
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "qty": filled_qty,
                "price": filled_avg_price,
                "notional": filled_qty * filled_avg_price,
                "status": status,
            }
        )
    # Build realized win-rate telemetry on sell fills using FIFO matched buy costs.
    fifo_costs: dict[str, list[tuple[float, float]]] = {}
    sell_trades_evaluated = 0
    sell_wins = 0
    ai_cfg = (policy_effective.get("aiScheduler") or {}) if "policy_effective" in locals() else {}
    roundtrip_cost_pct_stocks = (_to_float(ai_cfg.get("slippageBufferBpsStocks"), 5.0) * 2.0) / 10000.0
    roundtrip_cost_pct_crypto = (_to_float(ai_cfg.get("slippageBufferBpsCrypto"), 10.0) * 2.0) / 10000.0
    net_sell_wins = 0
    realized_win_pcts: list[float] = []
    realized_loss_pcts: list[float] = []
    gross_profit_usd = 0.0
    gross_loss_usd = 0.0
    segment_allow = (policy_effective.get("allowedSegments") or {}) if "policy_effective" in locals() else {}
    index_symbols = set(segment_allow.get("indexFunds", {}).get("symbolsAllowlist", []))
    large_cap_symbols = set(segment_allow.get("largeCapStocks", {}).get("symbolsAllowlist", []))
    crypto_symbols = set(segment_allow.get("crypto", {}).get("symbolsAllowlist", []))
    segment_hints = _load_symbol_segment_hints()
    segment_overrides = _auto_promote_segment_overrides(
        index_symbols=index_symbols,
        large_cap_symbols=large_cap_symbols,
        crypto_symbols=crypto_symbols,
        segment_hints=segment_hints,
    )
    segment_stats: dict[str, dict[str, float]] = {
        "indexFunds": {"evaluated": 0.0, "wins": 0.0},
        "largeCapStocks": {"evaluated": 0.0, "wins": 0.0},
        "crypto": {"evaluated": 0.0, "wins": 0.0},
        "pennyStocks": {"evaluated": 0.0, "wins": 0.0},
        "other": {"evaluated": 0.0, "wins": 0.0},
    }

    for fill in sorted(filled_orders, key=lambda r: r["filled_at"]):
        symbol = str(fill.get("symbol") or "")
        side = str(fill.get("side") or "").lower()
        qty = _to_float(fill.get("qty"))
        price = _to_float(fill.get("price"))
        if not symbol or qty <= 0 or price <= 0:
            continue

        if side == "buy":
            fifo_costs.setdefault(symbol, []).append((qty, price))
            continue
        if side != "sell":
            continue

        remaining = qty
        consumed_cost = 0.0
        consumed_qty = 0.0
        lots = fifo_costs.setdefault(symbol, [])
        while remaining > 0 and lots:
            lot_qty, lot_price = lots[0]
            take = min(remaining, lot_qty)
            consumed_cost += take * lot_price
            consumed_qty += take
            remaining -= take
            lot_qty -= take
            if lot_qty <= 1e-9:
                lots.pop(0)
            else:
                lots[0] = (lot_qty, lot_price)
        if consumed_qty <= 0:
            continue

        avg_cost = consumed_cost / consumed_qty
        realized_pl_pct = ((price - avg_cost) / avg_cost * 100.0) if avg_cost > 0 else 0.0
        realized_pl_usd = (price - avg_cost) * consumed_qty
        fill["realized_pl_pct"] = realized_pl_pct
        sell_trades_evaluated += 1
        if realized_pl_pct > 0:
            sell_wins += 1
            realized_win_pcts.append(realized_pl_pct)
        elif realized_pl_pct < 0:
            realized_loss_pcts.append(abs(realized_pl_pct))

        if realized_pl_usd > 0:
            gross_profit_usd += realized_pl_usd
        elif realized_pl_usd < 0:
            gross_loss_usd += abs(realized_pl_usd)

        seg = _segment_for_symbol(
            symbol,
            index_symbols=index_symbols,
            large_cap_symbols=large_cap_symbols,
            crypto_symbols=crypto_symbols,
            segment_hints=segment_hints,
            segment_overrides=segment_overrides,
        )
        seg_bucket = segment_stats.setdefault(seg, {"evaluated": 0.0, "wins": 0.0})
        seg_bucket["evaluated"] += 1.0
        if realized_pl_pct > 0:
            seg_bucket["wins"] += 1.0

        is_crypto = seg == "crypto"
        net_realized_pl_pct = realized_pl_pct - ((roundtrip_cost_pct_crypto if is_crypto else roundtrip_cost_pct_stocks) * 100.0)
        fill["net_realized_pl_pct"] = net_realized_pl_pct
        if net_realized_pl_pct > 0:
            net_sell_wins += 1

    sell_win_rate_pct = (sell_wins / sell_trades_evaluated * 100.0) if sell_trades_evaluated > 0 else 0.0
    net_sell_win_rate_pct = (net_sell_wins / sell_trades_evaluated * 100.0) if sell_trades_evaluated > 0 else 0.0
    avg_win_pct = (sum(realized_win_pcts) / len(realized_win_pcts)) if realized_win_pcts else 0.0
    avg_loss_pct = (sum(realized_loss_pcts) / len(realized_loss_pcts)) if realized_loss_pcts else 0.0
    payoff_ratio = (avg_win_pct / avg_loss_pct) if avg_loss_pct > 0 else 0.0
    profit_factor = (gross_profit_usd / gross_loss_usd) if gross_loss_usd > 0 else 0.0
    sell_win_rate_by_segment: dict[str, dict[str, float]] = {}
    for seg_name, bucket in segment_stats.items():
        evaluated = int(bucket.get("evaluated", 0))
        wins = int(bucket.get("wins", 0))
        sell_win_rate_by_segment[seg_name] = {
            "evaluated": evaluated,
            "wins": wins,
            "win_rate_pct": (wins / evaluated * 100.0) if evaluated > 0 else 0.0,
        }

    overall_stats = _compute_overall_sell_stats(
        index_symbols=index_symbols,
        large_cap_symbols=large_cap_symbols,
        crypto_symbols=crypto_symbols,
        segment_hints=segment_hints,
        segment_overrides=segment_overrides,
        roundtrip_cost_pct_stocks=roundtrip_cost_pct_stocks,
        roundtrip_cost_pct_crypto=roundtrip_cost_pct_crypto,
    )
    use_overall = int(overall_stats.get("evaluated", 0)) > 0

    recent_fills = list(filled_orders)
    recent_fills.sort(key=lambda r: r["filled_at"], reverse=True)
    recent_fills = recent_fills[:40]

    ts = portfolio.get("timestamp", []) or []
    eq = portfolio.get("equity", []) or []
    pr = portfolio.get("profit_loss", []) or []
    series: list[dict[str, Any]] = []
    max_len = min(len(ts), len(eq), len(pr))
    if max_len:
        max_ts = int(ts[max_len - 1])
        cutoff = max_ts - 3600  # last 60 minutes only
    else:
        cutoff = 0
    for i in range(max_len):
        point_ts = int(ts[i])
        if point_ts < cutoff:
            continue
        series.append(
            {
                "t": datetime.fromtimestamp(point_ts, tz=timezone.utc).isoformat(),
                "equity": _to_float(eq[i]),
                "profit_loss": _to_float(pr[i]),
            }
        )

    return {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "fear_climate": fear_state,
        "summary": {
            "equity": equity,
            "cash": cash,
            "buying_power": buying_power,
            "working_budget": working_budget,
            "day_start_equity": day_start_equity,
            "day_pnl": day_pnl,
            "day_pnl_pct": day_pnl_pct,
            "open_positions_count": len(open_positions),
            "max_open_positions": max_open_positions,
            "open_positions_market_value": total_market_value,
            "open_positions_unrealized_pl": total_unrealized_pl,
            "sell_trades_evaluated": int(overall_stats["evaluated"]) if use_overall else sell_trades_evaluated,
            "sell_wins": int(overall_stats["wins"]) if use_overall else sell_wins,
            "sell_win_rate_pct": float(overall_stats["win_rate_pct"]) if use_overall else sell_win_rate_pct,
            "net_sell_wins": int(overall_stats["net_wins"]) if use_overall else net_sell_wins,
            "net_sell_win_rate_pct": float(overall_stats["net_win_rate_pct"]) if use_overall else net_sell_win_rate_pct,
            "avg_win_pct": float(overall_stats["avg_win_pct"]) if use_overall else avg_win_pct,
            "avg_loss_pct": float(overall_stats["avg_loss_pct"]) if use_overall else avg_loss_pct,
            "payoff_ratio": float(overall_stats["payoff_ratio"]) if use_overall else payoff_ratio,
            "profit_factor": float(overall_stats["profit_factor"]) if use_overall else profit_factor,
            "roundtrip_cost_bps_stocks": roundtrip_cost_pct_stocks * 10000.0,
            "roundtrip_cost_bps_crypto": roundtrip_cost_pct_crypto * 10000.0,
            "sell_win_rate_by_segment": overall_stats["by_segment"] if use_overall else sell_win_rate_by_segment,
            "reserve_balance": reserve_balance,
            "reserve_target_request": reserve_target_request,
            "deployable_cash": deployable_cash,
            "fear_climate_enabled": bool(fear_state.get("enabled", False)),
            "win_rate_basis": "overall_exit_history" if use_overall else "recent_broker_fills",
        },
        "open_positions": open_positions,
        "recent_fills": recent_fills,
        "portfolio_series": series,
    }


LIVE_HTML = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Live Trading Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
      :root {
        --bg: #0a0f1e;
        --panel: #121a2f;
        --text: #dbe7ff;
        --muted: #9fb1dd;
        --good: #19d39f;
        --bad: #ff5b6b;
        --line: #2f3f6d;
      }
      * { box-sizing: border-box; }
      body { margin: 0; padding: 16px; font-family: "Segoe UI", Arial, sans-serif; background: var(--bg); color: var(--text); }
      h1 { margin: 0 0 4px; font-size: 22px; }
      .sub { margin: 0 0 14px; color: var(--muted); font-size: 13px; }
      .titleStack { display: flex; flex-direction: column; gap: 8px; }
      .killSwitchBtn {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 7px 12px;
        border-radius: 8px;
        border: 1px solid #6b1f2a;
        background: linear-gradient(180deg, #2b0f16, #1c0a10);
        color: #ffdbe2;
        font-weight: 700;
        cursor: pointer;
      }
      .killSwitchBtn:hover { filter: brightness(1.12); }
      .killStatus { color: var(--muted); font-size: 12px; margin-top: 4px; min-height: 16px; }
      .healthAlert {
        display: none;
        margin: 10px 0 12px;
        padding: 10px 12px;
        border-radius: 8px;
        border: 1px solid #7a2635;
        background: linear-gradient(180deg, #3a121d, #2a0d15);
        color: #ffdbe2;
        font-size: 13px;
        font-weight: 700;
      }
      .dotSkull { width: 28px; height: 28px; }
      .dotSkull circle { fill: #ffdbe2; }
      .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin-bottom: 12px; }
      .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 10px; }
      .k { color: var(--muted); font-size: 11px; text-transform: uppercase; }
      .v { margin-top: 5px; font-size: 17px; font-weight: 700; }
      .grid { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(0, 1fr); gap: 12px; min-height: 520px; }
      .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 10px; display: flex; flex-direction: column; min-height: 0; overflow: hidden; }
      .panel h2 { margin: 0 0 8px; font-size: 15px; }
      .panelHeader { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; margin: 0 0 8px; }
      .panelHeader h2 { margin: 0; }
      .chartMeta { color: var(--muted); font-size: 12px; text-align: right; white-space: nowrap; }
      table { width: 100%; border-collapse: collapse; font-size: 12px; table-layout: fixed; }
      th, td { padding: 6px; border-bottom: 1px solid #223056; text-align: right; }
      th:first-child, td:first-child { text-align: left; }
      th { background: #0f1830; }
      .good { color: var(--good); }
      .bad { color: var(--bad); }
      .buyRow { background: rgba(25, 211, 159, 0.08); }
      .sellRow { background: rgba(255, 91, 107, 0.08); }
      .sideTag { padding: 2px 6px; border-radius: 999px; font-size: 11px; font-weight: 700; display: inline-block; }
      .sideBuy { background: rgba(25, 211, 159, 0.2); color: var(--good); }
      .sideSell { background: rgba(255, 91, 107, 0.2); color: var(--bad); }
      .leftCol, .rightCol { display: flex; flex-direction: column; gap: 12px; min-height: 0; }
      .leftCol > .panel:first-child { min-height: 280px; flex: 1.1; }
      .leftCol > .panel:last-child { min-height: 250px; flex: 1; }
      .rightCol > .panel:first-child { min-height: 250px; flex: 1; }
      .rightCol > .panel:last-child { min-height: 320px; flex: 1.1; }
      .panelBody { flex: 1; min-height: 0; overflow: hidden; }
      .scroll { height: 100%; overflow: auto; overscroll-behavior: contain; }
      .scroll thead th { position: sticky; top: 0; z-index: 1; }
      #equityChart { width: 100% !important; height: 100% !important; }
      .segTable { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 11px; }
      .segTable th, .segTable td { border-bottom: 1px solid #223056; padding: 5px; text-align: right; }
      .segTable th:first-child, .segTable td:first-child { text-align: left; }
      .metricGrid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      .metricItem { background: #0f1830; border: 1px solid #2b3b67; border-radius: 8px; padding: 8px; }
      .metricItem .label { color: var(--muted); font-size: 11px; }
      .metricItem .value { margin-top: 4px; font-size: 15px; font-weight: 700; }
      .miniBar { margin-top: 8px; height: 6px; background: #1c2a4f; border-radius: 999px; overflow: hidden; }
      .miniBar > div { height: 100%; background: linear-gradient(90deg, #63a4ff, #35d4a0); width: 0%; }
      @media (max-width: 1200px) {
        .grid { grid-template-columns: 1fr; min-height: 0; }
        .leftCol > .panel:first-child,
        .leftCol > .panel:last-child,
        .rightCol > .panel:first-child,
        .rightCol > .panel:last-child { min-height: 280px; }
      }
      @media (max-width: 760px) {
        .metricGrid { grid-template-columns: 1fr; }
      }
      .krakenGrid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
      .krakenGrid .panel { min-height: 300px; }
      @media (max-width: 1200px) {
        .krakenGrid { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
      <div>
        <div class="titleStack">
          <h1>RevenueGenerator Live Split View</h1>
          <button class="killSwitchBtn" onclick="triggerKillSwitch()">
            <svg class="dotSkull" viewBox="0 0 28 28" aria-hidden="true">
              <!-- dot-matrix skull -->
              <circle cx="9" cy="6" r="1.1"/><circle cx="11.5" cy="5" r="1.1"/><circle cx="14" cy="4.5" r="1.1"/><circle cx="16.5" cy="5" r="1.1"/><circle cx="19" cy="6" r="1.1"/>
              <circle cx="7.5" cy="8.5" r="1.1"/><circle cx="10" cy="8" r="1.1"/><circle cx="12.5" cy="7.7" r="1.1"/><circle cx="15.5" cy="7.7" r="1.1"/><circle cx="18" cy="8" r="1.1"/><circle cx="20.5" cy="8.5" r="1.1"/>
              <circle cx="7" cy="11" r="1.1"/><circle cx="10" cy="11.5" r="1.1"/><circle cx="18" cy="11.5" r="1.1"/><circle cx="21" cy="11" r="1.1"/>
              <circle cx="8.5" cy="14" r="1.1"/><circle cx="11.5" cy="14.5" r="1.1"/><circle cx="14" cy="15" r="1.1"/><circle cx="16.5" cy="14.5" r="1.1"/><circle cx="19.5" cy="14" r="1.1"/>
              <circle cx="11.5" cy="18" r="1.1"/><circle cx="14" cy="18.4" r="1.1"/><circle cx="16.5" cy="18" r="1.1"/>
              <circle cx="12.5" cy="20.7" r="1.0"/><circle cx="15.5" cy="20.7" r="1.0"/>
              <!-- crossbones -->
              <circle cx="6.2" cy="21.8" r="1.0"/><circle cx="8.0" cy="23.0" r="1.0"/><circle cx="9.8" cy="24.2" r="1.0"/><circle cx="11.6" cy="25.4" r="1.0"/>
              <circle cx="6.1" cy="25.5" r="1.0"/><circle cx="8.1" cy="24.2" r="1.0"/><circle cx="10.1" cy="22.9" r="1.0"/><circle cx="12.1" cy="21.6" r="1.0"/>
              <circle cx="16.4" cy="25.4" r="1.0"/><circle cx="18.2" cy="24.2" r="1.0"/><circle cx="20.0" cy="23.0" r="1.0"/><circle cx="21.8" cy="21.8" r="1.0"/>
              <circle cx="15.9" cy="21.6" r="1.0"/><circle cx="17.9" cy="22.9" r="1.0"/><circle cx="19.9" cy="24.2" r="1.0"/><circle cx="21.9" cy="25.5" r="1.0"/>
            </svg>
            Kill Scheduler
          </button>
          <div id="killStatus" class="killStatus"></div>
          <p class="sub">Auto-refresh every 5s. Chart shows last 60 minutes (1-min points, ET).</p>
        </div>
      </div>
      <div class="card" style="width:min(100%, 420px); margin-bottom:12px;">
        <div class="k">Reserve Controls</div>
        <div style="margin-top:8px; display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
          <input id="reserveTargetInput" type="number" step="0.01" min="0" placeholder="Reserve request (USD)" style="width:170px; padding:6px;" />
          <button onclick="setReserveTarget()" style="padding:6px 10px;">Set Reserve Request</button>
          <button onclick="recirculateReserves()" style="padding:6px 10px;">Recirculate Reserves</button>
        </div>
        <div id="reserveStatus" style="margin-top:8px; color:var(--muted); font-size:12px;">Reserve pot: -- | Pending request: --</div>
        <div style="margin-top:10px; border-top:1px solid #223056; padding-top:10px;">
          <div class="k">Fear Climate Mode (Crypto/Kraken)</div>
          <div style="margin-top:8px; display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
            <button id="fearToggleBtn" onclick="toggleFearClimate()" style="padding:6px 10px;">Enable Fear Mode</button>
          </div>
          <div id="fearClimateStatus" style="margin-top:8px; color:var(--muted); font-size:12px;">Fear mode: --</div>
        </div>
      </div>
    </div>
    <div id="exitMgrAlert" class="healthAlert">Exit manager health alert.</div>
    <div id="schedulerAlert" class="healthAlert">Scheduler health alert.</div>

    <div class="cards">
      <div class="card"><div class="k">Budget (Deployable Equity)</div><div class="v" id="budget">$0</div></div>
      <div class="card"><div class="k">Balance (Equity)</div><div class="v" id="equity">$0</div></div>
      <div class="card"><div class="k">Open Positions</div><div class="v" id="openCountTop">0</div></div>
      <div class="card"><div class="k">Sell Win Rate (Overall)</div><div class="v" id="sellWinRateTop">0.00%</div></div>
      <div class="card"><div class="k">Net Sell Win Rate (Overall)</div><div class="v" id="netSellWinRateTop">0.00%</div></div>
      <div class="card"><div class="k">Payoff Ratio</div><div class="v" id="payoffRatioTop">0.00x</div></div>
      <div class="card"><div class="k">Profit Factor</div><div class="v" id="profitFactorTop">0.00x</div></div>
      <div class="card"><div class="k">Reserve Pot</div><div class="v" id="reserveTop">$0</div></div>
      <div class="card"><div class="k">Cash</div><div class="v" id="cash">$0</div></div>
      <div class="card"><div class="k">Deployable Cash</div><div class="v" id="deployableCash">$0</div></div>
      <div class="card">
        <div class="k">Exit Manager Health</div>
        <div class="v" id="exitMgrHealthTop">--</div>
        <div id="exitMgrHealthMeta" style="margin-top:6px; color:var(--muted); font-size:11px;">Heartbeat: --</div>
      </div>
      <div class="card">
        <div class="k">Scheduler Health</div>
        <div class="v" id="schedulerHealthTop">--</div>
        <div id="schedulerHealthMeta" style="margin-top:6px; color:var(--muted); font-size:11px;">Heartbeat: --</div>
      </div>
      <div class="card"><div class="k">Purchases (Open MV)</div><div class="v" id="openMv">$0</div></div>
      <div class="card"><div class="k">Returns (Unrealized)</div><div class="v" id="unrealized">$0</div></div>
      <div class="card"><div class="k">Up / Down Today</div><div class="v" id="dayPnl">$0</div></div>
    </div>

    <div class="grid">
      <div class="leftCol">
      <div class="panel">
        <div class="panelHeader">
          <h2>Equity and P/L Through Time (Last Hour)</h2>
          <div id="chartMeta" class="chartMeta">Last refresh: -- | Latest point: --</div>
        </div>
        <div class="panelBody">
          <canvas id="equityChart"></canvas>
        </div>
      </div>
      <div class="panel">
        <h2 style="margin-top:12px;">Open Positions (Active rows)</h2>
        <div class="panelBody scroll">
          <table id="positionsTable">
            <thead>
              <tr>
                <th>Symbol</th><th>Qty</th><th>Entry</th><th>Current</th><th>Market Value</th><th>U P/L</th><th>U P/L %</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      </div>
      <div class="rightCol">
        <div class="panel">
          <h2>Recent Acquisitions / Sells (fills)</h2>
          <div class="panelBody scroll">
            <table id="fillsTable">
              <thead>
                <tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th><th>Notional</th></tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <div class="panel">
          <h2>Session Metrics</h2>
          <div class="panelBody scroll">
            <div class="metricGrid">
              <div class="metricItem"><div class="label">Open Positions</div><div class="value" id="mOpenCount">0</div></div>
              <div class="metricItem"><div class="label">Buying Power</div><div class="value" id="mBuyingPower">$0</div></div>
              <div class="metricItem"><div class="label">Day P/L %</div><div class="value" id="mDayPct">0.00%</div></div>
              <div class="metricItem"><div class="label">As Of (ET)</div><div class="value" id="mAsOf">--</div></div>
            </div>
            <div class="metricItem" style="margin-top:8px;">
              <div class="label">Capital Utilization (Open MV / Equity)</div>
              <div class="value" id="mUtilPct">0.00%</div>
              <div class="miniBar"><div id="mUtilBar"></div></div>
            </div>
            <div class="metricItem" style="margin-top:8px;">
              <div class="label">Cash Ratio (Cash / Equity)</div>
              <div class="value" id="mCashPct">0.00%</div>
              <div class="miniBar"><div id="mCashBar"></div></div>
            </div>
            <div class="metricItem" style="margin-top:8px;">
              <div class="label">Sell Win Rate by Segment</div>
              <table class="segTable" id="segmentWinTable">
                <thead>
                  <tr><th>Segment</th><th>Wins</th><th>Evaluated</th><th>Win %</th></tr>
                </thead>
                <tbody></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div id="krakenSection" style="margin-top:20px;">
      <h1 style="margin:0 0 4px; font-size:20px; color:#f7931a;">Kraken Crypto Broker</h1>
      <p class="sub">Dedicated 24/7 crypto execution via Kraken. Auto-refresh with main dashboard.</p>
      <div id="krakenStatus" style="color:var(--muted); font-size:13px; margin-bottom:8px;">Loading Kraken data...</div>
      <div class="cards" id="krakenCards">
        <div class="card" style="border-color:#f7931a44;"><div class="k">Kraken USD</div><div class="v" id="kUsd">$0</div></div>
        <div class="card" style="border-color:#f7931a44;"><div class="k">Kraken Equity</div><div class="v" id="kEquity">$0</div></div>
        <div class="card" style="border-color:#f7931a44;"><div class="k">Free Margin</div><div class="v" id="kMargin">$0</div></div>
        <div class="card" style="border-color:#f7931a44;"><div class="k">Open Orders</div><div class="v" id="kOpenOrders">0</div></div>
        <div class="card" style="border-color:#f7931a44;"><div class="k">Filled Trades</div><div class="v" id="kTradeCount">0</div></div>
      </div>
      <div class="krakenGrid">
        <div class="panel" style="border-color:#f7931a33;">
          <h2 style="color:#f7931a;">Live Prices &amp; Holdings</h2>
          <div class="panelBody scroll">
            <table id="krakenPricesTable">
              <thead><tr><th>Pair</th><th>Price (USD)</th></tr></thead>
              <tbody></tbody>
            </table>
            <h3 style="margin:10px 0 4px; font-size:13px; color:#f7931a;">Holdings</h3>
            <table id="krakenHoldingsTable">
              <thead><tr><th>Asset</th><th>Qty</th><th>Price</th><th>Value</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <div class="panel" style="border-color:#f7931a33;">
          <h2 style="color:#f7931a;">Open Orders &amp; Trades</h2>
          <div class="panelBody scroll">
            <table id="krakenOrdersTable">
              <thead><tr><th>ID</th><th>Description</th><th>Vol</th><th>Filled</th><th>Status</th></tr></thead>
              <tbody></tbody>
            </table>
            <h3 style="margin:10px 0 4px; font-size:13px; color:#f7931a;">Recent Trades</h3>
            <table id="krakenTradesTable">
              <thead><tr><th>Pair</th><th>Side</th><th>Price</th><th>Vol</th><th>Cost</th><th>Fee</th></tr></thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <script>
      const fmt = (n) => Number(n || 0).toLocaleString(undefined, { style: "currency", currency: "USD" });
      const fmtPct = (n) => `${Number(n || 0).toFixed(2)}%`;
      const ET_ZONE = "America/New_York";
      const fmtEtTime = (d) => new Date(d).toLocaleTimeString([], { timeZone: ET_ZONE, hour: "numeric", minute: "2-digit", second: "2-digit" });
      const colorPnL = (el, v) => { el.classList.remove("good","bad"); el.classList.add(Number(v) >= 0 ? "good" : "bad"); };

      const ctx = document.getElementById("equityChart").getContext("2d");
      const equityChart = new Chart(ctx, {
        type: "line",
        data: {
          labels: [],
          datasets: [
            { label: "Equity", data: [], borderColor: "#63a4ff", backgroundColor: "rgba(99,164,255,0.18)", pointRadius: 2, pointHoverRadius: 3, tension: 0.22, fill: true, yAxisID: "y" },
            { label: "Profit/Loss", data: [], borderColor: "#35d4a0", backgroundColor: "rgba(53,212,160,0.15)", pointRadius: 1.5, pointHoverRadius: 2.5, tension: 0.22, fill: false, yAxisID: "y1" }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            x: { ticks: { color: "#aabde5", maxTicksLimit: 12 } },
            y: { ticks: { color: "#aabde5" } },
            y1: { position: "right", ticks: { color: "#7ae6c3" }, grid: { drawOnChartArea: false } }
          },
          plugins: { legend: { labels: { color: "#dbe7ff" } } }
        }
      });

      function renderPositions(rows) {
        const body = document.querySelector("#positionsTable tbody");
        body.innerHTML = "";
        for (const r of rows) {
          const tr = document.createElement("tr");
          const pnlClass = Number(r.unrealized_pl) >= 0 ? "good" : "bad";
          tr.innerHTML = `
            <td>${r.symbol}</td>
            <td>${r.qty}</td>
            <td>${fmt(r.avg_entry_price)}</td>
            <td>${fmt(r.current_price)}</td>
            <td>${fmt(r.market_value)}</td>
            <td class="${pnlClass}">${fmt(r.unrealized_pl)}</td>
            <td class="${pnlClass}">${fmtPct(r.unrealized_plpc)}</td>
          `;
          body.appendChild(tr);
        }
      }

      function renderFills(rows) {
        const body = document.querySelector("#fillsTable tbody");
        body.innerHTML = "";
        for (const r of rows.slice(0, 50)) {
          const tr = document.createElement("tr");
          const side = String(r.side || "").toLowerCase();
          tr.className = side === "buy" ? "buyRow" : (side === "sell" ? "sellRow" : "");
          const sideTagClass = side === "buy" ? "sideBuy" : "sideSell";
          tr.innerHTML = `
            <td>${fmtEtTime(r.filled_at)}</td>
            <td>${r.symbol}</td>
            <td><span class="sideTag ${sideTagClass}">${(r.side || "").toUpperCase()}</span></td>
            <td>${Number(r.qty).toFixed(6).replace(/\\.0+$/, "")}</td>
            <td>${fmt(r.price)}</td>
            <td>${fmt(r.notional)}</td>
          `;
          body.appendChild(tr);
        }
      }

      function renderSessionMetrics(data, s) {
        const equity = Number(s.equity || 0);
        const openMv = Number(s.open_positions_market_value || 0);
        const cash = Number(s.cash || 0);
        const utilPct = equity > 0 ? (openMv / equity) * 100 : 0;
        const cashPct = equity > 0 ? (cash / equity) * 100 : 0;
        document.getElementById("mOpenCount").textContent = String(s.open_positions_count || 0);
        document.getElementById("mBuyingPower").textContent = fmt(s.buying_power);
        document.getElementById("mDayPct").textContent = fmtPct(s.day_pnl_pct);
        document.getElementById("mAsOf").textContent = `${fmtEtTime(data.as_of)} ET`;
        document.getElementById("mUtilPct").textContent = fmtPct(utilPct);
        document.getElementById("mCashPct").textContent = fmtPct(cashPct);
        document.getElementById("mUtilBar").style.width = `${Math.max(0, Math.min(100, utilPct)).toFixed(2)}%`;
        document.getElementById("mCashBar").style.width = `${Math.max(0, Math.min(100, cashPct)).toFixed(2)}%`;

        const segTableBody = document.querySelector("#segmentWinTable tbody");
        segTableBody.innerHTML = "";
        const segMap = s.sell_win_rate_by_segment || {};
        const segmentRows = [
          { key: "crypto", label: "crypto" },
          { key: "largeCapStocks", label: "largeCapStocks" },
          { key: "indexFunds", label: "indexFunds" },
          { key: "pennyStocks", label: "pennyStocks" },
          { key: "other", label: "other" },
        ];
        for (const segInfo of segmentRows) {
          const seg = segInfo.key;
          const row = segMap[seg] || { wins: 0, evaluated: 0, win_rate_pct: 0 };
          const tr = document.createElement("tr");
          tr.innerHTML = `<td>${segInfo.label}</td><td>${row.wins || 0}</td><td>${row.evaluated || 0}</td><td>${fmtPct(row.win_rate_pct || 0)}</td>`;
          segTableBody.appendChild(tr);
        }
      }

      async function setReserveTarget() {
        const input = document.getElementById("reserveTargetInput");
        const value = Number(input.value || 0);
        await fetch("/api/live-dashboard/reserve-target", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ target: value }),
        });
        input.value = "";
        await refresh();
      }

      async function recirculateReserves() {
        await fetch("/api/live-dashboard/reserve-recirculate", { method: "POST" });
        await refresh();
      }

      function renderFearClimate(state) {
        const enabled = !!(state && state.enabled);
        const btn = document.getElementById("fearToggleBtn");
        const status = document.getElementById("fearClimateStatus");
        btn.textContent = enabled ? "Disable Fear Mode" : "Enable Fear Mode";
        status.textContent = `Fear mode: ${enabled ? "ON (stricter crypto entries)" : "OFF"}`;
        btn.style.background = enabled ? "#3c1720" : "";
        btn.style.color = enabled ? "#ffdbe2" : "";
        btn.style.border = enabled ? "1px solid #7a2635" : "";
      }

      async function toggleFearClimate() {
        const btn = document.getElementById("fearToggleBtn");
        btn.disabled = true;
        try {
          const enabled = btn.textContent.toLowerCase().includes("enable");
          await fetch("/api/fear-climate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ enabled }),
          });
          await refresh();
        } finally {
          btn.disabled = false;
        }
      }

      async function triggerKillSwitch() {
        const killStatus = document.getElementById("killStatus");
        killStatus.textContent = "KILL command sent...";
        const resp = await fetch("/api/live-dashboard/kill-switch", { method: "POST" });
        const data = await resp.json();
        if (data.ok) {
          const list = Array.isArray(data.killed) ? data.killed.join(", ") : "";
          killStatus.textContent = data.count > 0
            ? `Scheduler stopped. Killed PID(s): ${list}`
            : "No running scheduler process found.";
        } else {
          killStatus.textContent = `Kill switch failed: ${data.error || "unknown error"}`;
        }
      }

      function renderExitManagerHealth(h) {
        const top = document.getElementById("exitMgrHealthTop");
        const meta = document.getElementById("exitMgrHealthMeta");
        top.classList.remove("good", "bad");
        if (!h || !h.available) {
          top.textContent = "UNAVAILABLE";
          top.classList.add("bad");
          meta.textContent = `Heartbeat: ${h && h.reason ? h.reason : "not available"}`;
          return;
        }

        const status = String(h.status || "unknown").toUpperCase();
        const age = Number(h.age_seconds ?? -1);
        if (h.healthy) {
          top.textContent = "HEALTHY";
          top.classList.add("good");
        } else {
          top.textContent = status;
          top.classList.add("bad");
        }

        const updatedAt = h.updated_at ? `${fmtEtTime(h.updated_at)} ET` : "--";
        const pid = h.pid != null ? String(h.pid) : "--";
        const cycle = Number(h.cycle_count || 0);
        const errors = Number(h.consecutive_errors || 0);
        const ageText = age >= 0 ? `${age}s` : "--";
        meta.textContent = `Heartbeat: ${updatedAt} | Age: ${ageText} | PID: ${pid} | Cycle: ${cycle} | Errors: ${errors}`;
      }

      function renderExitManagerAlert(h) {
        const alert = document.getElementById("exitMgrAlert");
        if (!h || !h.available) {
          alert.style.display = "block";
          alert.textContent = "Exit manager alert: heartbeat unavailable.";
          return;
        }
        if (h.healthy) {
          alert.style.display = "none";
          return;
        }
        const status = String(h.status || "unknown").toUpperCase();
        const age = Number(h.age_seconds ?? -1);
        const ageText = age >= 0 ? `${age}s` : "--";
        const detail = h.error ? ` | Error: ${h.error}` : "";
        alert.style.display = "block";
        alert.textContent = `Exit manager alert: ${status} | Heartbeat age: ${ageText}${detail}`;
      }

      async function refreshExitManagerHealth() {
        try {
          const resp = await fetch("/api/live-dashboard/exit-manager-health");
          const health = await resp.json();
          renderExitManagerHealth(health);
          renderExitManagerAlert(health);
        } catch (err) {
          const fallback = { available: false, reason: err && err.message ? err.message : "fetch failed" };
          renderExitManagerHealth(fallback);
          renderExitManagerAlert(fallback);
        }
      }

      function renderSchedulerHealth(h) {
        const top = document.getElementById("schedulerHealthTop");
        const meta = document.getElementById("schedulerHealthMeta");
        top.classList.remove("good", "bad");
        if (!h || !h.available) {
          top.textContent = "UNAVAILABLE";
          top.classList.add("bad");
          meta.textContent = `Heartbeat: ${h && h.reason ? h.reason : "not available"}`;
          return;
        }
        const status = String(h.status || "unknown").toUpperCase();
        const age = Number(h.age_seconds ?? -1);
        if (h.healthy) {
          top.textContent = "HEALTHY";
          top.classList.add("good");
        } else {
          top.textContent = status;
          top.classList.add("bad");
        }
        const updatedAt = h.updated_at ? `${fmtEtTime(h.updated_at)} ET` : "--";
        const pid = h.pid != null ? String(h.pid) : "--";
        const cycle = Number(h.cycle_count || 0);
        const ageText = age >= 0 ? `${age}s` : "--";
        const mode = h.schedule_mode ? String(h.schedule_mode) : "--";
        meta.textContent = `Heartbeat: ${updatedAt} | Age: ${ageText} | PID: ${pid} | Cycle: ${cycle} | Mode: ${mode}`;
      }

      function renderSchedulerAlert(h) {
        const alert = document.getElementById("schedulerAlert");
        if (!h || !h.available) {
          alert.style.display = "block";
          alert.textContent = "Scheduler alert: heartbeat unavailable.";
          return;
        }
        if (h.healthy) {
          alert.style.display = "none";
          return;
        }
        const status = String(h.status || "unknown").toUpperCase();
        const age = Number(h.age_seconds ?? -1);
        const ageText = age >= 0 ? `${age}s` : "--";
        const detail = h.error ? ` | Error: ${h.error}` : "";
        alert.style.display = "block";
        alert.textContent = `Scheduler alert: ${status} | Heartbeat age: ${ageText}${detail}`;
      }

      async function refreshSchedulerHealth() {
        try {
          const resp = await fetch("/api/live-dashboard/scheduler-health");
          const health = await resp.json();
          renderSchedulerHealth(health);
          renderSchedulerAlert(health);
        } catch (err) {
          const fallback = { available: false, reason: err && err.message ? err.message : "fetch failed" };
          renderSchedulerHealth(fallback);
          renderSchedulerAlert(fallback);
        }
      }

      async function refresh() {
        const resp = await fetch("/api/live-dashboard");
        const data = await resp.json();
        const s = data.summary;

        document.getElementById("budget").textContent = fmt(s.working_budget);
        document.getElementById("equity").textContent = fmt(s.equity);
        const openCount = Number(s.open_positions_count || 0);
        const maxOpen = Number(s.max_open_positions || 0);
        document.getElementById("openCountTop").textContent = maxOpen > 0 ? `${openCount}/${maxOpen}` : String(openCount);
        document.getElementById("sellWinRateTop").textContent = fmtPct(s.sell_win_rate_pct);
        document.getElementById("netSellWinRateTop").textContent = fmtPct(s.net_sell_win_rate_pct);
        document.getElementById("payoffRatioTop").textContent = `${Number(s.payoff_ratio || 0).toFixed(2)}x`;
        document.getElementById("profitFactorTop").textContent = `${Number(s.profit_factor || 0).toFixed(2)}x`;
        document.getElementById("reserveTop").textContent = fmt(s.reserve_balance);
        document.getElementById("cash").textContent = fmt(s.cash);
        document.getElementById("deployableCash").textContent = fmt(s.deployable_cash);
        document.getElementById("openMv").textContent = fmt(s.open_positions_market_value);
        document.getElementById("unrealized").textContent = fmt(s.open_positions_unrealized_pl);
        document.getElementById("dayPnl").textContent = `${fmt(s.day_pnl)} (${fmtPct(s.day_pnl_pct)})`;
        colorPnL(document.getElementById("dayPnl"), s.day_pnl);
        colorPnL(document.getElementById("unrealized"), s.open_positions_unrealized_pl);

        const series = data.portfolio_series || [];
        equityChart.data.labels = series.map(p => fmtEtTime(p.t));
        equityChart.data.datasets[0].data = series.map(p => p.equity);
        equityChart.data.datasets[1].data = series.map(p => p.profit_loss);
        equityChart.update("none");
        const latestPoint = series.length ? fmtEtTime(series[series.length - 1].t) : "--";
        const refreshAt = fmtEtTime(data.as_of);
        document.getElementById("chartMeta").textContent = `Last refresh: ${refreshAt} ET | Latest point: ${latestPoint} ET`;

        renderPositions(data.open_positions || []);
        renderFills(data.recent_fills || []);
        renderSessionMetrics(data, s);
        renderFearClimate(data.fear_climate || {});
        document.getElementById("reserveStatus").textContent =
          `Reserve pot: ${fmt(s.reserve_balance)} | Pending request: ${fmt(s.reserve_target_request)}`;
        await refreshExitManagerHealth();
        await refreshSchedulerHealth();
      }

      async function refreshKraken() {
        const statusEl = document.getElementById("krakenStatus");
        let k;
        try {
          const resp = await fetch("/api/kraken-dashboard");
          k = await resp.json();
        } catch (err) {
          statusEl.textContent = "Kraken API fetch error: " + err.message;
          return;
        }
        if (!k.available) {
          statusEl.textContent = "Kraken not available: " + (k.reason || "unknown");
          return;
        }
        statusEl.textContent = "";

        document.getElementById("kUsd").textContent = fmt(k.usd_balance);
        document.getElementById("kEquity").textContent = fmt(k.equity);
        document.getElementById("kMargin").textContent = fmt(k.free_margin);
        document.getElementById("kOpenOrders").textContent = String(k.open_orders.length);
        document.getElementById("kTradeCount").textContent = String(k.recent_trades.length);

        const priceBody = document.querySelector("#krakenPricesTable tbody");
        priceBody.innerHTML = "";
        const pairs = Object.entries(k.live_prices || {}).sort((a, b) => b[1] - a[1]);
        for (const [pair, price] of pairs) {
          const tr = document.createElement("tr");
          tr.innerHTML = `<td>${pair}</td><td>${price >= 100 ? fmt(price) : "$" + Number(price).toFixed(4)}</td>`;
          priceBody.appendChild(tr);
        }

        const holdBody = document.querySelector("#krakenHoldingsTable tbody");
        holdBody.innerHTML = "";
        for (const h of k.holdings) {
          const tr = document.createElement("tr");
          const priceStr = h.price ? (h.price >= 100 ? fmt(h.price) : "$" + Number(h.price).toFixed(4)) : "--";
          tr.innerHTML = `<td>${h.symbol || h.asset}</td><td>${Number(h.qty).toFixed(6)}</td><td>${priceStr}</td><td>${fmt(h.value_usd)}</td>`;
          holdBody.appendChild(tr);
        }

        const ordBody = document.querySelector("#krakenOrdersTable tbody");
        ordBody.innerHTML = "";
        for (const o of k.open_orders) {
          const tr = document.createElement("tr");
          const sideClass = o.side === "buy" ? "sideBuy" : "sideSell";
          tr.innerHTML = `<td style="font-size:10px;">${o.id}</td><td>${o.description}</td><td>${Number(o.volume).toFixed(6)}</td><td>${Number(o.filled).toFixed(6)}</td><td>${o.status}</td>`;
          ordBody.appendChild(tr);
        }

        const trBody = document.querySelector("#krakenTradesTable tbody");
        trBody.innerHTML = "";
        for (const t of k.recent_trades) {
          const tr = document.createElement("tr");
          const sideClass = t.side === "buy" ? "sideBuy" : "sideSell";
          tr.innerHTML = `<td>${t.pair}</td><td><span class="sideTag ${sideClass}">${t.side.toUpperCase()}</span></td><td>${fmt(t.price)}</td><td>${Number(t.volume).toFixed(6)}</td><td>${fmt(t.cost)}</td><td>${fmt(t.fee)}</td>`;
          trBody.appendChild(tr);
        }
      }

      refresh();
      refreshKraken();
      setInterval(refresh, 5000);
      setInterval(refreshKraken, 5000);
    </script>
  </body>
</html>
"""


_kraken_cache: dict[str, Any] = {"payload": None, "ts": 0.0}
_KRAKEN_CACHE_TTL = 60.0  # seconds between Kraken API polls

def _build_kraken_payload() -> dict[str, Any]:
    if not kraken_client:
        return {"available": False, "reason": "Kraken not configured"}
    now = time.time()
    if _kraken_cache["payload"] and (now - _kraken_cache["ts"]) < _KRAKEN_CACHE_TTL:
        return _kraken_cache["payload"]
    try:
        balance = kraken_client._private("Balance")
        time.sleep(1.5)
        trade_bal = kraken_client._private("TradeBalance", {"asset": "ZUSD"})
        time.sleep(1.5)
        open_orders_raw = kraken_client._private("OpenOrders").get("open", {})
        time.sleep(1.5)
        trades_raw = kraken_client._private("TradesHistory").get("trades", {})
    except Exception as err:
        if _kraken_cache["payload"]:
            return _kraken_cache["payload"]
        return {"available": False, "reason": str(err)}

    usd_balance = _to_float(balance.get("ZUSD", balance.get("USD", 0)))
    equity = _to_float(trade_bal.get("eb", usd_balance))
    free_margin = _to_float(trade_bal.get("mf", usd_balance))

    _KRAKEN_ASSET_MAP = {
        "XXBT": "BTC/USD", "XETH": "ETH/USD", "XLTC": "LTC/USD",
        "XXRP": "XRP/USD", "XXDG": "DOGE/USD", "XDG": "DOGE/USD",
        "SOL": "SOL/USD", "DOT": "DOT/USD", "LINK": "LINK/USD",
        "AVAX": "AVAX/USD", "UNI": "UNI/USD", "AAVE": "AAVE/USD",
        "BCH": "BCH/USD", "SHIB": "SHIB/USD", "MKR": "MKR/USD",
        "GRT": "GRT/USD", "BAT": "BAT/USD", "CRV": "CRV/USD",
        "SUSHI": "SUSHI/USD", "ALGO": "ALGO/USD", "BABY": "BABY/USD",
    }
    holdings: list[dict[str, Any]] = []
    skip_assets = {"ZUSD", "USD", "USD.HOLD", "USDG"}
    for asset, qty_str in balance.items():
        qty = _to_float(qty_str)
        if qty <= 0 or asset in skip_assets:
            continue
        friendly = _KRAKEN_ASSET_MAP.get(asset, asset)
        price = None
        try:
            price = kraken_client.get_latest_price(friendly)
        except Exception:
            pass
        holdings.append({
            "asset": asset,
            "symbol": friendly,
            "qty": qty,
            "price": price,
            "value_usd": qty * (price or 0),
        })

    open_orders: list[dict[str, Any]] = []
    for txid, info in open_orders_raw.items():
        descr = info.get("descr", {})
        open_orders.append({
            "id": txid,
            "pair": descr.get("pair", ""),
            "side": descr.get("type", ""),
            "order_type": descr.get("ordertype", ""),
            "price": descr.get("price", "0"),
            "volume": info.get("vol", "0"),
            "filled": info.get("vol_exec", "0"),
            "status": info.get("status", ""),
            "description": descr.get("order", ""),
        })

    recent_trades: list[dict[str, Any]] = []
    for txid, info in sorted(trades_raw.items(), key=lambda x: _to_float(x[1].get("time", 0)), reverse=True)[:30]:
        recent_trades.append({
            "id": txid,
            "pair": info.get("pair", ""),
            "side": info.get("type", ""),
            "price": _to_float(info.get("price")),
            "volume": _to_float(info.get("vol")),
            "cost": _to_float(info.get("cost")),
            "fee": _to_float(info.get("fee")),
            "time": info.get("time"),
        })

    top_prices: dict[str, float] = {}
    try:
        top_syms = ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD", "AVAX/USD", "LINK/USD"]
        top_prices = kraken_client.get_crypto_latest_prices(top_syms)
    except Exception:
        pass

    result = {
        "available": True,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "usd_balance": usd_balance,
        "equity": equity,
        "free_margin": free_margin,
        "holdings": holdings,
        "open_orders": open_orders,
        "recent_trades": recent_trades,
        "live_prices": top_prices,
    }
    _kraken_cache["payload"] = result
    _kraken_cache["ts"] = time.time()
    return result


@app.get("/")
def index() -> str:
    return render_template_string(LIVE_HTML)


@app.get("/api/live-dashboard")
def api_live_dashboard():
    return jsonify(_build_dashboard_payload())


@app.post("/api/live-dashboard/reserve-target")
def api_set_reserve_target():
    payload = request.get_json(silent=True) or {}
    target = max(_to_float(payload.get("target"), 0.0), 0.0)
    state = reserve_store.set_target(target)
    return jsonify({"ok": True, "reserve_state": state})


@app.post("/api/live-dashboard/reserve-recirculate")
def api_recirculate_reserve():
    state = reserve_store.recirculate()
    return jsonify({"ok": True, "reserve_state": state})


@app.get("/api/kraken-dashboard")
def api_kraken_dashboard():
    return jsonify(_build_kraken_payload())


@app.get("/api/fear-climate")
def api_get_fear_climate():
    return jsonify(load_fear_climate_state())


@app.post("/api/fear-climate")
def api_set_fear_climate():
    payload = request.get_json(silent=True) or {}
    enabled = bool(payload.get("enabled", False))
    state = set_fear_climate_enabled(enabled)
    return jsonify({"ok": True, "fear_climate": state})


@app.post("/api/live-dashboard/kill-switch")
def api_kill_switch():
    result = _kill_scheduler_processes()
    return jsonify(result), (200 if result.get("ok") else 500)


@app.get("/api/live-dashboard/exit-manager-health")
def api_exit_manager_health():
    return jsonify(_load_exit_manager_health())


@app.get("/api/live-dashboard/scheduler-health")
def api_scheduler_health():
    return jsonify(_load_scheduler_health())


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8790, debug=False)
