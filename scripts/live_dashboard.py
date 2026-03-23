from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template_string

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.config import build_runtime_config

app = Flask(__name__)
cfg = build_runtime_config()
client = AlpacaClient(cfg=cfg)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
    # Working budget = funded principal +/- realized/unrealized P/L, which is current equity.
    working_budget = equity

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

    recent_fills: list[dict[str, Any]] = []
    for o in orders:
        status = str(o.get("status", ""))
        filled_at = o.get("filled_at")
        filled_qty = _to_float(o.get("filled_qty"))
        filled_avg_price = _to_float(o.get("filled_avg_price"))
        if status not in {"filled", "partially_filled"} or not filled_at or filled_qty <= 0:
            continue
        recent_fills.append(
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
        "summary": {
            "equity": equity,
            "cash": cash,
            "buying_power": buying_power,
            "working_budget": working_budget,
            "day_start_equity": day_start_equity,
            "day_pnl": day_pnl,
            "day_pnl_pct": day_pnl_pct,
            "open_positions_count": len(open_positions),
            "open_positions_market_value": total_market_value,
            "open_positions_unrealized_pl": total_unrealized_pl,
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
      .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin-bottom: 12px; }
      .card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 10px; }
      .k { color: var(--muted); font-size: 11px; text-transform: uppercase; }
      .v { margin-top: 5px; font-size: 17px; font-weight: 700; }
      .grid { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(320px, 1fr); gap: 12px; min-height: calc(100vh - 240px); }
      .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 10px; display: flex; flex-direction: column; min-height: 0; }
      .panel h2 { margin: 0 0 8px; font-size: 15px; }
      .panelHeader { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; margin: 0 0 8px; }
      .panelHeader h2 { margin: 0; }
      .chartMeta { color: var(--muted); font-size: 12px; text-align: right; white-space: nowrap; }
      table { width: 100%; border-collapse: collapse; font-size: 12px; }
      th, td { padding: 6px; border-bottom: 1px solid #223056; text-align: right; }
      th:first-child, td:first-child { text-align: left; }
      th { position: sticky; top: 0; z-index: 2; background: #0f1830; }
      .good { color: var(--good); }
      .bad { color: var(--bad); }
      .buyRow { background: rgba(25, 211, 159, 0.08); }
      .sellRow { background: rgba(255, 91, 107, 0.08); }
      .sideTag { padding: 2px 6px; border-radius: 999px; font-size: 11px; font-weight: 700; display: inline-block; }
      .sideBuy { background: rgba(25, 211, 159, 0.2); color: var(--good); }
      .sideSell { background: rgba(255, 91, 107, 0.2); color: var(--bad); }
      .leftCol { display: grid; grid-template-rows: minmax(280px, 45%) minmax(220px, 1fr); gap: 12px; min-height: 0; }
      .rightCol { display: grid; grid-template-rows: minmax(260px, 1fr) minmax(220px, 1fr); gap: 12px; min-height: 0; }
      .panelBody { flex: 1; min-height: 0; }
      .scroll { height: 100%; overflow: auto; }
      #equityChart { width: 100% !important; height: 100% !important; }
      .metricGrid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      .metricItem { background: #0f1830; border: 1px solid #2b3b67; border-radius: 8px; padding: 8px; }
      .metricItem .label { color: var(--muted); font-size: 11px; }
      .metricItem .value { margin-top: 4px; font-size: 15px; font-weight: 700; }
      .miniBar { margin-top: 8px; height: 6px; background: #1c2a4f; border-radius: 999px; overflow: hidden; }
      .miniBar > div { height: 100%; background: linear-gradient(90deg, #63a4ff, #35d4a0); width: 0%; }
      @media (max-width: 1200px) {
        .grid { grid-template-columns: 1fr; min-height: auto; }
        .leftCol, .rightCol { min-height: auto; }
      }
    </style>
  </head>
  <body>
    <h1>RevenueGenerator Live Split View</h1>
    <p class="sub">Auto-refresh every 5s. Chart shows last 60 minutes (1-min points, ET).</p>

    <div class="cards">
      <div class="card"><div class="k">Budget (Funded +/- P/L)</div><div class="v" id="budget">$0</div></div>
      <div class="card"><div class="k">Balance (Equity)</div><div class="v" id="equity">$0</div></div>
      <div class="card"><div class="k">Cash</div><div class="v" id="cash">$0</div></div>
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
          <div class="panelBody">
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
      }

      async function refresh() {
        const resp = await fetch("/api/live-dashboard");
        const data = await resp.json();
        const s = data.summary;

        document.getElementById("budget").textContent = fmt(s.working_budget);
        document.getElementById("equity").textContent = fmt(s.equity);
        document.getElementById("cash").textContent = fmt(s.cash);
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
      }

      refresh();
      setInterval(refresh, 5000);
    </script>
  </body>
</html>
"""


@app.get("/")
def index() -> str:
    return render_template_string(LIVE_HTML)


@app.get("/api/live-dashboard")
def api_live_dashboard():
    return jsonify(_build_dashboard_payload())


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8790, debug=False)
