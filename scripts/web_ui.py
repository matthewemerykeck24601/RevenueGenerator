from __future__ import annotations

import json
import sys
from pathlib import Path

from flask import Flask, jsonify, redirect, render_template_string, request, url_for

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from revenue_generator.alpaca_client import AlpacaClient
from revenue_generator.ai_bridge import run_openclaw_analysis, validate_and_plan_signal
from revenue_generator.config import build_runtime_config, ensure_risk_policy
from revenue_generator.exit_manager import ExitManager
from revenue_generator.journal import TradeJournal
from revenue_generator.scheduler import BotScheduler, RunnerConfig

app = Flask(__name__)
cfg = build_runtime_config()
policy = ensure_risk_policy()
client = AlpacaClient(cfg=cfg)
journal = TradeJournal()
scheduler = BotScheduler(client=client, risk_policy=policy, journal=journal)
exit_manager = ExitManager(client=client, risk_policy=policy, journal=journal)
last_exit_result = {"message": "No exit checks run yet."}
last_ai_result = {"message": "No AI analysis run yet."}


HTML = """
<!doctype html>
<html>
  <head>
    <title>Revenue Generator Control</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; max-width: 820px; }
      fieldset { margin-bottom: 16px; }
      label { display: block; margin-top: 8px; }
      input, select { padding: 6px; width: 300px; }
      button { padding: 8px 12px; margin-right: 8px; margin-top: 12px; }
      pre { background: #111; color: #eee; padding: 12px; border-radius: 6px; }
      .warn { color: #b33; font-weight: 600; }
    </style>
  </head>
  <body>
    <h1>Revenue Generator Bot</h1>
    <p class="warn">Paper trading first. Use execute carefully.</p>
    <form method="post" action="{{ url_for('start') }}">
      <fieldset>
        <legend>Run Config</legend>
        <label>Segment</label>
        <select name="segment">
          <option value="largeCapStocks">largeCapStocks</option>
          <option value="pennyStocks">pennyStocks</option>
          <option value="crypto">crypto</option>
          <option value="indexFunds">indexFunds</option>
        </select>
        <label>Budget (USD)</label>
        <input name="budget" type="number" step="0.01" value="2000" />
        <label>Interval seconds</label>
        <input name="interval" type="number" value="300" />
        <label><input type="checkbox" name="execute" value="1" /> Execute real paper orders</label>
      </fieldset>
      <button type="submit">Start Scheduler</button>
    </form>
    <form method="post" action="{{ url_for('run_now') }}" style="display:inline-block;">
      <input type="hidden" name="segment" value="largeCapStocks" />
      <input type="hidden" name="budget" value="2000" />
      <button type="submit">Run Once (Dry)</button>
    </form>
    <form method="post" action="{{ url_for('stop') }}" style="display:inline-block;">
      <button type="submit">Stop Scheduler</button>
    </form>
    <div style="margin-top:12px;">
      <form method="post" action="{{ url_for('run_exit_dry') }}" style="display:inline-block;">
        <button type="submit">Run Exit Check (Dry)</button>
      </form>
      <form method="post" action="{{ url_for('run_exit_execute') }}" style="display:inline-block;">
        <button type="submit">Run Exit Check (Execute)</button>
      </form>
    </div>
    <h2>Status</h2>
    <pre>{{ status_json }}</pre>
    <h2>Last Exit Check</h2>
    <pre>{{ exit_status_json }}</pre>
    <h2>Last AI Result</h2>
    <pre>{{ ai_status_json }}</pre>
  </body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(
        HTML,
        status_json=json.dumps(scheduler.status(), indent=2),
        exit_status_json=json.dumps(last_exit_result, indent=2),
        ai_status_json=json.dumps(last_ai_result, indent=2),
    )


@app.post("/start")
def start():
    segment = request.form.get("segment", "largeCapStocks")
    budget = float(request.form.get("budget", "2000"))
    interval = int(request.form.get("interval", "300"))
    execute = request.form.get("execute") == "1"
    cfg_obj = RunnerConfig(
        segment=segment,
        budget=budget,
        interval_seconds=interval,
        execute=execute,
    )
    try:
        scheduler.start(cfg_obj)
    except RuntimeError:
        pass
    return redirect(url_for("index"))


@app.post("/stop")
def stop():
    scheduler.stop()
    return redirect(url_for("index"))


@app.post("/run-now")
def run_now():
    segment = request.form.get("segment", "largeCapStocks")
    budget = float(request.form.get("budget", "2000"))
    cfg_obj = RunnerConfig(segment=segment, budget=budget, interval_seconds=300, execute=False)
    scheduler.run_once_now(cfg_obj)
    return redirect(url_for("index"))


@app.post("/run-exit-dry")
def run_exit_dry():
    global last_exit_result
    last_exit_result = exit_manager.run_cycle(execute=False)
    return redirect(url_for("index"))


@app.post("/run-exit-execute")
def run_exit_execute():
    global last_exit_result
    last_exit_result = exit_manager.run_cycle(execute=True)
    return redirect(url_for("index"))


@app.get("/api/status")
def api_status():
    return jsonify(scheduler.status())


@app.get("/api/ai/health")
def api_ai_health():
    return jsonify(
        {
            "openclaw_gateway": "ws://127.0.0.1:18789",
            "analysis_mode": "openclaw_local_agent",
            "note": "Requires OpenClaw model auth profile key (Anthropic/OpenAI/etc) for agent turns.",
        }
    )


@app.post("/api/ai/analyze")
def api_ai_analyze():
    global last_ai_result
    payload = request.get_json(silent=True) or {}
    segment = str(payload.get("segment", "crypto"))
    budget = float(payload.get("budget", 1000.0))
    segment_cfg = (policy.get("allowedSegments") or {}).get(segment, {})
    allowed_symbols = segment_cfg.get("symbolsAllowlist") or []
    if not allowed_symbols:
        return jsonify({"ok": False, "error": f"No allowed symbols configured for segment '{segment}'."}), 400

    try:
        signal = run_openclaw_analysis(
            segment=segment,
            budget=budget,
            allowed_symbols=allowed_symbols,
            risk_policy=policy,
        )
    except Exception as err:
        last_ai_result = {"ok": False, "error": str(err)}
        return jsonify(last_ai_result), 500

    account = client.get_account()
    positions = client.get_open_positions()
    decision = validate_and_plan_signal(
        signal=signal,
        budget=budget,
        segment=segment,
        risk_policy=policy,
        account=account,
        open_positions=positions,
        client=client,
    )
    last_ai_result = {
        "ok": True,
        "signal": decision.normalized_signal,
        "allowed": decision.allowed,
        "reason": decision.reason,
        "planned_order": decision.planned_order,
    }
    return jsonify(last_ai_result)


@app.post("/api/ai/execute")
def api_ai_execute():
    global last_ai_result
    payload = request.get_json(silent=True) or {}
    segment = str(payload.get("segment", "crypto"))
    budget = float(payload.get("budget", 1000.0))
    signal = payload.get("signal")
    if not isinstance(signal, dict):
        return jsonify({"ok": False, "error": "Missing 'signal' object in request body."}), 400

    account = client.get_account()
    positions = client.get_open_positions()
    decision = validate_and_plan_signal(
        signal=signal,
        budget=budget,
        segment=segment,
        risk_policy=policy,
        account=account,
        open_positions=positions,
        client=client,
    )
    if not decision.allowed or not decision.planned_order:
        last_ai_result = {
            "ok": False,
            "signal": decision.normalized_signal,
            "allowed": False,
            "reason": decision.reason,
        }
        return jsonify(last_ai_result), 400

    planned = decision.planned_order
    try:
        placed = client.place_order(
            symbol=planned["symbol"],
            qty=planned["qty"],
            side=planned["side"],
            order_type=planned["order_type"],
            tif=planned["time_in_force"],
            limit_price=planned["limit_price"] if planned["order_type"] == "limit" else None,
        )
    except Exception as err:
        last_ai_result = {
            "ok": False,
            "allowed": True,
            "reason": decision.reason,
            "signal": decision.normalized_signal,
            "planned_order": planned,
            "error": str(err),
        }
        return jsonify(last_ai_result), 502
    last_ai_result = {
        "ok": True,
        "allowed": True,
        "reason": decision.reason,
        "signal": decision.normalized_signal,
        "planned_order": planned,
        "order_result": placed,
    }
    return jsonify(last_ai_result)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8787, debug=False)
