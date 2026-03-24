# revenueGenerator

AI-assisted trading app scaffold for Alpaca.

This repository starts with operational checklists and safety-first defaults so you can:

- set up Alpaca account access
- validate paper trading credentials
- define risk controls before enabling live funds
- run a paper-trading proving period

## Quick Start

1. Copy `.env.example` to `.env` and fill in paper credentials.
2. Run `python scripts/validate_alpaca_connection.py`.
3. Install deps: `pip install -r requirements.txt`
4. Run dry-run strategy:
   - `python scripts/run_paper_bot.py --segment largeCapStocks --budget 2000`
5. Place real paper orders (when ready):
   - `python scripts/run_paper_bot.py --segment largeCapStocks --budget 2000 --execute`
6. Start recurring scheduler:
   - `python scripts/run_scheduler.py --segment largeCapStocks --budget 2000 --interval 300`
7. Multi-sector cadence scheduler (penny fast, crypto medium, index slower):
   - `python scripts/run_multi_sector_scheduler.py --budget 1000 --execute`
   - Scheduler now supports OpenClaw in-cycle AI decisions via `aiScheduler` in `config/risk_policy.json` with rule-engine fallback.
   - AI calls can be limited to U.S. market hours via `aiScheduler.marketHoursOnly` and ET window settings.
7. Start web control panel:
   - `python scripts/web_ui.py` then open `http://127.0.0.1:8787`
8. Run dynamic sell monitor (dry-run first):
   - `python scripts/run_exit_manager.py --interval 20`
9. Enable live exits once validated:
   - `python scripts/run_exit_manager.py --interval 20 --execute`
10. Launch live split-view dashboard:
   - `python scripts/live_dashboard.py` then open `http://127.0.0.1:8790`
11. Launch control/API bridge (includes OpenClaw AI endpoints):
   - `python scripts/web_ui.py` then use `http://127.0.0.1:8787`
12. Complete `docs/paper_trading_trial.md` before going live.
13. Run weekly performance review + bounded tuning suggestions:
   - `python scripts/weekly_review.py --days 7`
   - optional apply: `python scripts/weekly_review.py --days 7 --apply`
14. Run log-replay backtest with slippage assumptions:
   - `python scripts/replay_backtest.py --days 30 --starting-equity 10000`

## Repository Layout

- `docs/alpaca_account_setup_checklist.md` - end-to-end account setup tasks
- `docs/funding_runbook.md` - ACH/wire setup and deposit validation process
- `docs/paper_trading_trial.md` - 2-4 week paper trial execution guide
- `config/risk_policy.json` - medium-risk aggressive trading profile
- `config/risk_policy.example.json` - baseline risk constraints template
- `scripts/validate_alpaca_connection.py` - validates API connectivity using `/account`
- `scripts/run_paper_bot.py` - runs one strategy cycle with risk checks
- `scripts/run_scheduler.py` - recurring loop runner with interval control
- `scripts/run_multi_sector_scheduler.py` - runs sector cadence profile from risk policy
- `scripts/web_ui.py` - start/stop/status web interface
- `scripts/run_exit_manager.py` - spike/exit hook monitor (partial TP, trailing stop, break-even)
- `scripts/live_dashboard.py` - live split view (equity graph, positions, fills, budget and returns)
- `scripts/weekly_review.py` - strategy attribution + execution quality + bounded tuning suggestions
- `src/revenue_generator/ai_bridge.py` - OpenClaw signal generation + risk gate normalization
- `src/revenue_generator/external_research.py` - external market scanner and ranking
- `logs/cycles.csv` and `logs/trades.db` - cycle journaling outputs

## Safety Notes

- Do not store live API keys in development environments.
- Start with paper trading only.
- Enable live trading only after risk policy and trial criteria are met.
- This bot uses a momentum+volatility edge heuristic and dynamic position sizing by confidence.
- Equities orders can place bracket exits using `takeProfitPct` and `stopLossPct` from risk policy.
- Dynamic exits are configured under `exitHooks` in `config/risk_policy.json`.

## OpenClaw AI Bridge API

- `GET /api/ai/health` - integration health and requirements
- `POST /api/ai/analyze` - run OpenClaw local agent and return risk-gated plan
- `POST /api/ai/execute` - execute a provided signal after risk validation

Example analyze request:

- `POST http://127.0.0.1:8787/api/ai/analyze`
- body: `{"segment":"crypto","budget":1000}`

Important:

- OpenClaw must have a model provider key configured for `agent main` (Anthropic/OpenAI/etc), otherwise `/api/ai/analyze` returns an auth error.
