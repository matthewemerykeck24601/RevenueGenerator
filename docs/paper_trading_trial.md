# Paper Trading Trial (2-4 Weeks)

Use this to validate your strategy behavior before touching live funds.

## Trial Objectives

- Demonstrate stable execution and error handling
- Confirm risk policy enforcement
- Measure slippage, win rate, and drawdown behavior by segment

## Daily Checklist

- [ ] Bot starts cleanly and connects to Alpaca
- [ ] No rejected orders due to malformed payloads
- [ ] Position limits never exceeded
- [ ] Daily loss cap never breached
- [ ] Logs include all entries/exits and reasons

## Metrics to Track

- Total return %
- Max intraday drawdown %
- Win rate %
- Average gain/loss ratio
- Rejection/error rate
- Realized slippage vs expected

## Pass/Fail Gates

Pass criteria:

- No uncontrolled risk breaches for at least 10 consecutive trading days
- Strategy remains within max drawdown threshold
- Operational alerts and kill switch tested at least once

Fail criteria:

- Any day exceeds hard loss cap without auto-stop
- Repeated API/order failures without graceful recovery
- Metrics materially degrade under normal market conditions

## Go-Live Recommendation

Only proceed to live with small capital after pass criteria are met and reviewed.
