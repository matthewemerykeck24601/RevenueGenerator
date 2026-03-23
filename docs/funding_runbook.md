# Funding Runbook (Live Account)

This runbook is for moving from paper to live with minimal operational risk.

## Preconditions

- Paper trading trial completed (see `docs/paper_trading_trial.md`)
- Risk policy finalized (`config/risk_policy.json`)
- Manual kill switch process documented

## Bank Link and Transfer Setup

- [ ] Link bank account in Alpaca dashboard
- [ ] Verify ACH and/or wire details
- [ ] Confirm transfer limits and cut-off times
- [ ] Confirm expected settlement times

## Test Deposit Process

1. Initiate a small test deposit (example: $25-$100).
2. Wait for transfer status to settle/complete.
3. Verify reflected buying power in account dashboard.
4. Record timestamps and any delays.

## Production Deposit Process

1. Deposit planned initial live budget.
2. Confirm funds settled and available.
3. Enable live mode only for a constrained subset of symbols.
4. Cap first-day max risk below policy default.

## Rollback / Incident Response

- If transfer mismatch or latency exceeds expectations:
  - pause live execution
  - verify transfer status in dashboard
  - reconcile expected vs posted balance
- If unresolved:
  - keep trading disabled
  - contact Alpaca support and keep ticket reference
