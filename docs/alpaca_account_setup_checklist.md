# Alpaca Account Setup Checklist

Use this checklist to complete the account side before enabling AI-driven execution.

## 1) Account Verification and Agreements

- [ ] Complete identity verification (KYC)
- [ ] Submit tax documentation required by your account type
- [ ] Read and accept equities disclosures
- [ ] Read and accept crypto disclosures (if trading crypto)
- [ ] Record your account status and approval date

Evidence to keep:

- Screenshot/PDF showing account approved and active
- Date verification was completed

## 2) Product Permissions

- [ ] Confirm equities trading is enabled
- [ ] Enable crypto trading (if needed for your strategy)
- [ ] Keep options disabled for initial launch

## 3) Paper Trading Credentials

- [ ] Generate paper API key + secret
- [ ] Save credentials in password manager
- [ ] Configure `.env` from `.env.example`
- [ ] Run `python scripts/validate_alpaca_connection.py`
- [ ] Confirm `/v2/account` returns active account details

## 4) Live Credential Separation

- [ ] Do not place live keys into local dev environments
- [ ] Prepare separate secret storage location for live credentials
- [ ] Document key rotation process

## Exit Criteria

This checklist is complete when all items above are checked and validated.
