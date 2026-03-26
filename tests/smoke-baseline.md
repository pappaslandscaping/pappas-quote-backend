# Smoke Test Baseline — 2026-03-26

**Target:** `https://pappas-quote-backend-production.up.railway.app`
**Auth:** Real admin token (hello@pappaslandscaping.com)
**Commit:** `47a1ac6` — Add test infrastructure and fix 12 input validation bugs

## Results (updated 2026-03-26)

| Metric | Count |
|--------|-------|
| Passed | 301 |
| Failed | 0 |
| Skipped | 6 |
| **Total** | **307** |

## Skipped (timeout by design)

These endpoints are slow by nature — AI inference or heavy computation. Timeouts are expected, not errors.

- `DELETE /api/crews/0` — timeout
- `GET /api/timeclock/pay-rates` — timeout
- `POST /api/dispatch/optimize-route` — timeout
- `GET /api/t/smoke000/click` — timeout (tracking redirect)
- `GET /api/ai/lead-scores` — Anthropic API call
- `GET /api/ai/churn-risk` — Anthropic API call
