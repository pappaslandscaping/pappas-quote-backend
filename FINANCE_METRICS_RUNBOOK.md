# Finance Metrics Runbook

This runbook covers the Copilot-backed finance metrics that are surfaced in production:

- Home `Revenue This Month`
- Invoice page `Receivables Aging`

## Expected Normal Behavior

### Revenue This Month

Normal source values:

- `live_copilot`
  - The app fetched the current Copilot Revenue by Crew report successfully.
- `persisted_copilot_snapshot`
  - A same-window Copilot snapshot was reused.
  - This is acceptable during warm-cache behavior or transient Copilot fetch issues.
- `database_fallback`
  - Copilot revenue could not be used, so the app fell back to local paid invoice math.
  - This should be treated as an operational warning, not normal steady-state behavior.

The source is returned from `/api/finance/summary` at:

- `thisMonth.revenue_source`
- `thisMonth.revenue_as_of`
- `thisMonth.revenue_period_start`
- `thisMonth.revenue_period_end`

### Receivables Aging

Normal source values:

- `live_copilot`
  - The current Copilot aging payload was built from live Copilot invoice data.
- `persisted_copilot_snapshot`
  - The invoice page is using the last persisted Copilot aging snapshot.
  - This is acceptable during cache reuse or short-term Copilot fetch issues.
- `database_fallback`
  - Copilot aging could not be served, so the app fell back to local invoice aging.
  - This should be treated as an operational warning.

The source is returned from `/api/invoices/aging` at:

- `source`
- `as_of`

## What Fallback Means

### If revenue uses `database_fallback`

Interpretation:

- Copilot Revenue by Crew could not be used for the current request.
- The Home card is showing local paid-invoice math instead of Copilot-canonical collected revenue.

Check first:

1. Application logs for `Finance summary revenue fallback`
2. Whether `fetchCopilotCollectedRevenueSnapshot()` is failing to authenticate to Copilot
3. Whether the Copilot Revenue by Crew report structure changed
4. Whether the current snapshot window matches the current Eastern date

Critical parser assumption:

- The authoritative Copilot total comes from the `Revenue by Crew` report table.
- The parser expects a table with headers like `Crew | Apr-26 | Total`
- It expects the authoritative total row to have first cell `Total`

### If aging uses `database_fallback`

Interpretation:

- Copilot aging data was not available for the current request.
- The invoice page is showing local aging instead of the reconciled Copilot aging snapshot.

Check first:

1. Application logs for `Invoice aging fallback`
2. Whether the Copilot invoice list crawl is failing
3. Whether the persisted Copilot aging snapshot exists in `copilot_sync_settings`
4. Whether Copilot auth/session has expired

Critical parser/report assumption:

- The live aging path is based on Copilot invoice list data plus the reconciled bucket rules.
- Drift usually means Copilot fetch/auth issues before it means bucket math issues.

## Where To Inspect

Application/API:

- `GET /api/finance/summary`
- `GET /api/invoices/aging`

Production logs:

- `Finance summary revenue source`
- `Finance summary revenue fallback`
- `Invoice aging source`
- `Invoice aging fallback`

Persistence:

- table `copilot_sync_settings`
- keys:
  - `copilot_revenue_this_month_collected`
  - `copilot_invoice_last_aging`

## First Checks When Numbers Drift

1. Inspect the API source field before assuming the metric math is wrong.
2. If the source is fallback, treat it as a Copilot availability or parser problem first.
3. Confirm whether the persisted snapshot is current enough for the requested window.
4. If revenue drift is isolated:
   - inspect the Copilot Revenue by Crew report structure
   - confirm the bottom `Total` row still contains the collected total
5. If aging drift is isolated:
   - confirm the aging source is Copilot-backed
   - verify the persisted snapshot timestamp and fallback warnings in logs

## Residual Blind Spots

- `persisted_copilot_snapshot` can mask short-lived live Copilot failures unless logs are monitored.
- The app does not currently emit external alerts; visibility is via response fields, UI labels, and logs.
- Parser assumptions still depend on Copilot keeping the Revenue by Crew totals row and aging/invoice list structures stable.
