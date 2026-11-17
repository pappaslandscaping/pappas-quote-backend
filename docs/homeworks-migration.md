# Official Homeworks integration migration

YardDesk reads operational records from the official Homeworks GraphQL API. Existing numeric YardDesk customer, property, and invoice URLs use additive canonical-ID links and disposable mirrors. Office notes, route proposals, print notes, map pins, and communication history remain YardDesk overlays.

## Behavior

- Schedule and Dispatch preserve Homeworks route-stop order, crew assignments, custom locations, recurrence frequency, and explicit false invoiceable flags. Opening a board never applies a route template or changes remote records.
- Route saves and crew assignments use canonical event IDs, preflight the current visit set, and verify the stored result. Partial failures require a refresh rather than an automatic retry.
- Invoice lists, balances, aging, payments, reports, and mail confirmation use official records. Totals include fees once; fractional quantities remain intact. Payments are never imported twice or confused with invoice revenue.
- Mailed invoices use markInvoicesAsSent(SNAIL_MAIL), followed by stored-flag verification. This does not queue another delivery.
- Completion uses closeEvent only after explicit billing confirmation for an invoiceable visit. No second local invoice is generated. The live schedule remains read-only in screens without source write controls.
- Individual visit creation, customer/property edits, and crew management use official operations and readback. Recurring-series setup and unsupported edits are explicit Homeworks handoffs.
- Homeworks Review contains unsent invoices, past open visits, estimate changes, failed SMS, snow list reconciliation, and signed agreement attachment status. Loading these queues does not send messages, close work, or change invoices.
- Communication matching rejects ambiguous phone matches. Unified histories deduplicate by provider ID. Source failures are reported as unavailable. General call notes are explicitly YardDesk office notes because the API does not expose the customer call-note timeline.
- Signed PDFs attach to verified service properties. A persistent ledger blocks uncertain retries. Confirmation requires a stored property-media ID. Estimate media attachments are unavailable through the API.
- Retired local recurring-job, monthly-invoice, late-fee, ZIP dispatch, Square charging, and QuickBooks operational-import paths cannot create competing records. QuickBooks accounting outside YardDesk is unaffected.

## API limits and business review

| Workflow | Boundary |
| --- | --- |
| Snow roster | Reconcile accepted services and crew/billing decisions before creating or dispatching current-season templates. Prior-season lists remain untouched. |
| Collected tax | No authoritative collected-tax report is exposed. Historical scraped snapshots cannot generate new tax transfers. Review the Homeworks report manually. |
| Call notes | Office notes stay in YardDesk. No remote timeline write is claimed. Unsupported notifications and follow-ups are rejected. |
| Payments and cards | No create/record/charge operation is exposed. Use the secure Homeworks workflow. |
| Recurring series | General create/update/cancel is unavailable. Route-specific recurrence uses the supported route operation. |
| Signed estimate media | Property attachment is supported; estimate attachment is unavailable. Uncertain attempts require checking the property before another upload. |
| Delivery | Existing disabled backend email behavior remains disabled. Marked sent and actual delivery remain separate. |
| Identity conflicts | Conflicting invoice numbers or ambiguous contact/property links stop reassignment. Resolve linkage before using the affected screen. |

## Release checks

Run `.github/workflows/ci-health.yml`, including the Node-runner suites and frontend typecheck/build. The managed local environment cannot start listening HTTP servers; those suites must run in hosted CI. They remain enabled in the workflow.

Before production cutover, verify runtime Homeworks credentials and the additive schema migration, compare representative balances and invoice totals, open Schedule and Dispatch for the same date, and test a reviewed route save/readback on a non-production visit set. Do not use real invoice sending, card charges, or mass completion as smoke tests. Confirm both Railway services reach SUCCESS and application health responds before reporting a release.

## Rollback

Keep the pre-migration production commit available. Schema additions are additive; retain canonical links, action receipts, and attachment ledgers. Remote Homeworks changes are not undone by deploying earlier code. Do not resume retired billing automations without reviewing duplicate risk.
