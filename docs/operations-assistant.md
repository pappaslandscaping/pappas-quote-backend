# Operations Assistant

Open `/operations.html` in YardDesk. All APIs below require an authenticated owner/admin who is not an employee. Source reads use the official Homeworks API. Review state and internal drafts are YardDesk overlays; this module contains no Homeworks mutations or customer sending operations.

## Delivered workflows

1. Office briefing: today's visits, crews, completion counts, and prioritized findings.
2. Scheduling: expected recurrence dates, biweekly parity, off-seasons, missing/duplicate visits, crew mismatches, past open work, and message cues for possible skip requests. Explicit skipped/cancelled occurrences count as intentional; nearby rescheduled occurrences suppress a missing-date flag. Unsupported patterns require manual calendar review.
3. Accepted work and snow readiness: included accepted service lines with `NO_EVENT`, current-season dispatch coverage, crew/billing settings, exact estimate/customer agreement matches, and recorded attachment status. A recorded upload is not a fresh property-media verification.
4. Billing: completed invoiceable visits without invoice links, unsent invoices, and possible duplicate sent invoices. Missing links are not proof of unbilled work. Monthly/seasonal billing and asynchronous invoice processing require review.
5. Follow-ups: fresh account/context reads, provider-ID message deduplication, explicit phone matching for office conversations, AI drafting with factual templates as fallback, and reviewed internal drafts. Source-context fingerprints block saving after account/conversation changes. Pending rewrites and edits clear review approval.
6. Pricing: included service-line unit rates, distinct latest customer/property/service/year comparisons, won/pending/declined separation, and a provisional midpoint only with at least three won samples. Whole estimate totals, tax, and estimate-level discounts are not presented as a service rate; missing property measurements remain explicit.
7. Crew reports: completed time-entry hours, coverage, comparable budget variance, property overruns, and route mileage. Running timers are excluded. Service value is not profit or collected revenue. Route previews compare existing order against an optimized proposal using the same stop set and first-stop pin, without saving routes.
8. Renewals: included accepted/invoiced service lines, source/target years, explicit percentage adjustments, fractional quantities, and current-owner/existing-target checks. Saving prepares an internal proposal; it does not publish, accept, invoice, or send a Homeworks estimate. Dates, descriptions, tax, and scope still need final review.

## Freshness and availability

The default visit window is 28 days before/after today in America/New_York, configurable to at most 93 total days. Invoice checks cover 90 days; message scans cover 14 days. Customer follow-up drafts check 30 days of SMS plus up to 99 source estimates/open invoices and explicitly matched office messages. Estimate comparison history is 550 days. Source-year renewal plans outside that coverage are rejected.

A review shares in-flight reads and caches source data for five minutes. Refresh bypasses completed cache entries. Each source has independent availability; failure is never silently treated as successful empty data. Top-level and nested collection limits fail visibly. Source findings are recomputed; saved review state only applies while its evidence fingerprint matches, and expired snoozes reopen.

The backend warms the read-only review once after startup and logs counts and source availability, without customer names or message content. This checks the real API and office overlay tables without creating operational records or blocking the health endpoint.

A separate daily `Pappas Office Briefing` ChatGPT automation starts November 19, 2026, around 8 a.m. America/New_York. It reads current Homeworks data and produces an owner briefing here. YardDesk itself refreshes on opening the assistant or pressing Refresh records; this release does not add another automatic sender or competing background billing process.

## Storage and endpoints

Additive tables `homeworks_operations_reviews` and `homeworks_operations_drafts` are created on first use. No operational source record is replaced by these tables.

- GET `/api/homeworks/operations`: complete source review and choices.
- POST `/api/homeworks/operations/refresh`: refresh the selected date window.
- POST `/api/homeworks/operations/review`: save reviewed/open/snoozed office state against fresh evidence.
- GET `/api/homeworks/operations/pricing`: service/property/city comparisons.
- GET `/api/homeworks/operations/renewals`: review candidate proposals.
- POST `/api/homeworks/operations/renewal-draft`: refresh and save one eligible internal proposal.
- POST `/api/homeworks/operations/followup-draft`: prepare an unsent draft with current context.
- POST `/api/homeworks/operations/save-followup`: recheck the source fingerprint and save the reviewed internal text.
- GET `/api/homeworks/operations/drafts`: latest 200 office drafts.
- POST `/api/homeworks/operations/route-preview`: query-only current/proposed route comparison.

## Validation and rollback

Run the portable Jest suite, Node-runner regressions, frontend typecheck/build, and hosted CI. Live checks should load all eight sections, confirm source availability and matching dates/counts, and prepare a draft/route preview without sending or changing operational records. Database additions are safe to retain on rollback; revert the feature commit to remove the navigation and routes.
