# Changelog

## 2026-04-18

### Tax Transfer Instruction Snapshot Lookup Fix
- Fixed transfer-instruction generation and yesterday-status reads to recognize persisted Copilot tax summary snapshots stored under the current `live_copilot` source
- This keeps snapshot-backed instruction generation aligned with the same persisted recommendation rows used after Tax Summary sync, without changing transfer math or recommendation source of truth

### Dispatch Execution Status Workflow Foundation
- Added additive dispatch execution fields on `scheduled_jobs` for started/completed metadata, proof-of-work coordinates, last-status tracking, and dispatch issue flags
- Added a canonical job status transition workflow with support for `pending`, `in_progress`, `completed`, `skipped`, and `cancelled`
- Added `PATCH /api/jobs/:id/status` for dispatch/crew status updates without changing existing board UI behavior
- Expanded `PATCH /api/jobs/:id/complete` to persist crew proof-of-work fields already being sent while preserving the existing invoice side effects and direct pending-to-completed flow

### Copilot Dispatch Execution Sync Mirror
- Added additive `copilot_*` mirror and provenance columns on `scheduled_jobs` so Copilot-authored execution state can be mirrored without changing YardDesk’s canonical execution or billing fields
- Added a protected manual `POST /api/copilot/dispatch-execution/sync` endpoint that fetches Copilot route status data, applies exact visit/job matching, supports `dry_run` and `force`, and updates only Copilot mirror fields
- Added stale-event and unchanged-payload hash protections so older or identical Copilot execution payloads are skipped instead of rewriting local rows

## 2026-04-17

### Tax Transfers Automation Health
- Added a read-only automation health panel on Tax Transfers showing the latest freshness-sync and transfer-instruction runs, last failures, next expected scheduled run times, and whether the most recent run was manual or scheduled
- Persisted transfer-instruction generation run history in the same snapshot-settings store used for freshness status so failed cron/manual runs remain visible on the dashboard

### Payments Display Normalization
- Payments now prefer customer-facing invoice references from normalized invoice metadata instead of falling back to raw Copilot internal invoice ids
- Legacy Copilot-linked rows with bad placeholder invoice labels now show `—` unless a real customer-facing invoice number is available

### Tax Transfer Instruction Workflow
- Added a phase-1 tax transfer instruction workflow on Tax Transfers for yesterday's Chase-to-Huntington sales tax move
- Added approval-only instruction generation, history, and audit fields without changing Copilot collected-tax recommendation logic
- Added cron-safe instruction generation that reads persisted Copilot tax snapshots only and never initiates bank transfers directly
- Added cutoff-aware Yesterday instruction alerts on Tax Transfers so missing, unapproved, and unsubmitted Chase steps are obvious before and after the Eastern-time deadline
- Added CSV export and exception reporting for transfer operations so accounting and ops can review missing, late, superseded, canceled, and changed-submission instruction states outside the page

### Tax Transfer Payment Reconciliation
- Fixed payment tax reconstruction to allocate against the taxed line-item gross total when available instead of the full invoice total
- This prevents non-taxable invoice surcharges and fee-style adjustments from understating backend reconstructed tax on Tax Transfers

### Daily Tax Transfer Freshness Sync
- Added a protected daily tax-transfer freshness sync endpoint for Copilot payments and Copilot Tax Summary collected snapshots
- Added persisted automation status plus same-day freshness indicators and failure messaging on the Tax Transfers page
- Excluded leaked Copilot `Page Total` footer rows from Tax Transfers reconciliation and added cleanup on payments sync

### Copilot Payment Review
- Added a read-only Copilot payment review section to Tax Transfer Reconciliation for inspecting unresolved payment rows by date range
- Added `GET /api/copilot/payment-review` to explain linkage failures without changing Copilot Tax Summary recommendation behavior
- Added a Reports quick link to Tax Transfer Reconciliation so the review flow is easier to reach

## 2026-04-15

### Startup Schema Extraction
- Added Railway-friendly database config fallback so `DATABASE_URL` can be derived from `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`
- Added reusable `npm run migrate` and `npm run bootstrap` scripts for core database startup/bootstrap work
- Extracted the large startup schema/bootstrap logic into `lib/startup-schema.js` so app boot and manual DB setup share the same code path

## 2026-04-14

### Route Sheet — Name & Address Parsing Fixes
- Addresses no longer get truncated when a city name appears inside the street (e.g., "14950 Lakewood Heights Boulevard", "1283 Westlake Avenue" — previously collapsed to just the house number)
- Personal names now use the surname only. "Mary Ann Marsal" → "Marsal", "Dallas Marie Holifield" → "Holifield" (previously included middle names)
- "Ready" removed from business-keyword list so "Martha Ready" → "Ready" instead of showing the full name
- Names with lowercase internal words (e.g., "Mews at Rockport") now render in full instead of being stripped to "at Rockport"
- Added "COA" to business-keyword list so "Kirtland House COA" renders in full
- Non-mowing service types (e.g., "Weed Whacking - Pavement Area") now appear in the Comments column instead of being dropped

## 2026-03-27

### Morning Briefing Endpoint
- New `POST /api/morning-briefing` endpoint assembles a daily summary and sends it to Telegram
- Sections: today's jobs by crew (from copilot_sync_jobs), past due invoices (live from CopilotCRM), Stripe failed payments (when configured)
- Briefing sent to Telegram via bot API; also returned in JSON response for N8N logging
- Protected with auth middleware — N8N uses existing service token

## 2026-03-20

### Contract Signing → Copilot Portal
- Replaced Square card-on-file step with redirect to Copilot client portal after contract signing
- Auto-sends branded portal invite email via Copilot's sendMail API as part of CopilotCRM sync
- Email emphasizes adding card on file with emoji-styled feature list (💳 Card on File, 📄 Quotes & Invoices, 📅 Service Schedule, 💬 Direct Messaging)
- Links directly to forgot-password page (`secure.copilotcrm.com/client/forget?co=5261`) so new customers skip the login step
- Removed Square Web Payments SDK from sign-contract.html

### Send Estimates by Text
- New `POST /api/sent-quotes/:id/send-sms` endpoint sends quote link via Twilio SMS
- SMS uses Tim's personal tone matching the Copilot "New Estimate" template
- "Send by Text" button added to sent-quote-detail.html and sent-quotes.html list view
- Timeline shows "💬 Sent by Text" label with phone number

### Quote Edit Modal — Editable Descriptions
- Each service row in the Edit Quote modal now has a description textarea
- Descriptions are fully editable before resending (previously always pulled from predefined list)
- Selecting a predefined service pre-fills the description; custom services start blank

### Topbar Buttons Fix
- Fixed invisible action buttons on sent-quote-detail.html (Edit, Resend, Send by Text, Download PDF, Delete)
- Root cause: `topbar-right` div was missing `display: flex` CSS

### Social Media AI
- Built `POST /api/social-media/generate` — Claude creates tailored posts for Facebook, Instagram, Nextdoor, TikTok, Google, and Twitter/X
- Each platform gets appropriate style (hashtags for IG, 280 chars for X, neighborly for Nextdoor)
- Built `POST /api/social-media/refine` — ongoing conversation to refine posts ("make it shorter", "add hashtags", "which is best?")
- Built `GET /api/social-media/history` — returns recent generated posts
- New `social_media_posts` table stores generation history
- Chat is now fully conversational — follow-ups refine in-place, say "new post" to start fresh
