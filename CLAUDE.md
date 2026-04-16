# YardDesk — Pappas & Co. Landscaping

Business management app for Pappas & Co. Landscaping in Cleveland, OH. Service areas: Lakewood, Brook Park, Bay Village, and Westpark.

---

## Quick Start

```bash
npm run migrate # run core database migrations/bootstrap without starting the server
npm run bootstrap # explicit one-shot DB bootstrap helper
npm run dev     # nodemon with auto-reload (local development)
npm start       # direct node server.js (production)
```

Requires Node ≥18.0.0. Environment variables in `.env` (never commit to git).

**Production:** Railway auto-deploys on `git push origin main`. URL: `https://pappas-quote-backend-production.up.railway.app/`

**Database:** PostgreSQL on Railway (same schema as local). Local: `postgresql://theresapappas@localhost:5432/yarddesk`

**IMPORTANT: Before ANY `pg_dump` import to production**, back up the production DB first:
```bash
pg_dump $RAILWAY_DB_URL > backup-$(date +%Y%m%d).sql
```
A past import wiped active customer tokens and broke confirm links.

---

## Design & Operations Philosophy

- **Look like Jobber** — clean, professional UI. Card layouts, consistent tables, green brand palette.
- **Operate like home.works** — one connected system where work flows automatically: lead → estimate → accepted → scheduled → completed → invoiced → paid.
- Everything should feel like one app, not separate pages.

### Brand
- Colors: forest green `#2e403d`, lime `#c9dd80`
- Font: DM Sans (Google Fonts) — use DM Sans consistently everywhere. Do NOT use Qualy or any other font.
- Company: **Pappas & Co. Landscaping** — never shorten to "Pappas & Co." or just "Pappas"
- Tim is the customer-facing name — use his name (not Theresa's) in all customer-facing texts, emails, and communications

---

## Tech Stack & Versions

| Layer | Tech | Version |
|-------|------|---------|
| Runtime | Node.js | ≥18.0.0 |
| Server | Express | 4.18.2 |
| Database | PostgreSQL via `pg` | 8.11.3 |
| Auth | JWT + bcryptjs | 9.0.0 / 3.0.3 |
| Payments | Square SDK | 44.0.0 |
| SMS/Voice | Twilio | 4.19.0 |
| AI | Anthropic SDK | 0.78.0 |
| Accounting | Intuit OAuth (QuickBooks) | 4.2.2 |
| Email | Resend (via fetch) | — |
| PDF | pdf-lib + fontkit | 1.17.1 / 1.1.1 |
| File uploads | multer | 1.4.5-lts.1 |
| Rate limiting | express-rate-limit | 8.3.1 |
| Dev | nodemon | 3.0.2 |

No build tools, bundlers, transpilers, or preprocessors. Pure vanilla stack.

---

## Architecture

### Backend: Single Monolith (`server.js` — ~16,600 lines)

All backend logic lives in one file. No route splitting or module system. The file is organized in sections:

| Section | Lines (approx) | What it does |
|---------|----------------|-------------|
| Imports & init | 1–250 | Dependencies, SDK setup, security helpers |
| Config & constants | 251–340 | PORT, JWT_SECRET, SERVICE_DESCRIPTIONS (39 service types) |
| Middleware | 340–375 | CORS, security headers, rate limiting, static files |
| Auth | 375–650 | JWT auth, admin users, login/logout, password reset |
| Customer endpoints | — | CRUD, search, dedup, timeline, stats (13 endpoints) |
| Job/Scheduling | — | CRUD, recurring, completion, dispatch, route optimization (15+ endpoints) |
| Quotes | — | AI generation, send, sign, PDF, 4-stage followups (12+ endpoints) |
| Invoices | — | CRUD, PDF, recurring billing, late fees (10+ endpoints) |
| Payments | — | Square processing, webhooks, card-on-file (8+ endpoints) |
| Properties | — | CRUD, photos, service history (8+ endpoints) |
| Crews/Employees | — | Management, performance, permissions (5+ endpoints) |
| Campaigns/Broadcasts | — | Marketing email/SMS, landing pages, tracking (11+ endpoints) |
| QuickBooks | — | OAuth flow, sync customers/invoices/expenses (7 endpoints) |
| AI Services | — | Lead scoring, churn risk, social media, revenue forecast (10+ endpoints) |
| Reports | — | Financial, crew performance, sales tax, job costing (6+ endpoints) |
| Customer Portal | — | Self-service: invoices, estimates, reviews, work requests (10+ endpoints) |
| Cron/Automations | — | Recurring jobs, monthly invoices, late fees, followups (6+ endpoints) |
| Communications | — | Twilio SMS/voice, email log, push notifications (15+ endpoints) |
| Settings | — | Business profile, tax config, templates, processing fees (8+ endpoints) |
| Startup | End of file | Table creation, pool init, listen, graceful shutdown |

**Total: ~349 API endpoints, 48+ database tables, 100+ helper functions.**

### Startup Schema Path

- Core startup schema/bootstrap logic now lives in `lib/startup-schema.js`
- Reusable scripts:
  - `npm run migrate`
  - `npm run bootstrap`
- `server.js` still performs app boot, but the large database startup mutation path is no longer defined inline there

### Frontend: 56 Vanilla HTML Pages in `/public/`

- **`shell.js`** (665 lines) — shared auth, sidebar nav, permissions, quick-create menu, global search
- **`shared.css`** (1,272 lines) — all shared styles, brand variables, component classes
- **45 internal pages** use shell.js; **11 public pages** do not
- All page JS is inline `<script>` at end of body — no separate JS utility files

### Shell System (`public/shell.js`)

All internal pages share a common shell. Every page just needs:
```html
<body>
<script src="shell.js"></script>
```
- shell.js provides: JWT auth bootstrap, sidebar nav, quick-create menu, global search, sidebar collapse persistence
- Auto-wraps `window.fetch` to inject Bearer token on all `/api/*` requests and auto-logout on 401
- Do NOT add inline auth script blocks
- Do NOT add `<aside class="sidebar">` manually
- shell.js looks for `.main`, `.main-area`, `.main-content`, or `<main>` as the content wrapper
- Topbar is kept per-page (shell adds one if missing)
- Permission system: `window.YardDesk.hasPageAccess()`, `YardDesk.permissions`, `YardDesk.isEmployee`

### Public Pages (NO shell.js)
These pages are customer-facing. Do NOT add shell.js or auth to these:
login, pay-invoice, customer-portal, sign-quote, sign-contract, monthly-plan-request, confirm-services, campaign, quote-generator, quote-calculator, reset-password, crew-photos, unsubscribe

---

## Data Flow

```
Customer arrives via:
  Campaign landing page → campaign_submissions
  Quote request form → sent_quotes
  Manual entry → customers

Quote flow:
  AI generates quote → sent_quotes → email/SMS to customer
  → 4-stage auto-followup (days 1, 3, 5, 7)
  → Customer signs → CopilotCRM sync + contract PDF
  → "Create Job" / "Create Invoice" buttons appear

Job flow:
  scheduled_jobs → dispatch/scheduling views
  → Recurring jobs auto-generated via daily cron
  → Job completed → auto-creates invoice

Invoice flow:
  invoices (line_items as JSONB) → email/SMS to customer
  → Customer pays via Square (card/ACH)
  → Square webhook confirms → payments table
  → Syncs to QuickBooks

All entities link back to customers via customer_id.
```

### Workflow Connections
- Quote Request → "Create Quote" → quote-generator (pre-filled)
- Signed Quote → "Create Job" + "Create Invoice" buttons
- Completed Job → "Create Invoice" / "View Invoice"
- Customer Detail → dropdown with Quick actions (Quote, Job, Invoice)
- Detail pages have breadcrumbs and cross-link to related records
- Dashboard pipeline links to filtered list views (e.g., `quotes.html?status=new`)

---

## Database

### Key Tables

| Table | Key Columns | Notes |
|-------|------------|-------|
| `customers` | id, name, first_name, last_name, email, phone, mobile, street, city, state, postal_code | See name fallback chain below |
| `scheduled_jobs` | customer_id, customer_name, service_type, service_frequency, service_price, address | Jobs/scheduling |
| `invoices` | customer_id, customer_name, line_items (JSONB), status (draft/sent/paid/overdue) | `processing_fee` columns for pass-through |
| `payments` | invoice_id, square_payment_id, amount, method | Square-linked |
| `sent_quotes` | customer_id, services (JSONB), status, token | Quote tracking |
| `business_settings` | key, value (JSONB) | App-wide config |
| `employees` | name, permissions | Crew/staff with permission flags |
| `campaigns` | slug, form_heading, form_fields | Marketing with public landing pages |
| `season_kickoff_responses` | customer_id, confirmed, services | Annual service confirmations |

48+ tables total — all created via `CREATE TABLE IF NOT EXISTS` scattered throughout server.js.

### Customer Name Fallback Chain
The `customers` table has `name`, `first_name`, `last_name` with inconsistent population. Always use:
```js
c.name || ((c.first_name || '') + (c.last_name ? ' ' + c.last_name : '')).trim() || 'Unknown'
```

### Database Patterns
- All queries use parameterized `$1, $2` format — never interpolate variables into SQL
- JSONB for complex data: invoice line items, business settings, service details
- `FOR UPDATE` locking for sequential ID generation (invoice numbers, customer numbers)
- Schema evolution via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` wrapped in try-catch
- Table init at startup via async IIFE (`ensureInvoicesTable()`, `ensureQuoteEventsTable()`, etc.)

---

## Key API Endpoints

| Endpoint | Auth | Notes |
|----------|------|-------|
| `GET /api/customers` | Yes | Returns `{ customers: [...] }` |
| `GET /api/services` | **No** (public) | Returns predefined SERVICE_DESCRIPTIONS |
| `GET /api/crew` | Yes | Crew list |
| `POST /api/jobs` | Yes | Creates job in scheduled_jobs |
| `POST /api/jobs/:id/setup-recurring` | Yes | Sets up recurring schedule |
| `PATCH /api/jobs/:id/complete` | Yes | Marks complete + auto-invoices |
| `POST /api/webhooks/square` | No (signature verified) | Raw body, before express.json() |
| `POST /api/sms/webhook` | No | Twilio inbound |
| `POST/GET /api/cron/daily-automation` | No | Hit by cron-job.org |
| `GET /api/sign/:token` | No (token auth) | Customer quote signing |
| `GET /api/pay/:token` | No (token auth) | Customer invoice payment |
| `GET /api/portal/:token/*` | No (token auth) | Customer self-service portal |

---

## External Integrations

| Service | Purpose | Config |
|---------|---------|--------|
| **Square** | Card/ACH payments, webhooks, card-on-file | `SQUARE_*` env vars. SDK has version compatibility shims for v41+ API changes. |
| **Twilio** | SMS/voice. Two phone numbers: primary (440) and secondary (216) | `TWILIO_*` env vars |
| **QuickBooks** | OAuth sync: customers, invoices, expenses, payments | `QB_*` env vars. Tokens stored in `qb_tokens` table with auto-refresh. |
| **Anthropic Claude** | AI quote generation, followup emails, social media, lead scoring | `ANTHROPIC_API_KEY`. Graceful degradation if not set. |
| **Resend** | Transactional email delivery | `RESEND_API_KEY`. From: `hello@pappaslandscaping.com` |
| **Google Maps** | Geocoding, route optimization | `GOOGLE_MAPS_API_KEY`. Falls back to Nominatim. |
| **CopilotCRM** | Contract sync on quote signing | `COPILOTCRM_USERNAME/PASSWORD`. Reverse-engineered internal API. See protected features. |

### CopilotCRM Integration (DO NOT REMOVE)
- **Contract signed → CopilotCRM sync** lives in the `POST /api/sent-quotes/:id/sign-contract` handler
- When a contract is signed: logs into CopilotCRM API, finds customer, matches estimate by quote number, marks estimate as accepted, uploads signed contract PDF
- **Backfill endpoint:** `POST /api/copilotcrm/backfill-contract` with `{ customer_name }`
- **NEVER replace this code with a Zapier webhook.** It was accidentally removed once before and broke the integration.

---

## How We Work
- Plan before coding. Output: assumptions, risks, file-level plan, test plan. No code until approved.
- Keep changes small. Touch minimum files per step. Keep diffs under 100 lines when possible.
- Ask before touching anything outside the direct scope of the task.
- Never refactor while fixing a bug. One thing at a time.
- After every change, tell me what you changed and what to test.

### Changelog
- Update `CHANGELOG.md` whenever you complete substantive work (new features, bug fixes, UI changes)
- Write from the user's perspective — what changed, not how it was implemented

---

## Testing
- Smoke test lives at `tests/smoke.js` — covers all 299 endpoints
- Run with: `node tests/smoke.js`
- Accepts `AUTH_TOKEN` env var or mints its own via `JWT_SECRET`
- Run this after any change to server.js before considering the task done
- **Baseline (2026-03-26):** 293 passed, 0 failed, 6 expected timeouts (AI endpoints + route optimization). Any future run should meet or beat this. Full baseline at `tests/smoke-baseline.md`.

## Testing Rules
- After every change, tell me what to manually test and how.
- If adding a new endpoint, write a smoke test for it at the same time.
- Never mark a task complete without a test plan or manual verification steps.
- Before touching any protected feature, confirm the relevant test still passes after.

---

## Single File Warning

server.js is 16,644 lines. Search carefully before adding anything new.
Never duplicate a route or helper that already exists. Ask if unsure.

---

## Protected Features — NEVER REMOVE OR MODIFY WITHOUT EXPLICIT PERMISSION
- CopilotCRM sync inside quote-signing handler
- Season kickoff flow
- Invoice payment processing
- Customer portal
- All Twilio webhooks
- Square webhook (must stay BEFORE express.json() — raw body required)
- Daily automations (cron endpoints)
- QuickBooks sync

### NEVER Send Communications Without Permission
**NEVER send any email, text, SMS, voice call, or any outgoing communication to real customers without explicit permission from Theresa.** This includes:
- Apology emails, test emails, and any automated sends
- Triggering endpoints that send via Resend, Twilio, or any external messaging service
- Running cron/automation endpoints that process followups, late fees, or bulk sends
- Running smoke tests against endpoints that send real communications

If a task involves testing a send endpoint, use mock/dry-run mode or ask first. When in doubt, **ask before sending.**

---

## Gotchas — Read Before Every Task
- Square webhook order: MUST come before express.json(). Moving it breaks payment confirmations silently.
- Customer name fallback: always use `name || (first_name + ' ' + last_name) || 'Unknown'`
- SSL `rejectUnauthorized: false` is intentional for Railway PostgreSQL. Not a bug.
- All customer-facing communications use Tim's name, not Theresa's.
- Square SDK has manual shims for v41+ API changes. Check before touching Square code.
- No background workers — all scheduled tasks run via HTTP endpoints hit by cron-job.org. If the cron service stops, automations stop silently.
- Two Twilio phone numbers — primary +14408867318 and secondary +12169413737 with fallback logic.
- Duplicate table definitions (quote_views x4, social_media_posts x2) — harmless due to IF NOT EXISTS.

---

## Leave These Alone (Incomplete Migrations — Do Not Run or Remove)
- `ALTER TABLE scheduled_jobs ADD COLUMN start_time / end_time`
- `CREATE TABLE review_requests`
- `const laborRate = 35` (hardcoded intentionally for now)
- Dead Nodemailer import — leave it
- Google OAuth credentials in .env — unused, ignore

---

## Backend Coding Standards

### Security
- **Never return `error.message` to clients** — use `serverError(res, error)` for all 500 responses
- **Always `escapeHtml()` user input** before inserting into email templates
- **Always use parameterized queries** — never interpolate variables into SQL strings
- **Rate limiting** is applied to all public endpoints (login, quotes, sign, pay)

### Error Handling
- Use `async/await` consistently — never mix `.then()` chains with async functions
- Never silently swallow errors with `catch(e) {}` — at minimum `console.error()`
- Use `serverError(res, error, 'Context message')` for all catch blocks in routes

### API Responses
- Success: `{ success: true, data... }`
- Client errors (400/401/403/404): `{ success: false, error: 'Human-readable message' }`
- Server errors (500): Always use `serverError()` — generic message to client, full error to logs

---

## Auto-Update Memory (MANDATORY)

Update memory files AS YOU GO, not at the end. When you learn something new, update immediately.

| Trigger | Action |
|---------|--------|
| User shares a fact about themselves | → Update `.claude/memory/memory-profile.md` |
| User states a preference | → Update `.claude/memory/memory-preferences.md` |
| A decision is made | → Update `.claude/memory/memory-decisions.md` with date |
| Completing substantive work | → Add to `.claude/memory/memory-sessions.md` |

### Past Mistakes Tracking
- Maintain `.claude/memory/memory-mistakes.md` — log patterns where things went wrong
- Review this file at the start of every session to avoid repeating errors
- Format: `- [date] What went wrong → What to do instead`

---

## Target Operational Model (home.works-inspired)

Build toward these concepts incrementally:

| System | Purpose | Status |
|--------|---------|--------|
| Dispatch Board | Map + table, drag-and-drop, route optimization, bulk crew assignment | Future |
| Quick Dispatch | Templates for fast scheduling with reusable property/service combos | Future |
| Unscheduled Work | All accepted estimates not yet scheduled | Future |
| Waitlist | Parking spot for future work with date ranges + auto-tags | Future |
| Recurring Series | Auto-generates jobs, off-season pause, budgeted hours | Partial |
| Automations | Conditional triggers + multiple actions | Future |
| Billing Options | Per-job: Invoice services / Link to Level Billing / Do not Invoice | Future |
| Auto-send / Auto-charge | Invoices auto-send, auto-charge saved cards | Future |
| Customer Portal | Submit work requests, review estimates, pay invoices | Partial |
| Reports | Revenue Forecast, Sales Tax, Revenue by Customer, Lifetime Value | Partial |
| Price Book / Rate Matrix | Centralized pricing with matrix support | Future |
| Wiki | Internal knowledge base for crews | Future |
| Mobile Crew App | Offline-capable crew app (time tracking, media, forms) | Future |
