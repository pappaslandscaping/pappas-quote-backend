# YardDesk — Pappas & Co. Landscaping

Business management app for a landscaping company in Cleveland, OH.

## Design & Operations Philosophy
- **Look like Jobber** — clean, professional UI. Card layouts, consistent tables, green brand palette.
- **Operate like home.works** — one connected system where work flows automatically from lead → estimate → accepted → scheduled → completed → invoiced → paid, with minimal manual steps.
- Everything should feel like one app, not separate pages.

## Brand
- Colors: forest green `#2e403d`, lime `#c9dd80`
- Font: DM Sans (Google Fonts) — use DM Sans consistently everywhere. Do NOT use Qualy or any other font.
- Company: Pappas & Co. Landscaping, Cleveland OH

## Tech Stack
- **Backend:** Express.js (`server.js` — single file)
- **Frontend:** Vanilla HTML/CSS/JS in `/public/` (44+ pages, no framework)
- **Database:** PostgreSQL via `pool.query`
- **Auth:** JWT via `Authorization: Bearer` header
- **Shared styles:** `/public/shared.css` (1300+ lines) — use existing classes first
- **Shared shell:** `/public/shell.js` (492 lines) — auth, sidebar, nav
- **Run locally:** `npm run dev` (nodemon) or `npm start`
- **Integrations:** Square (payments), Twilio (SMS), Intuit OAuth (QuickBooks), Anthropic Claude (AI), Nodemailer (email)
- **Deployment:** Railway (production) — `git push origin main` auto-deploys
- **Production URL:** `https://pappas-quote-backend-production.up.railway.app/`
- **Railway DB:** PostgreSQL on Railway (same schema as local)
- **Deploy process:** Commit → push to GitHub → Railway auto-deploys code. For DB changes, export local with `pg_dump` and import via Railway's public Postgres URL.
- **IMPORTANT: Before ANY `pg_dump` import to production**, back up the production DB first: `pg_dump $RAILWAY_DB_URL > backup-$(date +%Y%m%d).sql`. A past import wiped active customer tokens and broke confirm links.

## Shell System (`public/shell.js`)
All internal pages share a common shell. Every page just needs:
```html
<body>
<script src="shell.js"></script>
```
- shell.js provides: JWT auth bootstrap, sidebar nav, quick-create menu, global search, sidebar collapse persistence
- Do NOT add inline auth script blocks
- Do NOT add `<aside class="sidebar">` manually
- shell.js looks for `.main`, `.main-area`, `.main-content`, or `<main>` as the content wrapper
- Topbar is kept per-page (shell adds one if missing)

### Public Pages (NO shell.js)
These pages are public-facing (customers access them without logging in). Do NOT add shell.js or auth to these:
login, pay-invoice, customer-portal, sign-quote, sign-contract, monthly-plan-request, confirm-services, campaign, quote-generator, quote-calculator, reset-password

## Database Tables
- `customers` — id, name, first_name, last_name, email, phone, mobile, street, city, state, postal_code
- `scheduled_jobs` — customer_name, customer_id, service_type, service_frequency, service_price, address
- `invoices` — customer_id, customer_name, line_items (JSONB), status (draft/sent/paid)

### Customer Name Pattern
```js
c.name || ((c.first_name||'')+(c.last_name?' '+c.last_name:'')).trim() || 'Unknown'
```

## Key API Endpoints
- `GET /api/customers` — requires auth
- `GET /api/services` — PUBLIC (no auth)
- `GET /api/crew` — requires auth
- `POST /api/jobs` — creates job
- `POST /api/jobs/:id/setup-recurring` — recurring schedule
- `PATCH /api/jobs/:id/complete` — marks complete + auto-invoices

## CopilotCRM Integration (DO NOT REMOVE)
- **Contract signed → CopilotCRM sync** lives in the `POST /api/sent-quotes/:id/sign-contract` handler
- Uses `COPILOTCRM_USERNAME` and `COPILOTCRM_PASSWORD` env vars (set in Railway)
- When a contract is signed: logs into CopilotCRM API, finds customer, matches estimate by quote number, marks estimate as accepted, uploads signed contract PDF
- **Backfill endpoint:** `POST /api/copilotcrm/backfill-contract` with `{ customer_name }` — manually triggers the same sync for a previously signed quote
- **NEVER replace this code with a Zapier webhook.** It was accidentally removed once before and broke the integration.

## Dashboard Structure (index.html)
- 4 stat cards: Revenue This Month, YTD Revenue, Outstanding Invoices, Total Customers
- Workflow pipeline bar: Requests → Quotes → Jobs → Invoices (clickable, with counts)
- 6-month revenue sparkline + Quote conversion rate card
- "Needs Your Attention" action alerts
- Today's Schedule with job cards
- 3-column workflow board (Requests, Quotes, Jobs with status breakdowns)
- Activity Feed + Recent Messages + Recent Customers
- AI Assistant floating panel (Claude-powered chat)
- Quick-create modals for Customer and Job

## Workflow Connections (existing)
- Quote Request → "Create Quote" → quote-generator (pre-filled)
- Signed Quote → "Create Job" + "Create Invoice" buttons
- Completed Job → "Create Invoice" / "View Invoice"
- Customer Detail → dropdown with Quick actions (Quote, Job, Invoice)
- Detail pages have breadcrumbs and cross-link to related records
- Dashboard pipeline links to filtered list views (e.g., `quotes.html?status=new`)

## Target Operational Model (home.works-inspired)
Build toward these concepts incrementally:

| System | Purpose | Status |
|--------|---------|--------|
| **Dispatch Board** | Map + table, drag-and-drop, route optimization, saved weekly routes, bulk crew assignment | Future |
| **Quick Dispatch** | Templates for fast scheduling with reusable property/service combos | Future |
| **Unscheduled Work** | Single page: all accepted estimates not yet scheduled. Group by estimate, customer, service | Future |
| **Waitlist** | Parking spot for future work with date ranges + auto-tags (Upcoming, Due Soon, Overdue) | Future |
| **Recurring Series** | Auto-generates jobs, off-season pause, budgeted hours tracking | Partial |
| **Automations** | Conditional triggers + multiple actions (e.g., job complete → SMS customer → send invoice) | Future |
| **Billing Options** | Per-job: Invoice services / Link to Level Billing / Do not Invoice | Future |
| **Auto-send / Auto-charge** | Invoices auto-send on completion, auto-charge saved cards | Future |
| **Customer Portal** | Branded — submit work requests, review estimates, pay invoices | Partial |
| **Reports** | Revenue Forecast, Sales Tax, Revenue by Customer, Lifetime Value | Partial |
| **Price Book / Rate Matrix** | Centralized pricing with matrix support | Future |
| **Wiki** | Internal knowledge base for crews | Future |
| **Mobile Crew App** | Offline-capable crew app (time tracking, media, forms) | Future |

## Rules

### NEVER Delete Working Code (MANDATORY)
**This is the #1 rule. Violations have broken customer-facing features in production.**

- **NEVER remove, replace, or "clean up" existing endpoint code** unless the user explicitly asks you to delete that specific feature
- **NEVER refactor server.js by rewriting large sections** — only make targeted edits to the code you're changing
- Before ANY commit that removes more than 10 lines, **verify every deleted function/endpoint is truly unused** by checking all HTML pages and other JS files that call it
- If you're editing server.js and your diff deletes routes, table definitions, or helper functions you didn't create — **STOP and put them back**
- When in doubt, **leave code alone**. Extra unused code is infinitely better than accidentally deleting a feature that customers depend on

**Protected features that must NEVER be removed** (features marked with * have been accidentally deleted before):

| Feature | Backend | Frontend | What breaks if removed |
|---------|---------|----------|----------------------|
| Season Kickoff* | `season-kickoff/*` endpoints, `buildKickoffContent()`, `season_kickoff_responses` table creation | `season-kickoff.html`, `confirm-services.html` | Customers can't confirm their annual services |
| CopilotCRM sync* | Inside `POST /api/sent-quotes/:id/sign-contract`, backfill endpoint | — | Signed contracts don't sync to CopilotCRM |
| Password reset | `forgot-password`, `reset-password` endpoints | `login.html`, `reset-password.html` | Users can't reset passwords |
| 2025 Services Report* | `GET /api/reports/2025-services` | Used by `season-kickoff.html` | Season kickoff page can't load customer data |
| Quote signing | `GET/POST /api/sign/:token`, decline, request-changes | `sign-quote.html`, `sign-contract.html` | Customers can't sign quotes or contracts |
| Invoice payments | `GET/POST /api/pay/:token/*`, Square payment processing | `pay-invoice.html` | Customers can't pay invoices online |
| Customer portal | `GET/POST /api/portal/:token/*` (dashboard, invoices, cards, requests, reviews) | `customer-portal.html` | Customer self-service portal breaks |
| Square webhook | `POST /api/webhooks/square` (before express.json middleware) | — | Payment confirmations stop processing |
| SMS webhook | `POST /api/sms/webhook` | — | Inbound text messages stop being received |
| Email tracking | `GET /api/t/:trackingId/open.png`, `GET /api/t/:trackingId/click` | — | Email open/click tracking breaks |
| Quote followups | `quote-followups/*` endpoints, `POST /api/cron/process-followups` | `sent-quotes.html` | Automated follow-up emails stop sending |
| QuickBooks sync | `quickbooks/*` endpoints (auth, callback, sync, status) | `settings.html` | Accounting sync breaks |
| Daily automations | `POST/GET /api/cron/daily-automation` | — | Late fees, recurring jobs, auto-invoicing stops |
| Broadcasts | `broadcasts/*` endpoints, `campaigns/:id/send` | `broadcasts.html`, `campaigns.html` | Can't send bulk emails/SMS to customers |

### Changelog (MANDATORY)
- **Update `CHANGELOG.md` at the root of the repo** whenever you complete substantive work (new features, bug fixes, UI changes)
- Group entries by date, use clear headers and bullet points
- Write from the user's perspective — what changed, not how it was implemented
- This file is the permanent record of all changes to the app

### Testing (MANDATORY)
- ALWAYS test new pages/features against the running app before saying it works
- Use `curl` with auth or DB queries to verify data flow
- Never assume a page works just because the HTML looks correct

### Auto-Update Memory (MANDATORY)
**Update memory files AS YOU GO, not at the end.** When you learn something new, update immediately.

| Trigger | Action |
|---------|--------|
| User shares a fact about themselves | → Update `.claude/memory/memory-profile.md` |
| User states a preference | → Update `.claude/memory/memory-preferences.md` |
| A decision is made | → Update `.claude/memory/memory-decisions.md` with date |
| Completing substantive work | → Add to `.claude/memory/memory-sessions.md` |

**Skip:** Quick factual questions, trivial tasks with no new info.

**DO NOT ASK. Just update the files when you learn something.**

### Past Mistakes Tracking (MANDATORY)
- Maintain `.claude/memory/memory-mistakes.md` — log patterns where things went wrong (repeated instructions, incorrect outputs, misunderstandings)
- Review this file at the start of every session to avoid repeating errors
- Format: `- [date] What went wrong → What to do instead`

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

### Database
- Table creation (`CREATE TABLE IF NOT EXISTS`, `ensureTable()`) runs at startup, not per-request
- Use `FOR UPDATE` when generating sequential IDs (invoice numbers) to prevent race conditions
- Always use parameterized queries ($1, $2) — never string interpolation for values

### API Responses
- Success: `{ success: true, data... }`
- Client errors (400/401/403/404): `{ success: false, error: 'Human-readable message' }`
- Server errors (500): Always use `serverError()` — generic message to client, full error to logs
