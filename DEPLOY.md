# YardDesk — Deploy & Environment Reference

## Deploy Order

```
1. Database available (PostgreSQL)
2. Environment variables set (see below)
3. Run migrations:        node lib/startup-schema.js
4. Start server:          npm start  (or npm run dev locally)
```

Railway auto-deploys on `git push origin main`. Migrations run automatically at server startup — the standalone script is for manual/pre-deploy verification.

### Pre-deploy checklist

- [ ] `node -c server.js` — syntax check passes
- [ ] `node tests/smoke.js` — 280+ endpoints pass locally
- [ ] `DRY_RUN=1 node lib/startup-schema.js` — DB connection verified
- [ ] No `.env` or credential files staged

### Rollback

Railway keeps previous deploys. To rollback:
1. Railway dashboard → Deployments → click previous deploy → Redeploy
2. Or: `git revert HEAD && git push origin main`

Database migrations are additive (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`) — they do not need rollback. No destructive migrations exist.

---

## Environment Variables

### Required (server won't function without these)

| Variable | Purpose | Example |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://user:pass@host:5432/yarddesk` |
| `JWT_SECRET` | Signs auth tokens | Random 64+ char string |
| `CRON_SECRET` | Protects cron-only sync/repair endpoints | Random 32+ char string |
| `ADMIN_PASSWORD` | Default password for seeded admin accounts | Set on first deploy, change after |

### Required for core features

| Variable | Purpose | Notes |
|---|---|---|
| `RESEND_API_KEY` | Email delivery (Resend) | All transactional email stops without this |
| `TWILIO_ACCOUNT_SID` | SMS/voice | SMS reminders, quote followups, voice calls |
| `TWILIO_AUTH_TOKEN` | SMS/voice auth | Paired with account SID |
| `SQUARE_ACCESS_TOKEN` | Payment processing | Card/ACH payments, webhooks |
| `SQUARE_APPLICATION_ID` | Square app ID | Required for payment forms |
| `SQUARE_LOCATION_ID` | Square location | Required for payment processing |
| `SQUARE_ENVIRONMENT` | `sandbox` or `production` | Defaults to `sandbox` if not set |
| `SQUARE_WEBHOOK_SIGNATURE_KEY` | Webhook verification | Payment confirmation breaks without this |

### Optional integrations

| Variable | Purpose | Default/Fallback |
|---|---|---|
| `ANTHROPIC_API_KEY` | AI quote generation, followups | Graceful degradation — AI features disabled |
| `GOOGLE_MAPS_API_KEY` | Geocoding, route optimization | Falls back to Nominatim (free, slower) |
| `COPILOTCRM_USERNAME` | CopilotCRM contract sync | Sync skipped if not set |
| `COPILOTCRM_PASSWORD` | CopilotCRM auth | Paired with username |
| `QB_CLIENT_ID` | QuickBooks OAuth | QB sync disabled if not set |
| `QB_CLIENT_SECRET` | QuickBooks OAuth | Paired with client ID |
| `QB_REDIRECT_URI` | QuickBooks OAuth callback | Defaults to `http://localhost:3000/api/quickbooks/callback` |
| `QB_ENVIRONMENT` | `sandbox` or `production` | Defaults to `sandbox` |
| `RECAPTCHA_SECRET_KEY` | reCAPTCHA on public forms | Forms work without, no bot protection |
| `ZAPIER_CUSTOMER_WEBHOOK` | Customer sync to Zapier | Webhook skipped if not set |

### Optional automeasure integrations

| Variable | Purpose | Behavior when missing |
|---|---|---|
| `GOOGLE_MAPS_API_KEY` | Property geocoding, static map imagery, map embeds | Address-based map features degrade; automeasure cannot fetch Google imagery |
| `MEASUREMENT_ENGINE` | Measurement engine selector: `legacy_sam_prompt` or `roboflow_semantic` | Defaults to `legacy_sam_prompt` |
| `FAL_API_KEY` | Legacy prompt-only SAM engine | `legacy_sam_prompt` falls back to estimate ratios |
| `ROBOFLOW_API_KEY` | Roboflow semantic segmentation engine | `roboflow_semantic` cannot run without it |
| `ROBOFLOW_MODEL_SLUG` | Roboflow model slug for semantic segmentation | `roboflow_semantic` cannot run without it |
| `ROBOFLOW_MODEL_VERSION` | Roboflow model version for semantic segmentation | `roboflow_semantic` cannot run without it |
| `REGRID_API_TOKEN` | Optional parcel boundary and lot-size lookup | Automeasure remains approximate without parcel enrichment |

### Optional configuration

| Variable | Purpose | Default |
|---|---|---|
| `PORT` | Server port | `3000` |
| `NODE_ENV` | `production` enables SSL for DB | Not set = no SSL |
| `BASE_URL` | Public URL for links in emails/SMS | `https://app.pappaslandscaping.com` |
| `EMAIL_ASSETS_URL` | CDN for email images | Falls back to `BASE_URL` |
| `ALLOWED_ORIGINS` | CORS origins (comma-separated) | Required for cross-origin frontend access in production |
| `NOTIFICATION_EMAIL` | Admin notification recipient | `hello@pappaslandscaping.com` |

### Twilio advanced (voice/app features)

| Variable | Purpose |
|---|---|
| `TWILIO_API_KEY_SID` | Twilio API key for voice tokens |
| `TWILIO_API_KEY_SECRET` | Paired with API key SID |
| `TWILIO_TWIML_APP_SID` | TwiML app for browser calling |
| `TWILIO_PUSH_CREDENTIAL_SID` | Push notifications for mobile |
| `APP_PASSWORD` | Password for mobile app auth |

### Rarely used

| Variable | Purpose |
|---|---|
| `GOOGLE_CLOUD_VISION_API_KEY` | Image analysis |
| `EXPO_ACCESS_TOKEN` | Expo push notifications |

---

## Daily Tax Transfer Freshness Sync

This app now exposes a dedicated cron-safe endpoint for keeping the Tax Transfers page fresh without moving money:

- Endpoint: `POST /api/cron/tax-transfer-freshness-sync`
- Compatibility fallback for simple schedulers that cannot send `POST`: `GET /api/cron/tax-transfer-freshness-sync`
- Auth: prefer `x-cron-secret: $CRON_SECRET`; `?key=$CRON_SECRET` is supported for basic schedulers
- Default scope: sync Copilot payments plus Copilot Tax Summary `collected` for `today + 1 day back`
- Time zone: `America/New_York`

Recommended schedule:

- Send a daily `POST` at `15 20 * * *` in `America/New_York`
- Reason: it refreshes same-day tax reporting after most business-day payments have landed, while manual sync buttons remain available for intraday updates

Optional query/body overrides:

- `daysBack`: `0-7` (default `1`)
- `pageSize`: payment page size (default `100`)
- `maxPages`: payment page fetch cap (default `25`)
- `force=false`: reuse cached data instead of forcing live refresh

Operational requirements:

- `CRON_SECRET` must be configured
- Valid Copilot auth must already exist in `copilot_sync_settings` (`copilot_cookies` or `copilot_token`)
- The endpoint returns non-2xx on auth failure, missing `CRON_SECRET`, degraded runs, and failed runs so external scheduler alerting can key off the HTTP status
- No bank transfer or payout automation is performed by this endpoint; it only refreshes reporting inputs and stores run status

The Tax Transfers UI reads the stored run status and same-day freshness timestamps from this job, including partial-failure details.

---

## Startup Behavior

On `npm start`, the server:

1. Initializes SDK clients (Square, Anthropic, Twilio) — logs warnings if keys missing
2. Starts Express on `PORT`
3. Runs `runStartupTableInit()` — creates core tables (invoices, quote_events, copilot_sync, quote_views)
4. Runs `runStartupMigrations()` — creates all remaining tables, adds columns, seeds defaults

Steps 3-4 are idempotent (`IF NOT EXISTS` / `ON CONFLICT DO NOTHING`). Safe to run on every boot.

---

## Next.js Frontend Deployment

The new React/Next.js frontend in `frontend/` is intentionally separate from the existing Express backend. Keep the backend deployment unchanged for now: it still runs from the repository root with `npm start`, serves the legacy `public/*.html` pages, and owns all API/database behavior.

### Recommended production shape

Run two services:

1. **Backend API service**
   - Root directory: repository root
   - Build/install: existing platform behavior, or `npm ci --omit=dev`
   - Start command: `npm start`
   - Public URL example: `https://api.yarddesk.example.com`

2. **Frontend Next service**
   - Root directory: `frontend`
   - Install command: `npm ci`
   - Build command: `npm run build`
   - Start command: `npm start`
   - Required env var: `NEXT_PUBLIC_API_BASE_URL=https://api.yarddesk.example.com`

This is the lowest-risk option because the backend container, Express routes, database startup, and old public HTML screens remain untouched. The tradeoff is that CORS must allow the frontend origin.

### CORS for separate frontend/backend origins

If the frontend and backend are on different domains, set `ALLOWED_ORIGINS` on the backend to the frontend URL:

```bash
ALLOWED_ORIGINS=https://app.yarddesk.example.com
```

For multiple allowed origins, use a comma-separated list:

```bash
ALLOWED_ORIGINS=https://app.yarddesk.example.com,https://preview.yarddesk.example.com
```

In production, cross-origin browser requests are allowed only for exact origins listed in `ALLOWED_ORIGINS`. Outside production, the backend also allows the local Next frontend origins:

- `http://localhost:3001`
- `http://127.0.0.1:3001`

### Local run commands

Run the backend and frontend separately:

```bash
# Terminal 1: Express backend and API
npm run dev:backend

# Terminal 2: Next frontend
NEXT_PUBLIC_API_BASE_URL=http://localhost:3000 npm run dev:frontend
```

Open the frontend at `http://127.0.0.1:3001`. The legacy backend pages remain available at `http://localhost:3000`.

To test a production frontend build locally:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:3000 npm run build:frontend
NEXT_PUBLIC_API_BASE_URL=http://localhost:3000 npm run start:frontend:local
```

### Production run commands

Backend service:

```bash
npm start
```

Frontend service from `frontend/`:

```bash
NEXT_PUBLIC_API_BASE_URL=https://api.yarddesk.example.com npm run build
PORT=3001 npm start
```

Or from the repository root:

```bash
NEXT_PUBLIC_API_BASE_URL=https://api.yarddesk.example.com npm run build:frontend
PORT=3001 npm run start:frontend
```

### Other deployment options

- **Single container running both Express and Next:** possible, but it requires a process manager, Dockerfile changes, and a reverse-proxy/routing decision. Avoid this until the React frontend is ready to replace the old HTML screens.
- **Serve static export from Express:** not recommended because these pages use authenticated client-side API calls and dynamic Next routing such as `/quotes/[id]`, `/customers/[id]`, `/invoices/[id]`, and `/jobs/[id]`.
- **Vercel/Netlify for frontend plus Railway for backend:** safe if `NEXT_PUBLIC_API_BASE_URL` points at the Railway backend and backend `ALLOWED_ORIGINS` includes the frontend domain.

### Standalone migration

```bash
# Verify DB connection without changes
DRY_RUN=1 node lib/startup-schema.js

# Run all migrations explicitly
node lib/startup-schema.js

# Against production (be careful)
DATABASE_URL=$RAILWAY_DB_URL NODE_ENV=production node lib/startup-schema.js
```

---

## Database Backup

**Before any production DB changes:**

```bash
pg_dump $RAILWAY_DB_URL > backup-$(date +%Y%m%d).sql
```

A past import wiped active customer tokens and broke confirm links. Always back up first.
