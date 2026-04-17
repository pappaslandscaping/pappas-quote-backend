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

### Optional configuration

| Variable | Purpose | Default |
|---|---|---|
| `PORT` | Server port | `3000` |
| `NODE_ENV` | `production` enables SSL for DB | Not set = no SSL |
| `BASE_URL` | Public URL for links in emails/SMS | `https://app.pappaslandscaping.com` |
| `EMAIL_ASSETS_URL` | CDN for email images | Falls back to `BASE_URL` |
| `ALLOWED_ORIGINS` | CORS origins (comma-separated) | All origins allowed |
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
| `REGRID_API_TOKEN` | Property/parcel data API |
| `FAL_API_KEY` | AI image generation |
| `GOOGLE_CLOUD_VISION_API_KEY` | Image analysis |
| `EXPO_ACCESS_TOKEN` | Expo push notifications |

---

## Startup Behavior

On `npm start`, the server:

1. Initializes SDK clients (Square, Anthropic, Twilio) — logs warnings if keys missing
2. Starts Express on `PORT`
3. Runs `runStartupTableInit()` — creates core tables (invoices, quote_events, copilot_sync, quote_views)
4. Runs `runStartupMigrations()` — creates all remaining tables, adds columns, seeds defaults

Steps 3-4 are idempotent (`IF NOT EXISTS` / `ON CONFLICT DO NOTHING`). Safe to run on every boot.

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
