# Pappas Quote Backend

Backend API for the Pappas & Co. Landscaping operating system. This service powers quoting, invoicing, scheduling, portal access, messaging, payment workflows, and AI-assisted back-office features.

## Stack

- Node.js 18+
- Express
- PostgreSQL
- Twilio, Square, Stripe, QuickBooks, Anthropic

## Current Shape

The app currently runs from a large `server.js` entrypoint plus a small set of extracted modules:

- `config.js`: environment parsing and required config
- `middleware/auth.js`: token auth helpers
- `lib/audit.js`: audit logging
- `lib/startup-schema.js`: shared startup schema/bootstrap path
- `scripts/migrate.js`: one-shot core database migration runner
- `scripts/bootstrap.js`: explicit database bootstrap entrypoint
- `tests/`: structural and integration coverage

## Local Setup

1. Install dependencies:

```bash
npm install
```

2. Copy env settings:

```bash
cp .env.example .env
```

3. Set at minimum:

- `DATABASE_URL`
- `JWT_SECRET`
- `CRON_SECRET`

You can also omit `DATABASE_URL` if Railway provides:

- `PGHOST`
- `PGPORT`
- `PGUSER`
- `PGPASSWORD`
- `PGDATABASE`

4. Start the API:

```bash
npm run dev
```

## Scripts

- `npm start`: run the production server
- `npm run dev`: run with `nodemon`
- `npm run migrate`: run the shared core database migration/bootstrap path without starting the API
- `npm run bootstrap`: explicit one-shot bootstrap command for the app schema/setup
- `npm test`: run the fast default test suite
- `npm run test:unit`: run tests that do not require a live database
- `npm run test:integration`: run Postgres-backed integration tests
- `npm run test:all`: run unit tests, then integration tests

## Testing Model

The repository contains two kinds of tests:

- Unit and structural tests: safe to run without external services
- Integration tests: expect a working Postgres database and real schema

If you are not connected to the application database, use `npm test` or `npm run test:unit`.

## Environment Variables

See [.env.example](/Users/theresapappas/Documents/New project/pappas-quote-backend/.env.example) for the current full list.

Key groups:

- Core: `DATABASE_URL`, `JWT_SECRET`, `CRON_SECRET`, `NODE_ENV`, `PORT`
- App URLs: `BASE_URL`, `EMAIL_ASSETS_URL`
- Notifications: `NOTIFICATION_EMAIL`, Twilio credentials
- Payments: Square and Stripe credentials
- AI: `ANTHROPIC_API_KEY`
- Accounting and CRM: QuickBooks and CopilotCRM credentials

## Deployment Notes

- The app expects Postgres in runtime.
- Production SSL behavior is controlled in the server database pool config.
- The server now uses the same startup schema path as the CLI scripts, so app boot and manual bootstrap share one code path.
- A `Dockerfile` is included for containerized deployment.

## Immediate Improvement Priorities

- Continue breaking `server.js` into domain modules.
- Move integration-specific startup logic behind service adapters.
- Add explicit health/readiness checks and deployment smoke tests.
- Add CI that runs unit tests on every push and integration tests in a DB-backed environment.
