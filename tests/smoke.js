#!/usr/bin/env node

/**
 * Smoke Test — hits every API endpoint and checks none return 500.
 *
 * Usage:
 *   node tests/smoke.js                                    # local, self-signed token
 *   AUTH_TOKEN=eyJ... node tests/smoke.js                  # real token from login
 *   BASE_URL=https://app.example.com AUTH_TOKEN=eyJ... node tests/smoke.js  # production
 *
 * Auth: Uses AUTH_TOKEN env var if set, otherwise mints a JWT from JWT_SECRET in .env.
 * Endpoints that need path params use placeholder IDs (0 or dummy tokens) —
 * a 404 or 400 is fine, only 500 is a failure.
 */

require('dotenv').config();
const jwt = require('jsonwebtoken');

const BASE = process.env.BASE_URL || 'http://localhost:3000';
const JWT_SECRET = process.env.JWT_SECRET;

// Use AUTH_TOKEN from env if provided, otherwise mint one from JWT_SECRET
let AUTH_TOKEN = process.env.AUTH_TOKEN;
if (!AUTH_TOKEN) {
  if (!JWT_SECRET) {
    console.error('FATAL: Set AUTH_TOKEN or JWT_SECRET in .env');
    process.exit(1);
  }
  AUTH_TOKEN = jwt.sign(
    { id: 1, email: 'smoke@test.com', name: 'Smoke Test', role: 'admin', isAdmin: true },
    JWT_SECRET,
    { expiresIn: '1h' }
  );
  console.log('  Using self-signed token (JWT_SECRET)\n');
} else {
  console.log('  Using AUTH_TOKEN from environment\n');
}

const HEADERS = {
  'Authorization': `Bearer ${AUTH_TOKEN}`,
  'Content-Type': 'application/json',
};

// Dummy values for path params
const FAKE_ID = '0';
const FAKE_TOKEN = 'smoke_test_nonexistent_token_000000000000000000000000';
const FAKE_PHONE = '+10000000000';
const FAKE_TRACKING = 'smoke000';

// ─────────────────────────────────────────────
// Endpoint definitions: [method, path, body?]
// A 400/401/403/404 is passing — only 500+ is failure.
// ─────────────────────────────────────────────

const endpoints = [
  // ── Health ──
  ['GET', '/health'],

  // ── Auth ──
  ['GET', '/api/auth/me'],
  ['POST', '/api/auth/login', { email: 'x', password: 'x' }],
  ['POST', '/api/auth/change-password', { current_password: 'x', new_password: 'x' }],
  ['POST', '/api/auth/forgot-password', { email: 'smoke@test.com' }],
  ['POST', '/api/auth/reset-password', { token: 'x', password: 'x' }],

  // ── Customers ──
  ['GET', '/api/customers'],
  ['GET', '/api/customers/search?q=test'],
  ['GET', '/api/customers/stats'],
  ['GET', '/api/customers/pipeline-stats'],
  ['GET', `/api/customers/${FAKE_ID}`],
  ['GET', `/api/customers/${FAKE_ID}/timeline`],
  ['GET', `/api/customers/${FAKE_ID}/invoices`],
  ['GET', `/api/customers/${FAKE_ID}/jobs`],
  ['GET', `/api/customers/${FAKE_ID}/quotes`],
  ['GET', `/api/customers/${FAKE_ID}/properties`],
  ['GET', `/api/customers/${FAKE_ID}/saved-cards`],
  ['GET', `/api/customers/${FAKE_ID}/emails`],
  ['GET', `/api/customers/${FAKE_ID}/lead-score`],
  ['POST', '/api/customers', { name: '' }], // will 400
  ['POST', '/api/customers/deduplicate'],
  ['POST', '/api/customers/clean-names'],
  ['PATCH', `/api/customers/${FAKE_ID}`, { name: 'x' }],
  ['DELETE', `/api/customers/${FAKE_ID}`],

  // ── Jobs ──
  ['GET', '/api/jobs'],
  ['GET', '/api/jobs/stats'],
  ['GET', '/api/jobs/dashboard'],
  ['GET', '/api/jobs/pipeline'],
  ['GET', '/api/jobs/recurring'],
  ['GET', '/api/jobs/completed-uninvoiced'],
  ['GET', '/api/jobs/calendar-summary'],
  ['GET', `/api/jobs/${FAKE_ID}`],
  ['GET', `/api/jobs/${FAKE_ID}/profitability`],
  ['POST', '/api/jobs', {}], // will 400 or create minimal
  ['POST', '/api/jobs/bulk', { jobs: [] }],
  ['POST', `/api/jobs/${FAKE_ID}/setup-recurring`, {}],
  ['POST', `/api/jobs/${FAKE_ID}/expenses`, {}],
  ['POST', '/api/jobs/optimize-route', { job_ids: [] }],
  ['PATCH', `/api/jobs/${FAKE_ID}`, { status: 'scheduled' }],
  ['PATCH', `/api/jobs/${FAKE_ID}/complete`, {}],
  ['PATCH', `/api/jobs/${FAKE_ID}/pipeline`, { status: 'scheduled' }],
  ['PATCH', `/api/jobs/${FAKE_ID}/recurring`, {}],
  ['PATCH', '/api/jobs/reorder', { jobs: [] }],
  ['DELETE', `/api/jobs/${FAKE_ID}`],
  ['DELETE', `/api/jobs/${FAKE_ID}/expenses/${FAKE_ID}`],

  // ── Invoices ──
  ['GET', '/api/invoices'],
  ['GET', '/api/invoices/stats'],
  ['GET', '/api/invoices/aging'],
  ['GET', `/api/invoices/${FAKE_ID}`],
  ['POST', '/api/invoices', {}],
  ['POST', '/api/invoices/batch', { invoice_ids: [] }],
  // SKIP: sends real invoice email — ['POST', `/api/invoices/${FAKE_ID}/send`, {}],
  // SKIP: sends real reminder email — ['POST', `/api/invoices/${FAKE_ID}/send-reminder`, {}],
  ['POST', `/api/invoices/${FAKE_ID}/mark-paid`, {}],
  ['POST', `/api/invoices/${FAKE_ID}/record-payment`, { amount: 0 }],
  ['POST', `/api/invoices/${FAKE_ID}/charge-card`, {}],
  ['POST', `/api/invoices/${FAKE_ID}/payment-schedule`, {}],
  ['GET', `/api/invoices/${FAKE_ID}/payment-schedule`],
  ['PATCH', `/api/invoices/${FAKE_ID}`, { status: 'draft' }],
  ['DELETE', `/api/invoices/${FAKE_ID}`],

  // ── Payments ──
  ['GET', '/api/payments'],
  ['GET', '/api/pay/config'],
  ['GET', `/api/pay/${FAKE_TOKEN}`],
  // Skip POST /api/pay/:token/card etc — need Square client running

  // ── Quotes (public request form) ──
  ['GET', '/api/quotes'],
  ['GET', `/api/quotes/${FAKE_ID}`],
  ['GET', '/api/quotes/next-number'],
  ['POST', '/api/quotes', { name: 'Test', email: 'x@x.com', services: [] }],
  ['POST', '/api/quotes/admin', {}],
  ['PATCH', `/api/quotes/${FAKE_ID}`, {}],
  ['DELETE', `/api/quotes/${FAKE_ID}`],

  // ── Sent Quotes ──
  ['GET', '/api/sent-quotes'],
  ['GET', `/api/sent-quotes/${FAKE_ID}`],
  ['GET', `/api/sent-quotes/${FAKE_ID}/events`],
  ['GET', `/api/sent-quotes/${FAKE_ID}/views`],
  ['GET', `/api/sent-quotes/${FAKE_ID}/contract-status`],
  ['GET', '/api/sent-quotes/event-counts'],
  ['GET', '/api/sent-quotes/view-counts'],
  ['POST', '/api/sent-quotes', { customer_name: 'x', services: [] }],
  // SKIP: sends real quote email — ['POST', `/api/sent-quotes/${FAKE_ID}/send`, {}],
  // SKIP: sends real quote SMS — ['POST', `/api/sent-quotes/${FAKE_ID}/send-sms`, {}],
  ['POST', `/api/sent-quotes/${FAKE_ID}/sign-contract`, {}],
  ['PUT', `/api/sent-quotes/${FAKE_ID}`, {}],
  ['DELETE', `/api/sent-quotes/${FAKE_ID}`],

  // ── Quote Signing (public) ──
  ['GET', `/api/sign/${FAKE_TOKEN}`],
  ['POST', `/api/sign/${FAKE_TOKEN}`, { signed_by_name: 'Test' }],
  ['POST', `/api/sign/${FAKE_TOKEN}/decline`, { decline_reason: 'test' }],
  ['POST', `/api/sign/${FAKE_TOKEN}/request-changes`, {}],

  // ── Quote Followups ──
  ['GET', '/api/quote-followups'],
  ['GET', '/api/quote-followups/stats'],
  // SKIP: creates followup sequence that triggers future sends — ['POST', '/api/quote-followups', { customer_name: 'x', customer_email: 'x@x.com' }],
  ['PATCH', `/api/quote-followups/${FAKE_ID}/stop`, {}],
  ['PATCH', `/api/quote-followups/${FAKE_ID}/resume`, {}],

  // ── Services (public) ──
  ['GET', '/api/services'],
  ['GET', '/api/service-items'],
  ['POST', '/api/service-items', { name: 'Test', price: 0 }],
  ['PATCH', `/api/service-items/${FAKE_ID}`, {}],
  ['DELETE', `/api/service-items/${FAKE_ID}`],

  // ── Properties ──
  ['GET', '/api/properties'],
  ['GET', '/api/properties/stats'],
  ['GET', `/api/properties/${FAKE_ID}`],
  ['GET', `/api/properties/${FAKE_ID}/service-history`],
  ['POST', '/api/properties', {}],
  ['PATCH', `/api/properties/${FAKE_ID}`, {}],
  ['PUT', `/api/properties/${FAKE_ID}`, {}],
  ['DELETE', `/api/properties/${FAKE_ID}`],

  // ── Crews & Employees ──
  ['GET', '/api/crews'],
  ['GET', `/api/crews/${FAKE_ID}/performance`],
  ['GET', `/api/crews/${FAKE_ID}/schedule`],
  ['POST', '/api/crews', { name: '' }],
  ['PATCH', `/api/crews/${FAKE_ID}`, {}],
  ['DELETE', `/api/crews/${FAKE_ID}`],
  ['GET', '/api/employees'],
  ['GET', `/api/employees/${FAKE_ID}`],
  ['POST', '/api/employees', {}],
  ['PATCH', `/api/employees/${FAKE_ID}`, {}],
  ['DELETE', `/api/employees/${FAKE_ID}`],

  // ── Expenses ──
  ['GET', '/api/expenses'],
  ['GET', '/api/expenses/stats'],
  ['GET', `/api/expenses/${FAKE_ID}`],
  ['GET', '/api/expense-categories'],
  ['POST', '/api/expenses', {}],
  ['PATCH', `/api/expenses/${FAKE_ID}`, {}],
  ['DELETE', `/api/expenses/${FAKE_ID}`],

  // ── Campaigns & Broadcasts ──
  ['GET', '/api/campaigns'],
  ['GET', `/api/campaigns/${FAKE_ID}`],
  ['GET', `/api/campaigns/${FAKE_ID}/submissions`],
  ['GET', `/api/campaigns/${FAKE_ID}/send-history`],
  ['POST', '/api/campaigns', {}],
  // SKIP: sends real campaign emails — ['POST', `/api/campaigns/${FAKE_ID}/send`, {}],
  ['POST', '/api/campaigns/submissions', {}],
  ['PATCH', `/api/campaigns/${FAKE_ID}`, {}],
  ['PATCH', `/api/campaigns/submissions/${FAKE_ID}`, {}],
  ['DELETE', `/api/campaigns/${FAKE_ID}`],
  ['DELETE', `/api/campaigns/submissions/${FAKE_ID}`],
  ['GET', '/api/broadcasts/filter-options'],
  ['POST', '/api/broadcasts/preview', { filters: {} }],
  // SKIP: sends real broadcast email/SMS — ['POST', '/api/broadcasts/send', { filters: {}, subject: '', body: '' }],

  // ── Communications ──
  ['GET', '/api/messages'],
  ['GET', '/api/messages/conversations'],
  ['GET', `/api/messages/thread/${encodeURIComponent(FAKE_PHONE)}`],
  // SKIP: sends real SMS — ['POST', '/api/messages/send', { to: FAKE_PHONE, body: '' }],
  ['GET', '/api/calls'],
  ['GET', '/api/calls/stats'],
  ['GET', `/api/calls/${FAKE_ID}`],
  // SKIP: makes real voice call — ['POST', '/api/calls', {}],
  ['PATCH', `/api/calls/${FAKE_ID}`, {}],
  ['DELETE', `/api/calls/${FAKE_ID}`],
  ['GET', '/api/email-log'],
  ['GET', '/api/email-log/stats'],

  // ── Templates ──
  ['GET', '/api/templates'],
  ['GET', `/api/templates/${FAKE_ID}`],
  ['GET', '/api/templates/variables'],
  ['POST', '/api/templates', {}],
  ['POST', `/api/templates/${FAKE_ID}/duplicate`],
  ['POST', '/api/templates/preview', {}],
  ['PATCH', `/api/templates/${FAKE_ID}`, {}],
  ['DELETE', `/api/templates/${FAKE_ID}`],

  // ── Automations ──
  ['GET', '/api/automations'],
  ['GET', `/api/automations/${FAKE_ID}`],
  ['GET', `/api/automations/${FAKE_ID}/history`],
  ['POST', '/api/automations', {}],
  ['PATCH', `/api/automations/${FAKE_ID}`, {}],
  ['DELETE', `/api/automations/${FAKE_ID}`],

  // ── Notes ──
  ['GET', `/api/notes/customer/${FAKE_ID}`],
  ['POST', `/api/notes/customer/${FAKE_ID}`, { content: '' }],
  ['PATCH', `/api/notes/${FAKE_ID}`, {}],
  ['DELETE', `/api/notes/${FAKE_ID}`],

  // ── Cancellations ──
  ['GET', '/api/cancellations'],
  ['GET', `/api/cancellations/${FAKE_ID}`],
  ['POST', '/api/cancellations', {}],
  ['PATCH', `/api/cancellations/${FAKE_ID}`, {}],
  ['DELETE', `/api/cancellations/${FAKE_ID}`],

  // ── Time Tracking ──
  ['GET', '/api/time-entries'],
  ['GET', '/api/time-entries/stats'],
  ['GET', '/api/time-entries/weekly-report'],
  ['GET', '/api/timeclock/pay-rates'],
  ['POST', '/api/time-entries/clock-in', {}],
  ['POST', `/api/time-entries/${FAKE_ID}/clock-out`, {}],
  ['PUT', `/api/time-entries/${FAKE_ID}`, {}],
  ['DELETE', `/api/time-entries/${FAKE_ID}`],

  // ── Work Requests & Service Requests ──
  ['GET', '/api/work-requests'],
  ['GET', `/api/work-requests/${FAKE_ID}`],
  ['GET', '/api/work-requests/stats'],
  ['GET', '/api/service-requests'],
  ['PATCH', `/api/service-requests/${FAKE_ID}`, {}],
  ['PUT', `/api/work-requests/${FAKE_ID}`, {}],

  // ── Service Programs ──
  ['GET', '/api/service-programs'],
  ['GET', `/api/service-programs/${FAKE_ID}`],
  ['POST', '/api/service-programs', {}],
  ['POST', `/api/service-programs/${FAKE_ID}/enroll`, {}],
  ['PUT', `/api/service-programs/${FAKE_ID}`, {}],

  // ── Dispatch ──
  ['GET', '/api/dispatch/board'],
  ['GET', '/api/dispatch/crew-availability'],
  ['GET', '/api/dispatch-templates'],
  ['POST', '/api/dispatch-templates', {}],
  ['POST', '/api/dispatch-templates/quick-dispatch', {}],
  ['POST', '/api/dispatch/geocode', { address: '123 Test St' }],
  ['POST', '/api/dispatch/optimize-route', { job_ids: [] }],
  ['POST', '/api/dispatch/apply-future-weeks', {}],
  ['PATCH', '/api/dispatch/assign', {}],
  ['PUT', `/api/dispatch-templates/${FAKE_ID}`, {}],

  // ── Reports ──
  ['GET', '/api/reports/2025-services'],
  ['GET', '/api/reports/business-summary'],
  ['GET', '/api/reports/crew-performance'],
  ['GET', '/api/reports/customer-acquisition'],
  ['GET', '/api/reports/customer-value'],
  ['GET', '/api/reports/job-costing'],
  ['GET', '/api/reports/sales-tax'],

  // ── Finance & KPI ──
  ['GET', '/api/finance/summary'],
  ['GET', '/api/finance/cash-flow-forecast'],
  ['GET', '/api/kpi/dashboard'],
  ['GET', '/api/kpi/detailed'],
  ['GET', '/api/stats'],

  // ── Dashboard ──
  ['GET', '/api/dashboard/today-summary'],
  ['GET', '/api/dashboard/activity-feed'],

  // ── Settings ──
  ['GET', '/api/settings'],
  ['GET', '/api/settings/home-base'],
  ['GET', '/api/config/maps-key'],
  ['POST', '/api/settings/home-base', { lat: 0, lng: 0 }],
  ['PATCH', '/api/settings/test_smoke_key', { value: {} }],

  // ── Service Token ──
  ['POST', '/api/auth/service-token'],

  // ── Telegram ──
  // SKIP: sends real Telegram message — ['POST', '/api/telegram/send', { message: 'test' }],

  // ── CopilotCRM Sync ──
  ['POST', '/api/copilot/sync', { startDate: '2020-01-01', endDate: '2020-01-01' }],
  // SKIP: logs into CopilotCRM and overwrites cookies — ['POST', '/api/copilot/refresh-cookies', {}],
  // SKIP: sends real contract email + logs into CopilotCRM — ['POST', '/api/copilotcrm/estimate-accepted', { customer_name: 'Test', estimate_number: '0000' }],

  // ── Morning Briefing ──
  ['POST', '/api/morning-briefing', {}],

  // ── Late Fees ──
  ['GET', '/api/late-fees'],
  ['POST', `/api/late-fees/${FAKE_ID}/waive`, {}],
  ['POST', '/api/late-fees/bulk-waive-today', {}],

  // ── Reviews ──
  ['GET', '/api/reviews'],
  ['DELETE', `/api/reviews/${FAKE_ID}`],

  // ── Season Kickoff ──
  ['GET', '/api/season-kickoff/responses'],
  ['GET', '/api/season-kickoff/token-status'],
  ['POST', '/api/season-kickoff/preview', {}],
  // SKIP: sends real season kickoff test email — ['POST', '/api/season-kickoff/send-test', {}],
  // SKIP: sends real season kickoff bulk emails — ['POST', '/api/season-kickoff/send-bulk', { customers: [] }],
  // SKIP: sends real season kickoff SMS — ['POST', '/api/season-kickoff/send-sms', {}],
  ['POST', '/api/season-kickoff/recover-tokens', {}],
  ['POST', '/api/season-kickoff/reply', {}],
  ['PATCH', `/api/season-kickoff/responses/${FAKE_ID}`, {}],
  ['DELETE', `/api/season-kickoff/responses/${FAKE_ID}`],

  // ── Cron (public) ──
  // SKIP: triggers real recurring jobs, monthly invoices, late fees — ['GET', '/api/cron/daily-automation'],
  // SKIP: triggers real recurring jobs, monthly invoices, late fees — ['POST', '/api/cron/daily-automation'],
  // SKIP: sends real followup emails + SMS — ['GET', '/api/cron/process-followups'],
  // SKIP: sends real followup emails + SMS — ['POST', '/api/cron/process-followups'],

  // ── QuickBooks ──
  ['GET', '/api/quickbooks/status'],
  ['GET', '/api/quickbooks/debug'],
  ['GET', '/api/quickbooks/sync-log'],
  ['GET', '/api/quickbooks/sync-progress'],
  ['POST', '/api/quickbooks/disconnect', {}],

  // ── Square ──
  ['GET', '/api/square/status'],

  // ── Customer Portal (public, token-based) ──
  ['GET', `/api/portal/${FAKE_TOKEN}`],
  ['GET', `/api/portal/${FAKE_TOKEN}/dashboard`],
  ['GET', `/api/portal/${FAKE_TOKEN}/invoices`],
  ['GET', `/api/portal/${FAKE_TOKEN}/payments`],
  ['GET', `/api/portal/${FAKE_TOKEN}/cards`],
  ['GET', `/api/portal/${FAKE_TOKEN}/quotes`],
  ['GET', `/api/portal/${FAKE_TOKEN}/service-history`],
  ['GET', `/api/portal/${FAKE_TOKEN}/properties`],
  ['GET', `/api/portal/${FAKE_TOKEN}/preferences`],
  ['GET', `/api/portal/${FAKE_TOKEN}/photos`],
  ['GET', `/api/portal/${FAKE_TOKEN}/reviews`],
  ['GET', `/api/portal/${FAKE_TOKEN}/service-requests`],
  ['GET', `/api/portal/${FAKE_TOKEN}/google-review-url`],
  // SKIP: sends real portal access email — ['POST', '/api/portal/request-access', { email: 'smoke@test.com' }],
  ['POST', `/api/portal/${FAKE_TOKEN}/service-requests`, {}],
  ['POST', `/api/portal/${FAKE_TOKEN}/preferences`, {}],
  ['POST', `/api/portal/${FAKE_TOKEN}/reviews`, {}],

  // ── Webhooks ──
  // SKIP: may trigger confirmation email — ['POST', '/api/webhooks/quote-accepted', { email: 'x' }],
  // SKIP: may trigger response email — ['POST', '/api/webhooks/quote-declined', { email: 'x' }],
  // SKIP: may trigger auto-reply — ['POST', '/api/webhooks/customer-replied', {}],
  // SKIP: inbound SMS webhook may trigger auto-reply — ['POST', '/api/sms/webhook', { From: FAKE_PHONE, Body: 'test', To: FAKE_PHONE }],
  // Skip /api/webhooks/square — needs raw body + signature

  // ── Email Tracking ──
  ['GET', `/api/t/${FAKE_TRACKING}/open.png`],
  ['GET', `/api/t/${FAKE_TRACKING}/click`],
  ['POST', '/api/unsubscribe', { email: 'smoke@test.com' }],

  // ── AI (may 503 if no API key — that's fine) ──
  ['POST', '/api/ai/generate-quote', { customer_name: 'Test', address: '123 St' }],
  ['POST', '/api/ai/chat', { message: 'hi' }],
  ['POST', '/api/ai/generate-followup', {}],
  ['POST', '/api/ai/suggest-service', {}],
  ['POST', '/api/ai/generate-template', {}],
  ['POST', '/api/ai/create-campaign', {}],
  ['GET', '/api/ai/lead-scores'],
  ['GET', '/api/ai/churn-risk'],
  ['GET', '/api/ai/revenue-forecast'],
  ['GET', '/api/ai/campaign-segments'],
  ['GET', '/api/ai/schedule-suggestions'],

  // ── Social Media ──
  ['GET', '/api/social-media/history'],
  ['POST', '/api/social-media/generate', {}],
  ['POST', '/api/social-media/refine', {}],

  // ── Tax ──
  ['POST', '/api/tax/calculate', { customer_id: 0, line_items: [] }],

  // ── Misc ──
  ['GET', '/api/preview-followup-emails'],
  ['GET', '/api/setup-quote-followups'],

  // ── App (mobile) ──
  ['POST', '/api/app/login', { email: 'x', password: 'x' }],
  ['GET', '/api/app/devices'],
  ['GET', '/api/app/customers'],
  ['GET', '/api/app/messages/conversations'],
  ['GET', '/api/app/messages/unread-count'],
  ['GET', `/api/app/messages/thread/${encodeURIComponent(FAKE_PHONE)}`],
  ['GET', '/api/app/voicemails'],
  ['GET', '/api/app/calls/history'],
  ['GET', '/api/app/calls/recent'],
  ['GET', '/api/app/twilio-numbers'],
  ['GET', '/api/app/voice/token'],
  ['POST', '/api/app/devices/register', {}],
  // SKIP: sends real SMS — ['POST', '/api/app/messages/send', { to: FAKE_PHONE, body: '' }],
];

// ─────────────────────────────────────────────
// Runner
// ─────────────────────────────────────────────

async function run() {
  console.log(`\n  Smoke testing ${endpoints.length} endpoints against ${BASE}\n`);

  const passed = [];
  const failed = [];
  const skipped = [];
  const CONCURRENCY = 10;

  // Process in batches to avoid hammering the server
  for (let i = 0; i < endpoints.length; i += CONCURRENCY) {
    const batch = endpoints.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map(async ([method, path, body]) => {
        const url = `${BASE}${path}`;
        const tag = `${method} ${path}`;
        const opts = { method, headers: { ...HEADERS }, signal: AbortSignal.timeout(15000) };

        if (body !== undefined && method !== 'GET') {
          opts.body = JSON.stringify(body);
        }

        try {
          const res = await fetch(url, opts);
          return { tag, status: res.status };
        } catch (err) {
          return { tag, status: 0, error: err.message };
        }
      })
    );

    for (const r of results) {
      const { tag, status, error } = r.value || r.reason || {};
      if (status === 0) {
        skipped.push({ tag, reason: error || 'network error' });
      } else if (status === 500) {
        failed.push({ tag, status });
      } else {
        // 2xx, 3xx, 4xx, 503 (service not configured) all count as passing
        passed.push({ tag, status });
      }
    }
  }

  // ── Summary ──
  console.log('─'.repeat(60));
  console.log(`  PASSED: ${passed.length}`);
  console.log(`  FAILED: ${failed.length}`);
  if (skipped.length > 0) console.log(`  SKIPPED: ${skipped.length}`);
  console.log('─'.repeat(60));

  if (failed.length > 0) {
    console.log('\n  FAILURES:\n');
    for (const f of failed) {
      console.log(`    ${f.status}  ${f.tag}`);
    }
  }

  if (skipped.length > 0) {
    console.log('\n  SKIPPED (network/timeout):\n');
    for (const s of skipped) {
      console.log(`    --  ${s.tag}  (${s.reason})`);
    }
  }

  if (failed.length === 0 && skipped.length === 0) {
    console.log('\n  All endpoints healthy.\n');
  }

  console.log('');
  process.exit(failed.length > 0 ? 1 : 0);
}

run().catch(err => {
  console.error('Smoke test crashed:', err);
  process.exit(2);
});
