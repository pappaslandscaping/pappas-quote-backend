#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════
// Communications Tests
// Tests templates, campaigns, messages, broadcasts: validation,
// auth boundaries, and basic CRUD shape.
//
// Usage: node tests/communications.js
// Requires: server running on localhost:3000, JWT_SECRET in .env
// ═══════════════════════════════════════════════════════════

require('dotenv').config();
const jwt = require('jsonwebtoken');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3000';
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) { console.error('❌ JWT_SECRET not set'); process.exit(1); }

const ADMIN_TOKEN = jwt.sign({ id: 1, email: 'hello@pappaslandscaping.com', isAdmin: true, role: 'owner', name: 'Test' }, JWT_SECRET, { expiresIn: '1h' });
const EMP_TOKEN = jwt.sign({ id: 2, email: 'emp@test.com', isAdmin: false, isEmployee: true, role: 'employee' }, JWT_SECRET, { expiresIn: '1h' });

let passed = 0, failed = 0;

async function test(name, url, opts, expectStatus) {
  try {
    const res = await fetch(url, opts);
    const ok = res.status === expectStatus;
    if (ok) { passed++; console.log(`  ✅ ${name}`); }
    else {
      failed++;
      const body = await res.text().catch(() => '');
      console.log(`  ❌ ${name} — expected ${expectStatus}, got ${res.status} ${body.slice(0, 100)}`);
    }
  } catch (err) {
    failed++;
    console.log(`  ❌ ${name} — error: ${err.message}`);
  }
}

const auth = (token) => ({ headers: { Authorization: `Bearer ${token}` } });
const post = (token, body) => ({
  method: 'POST',
  headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
  body: JSON.stringify(body),
});

(async () => {
  console.log(`\n  Communications tests against ${BASE}\n`);

  // ── Templates ─────────────────────────────────────────
  console.log('── Templates ──');
  await test('GET /api/templates (admin)', `${BASE}/api/templates`, auth(ADMIN_TOKEN), 200);
  await test('GET /api/templates (employee)', `${BASE}/api/templates`, auth(EMP_TOKEN), 200);
  await test('GET /api/templates (no auth blocked)', `${BASE}/api/templates`, {}, 401);
  await test('GET /api/templates/variables', `${BASE}/api/templates/variables`, auth(ADMIN_TOKEN), 200);
  await test('GET /api/templates/library', `${BASE}/api/templates/library`, auth(ADMIN_TOKEN), 200);
  await test('POST /api/templates/preview (missing slug)', `${BASE}/api/templates/preview`, post(ADMIN_TOKEN, {}), 404);

  // ── Campaigns ─────────────────────────────────────────
  console.log('\n── Campaigns ──');
  await test('GET /api/campaigns', `${BASE}/api/campaigns`, auth(ADMIN_TOKEN), 200);
  await test('GET /api/campaigns (no auth blocked)', `${BASE}/api/campaigns`, {}, 401);
  await test('POST /api/campaigns (missing name)', `${BASE}/api/campaigns`, post(ADMIN_TOKEN, {}), 400);
  // Public submission route (no auth)
  await test('POST /api/campaigns/submissions (public, missing campaign_id)', `${BASE}/api/campaigns/submissions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}'
  }, 400);

  // ── Messages ──────────────────────────────────────────
  console.log('\n── Messages ──');
  await test('GET /api/messages/conversations', `${BASE}/api/messages/conversations`, auth(ADMIN_TOKEN), 200);
  await test('GET /api/messages (no auth blocked)', `${BASE}/api/messages`, {}, 401);
  await test('POST /api/messages/send (empty body)', `${BASE}/api/messages/send`, post(ADMIN_TOKEN, {}), 400);
  await test('POST /api/messages/send (missing body text)', `${BASE}/api/messages/send`, post(ADMIN_TOKEN, { to: '+15551234567' }), 400);

  // ── Broadcasts ────────────────────────────────────────
  console.log('\n── Broadcasts ──');
  await test('GET /api/broadcasts/filter-options', `${BASE}/api/broadcasts/filter-options`, auth(ADMIN_TOKEN), 200);
  await test('GET /api/broadcasts/filter-options (employee allowed for read)', `${BASE}/api/broadcasts/filter-options`, auth(EMP_TOKEN), 200);
  await test('POST /api/broadcasts/send (employee blocked)', `${BASE}/api/broadcasts/send`, post(EMP_TOKEN, {}), 403);
  await test('POST /api/broadcasts/send (no auth blocked)', `${BASE}/api/broadcasts/send`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}'
  }, 401);

  // ── Email log ─────────────────────────────────────────
  console.log('\n── Email Log ──');
  await test('GET /api/email-log', `${BASE}/api/email-log`, auth(ADMIN_TOKEN), 200);
  await test('GET /api/email-log/stats', `${BASE}/api/email-log/stats`, auth(ADMIN_TOKEN), 200);
  await test('GET /api/email-log (no auth blocked)', `${BASE}/api/email-log`, {}, 401);

  // ── Tracking pixels (public) ──────────────────────────
  console.log('\n── Tracking (public) ──');
  await test('GET /api/t/:trackingId/open.png (no auth needed)', `${BASE}/api/t/test123/open.png`, {}, 200);

  // ── Summary ───────────────────────────────────────────
  console.log('\n' + '─'.repeat(60));
  console.log(`  PASSED: ${passed}`);
  console.log(`  FAILED: ${failed}`);
  console.log('─'.repeat(60));
  if (failed > 0) {
    console.log('\n  ⚠️  Some tests failed!\n');
    process.exit(1);
  } else {
    console.log('\n  All communications tests passed.\n');
  }
})();
