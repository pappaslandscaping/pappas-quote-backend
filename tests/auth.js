#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════
// Auth & Access Control Tests
// Tests login, token validation, admin vs employee boundaries,
// public route access, and password reset flow.
//
// Usage: node tests/auth.js
// Requires: server running on localhost:3000, JWT_SECRET in .env
// ═══════════════════════════════════════════════════════════

require('dotenv').config();
const jwt = require('jsonwebtoken');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3000';
const JWT_SECRET = process.env.JWT_SECRET;

if (!JWT_SECRET) {
  console.error('❌ JWT_SECRET not set in .env');
  process.exit(1);
}

let passed = 0, failed = 0;

function adminToken() {
  return jwt.sign({ id: 1, email: 'hello@pappaslandscaping.com', isAdmin: true, role: 'owner', name: 'Test Admin' }, JWT_SECRET, { expiresIn: '1h' });
}

function employeeToken() {
  return jwt.sign({ id: 2, email: 'emp@test.com', isAdmin: false, isEmployee: true, role: 'employee', name: 'Test Employee', permissions: [] }, JWT_SECRET, { expiresIn: '1h' });
}

function expiredToken() {
  return jwt.sign({ id: 1, email: 'test@test.com', isAdmin: true }, JWT_SECRET, { expiresIn: '-1s' });
}

async function test(name, url, opts, expectStatus) {
  try {
    const res = await fetch(url, opts);
    const ok = res.status === expectStatus;
    if (ok) {
      passed++;
      console.log(`  ✅ ${name}`);
    } else {
      failed++;
      const body = await res.text().catch(() => '');
      console.log(`  ❌ ${name} — expected ${expectStatus}, got ${res.status} ${body.slice(0, 100)}`);
    }
  } catch (err) {
    failed++;
    console.log(`  ❌ ${name} — error: ${err.message}`);
  }
}

function auth(token) {
  return { headers: { Authorization: `Bearer ${token}` } };
}

(async () => {
  console.log(`\n  Auth tests against ${BASE}\n`);

  // ─── No auth → protected routes ──────────────────────
  console.log('── Unauthenticated access (should be blocked) ──');
  await test('No token → GET /api/customers', `${BASE}/api/customers`, {}, 401);
  await test('No token → GET /api/jobs', `${BASE}/api/jobs`, {}, 401);
  await test('No token → GET /api/invoices', `${BASE}/api/invoices`, {}, 401);
  await test('No token → GET /api/employees', `${BASE}/api/employees`, {}, 401);
  await test('No token → GET /api/settings', `${BASE}/api/settings`, {}, 401);
  await test('No token → GET /api/crews', `${BASE}/api/crews`, {}, 401);
  await test('No token → GET /api/expenses', `${BASE}/api/expenses`, {}, 401);
  await test('No token → GET /api/reports/business-summary', `${BASE}/api/reports/business-summary`, {}, 401);
  await test('No token → GET /api/sent-quotes', `${BASE}/api/sent-quotes`, {}, 401);
  await test('No token → GET /api/campaigns', `${BASE}/api/campaigns`, {}, 401);
  await test('No token → GET /api/quickbooks/status', `${BASE}/api/quickbooks/status`, {}, 401);
  await test('No token → GET /api/email-log', `${BASE}/api/email-log`, {}, 401);
  await test('No token → GET /api/dashboard/activity-feed', `${BASE}/api/dashboard/activity-feed`, {}, 401);

  // ─── Expired/invalid tokens ──────────────────────────
  console.log('\n── Expired/invalid tokens ──');
  await test('Expired token → GET /api/customers', `${BASE}/api/customers`, auth(expiredToken()), 401);
  await test('Garbage token → GET /api/customers', `${BASE}/api/customers`, auth('not.a.token'), 401);
  await test('Empty bearer → GET /api/customers', `${BASE}/api/customers`, { headers: { Authorization: 'Bearer ' } }, 401);

  // ─── Public routes (should work without auth) ────────
  console.log('\n── Public routes (no auth needed) ──');
  await test('GET /health', `${BASE}/health`, {}, 200);
  await test('GET /api/services', `${BASE}/api/services`, {}, 200);
  await test('GET /api/config/maps-key', `${BASE}/api/config/maps-key`, {}, 200);
  await test('GET /api/pay/config', `${BASE}/api/pay/config`, {}, 200);
  await test('GET /api/square/status', `${BASE}/api/square/status`, {}, 200);
  await test('GET /api/quickbooks/auth', `${BASE}/api/quickbooks/auth`, {}, 200);
  // 400 = reached handler but missing fields; not 401 = auth correctly bypassed
  await test('POST /api/quotes (public form, no auth needed)', `${BASE}/api/quotes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: 'Auth Test', email: 'authtest@test.com', phone: '555-0000' })
  }, 400);

  // ─── Admin access ────────────────────────────────────
  console.log('\n── Admin access (should succeed) ──');
  await test('Admin → GET /api/customers', `${BASE}/api/customers`, auth(adminToken()), 200);
  await test('Admin → GET /api/jobs', `${BASE}/api/jobs`, auth(adminToken()), 200);
  await test('Admin → GET /api/invoices', `${BASE}/api/invoices`, auth(adminToken()), 200);
  await test('Admin → GET /api/employees', `${BASE}/api/employees`, auth(adminToken()), 200);
  await test('Admin → GET /api/settings', `${BASE}/api/settings`, auth(adminToken()), 200);
  await test('Admin → GET /api/quickbooks/status', `${BASE}/api/quickbooks/status`, auth(adminToken()), 200);
  await test('Admin → GET /api/auth/me', `${BASE}/api/auth/me`, auth(adminToken()), 200);

  // ─── Token shape assertions ──────────────────────────
  console.log('\n── Token shape (admin vs employee claims) ──');
  {
    const adminMe = await fetch(`${BASE}/api/auth/me`, auth(adminToken())).then(r => r.json());
    const adminOk = adminMe.success && adminMe.user.isAdmin === true && !adminMe.user.isEmployee;
    if (adminOk) { passed++; console.log('  ✅ Admin /auth/me: isAdmin=true, isEmployee absent/false'); }
    else { failed++; console.log('  ❌ Admin /auth/me shape wrong:', JSON.stringify(adminMe.user)); }
  }
  {
    const empMe = await fetch(`${BASE}/api/auth/me`, auth(employeeToken())).then(r => r.json());
    const empOk = empMe.success && empMe.user.isAdmin === false && empMe.user.isEmployee === true;
    if (empOk) { passed++; console.log('  ✅ Employee /auth/me: isAdmin=false, isEmployee=true'); }
    else { failed++; console.log('  ❌ Employee /auth/me shape wrong:', JSON.stringify(empMe.user)); }
  }

  // ─── Employee access ─────────────────────────────────
  console.log('\n── Employee access (normal routes allowed) ──');
  await test('Employee → GET /api/customers', `${BASE}/api/customers`, auth(employeeToken()), 200);
  await test('Employee → GET /api/jobs', `${BASE}/api/jobs`, auth(employeeToken()), 200);
  await test('Employee → GET /api/invoices', `${BASE}/api/invoices`, auth(employeeToken()), 200);
  await test('Employee → GET /api/crews', `${BASE}/api/crews`, auth(employeeToken()), 200);
  await test('Employee → GET /api/auth/me', `${BASE}/api/auth/me`, auth(employeeToken()), 200);

  // ─── Employee blocked from admin-only routes ─────────
  console.log('\n── Employee blocked from admin-only routes ──');
  await test('Employee → GET /api/employees (blocked)', `${BASE}/api/employees`, auth(employeeToken()), 403);
  await test('Employee → GET /api/settings (blocked)', `${BASE}/api/settings`, auth(employeeToken()), 403);
  await test('Employee → GET /api/quickbooks/status (blocked)', `${BASE}/api/quickbooks/status`, auth(employeeToken()), 403);
  await test('Employee → GET /api/copilot/settings (blocked)', `${BASE}/api/copilot/settings`, auth(employeeToken()), 403);

  // ─── Login endpoint ──────────────────────────────────
  console.log('\n── Login endpoint ──');
  await test('Login with no body', `${BASE}/api/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}'
  }, 400);
  await test('Login with wrong password', `${BASE}/api/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: 'hello@pappaslandscaping.com', password: 'wrongpassword123' })
  }, 401);
  await test('Login with nonexistent user', `${BASE}/api/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email: 'nobody@nowhere.com', password: 'whatever' })
  }, 401);

  // ─── Password change ────────────────────────────────
  console.log('\n── Password change validation ──');
  await test('Employee cannot change admin passwords', `${BASE}/api/auth/change-password`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${employeeToken()}` },
    body: JSON.stringify({ current_password: 'oldpass', new_password: 'newpass123' })
  }, 403);
  await test('Change password without current', `${BASE}/api/auth/change-password`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${adminToken()}` },
    body: JSON.stringify({ new_password: 'newpass123' })
  }, 400);
  await test('Change password too short', `${BASE}/api/auth/change-password`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${adminToken()}` },
    body: JSON.stringify({ current_password: 'old', new_password: 'short' })
  }, 400);

  // ─── Summary ─────────────────────────────────────────
  console.log('\n' + '─'.repeat(60));
  console.log(`  PASSED: ${passed}`);
  console.log(`  FAILED: ${failed}`);
  console.log('─'.repeat(60));
  if (failed > 0) {
    console.log('\n  ⚠️  Some tests failed!\n');
    process.exit(1);
  } else {
    console.log('\n  All auth tests passed.\n');
  }
})();
