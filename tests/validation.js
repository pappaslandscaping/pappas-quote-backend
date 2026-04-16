#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════
// Validation & Error Handling Tests
// Tests that invalid payloads return structured errors,
// not 500s or silent bad records.
//
// Usage: node tests/validation.js
// Requires: server running on localhost:3000, JWT_SECRET in .env
// ═══════════════════════════════════════════════════════════

require('dotenv').config();
const jwt = require('jsonwebtoken');

const BASE = process.env.TEST_BASE_URL || 'http://localhost:3000';
const JWT_SECRET = process.env.JWT_SECRET;

if (!JWT_SECRET) { console.error('❌ JWT_SECRET not set'); process.exit(1); }

const TOKEN = jwt.sign({ id: 1, email: 'hello@pappaslandscaping.com', isAdmin: true, role: 'owner', name: 'Test' }, JWT_SECRET, { expiresIn: '1h' });

let passed = 0, failed = 0;

async function test(name, url, opts, expectStatus, expectShape) {
  try {
    const res = await fetch(url, opts);
    let ok = res.status === expectStatus;
    let body = null;
    try { body = await res.json(); } catch {}

    if (ok && expectShape && body) {
      if (expectShape.hasError && !body.error) ok = false;
      if (expectShape.hasCode && !body.code) ok = false;
      if (expectShape.hasDetails && !body.details) ok = false;
      if (expectShape.successFalse && body.success !== false) ok = false;
    }

    if (ok) {
      passed++;
      console.log(`  ✅ ${name}`);
    } else {
      failed++;
      console.log(`  ❌ ${name} — expected ${expectStatus}, got ${res.status}${body ? ' ' + JSON.stringify(body).slice(0, 120) : ''}`);
    }
  } catch (err) {
    failed++;
    console.log(`  ❌ ${name} — error: ${err.message}`);
  }
}

function post(url, body) {
  return {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` },
    body: JSON.stringify(body),
  };
}

function patch(url, body) {
  return {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` },
    body: JSON.stringify(body),
  };
}

(async () => {
  console.log(`\n  Validation tests against ${BASE}\n`);

  // ─── Auth validation ──────────────────────────────────
  console.log('── Auth ──');
  await test('Login: empty body', `${BASE}/api/auth/login`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' },
    400, { hasError: true, hasCode: true, hasDetails: true });

  await test('Login: missing password', `${BASE}/api/auth/login`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: 'test@test.com' }) },
    400, { hasError: true, hasDetails: true });

  await test('Login: invalid email format', `${BASE}/api/auth/login`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: 'notanemail', password: 'test123' }) },
    400, { hasError: true, hasDetails: true });

  await test('Change password: missing fields', `${BASE}/api/auth/change-password`,
    post(`${BASE}/api/auth/change-password`, {}),
    400, { hasError: true, hasDetails: true });

  await test('Change password: too short', `${BASE}/api/auth/change-password`,
    post(`${BASE}/api/auth/change-password`, { current_password: 'old', new_password: 'short' }),
    400, { hasError: true, hasDetails: true });

  // ─── Customer validation ──────────────────────────────
  console.log('\n── Customers ──');
  await test('Create customer: invalid email', `${BASE}/api/customers`,
    post(`${BASE}/api/customers`, { name: 'Test', email: 'not-an-email' }),
    400, { hasError: true, hasDetails: true });

  await test('Create customer: valid (minimal)', `${BASE}/api/customers`,
    post(`${BASE}/api/customers`, { name: 'Validation Test Customer', email: 'validtest@example.com' }),
    200);

  // ─── Job validation ───────────────────────────────────
  console.log('\n── Jobs ──');
  await test('Create job: missing required fields', `${BASE}/api/jobs`,
    post(`${BASE}/api/jobs`, {}),
    400, { hasError: true, hasDetails: true });

  await test('Create job: missing service_type', `${BASE}/api/jobs`,
    post(`${BASE}/api/jobs`, { customer_name: 'Test' }),
    400, { hasError: true, hasDetails: true });

  // ─── Quote validation ─────────────────────────────────
  console.log('\n── Sent Quotes ──');
  await test('Create sent quote: empty body', `${BASE}/api/sent-quotes`,
    post(`${BASE}/api/sent-quotes`, {}),
    400, { hasError: true, hasDetails: true });

  await test('Create sent quote: missing services', `${BASE}/api/sent-quotes`,
    post(`${BASE}/api/sent-quotes`, { customer_name: 'Test', total: 100 }),
    400, { hasError: true, hasDetails: true });

  await test('Create sent quote: services not array', `${BASE}/api/sent-quotes`,
    post(`${BASE}/api/sent-quotes`, { customer_name: 'Test', services: 'not-array', total: 100 }),
    400, { hasError: true, hasDetails: true });

  // ─── Invoice validation ───────────────────────────────
  console.log('\n── Invoices ──');
  await test('Create invoice: empty body', `${BASE}/api/invoices`,
    post(`${BASE}/api/invoices`, {}),
    400, { hasError: true, hasDetails: true });

  await test('Create invoice: missing line_items', `${BASE}/api/invoices`,
    post(`${BASE}/api/invoices`, { customer_name: 'Test', total: 100 }),
    400, { hasError: true, hasDetails: true });

  await test('Create invoice: negative total', `${BASE}/api/invoices`,
    post(`${BASE}/api/invoices`, { customer_name: 'Test', line_items: [], total: -50 }),
    400, { hasError: true, hasDetails: true });

  // ─── Crew validation ──────────────────────────────────
  console.log('\n── Crews ──');
  await test('Create crew: missing name', `${BASE}/api/crews`,
    post(`${BASE}/api/crews`, {}),
    400, { hasError: true, hasDetails: true });

  // ─── Employee validation ──────────────────────────────
  console.log('\n── Employees ──');
  await test('Create employee: missing names', `${BASE}/api/employees`,
    post(`${BASE}/api/employees`, {}),
    400, { hasError: true, hasDetails: true });

  await test('Create employee: missing last_name', `${BASE}/api/employees`,
    post(`${BASE}/api/employees`, { first_name: 'Test' }),
    400, { hasError: true, hasDetails: true });

  // ─── Expense validation ───────────────────────────────
  console.log('\n── Expenses ──');
  await test('Create expense: missing amount', `${BASE}/api/expenses`,
    post(`${BASE}/api/expenses`, {}),
    400, { hasError: true, hasDetails: true });

  await test('Create expense: negative amount', `${BASE}/api/expenses`,
    post(`${BASE}/api/expenses`, { amount: -10 }),
    400, { hasError: true, hasDetails: true });

  // ─── Note validation ──────────────────────────────────
  console.log('\n── Notes ──');
  await test('Create note: missing content', `${BASE}/api/notes/customer/1`,
    post(`${BASE}/api/notes/customer/1`, {}),
    400, { hasError: true, hasDetails: true });

  // ─── Message validation ───────────────────────────────
  console.log('\n── Messages ──');
  await test('Send message: empty body', `${BASE}/api/messages/send`,
    post(`${BASE}/api/messages/send`, {}),
    400, { hasError: true, hasDetails: true });

  await test('Send message: missing body text', `${BASE}/api/messages/send`,
    post(`${BASE}/api/messages/send`, { to: '+15551234567' }),
    400, { hasError: true, hasDetails: true });

  // ─── Campaign validation ──────────────────────────────
  console.log('\n── Campaigns ──');
  await test('Create campaign: missing name', `${BASE}/api/campaigns`,
    post(`${BASE}/api/campaigns`, {}),
    400, { hasError: true, hasDetails: true });

  // ─── Error format consistency ─────────────────────────
  console.log('\n── Error format ──');
  // All validation errors should have success:false, error, code, details
  const validationRes = await fetch(`${BASE}/api/auth/login`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}'
  });
  const validationBody = await validationRes.json();
  const hasStructure = validationBody.success === false &&
    typeof validationBody.error === 'string' &&
    validationBody.code === 'VALIDATION_ERROR' &&
    Array.isArray(validationBody.details) &&
    validationBody.details[0]?.field &&
    validationBody.details[0]?.message;
  if (hasStructure) {
    passed++;
    console.log('  ✅ Validation error has standard structure (success, error, code, details[{field, message}])');
  } else {
    failed++;
    console.log('  ❌ Validation error missing standard structure:', JSON.stringify(validationBody));
  }

  // ─── Summary ──────────────────────────────────────────
  console.log('\n' + '─'.repeat(60));
  console.log(`  PASSED: ${passed}`);
  console.log(`  FAILED: ${failed}`);
  console.log('─'.repeat(60));
  if (failed > 0) {
    console.log('\n  ⚠️  Some tests failed!\n');
    process.exit(1);
  } else {
    console.log('\n  All validation tests passed.\n');
  }
})();
