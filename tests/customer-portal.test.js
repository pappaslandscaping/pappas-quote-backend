/**
 * Customer Portal Login Tests
 *
 * Tests against the real local database (yarddesk).
 * Covers: portal token generation, token validation, token expiry,
 *         portal data access, and payment token system.
 */

require('dotenv').config();
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false,
});

const RUN_ID = crypto.randomBytes(4).toString('hex');
const TEST_EMAIL = `test_portal_${RUN_ID}@example.com`;
const TEST_NAME = `test_portal_customer_${RUN_ID}`;

const createdCustomerIds = [];
const createdTokenIds = [];
const createdInvoiceIds = [];

afterAll(async () => {
  if (createdInvoiceIds.length > 0) {
    await pool.query('DELETE FROM invoices WHERE id = ANY($1)', [createdInvoiceIds]);
  }
  if (createdTokenIds.length > 0) {
    await pool.query('DELETE FROM customer_portal_tokens WHERE id = ANY($1)', [createdTokenIds]);
  }
  if (createdCustomerIds.length > 0) {
    await pool.query('DELETE FROM customers WHERE id = ANY($1)', [createdCustomerIds]);
  }
  await pool.end();
});

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

async function createTestCustomer(overrides = {}) {
  const result = await pool.query(
    `INSERT INTO customers (name, email, phone, street, city, state, postal_code, created_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP) RETURNING *`,
    [
      overrides.name || TEST_NAME,
      overrides.email || TEST_EMAIL,
      '440-555-0300',
      '789 Portal Ave',
      'Lakewood',
      'OH',
      '44107',
    ]
  );
  createdCustomerIds.push(result.rows[0].id);
  return result.rows[0];
}

async function createPortalToken(customerId, email, overrides = {}) {
  const token = overrides.token || generateToken();
  const expiresAt = overrides.expires_at || new Date(Date.now() + 30 * 24 * 60 * 60 * 1000); // 30 days

  const result = await pool.query(
    `INSERT INTO customer_portal_tokens (token, customer_id, email, expires_at)
     VALUES ($1, $2, $3, $4) RETURNING *`,
    [token, customerId, email, expiresAt]
  );
  createdTokenIds.push(result.rows[0].id);
  return result.rows[0];
}

// ─────────────────────────────────────────────
// Portal Token Generation
// ─────────────────────────────────────────────

describe('Portal Token Generation', () => {
  test('creates token with 30-day expiry linked to customer', async () => {
    const cust = await createTestCustomer();
    const beforeCreate = Date.now();

    const tokenRecord = await createPortalToken(cust.id, cust.email);

    expect(tokenRecord.token).toHaveLength(64);
    expect(tokenRecord.customer_id).toBe(cust.id);
    expect(tokenRecord.email).toBe(cust.email);

    // Verify expiry is ~30 days out
    const expiresMs = new Date(tokenRecord.expires_at).getTime();
    const thirtyDays = 30 * 24 * 60 * 60 * 1000;
    expect(expiresMs - beforeCreate).toBeGreaterThan(thirtyDays - 60000); // within 1 min tolerance
    expect(expiresMs - beforeCreate).toBeLessThan(thirtyDays + 60000);
  });

  test('token is unique', async () => {
    const cust = await createTestCustomer({ email: `unique_tok_${RUN_ID}@example.com` });
    const fixedToken = generateToken();

    await createPortalToken(cust.id, cust.email, { token: fixedToken });

    await expect(
      pool.query(
        'INSERT INTO customer_portal_tokens (token, customer_id, email, expires_at) VALUES ($1, $2, $3, $4)',
        [fixedToken, cust.id, cust.email, new Date(Date.now() + 86400000)]
      )
    ).rejects.toThrow(/unique|duplicate/i);
  });

  test('multiple tokens for same customer allowed', async () => {
    const cust = await createTestCustomer({ email: `multi_tok_${RUN_ID}@example.com` });

    const t1 = await createPortalToken(cust.id, cust.email);
    const t2 = await createPortalToken(cust.id, cust.email);

    expect(t1.token).not.toBe(t2.token);

    const tokens = await pool.query(
      'SELECT * FROM customer_portal_tokens WHERE customer_id = $1',
      [cust.id]
    );
    expect(tokens.rows.length).toBeGreaterThanOrEqual(2);
  });
});

// ─────────────────────────────────────────────
// Portal Token Validation
// ─────────────────────────────────────────────

describe('Portal Token Validation', () => {
  test('valid token returns customer data via JOIN', async () => {
    const cust = await createTestCustomer({ email: `valid_${RUN_ID}@example.com` });
    const tokenRecord = await createPortalToken(cust.id, cust.email);

    // Replicate GET /api/portal/:token query
    const result = await pool.query(
      `SELECT pt.*, c.name as customer_name, c.email as customer_email, c.phone,
              CONCAT_WS(', ', c.street, c.city, c.state, c.postal_code) as address
       FROM customer_portal_tokens pt
       LEFT JOIN customers c ON pt.customer_id = c.id
       WHERE pt.token = $1 AND pt.expires_at > NOW()`,
      [tokenRecord.token]
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].customer_name).toBe(cust.name);
    expect(result.rows[0].customer_email).toBe(cust.email);
    expect(result.rows[0].address).toContain('Lakewood');
  });

  test('expired token returns no rows', async () => {
    const cust = await createTestCustomer({ email: `expired_${RUN_ID}@example.com` });
    const expiredToken = await createPortalToken(cust.id, cust.email, {
      expires_at: new Date(Date.now() - 86400000), // 1 day ago
    });

    const result = await pool.query(
      `SELECT * FROM customer_portal_tokens
       WHERE token = $1 AND expires_at > NOW()`,
      [expiredToken.token]
    );

    expect(result.rows).toHaveLength(0);
  });

  test('nonexistent token returns no rows', async () => {
    const result = await pool.query(
      `SELECT * FROM customer_portal_tokens
       WHERE token = $1 AND expires_at > NOW()`,
      ['nonexistent-token-that-does-not-exist-in-db-at-all-ever-xxxxx']
    );

    expect(result.rows).toHaveLength(0);
  });
});

// ─────────────────────────────────────────────
// Portal Token → Data Access
// ─────────────────────────────────────────────

describe('Portal Data Access', () => {
  test('valid token grants access to customer invoices', async () => {
    const cust = await createTestCustomer({ email: `inv_access_${RUN_ID}@example.com` });
    const tokenRecord = await createPortalToken(cust.id, cust.email);

    // Create an invoice for this customer
    const invResult = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, status, total, line_items)
       VALUES ($1, $2, $3, $4, 'sent', 150, '[]') RETURNING *`,
      [`TEST-PORTAL-${RUN_ID}`, cust.id, cust.name, cust.email]
    );
    createdInvoiceIds.push(invResult.rows[0].id);

    // Step 1: validate token (same as validatePortalToken helper)
    const tokenCheck = await pool.query(
      'SELECT customer_id, email FROM customer_portal_tokens WHERE token = $1 AND expires_at > NOW()',
      [tokenRecord.token]
    );
    expect(tokenCheck.rows).toHaveLength(1);

    // Step 2: fetch invoices for that customer
    const { customer_id } = tokenCheck.rows[0];
    const invoices = await pool.query(
      'SELECT * FROM invoices WHERE customer_id = $1 ORDER BY created_at DESC',
      [customer_id]
    );

    const found = invoices.rows.find(i => i.id === invResult.rows[0].id);
    expect(found).toBeTruthy();
    expect(parseFloat(found.total)).toBe(150);
  });

  test('expired token blocks data access', async () => {
    const cust = await createTestCustomer({ email: `blocked_${RUN_ID}@example.com` });
    const expiredToken = await createPortalToken(cust.id, cust.email, {
      expires_at: new Date(Date.now() - 86400000),
    });

    // Step 1: validate token — should fail
    const tokenCheck = await pool.query(
      'SELECT customer_id, email FROM customer_portal_tokens WHERE token = $1 AND expires_at > NOW()',
      [expiredToken.token]
    );
    expect(tokenCheck.rows).toHaveLength(0);
    // Handler would return 404 here — no data access
  });

  test('token cannot access other customers invoices', async () => {
    const cust1 = await createTestCustomer({ email: `cust1_${RUN_ID}@example.com` });
    const cust2 = await createTestCustomer({ email: `cust2_${RUN_ID}@example.com` });
    const token1 = await createPortalToken(cust1.id, cust1.email);

    // Create invoice for cust2
    const invResult = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_id, customer_name, total, line_items)
       VALUES ($1, $2, $3, 200, '[]') RETURNING *`,
      [`TEST-OTHER-${RUN_ID}`, cust2.id, cust2.name]
    );
    createdInvoiceIds.push(invResult.rows[0].id);

    // Validate token1 — belongs to cust1
    const tokenCheck = await pool.query(
      'SELECT customer_id FROM customer_portal_tokens WHERE token = $1 AND expires_at > NOW()',
      [token1.token]
    );
    const customer_id = tokenCheck.rows[0].customer_id;
    expect(customer_id).toBe(cust1.id);

    // Query invoices for cust1 — should NOT include cust2's invoice
    const invoices = await pool.query(
      'SELECT * FROM invoices WHERE customer_id = $1',
      [customer_id]
    );
    const otherInv = invoices.rows.find(i => i.id === invResult.rows[0].id);
    expect(otherInv).toBeUndefined();
  });
});

// ─────────────────────────────────────────────
// Payment Token (separate from portal token)
// ─────────────────────────────────────────────

describe('Payment Token System', () => {
  test('payment token lives on invoice, not in separate table', async () => {
    const token = generateToken();
    const invResult = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_name, total, line_items, payment_token, payment_token_created_at)
       VALUES ($1, $2, 100, '[]', $3, CURRENT_TIMESTAMP) RETURNING *`,
      [`TEST-PAYTOK-${RUN_ID}`, TEST_NAME, token]
    );
    createdInvoiceIds.push(invResult.rows[0].id);

    // Look up by payment token — same as GET /api/pay/:token
    const lookup = await pool.query('SELECT id FROM invoices WHERE payment_token = $1', [token]);
    expect(lookup.rows).toHaveLength(1);
    expect(lookup.rows[0].id).toBe(invResult.rows[0].id);
  });

  test('payment token has no expiry (unlike portal token)', async () => {
    const token = generateToken();
    const invResult = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_name, total, line_items, payment_token, payment_token_created_at)
       VALUES ($1, $2, 100, '[]', $3, NOW() - INTERVAL '365 days') RETURNING *`,
      [`TEST-OLDPAY-${RUN_ID}`, TEST_NAME, token]
    );
    createdInvoiceIds.push(invResult.rows[0].id);

    // Even a year-old token still works (no expiry check in the query)
    const lookup = await pool.query('SELECT id FROM invoices WHERE payment_token = $1', [token]);
    expect(lookup.rows).toHaveLength(1);
  });
});
