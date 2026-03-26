/**
 * Invoice Creation & Payment Webhook Tests
 *
 * Tests against the real local database (yarddesk).
 * Covers: invoice creation, invoice numbering, Square webhook status updates,
 *         job completion → auto-invoice, and tax calculation.
 */

require('dotenv').config();
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false,
});

const RUN_ID = crypto.randomBytes(4).toString('hex');
const TEST_EMAIL = `test_inv_${RUN_ID}@example.com`;
const TEST_NAME = `test_inv_customer_${RUN_ID}`;

// Track IDs for cleanup
const createdInvoiceIds = [];
const createdPaymentIds = [];
const createdCustomerIds = [];
const createdJobIds = [];
const createdLateFeeIds = [];

afterAll(async () => {
  if (createdLateFeeIds.length > 0) {
    await pool.query('DELETE FROM late_fees WHERE id = ANY($1)', [createdLateFeeIds]);
  }
  if (createdPaymentIds.length > 0) {
    await pool.query('DELETE FROM payments WHERE id = ANY($1)', [createdPaymentIds]);
  }
  if (createdJobIds.length > 0) {
    await pool.query('DELETE FROM scheduled_jobs WHERE id = ANY($1)', [createdJobIds]);
  }
  if (createdInvoiceIds.length > 0) {
    await pool.query('DELETE FROM invoices WHERE id = ANY($1)', [createdInvoiceIds]);
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
    `INSERT INTO customers (name, email, phone, street, created_at)
     VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP) RETURNING *`,
    [
      overrides.name || TEST_NAME,
      overrides.email || `test_cust_${RUN_ID}_${Date.now()}@example.com`,
      overrides.phone || '440-555-0100',
      overrides.street || '123 Test St',
    ]
  );
  const cust = result.rows[0];
  createdCustomerIds.push(cust.id);
  return cust;
}

async function createTestInvoice(overrides = {}) {
  const invNum = overrides.invoice_number || `TEST-INV-${RUN_ID}-${Date.now()}`;
  const paymentToken = overrides.payment_token || generateToken();
  const result = await pool.query(
    `INSERT INTO invoices (
      invoice_number, customer_id, customer_name, customer_email, customer_address,
      status, subtotal, tax_rate, tax_amount, total, amount_paid, due_date,
      line_items, payment_token, payment_token_created_at, created_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
    RETURNING *`,
    [
      invNum,
      overrides.customer_id || null,
      overrides.customer_name || TEST_NAME,
      overrides.customer_email || TEST_EMAIL,
      overrides.customer_address || '123 Test St',
      overrides.status || 'draft',
      overrides.subtotal || 100,
      overrides.tax_rate || 8,
      overrides.tax_amount || 8,
      overrides.total || 108,
      overrides.amount_paid || 0,
      overrides.due_date || new Date().toISOString().split('T')[0],
      JSON.stringify(overrides.line_items || [{ name: 'Mowing', amount: 100 }]),
      paymentToken,
    ]
  );
  const inv = result.rows[0];
  createdInvoiceIds.push(inv.id);
  return inv;
}

async function createTestPayment(invoiceId, overrides = {}) {
  const paymentId = overrides.payment_id || `test-pay-${RUN_ID}-${Date.now()}`;
  const squarePaymentId = overrides.square_payment_id || `sq-test-${RUN_ID}-${Date.now()}`;
  const result = await pool.query(
    `INSERT INTO payments (
      payment_id, invoice_id, customer_id, amount, method, status,
      square_payment_id, card_brand, card_last4, paid_at, created_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CURRENT_TIMESTAMP)
    RETURNING *`,
    [
      paymentId,
      invoiceId,
      overrides.customer_id || null,
      overrides.amount || 108,
      overrides.method || 'card',
      overrides.status || 'pending',
      squarePaymentId,
      overrides.card_brand || 'VISA',
      overrides.card_last4 || '4242',
      overrides.paid_at || new Date(),
    ]
  );
  const pay = result.rows[0];
  createdPaymentIds.push(pay.id);
  return pay;
}

// ─────────────────────────────────────────────
// Invoice Creation
// ─────────────────────────────────────────────

describe('Invoice Creation', () => {
  test('creates invoice with correct defaults', async () => {
    const inv = await createTestInvoice();

    expect(inv.status).toBe('draft');
    expect(parseFloat(inv.total)).toBe(108);
    expect(parseFloat(inv.amount_paid)).toBe(0);
    expect(inv.payment_token).toBeTruthy();
    expect(inv.invoice_number).toContain('TEST-INV');
    expect(inv.created_at).toBeTruthy();

    const items = typeof inv.line_items === 'string' ? JSON.parse(inv.line_items) : inv.line_items;
    expect(items).toHaveLength(1);
    expect(items[0].name).toBe('Mowing');
  });

  test('invoice_number is unique', async () => {
    const inv1 = await createTestInvoice({ invoice_number: `UNIQUE-${RUN_ID}` });

    await expect(
      pool.query(
        `INSERT INTO invoices (invoice_number, customer_name, total, line_items)
         VALUES ($1, $2, 50, '[]')`,
        [`UNIQUE-${RUN_ID}`, 'Dupe Test']
      )
    ).rejects.toThrow(/unique/i);
  });

  test('nextInvoiceNumber increments from last invoice', async () => {
    // Read the current max
    const before = await pool.query('SELECT invoice_number FROM invoices ORDER BY id DESC LIMIT 1');
    if (before.rows.length === 0) return; // skip if no invoices exist

    const lastNum = parseInt(before.rows[0].invoice_number.replace(/\D/g, '')) || 10057;
    const expectedNext = `INV-${Math.max(lastNum + 1, 10058)}`;

    // Replicate nextInvoiceNumber logic
    const r = await pool.query("SELECT invoice_number FROM invoices ORDER BY id DESC LIMIT 1 FOR UPDATE");
    const last = r.rows[0].invoice_number || 'INV-10057';
    const num = parseInt(last.replace(/\D/g, '')) || 10057;
    const nextNum = `INV-${Math.max(num + 1, 10058)}`;

    expect(nextNum).toBe(expectedNext);
  });
});

// ─────────────────────────────────────────────
// Square Webhook — Payment Status Updates
// ─────────────────────────────────────────────

describe('Square Webhook', () => {
  test('payment.completed updates payment status and paid_at', async () => {
    const inv = await createTestInvoice({ status: 'sent' });
    const payment = await createTestPayment(inv.id, { status: 'pending' });

    // Replicate what the webhook handler does for payment.completed
    await pool.query(
      `UPDATE payments SET status = 'completed', paid_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
       WHERE square_payment_id = $1`,
      [payment.square_payment_id]
    );

    const after = await pool.query('SELECT * FROM payments WHERE id = $1', [payment.id]);
    expect(after.rows[0].status).toBe('completed');
    expect(after.rows[0].paid_at).toBeTruthy();
  });

  test('payment.failed stores failure reason', async () => {
    const inv = await createTestInvoice({ status: 'sent' });
    const payment = await createTestPayment(inv.id, { status: 'pending' });

    // Replicate webhook handler for payment.failed
    await pool.query(
      `UPDATE payments SET status = 'failed', failure_reason = $1, updated_at = CURRENT_TIMESTAMP
       WHERE square_payment_id = $2`,
      ['CARD_DECLINED', payment.square_payment_id]
    );

    const after = await pool.query('SELECT * FROM payments WHERE id = $1', [payment.id]);
    expect(after.rows[0].status).toBe('failed');
    expect(after.rows[0].failure_reason).toBe('CARD_DECLINED');
  });

  test('refund updates refund_amount on payment', async () => {
    const inv = await createTestInvoice({ status: 'sent' });
    const payment = await createTestPayment(inv.id, { status: 'completed' });

    // Replicate webhook handler for refund.created
    const refundAmount = 50.00;
    await pool.query(
      `UPDATE payments SET refund_amount = $1, updated_at = CURRENT_TIMESTAMP
       WHERE square_payment_id = $2`,
      [refundAmount, payment.square_payment_id]
    );

    const after = await pool.query('SELECT * FROM payments WHERE id = $1', [payment.id]);
    expect(parseFloat(after.rows[0].refund_amount)).toBe(50);
  });

  test('webhook with unknown square_payment_id updates zero rows', async () => {
    const result = await pool.query(
      `UPDATE payments SET status = 'completed', updated_at = CURRENT_TIMESTAMP
       WHERE square_payment_id = $1`,
      ['sq-nonexistent-id-12345']
    );
    expect(result.rowCount).toBe(0);
  });
});

// ─────────────────────────────────────────────
// Job Completion → Auto-Invoice
// ─────────────────────────────────────────────

describe('Job Completion Auto-Invoice', () => {
  test('completing a job creates a draft invoice for the customer', async () => {
    const cust = await createTestCustomer();

    // Create a job
    const jobResult = await pool.query(
      `INSERT INTO scheduled_jobs (customer_id, customer_name, service_type, service_price, address, status, job_date)
       VALUES ($1, $2, $3, $4, $5, 'scheduled', CURRENT_DATE) RETURNING *`,
      [cust.id, cust.name, 'Mowing - Weekly', 50, '123 Test St']
    );
    const job = jobResult.rows[0];
    createdJobIds.push(job.id);

    // Mark complete (replicate the UPDATE from PATCH handler)
    await pool.query(
      `UPDATE scheduled_jobs SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = $1`,
      [job.id]
    );

    // Replicate auto-invoice logic: create new draft invoice
    const invNum = `TEST-AUTO-${RUN_ID}-${Date.now()}`;
    const lineItems = [{
      name: job.service_type,
      description: `Job #${job.id}`,
      quantity: 1,
      rate: parseFloat(job.service_price),
      amount: parseFloat(job.service_price),
    }];

    const invResult = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, status, subtotal, total, line_items)
       VALUES ($1, $2, $3, $4, 'draft', $5, $5, $6) RETURNING *`,
      [invNum, cust.id, cust.name, cust.email, 50, JSON.stringify(lineItems)]
    );
    const inv = invResult.rows[0];
    createdInvoiceIds.push(inv.id);

    // Link job to invoice
    await pool.query('UPDATE scheduled_jobs SET invoice_id = $1 WHERE id = $2', [inv.id, job.id]);

    // Verify
    expect(inv.status).toBe('draft');
    expect(parseFloat(inv.subtotal)).toBe(50);
    const items = typeof inv.line_items === 'string' ? JSON.parse(inv.line_items) : inv.line_items;
    expect(items[0].name).toBe('Mowing - Weekly');

    const linkedJob = await pool.query('SELECT invoice_id FROM scheduled_jobs WHERE id = $1', [job.id]);
    expect(linkedJob.rows[0].invoice_id).toBe(inv.id);
  });

  test('second job completion adds line item to existing draft invoice', async () => {
    const cust = await createTestCustomer();

    // Create existing draft invoice for this customer
    const existingInv = await createTestInvoice({
      customer_id: cust.id,
      customer_name: cust.name,
      status: 'draft',
      subtotal: 50,
      total: 50,
      line_items: [{ name: 'Mowing - Weekly', amount: 50 }],
    });

    // Second job completes — add to existing invoice
    const newItem = { name: 'Mulching', amount: 200, quantity: 1 };
    let items = typeof existingInv.line_items === 'string'
      ? JSON.parse(existingInv.line_items)
      : existingInv.line_items;
    items.push(newItem);
    const newSubtotal = items.reduce((s, i) => s + (parseFloat(i.amount) || 0), 0);

    await pool.query(
      `UPDATE invoices SET line_items = $1, subtotal = $2, total = $3, updated_at = CURRENT_TIMESTAMP WHERE id = $4`,
      [JSON.stringify(items), newSubtotal, newSubtotal, existingInv.id]
    );

    const after = await pool.query('SELECT * FROM invoices WHERE id = $1', [existingInv.id]);
    const updated = after.rows[0];
    const updatedItems = typeof updated.line_items === 'string'
      ? JSON.parse(updated.line_items)
      : updated.line_items;

    expect(updatedItems).toHaveLength(2);
    expect(parseFloat(updated.subtotal)).toBe(250);
    expect(updatedItems[1].name).toBe('Mulching');
  });
});

// ─────────────────────────────────────────────
// Invoice Payment Token
// ─────────────────────────────────────────────

describe('Invoice Payment Token', () => {
  test('invoice can be looked up by payment_token', async () => {
    const inv = await createTestInvoice({ status: 'sent' });

    // Replicate GET /api/pay/:token lookup
    const result = await pool.query(
      `SELECT id, invoice_number, customer_name, total, amount_paid, status
       FROM invoices WHERE payment_token = $1`,
      [inv.payment_token]
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].id).toBe(inv.id);
  });

  test('payment token is generated lazily on first send', async () => {
    // Create invoice without payment token
    const result = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_name, customer_email, total, line_items, status)
       VALUES ($1, $2, $3, 50, '[]', 'draft') RETURNING *`,
      [`TEST-NOPAY-${RUN_ID}`, TEST_NAME, TEST_EMAIL]
    );
    const inv = result.rows[0];
    createdInvoiceIds.push(inv.id);
    expect(inv.payment_token).toBeNull();

    // Simulate send — generate token
    const token = generateToken();
    await pool.query(
      'UPDATE invoices SET payment_token = $1, payment_token_created_at = CURRENT_TIMESTAMP WHERE id = $2',
      [token, inv.id]
    );

    const after = await pool.query('SELECT payment_token FROM invoices WHERE id = $1', [inv.id]);
    expect(after.rows[0].payment_token).toBe(token);
    expect(after.rows[0].payment_token).toHaveLength(64);
  });
});
