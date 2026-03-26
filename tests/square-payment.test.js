/**
 * Square Payment Processing Tests
 *
 * Tests against the real local database (yarddesk).
 * Covers: payment recording, balance tracking, processing fee calculation,
 *         saved cards, ACH vs card differences, and payment-invoice linkage.
 *
 * Does NOT call Square API — tests the DB operations that surround it.
 */

require('dotenv').config();
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false,
});

const RUN_ID = crypto.randomBytes(4).toString('hex');
const TEST_EMAIL = `test_pay_${RUN_ID}@example.com`;
const TEST_NAME = `test_pay_customer_${RUN_ID}`;

const createdInvoiceIds = [];
const createdPaymentIds = [];
const createdCustomerIds = [];
const createdCardIds = [];

afterAll(async () => {
  if (createdCardIds.length > 0) {
    await pool.query('DELETE FROM customer_saved_cards WHERE id = ANY($1)', [createdCardIds]);
  }
  if (createdPaymentIds.length > 0) {
    await pool.query('DELETE FROM payments WHERE id = ANY($1)', [createdPaymentIds]);
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
      overrides.email || `test_paycust_${RUN_ID}_${Date.now()}@example.com`,
      '440-555-0200',
      '456 Pay St',
    ]
  );
  createdCustomerIds.push(result.rows[0].id);
  return result.rows[0];
}

async function createTestInvoice(overrides = {}) {
  const result = await pool.query(
    `INSERT INTO invoices (
      invoice_number, customer_id, customer_name, customer_email,
      status, subtotal, total, amount_paid, payment_token, line_items, created_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CURRENT_TIMESTAMP) RETURNING *`,
    [
      overrides.invoice_number || `TEST-PAY-${RUN_ID}-${Date.now()}`,
      overrides.customer_id || null,
      overrides.customer_name || TEST_NAME,
      overrides.customer_email || TEST_EMAIL,
      overrides.status || 'sent',
      overrides.subtotal || 100,
      overrides.total || 100,
      overrides.amount_paid || 0,
      overrides.payment_token || generateToken(),
      JSON.stringify(overrides.line_items || [{ name: 'Mowing', amount: 100 }]),
    ]
  );
  createdInvoiceIds.push(result.rows[0].id);
  return result.rows[0];
}

async function recordPayment(invoiceId, overrides = {}) {
  const result = await pool.query(
    `INSERT INTO payments (
      payment_id, invoice_id, customer_id, amount, method, status,
      square_payment_id, card_brand, card_last4, ach_bank_name, paid_at, created_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,CURRENT_TIMESTAMP) RETURNING *`,
    [
      overrides.payment_id || `pay-${RUN_ID}-${Date.now()}`,
      invoiceId,
      overrides.customer_id || null,
      overrides.amount || 100,
      overrides.method || 'card',
      overrides.status || 'completed',
      overrides.square_payment_id || `sq-${RUN_ID}-${Date.now()}`,
      overrides.card_brand || (overrides.method === 'ach' ? null : 'VISA'),
      overrides.card_last4 || (overrides.method === 'ach' ? null : '4242'),
      overrides.ach_bank_name || (overrides.method === 'ach' ? 'Chase Bank' : null),
      overrides.paid_at || new Date(),
    ]
  );
  createdPaymentIds.push(result.rows[0].id);
  return result.rows[0];
}

// ─────────────────────────────────────────────
// Payment Recording & Invoice Balance
// ─────────────────────────────────────────────

describe('Payment Recording', () => {
  test('card payment records correctly with card details', async () => {
    const inv = await createTestInvoice();
    const payment = await recordPayment(inv.id, {
      amount: 100,
      method: 'card',
      card_brand: 'MASTERCARD',
      card_last4: '5678',
    });

    expect(payment.method).toBe('card');
    expect(parseFloat(payment.amount)).toBe(100);
    expect(payment.card_brand).toBe('MASTERCARD');
    expect(payment.card_last4).toBe('5678');
    expect(payment.ach_bank_name).toBeNull();
    expect(payment.status).toBe('completed');
  });

  test('ACH payment records with bank name, no card details', async () => {
    const inv = await createTestInvoice();
    const payment = await recordPayment(inv.id, {
      amount: 100,
      method: 'ach',
      status: 'pending', // ACH starts pending
      ach_bank_name: 'Chase Bank',
    });

    expect(payment.method).toBe('ach');
    expect(payment.ach_bank_name).toBe('Chase Bank');
    expect(payment.card_brand).toBeNull();
    expect(payment.card_last4).toBeNull();
    expect(payment.status).toBe('pending'); // ACH doesn't clear immediately
  });

  test('payment updates invoice amount_paid and status', async () => {
    const inv = await createTestInvoice({ total: 200, amount_paid: 0, status: 'sent' });

    // Record payment
    await recordPayment(inv.id, { amount: 200 });

    // Replicate invoice update from payment handler
    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid);
    const newAmountPaid = parseFloat(inv.amount_paid) + balance;
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : 'partial';

    await pool.query(
      `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
      [newAmountPaid, newStatus, inv.id]
    );

    const after = await pool.query('SELECT * FROM invoices WHERE id = $1', [inv.id]);
    expect(parseFloat(after.rows[0].amount_paid)).toBe(200);
    expect(after.rows[0].status).toBe('paid');
    expect(after.rows[0].paid_at).toBeTruthy();
  });

  test('partial payment leaves invoice in partial status', async () => {
    const inv = await createTestInvoice({ total: 200, amount_paid: 0, status: 'sent' });

    await recordPayment(inv.id, { amount: 75 });

    // Partial update
    const newAmountPaid = 75;
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : 'partial';

    await pool.query(
      `UPDATE invoices SET amount_paid = $1, status = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
      [newAmountPaid, newStatus, inv.id]
    );

    const after = await pool.query('SELECT * FROM invoices WHERE id = $1', [inv.id]);
    expect(parseFloat(after.rows[0].amount_paid)).toBe(75);
    expect(after.rows[0].status).toBe('partial');
    expect(after.rows[0].paid_at).toBeNull();
  });
});

// ─────────────────────────────────────────────
// Processing Fee Calculation
// ─────────────────────────────────────────────

describe('Processing Fee Calculation', () => {
  test('card fee: 2.9% + $0.30', () => {
    const balance = 100;
    const pct = 2.9;
    const fixed = 0.30;
    const fee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;

    expect(fee).toBe(3.20);
  });

  test('ACH fee: 1% + $0', () => {
    const balance = 100;
    const pct = 1.0;
    const fixed = 0;
    const fee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;

    expect(fee).toBe(1.00);
  });

  test('fee on large amount rounds correctly', () => {
    const balance = 1234.56;
    const pct = 2.9;
    const fixed = 0.30;
    const fee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;

    expect(fee).toBe(36.10); // 35.80 + 0.30
  });

  test('fee with zero balance is just the fixed fee', () => {
    const balance = 0;
    const pct = 2.9;
    const fixed = 0.30;
    const fee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;

    expect(fee).toBe(0.30);
  });

  test('invoice tracks processing fee separately', async () => {
    const inv = await createTestInvoice({ total: 100, status: 'sent' });
    const processingFee = 3.20;

    // Replicate how payment handler stores fee on invoice
    await pool.query(
      `UPDATE invoices SET processing_fee = $1, processing_fee_passed = true, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
      [processingFee, inv.id]
    );

    const after = await pool.query('SELECT processing_fee, processing_fee_passed FROM invoices WHERE id = $1', [inv.id]);
    expect(parseFloat(after.rows[0].processing_fee)).toBe(3.20);
    expect(after.rows[0].processing_fee_passed).toBe(true);
  });
});

// ─────────────────────────────────────────────
// Saved Cards
// ─────────────────────────────────────────────

describe('Saved Cards', () => {
  test('saves card on file for customer', async () => {
    const cust = await createTestCustomer();

    const result = await pool.query(
      `INSERT INTO customer_saved_cards (
        customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name
      ) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [cust.id, `sq-card-${RUN_ID}`, 'VISA', '4242', 12, 2027, 'Test User']
    );
    createdCardIds.push(result.rows[0].id);

    expect(result.rows[0].enabled).toBe(true);
    expect(result.rows[0].card_brand).toBe('VISA');
    expect(result.rows[0].last4).toBe('4242');
  });

  test('disabled card is excluded from lookup', async () => {
    const cust = await createTestCustomer();

    const cardResult = await pool.query(
      `INSERT INTO customer_saved_cards (
        customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name, enabled
      ) VALUES ($1, $2, 'VISA', '9999', 6, 2028, 'Disabled Card', false) RETURNING *`,
      [cust.id, `sq-disabled-${RUN_ID}`]
    );
    createdCardIds.push(cardResult.rows[0].id);

    // Replicate the lookup from GET /api/pay/:token
    const lookup = await pool.query(
      'SELECT id FROM customer_saved_cards WHERE customer_id = $1 AND enabled = true',
      [cust.id]
    );
    const disabledFound = lookup.rows.find(r => r.id === cardResult.rows[0].id);
    expect(disabledFound).toBeUndefined();
  });

  test('multiple cards per customer allowed', async () => {
    const cust = await createTestCustomer();

    for (let i = 0; i < 3; i++) {
      const r = await pool.query(
        `INSERT INTO customer_saved_cards (customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name)
         VALUES ($1, $2, 'VISA', $3, 12, 2027, 'Multi Card') RETURNING *`,
        [cust.id, `sq-multi-${RUN_ID}-${i}`, String(1000 + i)]
      );
      createdCardIds.push(r.rows[0].id);
    }

    const cards = await pool.query(
      'SELECT * FROM customer_saved_cards WHERE customer_id = $1 AND enabled = true',
      [cust.id]
    );
    expect(cards.rows.length).toBeGreaterThanOrEqual(3);
  });
});

// ─────────────────────────────────────────────
// Payment-Invoice Linkage
// ─────────────────────────────────────────────

describe('Payment-Invoice Linkage', () => {
  test('payment references invoice via invoice_id', async () => {
    const inv = await createTestInvoice();
    const payment = await recordPayment(inv.id);

    const result = await pool.query(
      'SELECT i.invoice_number, p.amount FROM payments p JOIN invoices i ON p.invoice_id = i.id WHERE p.id = $1',
      [payment.id]
    );
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].invoice_number).toBe(inv.invoice_number);
  });

  test('multiple payments on same invoice creates payment history', async () => {
    const inv = await createTestInvoice({ total: 300 });

    await recordPayment(inv.id, { amount: 100, payment_id: `hist1-${RUN_ID}`, square_payment_id: `sq-hist1-${RUN_ID}` });
    await recordPayment(inv.id, { amount: 100, payment_id: `hist2-${RUN_ID}`, square_payment_id: `sq-hist2-${RUN_ID}` });
    await recordPayment(inv.id, { amount: 100, payment_id: `hist3-${RUN_ID}`, square_payment_id: `sq-hist3-${RUN_ID}` });

    // Replicate payment history lookup from GET /api/pay/:token
    const history = await pool.query(
      'SELECT amount, method, status, paid_at FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC',
      [inv.id]
    );
    expect(history.rows.length).toBeGreaterThanOrEqual(3);
  });

  test('payment_id is unique across all payments', async () => {
    const inv = await createTestInvoice();
    const uniqueId = `unique-pay-${RUN_ID}`;

    await recordPayment(inv.id, { payment_id: uniqueId, square_payment_id: `sq-u1-${RUN_ID}` });

    await expect(
      pool.query(
        `INSERT INTO payments (payment_id, invoice_id, amount, method, status)
         VALUES ($1, $2, 50, 'card', 'pending')`,
        [uniqueId, inv.id]
      )
    ).rejects.toThrow(/unique|duplicate/i);
  });
});
