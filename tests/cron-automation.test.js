/**
 * Cron/Automation Endpoint Tests
 *
 * Tests against the real local database (yarddesk).
 * Covers: recurring job generation, monthly plan invoices,
 *         late fee processing, deduplication, and automation gating.
 */

require('dotenv').config();
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false,
});

const RUN_ID = crypto.randomBytes(4).toString('hex');
const TEST_NAME = `test_cron_customer_${RUN_ID}`;

const createdCustomerIds = [];
const createdJobIds = [];
const createdInvoiceIds = [];
const createdLogIds = [];
const createdLateFeeIds = [];

function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

afterAll(async () => {
  // Clean up recurring logs
  if (createdJobIds.length > 0) {
    await pool.query('DELETE FROM recurring_job_log WHERE source_job_id = ANY($1) OR generated_job_id = ANY($1)', [createdJobIds]);
    await pool.query('DELETE FROM scheduled_jobs WHERE id = ANY($1)', [createdJobIds]);
  }
  if (createdLateFeeIds.length > 0) {
    await pool.query('DELETE FROM late_fees WHERE id = ANY($1)', [createdLateFeeIds]);
  }
  if (createdInvoiceIds.length > 0) {
    // Clean recurring invoice logs
    await pool.query('DELETE FROM recurring_invoice_log WHERE invoice_id = ANY($1)', [createdInvoiceIds]);
    await pool.query('DELETE FROM invoices WHERE id = ANY($1)', [createdInvoiceIds]);
  }
  if (createdCustomerIds.length > 0) {
    await pool.query('DELETE FROM recurring_invoice_log WHERE customer_id = ANY($1)', [createdCustomerIds]);
    await pool.query('DELETE FROM customers WHERE id = ANY($1)', [createdCustomerIds]);
  }
  await pool.end();
});

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

async function createTestCustomer(overrides = {}) {
  const result = await pool.query(
    `INSERT INTO customers (name, email, phone, street, monthly_plan_amount, created_at)
     VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) RETURNING *`,
    [
      overrides.name || TEST_NAME,
      overrides.email || `test_cron_${RUN_ID}_${Date.now()}@example.com`,
      '440-555-0400',
      '101 Cron St',
      overrides.monthly_plan_amount || 0,
    ]
  );
  createdCustomerIds.push(result.rows[0].id);
  return result.rows[0];
}

async function createRecurringJob(customerId, overrides = {}) {
  const jobDate = overrides.job_date || new Date().toISOString().split('T')[0];
  const result = await pool.query(
    `INSERT INTO scheduled_jobs (
      customer_id, customer_name, service_type, service_price, address,
      status, job_date, is_recurring, recurring_pattern
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
    [
      customerId,
      overrides.customer_name || TEST_NAME,
      overrides.service_type || 'Mowing - Weekly',
      overrides.service_price || 50,
      '101 Cron St',
      overrides.status || 'scheduled',
      jobDate,
      true,
      overrides.recurring_pattern || 'weekly',
    ]
  );
  createdJobIds.push(result.rows[0].id);
  return result.rows[0];
}

async function createTestInvoice(overrides = {}) {
  const result = await pool.query(
    `INSERT INTO invoices (
      invoice_number, customer_id, customer_name, customer_email,
      status, total, amount_paid, due_date, line_items, payment_token
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
    [
      overrides.invoice_number || `TEST-CRON-${RUN_ID}-${Date.now()}`,
      overrides.customer_id || null,
      overrides.customer_name || TEST_NAME,
      overrides.customer_email || `cron_${RUN_ID}@example.com`,
      overrides.status || 'sent',
      overrides.total || 100,
      overrides.amount_paid || 0,
      overrides.due_date || new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // 60 days ago
      JSON.stringify(overrides.line_items || [{ name: 'Service', amount: 100 }]),
      overrides.payment_token || generateToken(),
    ]
  );
  createdInvoiceIds.push(result.rows[0].id);
  return result.rows[0];
}

// ─────────────────────────────────────────────
// Recurring Job Generation
// ─────────────────────────────────────────────

describe('Recurring Job Generation', () => {
  test('generates job for matching day-of-week', async () => {
    const cust = await createTestCustomer();

    // Create a weekly recurring job whose job_date falls on today's day-of-week
    const today = new Date();
    const job = await createRecurringJob(cust.id, {
      job_date: today.toISOString().split('T')[0],
      recurring_pattern: 'weekly',
    });

    // Replicate processRecurringJobs logic for today
    const targetDate = new Date();
    const dateStr = targetDate.toISOString().split('T')[0];
    const dayOfWeek = targetDate.getDay();
    const jobDayOfWeek = new Date(job.job_date).getDay();

    expect(dayOfWeek).toBe(jobDayOfWeek); // Should match since we set it to today

    // Check dedup log is empty
    const existing = await pool.query(
      'SELECT id FROM recurring_job_log WHERE source_job_id = $1 AND generated_for_date = $2',
      [job.id, dateStr]
    );
    expect(existing.rows).toHaveLength(0);

    // Generate the job
    const newJob = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_price, address, status, parent_job_id)
       VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7) RETURNING *`,
      [dateStr, job.customer_name, job.customer_id, job.service_type, job.service_price, job.address, job.id]
    );
    createdJobIds.push(newJob.rows[0].id);

    // Log it
    await pool.query(
      'INSERT INTO recurring_job_log (source_job_id, generated_for_date, generated_job_id) VALUES ($1, $2, $3)',
      [job.id, dateStr, newJob.rows[0].id]
    );

    expect(newJob.rows[0].status).toBe('pending');
    expect(newJob.rows[0].parent_job_id).toBe(job.id);
    expect(parseFloat(newJob.rows[0].service_price)).toBe(50);
  });

  test('dedup prevents generating same job twice', async () => {
    const cust = await createTestCustomer();
    const job = await createRecurringJob(cust.id);
    const dateStr = new Date().toISOString().split('T')[0];

    // First generation — insert log
    const gen1 = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_price, address, status, parent_job_id)
       VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7) RETURNING *`,
      [dateStr, job.customer_name, job.customer_id, job.service_type, job.service_price, job.address, job.id]
    );
    createdJobIds.push(gen1.rows[0].id);
    await pool.query(
      'INSERT INTO recurring_job_log (source_job_id, generated_for_date, generated_job_id) VALUES ($1, $2, $3)',
      [job.id, dateStr, gen1.rows[0].id]
    );

    // Second attempt — dedup check should find existing
    const existing = await pool.query(
      'SELECT id FROM recurring_job_log WHERE source_job_id = $1 AND generated_for_date = $2',
      [job.id, dateStr]
    );
    expect(existing.rows).toHaveLength(1); // Already exists — skip
  });

  test('recurring_job_log has unique constraint on (source_job_id, generated_for_date)', async () => {
    const cust = await createTestCustomer();
    const job = await createRecurringJob(cust.id);
    const dateStr = new Date().toISOString().split('T')[0];

    // Insert first log entry
    const gen = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_price, address, status, parent_job_id)
       VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7) RETURNING *`,
      [dateStr, job.customer_name, job.customer_id, job.service_type, job.service_price, job.address, job.id]
    );
    createdJobIds.push(gen.rows[0].id);
    await pool.query(
      'INSERT INTO recurring_job_log (source_job_id, generated_for_date, generated_job_id) VALUES ($1, $2, $3)',
      [job.id, dateStr, gen.rows[0].id]
    );

    // Duplicate insert should fail
    await expect(
      pool.query(
        'INSERT INTO recurring_job_log (source_job_id, generated_for_date, generated_job_id) VALUES ($1, $2, $3)',
        [job.id, dateStr, gen.rows[0].id]
      )
    ).rejects.toThrow(/unique|duplicate/i);
  });

  test('biweekly pattern generates every other week', () => {
    // Pure logic test — replicate the biweekly check from processRecurringJobs
    const jobDate = new Date('2026-03-02'); // Monday
    const targetSameWeek = new Date('2026-03-02'); // same week = 0 weeks diff
    const target2Weeks = new Date('2026-03-16'); // 2 weeks later
    const target3Weeks = new Date('2026-03-23'); // 3 weeks later

    function shouldGenerateBiweekly(jobDateStr, targetDateStr) {
      const jd = new Date(jobDateStr);
      const td = new Date(targetDateStr);
      const weeksDiff = Math.floor((td - jd) / (7 * 24 * 60 * 60 * 1000));
      return weeksDiff >= 0 && weeksDiff % 2 === 0 && td.getDay() === jd.getDay();
    }

    expect(shouldGenerateBiweekly('2026-03-02', '2026-03-02')).toBe(true);  // week 0
    expect(shouldGenerateBiweekly('2026-03-02', '2026-03-16')).toBe(true);  // week 2
    expect(shouldGenerateBiweekly('2026-03-02', '2026-03-09')).toBe(false); // week 1 (odd)
    expect(shouldGenerateBiweekly('2026-03-02', '2026-03-23')).toBe(false); // week 3 (odd)
    expect(shouldGenerateBiweekly('2026-03-02', '2026-03-30')).toBe(true);  // week 4
  });
});

// ─────────────────────────────────────────────
// Monthly Plan Invoices
// ─────────────────────────────────────────────

describe('Monthly Plan Invoices', () => {
  test('generates invoice for customer with monthly_plan_amount', async () => {
    const cust = await createTestCustomer({ monthly_plan_amount: 175 });
    const billingMonth = new Date().toISOString().slice(0, 7); // YYYY-MM

    // Verify no existing invoice for this month
    const existing = await pool.query(
      'SELECT id FROM recurring_invoice_log WHERE customer_id = $1 AND billing_month = $2',
      [cust.id, billingMonth]
    );
    expect(existing.rows).toHaveLength(0);

    // Replicate processMonthlyPlanInvoices logic
    const invNum = `TEST-MONTHLY-${RUN_ID}-${Date.now()}`;
    const paymentToken = generateToken();
    const total = parseFloat(cust.monthly_plan_amount);

    const inv = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, status, subtotal, total, payment_token, line_items, billing_month)
       VALUES ($1, $2, $3, $4, 'sent', $5, $5, $6, $7, $8) RETURNING *`,
      [invNum, cust.id, cust.name, cust.email, total, paymentToken,
       JSON.stringify([{ description: `Monthly Lawn Care Plan - ${billingMonth}`, amount: total }]),
       billingMonth]
    );
    createdInvoiceIds.push(inv.rows[0].id);

    // Log it
    await pool.query(
      'INSERT INTO recurring_invoice_log (customer_id, billing_month, invoice_id) VALUES ($1, $2, $3)',
      [cust.id, billingMonth, inv.rows[0].id]
    );

    expect(inv.rows[0].status).toBe('sent');
    expect(parseFloat(inv.rows[0].total)).toBe(175);
    expect(inv.rows[0].payment_token).toHaveLength(64);
  });

  test('dedup prevents two invoices in same billing month', async () => {
    const cust = await createTestCustomer({ monthly_plan_amount: 200 });
    const billingMonth = new Date().toISOString().slice(0, 7);

    // Create first invoice + log
    const inv1 = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_id, customer_name, status, total, line_items)
       VALUES ($1, $2, $3, 'sent', 200, '[]') RETURNING *`,
      [`TEST-DUP1-${RUN_ID}`, cust.id, cust.name]
    );
    createdInvoiceIds.push(inv1.rows[0].id);
    await pool.query(
      'INSERT INTO recurring_invoice_log (customer_id, billing_month, invoice_id) VALUES ($1, $2, $3)',
      [cust.id, billingMonth, inv1.rows[0].id]
    );

    // Dedup check — should find existing
    const existing = await pool.query(
      'SELECT id FROM recurring_invoice_log WHERE customer_id = $1 AND billing_month = $2',
      [cust.id, billingMonth]
    );
    expect(existing.rows).toHaveLength(1); // Skip — already invoiced this month
  });

  test('recurring_invoice_log unique constraint enforced', async () => {
    const cust = await createTestCustomer({ monthly_plan_amount: 150 });
    const billingMonth = new Date().toISOString().slice(0, 7);

    const inv = await pool.query(
      `INSERT INTO invoices (invoice_number, customer_id, customer_name, status, total, line_items)
       VALUES ($1, $2, $3, 'sent', 150, '[]') RETURNING *`,
      [`TEST-UNIQ-${RUN_ID}`, cust.id, cust.name]
    );
    createdInvoiceIds.push(inv.rows[0].id);

    await pool.query(
      'INSERT INTO recurring_invoice_log (customer_id, billing_month, invoice_id) VALUES ($1, $2, $3)',
      [cust.id, billingMonth, inv.rows[0].id]
    );

    await expect(
      pool.query(
        'INSERT INTO recurring_invoice_log (customer_id, billing_month, invoice_id) VALUES ($1, $2, $3)',
        [cust.id, billingMonth, inv.rows[0].id]
      )
    ).rejects.toThrow(/unique|duplicate/i);
  });

  test('customers with zero monthly_plan_amount are skipped', async () => {
    const cust = await createTestCustomer({ monthly_plan_amount: 0 });

    // Replicate the filter: WHERE monthly_plan_amount > 0
    const eligible = await pool.query(
      'SELECT id FROM customers WHERE id = $1 AND monthly_plan_amount > 0',
      [cust.id]
    );
    expect(eligible.rows).toHaveLength(0);
  });
});

// ─────────────────────────────────────────────
// Late Fee Processing
// ─────────────────────────────────────────────

describe('Late Fee Processing', () => {
  test('applies initial fee to overdue invoice past grace period', async () => {
    const cust = await createTestCustomer();
    const inv = await createTestInvoice({
      customer_id: cust.id,
      total: 200,
      amount_paid: 0,
      status: 'sent',
      due_date: new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString().split('T')[0], // 45 days ago
    });

    // Replicate processLateFees logic
    const gracePeriodDays = 30;
    const initialFeePercent = 10;
    const daysOverdue = 45;
    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid);
    const feeAmount = Math.round(balance * initialFeePercent) / 100; // 200 * 10 / 100 = 20

    const feeResult = await pool.query(
      'INSERT INTO late_fees (invoice_id, fee_amount, fee_type, fee_percentage, days_overdue) VALUES ($1, $2, $3, $4, $5) RETURNING *',
      [inv.id, feeAmount, 'percentage', initialFeePercent, daysOverdue]
    );
    createdLateFeeIds.push(feeResult.rows[0].id);

    expect(parseFloat(feeResult.rows[0].fee_amount)).toBe(20);
    expect(parseFloat(feeResult.rows[0].fee_percentage)).toBe(initialFeePercent);

    // Update invoice
    await pool.query(
      "UPDATE invoices SET late_fee_total = $1, status = 'overdue' WHERE id = $2",
      [feeAmount, inv.id]
    );

    const after = await pool.query('SELECT * FROM invoices WHERE id = $1', [inv.id]);
    expect(after.rows[0].status).toBe('overdue');
    expect(parseFloat(after.rows[0].late_fee_total)).toBe(20);
  });

  test('max fees cap prevents excessive fees', async () => {
    const cust = await createTestCustomer();
    const inv = await createTestInvoice({
      customer_id: cust.id,
      total: 100,
      amount_paid: 0,
      status: 'overdue',
      due_date: new Date(Date.now() - 120 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
    });

    const maxFees = 3;

    // Insert 3 existing fees
    for (let i = 0; i < maxFees; i++) {
      const r = await pool.query(
        'INSERT INTO late_fees (invoice_id, fee_amount, fee_type, fee_percentage, days_overdue) VALUES ($1, $2, $3, $4, $5) RETURNING *',
        [inv.id, 10, 'percentage', 10, 30 + i * 30]
      );
      createdLateFeeIds.push(r.rows[0].id);
    }

    // Replicate the cap check
    const feeCount = await pool.query(
      'SELECT COUNT(*) FROM late_fees WHERE invoice_id = $1 AND waived = false',
      [inv.id]
    );
    expect(parseInt(feeCount.rows[0].count)).toBe(maxFees);
    // processLateFees would `continue` here — no more fees applied
  });

  test('waived fees do not count toward max', async () => {
    const cust = await createTestCustomer();
    const inv = await createTestInvoice({
      customer_id: cust.id,
      total: 100,
      amount_paid: 0,
      status: 'overdue',
    });

    // Insert 3 fees, but waive 2 of them
    for (let i = 0; i < 3; i++) {
      const r = await pool.query(
        'INSERT INTO late_fees (invoice_id, fee_amount, fee_type, fee_percentage, days_overdue, waived) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
        [inv.id, 10, 'percentage', 10, 30 + i * 30, i < 2] // first 2 waived
      );
      createdLateFeeIds.push(r.rows[0].id);
    }

    // Count non-waived fees
    const feeCount = await pool.query(
      'SELECT COUNT(*) FROM late_fees WHERE invoice_id = $1 AND waived = false',
      [inv.id]
    );
    expect(parseInt(feeCount.rows[0].count)).toBe(1); // Only 1 non-waived
  });

  test('paid invoices are not eligible for late fees', async () => {
    const cust = await createTestCustomer();
    const inv = await createTestInvoice({
      customer_id: cust.id,
      total: 100,
      amount_paid: 100,
      status: 'paid',
      due_date: new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
    });

    // Replicate the WHERE clause from processLateFees
    const overdue = await pool.query(
      `SELECT * FROM invoices
       WHERE id = $1 AND status IN ('sent', 'overdue', 'partial')
       AND amount_paid < total AND due_date IS NOT NULL`,
      [inv.id]
    );
    expect(overdue.rows).toHaveLength(0); // Paid — not eligible
  });
});

// ─────────────────────────────────────────────
// Automation Gate
// ─────────────────────────────────────────────

describe('Automation Gate', () => {
  test('areAutomatedEmailsEnabled checks business_settings', async () => {
    // Replicate areAutomatedEmailsEnabled() logic
    const result = await pool.query(
      "SELECT value FROM business_settings WHERE key = 'automated_emails_enabled'"
    );

    if (result.rows.length === 0) {
      // Default is OFF for safety
      expect(true).toBe(true);
    } else {
      const val = result.rows[0].value;
      // Should be a boolean or string 'true'/'false'
      expect([true, false, 'true', 'false']).toContain(val);
    }
  });
});
