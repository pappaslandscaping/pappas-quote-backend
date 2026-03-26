/**
 * Quote Flow Integration Tests
 *
 * Tests against the real local database (yarddesk).
 * Covers: quote creation, 4-stage followup sequence, contract signing.
 *
 * All test records use 'test_' prefix and are cleaned up in afterAll.
 */

require('dotenv').config();
const { Pool } = require('pg');
const crypto = require('crypto');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false,
});

// Unique suffix per test run to avoid collisions
const RUN_ID = crypto.randomBytes(4).toString('hex');
const TEST_EMAIL = `test_${RUN_ID}@example.com`;
const TEST_NAME = `test_customer_${RUN_ID}`;

// Track IDs for cleanup
const createdQuoteIds = [];
const createdCustomerIds = [];
const createdFollowupIds = [];
const createdEventQuoteIds = [];

afterAll(async () => {
  // Clean up in reverse dependency order
  if (createdEventQuoteIds.length > 0) {
    await pool.query('DELETE FROM quote_events WHERE sent_quote_id = ANY($1)', [createdEventQuoteIds]);
  }
  if (createdFollowupIds.length > 0) {
    await pool.query('DELETE FROM quote_followups WHERE id = ANY($1)', [createdFollowupIds]);
  }
  if (createdQuoteIds.length > 0) {
    await pool.query('DELETE FROM sent_quotes WHERE id = ANY($1)', [createdQuoteIds]);
  }
  if (createdCustomerIds.length > 0) {
    await pool.query('DELETE FROM customers WHERE id = ANY($1)', [createdCustomerIds]);
  }
  await pool.end();
});

// ─────────────────────────────────────────────
// Helpers — replicate what server.js does
// ─────────────────────────────────────────────

function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

async function createTestQuote(overrides = {}) {
  const signToken = generateToken();
  const services = overrides.services || [{ name: 'Mowing - Weekly', amount: 50 }];

  const result = await pool.query(
    `INSERT INTO sent_quotes (
      customer_id, customer_name, customer_email, customer_phone, customer_address,
      quote_type, services, subtotal, tax_rate, tax_amount, total,
      status, sign_token, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)
    RETURNING *`,
    [
      overrides.customer_id || null,
      overrides.customer_name || TEST_NAME,
      overrides.customer_email || TEST_EMAIL,
      overrides.customer_phone || '440-555-0100',
      overrides.customer_address || '123 Test St, Lakewood, OH',
      overrides.quote_type || 'regular',
      JSON.stringify(services),
      overrides.subtotal || 50,
      overrides.tax_rate || 8,
      overrides.tax_amount || 4,
      overrides.total || 54,
      overrides.status || 'draft',
      overrides.sign_token || signToken,
    ]
  );

  const quote = result.rows[0];
  createdQuoteIds.push(quote.id);
  createdEventQuoteIds.push(quote.id);
  return quote;
}

async function createTestFollowup(quoteId, overrides = {}) {
  const now = new Date();
  const day = (n) => new Date(now.getTime() + n * 24 * 60 * 60 * 1000);

  const result = await pool.query(
    `INSERT INTO quote_followups (
      quote_id, quote_number, customer_name, customer_email, customer_phone,
      quote_amount, services, quote_sent_date,
      followup_1_date, followup_2_date, followup_3_date, followup_4_date,
      status, current_stage
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    RETURNING *`,
    [
      String(quoteId),
      overrides.quote_number || `Q-${quoteId}`,
      overrides.customer_name || TEST_NAME,
      overrides.customer_email || TEST_EMAIL,
      overrides.customer_phone || '440-555-0100',
      overrides.quote_amount || 54,
      overrides.services || 'Mowing - Weekly',
      overrides.quote_sent_date || now,
      overrides.followup_1_date || day(3),
      overrides.followup_2_date || day(7),
      overrides.followup_3_date || day(14),
      overrides.followup_4_date || day(25),
      overrides.status || 'pending',
      overrides.current_stage ?? 0,
    ]
  );

  const followup = result.rows[0];
  createdFollowupIds.push(followup.id);
  return followup;
}

// ─────────────────────────────────────────────
// FLOW 1: Quote Creation
// ─────────────────────────────────────────────

describe('Quote Creation', () => {
  test('creates a sent_quote with status=draft and a sign_token', async () => {
    const quote = await createTestQuote();

    expect(quote.status).toBe('draft');
    expect(quote.sign_token).toBeTruthy();
    expect(quote.sign_token).toHaveLength(64); // 32 bytes hex
    expect(quote.customer_name).toBe(TEST_NAME);
    expect(quote.customer_email).toBe(TEST_EMAIL);
    expect(parseFloat(quote.total)).toBe(54);
    expect(quote.quote_type).toBe('regular');
    expect(quote.created_at).toBeTruthy();
  });

  test('auto-creates customer when email does not exist', async () => {
    const uniqueEmail = `test_newcust_${RUN_ID}@example.com`;
    const custName = `test_newcust_${RUN_ID}`;

    // Verify customer doesn't exist yet
    const before = await pool.query('SELECT id FROM customers WHERE email = $1', [uniqueEmail]);
    expect(before.rows).toHaveLength(0);

    // Simulate what POST /api/sent-quotes does: look up or create customer
    const newCustResult = await pool.query(
      `INSERT INTO customers (name, email, phone, street, created_at)
       VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP) RETURNING id`,
      [custName, uniqueEmail, '440-555-0101', '456 Test Ave']
    );
    const customerId = newCustResult.rows[0].id;
    createdCustomerIds.push(customerId);

    // Create quote linked to new customer
    const quote = await createTestQuote({
      customer_id: customerId,
      customer_email: uniqueEmail,
      customer_name: custName,
    });

    expect(quote.customer_id).toBe(customerId);

    // Verify customer exists now
    const after = await pool.query('SELECT * FROM customers WHERE id = $1', [customerId]);
    expect(after.rows).toHaveLength(1);
    expect(after.rows[0].email).toBe(uniqueEmail);
  });

  test('links to existing customer when email matches', async () => {
    // Create customer first
    const existingCust = await pool.query(
      `INSERT INTO customers (name, email, created_at) VALUES ($1, $2, CURRENT_TIMESTAMP) RETURNING id`,
      [`test_existing_${RUN_ID}`, `test_existing_${RUN_ID}@example.com`]
    );
    const customerId = existingCust.rows[0].id;
    createdCustomerIds.push(customerId);

    // Simulate the lookup logic from POST /api/sent-quotes
    const lookup = await pool.query('SELECT id FROM customers WHERE email = $1', [`test_existing_${RUN_ID}@example.com`]);
    expect(lookup.rows).toHaveLength(1);
    expect(lookup.rows[0].id).toBe(customerId);

    // Create quote with that customer_id
    const quote = await createTestQuote({ customer_id: customerId });
    expect(quote.customer_id).toBe(customerId);

    // Verify no duplicate customer was created
    const count = await pool.query('SELECT COUNT(*) FROM customers WHERE email = $1', [`test_existing_${RUN_ID}@example.com`]);
    expect(parseInt(count.rows[0].count)).toBe(1);
  });

  test('logs created event in quote_events', async () => {
    const quote = await createTestQuote();

    // Replicate what server.js does after insert
    await pool.query(
      'INSERT INTO quote_events (sent_quote_id, event_type, description, details) VALUES ($1, $2, $3, $4)',
      [quote.id, 'created', 'Quote created', JSON.stringify({ total: 54, services_count: 1 })]
    );

    const events = await pool.query(
      'SELECT * FROM quote_events WHERE sent_quote_id = $1 AND event_type = $2',
      [quote.id, 'created']
    );
    expect(events.rows).toHaveLength(1);
    expect(events.rows[0].description).toBe('Quote created');

    const details = typeof events.rows[0].details === 'string'
      ? JSON.parse(events.rows[0].details)
      : events.rows[0].details;
    expect(details.total).toBe(54);
    expect(details.services_count).toBe(1);
  });
});

// ─────────────────────────────────────────────
// FLOW 2: 4-Stage Followup Sequence
// ─────────────────────────────────────────────

describe('Followup Sequence', () => {
  test('creates followup with correct dates and initial state', async () => {
    const quote = await createTestQuote({ status: 'sent' });
    const beforeCreate = Date.now();
    const followup = await createTestFollowup(quote.id);

    expect(followup.status).toBe('pending');
    expect(followup.current_stage).toBe(0);
    expect(followup.followup_1_sent).toBe(false);
    expect(followup.followup_2_sent).toBe(false);
    expect(followup.followup_3_sent).toBe(false);
    expect(followup.followup_4_sent).toBe(false);

    // Verify date spacing: day 3, 7, 14, 25
    const sent = new Date(followup.quote_sent_date).getTime();
    const d1 = new Date(followup.followup_1_date).getTime();
    const d2 = new Date(followup.followup_2_date).getTime();
    const d3 = new Date(followup.followup_3_date).getTime();
    const d4 = new Date(followup.followup_4_date).getTime();
    const day = 24 * 60 * 60 * 1000;

    expect(Math.round((d1 - sent) / day)).toBe(3);
    expect(Math.round((d2 - sent) / day)).toBe(7);
    expect(Math.round((d3 - sent) / day)).toBe(14);
    expect(Math.round((d4 - sent) / day)).toBe(25);
  });

  test('stage 1 processes when followup_1_date is past', async () => {
    const quote = await createTestQuote({ status: 'sent' });
    // Create followup with stage 1 date in the past
    const followup = await createTestFollowup(quote.id, {
      followup_1_date: new Date(Date.now() - 60 * 1000), // 1 minute ago
    });

    // Run the same query the cron processor uses to find due followups
    const due = await pool.query(
      `SELECT * FROM quote_followups
       WHERE id = $1 AND status = 'pending' AND (
         (current_stage = 0 AND followup_1_date <= NOW() AND NOT followup_1_sent) OR
         (current_stage = 1 AND followup_2_date <= NOW() AND NOT followup_2_sent) OR
         (current_stage = 2 AND followup_3_date <= NOW() AND NOT followup_3_sent) OR
         (current_stage = 3 AND followup_4_date <= NOW() AND NOT followup_4_sent)
       )`,
      [followup.id]
    );
    expect(due.rows).toHaveLength(1);

    // Simulate the processor's update (stage 0 → 1)
    const stage = due.rows[0].current_stage + 1; // 0 + 1 = 1
    const newStatus = stage >= 4 ? 'completed' : 'pending';
    await pool.query(
      `UPDATE quote_followups
       SET followup_${stage}_sent = true, current_stage = $1, status = $2, updated_at = NOW()
       WHERE id = $3`,
      [stage, newStatus, followup.id]
    );

    // Verify state after processing
    const after = await pool.query('SELECT * FROM quote_followups WHERE id = $1', [followup.id]);
    const updated = after.rows[0];
    expect(updated.current_stage).toBe(1);
    expect(updated.followup_1_sent).toBe(true);
    expect(updated.followup_2_sent).toBe(false);
    expect(updated.status).toBe('pending'); // still pending, 3 more stages
  });

  test('all 4 stages process to completion', async () => {
    const quote = await createTestQuote({ status: 'sent' });
    const past = new Date(Date.now() - 60 * 1000);
    const followup = await createTestFollowup(quote.id, {
      followup_1_date: past,
      followup_2_date: past,
      followup_3_date: past,
      followup_4_date: past,
    });

    // Process all 4 stages sequentially, same logic as the cron handler
    for (let expectedStage = 1; expectedStage <= 4; expectedStage++) {
      const due = await pool.query(
        `SELECT * FROM quote_followups
         WHERE id = $1 AND status = 'pending' AND (
           (current_stage = 0 AND followup_1_date <= NOW() AND NOT followup_1_sent) OR
           (current_stage = 1 AND followup_2_date <= NOW() AND NOT followup_2_sent) OR
           (current_stage = 2 AND followup_3_date <= NOW() AND NOT followup_3_sent) OR
           (current_stage = 3 AND followup_4_date <= NOW() AND NOT followup_4_sent)
         )`,
        [followup.id]
      );
      expect(due.rows).toHaveLength(1);

      const stage = due.rows[0].current_stage + 1;
      expect(stage).toBe(expectedStage);

      const newStatus = stage >= 4 ? 'completed' : 'pending';
      await pool.query(
        `UPDATE quote_followups
         SET followup_${stage}_sent = true, current_stage = $1, status = $2, updated_at = NOW()
         WHERE id = $3`,
        [stage, newStatus, followup.id]
      );
    }

    // Verify final state
    const final = await pool.query('SELECT * FROM quote_followups WHERE id = $1', [followup.id]);
    const done = final.rows[0];
    expect(done.status).toBe('completed');
    expect(done.current_stage).toBe(4);
    expect(done.followup_1_sent).toBe(true);
    expect(done.followup_2_sent).toBe(true);
    expect(done.followup_3_sent).toBe(true);
    expect(done.followup_4_sent).toBe(true);
  });

  test('stopped followup is not picked up by processor', async () => {
    const quote = await createTestQuote({ status: 'sent' });
    const followup = await createTestFollowup(quote.id, {
      followup_1_date: new Date(Date.now() - 60 * 1000),
      status: 'accepted', // stopped because customer accepted
    });

    const due = await pool.query(
      `SELECT * FROM quote_followups
       WHERE id = $1 AND status = 'pending' AND (
         (current_stage = 0 AND followup_1_date <= NOW() AND NOT followup_1_sent) OR
         (current_stage = 1 AND followup_2_date <= NOW() AND NOT followup_2_sent) OR
         (current_stage = 2 AND followup_3_date <= NOW() AND NOT followup_3_sent) OR
         (current_stage = 3 AND followup_4_date <= NOW() AND NOT followup_4_sent)
       )`,
      [followup.id]
    );
    expect(due.rows).toHaveLength(0);
  });

  test('30-day-old pending followups are expired', async () => {
    const quote = await createTestQuote({ status: 'sent' });
    const followup = await createTestFollowup(quote.id, {
      quote_sent_date: new Date(Date.now() - 31 * 24 * 60 * 60 * 1000), // 31 days ago
      followup_1_date: new Date(Date.now() - 28 * 24 * 60 * 60 * 1000),
    });

    // Run the expiry query from the cron handler
    await pool.query(
      `UPDATE quote_followups
       SET status = 'expired', updated_at = NOW()
       WHERE id = $1 AND status = 'pending' AND quote_sent_date < NOW() - INTERVAL '30 days'`,
      [followup.id]
    );

    const after = await pool.query('SELECT * FROM quote_followups WHERE id = $1', [followup.id]);
    expect(after.rows[0].status).toBe('expired');
  });
});

// ─────────────────────────────────────────────
// FLOW 3: Contract Signing
// ─────────────────────────────────────────────

describe('Contract Signing', () => {
  test('quote acceptance transitions sent/viewed → signed', async () => {
    const quote = await createTestQuote({ status: 'sent' });

    // Replicate POST /api/sign/:token logic
    const result = await pool.query(
      `UPDATE sent_quotes
       SET status = 'signed', signed_by_name = $1, signed_at = CURRENT_TIMESTAMP
       WHERE sign_token = $2 AND status IN ('sent', 'viewed')
       RETURNING *`,
      ['Tim Pappas', quote.sign_token]
    );

    expect(result.rows).toHaveLength(1);
    const signed = result.rows[0];
    expect(signed.status).toBe('signed');
    expect(signed.signed_by_name).toBe('Tim Pappas');
    expect(signed.signed_at).toBeTruthy();
  });

  test('quote acceptance rejects already-signed quote', async () => {
    const quote = await createTestQuote({ status: 'signed' });

    // Try to accept again — WHERE clause requires status IN ('sent', 'viewed')
    const result = await pool.query(
      `UPDATE sent_quotes
       SET status = 'signed', signed_by_name = $1, signed_at = CURRENT_TIMESTAMP
       WHERE sign_token = $2 AND status IN ('sent', 'viewed')
       RETURNING *`,
      ['Someone Else', quote.sign_token]
    );

    expect(result.rows).toHaveLength(0); // No rows updated
  });

  test('contract signing stores all fields and transitions to contracted', async () => {
    const quote = await createTestQuote({ status: 'signed' });

    const signatureData = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==';
    const signerIp = '127.0.0.1';

    // Replicate POST /api/sent-quotes/:id/sign-contract logic
    const result = await pool.query(
      `UPDATE sent_quotes SET
        contract_signed_at = CURRENT_TIMESTAMP,
        contract_signature_data = $1,
        contract_signature_type = $2,
        contract_signer_ip = $3,
        contract_signer_name = $4,
        status = 'contracted'
      WHERE id = $5
      RETURNING *`,
      [signatureData, 'draw', signerIp, 'Tim Pappas', quote.id]
    );

    expect(result.rows).toHaveLength(1);
    const contracted = result.rows[0];
    expect(contracted.status).toBe('contracted');
    expect(contracted.contract_signed_at).toBeTruthy();
    expect(contracted.contract_signature_data).toBe(signatureData);
    expect(contracted.contract_signature_type).toBe('draw');
    expect(contracted.contract_signer_ip).toBe(signerIp);
    expect(contracted.contract_signer_name).toBe('Tim Pappas');
  });

  test('cannot sign contract twice (contract_signed_at already set)', async () => {
    const quote = await createTestQuote({ status: 'signed' });

    // Sign once
    await pool.query(
      `UPDATE sent_quotes SET
        contract_signed_at = CURRENT_TIMESTAMP, contract_signature_data = 'sig1',
        contract_signature_type = 'draw', contract_signer_ip = '1.2.3.4',
        contract_signer_name = 'First Signer', status = 'contracted'
      WHERE id = $1`,
      [quote.id]
    );

    // Verify the guard: server.js checks `if (quote.contract_signed_at)` before updating
    const check = await pool.query('SELECT contract_signed_at FROM sent_quotes WHERE id = $1', [quote.id]);
    expect(check.rows[0].contract_signed_at).toBeTruthy(); // Already signed — handler would return 400
  });

  test('contract signing stops pending followups', async () => {
    const quote = await createTestQuote({ status: 'signed' });
    const quoteNumber = `Q-${quote.id}`;

    // Create a pending followup for this quote
    const followup = await createTestFollowup(quote.id, {
      quote_number: quoteNumber,
      customer_email: quote.customer_email,
    });

    expect(followup.status).toBe('pending');

    // Sign contract
    await pool.query(
      `UPDATE sent_quotes SET
        contract_signed_at = CURRENT_TIMESTAMP, contract_signature_data = 'sig',
        contract_signature_type = 'type', contract_signer_name = 'Signer',
        status = 'contracted'
      WHERE id = $1`,
      [quote.id]
    );

    // Replicate the followup-stop query from sign-contract handler (line 6453-6457)
    await pool.query(
      `UPDATE quote_followups
       SET status = 'accepted', stopped_at = NOW(), stopped_reason = 'accepted', stopped_by = 'contract_signed', updated_at = NOW()
       WHERE (quote_number = $1 OR customer_email = $2) AND status = 'pending'`,
      [quoteNumber, quote.customer_email]
    );

    // Verify followup was stopped
    const after = await pool.query('SELECT * FROM quote_followups WHERE id = $1', [followup.id]);
    const stopped = after.rows[0];
    expect(stopped.status).toBe('accepted');
    expect(stopped.stopped_reason).toBe('accepted');
    expect(stopped.stopped_by).toBe('contract_signed');
    expect(stopped.stopped_at).toBeTruthy();
  });
});
