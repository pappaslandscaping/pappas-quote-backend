#!/usr/bin/env node

require('dotenv').config();
const { Pool } = require('pg');

function buildPool() {
  const connectionString = process.env.DATABASE_URL;
  return new Pool({
    connectionString,
    ssl: connectionString && connectionString.includes('railway')
      ? { rejectUnauthorized: false }
      : undefined,
  });
}

async function detachInvoiceReferences(client, ids) {
  const cleanupSteps = [
    'DELETE FROM payments WHERE invoice_id = ANY($1::int[])',
    'DELETE FROM late_fees WHERE invoice_id = ANY($1::int[])',
    'DELETE FROM recurring_invoice_log WHERE invoice_id = ANY($1::int[])',
    'UPDATE scheduled_jobs SET invoice_id = NULL WHERE invoice_id = ANY($1::int[])',
    'UPDATE email_log SET invoice_id = NULL WHERE invoice_id = ANY($1::int[])',
  ];
  for (const sql of cleanupSteps) {
    await client.query(sql, [ids]);
  }
}

function summarize(rows) {
  const byCustomer = {};
  for (const row of rows) {
    const key = row.customer_name || '(blank)';
    byCustomer[key] = (byCustomer[key] || 0) + 1;
  }
  return {
    count: rows.length,
    first_invoice_number: rows[0]?.invoice_number || null,
    last_invoice_number: rows[rows.length - 1]?.invoice_number || null,
    by_customer: byCustomer,
  };
}

async function main() {
  const confirm = process.argv.includes('--confirm');
  const pool = buildPool();
  const client = await pool.connect();

  try {
    const candidateResult = await client.query(`
      SELECT id, invoice_number, customer_name, created_at, external_invoice_id
      FROM invoices
      WHERE status = 'draft'
        AND external_source = 'copilotcrm'
      ORDER BY id
    `);
    const candidates = candidateResult.rows;

    if (!confirm) {
      console.log(JSON.stringify({
        dry_run: true,
        ...summarize(candidates),
      }, null, 2));
      return;
    }

    const ids = candidates.map((row) => row.id);
    if (!ids.length) {
      console.log(JSON.stringify({ deleted: 0, by_customer: {} }, null, 2));
      return;
    }

    await client.query('BEGIN');
    await detachInvoiceReferences(client, ids);
    const deletedResult = await client.query(
      'DELETE FROM invoices WHERE id = ANY($1::int[]) RETURNING id, invoice_number, customer_name',
      [ids]
    );
    await client.query('COMMIT');

    console.log(JSON.stringify({
      deleted: deletedResult.rows.length,
      ...summarize(deletedResult.rows),
    }, null, 2));
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch (_) {}
    console.error(error.stack || error.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();
