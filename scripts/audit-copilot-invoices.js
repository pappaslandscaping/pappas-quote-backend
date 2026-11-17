#!/usr/bin/env node

require('dotenv').config();

const { Pool } = require('pg');

function parseArgs(argv) {
  const args = { pageSize: 100, maxPages: 150 };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--page-size') args.pageSize = Number(argv[++i] || 100) || 100;
    else if (arg === '--max-pages') args.maxPages = Number(argv[++i] || 150) || 150;
    else if (arg === '--help' || arg === '-h') {
      console.log('Usage: node scripts/audit-copilot-invoices.js [--page-size 100] [--max-pages 150]');
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return args;
}

async function fetchAllCopilotInvoices(pool) {
  const invoices = await require('../services/homeworks/records').loadInvoices({pool}, {}, {details:false});
  return {available:true,invoices,totalCount:invoices.length,source:'official_homeworks_graphql'};
}

async function queryDbAudit(pool) {
  const counts = await pool.query(`
    SELECT
      COUNT(*)::int AS db_total,
      (COUNT(*) FILTER (WHERE external_source = 'copilotcrm'))::int AS copilot_rows,
      (COUNT(DISTINCT external_invoice_id) FILTER (WHERE external_source = 'copilotcrm' AND external_invoice_id IS NOT NULL))::int AS distinct_external_invoice_id,
      (COUNT(DISTINCT invoice_number) FILTER (WHERE external_source = 'copilotcrm' AND COALESCE(TRIM(invoice_number), '') <> ''))::int AS distinct_copilot_invoice_number,
      (COUNT(*) FILTER (WHERE external_source IS NULL))::int AS legacy_without_external_source,
      (COUNT(*) FILTER (
        WHERE external_source = 'copilotcrm'
          AND (sent_quote_id IS NOT NULL OR job_id IS NOT NULL OR payment_token IS NOT NULL OR payment_token_created_at IS NOT NULL)
      ))::int AS likely_invoice_number_fallback_merges
    FROM invoices
  `);

  const malformed = await pool.query(`
    SELECT id, invoice_number, external_invoice_id, external_source, customer_name, total, created_at
      FROM invoices
     WHERE COALESCE(TRIM(invoice_number), '') = ''
        OR invoice_number ~ '^[A-Z][a-z]{2} [0-9]{2}, [0-9]{4}$'
        OR invoice_number ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$'
        OR invoice_number ~ '^\\d{4}-\\d{2}-\\d{2}$'
     ORDER BY created_at DESC
     LIMIT 100
  `);

  return {
    counts: counts.rows[0],
    malformedRows: malformed.rows,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set');

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : undefined,
  });

  try {
    const dbAudit = await queryDbAudit(pool);
    const copilotAudit = await fetchAllCopilotInvoices(pool, args);

    const output = {
      db: dbAudit,
      copilot: {
        available: copilotAudit.available,
        reason: copilotAudit.reason || null,
        total: copilotAudit.totalCount || copilotAudit.invoices.length,
        fetchedRows: copilotAudit.invoices.length,
      },
      missing: {
        count: null,
        invoice_numbers: [],
      },
      api: {
        default_limit: 25000,
        default_filter_excludes_only_blank_drafts: true,
      },
    };

    if (copilotAudit.available) {
      const dbRows = await pool.query(`
        SELECT invoice_number, external_invoice_id, external_source
          FROM invoices
      `);
      const dbByInvoiceNumber = new Set(
        dbRows.rows
          .map(row => String(row.invoice_number || '').trim())
          .filter(Boolean)
      );
      const dbByExternalId = new Set(
        dbRows.rows
          .filter(row => row.external_source === 'copilotcrm' && row.external_invoice_id)
          .map(row => String(row.external_invoice_id))
      );

      const missing = [];
      for (const inv of copilotAudit.invoices) {
        const invoiceNumber = String(inv.invoice_number || '').trim();
        const externalId = String(inv.external_invoice_id || '').trim();
        const represented = (invoiceNumber && dbByInvoiceNumber.has(invoiceNumber))
          || (externalId && dbByExternalId.has(externalId));
        if (!represented) missing.push(invoiceNumber || externalId || '(unknown)');
      }

      output.missing.count = missing.length;
      output.missing.invoice_numbers = missing.slice(0, 500);
    }

    console.log(JSON.stringify(output, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error('Copilot invoice audit failed:', error.message);
  process.exit(1);
});
