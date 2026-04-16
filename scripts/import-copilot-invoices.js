#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

function loadDotenvIfAvailable() {
  try {
    const dotenv = require('dotenv');
    const candidatePaths = [
      path.resolve(process.cwd(), '.env'),
      path.resolve(__dirname, '..', '.env'),
    ];

    candidatePaths.forEach((envPath) => {
      if (fs.existsSync(envPath)) {
        dotenv.config({ path: envPath, override: false });
      }
    });
  } catch (error) {
    // The repo has some iCloud-backed placeholders; skip dotenv if it is unavailable.
  }
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function stripTags(value) {
  return decodeHtml(String(value || '').replace(/<[^>]+>/g, ' '));
}

function parseMoney(value) {
  if (!value) return 0;
  const normalized = String(value).replace(/[$,\s]/g, '').trim();
  const number = Number.parseFloat(normalized);
  return Number.isFinite(number) ? number : 0;
}

function parseText(value) {
  return stripTags(value).replace(/\s+/g, ' ').trim();
}

function parseDate(value) {
  const text = parseText(value);
  if (!text) return null;
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function splitTableCells(rowHtml) {
  const cells = [];
  const tdPattern = /<td\b[^>]*>([\s\S]*?)<\/td>/gi;
  let match;
  while ((match = tdPattern.exec(rowHtml))) {
    cells.push(match[1]);
  }
  return cells;
}

function getFirstHref(html) {
  const match = String(html || '').match(/href="([^"]+)"/i);
  return match ? decodeHtml(match[1]) : '';
}

function parseCustomerCell(cellHtml) {
  const html = String(cellHtml || '');
  const parts = html
    .split(/<br\s*\/?>/i)
    .map(parseText)
    .filter(Boolean);

  const name = parts[0] || parseText(html);
  const email = parts.find((part) => /@/.test(part)) || '';
  const href = getFirstHref(html);
  const customerIdMatch = href.match(/\/customers\/details\/(\d+)/);

  return {
    customer_name: name,
    customer_email: email,
    external_customer_id: customerIdMatch ? customerIdMatch[1] : null,
  };
}

function parseSentStatus(cellHtml) {
  const html = String(cellHtml || '');
  const badges = [];
  const badgePattern = /<span\b[^>]*class="[^"]*badge[^"]*"[^>]*>([\s\S]*?)<\/span>/gi;
  let match;
  while ((match = badgePattern.exec(html))) {
    badges.push(parseText(match[1]));
  }
  if (badges.length) return badges.join(' | ');
  return parseText(html);
}

function parseInvoiceRow(rowHtml, rowId) {
  const tds = splitTableCells(rowHtml);
  if (tds.length < 10) return null;

  const id = String(rowId || '').trim();
  if (!id) return null;

  const viewPath = getFirstHref(tds[1]);
  const customer = parseCustomerCell(tds[3]);

  const invoice = {
    external_source: 'copilotcrm',
    external_invoice_id: id,
    invoice_number: parseText(tds[1]),
    created_at: parseDate(tds[2]),
    invoice_date_raw: parseText(tds[2]),
    customer_name: customer.customer_name,
    customer_email: customer.customer_email || null,
    external_customer_id: customer.external_customer_id,
    title_description: parseText(tds[4]) || null,
    property_name: parseText(tds[5]) || null,
    property_address: parseText(tds[6]) || null,
    crew: parseText(tds[7]) || null,
    tax_amount: parseMoney(tds[8]),
    total: parseMoney(tds[9]),
    total_due: parseMoney(tds[10]),
    amount_paid: parseMoney(tds[11]),
    credit_available: parseMoney(tds[12]),
    status: parseText(tds[13]) || 'pending',
    sent_status: parseSentStatus(tds[14]) || null,
    view_path: viewPath || null,
    metadata: {
      crew: parseText(tds[7]) || null,
      property_name: parseText(tds[5]) || null,
      property_address: parseText(tds[6]) || null,
      sent_status: parseSentStatus(tds[14]) || null,
      external_customer_id: customer.external_customer_id,
      view_path: viewPath || null,
      invoice_date_raw: parseText(tds[2]) || null,
    },
  };

  invoice.subtotal = Math.max(0, Number((invoice.total - invoice.tax_amount).toFixed(2)));

  return invoice;
}

function parseInvoiceListHtml(html) {
  const invoices = [];
  const rowPattern = /<tr\b[^>]*id="([^"]+)"[^>]*>([\s\S]*?)<\/tr>/gi;
  let match;
  while ((match = rowPattern.exec(String(html || '')))) {
    const parsed = parseInvoiceRow(match[2], match[1]);
    if (parsed) invoices.push(parsed);
  }

  return invoices;
}

function extractHtmlPayloads(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(raw);

  if (Array.isArray(parsed)) {
    return parsed
      .map((entry) => ({
        source_file: path.basename(filePath),
        page: entry.page || null,
        html: entry?.data?.html || entry?.html || '',
      }))
      .filter((entry) => entry.html);
  }

  if (parsed && typeof parsed === 'object' && parsed.html) {
    return [{
      source_file: path.basename(filePath),
      page: parsed.page || null,
      html: parsed.html,
    }];
  }

  return [];
}

function collectInvoicesFromDirectory(importDir) {
  const entries = fs.readdirSync(importDir)
    .filter((name) => name.endsWith('.json'))
    .map((name) => path.join(importDir, name));

  const invoices = [];
  const fileSummaries = [];

  for (const filePath of entries) {
    const payloads = extractHtmlPayloads(filePath);
    let count = 0;
    for (const payload of payloads) {
      const parsed = parseInvoiceListHtml(payload.html).map((invoice) => ({
        ...invoice,
        source_file: payload.source_file,
        source_page: payload.page,
      }));
      invoices.push(...parsed);
      count += parsed.length;
    }

    fileSummaries.push({
      file: path.basename(filePath),
      pages: payloads.length,
      invoices: count,
    });
  }

  return { invoices, fileSummaries };
}

function dedupeInvoices(invoices) {
  const byKey = new Map();
  for (const invoice of invoices) {
    const key = `${invoice.external_source}:${invoice.external_invoice_id}`;
    byKey.set(key, invoice);
  }
  return [...byKey.values()];
}

async function ensureCopilotColumns(pool) {
  const statements = [
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS external_source VARCHAR(50)",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS external_invoice_id VARCHAR(100)",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS tax_amount DECIMAL(10,2) DEFAULT 0",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS total_due DECIMAL(10,2) DEFAULT 0",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS credit_available DECIMAL(10,2) DEFAULT 0",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS property_name TEXT",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS property_address TEXT",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS sent_status TEXT",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS external_customer_id VARCHAR(100)",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb",
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS title_description TEXT",
  ];

  for (const statement of statements) {
    await pool.query(statement);
  }

  await pool.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS invoices_external_source_external_invoice_id_idx
     ON invoices (external_source, external_invoice_id)
     WHERE external_source IS NOT NULL AND external_invoice_id IS NOT NULL`
  );
}

async function maybeLinkCustomer(pool, invoice) {
  if (invoice.customer_email) {
    const byEmail = await pool.query(
      'SELECT id FROM customers WHERE LOWER(email) = LOWER($1) ORDER BY id ASC LIMIT 1',
      [invoice.customer_email]
    );
    if (byEmail.rows[0]) return byEmail.rows[0].id;
  }

  if (invoice.customer_name) {
    const byName = await pool.query(
      'SELECT id FROM customers WHERE LOWER(name) = LOWER($1) ORDER BY id ASC LIMIT 1',
      [invoice.customer_name]
    );
    if (byName.rows[0]) return byName.rows[0].id;
  }

  return null;
}

async function upsertInvoice(pool, invoice, { linkCustomers = false } = {}) {
  let customerId = null;
  if (linkCustomers) {
    customerId = await maybeLinkCustomer(pool, invoice);
  }

  const existing = await pool.query(
    `SELECT id
       FROM invoices
      WHERE (external_source = $1 AND external_invoice_id = $2)
         OR invoice_number = $3
      ORDER BY CASE WHEN external_source = $1 AND external_invoice_id = $2 THEN 0 ELSE 1 END
      LIMIT 1`,
    [invoice.external_source, invoice.external_invoice_id, invoice.invoice_number]
  );

  const params = [
    invoice.invoice_number,
    customerId,
    invoice.customer_name,
    invoice.customer_email,
    invoice.status.toLowerCase(),
    invoice.subtotal,
    invoice.total,
    invoice.amount_paid,
    JSON.stringify([]),
    invoice.created_at ? new Date(invoice.created_at) : new Date(),
    invoice.external_source,
    invoice.external_invoice_id,
    invoice.tax_amount,
    invoice.total_due,
    invoice.credit_available,
    invoice.property_name,
    invoice.property_address,
    invoice.sent_status,
    invoice.external_customer_id,
    JSON.stringify(invoice.metadata || {}),
    invoice.title_description,
  ];

  if (existing.rows[0]) {
    await pool.query(
      `UPDATE invoices
          SET invoice_number = $1,
              customer_id = COALESCE($2, customer_id),
              customer_name = $3,
              customer_email = $4,
              status = $5,
              subtotal = $6,
              total = $7,
              amount_paid = $8,
              line_items = $9,
              created_at = $10,
              external_source = $11,
              external_invoice_id = $12,
              tax_amount = $13,
              total_due = $14,
              credit_available = $15,
              property_name = $16,
              property_address = $17,
              sent_status = $18,
              external_customer_id = $19,
              metadata = COALESCE(metadata, '{}'::jsonb) || $20::jsonb,
              title_description = $21,
              updated_at = CURRENT_TIMESTAMP
        WHERE id = $22`,
      [...params, existing.rows[0].id]
    );
    return { action: 'updated', id: existing.rows[0].id };
  }

  const inserted = await pool.query(
    `INSERT INTO invoices (
        invoice_number, customer_id, customer_name, customer_email,
        status, subtotal, total, amount_paid, line_items, created_at,
        external_source, external_invoice_id, tax_amount, total_due,
        credit_available, property_name, property_address, sent_status,
        external_customer_id, metadata, title_description
      ) VALUES (
        $1,$2,$3,$4,
        $5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,
        $15,$16,$17,$18,
        $19,$20::jsonb,$21
      ) RETURNING id`,
    params
  );

  return { action: 'inserted', id: inserted.rows[0].id };
}

async function syncInvoicesToDatabase(pool, invoices, { linkCustomers = false } = {}) {
  await ensureCopilotColumns(pool);

  const deduped = dedupeInvoices(invoices);
  let inserted = 0;
  let updated = 0;

  for (const invoice of deduped) {
    const result = await upsertInvoice(pool, invoice, { linkCustomers });
    if (result.action === 'inserted') inserted += 1;
    if (result.action === 'updated') updated += 1;
  }

  return {
    total: deduped.length,
    inserted,
    updated,
  };
}

function printSummary(summary, invoices) {
  console.log(`Parsed ${invoices.length} invoices from ${summary.length} file(s).`);
  summary.forEach((item) => {
    console.log(`- ${item.file}: ${item.invoices} invoice row(s) across ${item.pages} payload(s)`);
  });

  const sample = invoices.slice(0, 5);
  if (sample.length) {
    console.log('\nSample invoices:');
    sample.forEach((invoice) => {
      console.log(
        `- #${invoice.invoice_number} | ${invoice.customer_name || 'Unknown'} | total ${invoice.total.toFixed(2)} | status ${invoice.status}`
      );
    });
  }
}

async function main(argv = process.argv.slice(2)) {
  loadDotenvIfAvailable();
  const importDirArg = argv.find((arg) => !arg.startsWith('--'));
  const importDir = path.resolve(process.cwd(), importDirArg || 'scripts/copilot-imports');
  const apply = argv.includes('--apply');
  const linkCustomers = argv.includes('--link-customers');

  if (!fs.existsSync(importDir)) {
    console.error(`Import directory not found: ${importDir}`);
    process.exitCode = 1;
    return;
  }

  const { invoices, fileSummaries } = collectInvoicesFromDirectory(importDir);
  const deduped = dedupeInvoices(invoices);

  printSummary(fileSummaries, deduped);

  if (!apply) {
    console.log('\nDry run only. Re-run with --apply to write invoices.');
    return;
  }

  if (!process.env.DATABASE_URL) {
    console.error('\nDATABASE_URL is not set, so --apply cannot connect to the database.');
    process.exitCode = 1;
    return;
  }

  const { Pool } = require('pg');
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  });

  try {
    const { inserted, updated } = await syncInvoicesToDatabase(pool, deduped, { linkCustomers });

    console.log(`\nApply complete: ${inserted} inserted, ${updated} updated.`);
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = {
  main,
  parseInvoiceListHtml,
  collectInvoicesFromDirectory,
  dedupeInvoices,
  parseInvoiceRow,
  ensureCopilotColumns,
  upsertInvoice,
  syncInvoicesToDatabase,
};
