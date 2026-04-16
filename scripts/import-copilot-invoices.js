#!/usr/bin/env node
// ─────────────────────────────────────────────────────────────
// One-time CopilotCRM invoice importer.
//
// Reads JSON files captured from the browser (POST .../invoices/getInvoicesListAjax
// returns { html, isEmpty }), parses each row's HTML, and upserts into the
// local invoices table.
//
// Usage:
//   node scripts/import-copilot-invoices.js <file-or-dir> [--apply] [--quiet]
//
// Flags:
//   --apply         Actually write to the DB. Without it, runs as a dry run
//                   and prints a summary of what would be upserted.
//   --link-customers Try to resolve customer_id by matching on email, then
//                    on name. Off by default; opt-in only.
//   --quiet         Only print the final summary.
//
// Files: any *.json files at the given path are processed in lexical order.
//
// Idempotence: upsert key is (external_source='copilotcrm', external_invoice_id).
// Re-running over the same files will UPDATE rows in place — no duplicates.
// invoice_number is preserved if already set on the local row to avoid
// breaking links (we only fill it in when local invoice_number is NULL).
// ─────────────────────────────────────────────────────────────

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { parseInvoiceListHtml } = require('./parse-copilot-invoices');
const { parseInvoiceDetailHtml } = require('./parse-copilot-invoice-detail');

const SOURCE = 'copilotcrm';

// CLI flags are only parsed when this file is executed directly, not when
// it's `require()`-d from a test or another script.
let APPLY = false, QUIET = false, LINK_CUSTOMERS = false;

function log(...a) { if (!QUIET) console.log(...a); }
function warn(...a) { console.warn(...a); }

function listSourceFiles(p) {
  const stat = fs.statSync(p);
  if (stat.isFile()) return [p];
  if (stat.isDirectory()) {
    return fs.readdirSync(p)
      .filter(f => /\.(json|html?)$/i.test(f))
      .sort()
      .map(f => path.join(p, f));
  }
  throw new Error(`Path is neither a file nor a directory: ${p}`);
}

// File routing:
//   *.json  → list-response file (POST .../getInvoicesListAjax body)
//   *.html  → invoice detail page (GET .../invoices/view/{id})
// The list-vs-detail distinction matters because list rows seed the table
// and detail pages enrich them with line items, totals, notes, etc.
function classifyFile(file) {
  const ext = path.extname(file).toLowerCase();
  if (ext === '.html' || ext === '.htm') return 'detail';
  if (ext === '.json') return 'list';
  // Sniff content if extension is ambiguous.
  const head = fs.readFileSync(file, 'utf8').slice(0, 200).trim();
  if (head.startsWith('{')) return 'list';
  if (head.startsWith('<')) return 'detail';
  return 'list';
}

async function resolveCustomerId(pool, row) {
  if (!LINK_CUSTOMERS) return null;
  // Try email first, then exact name. Both are best-effort; we never
  // overwrite an already-set customer_id.
  if (row.customer_email) {
    const r = await pool.query(
      'SELECT id FROM customers WHERE LOWER(email) = LOWER($1) LIMIT 1',
      [row.customer_email]
    );
    if (r.rows.length) return r.rows[0].id;
  }
  if (row.customer_name) {
    const r = await pool.query(
      `SELECT id FROM customers
       WHERE LOWER(COALESCE(NULLIF(name, ''), TRIM(CONCAT_WS(' ', first_name, last_name)))) = LOWER($1)
       LIMIT 1`,
      [row.customer_name]
    );
    if (r.rows.length) return r.rows[0].id;
  }
  return null;
}

// Map a list-row → DB column values. Pure; no side effects.
function toDbValues(row, customerId) {
  const total = row.total != null ? row.total : null;
  const tax = row.tax_amount != null ? row.tax_amount : null;
  // Subtotal isn't in the list view; infer from total - tax when both exist.
  const subtotal = (total != null && tax != null) ? Math.max(0, total - tax) : (total != null ? total : null);

  const metadata = {
    copilot_customer_id: row.copilot_customer_id || null,
    property_name:       row.property_name       || null,
    property_address:    row.property_address    || null,
    crew:                row.crew                || null,
    sent_status:         row.sent_status         || null,
    raw_status:          row.raw_status          || null,
    total_due:           row.total_due           ?? null,
    credit_available:    row.credit_available    ?? null,
    view_path:           row.view_path           || null,
    edit_path:           row.edit_path           || null,
  };

  return {
    external_invoice_id: row.external_invoice_id || null,
    invoice_number:      row.invoice_number || null,
    customer_id:         customerId,
    customer_name:       row.customer_name || null,
    customer_email:      row.customer_email || null,
    customer_address:    row.property_address || null,
    status:              row.status || 'sent',
    subtotal,
    tax_amount:          tax,
    total,
    amount_paid:         row.amount_paid != null ? row.amount_paid : 0,
    due_date:            null, // not in list view
    created_at:          row.invoice_date || null,
    notes:               null,
    terms:               null,
    line_items:          [], // detail enrichment fills this in
    metadata,
  };
}

// Map a detail-page parsed object → DB column values. Carries line_items,
// notes, terms, and the more-accurate subtotal from the totals table.
function toDbValuesFromDetail(detail, customerId) {
  const metadata = {
    copilot_customer_id: detail.copilot_customer_id || null,
    property_name:       detail.property_name       || null,
    property_address:    detail.property_address    || null,
    crew:                detail.crew                || null,
    raw_status:          detail.raw_status          || null,
    total_due:           detail.total_due           ?? null,
    terms_raw:           detail.terms               || null,
  };
  return {
    external_invoice_id: detail.external_invoice_id || null,
    invoice_number:      detail.invoice_number || null,
    customer_id:         customerId,
    customer_name:       detail.customer_name || null,
    customer_email:      detail.customer_email || null,
    customer_address:    detail.customer_address || detail.property_address || null,
    status:              detail.status || 'sent',
    subtotal:            detail.subtotal,
    tax_amount:          detail.tax_amount,
    total:               detail.total,
    amount_paid:         detail.amount_paid != null ? detail.amount_paid : 0,
    due_date:            null,
    created_at:          detail.invoice_date || null,
    notes:               detail.notes || null,
    terms:               detail.terms || null,
    line_items:          Array.isArray(detail.line_items) ? detail.line_items : [],
    metadata,
  };
}

// Two-stage match: first by (external_source, external_invoice_id), then by
// invoice_number alone. If neither finds a row, INSERT a new one.
//
// This is hand-rolled (instead of a single ON CONFLICT) because we want the
// invoice_number fallback for cases like:
//   - importing a detail HTML for an invoice that wasn't in the captured list
//   - importing a list response after a manual invoice with the same number
//     was already created in the local app
//
// Updates merge fields conservatively:
//   - never NULL out an existing populated field with a missing import value
//   - external_metadata is JSONB-merged so prior import data survives
//   - line_items only overwrites when the import actually carries items
//   - notes / terms only overwrite when import carries non-empty values
async function upsert(pool, v) {
  // 1) Try external id match
  let existing = null;
  if (v.external_invoice_id) {
    const r = await pool.query(
      'SELECT id FROM invoices WHERE external_source = $1 AND external_invoice_id = $2',
      [SOURCE, v.external_invoice_id]
    );
    if (r.rows.length) existing = r.rows[0].id;
  }
  // 2) Fall back to invoice_number
  if (!existing && v.invoice_number) {
    const r = await pool.query(
      'SELECT id FROM invoices WHERE invoice_number = $1',
      [v.invoice_number]
    );
    if (r.rows.length) existing = r.rows[0].id;
  }

  if (existing) {
    const setParts = [
      // Stamp source + external id even if previously NULL (this is how
      // a manual local invoice gets adopted by re-runs).
      `external_source = $${1}`,
      `external_invoice_id = COALESCE(invoices.external_invoice_id, $${2})`,
      `invoice_number    = COALESCE(invoices.invoice_number, $${3})`,
      `customer_id       = COALESCE(invoices.customer_id, $${4})`,
      `customer_name     = COALESCE(NULLIF($${5}, ''), invoices.customer_name)`,
      `customer_email    = COALESCE(NULLIF($${6}, ''), invoices.customer_email)`,
      `customer_address  = COALESCE(NULLIF($${7}, ''), invoices.customer_address)`,
      `status            = COALESCE($${8}, invoices.status)`,
      `subtotal          = COALESCE($${9}, invoices.subtotal)`,
      `tax_amount        = COALESCE($${10}, invoices.tax_amount)`,
      `total             = COALESCE($${11}, invoices.total)`,
      `amount_paid       = COALESCE($${12}, invoices.amount_paid)`,
      `created_at        = COALESCE(invoices.created_at, $${13}::timestamp)`,
      // Notes/terms: empty string treated as missing so we don't blank
      // out a hand-edited note.
      `notes             = COALESCE(NULLIF($${14}, ''), invoices.notes)`,
      // Line items: only overwrite when the import actually carries items.
      `line_items        = CASE WHEN jsonb_array_length($${15}::jsonb) > 0 THEN $${15}::jsonb ELSE invoices.line_items END`,
      `external_metadata = invoices.external_metadata || $${16}::jsonb`,
      `imported_at       = CURRENT_TIMESTAMP`,
      `updated_at        = CURRENT_TIMESTAMP`,
    ];
    // terms isn't a real column today — store it inside external_metadata via the merged json above (already there).
    const params = [
      SOURCE, v.external_invoice_id, v.invoice_number,
      v.customer_id, v.customer_name, v.customer_email, v.customer_address,
      v.status, v.subtotal, v.tax_amount, v.total, v.amount_paid,
      v.created_at,
      v.notes, JSON.stringify(v.line_items || []),
      JSON.stringify(v.metadata || {}),
      existing,
    ];
    await pool.query(`UPDATE invoices SET ${setParts.join(', ')} WHERE id = $${params.length}`, params);
    return { id: existing, inserted: false };
  }

  // 3) Fresh INSERT
  const insertSql = `
    INSERT INTO invoices (
      external_source, external_invoice_id, invoice_number,
      customer_id, customer_name, customer_email, customer_address,
      status, subtotal, tax_amount, total, amount_paid,
      due_date, created_at, notes, line_items,
      external_metadata, imported_at
    )
    VALUES (
      $1, $2, $3,
      $4, $5, $6, $7,
      $8, $9, $10, $11, $12,
      $13, COALESCE($14::timestamp, CURRENT_TIMESTAMP),
      $15, $16::jsonb,
      $17::jsonb, CURRENT_TIMESTAMP
    )
    RETURNING id`;
  const r = await pool.query(insertSql, [
    SOURCE, v.external_invoice_id, v.invoice_number,
    v.customer_id, v.customer_name, v.customer_email, v.customer_address,
    v.status, v.subtotal, v.tax_amount, v.total, v.amount_paid,
    v.due_date, v.created_at,
    v.notes, JSON.stringify(v.line_items || []),
    JSON.stringify(v.metadata || {}),
  ]);
  return { id: r.rows[0].id, inserted: true };
}

async function main(target) {
  const files = listSourceFiles(target);
  log(`Found ${files.length} file(s) under ${target}`);

  // Parse all files first. List files seed rows; detail files each map to
  // exactly one row (the page they describe).
  const listRows = [];     // { source, row } from list responses
  const detailRows = [];   // { source, detail } from detail HTML pages

  for (const file of files) {
    let raw;
    try { raw = fs.readFileSync(file, 'utf8'); }
    catch (e) { warn(`! Could not read ${file}: ${e.message}`); continue; }

    const kind = classifyFile(file);
    if (kind === 'list') {
      let payload;
      try { payload = JSON.parse(raw); }
      catch (e) { warn(`! Skipping ${file}: not valid JSON (${e.message})`); continue; }
      let rows;
      try { rows = parseInvoiceListHtml(payload); }
      catch (e) { warn(`! Skipping ${file}: list parser error (${e.message})`); continue; }
      log(`  ${path.basename(file)}: list, ${rows.length} row(s)`);
      rows.forEach(r => listRows.push({ source: file, row: r }));
    } else {
      let detail;
      try { detail = parseInvoiceDetailHtml(raw); }
      catch (e) { warn(`! Skipping ${file}: detail parser error (${e.message})`); continue; }
      const numItems = (detail.line_items || []).length;
      log(`  ${path.basename(file)}: detail, invoice_number=${detail.invoice_number || '?'}, ${numItems} line item(s)`);
      detailRows.push({ source: file, detail });
    }
  }

  // Dedup list rows by external_invoice_id (later file wins for the same id).
  const byExtId = new Map();
  for (const item of listRows) {
    const key = item.row.external_invoice_id || `__no_id__:${item.row.invoice_number || Math.random()}`;
    byExtId.set(key, item);
  }
  const uniqueList = [...byExtId.values()];

  log(`Parsed ${listRows.length} list row(s) → ${uniqueList.length} unique; ${detailRows.length} detail page(s).`);

  if (!APPLY) {
    log('\n— DRY RUN —  Add --apply to write to the database.');
    if (uniqueList.length) {
      log('Sample list rows (first 2):');
      for (const u of uniqueList.slice(0, 2)) log('  •', JSON.stringify(u.row, null, 2));
    }
    if (detailRows.length) {
      log('Sample detail (first 1):');
      log('  •', JSON.stringify(detailRows[0].detail, null, 2));
    }
    log(`\nWould upsert ${uniqueList.length} list row(s) and ${detailRows.length} detail page(s).`);
    return;
  }

  // ── Apply ──
  if (!process.env.DATABASE_URL) {
    console.error('DATABASE_URL is not set. Aborting.');
    process.exit(2);
  }
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : undefined,
  });

  // Phase 1: list rows. These create stub invoices (or update existing
  // ones) so detail-only files have something to enrich.
  let listInserted = 0, listUpdated = 0, listErrors = 0;
  for (const { source, row } of uniqueList) {
    try {
      const customerId = await resolveCustomerId(pool, row);
      const v = toDbValues(row, customerId);
      const r = await upsert(pool, v);
      if (r.inserted) listInserted++; else listUpdated++;
    } catch (e) {
      listErrors++;
      warn(`! Error on list row external_id=${row.external_invoice_id} from ${path.basename(source)}: ${e.message}`);
    }
  }

  // Phase 2: detail pages. These overwrite line_items + notes + (more
  // accurate) totals on the matched invoice.
  let detailInserted = 0, detailUpdated = 0, detailErrors = 0;
  for (const { source, detail } of detailRows) {
    try {
      const customerId = await resolveCustomerId(pool, detail);
      const v = toDbValuesFromDetail(detail, customerId);
      const r = await upsert(pool, v);
      if (r.inserted) detailInserted++; else detailUpdated++;
    } catch (e) {
      detailErrors++;
      warn(`! Error on detail external_id=${detail.external_invoice_id} from ${path.basename(source)}: ${e.message}`);
    }
  }

  log('\n─────────────────────');
  log(`List rows  → inserted: ${listInserted}, updated: ${listUpdated}, errors: ${listErrors}`);
  log(`Detail pgs → inserted: ${detailInserted}, updated: ${detailUpdated}, errors: ${detailErrors}`);
  log('─────────────────────');

  await pool.end();
}

if (require.main === module) {
  const args = process.argv.slice(2);
  const target = args.find(a => !a.startsWith('--'));
  APPLY = args.includes('--apply');
  QUIET = args.includes('--quiet');
  LINK_CUSTOMERS = args.includes('--link-customers');
  if (!target) {
    console.error('Usage: node scripts/import-copilot-invoices.js <file-or-dir> [--apply] [--link-customers] [--quiet]');
    process.exit(2);
  }
  main(target).catch(e => {
    console.error('Importer failed:', e);
    process.exit(1);
  });
}

module.exports = { toDbValues, toDbValuesFromDetail, upsert };
