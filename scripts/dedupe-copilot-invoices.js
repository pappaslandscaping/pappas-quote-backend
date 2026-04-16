#!/usr/bin/env node

require('dotenv').config();

const { Pool } = require('pg');

function parseArgs(argv) {
  const args = {
    apply: false,
    invoiceNumber: null,
    limit: null,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--apply') args.apply = true;
    else if (arg === '--invoice-number') args.invoiceNumber = argv[++i] || null;
    else if (arg === '--limit') args.limit = Number(argv[++i] || 0) || null;
    else if (arg === '--help' || arg === '-h') {
      printUsage();
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return args;
}

function printUsage() {
  console.log('Usage: node scripts/dedupe-copilot-invoices.js [--apply] [--invoice-number 10448] [--limit 25]');
  console.log('');
  console.log('Default mode is dry-run. Use --apply to perform updates/deletes.');
}

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function clean(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeName(name) {
  return clean(name).toLowerCase();
}

function normalizeEmail(email) {
  return clean(email).toLowerCase();
}

function parseAmount(value) {
  const n = parseFloat(value);
  return Number.isFinite(n) ? n : null;
}

function amountsMatch(a, b) {
  const left = parseAmount(a);
  const right = parseAmount(b);
  if (left == null || right == null) return false;
  return Math.abs(left - right) < 0.01;
}

function amountsConflict(a, b) {
  const left = parseAmount(a);
  const right = parseAmount(b);
  if (left == null || right == null) return false;
  return Math.abs(left - right) >= 0.01;
}

function isCopilotCanonicalCandidate(row) {
  return row.external_source === 'copilotcrm' || !!row.external_invoice_id;
}

function canonicalScore(row) {
  let score = 0;
  if (row.external_source === 'copilotcrm') score += 100;
  if (row.external_invoice_id) score += 30;
  if (row.imported_at) score += 10;
  if (row.sent_status) score += 2;
  if (parseAmount(row.total) != null) score += 1;
  return score;
}

function chooseCanonical(rows) {
  const candidates = rows.filter(isCopilotCanonicalCandidate);
  if (!candidates.length) {
    return { canonical: null, reason: 'no_copilot_canonical' };
  }

  const sorted = [...candidates].sort((a, b) => {
    const diff = canonicalScore(b) - canonicalScore(a);
    if (diff !== 0) return diff;
    return a.id - b.id;
  });

  if (sorted.length > 1 && canonicalScore(sorted[0]) === canonicalScore(sorted[1])) {
    return { canonical: null, reason: 'multiple_equal_canonical_candidates', candidates: sorted.slice(0, 2).map(r => r.id) };
  }

  return { canonical: sorted[0], reason: null };
}

function evaluateLegacyMatch(canonical, legacy) {
  const reasons = [];
  const positives = [];

  if (canonical.id === legacy.id) {
    return { confident: false, reasons: ['same_row'], positives: [] };
  }

  if (canonical.invoice_number !== legacy.invoice_number) {
    reasons.push('invoice_number_mismatch');
  }

  if (amountsConflict(canonical.total, legacy.total)) {
    reasons.push(`total_conflict:${canonical.total}!=${legacy.total}`);
  } else if (amountsMatch(canonical.total, legacy.total)) {
    positives.push('total_match');
  }

  const canonicalEmail = normalizeEmail(canonical.customer_email);
  const legacyEmail = normalizeEmail(legacy.customer_email);
  if (canonicalEmail && legacyEmail && canonicalEmail !== legacyEmail) {
    reasons.push(`email_conflict:${canonicalEmail}!=${legacyEmail}`);
  } else if (canonicalEmail && legacyEmail && canonicalEmail === legacyEmail) {
    positives.push('email_match');
  }

  const canonicalName = normalizeName(canonical.customer_name);
  const legacyName = normalizeName(legacy.customer_name);
  if (canonicalName && legacyName && canonicalName === legacyName) {
    positives.push('name_match');
  }

  if (reasons.length) {
    return { confident: false, reasons, positives };
  }

  if (positives.length === 0) {
    return { confident: false, reasons: ['no_secondary_match_signal'], positives };
  }

  return { confident: true, reasons: [], positives };
}

function isBlankText(value) {
  return clean(value) === '';
}

function isEmptyLineItems(value) {
  if (value == null) return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed.length === 0 : !parsed;
    } catch (_) {
      return isBlankText(value);
    }
  }
  return false;
}

function mergePlan(canonical, legacy, invoiceColumns) {
  const updates = {};
  const mergedFields = [];

  const maybeSet = (field, shouldMerge) => {
    if (!invoiceColumns.has(field)) return;
    if (!shouldMerge) return;
    updates[field] = legacy[field];
    mergedFields.push(field);
  };

  maybeSet('payment_token', !canonical.payment_token && !!legacy.payment_token);
  maybeSet('payment_token_created_at', !canonical.payment_token_created_at && !!legacy.payment_token_created_at);
  maybeSet('notes', isBlankText(canonical.notes) && !isBlankText(legacy.notes));
  maybeSet('line_items', isEmptyLineItems(canonical.line_items) && !isEmptyLineItems(legacy.line_items));
  maybeSet('due_date', !canonical.due_date && !!legacy.due_date);
  maybeSet('paid_at', !canonical.paid_at && !!legacy.paid_at);
  maybeSet('sent_at', !canonical.sent_at && !!legacy.sent_at);

  return { updates, mergedFields };
}

async function getInvoiceColumns(pool) {
  const result = await pool.query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = 'invoices'`
  );
  return new Set(result.rows.map(row => row.column_name));
}

async function getInvoiceReferenceColumns(pool) {
  const result = await pool.query(
    `SELECT table_name, column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND column_name = 'invoice_id'
        AND table_name <> 'invoices'
      ORDER BY table_name`
  );
  return result.rows;
}

async function loadDuplicateGroups(pool, { invoiceNumber = null, limit = null } = {}) {
  const params = [];
  const where = [`invoice_number IS NOT NULL`, `invoice_number <> ''`];

  if (invoiceNumber) {
    params.push(invoiceNumber);
    where.push(`invoice_number = $${params.length}`);
  }

  let sql = `
    SELECT invoice_number
      FROM invoices
     WHERE ${where.join(' AND ')}
     GROUP BY invoice_number
    HAVING COUNT(*) > 1
       AND BOOL_OR(external_source = 'copilotcrm' OR external_invoice_id IS NOT NULL)
     ORDER BY invoice_number`;

  if (limit) {
    params.push(limit);
    sql += ` LIMIT $${params.length}`;
  }

  const groups = await pool.query(sql, params);
  return groups.rows.map(row => row.invoice_number);
}

async function loadInvoicesForNumber(pool, invoiceNumber) {
  const result = await pool.query(
    `SELECT *
       FROM invoices
      WHERE invoice_number = $1
      ORDER BY id ASC`,
    [invoiceNumber]
  );
  return result.rows;
}

async function countReferences(pool, references, invoiceId) {
  const counts = {};
  for (const ref of references) {
    const sql = `SELECT COUNT(*)::int AS count FROM ${quoteIdent(ref.table_name)} WHERE ${quoteIdent(ref.column_name)} = $1`;
    const result = await pool.query(sql, [invoiceId]);
    counts[ref.table_name] = result.rows[0].count;
  }
  return counts;
}

async function applyReferenceMove(pool, references, fromInvoiceId, toInvoiceId) {
  const moved = {};
  for (const ref of references) {
    const sql = `
      WITH updated AS (
        UPDATE ${quoteIdent(ref.table_name)}
           SET ${quoteIdent(ref.column_name)} = $1
         WHERE ${quoteIdent(ref.column_name)} = $2
       RETURNING 1
      )
      SELECT COUNT(*)::int AS count FROM updated`;
    const result = await pool.query(sql, [toInvoiceId, fromInvoiceId]);
    moved[ref.table_name] = result.rows[0].count;
  }
  return moved;
}

async function applyInvoiceMerge(pool, canonicalId, updates) {
  const fields = Object.keys(updates);
  if (!fields.length) return;

  const sets = [];
  const params = [];
  fields.forEach((field) => {
    params.push(updates[field]);
    sets.push(`${quoteIdent(field)} = $${params.length}`);
  });
  sets.push(`updated_at = CURRENT_TIMESTAMP`);
  params.push(canonicalId);

  await pool.query(
    `UPDATE invoices SET ${sets.join(', ')} WHERE id = $${params.length}`,
    params
  );
}

function formatInvoice(row) {
  return `#${row.id} invoice_number=${row.invoice_number || '—'} customer="${row.customer_name || ''}" email="${row.customer_email || ''}" total=${row.total || 'null'} source=${row.external_source || 'local'} external_invoice_id=${row.external_invoice_id || 'null'}`;
}

function summarizeReferenceCounts(counts) {
  return Object.entries(counts)
    .filter(([, count]) => count > 0)
    .map(([table, count]) => `${table}:${count}`)
    .join(', ');
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is not set');
  }

  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : undefined,
  });

  const summary = {
    matchedLegacyInvoices: 0,
    movedReferences: 0,
    deletedInvoices: 0,
    ambiguousGroups: 0,
    ambiguousInvoices: [],
  };

  try {
    const invoiceColumns = await getInvoiceColumns(pool);
    const references = await getInvoiceReferenceColumns(pool);
    const invoiceNumbers = await loadDuplicateGroups(pool, args);

    console.log(args.apply ? 'APPLY MODE' : 'DRY RUN');
    console.log(`Found ${invoiceNumbers.length} duplicate invoice_number group(s) with a Copilot candidate.`);
    if (references.length) {
      console.log(`Discovered invoice reference columns: ${references.map(ref => `${ref.table_name}.${ref.column_name}`).join(', ')}`);
    }
    console.log('');

    if (args.apply) {
      await pool.query('BEGIN');
    }

    for (const invoiceNumber of invoiceNumbers) {
      const rows = await loadInvoicesForNumber(pool, invoiceNumber);
      const { canonical, reason, candidates } = chooseCanonical(rows);

      if (!canonical) {
        summary.ambiguousGroups += 1;
        summary.ambiguousInvoices.push({
          invoice_number: invoiceNumber,
          reason,
          row_ids: rows.map(row => row.id),
          canonical_candidates: candidates || [],
        });
        console.log(`AMBIGUOUS invoice_number=${invoiceNumber}: ${reason} rows=[${rows.map(row => row.id).join(', ')}]`);
        continue;
      }

      const legacyRows = rows.filter(row => row.id !== canonical.id && !isCopilotCanonicalCandidate(row));
      if (!legacyRows.length) continue;

      for (const legacy of legacyRows) {
        const assessment = evaluateLegacyMatch(canonical, legacy);
        if (!assessment.confident) {
          summary.ambiguousGroups += 1;
          summary.ambiguousInvoices.push({
            invoice_number: invoiceNumber,
            reason: assessment.reasons,
            canonical_id: canonical.id,
            legacy_id: legacy.id,
          });
          console.log(`AMBIGUOUS invoice_number=${invoiceNumber}: keep ${canonical.id}, legacy ${legacy.id}, reasons=${assessment.reasons.join(';')}`);
          continue;
        }

        const { updates, mergedFields } = mergePlan(canonical, legacy, invoiceColumns);
        const referenceCounts = await countReferences(pool, references, legacy.id);
        const movedForLegacy = Object.values(referenceCounts).reduce((sum, count) => sum + count, 0);

        console.log(`MATCH invoice_number=${invoiceNumber}`);
        console.log(`  canonical: ${formatInvoice(canonical)}`);
        console.log(`  legacy:    ${formatInvoice(legacy)}`);
        console.log(`  confidence: ${assessment.positives.join(', ')}`);
        console.log(`  merge fields: ${mergedFields.length ? mergedFields.join(', ') : 'none'}`);
        console.log(`  references: ${summarizeReferenceCounts(referenceCounts) || 'none'}`);
        console.log(`  delete legacy invoice: yes`);

        if (args.apply) {
          await applyInvoiceMerge(pool, canonical.id, updates);
          const moved = await applyReferenceMove(pool, references, legacy.id, canonical.id);
          await pool.query('DELETE FROM invoices WHERE id = $1', [legacy.id]);
          summary.movedReferences += Object.values(moved).reduce((sum, count) => sum + count, 0);
          summary.deletedInvoices += 1;
        } else {
          summary.movedReferences += movedForLegacy;
          summary.deletedInvoices += 1;
        }

        summary.matchedLegacyInvoices += 1;
      }
    }

    if (args.apply) {
      await pool.query('COMMIT');
    }

    console.log('');
    console.log('Summary');
    console.log(`  matched legacy invoices: ${summary.matchedLegacyInvoices}`);
    console.log(`  references moved: ${summary.movedReferences}`);
    console.log(`  invoices deleted: ${summary.deletedInvoices}`);
    console.log(`  ambiguous groups/invoices: ${summary.ambiguousInvoices.length}`);
    if (summary.ambiguousInvoices.length) {
      console.log('  ambiguous details:');
      summary.ambiguousInvoices.forEach((item) => {
        console.log(`    - invoice_number=${item.invoice_number} ${JSON.stringify(item)}`);
      });
    }
  } catch (error) {
    if (args.apply) {
      try { await pool.query('ROLLBACK'); } catch (_) {}
    }
    throw error;
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error('Invoice dedupe failed:', error.message);
  process.exit(1);
});
