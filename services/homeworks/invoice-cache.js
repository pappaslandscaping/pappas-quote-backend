'use strict';
// This is a disposable mirror for existing YardDesk URLs and print metadata.
// Reads and financial writes are always verified against Homeworks first.
async function mirrorInvoices(pool, invoices, { details = true, allowConflicts = false } = {}) {
  if (!invoices.length) return [];
  const existing = await pool.query(
    'SELECT id,invoice_number,external_source,external_invoice_id FROM invoices WHERE external_invoice_id=ANY($1::text[]) OR invoice_number=ANY($2::text[])',
    [invoices.map((row) => row.external_invoice_id), invoices.map((row) => row.invoice_number)],
  );
  const conflicts = new Set();
  for (const row of invoices) {
    const numberOwner = existing.rows.find((item) => item.invoice_number === row.invoice_number);
    if (
      numberOwner &&
      (numberOwner.external_source !== 'copilotcrm' ||
        String(numberOwner.external_invoice_id) !== row.external_invoice_id)
    ) {
      if (allowConflicts) { conflicts.add(row.external_invoice_id); continue; }
      throw new Error(
        `Invoice #${row.invoice_number} has a conflicting YardDesk identity. Review its linkage before syncing.`,
      );
    }
  }
  const requested = invoices;
  invoices = invoices.filter(row => !conflicts.has(row.external_invoice_id));
  const mapped = [];
  for (let start = 0; start < invoices.length; start += 200) {
    const batch = invoices.slice(start, start + 200).map((row) => ({
      external_invoice_id: row.external_invoice_id,
      invoice_number: row.invoice_number,
      customer_name: row.customer_name,
      customer_email: row.customer_email,
      customer_address: row.customer_address,
      status: row.status,
      subtotal: row.subtotal,
      tax_amount: row.tax_amount,
      total: row.total,
      amount_paid: row.amount_paid,
      due_date: row.due_date?.slice(0, 10) || null,
      created_at: row.invoice_date,
      notes: row.notes,
      terms: row.terms,
      sent_status: row.sent_status,
      line_items: row.line_items,
      metadata: { ...row.metadata, raw_status: row.raw_status, details_complete: details },
    }));
    const result = await pool.query(
      `INSERT INTO invoices(external_source,external_invoice_id,invoice_number,customer_name,customer_email,customer_address,status,subtotal,tax_amount,total,amount_paid,due_date,created_at,notes,terms,sent_status,line_items,external_metadata,imported_at)
      SELECT 'copilotcrm',x.external_invoice_id,x.invoice_number,x.customer_name,x.customer_email,x.customer_address,x.status,x.subtotal,x.tax_amount,x.total,x.amount_paid,x.due_date,x.created_at,x.notes,x.terms,x.sent_status,x.line_items,x.metadata,NOW()
      FROM jsonb_to_recordset($1::jsonb) AS x(external_invoice_id TEXT,invoice_number TEXT,customer_name TEXT,customer_email TEXT,customer_address TEXT,status TEXT,subtotal NUMERIC,tax_amount NUMERIC,total NUMERIC,amount_paid NUMERIC,due_date DATE,created_at TIMESTAMP,notes TEXT,terms TEXT,sent_status TEXT,line_items JSONB,metadata JSONB)
      ON CONFLICT(external_source,external_invoice_id) WHERE external_invoice_id IS NOT NULL DO UPDATE SET
        customer_name=EXCLUDED.customer_name,customer_email=EXCLUDED.customer_email,customer_address=EXCLUDED.customer_address,status=EXCLUDED.status,subtotal=EXCLUDED.subtotal,tax_amount=EXCLUDED.tax_amount,total=EXCLUDED.total,amount_paid=EXCLUDED.amount_paid,due_date=EXCLUDED.due_date,notes=EXCLUDED.notes,terms=EXCLUDED.terms,sent_status=EXCLUDED.sent_status,
        line_items=CASE WHEN (EXCLUDED.external_metadata->>'details_complete')::boolean THEN EXCLUDED.line_items ELSE invoices.line_items END,
        external_metadata=invoices.external_metadata||EXCLUDED.external_metadata,imported_at=NOW(),updated_at=NOW()
      RETURNING *`,
      [JSON.stringify(batch)],
    );
    const byId = new Map(result.rows.map((row) => [String(row.external_invoice_id), row]));
    for (const remote of invoices.slice(start, start + 200)) {
      const local = byId.get(remote.external_invoice_id);
      if (!local) throw new Error('A Homeworks invoice mirror could not be verified.');
      mapped.push({
        ...local,
        ...remote,
        id: local.id,
        external_metadata: remote.metadata,
        source: 'official_homeworks_graphql',
      });
    }
  }
  const bySourceId = new Map(mapped.map(row => [row.external_invoice_id, row]));
  return requested.map(row => conflicts.has(row.external_invoice_id) ? {
    ...row, id: null, linkage_review_required: true,
    external_metadata: { ...row.metadata, linkage_review_required: true },
    source: 'official_homeworks_graphql',
  } : bySourceId.get(row.external_invoice_id));
}
module.exports = { mirrorInvoices };
