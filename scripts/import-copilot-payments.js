const { extractInvoiceNumberFromDetails } = require('../lib/copilot-payments');

function roundMoney(value) {
  const normalized = Number(value) || 0;
  return Math.round(normalized * 100) / 100;
}

function isDateLikeInvoiceLabel(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  return /^[A-Z][a-z]{2} [0-9]{2}, [0-9]{4}$/.test(s)
    || /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(s)
    || /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function formatDateLikeInvoiceLabel(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const parsed = new Date(`${raw}T12:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toLocaleDateString('en-US', { month: 'short', day: '2-digit', year: 'numeric', timeZone: 'UTC' });
}

function normalizeCustomerName(value) {
  return String(value || '')
    .replace(/^(customer|payer|payee)\s*:\s*/i, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function toIsoDate(value) {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString().slice(0, 10);
  }
  const raw = String(value).trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw.slice(0, 10);
  return parsed.toISOString().slice(0, 10);
}

function chooseFallbackInvoiceMatch(candidates = [], extractedInvoiceDate) {
  if (!Array.isArray(candidates) || !candidates.length) return null;
  const targetDateLabel = formatDateLikeInvoiceLabel(extractedInvoiceDate);
  const dateLikeCandidates = targetDateLabel
    ? candidates.filter((candidate) => isDateLikeInvoiceLabel(candidate.invoice_number) && String(candidate.invoice_number).trim() === targetDateLabel)
    : [];
  return choosePreferredInvoiceMatch(dateLikeCandidates.length ? dateLikeCandidates : candidates);
}

function computeTaxPortionCollected({ amount, tip_amount, invoice_total, invoice_tax_amount }) {
  const grossAmount = Number(amount) || 0;
  const tipAmount = Number(tip_amount) || 0;
  const invoiceTotal = Number(invoice_total) || 0;
  const invoiceTaxAmount = Number(invoice_tax_amount) || 0;
  if (invoiceTotal <= 0 || invoiceTaxAmount <= 0) return 0;

  const appliedAmount = Math.max(grossAmount - tipAmount, 0);
  const cappedAppliedAmount = Math.min(appliedAmount, invoiceTotal);
  return roundMoney((cappedAppliedAmount / invoiceTotal) * invoiceTaxAmount);
}

function compareInvoiceCandidates(a, b) {
  const aCopilot = a?.external_source === 'copilotcrm' ? 1 : 0;
  const bCopilot = b?.external_source === 'copilotcrm' ? 1 : 0;
  if (aCopilot !== bCopilot) return bCopilot - aCopilot;

  const aImportedAt = a?.imported_at ? new Date(a.imported_at).getTime() : 0;
  const bImportedAt = b?.imported_at ? new Date(b.imported_at).getTime() : 0;
  if (aImportedAt !== bImportedAt) return bImportedAt - aImportedAt;

  const aUpdatedAt = a?.updated_at ? new Date(a.updated_at).getTime() : 0;
  const bUpdatedAt = b?.updated_at ? new Date(b.updated_at).getTime() : 0;
  if (aUpdatedAt !== bUpdatedAt) return bUpdatedAt - aUpdatedAt;

  return (Number(b?.id) || 0) - (Number(a?.id) || 0);
}

function choosePreferredInvoiceMatch(candidates = []) {
  if (!Array.isArray(candidates) || !candidates.length) return null;
  return [...candidates].sort(compareInvoiceCandidates)[0];
}

async function loadInvoiceMatches(pool, invoiceNumbers = []) {
  const payments = Array.isArray(invoiceNumbers) ? invoiceNumbers : [];
  const cleaned = Array.from(new Set(
    payments
      .map((payment) => String(payment?.extracted_invoice_number || '').trim())
      .filter(Boolean)
  ));
  const matches = new Map();

  if (cleaned.length) {
    const result = await pool.query(
      `SELECT id, invoice_number, customer_id, customer_name, total, tax_amount,
              external_source, external_invoice_id, external_metadata, imported_at, updated_at, created_at
         FROM invoices
        WHERE invoice_number = ANY($1::text[])
        ORDER BY invoice_number ASC`,
      [cleaned]
    );

    const grouped = new Map();
    result.rows.forEach((row) => {
      const key = String(row.invoice_number || '').trim();
      if (!key) return;
      if (!grouped.has(key)) grouped.set(key, []);
      grouped.get(key).push(row);
    });

    grouped.forEach((candidates, invoiceNumber) => {
      matches.set(invoiceNumber, choosePreferredInvoiceMatch(candidates));
    });
  }

  const fallbackPayments = payments.filter((payment) => {
    const invoiceNumber = String(payment?.extracted_invoice_number || '').trim();
    const invoiceDate = String(payment?.extracted_invoice_date || '').trim();
    const customerName = normalizeCustomerName(payment?.customer_name);
    return invoiceNumber && !matches.has(invoiceNumber) && invoiceDate && customerName;
  });
  if (!fallbackPayments.length) return matches;

  const fallbackDates = Array.from(new Set(fallbackPayments.map((payment) => String(payment.extracted_invoice_date).trim())));
  const fallbackNames = Array.from(new Set(fallbackPayments.map((payment) => normalizeCustomerName(payment.customer_name)).filter(Boolean)));
  const fallbackResult = await pool.query(
    `SELECT id, invoice_number, customer_id, customer_name, total, tax_amount,
            external_source, external_invoice_id, external_metadata, imported_at, updated_at, created_at
       FROM invoices
      WHERE external_source = 'copilotcrm'
        AND created_at::date = ANY($1::date[])
        AND lower(trim(customer_name)) = ANY($2::text[])`,
    [fallbackDates, fallbackNames]
  );

  const fallbackGrouped = new Map();
  fallbackResult.rows.forEach((row) => {
    const key = `${normalizeCustomerName(row.customer_name)}|${toIsoDate(row.created_at)}`;
    if (!fallbackGrouped.has(key)) fallbackGrouped.set(key, []);
    fallbackGrouped.get(key).push(row);
  });

  fallbackPayments.forEach((payment) => {
    const key = `${normalizeCustomerName(payment.customer_name)}|${String(payment.extracted_invoice_date).trim()}`;
    const candidates = fallbackGrouped.get(key) || [];
    if (!candidates.length) return;

    const preferred = chooseFallbackInvoiceMatch(candidates, payment.extracted_invoice_date);
    if (!preferred) return;
    matches.set(String(payment.extracted_invoice_number).trim(), preferred);
  });

  return matches;
}

function buildCopilotPaymentRecord(payment, invoiceMatch) {
  const invoiceNumber = String(payment.extracted_invoice_number || '').trim() || null;
  const externalMetadata = {
    ...(payment.external_metadata || {}),
    extracted_invoice_number: invoiceNumber,
    extracted_invoice_date: payment.extracted_invoice_date || null,
    invoice_match_status: invoiceMatch ? 'linked' : 'unresolved',
    matched_invoice_id: invoiceMatch?.id || null,
    matched_invoice_number: invoiceMatch?.invoice_number || null,
    matched_invoice_source: invoiceMatch?.external_source || null,
  };

  return {
    payment_id: null,
    invoice_id: invoiceMatch?.id || null,
    customer_id: invoiceMatch?.customer_id || null,
    customer_name: payment.customer_name || invoiceMatch?.customer_name || null,
    amount: roundMoney(payment.amount),
    tip_amount: roundMoney(payment.tip_amount),
    method: payment.method || null,
    details: payment.details || null,
    notes: payment.notes || null,
    paid_at: payment.paid_at || null,
    source_date_raw: payment.source_date_raw || null,
    status: 'completed',
    external_source: payment.external_source || 'copilotcrm',
    external_payment_key: payment.external_payment_key,
    external_metadata: externalMetadata,
    imported_at: new Date().toISOString(),
    extracted_invoice_number: invoiceNumber,
    extracted_invoice_date: payment.extracted_invoice_date || null,
    invoice_total: Number(invoiceMatch?.total) || 0,
    invoice_tax_amount: Number(invoiceMatch?.tax_amount) || 0,
    tax_portion_collected: computeTaxPortionCollected({
      amount: payment.amount,
      tip_amount: payment.tip_amount,
      invoice_total: invoiceMatch?.total,
      invoice_tax_amount: invoiceMatch?.tax_amount,
    }),
  };
}

function getExtractedInvoiceNumberForPayment(payment) {
  const metadataInvoiceNumber = String(payment?.external_metadata?.extracted_invoice_number || '').trim();
  if (metadataInvoiceNumber) return metadataInvoiceNumber;
  const explicitInvoiceNumber = String(payment?.extracted_invoice_number || '').trim();
  if (explicitInvoiceNumber) return explicitInvoiceNumber;
  return extractInvoiceNumberFromDetails(payment?.details || '');
}

function describeCopilotPaymentLinkage(payment, invoiceMatch = null) {
  const extractedInvoiceNumber = getExtractedInvoiceNumberForPayment(payment);
  if (payment?.invoice_id) {
    return {
      link_status: 'linked',
      extracted_invoice_number: extractedInvoiceNumber,
      link_failure_reason: null,
    };
  }
  if (!extractedInvoiceNumber) {
    return {
      link_status: 'unresolved',
      extracted_invoice_number: null,
      link_failure_reason: 'No invoice number found in Copilot payment details.',
    };
  }
  if (!invoiceMatch) {
    return {
      link_status: 'unresolved',
      extracted_invoice_number: extractedInvoiceNumber,
      link_failure_reason: `Invoice #${extractedInvoiceNumber} was not found in YardDesk.`,
    };
  }
  return {
    link_status: 'unresolved',
    extracted_invoice_number: extractedInvoiceNumber,
    link_failure_reason: `Invoice #${extractedInvoiceNumber} exists in YardDesk, but this payment row is still unresolved.`,
  };
}

async function upsertCopilotPayments({ pool, payments = [] }) {
  const normalizedPayments = Array.isArray(payments)
    ? payments.filter((payment) => payment && payment.external_payment_key)
    : [];
  const invoiceMatches = await loadInvoiceMatches(
    pool,
    normalizedPayments
  );
  const existingKeysResult = await pool.query(
    `SELECT external_payment_key
       FROM payments
      WHERE external_source = 'copilotcrm'
        AND external_payment_key = ANY($1::text[])`,
    [normalizedPayments.map((payment) => payment.external_payment_key)]
  );
  const existingKeys = new Set(existingKeysResult.rows.map((row) => row.external_payment_key));

  const summary = {
    total: normalizedPayments.length,
    inserted: 0,
    updated: 0,
    linked: 0,
    unresolved: 0,
    payments: [],
  };

  for (const payment of normalizedPayments) {
    const invoiceMatch = payment.extracted_invoice_number
      ? invoiceMatches.get(String(payment.extracted_invoice_number).trim()) || null
      : null;
    const prepared = buildCopilotPaymentRecord(payment, invoiceMatch);

    const writeParams = [
      prepared.payment_id,
      prepared.invoice_id,
      prepared.customer_id,
      prepared.customer_name,
      prepared.amount,
      prepared.tip_amount,
      prepared.method,
      prepared.status,
      prepared.details,
      prepared.notes,
      prepared.paid_at,
      prepared.source_date_raw,
      prepared.external_source,
      prepared.external_payment_key,
      JSON.stringify(prepared.external_metadata),
      prepared.imported_at,
    ];

    const updateResult = await pool.query(
      `WITH target AS (
         SELECT id
           FROM payments
          WHERE external_source = $13
            AND external_payment_key = $14
          ORDER BY id DESC
          LIMIT 1
       )
       UPDATE payments p
          SET payment_id = $1,
              invoice_id = $2,
              customer_id = $3,
              customer_name = $4,
              amount = $5,
              tip_amount = $6,
              method = $7,
              status = $8,
              details = $9,
              notes = $10,
              paid_at = $11,
              source_date_raw = $12,
              external_metadata = $15,
              imported_at = $16,
              updated_at = NOW()
         FROM target
        WHERE p.id = target.id
       RETURNING p.id, p.invoice_id`,
      writeParams
    );

    const upsertResult = updateResult.rows.length
      ? updateResult
      : await pool.query(
          `INSERT INTO payments (
             payment_id, invoice_id, customer_id, customer_name, amount, tip_amount, method,
             status, details, notes, paid_at, source_date_raw,
             external_source, external_payment_key, external_metadata, imported_at
           ) VALUES (
             $1, $2, $3, $4, $5, $6, $7,
             $8, $9, $10, $11, $12,
             $13, $14, $15, $16
           )
           RETURNING id, invoice_id`,
          writeParams
        );

    if (existingKeys.has(prepared.external_payment_key)) summary.updated += 1;
    else summary.inserted += 1;
    if (upsertResult.rows[0]?.invoice_id) summary.linked += 1;
    else summary.unresolved += 1;

    summary.payments.push({
      id: upsertResult.rows[0]?.id || null,
      external_payment_key: prepared.external_payment_key,
      customer_name: prepared.customer_name,
      amount: prepared.amount,
      tip_amount: prepared.tip_amount,
      extracted_invoice_number: prepared.extracted_invoice_number,
      invoice_id: upsertResult.rows[0]?.invoice_id || null,
      invoice_total: prepared.invoice_total,
      invoice_tax_amount: prepared.invoice_tax_amount,
      tax_portion_collected: prepared.tax_portion_collected,
      link_status: upsertResult.rows[0]?.invoice_id ? 'linked' : 'unresolved',
    });
  }

  return summary;
}

function hydratePaymentRecord(record) {
  const hydrated = {
    ...record,
    amount: roundMoney(record.amount),
    tip_amount: roundMoney(record.tip_amount),
    invoice_total: roundMoney(record.invoice_total),
    invoice_tax_amount: roundMoney(record.invoice_tax_amount),
  };
  hydrated.applied_amount = roundMoney(
    Math.min(
      Math.max((Number(hydrated.amount) || 0) - (Number(hydrated.tip_amount) || 0), 0),
      Number(hydrated.invoice_total) || 0
    )
  );
  hydrated.tax_portion_collected = roundMoney(
    record.tax_portion_collected != null
      ? record.tax_portion_collected
      : computeTaxPortionCollected(hydrated)
  );
  return hydrated;
}

module.exports = {
  roundMoney,
  computeTaxPortionCollected,
  choosePreferredInvoiceMatch,
  chooseFallbackInvoiceMatch,
  loadInvoiceMatches,
  buildCopilotPaymentRecord,
  getExtractedInvoiceNumberForPayment,
  describeCopilotPaymentLinkage,
  upsertCopilotPayments,
  hydratePaymentRecord,
};
