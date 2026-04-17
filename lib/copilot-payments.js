const crypto = require('crypto');
const cheerio = require('cheerio');
const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
} = require('./copilot-metric-sources');

const COPILOT_PAYMENTS_BASE_PATH = '/finances/payments';
const COPILOT_PAYMENTS_BASE_PATH_WITH_SLASH = '/finances/payments/';

function cleanText(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeHeader(value) {
  return cleanText(value).toLowerCase();
}

function parseCurrencyAmount(value) {
  const normalized = String(value || '').replace(/[^0-9.-]/g, '');
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parsePaymentDate(value) {
  const raw = cleanText(value);
  if (!raw) return null;

  const dateOnlyMatch = raw.match(/^([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})$/);
  if (dateOnlyMatch) {
    const parsed = new Date(`${dateOnlyMatch[1]} 12:00:00 UTC`);
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString();
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function extractInvoiceNumberFromDetails(value) {
  const text = cleanText(value);
  if (!text) return null;
  const match = text.match(/invoice\s*#\s*([a-z0-9-]+)/i);
  return match ? match[1].trim() : null;
}

function buildHashedPaymentKey(parts) {
  return crypto
    .createHash('sha1')
    .update(parts.map((part) => cleanText(part)).join('|'))
    .digest('hex')
    .slice(0, 24);
}

function findPaymentsTable($) {
  return $('table').toArray().find((table) => {
    const headerRow = $(table).find('thead tr').first().length
      ? $(table).find('thead tr').first()
      : $(table).find('tr').first();
    const headers = headerRow.find('th,td').toArray()
      .map((cell) => normalizeHeader($(cell).text()));
    if (!headers.length) return false;
    const required = [
      'date',
      'payer / payee',
      'amount',
      'tip',
      'method',
      'details',
      'notes',
    ];
    const matches = required.filter((header) => headers.includes(header)).length;
    return matches >= 5;
  }) || null;
}

function extractPaginationTotal($) {
  const text = cleanText($('body').text());
  const patterns = [
    /\b\d+\s*-\s*\d+\s+of\s+(\d+)\b/i,
    /\bof\s+(\d+)\b/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const total = parseInt(match[1], 10);
    if (Number.isFinite(total)) return total;
  }
  return null;
}

function extractPaginationPaths($, pageUrl) {
  const base = new URL(pageUrl || `https://secure.copilotcrm.com${COPILOT_PAYMENTS_BASE_PATH}`);
  const paths = new Set();
  $('a[href]').each((_, link) => {
    const href = ($(link).attr('href') || '').trim();
    if (!href || href.startsWith('#') || href.startsWith('javascript:')) return;
    try {
      const resolved = new URL(href, base);
      if (
        resolved.pathname !== COPILOT_PAYMENTS_BASE_PATH
        && resolved.pathname !== COPILOT_PAYMENTS_BASE_PATH_WITH_SLASH
      ) return;
      const page = resolved.searchParams.get('page') || resolved.searchParams.get('p');
      if (page && page !== '1') {
        const normalizedPath = resolved.pathname === COPILOT_PAYMENTS_BASE_PATH_WITH_SLASH
          ? COPILOT_PAYMENTS_BASE_PATH
          : resolved.pathname;
        paths.add(`${normalizedPath}${resolved.search}`);
      }
    } catch (_error) {
      // ignore malformed links
    }
  });
  return Array.from(paths);
}

function findRowPaymentPath($, row, pageUrl) {
  const base = new URL(pageUrl || `https://secure.copilotcrm.com${COPILOT_PAYMENTS_BASE_PATH}`);
  const links = $(row).find('a[href]').toArray();
  for (const link of links) {
    const href = ($(link).attr('href') || '').trim();
    if (!href) continue;
    try {
      const resolved = new URL(href, base);
      if (/\/finances\/payments(\/|$)/i.test(resolved.pathname) || /payment/i.test(resolved.pathname)) {
        return `${resolved.pathname}${resolved.search}`;
      }
    } catch (_error) {
      // ignore malformed links
    }
  }
  return null;
}

function buildExternalPaymentKey({
  rowId,
  paymentPath,
  dateText,
  customerName,
  amount,
  tipAmount,
  method,
  details,
  notes,
}) {
  const normalizedRowId = cleanText(rowId);
  if (normalizedRowId) return `row:${normalizedRowId}`;
  const normalizedPath = cleanText(paymentPath);
  if (normalizedPath) return `path:${normalizedPath}`;
  return `hash:${buildHashedPaymentKey([
    dateText,
    customerName,
    amount,
    tipAmount,
    method,
    details,
    notes,
  ])}`;
}

function parseCopilotPaymentsHtml(html, pageUrl) {
  const $ = cheerio.load(html || '');
  const table = findPaymentsTable($);
  if (!table) {
    return {
      payments: [],
      total: extractPaginationTotal($) || 0,
      page_paths: extractPaginationPaths($, pageUrl),
    };
  }

  const headerRow = $(table).find('thead tr').first().length
    ? $(table).find('thead tr').first()
    : $(table).find('tr').first();
  const headers = headerRow.find('th,td').toArray()
    .map((cell) => normalizeHeader($(cell).text()));
  const headerIndex = Object.fromEntries(headers.map((header, index) => [header, index]));
  const rows = [];

  const bodyRows = $(table).find('tbody tr').length
    ? $(table).find('tbody tr').toArray()
    : $(table).find('tr').slice(1).toArray();

  bodyRows.forEach((row) => {
    const cells = $(row).find('td,th').toArray();
    if (!cells.length) return;

    function cellText(header) {
      const idx = headerIndex[header];
      if (!Number.isInteger(idx)) return '';
      return cleanText($(cells[idx]).text());
    }

    function cellNode(header) {
      const idx = headerIndex[header];
      if (!Number.isInteger(idx)) return null;
      return cells[idx] || null;
    }

    const dateText = cellText('date');
    const customerName = cellText('payer / payee');
    const amountText = cellText('amount');
    const tipText = cellText('tip');
    const method = cellText('method');
    const details = cellText('details');
    const notes = cellText('notes');

    if (!dateText && !customerName && !amountText && !details) return;

    const amount = parseCurrencyAmount(amountText);
    const tipAmount = parseCurrencyAmount(tipText);
    const paidAt = parsePaymentDate(dateText);
    const extractedInvoiceNumber = extractInvoiceNumberFromDetails(details);
    const customerNode = cellNode('payer / payee');
    const customerPath = customerNode ? cleanText($(customerNode).find('a').first().attr('href')) || null : null;
    const rowId = cleanText($(row).attr('id') || $(row).attr('data-id'));
    const paymentPath = findRowPaymentPath($, row, pageUrl);
    const externalPaymentKey = buildExternalPaymentKey({
      rowId,
      paymentPath,
      dateText,
      customerName,
      amount,
      tipAmount,
      method,
      details,
      notes,
    });

    rows.push({
      paid_at: paidAt,
      source_date_raw: dateText || null,
      customer_name: customerName || null,
      amount,
      tip_amount: tipAmount,
      method: method || null,
      details: details || null,
      notes: notes || null,
      extracted_invoice_number: extractedInvoiceNumber || null,
      external_source: 'copilotcrm',
      external_payment_key: externalPaymentKey,
      external_metadata: {
        row_id: rowId || null,
        payment_path: paymentPath || null,
        customer_path: customerPath || null,
        page_path: pageUrl ? new URL(pageUrl, 'https://secure.copilotcrm.com').pathname : COPILOT_PAYMENTS_BASE_PATH,
        raw_date: dateText || null,
        raw_payer_payee: customerName || null,
        raw_amount: amountText || null,
        raw_tip: tipText || null,
        raw_method: method || null,
        raw_details: details || null,
        raw_notes: notes || null,
      },
    });
  });

  return {
    payments: rows,
    total: extractPaginationTotal($) || rows.length,
    page_paths: extractPaginationPaths($, pageUrl),
  };
}

function normalizeCopilotPaymentsSnapshot(snapshot, sourceOverride = LIVE_COPILOT_SOURCE) {
  if (!snapshot || typeof snapshot !== 'object' || !Array.isArray(snapshot.payments)) return null;
  const payments = snapshot.payments.map((payment) => ({
    paid_at: payment.paid_at || null,
    source_date_raw: cleanText(payment.source_date_raw) || null,
    customer_name: cleanText(payment.customer_name) || null,
    amount: Number(payment.amount) || 0,
    tip_amount: Number(payment.tip_amount) || 0,
    method: cleanText(payment.method) || null,
    details: cleanText(payment.details) || null,
    notes: cleanText(payment.notes) || null,
    extracted_invoice_number: cleanText(payment.extracted_invoice_number) || null,
    external_source: 'copilotcrm',
    external_payment_key: cleanText(payment.external_payment_key) || buildExternalPaymentKey(payment),
    external_metadata: payment.external_metadata && typeof payment.external_metadata === 'object'
      ? payment.external_metadata
      : {},
  }));

  return {
    success: true,
    source: sourceOverride || snapshot.source || LIVE_COPILOT_SOURCE,
    as_of: snapshot.as_of || new Date().toISOString(),
    total: Number.isFinite(Number(snapshot.total)) ? Number(snapshot.total) : payments.length,
    payments,
  };
}

module.exports = {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
  COPILOT_PAYMENTS_BASE_PATH,
  cleanText,
  parseCurrencyAmount,
  parsePaymentDate,
  extractInvoiceNumberFromDetails,
  buildExternalPaymentKey,
  parseCopilotPaymentsHtml,
  normalizeCopilotPaymentsSnapshot,
  extractPaginationPaths,
};
