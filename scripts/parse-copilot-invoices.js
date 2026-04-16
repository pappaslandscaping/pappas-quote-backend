// ─────────────────────────────────────────────────────────────
// Pure parser for CopilotCRM's invoice list HTML.
//
// Source: POST https://secure.copilotcrm.com/finances/invoices/getInvoicesListAjax
// returns JSON: { html: "<table rows>", isEmpty: bool }
//
// This module is the deterministic, side-effect-free piece of the importer.
// It takes the html string (or the parsed { html } JSON), runs Cheerio over
// it, and returns an array of normalized row objects. The DB writer lives
// in scripts/import-copilot-invoices.js.
//
// Keeping the parser separate makes it trivial to unit-test against pinned
// fixtures whenever Copilot changes their markup.
// ─────────────────────────────────────────────────────────────

const cheerio = require('cheerio');
const {
  normalizeCopilotInvoiceStatus,
  normalizeCopilotSentStatus,
} = require('../lib/invoice-status');

// ── Helpers ────────────────────────────────────────────────────
function clean(text) {
  return (text || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseMoney(text) {
  if (text == null) return null;
  const s = String(text).replace(/[^0-9.\-]/g, '');
  if (s === '' || s === '-' || s === '.') return null;
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

function parseDate(text) {
  const t = clean(text);
  if (!t) return null;
  // Copilot list uses formats like "Aug 12, 2025" or "08/12/2025". Let
  // Date parse it; we return ISO yyyy-mm-dd for the DB DATE column.
  const d = new Date(t);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function extractIdFromHref(href, prefix) {
  if (!href) return null;
  const re = new RegExp(`${prefix.replace(/[/\\^$*+?.()|[\]{}]/g, '\\$&')}/(\\d+)`);
  const m = href.match(re);
  return m ? m[1] : null;
}

function mapStatus(rawStatus, amountPaid, total, sentStatus) {
  return normalizeCopilotInvoiceStatus({
    rawStatus,
    sentStatus,
    amountPaid,
    total,
    defaultUnpaidStatus: 'pending',
  }) || 'pending';
}

// Find the best <td> for a given header label. CopilotCRM's column order
// can shift; keying off the column header text is more resilient than
// hardcoded indices. We index columns once per table.
function buildColumnIndex($, $table) {
  const headers = [];
  $table.find('thead th').each((i, th) => {
    headers.push(clean($(th).text()).toLowerCase());
  });
  return headers;
}

function colByLabel(columns, ...labels) {
  for (const label of labels) {
    const want = label.toLowerCase();
    const idx = columns.findIndex(h => h === want || h.includes(want));
    if (idx >= 0) return idx;
  }
  return -1;
}

// ── Main parser ────────────────────────────────────────────────
function parseInvoiceListHtml(htmlOrPayload) {
  // Accept either a raw HTML string or the full JSON payload.
  let html;
  if (typeof htmlOrPayload === 'string') {
    html = htmlOrPayload;
  } else if (htmlOrPayload && typeof htmlOrPayload === 'object' && typeof htmlOrPayload.html === 'string') {
    html = htmlOrPayload.html;
  } else {
    throw new Error('parseInvoiceListHtml: expected an HTML string or { html } object');
  }

  // Wrap in a <table> if Copilot returned just <tr> rows. Cheerio is
  // forgiving but explicit framing makes column-index detection reliable
  // when a header isn't included.
  const wrapped = /\<table/i.test(html) ? html : `<table>${html}</table>`;
  const $ = cheerio.load(wrapped, { decodeEntities: true });

  const rows = [];
  $('table').each((_, tableEl) => {
    const $table = $(tableEl);
    const columns = buildColumnIndex($, $table);

    // If the response had no <thead>, fall back to a positional schema in
    // the order CopilotCRM's invoice list typically sends. This is the
    // documented field list from the user, in display order.
    const fallback = [
      'invoice #', 'date', 'customer', 'property', 'crew',
      'tax', 'total', 'due', 'paid', 'credit', 'status', 'sent',
    ];
    const cols = columns.length > 0 ? columns : fallback;

    const idx = {
      invoice:  colByLabel(cols, 'invoice #', 'invoice', 'inv #', 'inv'),
      date:     colByLabel(cols, 'date', 'invoice date', 'created'),
      customer: colByLabel(cols, 'customer', 'client'),
      property: colByLabel(cols, 'property', 'address'),
      crew:     colByLabel(cols, 'crew', 'team'),
      tax:      colByLabel(cols, 'tax', 'invoice tax'),
      total:    colByLabel(cols, 'total', 'invoice total', 'amount'),
      due:      colByLabel(cols, 'due', 'total due', 'balance'),
      paid:     colByLabel(cols, 'paid', 'amount paid'),
      credit:   colByLabel(cols, 'credit', 'credit available'),
      status:   colByLabel(cols, 'status'),
      sent:     colByLabel(cols, 'sent'),
    };

    $table.find('tbody tr, > tr').each((_, trEl) => {
      const $tr = $(trEl);
      // Skip header-ish or summary rows
      if ($tr.find('th').length > 0 && $tr.find('td').length === 0) return;
      const $tds = $tr.find('td');
      if ($tds.length === 0) return;

      const row = {};
      const cell = (i) => (i >= 0 && i < $tds.length) ? $($tds[i]) : null;
      const cellText = (i) => clean((cell(i) && cell(i).text()) || '');

      // External id from <tr id="...">. Strip non-digits in case of an
      // "invoice_123" prefix.
      const trId = $tr.attr('id') || '';
      row.external_invoice_id = trId.replace(/[^0-9]/g, '') || trId || null;

      // Invoice # cell often contains <a href="/finances/invoices/view/123">INV-1234</a>
      const $invCell = cell(idx.invoice);
      const $invAnchor = $invCell ? $invCell.find('a').first() : null;
      row.invoice_number = clean($invAnchor && $invAnchor.length ? $invAnchor.text() : ($invCell ? $invCell.text() : ''));
      row.view_path = $invAnchor && $invAnchor.length ? ($invAnchor.attr('href') || null) : null;
      // Edit link if present anywhere in the row.
      const $editAnchor = $tr.find('a[href*="/edit/"], a[href*="/invoices/edit"]').first();
      row.edit_path = $editAnchor.length ? ($editAnchor.attr('href') || null) : null;

      row.invoice_date = parseDate(cellText(idx.date));

      // Customer cell often has <a href="/customers/details/{id}">Name</a>
      // and an email below it.
      const $custCell = cell(idx.customer);
      if ($custCell) {
        const $custAnchor = $custCell.find('a[href*="/customers/details/"]').first();
        row.customer_name = clean($custAnchor.length ? $custAnchor.text() : $custCell.find('a').first().text() || $custCell.text());
        row.copilot_customer_id = extractIdFromHref($custAnchor.attr('href'), '/customers/details');
        // Email: try mailto: link first, then a plain email pattern in cell text.
        const $mailto = $custCell.find('a[href^="mailto:"]').first();
        if ($mailto.length) {
          row.customer_email = clean($mailto.attr('href').replace(/^mailto:/i, ''));
        } else {
          const m = $custCell.text().match(/[\w.+-]+@[\w-]+\.[\w.-]+/);
          row.customer_email = m ? m[0] : null;
        }
      }

      // Property cell: name + address. Copilot tends to show the property
      // name on one line and the street address below.
      const $propCell = cell(idx.property);
      if ($propCell) {
        // Use line-broken text where possible.
        const propHtml = $propCell.html() || '';
        const lines = propHtml
          .split(/<br\s*\/?>(?:\s*)/i)
          .map(seg => clean(cheerio.load('<x>' + seg + '</x>')('x').text()))
          .filter(Boolean);
        if (lines.length >= 2) {
          row.property_name = lines[0];
          row.property_address = lines.slice(1).join(', ');
        } else {
          row.property_name = clean($propCell.text()) || null;
          row.property_address = null;
        }
      }

      row.crew = cellText(idx.crew) || null;
      row.tax_amount = parseMoney(cellText(idx.tax));
      row.total = parseMoney(cellText(idx.total));
      row.total_due = parseMoney(cellText(idx.due));
      row.amount_paid = parseMoney(cellText(idx.paid));
      row.credit_available = parseMoney(cellText(idx.credit));
      row.raw_status = cellText(idx.status) || null;
      row.sent_status = normalizeCopilotSentStatus(cellText(idx.sent) || null);
      row.status = mapStatus(row.raw_status, row.amount_paid, row.total, row.sent_status);

      // Skip obvious junk rows (no id and no invoice number)
      if (!row.external_invoice_id && !row.invoice_number) return;

      rows.push(row);
    });
  });

  return rows;
}

module.exports = {
  parseInvoiceListHtml,
  // exported for tests
  _internal: { clean, parseMoney, parseDate, mapStatus, extractIdFromHref },
};
