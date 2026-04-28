// ─────────────────────────────────────────────────────────────
// Pure parser for CopilotCRM's invoice DETAIL page.
//
// Source: GET https://secure.copilotcrm.com/finances/invoices/view/{id}
// returns a full HTML page. Hidden inputs #inv_id and #inv_cust_id are the
// stable identity signals; everything else (line items, notes, totals) is
// scraped from documented containers.
//
// Returns one normalized invoice object suitable for the importer's
// upsert path. Side-effect-free; safe to call from tests with raw fixture
// strings.
// ─────────────────────────────────────────────────────────────

const cheerio = require('cheerio');
const {
  normalizeCopilotInvoiceStatus,
  normalizeCopilotSentStatus,
} = require('../lib/invoice-status');

// ── Helpers (mirrored from the list parser to keep this file standalone) ──
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

function parseNumber(text) {
  if (text == null) return null;
  const s = String(text).replace(/[^0-9.\-]/g, '');
  if (s === '' || s === '-' || s === '.') return null;
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

function parsePercent(text) {
  if (text == null) return null;
  const s = String(text).replace(/[^0-9.\-]/g, '');
  if (s === '' || s === '-' || s === '.') return null;
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

function parseDate(text) {
  const t = clean(text);
  if (!t) return null;
  const d = new Date(t);
  if (isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function mapStatus(rawStatus, amountPaid, total, dueDate, sentStatus) {
  return normalizeCopilotInvoiceStatus({
    rawStatus,
    sentStatus,
    amountPaid,
    total,
    dueDate,
    defaultUnpaidStatus: null,
  });
}

// Read either input.value or [data-value], whichever fires first.
function valOrText($el) {
  if (!$el || !$el.length) return null;
  const v = $el.attr('value') || $el.val && $el.val();
  if (v != null && v !== '') return clean(v);
  const txt = clean($el.text());
  return txt || null;
}

// Walk a label/value table where each <tr> has two <td>s and the first cell
// is the label. Returns a Map of normalized label → raw value-cell text.
function readLabelValueTable($, $table) {
  const out = new Map();
  $table.find('tr').each((_, tr) => {
    const $tds = cheerio.load('<x>' + $.html(tr) + '</x>')('x').find('td');
    // Use the live cheerio reference instead so attributes are intact:
    const $live = $(tr).find('td');
    if ($live.length < 2) return;
    const label = clean($live.eq(0).text()).toLowerCase().replace(/[:\s]+$/, '');
    const valueText = $live.eq($live.length - 1).text();
    if (label) out.set(label, valueText);
  });
  return out;
}

function pick(map, ...keys) {
  for (const k of keys) {
    const want = k.toLowerCase();
    for (const [label, value] of map.entries()) {
      if (label === want || label.includes(want)) return value;
    }
  }
  return null;
}

function collectLabelValues($) {
  const out = new Map();
  $('table').each((_, table) => {
    const rows = readLabelValueTable($, $(table));
    for (const [label, value] of rows.entries()) {
      if (!out.has(label)) out.set(label, value);
    }
  });
  return out;
}

function parseActivityPaidAt($) {
  const paymentDates = [];
  $('table.copilot-table tr').each((_, tr) => {
    const $cells = $(tr).find('td');
    if ($cells.length < 2) return;
    const activityText = clean($cells.eq(0).text()).toLowerCase();
    if (!activityText.includes('payment made')) return;
    const parsedDate = parseDate($cells.eq(1).text());
    if (parsedDate) paymentDates.push(parsedDate);
  });
  if (!paymentDates.length) return null;
  return paymentDates.sort().at(-1);
}

function parseDescriptionCell($td) {
  const $clone = $td.clone();
  const detailsText = clean($clone.find('small').text()) || null;
  $clone.find('small').remove();

  const mainText = clean($clone.text());
  const headingText = clean($td.find('span').first().text()) || null;
  const dateMatch = mainText.match(/^([A-Z][a-z]{2}\s+\d{2},\s+\d{4})\s+(.+)$/);

  const service_date = dateMatch ? parseDate(dateMatch[1]) : null;
  const description = headingText || (dateMatch ? clean(dateMatch[2]) : mainText) || null;

  return { service_date, description, detailsText };
}

function parseCustomerAddressBlock($) {
  const $address = $('address').filter((_, el) => $(el).find('a[href*="/customers/details/"]').length > 0).first();
  if (!$address.length) return null;

  const $clone = $address.clone();
  $clone.find('a, svg').remove();
  const text = clean(
    $clone
      .html()
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/<[^>]+>/g, ' ')
  );

  return text || null;
}

function buildParseDiagnostics($, line_items) {
  return {
    description_table_count: $('.table--description').length,
    description_row_count: $('.table--description tbody tr').length,
    subtotal_table_count: $('.table--sub-total').length,
    customer_link_count: $('a[href*="/customers/details/"]').length,
    line_item_count: Array.isArray(line_items) ? line_items.length : 0,
  };
}

// ── Main parser ────────────────────────────────────────────────
function parseInvoiceDetailHtml(html) {
  if (typeof html !== 'string' || html.length === 0) {
    throw new Error('parseInvoiceDetailHtml: expected an HTML string');
  }
  const $ = cheerio.load(html, { decodeEntities: true });

  // ── Identity ─────────────────────────────────────────────
  // Hidden inputs are the most reliable signals when present.
  const external_invoice_id = (
    valOrText($('#inv_id')) ||
    valOrText($('input[name="inv_id"]')) ||
    valOrText($('input[name="invoice_id"]')) ||
    null
  );
  const copilot_customer_id = (
    valOrText($('#inv_cust_id')) ||
    valOrText($('input[name="inv_cust_id"]')) ||
    valOrText($('input[name="customer_id"]')) ||
    null
  );

  // ── Invoice number ───────────────────────────────────────
  // Try several common locations. Copilot may render it in a heading,
  // a hidden input, or a "Invoice #NNNN" string somewhere on the page.
  let invoice_number = (
    valOrText($('#invoice_number')) ||
    valOrText($('input[name="invoice_number"]')) ||
    valOrText($('.invoice-number, .invoice_number, .inv-number')) ||
    null
  );
  if (!invoice_number) {
    // Scan headings/title for "Invoice #NNNN" or "Invoice NNNN"
    const haystacks = [
      clean($('title').text()),
      clean($('h1, h2, h3').first().text()),
      clean($('.invoice-header, .invoice_header, .inv-header').text()),
    ].filter(Boolean);
    for (const h of haystacks) {
      const m = h.match(/invoice\s*#?\s*(\d+)/i);
      if (m) { invoice_number = m[1]; break; }
    }
  }
  // Final fallback: when hidden #inv_id is the primary key Copilot uses, it
  // is usually identical to the invoice number too.
  if (!invoice_number && external_invoice_id) invoice_number = external_invoice_id;

  // ── Invoice date ─────────────────────────────────────────
  let invoice_date = (
    parseDate(valOrText($('#invoice_date'))) ||
    parseDate(valOrText($('input[name="invoice_date"]'))) ||
    parseDate(clean($('.invoice-date, .inv-date, .invoice_date').first().text()))
  );

  // ── Customer name + address ──────────────────────────────
  // Try a series of common containers; fall back to anchor with /customers/details/.
  let customer_name = (
    clean($('.customer-name, .cust-name, .customer_name').first().text()) ||
    clean($('.bill-to .name, .billing-name').first().text()) ||
    null
  );
  if (!customer_name) {
    const $a = $('a[href*="/customers/details/"]').first();
    if ($a.length) customer_name = clean($a.text());
  }

  let customer_address = (
    clean($('.customer-address, .cust-address, .customer_address').first().text()) ||
    clean($('.bill-to .address, .billing-address').first().text()) ||
    null
  );
  if (!customer_address) {
    customer_address = parseCustomerAddressBlock($);
  }

  let customer_email = (
    valOrText($('#email')) ||
    valOrText($('input[name="email"]')) ||
    null
  );
  const $mailto = $('a[href^="mailto:"]').first();
  if (!customer_email && $mailto.length) customer_email = clean($mailto.attr('href').replace(/^mailto:/i, ''));

  // ── Property info (mostly informational) ─────────────────
  const property_name = clean($('.property-name, .property_name').first().text()) || null;
  let property_address = clean($('.property-address, .property_address').first().text()) || null;

  // ── Line items ───────────────────────────────────────────
  const line_items = [];
  const $itemTable = $('.table--description').first();
  if ($itemTable.length) {
    // Build a label→column-index map from the header.
    const colByLabel = {};
    $itemTable.find('thead th').each((i, th) => {
      const label = clean($(th).text()).toLowerCase();
      if (!label) return;
      if (label.includes('date'))                 colByLabel.date         = i;
      else if (label.includes('desc') || label.includes('item') || label.includes('service')) colByLabel.description = i;
      else if (label.includes('rate') || label.includes('price')) colByLabel.rate = i;
      else if (label.includes('qty') || label.includes('quantity')) colByLabel.quantity = i;
      else if (label.includes('hour') || label.includes('budget')) colByLabel.budgeted_hours = i;
      else if (label.includes('tax'))             colByLabel.tax_percent  = i;
      else if (label.includes('total') || label.includes('amount')) colByLabel.line_total = i;
    });
    const colOrFallback = (k, fallbackIdx) => (colByLabel[k] != null ? colByLabel[k] : fallbackIdx);

    $itemTable.find('tbody tr').each((_, tr) => {
      const $tds = $(tr).find('td');
      if ($tds.length === 0) return;
      if ($tds.length === 1) {
        const singleText = clean($tds.eq(0).text());
        const propertyMatch = singleText.match(/^Property Address:\s*(.+)$/i);
        if (propertyMatch && !property_address) {
          property_address = clean(propertyMatch[1]);
        }
        return;
      }
      const cell = (i) => (i >= 0 && i < $tds.length) ? clean($tds.eq(i).text()) : '';
      const parsedDescription = parseDescriptionCell($tds.eq(colOrFallback('description', 1)));
      if (!property_address && parsedDescription.detailsText) {
        const propertyMatch = parsedDescription.detailsText.match(/^Property Address:\s*(.+)$/i);
        if (propertyMatch) {
          property_address = clean(propertyMatch[1]);
        }
      }
      // Positional fallback when no header (date, description, rate, qty, hours, tax, total)
      const item = {
        service_date:    colByLabel.date != null ? parseDate(cell(colOrFallback('date', 0))) : parsedDescription.service_date,
        description:     parsedDescription.description,
        rate:            parseMoney(cell(colOrFallback('rate', 2))),
        quantity:        parseNumber(cell(colOrFallback('quantity', 3))),
        budgeted_hours:  parseNumber(cell(colOrFallback('budgeted_hours', 4))),
        tax_percent:     parsePercent(cell(colOrFallback('tax_percent', 5))),
        line_total:      parseMoney(cell(colOrFallback('line_total', $tds.length - 1))),
      };
      // Skip a row that is clearly empty (no description AND no money).
      if (!item.description && item.line_total == null && item.rate == null) return;
      line_items.push(item);
    });
  }

  // ── Totals from .table--sub-total ────────────────────────
  let subtotal = null, tax_amount = null, total = null, amount_paid = null, total_due = null;
  const $totalsTable = $('.table--sub-total').first();
  if ($totalsTable.length) {
    const totals = readLabelValueTable($, $totalsTable);
    subtotal     = parseMoney(pick(totals, 'subtotal', 'sub-total', 'sub total'));
    tax_amount   = parseMoney(pick(totals, 'tax'));
    total        = parseMoney(pick(totals, 'total') ); // note: matches 'total', 'invoice total'
    // Re-pick more specifically since "total" may match "total due" first.
    const totalRaw = pick(totals, 'invoice total');
    if (totalRaw != null) total = parseMoney(totalRaw);
    amount_paid  = parseMoney(pick(totals, 'amount paid', 'paid'));
    total_due    = parseMoney(pick(totals, 'total due', 'balance due', 'due'));
    // If the generic 'total' match grabbed 'total due', recover the true total
    // by picking the row whose label is exactly 'total'.
    for (const [label, value] of totals.entries()) {
      if (label === 'total') { total = parseMoney(value); break; }
    }
  }

  // Sometimes subtotal is omitted from the totals table — derive it.
  if (subtotal == null && total != null && tax_amount != null) {
    subtotal = Math.max(0, total - tax_amount);
  }

  if (!property_address) {
    const rawHtmlPropertyMatch = String(html).match(/Property Address:\s*([^<\r\n]+)/i);
    if (rawHtmlPropertyMatch) {
      property_address = clean(rawHtmlPropertyMatch[1]);
    }
  }

  if (!property_address) {
    const bodyPropertyMatch = clean($('body').text()).match(/Property Address:\s*(.+?)(?=(?:Invoice #|Invoice Date|Description|Subtotal|Tax|Total|Amount Due)\b)/i);
    if (bodyPropertyMatch) {
      property_address = clean(bodyPropertyMatch[1]);
    }
  }

  if (!property_address) {
    property_address = customer_address || null;
  }

  const labelValues = collectLabelValues($);
  const due_date = (
    parseDate(valOrText($('#due_date'))) ||
    parseDate(valOrText($('input[name="due_date"]'))) ||
    parseDate(clean($('.invoice-due-date, .due-date, .inv-due-date').first().text())) ||
    parseDate(pick(labelValues, 'due date', 'invoice due'))
  );

  const paidAtRaw = (
    valOrText($('#paid_at')) ||
    valOrText($('input[name="paid_at"]')) ||
    clean($('.invoice-paid-date, .paid-date, .inv-paid-date').first().text()) ||
    pick(labelValues, 'paid date', 'payment date', 'date paid')
  );
  const paid_at = parseDate(paidAtRaw) || parseActivityPaidAt($);

  // ── Notes + terms ────────────────────────────────────────
  const notes = clean($('.inv-notes-container').first().text()) || null;
  const terms = clean($('.inv-terms-table').first().text()) || null;

  // ── Status hint (best-effort; detail page may not display it) ──
  const rawStatus = (
    clean($('.invoice-status, .inv-status').first().text()) ||
    clean($('[data-invoice-status]').first().attr('data-invoice-status')) ||
    clean(valOrText($('#invoice_status'))) ||
    clean(pick(labelValues, 'invoice status')) ||
    null
  );
  const sent_status = normalizeCopilotSentStatus(
    clean($('.invoice-sent-status, .sent-status, .inv-sent-status').first().text()) ||
    clean(valOrText($('#sent_status'))) ||
    clean(pick(labelValues, 'sent', 'sent status', 'delivery status')) ||
    null
  );
  const status = mapStatus(rawStatus, amount_paid, total, due_date, sent_status);

  // ── Crew (informational) ─────────────────────────────────
  const crew = clean($('.invoice-crew, .crew-name').first().text()) || null;
  const parse_diagnostics = buildParseDiagnostics($, line_items);
  if (parse_diagnostics.description_row_count > 0 && line_items.length === 0) {
    parse_diagnostics.warning = 'description_table_present_but_no_line_items_parsed';
  }

  return {
    external_invoice_id,
    copilot_customer_id,
    invoice_number,
    invoice_date,
    customer_name,
    customer_email,
    customer_address,
    property_name,
    property_address,
    crew,
    line_items,
    notes,
    terms,
    subtotal,
    tax_amount,
    total,
    amount_paid,
    total_due,
    due_date,
    paid_at,
    raw_status: rawStatus,
    sent_status,
    status,
    parse_diagnostics,
  };
}

module.exports = {
  parseInvoiceDetailHtml,
  _internal: { clean, parseMoney, parseNumber, parsePercent, parseDate, mapStatus },
};
