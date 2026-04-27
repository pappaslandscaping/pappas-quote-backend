const fs = require('fs');
const path = require('path');

const COMPANY = {
  name: 'Pappas & Co. Landscaping',
  phone: '(440) 886-7318',
  email: 'hello@pappaslandscaping.com',
  website: 'www.pappaslandscaping.com',
  remitLines: [
    'PO Box 770057',
    'Lakewood, OH 44107',
    '(440) 886-7318',
    'hello@pappaslandscaping.com',
  ],
};

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[char]));
}

function money(value) {
  return `$${Number(value || 0).toFixed(2)}`;
}

const MAIL_DEBUG_INVOICE_NUMBER = '10273';

function parseNumericCandidate(candidate) {
  if (candidate === null || candidate === undefined || candidate === '') return null;
  if (typeof candidate === 'number') return Number.isFinite(candidate) ? candidate : null;
  const cleaned = String(candidate).trim().replace(/[^0-9.\-]/g, '');
  if (!cleaned || cleaned === '-' || cleaned === '.') return null;
  const numeric = Number(cleaned);
  return Number.isFinite(numeric) ? numeric : null;
}

function roundMoney(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function normalizeCityStateZip(line) {
  const raw = String(line || '').trim();
  if (!raw) return '';
  const compact = raw.replace(/\s+/g, ' ');
  const match = compact.match(/^(.*?)(?:,\s*|\s+)([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/i);
  if (!match) return compact;
  const city = match[1].replace(/,\s*$/, '').trim();
  const state = match[2].toUpperCase();
  const zip = match[3];
  return `${city}, ${state} ${zip}`;
}

function splitSingleLineAddress(line) {
  const compact = String(line || '').trim().replace(/\s+/g, ' ');
  if (!compact) return [];

  const streetSuffixes = [
    'alley', 'aly', 'ave', 'avenue', 'blvd', 'boulevard', 'cir', 'circle',
    'court', 'ct', 'drive', 'dr', 'highway', 'hwy', 'lane', 'ln', 'parkway',
    'pkwy', 'place', 'pl', 'plaza', 'plz', 'road', 'rd', 'square', 'sq',
    'street', 'st', 'terrace', 'ter', 'trail', 'trl', 'way',
  ].join('|');

  const pattern = new RegExp(
    `^(.+?\\b(?:${streetSuffixes})\\.?` +
      `(?:\\s+(?:apt|apartment|unit|suite|ste|#)\\s*[^,]+)?)` +
      `\\s+(.+?)(?:,\\s*|\\s+)([A-Z]{2})\\s+(\\d{5}(?:-\\d{4})?)$`,
    'i'
  );
  const match = compact.match(pattern);
  if (!match) return [normalizeCityStateZip(compact)];

  const street = match[1].trim();
  const city = match[2].replace(/,\s*$/, '').trim();
  const state = match[3].toUpperCase();
  const zip = match[4];
  return [street, `${city}, ${state} ${zip}`];
}

function splitAddressLines(value) {
  const lines = String(value || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, 4);

  if (!lines.length) return [];
  if (lines.length === 1) return splitSingleLineAddress(lines[0]);
  if (lines.length === 2) return [lines[0], normalizeCityStateZip(lines[1])];

  const first = lines[0];
  const rest = normalizeCityStateZip(lines.slice(1).join(' '));
  return rest ? [first, rest] : [first];
}

function normalizeLineItems(lineItems) {
  if (Array.isArray(lineItems)) return lineItems;
  if (typeof lineItems === 'string') {
    try {
      const parsed = JSON.parse(lineItems);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  }
  return [];
}

function readLogoDataUri() {
  const candidates = [
    path.join(__dirname, '..', 'public', 'images', 'mail-logo-full.png'),
    path.join(__dirname, '..', 'public', 'images', 'email-logo.png'),
    path.join(__dirname, '..', 'public', 'logo.png'),
    path.join(__dirname, '..', 'public', 'badge-logo-transparent.png'),
  ];

  for (const candidate of candidates) {
    try {
      if (!fs.existsSync(candidate)) continue;
      const bytes = fs.readFileSync(candidate);
      const ext = path.extname(candidate).toLowerCase() === '.jpg' ? 'jpeg' : 'png';
      return `data:image/${ext};base64,${bytes.toString('base64')}`;
    } catch (_error) {
      continue;
    }
  }
  return null;
}

function toSummary(invoice) {
  const metadata = invoice.metadata && typeof invoice.metadata === 'object'
    ? invoice.metadata
    : {};
  const financials = deriveInvoiceFinancials(invoice);

  const explicitPriorBalance = parseNumericCandidate(
    metadata.prior_balance
    ?? metadata.previous_balance
    ?? metadata.past_due_balance
  );
  const metadataAccountDue = parseNumericCandidate(metadata.total_due_on_account ?? metadata.total_due);
  const metadataOutstanding = parseNumericCandidate(metadata.outstanding_balance);

  let priorBalance = 0;
  if (explicitPriorBalance !== null && explicitPriorBalance > 0) {
    priorBalance = roundMoney(explicitPriorBalance);
  } else if (metadataAccountDue !== null && metadataAccountDue > financials.total + 0.009) {
    priorBalance = roundMoney(metadataAccountDue - financials.total);
  } else if (metadataOutstanding !== null && metadataOutstanding > financials.total + 0.009) {
    priorBalance = roundMoney(metadataOutstanding - financials.total);
  }

  const thisInvoice = financials.total;
  const totalDueOnAccount = roundMoney(thisInvoice + priorBalance);

  return {
    priorBalance,
    thisInvoice,
    totalDueOnAccount,
    subtotal: financials.subtotal,
    tax: financials.tax,
    total: financials.total,
  };
}

function serviceTitle(item) {
  return String(item.name || item.description || 'Service').trim() || 'Service';
}

function stripMarkup(value) {
  return String(value || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isFuelSurcharge(item) {
  return serviceTitle(item).trim().toLowerCase() === 'fuel surcharge';
}

function formatHumanDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const date = new Date(`${raw}T00:00:00`);
    if (!Number.isNaN(date.getTime())) {
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }
  }

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  }

  return raw;
}

function serviceDateLabel(item, { hideFuel = false } = {}) {
  if (hideFuel && isFuelSurcharge(item)) return '';
  const raw = String(item.service_date_raw || item.date || item.service_date || '').trim();
  return formatHumanDate(raw);
}

function orderLineItemsForMail(lineItems) {
  const items = normalizeLineItems(lineItems).slice();
  const normal = [];
  const surcharge = [];
  items.forEach((item) => {
    if (isFuelSurcharge(item)) surcharge.push(item);
    else normal.push(item);
  });
  return normal.concat(surcharge);
}

function shortDescription(item) {
  if (isFuelSurcharge(item)) return 'Route fuel recovery';
  const raw = stripMarkup(item.rich_description || item.richDescription || item.description || '');
  if (!raw) return '';

  const title = serviceTitle(item).toLowerCase();
  const trimmed = raw.replace(new RegExp(`^${title.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s*[-:]?\\s*`, 'i'), '').trim();
  const sentence = trimmed.split(/(?<=[.!?])\s+/)[0].trim() || trimmed;
  return sentence.length > 88 ? `${sentence.slice(0, 85).trimEnd()}...` : sentence;
}

function compactNoteText(notes) {
  const cleaned = stripMarkup(notes).toLowerCase();
  if (cleaned && /(return|remittance|payment|check|payable)/.test(cleaned)) {
    return 'Please return the remittance stub with your payment. Make checks payable to Pappas & Co. Landscaping.';
  }
  return 'Please return the remittance stub with your payment. Make checks payable to Pappas & Co. Landscaping.';
}

function serviceDatesSummary(lineItems, fallbackDate) {
  const dates = orderLineItemsForMail(lineItems)
    .map((item) => serviceDateLabel(item, { hideFuel: true }))
    .filter(Boolean);
  const unique = Array.from(new Set(dates));
  if (!unique.length) return formatHumanDate(fallbackDate || '') || 'See services below';
  if (unique.length === 1) return unique[0];
  return `${unique[0]} through ${unique[unique.length - 1]}`;
}

function dueDateLabel(invoice) {
  return formatHumanDate(invoice.due_date || invoice.due_date_raw || '') || 'Upon receipt';
}

function accountLabel(invoice) {
  if (invoice.customer_number) return invoice.customer_number;
  if (invoice.customer_id) return String(invoice.customer_id);
  return String(invoice.id || '');
}

function lineItemAmount(item) {
  const directCandidates = [
    item.amount,
    item.line_total,
    item.total,
    item.extended_amount,
    item.extended_total,
    item.lineAmount,
    item.lineTotal,
  ];

  for (const candidate of directCandidates) {
    const numeric = parseNumericCandidate(candidate);
    if (numeric !== null) return numeric;
  }

  const quantity = parseNumericCandidate(item.quantity ?? 0);
  const rate = parseNumericCandidate(item.rate ?? item.unit_price ?? 0);
  if (Number.isFinite(quantity) && Number.isFinite(rate)) {
    return quantity * rate;
  }

  return 0;
}

function lineItemBaseAmount(item) {
  const directCandidates = [
    item.subtotal,
    item.base_amount,
    item.pre_tax_amount,
    item.extended_subtotal,
  ];

  for (const candidate of directCandidates) {
    const numeric = parseNumericCandidate(candidate);
    if (numeric !== null) return numeric;
  }

  const quantity = parseNumericCandidate(item.quantity ?? 0);
  const rate = parseNumericCandidate(item.rate ?? item.unit_price ?? 0);
  if (quantity !== null && rate !== null) {
    return quantity * rate;
  }

  return lineItemAmount(item);
}

function lineItemTaxAmount(item) {
  const explicitCandidates = [
    item.tax_amount,
    item.tax,
  ];

  for (const candidate of explicitCandidates) {
    const numeric = parseNumericCandidate(candidate);
    if (numeric !== null) return numeric;
  }

  const baseAmount = lineItemBaseAmount(item);
  const taxPercent = parseNumericCandidate(item.tax_percent ?? item.taxPercent);
  if (taxPercent !== null && baseAmount !== null) {
    return baseAmount * (taxPercent / 100);
  }

  const lineTotal = lineItemAmount(item);
  if (baseAmount !== null && lineTotal !== null) {
    return Math.max(0, lineTotal - baseAmount);
  }

  return 0;
}

function deriveInvoiceFinancials(invoice) {
  const items = orderLineItemsForMail(invoice?.line_items);
  const rowSubtotal = parseNumericCandidate(invoice?.subtotal);
  const rowTax = parseNumericCandidate(invoice?.tax_amount);
  const rowTotal = parseNumericCandidate(invoice?.total);

  const lineSubtotal = roundMoney(items.reduce((sum, item) => sum + lineItemBaseAmount(item), 0));
  const lineTax = roundMoney(items.reduce((sum, item) => sum + lineItemTaxAmount(item), 0));
  const lineTotal = roundMoney(items.reduce((sum, item) => sum + lineItemAmount(item), 0));

  let subtotal = rowSubtotal ?? 0;
  let tax = rowTax ?? 0;
  let total = rowTotal ?? roundMoney(subtotal + tax);

  if (items.length) {
    const rowMatchesLines = rowTotal !== null && Math.abs(rowTotal - lineTotal) <= 0.009;
    if (!rowMatchesLines && lineTotal > 0) {
      subtotal = lineSubtotal;
      tax = lineTax;
      total = lineTotal;
    } else {
      if (rowSubtotal === null && lineSubtotal > 0) subtotal = lineSubtotal;
      if (rowTax === null && lineTax >= 0) tax = lineTax;
      if (rowTotal === null && lineTotal > 0) total = lineTotal;
    }
  }

  subtotal = roundMoney(subtotal);
  tax = roundMoney(tax);
  total = roundMoney(total);

  if (Math.abs((subtotal + tax) - total) > 0.009) {
    if (items.length && Math.abs((lineSubtotal + lineTax) - lineTotal) <= 0.009) {
      subtotal = lineSubtotal;
      tax = lineTax;
      total = lineTotal;
    } else {
      tax = roundMoney(total - subtotal);
    }
  }

  return { subtotal, tax, total };
}

function isMailDebugInvoice(invoice) {
  const candidates = [
    invoice?.invoice_number,
    invoice?.external_invoice_id,
    invoice?.id,
  ]
    .map((value) => String(value ?? '').trim())
    .filter(Boolean);

  return candidates.includes(MAIL_DEBUG_INVOICE_NUMBER);
}

function summarizeRenderLineItems(lineItems) {
  return orderLineItemsForMail(lineItems).map((item, index) => ({
    index,
    service: serviceTitle(item),
    service_date_raw: item?.service_date_raw ?? item?.service_date ?? item?.date ?? null,
    quantity: item?.quantity ?? null,
    rate: item?.rate ?? item?.unit_price ?? null,
    amount: item?.amount ?? null,
    line_total: item?.line_total ?? null,
    total: item?.total ?? null,
    computedAmount: lineItemAmount(item),
  }));
}

function logMailDebug(label, invoice) {
  if (!isMailDebugInvoice(invoice)) return;
  const financials = deriveInvoiceFinancials(invoice);
  console.log('[mail-debug 10273]', JSON.stringify({
    label,
    rendererFile: __filename,
    rowId: invoice?.id ?? null,
    invoiceNumber: invoice?.invoice_number ?? null,
    externalInvoiceId: invoice?.external_invoice_id ?? null,
    subtotal: financials.subtotal,
    tax: financials.tax,
    total: financials.total,
    lineItemCount: summarizeRenderLineItems(invoice?.line_items).length,
    lineItems: summarizeRenderLineItems(invoice?.line_items),
  }));
}

function renderServiceRows(lineItems) {
  const items = orderLineItemsForMail(lineItems);
  if (!items.length) {
    return '<tr><td colspan="6" class="empty-row">No billed services listed on this invoice.</td></tr>';
  }

  return items.map((item) => {
    const title = serviceTitle(item);
    const description = shortDescription(item);
    const date = serviceDateLabel(item, { hideFuel: true });
    const rowClass = isFuelSurcharge(item) ? 'fuel-row' : '';
    const amount = lineItemAmount(item);

    return `
      <tr class="${rowClass}">
        <td class="date-cell">${date ? escapeHtml(date) : ''}</td>
        <td class="service-col"><span class="service-name">${escapeHtml(title)}</span></td>
        <td class="description-col">${description ? `<span class="description">${escapeHtml(description)}</span>` : ''}</td>
        <td class="num">${escapeHtml(item.quantity ?? 0)}</td>
        <td class="num">${money(item.rate || 0)}</td>
        <td class="num amount">${money(amount)}</td>
      </tr>
    `;
  }).join('');
}

function renderInvoiceSheet(invoice, { logoDataUri }) {
  const summary = toSummary(invoice);
  const customerLines = [invoice.customer_name || '', ...splitAddressLines(invoice.customer_address)];
  const lineItems = orderLineItemsForMail(invoice.line_items);
  const noteText = compactNoteText(invoice.notes || '');
  const propertyLines = splitAddressLines(invoice.property_address || invoice.customer_address || '');

  return `
    <section class="page page-break">
      <div class="top-rule"></div>

      <div class="masthead">
        ${logoDataUri ? `<img src="${logoDataUri}" alt="${escapeHtml(COMPANY.name)}" class="logo">` : `<div class="wordmark">${escapeHtml(COMPANY.name)}</div>`}
        <div class="company">
          <strong>${escapeHtml(COMPANY.name)}</strong>
          ${COMPANY.remitLines.map((line) => `${escapeHtml(line)}<br>`).join('')}
        </div>
      </div>

      <div class="invoice-head">
        <div>
          <h1>Invoice</h1>
          <p class="subtitle">Completed services billed for the current route cycle.</p>
          <div class="service-period">Service dates: ${escapeHtml(serviceDatesSummary(lineItems, invoice.invoice_date_raw))}</div>
        </div>

        <div class="summary-panel">
          <table class="summary" aria-label="Invoice summary">
            <tr><td>Invoice #</td><td>${escapeHtml(invoice.invoice_number || '')}</td></tr>
            <tr><td>Invoice Date</td><td>${escapeHtml(invoice.invoice_date_raw || '')}</td></tr>
            <tr><td>Total Due</td><td>${money(summary.totalDueOnAccount)}</td></tr>
            <tr class="total"><td>Due Date</td><td>${escapeHtml(dueDateLabel(invoice))}</td></tr>
          </table>
        </div>
      </div>

      <div class="address-band">
        <div class="address-block">
          <div class="eyebrow">Mail To</div>
          ${customerLines.map((line, index) => index === 0
            ? `<strong>${escapeHtml(line)}</strong>`
            : `<div>${escapeHtml(line)}</div>`).join('')}
        </div>

        <div class="address-block">
          <div class="eyebrow">Service Property</div>
          ${propertyLines.length
            ? propertyLines.map((line, index) => index === 0
              ? `<strong>${escapeHtml(line)}</strong>`
              : `<div>${escapeHtml(line)}</div>`).join('')
            : '<strong>See mailing address</strong>'}
        </div>

        <div class="address-meta">
          <div><strong>Account</strong> ${escapeHtml(accountLabel(invoice))}</div>
          <div><strong>Terms</strong> ${escapeHtml(dueDateLabel(invoice) === 'Upon receipt' ? 'Due upon receipt' : dueDateLabel(invoice))}</div>
          <div><strong>Remit To</strong> PO Box 770057, Lakewood, OH 44107</div>
        </div>
      </div>

      <div class="section-title">Services Performed</div>

      <table class="services" aria-label="Services Performed">
        <thead>
          <tr>
            <th>Date</th>
            <th>Service</th>
            <th>Description</th>
            <th class="num">Qty</th>
            <th class="num">Rate</th>
            <th class="num">Amount</th>
          </tr>
        </thead>
        <tbody>
          ${renderServiceRows(lineItems)}
        </tbody>
      </table>

      <div class="bottom">
        <div class="payment-note">
          <strong>Payment note:</strong> ${escapeHtml(noteText)}
        </div>

        <div class="totals-box">
          <div class="totals-row"><span>Subtotal</span><span>${money(summary.subtotal)}</span></div>
          <div class="totals-row"><span>Taxes</span><span>${money(summary.tax)}</span></div>
          ${summary.priorBalance > 0 ? `<div class="totals-row"><span>Prior Balance</span><span>${money(summary.priorBalance)}</span></div>` : ''}
          <div class="totals-row total"><span>Total Due</span><span>${money(summary.totalDueOnAccount)}</span></div>
        </div>
      </div>

      <div class="footer-line">
        <span>Make checks payable to ${escapeHtml(COMPANY.name)}</span>
        <span>Please include ${escapeHtml(invoice.invoice_number || '')} on your payment</span>
      </div>
    </section>
  `;
}

function renderStubSheet(invoice, { logoDataUri }) {
  const summary = toSummary(invoice);
  const customerLines = [invoice.customer_name || '', ...splitAddressLines(invoice.customer_address)];
  const remitLines = [COMPANY.name, ...COMPANY.remitLines.slice(0, 2)];

  return `
    <section class="page stub-page">
      <div class="detach-note">Detach and return with payment</div>
      <div class="detach-rule" aria-hidden="true"></div>

      <div class="stub">
        <div class="stub-head">
          ${logoDataUri ? `<img src="${logoDataUri}" alt="${escapeHtml(COMPANY.name)}">` : `<div class="wordmark small">${escapeHtml(COMPANY.name)}</div>`}
          <div class="stub-head-right">
            <strong>Remittance Stub</strong>
            <div>Invoice # ${escapeHtml(invoice.invoice_number || '')}</div>
            <div>Return with payment</div>
          </div>
        </div>

        <div class="stub-window-grid">
          <div class="window-stack">
            <div class="window-slot">
              <div class="window-address">
                ${remitLines.map((line, index) => index === 0
                  ? `<strong>${escapeHtml(line)}</strong>`
                  : `<div>${escapeHtml(line)}</div>`).join('')}
              </div>
            </div>

            <div class="window-slot">
              <div class="window-address">
                ${customerLines.map((line, index) => index === 0
                  ? `<strong>${escapeHtml(line)}</strong>`
                  : `<div>${escapeHtml(line)}</div>`).join('')}
              </div>
            </div>
          </div>

          <div class="stub-summary-card">
            <div class="stub-summary-row"><span>Amount Due</span><strong>${money(summary.totalDueOnAccount)}</strong></div>
            <div class="stub-summary-row"><span>Due Date</span><strong>${escapeHtml(dueDateLabel(invoice))}</strong></div>
            <div class="stub-summary-row"><span>Account</span><strong>${escapeHtml(accountLabel(invoice))}</strong></div>
            <div class="stub-summary-note">Please include invoice ${escapeHtml(invoice.invoice_number || '')} with your payment.</div>
          </div>
        </div>

        <div class="stub-bottom">
          <div class="amount-line">
            <label>Amount Enclosed</label>
            <div class="write-line"><span></span><span></span></div>
            <div class="check-line">Check # ____________</div>
          </div>

          <div class="amount-due">
            <div class="label">Invoice Reference</div>
            <div class="value ref">${escapeHtml(invoice.invoice_number || '')}</div>
          </div>
        </div>
      </div>

      <div class="blank-space" aria-hidden="true"></div>
    </section>
  `;
}

function renderPrintDocument(bodyHtml) {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mail Invoice Packet</title>
  <style>
    :root {
      --ink: #202a28;
      --muted: #61706b;
      --line: #c6cfc9;
      --line-strong: #9ba9a2;
      --sage: #d3ddc9;
      --paper: #ffffff;
      --shell: #e2e7e1;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      padding: 0;
      background: var(--shell);
      color: var(--ink);
      font-family: "Avenir Next", "Helvetica Neue", Helvetica, Arial, sans-serif;
    }
    .print-bar {
      position: sticky;
      top: 0;
      z-index: 20;
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: center;
      padding: 14px 16px;
      background: rgba(226, 231, 225, 0.96);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(8px);
    }
    .print-bar button {
      appearance: none;
      border: 0;
      background: #cfd9c8;
      color: var(--ink);
      font: 700 14px/1 "Avenir Next", "Helvetica Neue", Helvetica, Arial, sans-serif;
      padding: 12px 18px;
      border-radius: 999px;
      cursor: pointer;
    }
    .print-bar span {
      font: 600 13px/1.4 "Avenir Next", "Helvetica Neue", Helvetica, Arial, sans-serif;
      color: var(--muted);
    }
    .document { padding: 28px 0 40px; }
    .page {
      width: 8.5in;
      min-height: 11in;
      margin: 0 auto 22px;
      background: var(--paper);
      padding: 0.5in 0.54in;
      box-shadow: 0 18px 48px rgba(37, 46, 53, 0.12);
      position: relative;
      page-break-after: always;
    }
    .page:last-child { page-break-after: auto; }
    .top-rule {
      height: 0.11in;
      margin: -0.5in -0.54in 0.26in;
      background: linear-gradient(90deg, var(--sage) 0%, #e8ede6 55%, #ffffff 100%);
    }
    .masthead {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 0.34in;
      padding-bottom: 0.16in;
      border-bottom: 1px solid var(--line-strong);
    }
    .logo {
      width: 3.18in;
      height: auto;
      display: block;
    }
    .wordmark {
      font-size: 22px;
      font-weight: 700;
      letter-spacing: 0.01em;
    }
    .wordmark.small { font-size: 18px; }
    .company {
      text-align: right;
      font-size: 10pt;
      line-height: 1.45;
      color: var(--muted);
      padding-top: 0.03in;
    }
    .company strong {
      display: block;
      color: var(--ink);
      font-size: 10.8pt;
      margin-bottom: 0.02in;
    }
    .invoice-head {
      display: grid;
      grid-template-columns: 1fr 2.3in;
      gap: 0.42in;
      align-items: end;
      margin-top: 0.22in;
      padding-bottom: 0.16in;
      border-bottom: 1px solid var(--line);
    }
    h1 {
      margin: 0;
      font-size: 25pt;
      line-height: 1;
      font-weight: 700;
      letter-spacing: 0.01em;
    }
    .subtitle {
      margin: 0.06in 0 0;
      font-size: 10.2pt;
      line-height: 1.45;
      color: var(--muted);
    }
    .service-period {
      margin-top: 0.11in;
      font-size: 9.2pt;
      color: var(--muted);
      letter-spacing: 0.02em;
    }
    .summary-panel {
      border: 1px solid var(--line);
      padding: 0.12in 0.14in;
      background: #fff;
    }
    .summary {
      width: 100%;
      border-collapse: collapse;
      font-size: 10pt;
    }
    .summary td {
      padding: 0.05in 0;
      vertical-align: baseline;
    }
    .summary td:first-child {
      color: var(--muted);
      width: 1.02in;
      padding-right: 0.18in;
    }
    .summary tr.total td {
      padding-top: 0.1in;
      border-top: 1px solid var(--line);
    }
    .summary tr.total td:last-child {
      font-size: 11pt;
      font-weight: 700;
      color: var(--ink);
      letter-spacing: 0;
    }
    .address-band {
      display: grid;
      grid-template-columns: 1.08fr 1.08fr 1fr;
      gap: 0.32in;
      margin-top: 0.74in;
      padding: 0.14in 0;
      border-top: 1px solid var(--line);
      border-bottom: 1px solid var(--line);
    }
    .address-block {
      min-height: 1.25in;
    }
    .eyebrow {
      margin-bottom: 0.06in;
      font-size: 8.4pt;
      letter-spacing: 0.09em;
      text-transform: uppercase;
      color: #7d8c84;
      font-weight: 700;
    }
    .address-block strong,
    .stub-panel strong {
      display: block;
      margin-bottom: 0.02in;
      font-size: 10.8pt;
      color: var(--ink);
    }
    .address-block div,
    .stub-panel div {
      font-size: 10pt;
      line-height: 1.43;
      color: var(--ink);
    }
    .address-meta {
      padding-left: 0.18in;
      border-left: 1px solid var(--line);
      font-size: 9.8pt;
      line-height: 1.55;
      color: var(--muted);
    }
    .address-meta strong {
      color: var(--ink);
      font-weight: 700;
    }
    .section-title {
      margin: 0.28in 0 0.12in;
      font-size: 11pt;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--ink);
    }
    .services {
      width: 100%;
      border-collapse: collapse;
      font-size: 9.65pt;
    }
    .services th {
      text-align: left;
      padding: 0.08in 0.08in 0.09in;
      font-size: 8.3pt;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      border-top: 1px solid var(--line-strong);
      border-bottom: 1px solid var(--line-strong);
      font-weight: 700;
    }
    .services td {
      padding: 0.1in 0.08in;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      font-size: 9.65pt;
    }
    .date-cell {
      width: 0.96in;
      white-space: nowrap;
      color: var(--muted);
    }
    .service-col { width: 1.52in; }
    .description-col { color: var(--muted); }
    .services .num,
    .services th.num {
      text-align: right;
      white-space: nowrap;
    }
    .service-name {
      font-weight: 700;
      color: var(--ink);
    }
    .description { color: var(--muted); }
    .fuel-row td {
      color: var(--muted);
      padding-top: 0.085in;
      padding-bottom: 0.085in;
    }
    .fuel-row .service-name {
      font-weight: 600;
      color: #52615b;
    }
    .amount {
      font-weight: 700;
      color: var(--ink);
    }
    .empty-row {
      text-align: center;
      color: var(--muted);
      font-size: 13px;
      padding: 22px 14px;
    }
    .bottom {
      display: grid;
      grid-template-columns: 1fr 2.28in;
      gap: 0.42in;
      align-items: start;
      margin-top: 0.26in;
    }
    .payment-note {
      font-size: 10pt;
      line-height: 1.48;
      color: var(--muted);
      max-width: 4.7in;
      padding-top: 0.04in;
    }
    .payment-note strong { color: var(--ink); }
    .totals-box {
      border: 1px solid var(--line-strong);
      padding: 0.12in 0.16in;
      background: #fff;
    }
    .totals-row {
      display: flex;
      justify-content: space-between;
      gap: 0.18in;
      padding: 0.045in 0;
      font-size: 10pt;
    }
    .totals-row.total {
      margin-top: 0.06in;
      padding-top: 0.1in;
      border-top: 1px solid var(--line);
      font-size: 12pt;
      font-weight: 700;
    }
    .footer-line {
      display: flex;
      justify-content: space-between;
      gap: 0.25in;
      margin-top: 0.24in;
      padding-top: 0.12in;
      border-top: 1px solid var(--line);
      font-size: 9pt;
      color: var(--muted);
    }
    .stub-page {
      padding-top: 0.42in;
    }
    .detach-note {
      margin-bottom: 0.14in;
      font-size: 8.2pt;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #7d8c84;
      font-weight: 700;
    }
    .detach-rule {
      border-top: 1px dashed var(--line-strong);
      margin-bottom: 0.18in;
    }
    .stub {
      width: 100%;
      max-width: 7.1in;
      border: 1px solid var(--line-strong);
      padding: 0.18in 0.2in 0.18in;
      background: #fff;
    }
    .stub-head {
      display: grid;
      grid-template-columns: 1fr 2.1in;
      gap: 0.24in;
      align-items: start;
      padding-bottom: 0.11in;
      border-bottom: 1px solid var(--line);
    }
    .stub-head img {
      width: 2.28in;
      height: auto;
      display: block;
    }
    .stub-head-right {
      text-align: right;
    }
    .stub-head-right strong {
      display: block;
      font-size: 14.5pt;
      color: var(--ink);
      margin-bottom: 0.02in;
    }
    .stub-head-right div {
      font-size: 9.4pt;
      line-height: 1.4;
      color: var(--muted);
    }
    .stub-window-grid {
      display: grid;
      grid-template-columns: 3.92in 1fr;
      gap: 0.24in;
      margin-top: 0.14in;
      align-items: start;
    }
    .window-stack {
      display: grid;
      gap: 0.18in;
    }
    .window-slot {
      border: 1px solid var(--line);
      min-height: 0.98in;
      padding: 0.08in 0.14in 0.08in 0.16in;
      background: #fff;
    }
    .window-address strong {
      display: block;
      margin-bottom: 0.02in;
      font-size: 10.4pt;
      color: var(--ink);
    }
    .window-address div {
      font-size: 9.8pt;
      line-height: 1.3;
      color: var(--ink);
    }
    .stub-summary-card {
      border: 1px solid var(--line);
      padding: 0.12in 0.14in;
      min-height: 2.14in;
      background: #fff;
    }
    .stub-summary-row {
      display: flex;
      justify-content: space-between;
      gap: 0.12in;
      align-items: baseline;
      padding: 0.05in 0;
      border-bottom: 1px solid var(--line);
      font-size: 9.5pt;
      color: var(--muted);
    }
    .stub-summary-row:last-of-type {
      border-bottom: 0;
    }
    .stub-summary-row strong {
      font-size: 10.8pt;
      color: var(--ink);
    }
    .stub-summary-note {
      margin-top: 0.12in;
      font-size: 8.8pt;
      line-height: 1.4;
      color: var(--muted);
    }
    .stub-bottom {
      display: grid;
      grid-template-columns: 1fr 1.62in;
      gap: 0.22in;
      margin-top: 0.14in;
      align-items: stretch;
    }
    .amount-line,
    .amount-due {
      border: 1px solid var(--line);
      padding: 0.11in 0.13in;
      background: #fff;
    }
    .amount-line {
      display: flex;
      flex-direction: column;
      justify-content: center;
      gap: 0.08in;
    }
    .amount-line label,
    .amount-due .label {
      font-size: 8.4pt;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      font-weight: 700;
    }
    .write-line {
      border-bottom: 1px solid var(--ink);
      height: 0.28in;
    }
    .check-line {
      font-size: 8.9pt;
      color: var(--muted);
    }
    .amount-due {
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: flex-end;
      text-align: right;
    }
    .amount-due .value {
      margin-top: 0.04in;
      font-size: 16pt;
      font-weight: 700;
      color: var(--ink);
      letter-spacing: 0.01em;
    }
    .amount-due .value.ref {
      font-size: 14pt;
      letter-spacing: 0.03em;
    }
    .blank-space {
      min-height: 7.25in;
    }
    @media print {
      html, body { background: #fff; }
      .print-bar { display: none; }
      .document { padding: 0; }
      .page { margin: 0; box-shadow: none; }
    }
    @page {
      size: letter;
      margin: 0;
    }
  </style>
</head>
<body>
  <!-- mail-renderer: classic / lib/invoice-mail-html.js -->
  <div class="print-bar">
    <button onclick="window.print()">Print Packet</button>
    <span>Use 100% scale on plain letter paper for mailed invoice inserts.</span>
  </div>
  <main class="document">
    ${bodyHtml}
  </main>
</body>
</html>`;
}

function renderMailInvoiceHtml(invoice) {
  logMailDebug('renderMailInvoiceHtml', invoice);
  const logoDataUri = readLogoDataUri();
  return renderPrintDocument([
    renderInvoiceSheet(invoice, { logoDataUri }),
    renderStubSheet(invoice, { logoDataUri }),
  ].join(''));
}

function renderMailBatchHtml(invoices) {
  (Array.isArray(invoices) ? invoices : []).forEach((invoice) => logMailDebug('renderMailBatchHtml:item', invoice));
  const logoDataUri = readLogoDataUri();
  const bodyHtml = invoices.map((invoice) => [
    renderInvoiceSheet(invoice, { logoDataUri }),
    renderStubSheet(invoice, { logoDataUri }),
  ].join('')).join('');

  return renderPrintDocument(bodyHtml);
}

module.exports = {
  renderMailInvoiceHtml,
  renderMailBatchHtml,
};
