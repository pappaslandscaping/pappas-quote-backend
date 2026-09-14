const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');

const PAGE_WIDTH = 612;
const PAGE_HEIGHT = 792;
const MARGIN = 42;
const CONTENT_WIDTH = PAGE_WIDTH - (MARGIN * 2);

const COLORS = {
  ink: rgb(0.12, 0.17, 0.16),
  darkGreen: rgb(0.12, 0.20, 0.18),
  green: rgb(0.18, 0.43, 0.30),
  lime: rgb(0.79, 0.87, 0.50),
  muted: rgb(0.39, 0.45, 0.43),
  line: rgb(0.86, 0.89, 0.86),
  pale: rgb(0.98, 0.98, 0.98),
  amber: rgb(0.72, 0.39, 0.08),
  red: rgb(0.66, 0.17, 0.16),
  white: rgb(1, 1, 1),
};

function money(value) {
  const amount = Number(value);
  return `$${(Number.isFinite(amount) ? amount : 0).toFixed(2)}`;
}

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function safeText(value) {
  return String(value == null ? '' : value)
    .replace(/[\u2010-\u2015]/g, '-')
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201c\u201d]/g, '"')
    .replace(/[^\x20-\x7E]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function formatDate(value, fallback = '') {
  if (!value) return fallback;
  const raw = String(value);
  const parsed = /^\d{4}-\d{2}-\d{2}$/.test(raw)
    ? new Date(`${raw}T12:00:00Z`)
    : new Date(raw);
  if (Number.isNaN(parsed.getTime())) return safeText(value);
  return parsed.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
  });
}

function parseJson(value) {
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_error) { return {}; }
}

function parseLineItems(value) {
  const parsed = Array.isArray(value) ? value : parseJson(value);
  return Array.isArray(parsed) ? parsed : [];
}

function invoiceBalance(invoice) {
  const metadata = parseJson(invoice.external_metadata || invoice.metadata);
  const explicit = invoice.total_due ?? metadata.total_due;
  if (explicit != null && Number.isFinite(Number(explicit))) return Math.max(0, number(explicit));
  return Math.max(0, number(invoice.total) - number(invoice.amount_paid));
}

function invoiceFees(invoice) {
  const total = number(invoice.total);
  const subtotal = number(invoice.subtotal);
  const tax = number(invoice.tax_amount);
  return Math.max(0, total - subtotal - tax);
}

function invoiceDescription(invoice) {
  const items = parseLineItems(invoice.line_items);
  const descriptions = [...new Set(items.map((item) => safeText(item.description || item.name)).filter(Boolean))];
  if (descriptions.length) return descriptions.slice(0, 3).join(', ');
  const metadata = parseJson(invoice.external_metadata || invoice.metadata);
  return safeText(metadata.property_address || invoice.customer_address || 'Services');
}

function paymentReference(payment) {
  const joined = [payment.method, payment.notes, payment.details].map(safeText).filter(Boolean).join(' ');
  const check = joined.match(/check\s*#\s*([a-z0-9-]+)/i)
    || joined.match(/\bcheck\s+(\d[a-z0-9-]*)/i);
  if (check) return `Check #${check[1]}`;
  return safeText(payment.method || 'Payment');
}

function paymentInvoice(payment) {
  if (payment.invoice_number) return `Invoice #${safeText(payment.invoice_number)}`;
  const metadata = parseJson(payment.external_metadata || payment.metadata);
  const extracted = metadata.extracted_invoice_number;
  if (extracted) return `Invoice #${safeText(extracted)}`;
  const detail = safeText(payment.details || metadata.raw_details);
  const matches = [...detail.matchAll(/invoice\s*#\s*([a-z0-9-]+)/gi)].map((match) => match[1]);
  const unique = [...new Set(matches)];
  if (unique.length > 1) return `Invoices ${unique.map((value) => `#${value}`).join(', ')}`;
  if (unique.length === 1) return `Invoice #${unique[0]}`;
  if (/credit/i.test(detail)) return 'Account credit';
  return 'Account';
}

function wrapText(text, font, size, maxWidth) {
  const words = safeText(text).split(' ').filter(Boolean);
  const lines = [];
  let current = '';
  for (const word of words) {
    const candidate = current ? `${current} ${word}` : word;
    if (current && font.widthOfTextAtSize(candidate, size) > maxWidth) {
      lines.push(current);
      current = word;
    } else {
      current = candidate;
    }
  }
  if (current) lines.push(current);
  return lines.length ? lines : [''];
}

function rightText(page, text, rightX, y, options) {
  const width = options.font.widthOfTextAtSize(text, options.size);
  page.drawText(text, { ...options, x: rightX - width, y });
}

async function createContext() {
  const pdfDoc = await PDFDocument.create();
  const regular = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const bold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const display = bold;
  let logo = null;
  const logoPath = path.join(__dirname, '..', 'public', 'logo.png');
  if (fs.existsSync(logoPath)) {
    try { logo = await pdfDoc.embedPng(fs.readFileSync(logoPath)); } catch (_error) { logo = null; }
  }
  return { pdfDoc, regular, bold, display, logo };
}

function drawHeader(page, ctx, continuation = false) {
  const { regular, bold, display, logo } = ctx;
  let y = PAGE_HEIGHT - MARGIN;
  if (logo) {
    const dims = logo.scaleToFit(140, 70);
    page.drawImage(logo, { x: MARGIN, y: y - dims.height + 2, width: dims.width, height: dims.height });
  } else {
    page.drawText('Pappas & Co. Landscaping', { x: MARGIN, y: y - 10, size: 17, font: display, color: COLORS.darkGreen });
  }
  rightText(page, 'pappaslandscaping.com', PAGE_WIDTH - MARGIN, y, { size: 8.5, font: regular, color: COLORS.muted });
  rightText(page, 'hello@pappaslandscaping.com', PAGE_WIDTH - MARGIN, y - 12, { size: 8.5, font: regular, color: COLORS.muted });
  rightText(page, '(440) 886-7318', PAGE_WIDTH - MARGIN, y - 24, { size: 8.5, font: regular, color: COLORS.muted });
  y -= 82;
  page.drawRectangle({ x: MARGIN, y, width: CONTENT_WIDTH, height: 4, color: COLORS.lime });
  y -= 28;
  page.drawText(continuation ? 'ACCOUNT STATEMENT - CONTINUED' : 'ACCOUNT STATEMENT', {
    x: MARGIN,
    y,
    size: 16,
    font: bold,
    color: COLORS.darkGreen,
  });
  return y - 25;
}

function drawFooter(page, ctx, pageNumber, pageCount, sourceAsOf) {
  const { regular, bold } = ctx;
  page.drawRectangle({ x: MARGIN, y: 47, width: CONTENT_WIDTH, height: 2, color: COLORS.lime });
  page.drawText('Pappas & Co. Landscaping | PO Box 770057, Lakewood, OH 44107', {
    x: MARGIN,
    y: 32,
    size: 7.5,
    font: bold,
    color: COLORS.muted,
  });
  page.drawText(`HomeWorks account data as of ${formatDate(sourceAsOf || new Date().toISOString())}`, {
    x: MARGIN,
    y: 20,
    size: 7,
    font: regular,
    color: COLORS.muted,
  });
  rightText(page, `Page ${pageNumber} of ${pageCount}`, PAGE_WIDTH - MARGIN, 20, {
    size: 7,
    font: regular,
    color: COLORS.muted,
  });
}

function drawCustomerBlock(page, ctx, y, customer, statementDate, dateRange) {
  const { regular, bold } = ctx;
  page.drawRectangle({ x: MARGIN, y: y - 82, width: CONTENT_WIDTH, height: 82, color: COLORS.pale });
  page.drawText('PREPARED FOR', { x: MARGIN + 14, y: y - 17, size: 7.5, font: bold, color: COLORS.muted });
  page.drawText(safeText(customer.name || [customer.first_name, customer.last_name].filter(Boolean).join(' ')), {
    x: MARGIN + 14,
    y: y - 34,
    size: 11,
    font: bold,
    color: COLORS.ink,
  });
  const address = [customer.street, customer.street2, [customer.city, customer.state, customer.postal_code].filter(Boolean).join(' ')].filter(Boolean);
  address.slice(0, 2).forEach((line, index) => page.drawText(safeText(line), {
    x: MARGIN + 14,
    y: y - 49 - (index * 11),
    size: 8.5,
    font: regular,
    color: COLORS.muted,
  }));

  const rx = MARGIN + 330;
  page.drawText('STATEMENT DATE', { x: rx, y: y - 17, size: 7.5, font: bold, color: COLORS.muted });
  page.drawText(formatDate(statementDate), { x: rx, y: y - 34, size: 9.5, font: bold, color: COLORS.ink });
  if (dateRange) {
    page.drawText('ACTIVITY PERIOD', { x: rx, y: y - 51, size: 7.5, font: bold, color: COLORS.muted });
    page.drawText(safeText(dateRange), { x: rx, y: y - 66, size: 8.5, font: regular, color: COLORS.ink });
  }
  return y - 102;
}

function drawSummary(page, ctx, y, totals) {
  const { regular, bold } = ctx;
  const gap = 8;
  const width = (CONTENT_WIDTH - (gap * 2)) / 3;
  const cards = [
    ['PAST DUE', money(totals.pastDue), totals.pastDue > 0 ? COLORS.red : COLORS.green],
    ['CURRENT', money(totals.current), COLORS.ink],
    ['BALANCE DUE', money(totals.balance), COLORS.darkGreen],
  ];
  cards.forEach(([label, value, color], index) => {
    const x = MARGIN + (index * (width + gap));
    const isBalance = index === 2;
    page.drawRectangle({
      x,
      y: y - 54,
      width,
      height: 54,
      color: isBalance ? COLORS.darkGreen : COLORS.pale,
      borderColor: isBalance ? COLORS.darkGreen : COLORS.line,
      borderWidth: 0.7,
    });
    page.drawText(label, { x: x + 12, y: y - 17, size: 7.5, font: bold, color: isBalance ? COLORS.white : COLORS.muted });
    page.drawText(value, { x: x + 12, y: y - 40, size: 15, font: bold, color: isBalance ? COLORS.white : color });
  });
  page.drawText('Your balance is the total of the open invoice balances shown below after all recorded payments and credits.', {
    x: MARGIN,
    y: y - 72,
    size: 8.2,
    font: regular,
    color: COLORS.muted,
  });
  return y - 91;
}

function drawSectionTitle(page, ctx, y, title, subtitle) {
  const { regular, bold } = ctx;
  page.drawText(title, { x: MARGIN, y, size: 10.5, font: bold, color: COLORS.darkGreen });
  if (subtitle) page.drawText(subtitle, { x: MARGIN, y: y - 13, size: 7.8, font: regular, color: COLORS.muted });
  return y - (subtitle ? 28 : 17);
}

function drawInvoiceHeader(page, ctx, y) {
  const { bold } = ctx;
  page.drawRectangle({ x: MARGIN, y: y - 19, width: CONTENT_WIDTH, height: 22, color: COLORS.darkGreen });
  const options = { y: y - 12, size: 7.5, font: bold, color: COLORS.white };
  page.drawText('INVOICE / DATE', { x: MARGIN + 9, ...options });
  page.drawText('CHARGES', { x: MARGIN + 300, ...options });
  page.drawText('PAID / CREDIT', { x: MARGIN + 365, ...options });
  rightText(page, 'BALANCE', PAGE_WIDTH - MARGIN - 9, options.y, options);
  return y - 25;
}

function drawInvoiceRow(page, ctx, y, invoice, statementDate) {
  const { regular, bold } = ctx;
  const total = number(invoice.total);
  const paid = number(invoice.amount_paid);
  const balance = invoiceBalance(invoice);
  const dueAt = invoice.due_date ? new Date(`${String(invoice.due_date).slice(0, 10)}T23:59:59Z`) : null;
  const statementAt = new Date(statementDate);
  const isPastDue = balance > 0 && dueAt && !Number.isNaN(dueAt.getTime()) && dueAt < statementAt;
  const fees = invoiceFees(invoice);
  const description = invoiceDescription(invoice);
  const dueDate = invoice.due_date ? `Due ${formatDate(invoice.due_date)}` : safeText(invoice.status || '');
  const descLines = wrapText(description, regular, 7.5, 245).slice(0, 2);
  const rowHeight = 45 + ((descLines.length - 1) * 9);

  page.drawRectangle({ x: MARGIN, y: y - rowHeight + 6, width: CONTENT_WIDTH, height: rowHeight, color: COLORS.white, borderColor: COLORS.line, borderWidth: 0.5 });
  page.drawText(`#${safeText(invoice.invoice_number || invoice.id)}`, { x: MARGIN + 9, y: y - 8, size: 9, font: bold, color: COLORS.ink });
  page.drawText(formatDate(invoice.created_at || invoice.invoice_date), { x: MARGIN + 70, y: y - 8, size: 8, font: regular, color: COLORS.ink });
  page.drawText(dueDate, { x: MARGIN + 160, y: y - 8, size: 7.5, font: regular, color: COLORS.muted });
  rightText(page, money(total), MARGIN + 348, y - 8, { size: 8.5, font: regular, color: COLORS.ink });
  rightText(page, money(paid), MARGIN + 438, y - 8, { size: 8.5, font: regular, color: COLORS.ink });
  rightText(page, money(balance), PAGE_WIDTH - MARGIN - 9, y - 8, { size: 9, font: bold, color: isPastDue ? COLORS.red : COLORS.ink });
  descLines.forEach((line, index) => page.drawText(line, { x: MARGIN + 9, y: y - 23 - (index * 9), size: 7.5, font: regular, color: COLORS.muted }));
  if (fees > 0) {
    page.drawText(`Includes ${money(fees)} in fees or adjustments`, { x: MARGIN + 300, y: y - 23, size: 7.2, font: regular, color: COLORS.muted });
  }
  return y - rowHeight;
}

function drawPaymentHeader(page, ctx, y) {
  const { bold } = ctx;
  page.drawRectangle({ x: MARGIN, y: y - 19, width: CONTENT_WIDTH, height: 22, color: COLORS.darkGreen });
  const options = { y: y - 12, size: 7.5, font: bold, color: COLORS.white };
  page.drawText('DATE', { x: MARGIN + 9, ...options });
  page.drawText('METHOD / REFERENCE', { x: MARGIN + 105, ...options });
  page.drawText('APPLIED TO', { x: MARGIN + 310, ...options });
  rightText(page, 'AMOUNT', PAGE_WIDTH - MARGIN - 9, options.y, options);
  return y - 25;
}

function drawPaymentRow(page, ctx, y, payment, shaded) {
  const { regular, bold } = ctx;
  if (shaded) page.drawRectangle({ x: MARGIN, y: y - 17, width: CONTENT_WIDTH, height: 22, color: COLORS.pale });
  page.drawText(formatDate(payment.paid_at || payment.created_at || payment.source_date_raw), { x: MARGIN + 9, y: y - 10, size: 8, font: regular, color: COLORS.ink });
  page.drawText(paymentReference(payment), { x: MARGIN + 105, y: y - 10, size: 8, font: regular, color: COLORS.ink });
  page.drawText(paymentInvoice(payment), { x: MARGIN + 310, y: y - 10, size: 8, font: regular, color: COLORS.muted });
  rightText(page, money(payment.amount), PAGE_WIDTH - MARGIN - 9, y - 10, { size: 8.5, font: bold, color: COLORS.ink });
  return y - 22;
}

async function renderStatementPdf({
  customer = {},
  invoices = [],
  payments = [],
  statementDate = new Date().toISOString(),
  dateRange = '',
  sourceAsOf = new Date().toISOString(),
} = {}) {
  const ctx = await createContext();
  const { pdfDoc, regular, bold } = ctx;
  const openInvoices = invoices
    .filter((invoice) => invoiceBalance(invoice) > 0.004)
    .sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0));
  const activity = [...payments].sort((a, b) => new Date(b.paid_at || b.created_at || 0) - new Date(a.paid_at || a.created_at || 0));
  const statementAt = new Date(statementDate);
  const totals = openInvoices.reduce((summary, invoice) => {
    const balance = invoiceBalance(invoice);
    const due = invoice.due_date ? new Date(invoice.due_date) : null;
    const status = safeText(invoice.status).toLowerCase();
    const isPastDue = status.includes('overdue') || status.includes('past due') || (due && !Number.isNaN(due.getTime()) && due < statementAt);
    summary.balance += balance;
    if (isPastDue) summary.pastDue += balance;
    else summary.current += balance;
    summary.openCharges += number(invoice.total);
    summary.applied += number(invoice.amount_paid);
    return summary;
  }, { balance: 0, pastDue: 0, current: 0, openCharges: 0, applied: 0 });

  const pages = [];
  let page = pdfDoc.addPage([PAGE_WIDTH, PAGE_HEIGHT]);
  pages.push(page);
  let y = drawHeader(page, ctx);
  y = drawCustomerBlock(page, ctx, y, customer, statementDate, dateRange);
  y = drawSummary(page, ctx, y, totals);

  const newPage = () => {
    page = pdfDoc.addPage([PAGE_WIDTH, PAGE_HEIGHT]);
    pages.push(page);
    y = drawHeader(page, ctx, true);
  };

  y = drawSectionTitle(page, ctx, y, 'OPEN INVOICE DETAIL', 'Each row shows the original charge, payments or credits applied, and the amount still due.');
  y = drawInvoiceHeader(page, ctx, y);
  if (!openInvoices.length) {
    page.drawText('No open invoices as of this statement date.', { x: MARGIN + 9, y: y - 12, size: 9, font: regular, color: COLORS.green });
    y -= 34;
  } else {
    for (const invoice of openInvoices) {
      if (y < 112) { newPage(); y = drawSectionTitle(page, ctx, y, 'OPEN INVOICE DETAIL', 'Continued'); y = drawInvoiceHeader(page, ctx, y); }
      y = drawInvoiceRow(page, ctx, y, invoice, statementDate);
    }
  }

  if (y < 190) newPage();
  else y -= 16;
  y = drawSectionTitle(page, ctx, y, 'BALANCE CALCULATION', 'A simple reconciliation of the open invoices above.');
  page.drawRectangle({ x: MARGIN, y: y - 88, width: CONTENT_WIDTH, height: 92, color: COLORS.pale, borderColor: COLORS.line, borderWidth: 0.8 });
  page.drawRectangle({ x: MARGIN, y: y - 54, width: CONTENT_WIDTH, height: 29, color: COLORS.white });
  page.drawRectangle({ x: MARGIN, y: y - 88, width: CONTENT_WIDTH, height: 34, color: COLORS.darkGreen });
  const calcY = y - 15;
  page.drawText('Open invoice charges', { x: MARGIN + 14, y: calcY, size: 8.5, font: regular, color: COLORS.ink });
  rightText(page, money(totals.openCharges), PAGE_WIDTH - MARGIN - 14, calcY, { size: 8.5, font: bold, color: COLORS.ink });
  page.drawText('Payments and credits applied', { x: MARGIN + 14, y: calcY - 29, size: 8.5, font: regular, color: COLORS.ink });
  rightText(page, `-${money(totals.applied)}`, PAGE_WIDTH - MARGIN - 14, calcY - 29, { size: 8.5, font: bold, color: COLORS.ink });
  page.drawText('Balance due', { x: MARGIN + 14, y: calcY - 62, size: 9.5, font: bold, color: COLORS.white });
  rightText(page, money(totals.balance), PAGE_WIDTH - MARGIN - 14, calcY - 62, { size: 10.5, font: bold, color: COLORS.white });
  y -= 110;

  if (activity.length) {
    if (y < 145) newPage();
    y = drawSectionTitle(page, ctx, y, 'RECENT PAYMENT ACTIVITY', 'Payments and account credits recorded during the selected activity period.');
    y = drawPaymentHeader(page, ctx, y);
    for (let index = 0; index < activity.length; index += 1) {
      if (y < 88) { newPage(); y = drawSectionTitle(page, ctx, y, 'RECENT PAYMENT ACTIVITY', 'Continued'); y = drawPaymentHeader(page, ctx, y); }
      y = drawPaymentRow(page, ctx, y, activity[index], index % 2 === 0);
    }
  }

  pages.forEach((currentPage, index) => drawFooter(currentPage, ctx, index + 1, pages.length, sourceAsOf));
  const bytes = await pdfDoc.save();
  return { bytes, type: 'complete', summary: totals };
}

module.exports = {
  renderStatementPdf,
  _internal: {
    formatDate,
    invoiceBalance,
    invoiceFees,
    paymentInvoice,
    paymentReference,
    safeText,
  },
};
