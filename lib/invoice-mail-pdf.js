const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');

const PAGE_WIDTH = 612;
const PAGE_HEIGHT = 792;
const MARGIN = 42;
const CONTENT_WIDTH = PAGE_WIDTH - (MARGIN * 2);

const COLORS = {
  brand: rgb(0.14, 0.18, 0.21),
  brandText: rgb(0.2, 0.28, 0.27),
  body: rgb(0.27, 0.36, 0.45),
  muted: rgb(0.48, 0.56, 0.53),
  sage: rgb(0.78, 0.87, 0.5),
  sageSoft: rgb(0.96, 0.97, 0.93),
  shell: rgb(0.985, 0.985, 0.975),
  panel: rgb(0.985, 0.99, 0.985),
  border: rgb(0.86, 0.89, 0.85),
  white: rgb(1, 1, 1),
  black: rgb(0.08, 0.1, 0.12),
};

const COMPANY = {
  name: 'Pappas & Co. Landscaping',
  phone: '(440) 886-7318',
  email: 'hello@pappaslandscaping.com',
  website: 'www.pappaslandscaping.com',
  remit_lines: [
    'PO Box 770057',
    'Lakewood, OH 44107',
    '(440) 886-7318',
    'hello@pappaslandscaping.com',
    'www.pappaslandscaping.com',
  ],
};

const LOGO_PATHS = [
  path.join(__dirname, '..', 'public', 'images', 'email-logo.png'),
  path.join(__dirname, '..', 'public', 'logo.png'),
  path.join(__dirname, '..', 'public', 'badge-logo-transparent.png'),
];

function money(value) {
  return `$${Number(value || 0).toFixed(2)}`;
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

function splitAddressLines(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, 4);
}

function toSummary(invoice) {
  const metadata = invoice.metadata && typeof invoice.metadata === 'object'
    ? invoice.metadata
    : {};
  const priorBalance = Number(metadata.outstanding_balance || 0);
  const thisInvoice = Number(metadata.this_invoice || invoice.total || 0);
  const totalDueOnAccount = Number(metadata.total_due_on_account || (priorBalance + thisInvoice));

  return {
    priorBalance,
    thisInvoice,
    totalDueOnAccount,
  };
}

function wrapText(text, font, size, maxWidth) {
  const lines = [];
  const paragraphs = String(text || '').split('\n');

  paragraphs.forEach((paragraph, paragraphIndex) => {
    const words = paragraph.trim().split(/\s+/).filter(Boolean);
    if (!words.length) {
      lines.push('');
      return;
    }

    let current = words[0];
    for (let index = 1; index < words.length; index += 1) {
      const next = `${current} ${words[index]}`;
      if (font.widthOfTextAtSize(next, size) <= maxWidth) {
        current = next;
      } else {
        lines.push(current);
        current = words[index];
      }
    }
    lines.push(current);

    if (paragraphIndex < paragraphs.length - 1) lines.push('');
  });

  return lines;
}

function drawTextBlock(page, lines, x, y, { font, size, color, lineHeight }) {
  let cursorY = y;
  lines.forEach((line) => {
    page.drawText(line, { x, y: cursorY, font, size, color });
    cursorY -= lineHeight;
  });
  return cursorY;
}

function drawLabel(page, text, x, y, fonts) {
  page.drawText(String(text || '').toUpperCase(), {
    x,
    y,
    font: fonts.bold,
    size: 8.5,
    color: COLORS.muted,
  });
}

function drawKeyValue(page, label, value, x, y, width, fonts, options = {}) {
  const emphasize = options.emphasize === true;
  page.drawText(label, {
    x,
    y,
    font: fonts.regular,
    size: 9.5,
    color: COLORS.muted,
  });
  page.drawText(value, {
    x: x + width - fonts.bold.widthOfTextAtSize(value, emphasize ? 13.5 : 11.5),
    y: y - 1,
    font: fonts.bold,
    size: emphasize ? 13.5 : 11.5,
    color: emphasize ? COLORS.brandText : COLORS.black,
  });
}

function drawDivider(page, y) {
  page.drawLine({
    start: { x: MARGIN, y },
    end: { x: PAGE_WIDTH - MARGIN, y },
    thickness: 1,
    color: COLORS.border,
  });
}

function serviceTitle(item) {
  return String(item.name || item.description || 'Service').trim() || 'Service';
}

function serviceNarrative(item) {
  const title = serviceTitle(item);
  const detail = String(item.description || '').trim();
  if (!detail || detail === title) return '';
  return detail.replace(/\s+(?=[A-Z][A-Za-z/& ]{2,32}: )/g, '\n');
}

function serviceDetailLines(item, fonts, width) {
  const narrative = serviceNarrative(item);
  if (!narrative) return [];
  const paragraphs = narrative
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  return paragraphs.flatMap((paragraph, index) => {
    const wrapped = wrapText(paragraph, fonts.regular, 9.2, width);
    if (index === paragraphs.length - 1) return wrapped;
    return [...wrapped, ''];
  });
}

function serviceCardHeight(item, fonts) {
  const detailLines = serviceDetailLines(item, fonts, 300).filter((line) => line !== '');
  const linesHeight = detailLines.length ? (detailLines.length * 14) + 26 : 0;
  return 58 + linesHeight;
}

async function loadLogoImage(pdfDoc) {
  for (const logoPath of LOGO_PATHS) {
    try {
      if (!fs.existsSync(logoPath)) continue;
      const bytes = fs.readFileSync(logoPath);
      if (logoPath.endsWith('.png')) return pdfDoc.embedPng(bytes);
      return pdfDoc.embedJpg(bytes);
    } catch (_error) {
      continue;
    }
  }
  return null;
}

function drawLogo(page, logo, x, y, maxWidth) {
  if (!logo) return 0;
  const scale = maxWidth / logo.width;
  const width = maxWidth;
  const height = logo.height * scale;
  page.drawImage(logo, { x, y, width, height });
  return height;
}

function drawHeader(page, fonts, logo, continuation = null) {
  page.drawRectangle({
    x: MARGIN,
    y: PAGE_HEIGHT - 96,
    width: CONTENT_WIDTH,
    height: 68,
    color: COLORS.brand,
  });

  if (logo) {
    drawLogo(page, logo, MARGIN + 20, PAGE_HEIGHT - 78, 140);
  } else {
    page.drawText(COMPANY.name, {
      x: MARGIN + 20,
      y: PAGE_HEIGHT - 59,
      font: fonts.bold,
      size: 20,
      color: COLORS.white,
    });
  }

  if (continuation) {
    page.drawText(continuation, {
      x: PAGE_WIDTH - MARGIN - fonts.bold.widthOfTextAtSize(continuation, 11),
      y: PAGE_HEIGHT - 58,
      font: fonts.bold,
      size: 11,
      color: COLORS.sage,
    });
  }
}

function drawHero(page, invoice, fonts) {
  const summary = toSummary(invoice);
  const customerLines = [invoice.customer_name || '', ...splitAddressLines(invoice.customer_address)];

  page.drawRectangle({
    x: MARGIN,
    y: 554,
    width: CONTENT_WIDTH,
    height: 136,
    color: COLORS.white,
    borderColor: COLORS.border,
    borderWidth: 1,
  });

  page.drawRectangle({
    x: MARGIN + 18,
    y: 574,
    width: 258,
    height: 96,
    color: COLORS.sageSoft,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Mailing Address', MARGIN + 34, 648, fonts);
  drawTextBlock(page, customerLines.filter(Boolean), MARGIN + 34, 626, {
    font: fonts.bold,
    size: 11,
    color: COLORS.black,
    lineHeight: 13,
  });

  page.drawText('Invoice', {
    x: 342,
    y: 646,
    font: fonts.bold,
    size: 31,
    color: COLORS.brandText,
  });
  page.drawLine({
    start: { x: 342, y: 638 },
    end: { x: 440, y: 638 },
    thickness: 1,
    color: COLORS.border,
  });

  page.drawRectangle({
    x: 332,
    y: 584,
    width: 220,
    height: 84,
    color: COLORS.white,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawKeyValue(page, 'Invoice #', String(invoice.invoice_number || ''), 350, 635, 182, fonts);
  drawKeyValue(page, 'Invoice Date', invoice.invoice_date_raw || '', 350, 601, 182, fonts);
  drawKeyValue(page, 'Current Invoice', money(summary.thisInvoice), 350, 567, 182, fonts);
  drawKeyValue(page, 'Total Due', money(summary.totalDueOnAccount), 350, 533, 182, fonts, { emphasize: true });

  page.drawText('Account Summary', {
    x: MARGIN,
    y: 528,
    font: fonts.bold,
    size: 13,
    color: COLORS.brandText,
  });
  if (invoice.property_address) {
    page.drawText(`Service property: ${invoice.property_address}`, {
      x: 164,
      y: 528,
      font: fonts.regular,
      size: 8.8,
      color: COLORS.body,
    });
  }
  drawDivider(page, 516);
}

function drawServiceCard(page, item, yTop, fonts) {
  const title = serviceTitle(item);
  const detailLines = serviceDetailLines(item, fonts, 300);
  const narrativeLines = detailLines.filter(Boolean);
  const cardHeight = serviceCardHeight(item, fonts);
  const cardY = yTop - cardHeight;

  page.drawRectangle({
    x: MARGIN,
    y: cardY,
    width: CONTENT_WIDTH,
    height: cardHeight,
    color: COLORS.white,
    borderColor: COLORS.border,
    borderWidth: 1,
  });

  page.drawText(title, {
    x: MARGIN + 18,
    y: yTop - 24,
    font: fonts.bold,
    size: 12,
    color: COLORS.brandText,
  });
  if (item.service_date_raw) {
    page.drawText(item.service_date_raw, {
      x: MARGIN + 18,
      y: yTop - 10,
      font: fonts.regular,
      size: 8.2,
      color: COLORS.muted,
    });
  }

  const metricBoxX = PAGE_WIDTH - MARGIN - 148;
  page.drawRectangle({
    x: metricBoxX,
    y: cardY + 14,
    width: 130,
    height: 52,
    color: COLORS.sageSoft,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawKeyValue(page, 'Qty', String(item.quantity || 0), metricBoxX + 12, cardY + 52, 106, fonts);
  drawKeyValue(page, 'Rate', money(item.rate || 0), metricBoxX + 12, cardY + 34, 106, fonts);
  drawKeyValue(page, 'Total', money(item.amount || 0), metricBoxX + 12, cardY + 16, 106, fonts, { emphasize: true });

  if (narrativeLines.length) {
    drawLabel(page, "What's Included", MARGIN + 18, yTop - 52, fonts);
    page.drawLine({
      start: { x: MARGIN + 18, y: yTop - 58 },
      end: { x: MARGIN + 56, y: yTop - 58 },
      thickness: 2,
      color: COLORS.sage,
    });
    drawTextBlock(page, detailLines, MARGIN + 18, yTop - 78, {
      font: fonts.regular,
      size: 9.2,
      color: COLORS.body,
      lineHeight: 14,
    });
  } else {
    page.drawText('Service completed as billed.', {
      x: MARGIN + 18,
      y: yTop - 50,
      font: fonts.regular,
      size: 9.2,
      color: COLORS.body,
    });
  }

  return cardY - 12;
}

function drawSummaryAndNotes(page, invoice, fonts, yTop) {
  const summary = toSummary(invoice);
  const leftWidth = 332;
  const rightWidth = CONTENT_WIDTH - leftWidth - 14;
  const noteText = String(invoice.notes || '').trim()
    || 'Thank you for the opportunity to serve your property. If you need additional cleanup, mulch, trimming, or seasonal service, contact us anytime and we would be happy to prepare another estimate.';
  const noteLines = wrapText(noteText, fonts.regular, 9.2, leftWidth - 34);
  const leftHeight = Math.max(116, 48 + (noteLines.length * 13));
  const blockHeight = Math.max(leftHeight, 132);
  const leftY = yTop - blockHeight;

  page.drawRectangle({
    x: MARGIN,
    y: leftY,
    width: leftWidth,
    height: blockHeight,
    color: COLORS.white,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Thank You', MARGIN + 18, yTop - 20, fonts);
  page.drawLine({
    start: { x: MARGIN + 18, y: yTop - 26 },
    end: { x: MARGIN + 56, y: yTop - 26 },
    thickness: 2,
    color: COLORS.sage,
  });
  drawTextBlock(page, noteLines, MARGIN + 18, yTop - 46, {
    font: fonts.regular,
    size: 9.2,
    color: COLORS.body,
    lineHeight: 13,
  });

  const rightX = MARGIN + leftWidth + 14;
  page.drawRectangle({
    x: rightX,
    y: leftY,
    width: rightWidth,
    height: blockHeight,
    color: COLORS.sageSoft,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Invoice Totals', rightX + 16, yTop - 20, fonts);
  drawKeyValue(page, 'Subtotal', money(invoice.subtotal || 0), rightX + 16, yTop - 48, rightWidth - 32, fonts);
  drawKeyValue(page, 'Taxes', money(invoice.tax_amount || 0), rightX + 16, yTop - 74, rightWidth - 32, fonts);
  if (summary.priorBalance > 0) {
    drawKeyValue(page, 'Prior Balance', money(summary.priorBalance), rightX + 16, yTop - 100, rightWidth - 32, fonts);
    drawKeyValue(page, 'Total Due', money(summary.totalDueOnAccount), rightX + 16, yTop - 126, rightWidth - 32, fonts, { emphasize: true });
  } else {
    drawKeyValue(page, 'Total Due', money(summary.totalDueOnAccount), rightX + 16, yTop - 108, rightWidth - 32, fonts, { emphasize: true });
  }

  return leftY - 14;
}

function drawFooterBox(page, fonts) {
  page.drawRectangle({
    x: MARGIN,
    y: 54,
    width: CONTENT_WIDTH,
    height: 56,
    color: COLORS.shell,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Payment Options', MARGIN + 16, 92, fonts);
  page.drawText('Mail a Check: Make checks payable to Pappas & Co. Landscaping and include the payment stub.', {
    x: MARGIN + 16,
    y: 72,
    font: fonts.regular,
    size: 8.7,
    color: COLORS.body,
  });
  page.drawText(`Questions? Call ${COMPANY.phone} or email ${COMPANY.email}.`, {
    x: MARGIN + 16,
    y: 60,
    font: fonts.regular,
    size: 8.7,
    color: COLORS.body,
  });
}

function addContinuationPage(pdfDoc, invoice, fonts, logo) {
  const page = pdfDoc.addPage([PAGE_WIDTH, PAGE_HEIGHT]);
  drawHeader(page, fonts, logo, `Invoice #${invoice.invoice_number || invoice.id || ''} continued`);
  return page;
}

function renderInvoicePageBody(pdfDoc, invoice, fonts, logo) {
  let page = pdfDoc.addPage([PAGE_WIDTH, PAGE_HEIGHT]);
  drawHeader(page, fonts, logo);
  drawHero(page, invoice, fonts);

  const items = normalizeLineItems(invoice.line_items);
  let cursorY = 490;

  items.forEach((item) => {
    const neededHeight = serviceCardHeight(item, fonts) + 12;
    if (cursorY - neededHeight < 210) {
      page = addContinuationPage(pdfDoc, invoice, fonts, logo);
      cursorY = PAGE_HEIGHT - 118;
    }
    cursorY = drawServiceCard(page, item, cursorY, fonts);
  });

  if (cursorY < 220) {
    page = addContinuationPage(pdfDoc, invoice, fonts, logo);
    cursorY = PAGE_HEIGHT - 128;
  }

  drawSummaryAndNotes(page, invoice, fonts, cursorY);
  drawFooterBox(page, fonts);
}

function renderStubPage(pdfDoc, invoice, fonts, logo) {
  const page = pdfDoc.addPage([PAGE_WIDTH, PAGE_HEIGHT]);
  const summary = toSummary(invoice);
  const customerLines = [invoice.customer_name || '', ...splitAddressLines(invoice.customer_address)];

  drawHeader(page, fonts, logo, 'Payment Stub');
  page.drawText('Return this stub with payment', {
    x: MARGIN,
    y: 656,
    font: fonts.bold,
    size: 20,
    color: COLORS.brandText,
  });
  page.drawText(`Invoice #${invoice.invoice_number || ''}`, {
    x: PAGE_WIDTH - MARGIN - fonts.bold.widthOfTextAtSize(`Invoice #${invoice.invoice_number || ''}`, 12),
    y: 657,
    font: fonts.bold,
    size: 12,
    color: COLORS.brandText,
  });

  page.drawRectangle({
    x: MARGIN,
    y: 480,
    width: 236,
    height: 138,
    color: COLORS.sageSoft,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Remit To', MARGIN + 16, 596, fonts);
  drawTextBlock(page, [COMPANY.name, ...COMPANY.remit_lines], MARGIN + 16, 574, {
    font: fonts.regular,
    size: 9,
    color: COLORS.black,
    lineHeight: 12,
  });

  page.drawRectangle({
    x: 292,
    y: 480,
    width: PAGE_WIDTH - MARGIN - 292,
    height: 138,
    color: COLORS.white,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Payment Summary', 308, 596, fonts);
  drawKeyValue(page, 'Customer', invoice.customer_name || '', 308, 572, 238, fonts);
  drawKeyValue(page, 'Invoice Date', invoice.invoice_date_raw || '', 308, 548, 238, fonts);
  drawKeyValue(page, 'Current Invoice', money(summary.thisInvoice), 308, 524, 238, fonts);
  drawKeyValue(page, 'Prior Balance', money(summary.priorBalance), 308, 500, 238, fonts);
  drawKeyValue(page, 'Total Due', money(summary.totalDueOnAccount), 308, 476, 238, fonts, { emphasize: true });

  page.drawRectangle({
    x: MARGIN,
    y: 318,
    width: CONTENT_WIDTH,
    height: 120,
    color: COLORS.white,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Customer Mailing Address', MARGIN + 16, 416, fonts);
  drawTextBlock(page, customerLines.filter(Boolean), MARGIN + 16, 394, {
    font: fonts.regular,
    size: 9.3,
    color: COLORS.black,
    lineHeight: 12,
  });

  page.drawText('Amount Enclosed', {
    x: MARGIN,
    y: 276,
    font: fonts.bold,
    size: 10.5,
    color: COLORS.brandText,
  });
  page.drawLine({
    start: { x: 150, y: 278 },
    end: { x: PAGE_WIDTH - MARGIN, y: 278 },
    thickness: 1,
    color: COLORS.border,
  });

  page.drawRectangle({
    x: MARGIN,
    y: 76,
    width: CONTENT_WIDTH,
    height: 116,
    color: COLORS.shell,
    borderColor: COLORS.border,
    borderWidth: 1,
  });
  drawLabel(page, 'Payment Options', MARGIN + 16, 170, fonts);
  page.drawText('Mail a Check: Make checks payable to Pappas & Co. Landscaping.', {
    x: MARGIN + 16,
    y: 148,
    font: fonts.regular,
    size: 9.2,
    color: COLORS.body,
  });
  page.drawText('Include this stub in the enclosed #9 return envelope with your payment.', {
    x: MARGIN + 16,
    y: 132,
    font: fonts.regular,
    size: 9.2,
    color: COLORS.body,
  });
  page.drawText(`Questions? Call ${COMPANY.phone} or email ${COMPANY.email}.`, {
    x: MARGIN + 16,
    y: 104,
    font: fonts.regular,
    size: 9.2,
    color: COLORS.body,
  });
}

async function renderMailInvoicePdf(invoice) {
  const pdfDoc = await PDFDocument.create();
  const fonts = {
    regular: await pdfDoc.embedFont(StandardFonts.Helvetica),
    bold: await pdfDoc.embedFont(StandardFonts.HelveticaBold),
  };
  const logo = await loadLogoImage(pdfDoc);

  renderInvoicePageBody(pdfDoc, invoice, fonts, logo);
  renderStubPage(pdfDoc, invoice, fonts, logo);

  return pdfDoc.save();
}

module.exports = {
  renderMailInvoicePdf,
};
