const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');
const { renderMailInvoicePdf } = require('./invoice-mail-pdf');
const { MAIL_WINDOW_SPEC, RETURN_ENVELOPE9_SPEC } = require('./mail-window-spec');

const COLORS = {
  forest: rgb(0.18, 0.25, 0.24),
  lime: rgb(0.79, 0.87, 0.5),
  gray: rgb(0.42, 0.46, 0.45),
  light: rgb(0.93, 0.95, 0.94),
  border: rgb(0.85, 0.88, 0.87),
  white: rgb(1, 1, 1),
};

const COMPANY = {
  name: 'Pappas & Co. Landscaping',
  returnAddressLines: [
    'PO Box 770057',
    'Lakewood, OH 44107',
    '(440) 886-7318',
    'hello@pappaslandscaping.com',
  ],
};

function inches(value) {
  return value * 72;
}

async function maybeEmbedLogo(pdfDoc) {
  const logoPath = path.join(__dirname, '..', 'public', 'logo.png');
  if (!fs.existsSync(logoPath)) return null;
  try {
    const bytes = await fs.promises.readFile(logoPath);
    return await pdfDoc.embedPng(bytes);
  } catch (_error) {
    return null;
  }
}

function drawLines(page, lines, x, y, font, size, color, lineHeight) {
  let cursor = y;
  lines.forEach((line) => {
    page.drawText(line, { x, y: cursor, font, size, color });
    cursor -= lineHeight;
  });
}

async function renderEnvelope10SingleWindowPdf() {
  const pdfDoc = await PDFDocument.create();
  const page = pdfDoc.addPage([inches(MAIL_WINDOW_SPEC.envelope.widthIn), inches(MAIL_WINDOW_SPEC.envelope.heightIn)]);
  const regular = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const bold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const logo = await maybeEmbedLogo(pdfDoc);

  page.drawRectangle({ x: 0, y: 0, width: inches(MAIL_WINDOW_SPEC.envelope.widthIn), height: inches(MAIL_WINDOW_SPEC.envelope.heightIn), color: COLORS.white });
  page.drawRectangle({ x: 0, y: inches(MAIL_WINDOW_SPEC.envelope.heightIn) - 18, width: inches(MAIL_WINDOW_SPEC.envelope.widthIn), height: 18, color: COLORS.forest });
  page.drawRectangle({ x: 0, y: 0, width: 130, height: 10, color: COLORS.lime });
  page.drawRectangle({ x: inches(MAIL_WINDOW_SPEC.envelope.widthIn) - 150, y: 0, width: 150, height: 10, color: COLORS.forest });

  if (logo) {
    const dims = logo.scale(0.18);
    page.drawImage(logo, {
      x: 26,
      y: inches(MAIL_WINDOW_SPEC.envelope.heightIn) - dims.height - 30,
      width: dims.width,
      height: dims.height,
    });
  }

  page.drawText(COMPANY.name, {
    x: 102,
    y: inches(MAIL_WINDOW_SPEC.envelope.heightIn) - 52,
    font: bold,
    size: 15,
    color: COLORS.forest,
  });
  drawLines(page, COMPANY.returnAddressLines, 102, inches(MAIL_WINDOW_SPEC.envelope.heightIn) - 68, regular, 8.5, COLORS.gray, 11);

  page.drawRectangle({
    x: inches(MAIL_WINDOW_SPEC.window.leftIn),
    y: inches(MAIL_WINDOW_SPEC.window.bottomIn),
    width: inches(MAIL_WINDOW_SPEC.window.widthIn),
    height: inches(MAIL_WINDOW_SPEC.window.heightIn),
    borderColor: COLORS.border,
    borderWidth: 1,
    color: COLORS.white,
    opacity: 0.0001,
  });

  page.drawText('Single-window clear area', {
    x: inches(0.98),
    y: inches(1.46),
    font: regular,
    size: 7,
    color: COLORS.border,
  });

  page.drawText('For invoices only. Recipient address is shown through the window.', {
    x: inches(5.8),
    y: inches(0.74),
    font: regular,
    size: 7.5,
    color: COLORS.gray,
  });

  return pdfDoc.save();
}

async function renderEnvelope9ReturnPdf() {
  const pdfDoc = await PDFDocument.create();
  const page = pdfDoc.addPage([inches(RETURN_ENVELOPE9_SPEC.envelope.widthIn), inches(RETURN_ENVELOPE9_SPEC.envelope.heightIn)]);
  const regular = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const bold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const logo = await maybeEmbedLogo(pdfDoc);

  page.drawRectangle({
    x: 0,
    y: 0,
    width: inches(RETURN_ENVELOPE9_SPEC.envelope.widthIn),
    height: inches(RETURN_ENVELOPE9_SPEC.envelope.heightIn),
    color: COLORS.white,
  });
  page.drawRectangle({
    x: 0,
    y: inches(RETURN_ENVELOPE9_SPEC.envelope.heightIn) - 16,
    width: inches(RETURN_ENVELOPE9_SPEC.envelope.widthIn),
    height: 16,
    color: COLORS.forest,
  });
  page.drawRectangle({ x: 0, y: 0, width: 110, height: 8, color: COLORS.lime });

  if (logo) {
    const dims = logo.scale(0.14);
    page.drawImage(logo, {
      x: inches(4.7),
      y: inches(RETURN_ENVELOPE9_SPEC.envelope.heightIn) - dims.height - 24,
      width: dims.width,
      height: dims.height,
    });
  }

  page.drawText('Double-window return envelope', {
    x: inches(4.7),
    y: inches(RETURN_ENVELOPE9_SPEC.envelope.heightIn) - 42,
    font: bold,
    size: 10.5,
    color: COLORS.forest,
  });
  page.drawText(COMPANY.name, {
    x: inches(4.7),
    y: inches(RETURN_ENVELOPE9_SPEC.envelope.heightIn) - 60,
    font: bold,
    size: 13,
    color: COLORS.forest,
  });
  drawLines(
    page,
    ['Top window shows remit address.', 'Bottom window shows customer address.'],
    inches(4.7),
    inches(RETURN_ENVELOPE9_SPEC.envelope.heightIn) - 78,
    regular,
    8,
    COLORS.gray,
    11
  );
  page.drawText('Mail payment stub and check inside this envelope.', {
    x: inches(4.7),
    y: 34,
    font: regular,
    size: 8,
    color: COLORS.gray,
  });

  page.drawRectangle({
    x: inches(RETURN_ENVELOPE9_SPEC.topWindow.leftIn),
    y: inches(RETURN_ENVELOPE9_SPEC.topWindow.bottomIn),
    width: inches(RETURN_ENVELOPE9_SPEC.topWindow.widthIn),
    height: inches(RETURN_ENVELOPE9_SPEC.topWindow.heightIn),
    borderColor: COLORS.border,
    borderWidth: 1,
    color: COLORS.white,
    opacity: 0.0001,
  });

  page.drawRectangle({
    x: inches(RETURN_ENVELOPE9_SPEC.bottomWindow.leftIn),
    y: inches(RETURN_ENVELOPE9_SPEC.bottomWindow.bottomIn),
    width: inches(RETURN_ENVELOPE9_SPEC.bottomWindow.widthIn),
    height: inches(RETURN_ENVELOPE9_SPEC.bottomWindow.heightIn),
    borderColor: COLORS.border,
    borderWidth: 1,
    color: COLORS.white,
    opacity: 0.0001,
  });

  page.drawText('Top remit window', {
    x: inches(RETURN_ENVELOPE9_SPEC.topWindow.leftIn + 0.16),
    y: inches(RETURN_ENVELOPE9_SPEC.topWindow.bottomIn + RETURN_ENVELOPE9_SPEC.topWindow.heightIn - 0.18),
    font: regular,
    size: 7,
    color: COLORS.border,
  });
  page.drawText('Bottom customer window', {
    x: inches(RETURN_ENVELOPE9_SPEC.bottomWindow.leftIn + 0.16),
    y: inches(RETURN_ENVELOPE9_SPEC.bottomWindow.bottomIn + RETURN_ENVELOPE9_SPEC.bottomWindow.heightIn - 0.18),
    font: regular,
    size: 7,
    color: COLORS.border,
  });

  return pdfDoc.save();
}

async function renderMailBatchInsertPdf(invoices) {
  const batchDoc = await PDFDocument.create();

  for (const invoice of invoices) {
    const invoiceBytes = await renderMailInvoicePdf(invoice);
    const singleDoc = await PDFDocument.load(invoiceBytes);
    const copiedPages = await batchDoc.copyPages(singleDoc, singleDoc.getPageIndices());
    copiedPages.forEach((page) => batchDoc.addPage(page));
  }

  return batchDoc.save();
}

module.exports = {
  renderEnvelope10SingleWindowPdf,
  renderEnvelope9ReturnPdf,
  renderMailBatchInsertPdf,
};
