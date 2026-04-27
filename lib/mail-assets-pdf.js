const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');
const { renderMailInvoicePdf } = require('./invoice-mail-pdf');

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
  const page = pdfDoc.addPage([inches(9.5), inches(4.125)]);
  const regular = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const bold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const logo = await maybeEmbedLogo(pdfDoc);

  page.drawRectangle({ x: 0, y: 0, width: inches(9.5), height: inches(4.125), color: COLORS.white });
  page.drawRectangle({ x: 0, y: inches(4.125) - 18, width: inches(9.5), height: 18, color: COLORS.forest });
  page.drawRectangle({ x: 0, y: 0, width: 130, height: 10, color: COLORS.lime });
  page.drawRectangle({ x: inches(9.5) - 150, y: 0, width: 150, height: 10, color: COLORS.forest });

  if (logo) {
    const dims = logo.scale(0.18);
    page.drawImage(logo, {
      x: 26,
      y: inches(4.125) - dims.height - 30,
      width: dims.width,
      height: dims.height,
    });
  }

  page.drawText(COMPANY.name, {
    x: 102,
    y: inches(4.125) - 52,
    font: bold,
    size: 15,
    color: COLORS.forest,
  });
  drawLines(page, COMPANY.returnAddressLines, 102, inches(4.125) - 68, regular, 8.5, COLORS.gray, 11);

  page.drawRectangle({
    x: inches(0.875),
    y: inches(0.5),
    width: inches(4.5),
    height: inches(1.125),
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
  const page = pdfDoc.addPage([inches(8.875), inches(3.875)]);
  const regular = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const bold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
  const logo = await maybeEmbedLogo(pdfDoc);

  page.drawRectangle({ x: 0, y: 0, width: inches(8.875), height: inches(3.875), color: COLORS.white });
  page.drawRectangle({ x: 0, y: inches(3.875) - 16, width: inches(8.875), height: 16, color: COLORS.forest });
  page.drawRectangle({ x: 0, y: 0, width: 110, height: 8, color: COLORS.lime });

  if (logo) {
    const dims = logo.scale(0.14);
    page.drawImage(logo, {
      x: 22,
      y: inches(3.875) - dims.height - 26,
      width: dims.width,
      height: dims.height,
    });
  }

  page.drawText('Remit To', {
    x: 138,
    y: inches(3.875) - 44,
    font: bold,
    size: 11,
    color: COLORS.forest,
  });
  page.drawText(COMPANY.name, {
    x: 138,
    y: inches(3.875) - 62,
    font: bold,
    size: 14,
    color: COLORS.forest,
  });
  drawLines(page, COMPANY.returnAddressLines.slice(0, 2), 138, inches(3.875) - 80, regular, 9, COLORS.gray, 12);
  page.drawText('Mail payment stub and check inside this envelope.', {
    x: 138,
    y: 48,
    font: regular,
    size: 8,
    color: COLORS.gray,
  });

  page.drawRectangle({
    x: 18,
    y: 18,
    width: inches(8.875) - 36,
    height: inches(3.875) - 54,
    borderColor: COLORS.border,
    borderWidth: 1,
    color: COLORS.white,
    opacity: 0.0001,
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
