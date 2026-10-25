const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');
const fontkit = require('@pdf-lib/fontkit');

// Use the landscaping agreement's brand assets and layout, while rendering only
// the stored snow document. Never rebuild accepted terms or service descriptions.
async function generateSnowAgreementPDF(doc, signature = {}) {
  const pdf = await PDFDocument.create();
  pdf.setTitle(doc.title);
  pdf.setSubject(`Estimate ${doc.estimate_number}; ${doc.version}`);
  pdf.registerFontkit(fontkit);
  const normal = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const headingFont = await pdf.embedFont(fs.readFileSync(path.join(__dirname, '../public/MaisonduSoleil-Sans.otf')));
  const logo = await pdf.embedPng(fs.readFileSync(path.join(__dirname, '../public/logo.png')));
  const darkGreen = rgb(0.18, 0.25, 0.24);
  const limeGreen = rgb(0.79, 0.87, 0.50);
  const black = rgb(0, 0, 0);
  const gray = rgb(0.4, 0.4, 0.4);
  const light = rgb(0.98, 0.98, 0.98);
  const margin = 50, width = 512, bottom = 80;
  const safe = value => String(value == null ? '' : value)
    .replace(/[\u2018\u2019]/g, "'").replace(/[\u201c\u201d]/g, '"')
    .replace(/[\u2013\u2014]/g, '-').replace(/[^\x20-\x7e\xa0-\xff\n]/g, '');
  const money = n => n == null ? '[From accepted estimate]' : Number(n).toLocaleString('en-US', { style: 'currency', currency: 'USD' });
  let page, y;

  function linesFor(value, font = normal, size = 9, maxWidth = width) {
    const lines = [];
    for (const paragraph of safe(value).split('\n')) {
      let line = '';
      for (let word of paragraph.split(/\s+/).filter(Boolean)) {
        while (font.widthOfTextAtSize(word, size) > maxWidth) {
          if (line) { lines.push(line); line = ''; }
          let length = 1;
          while (length < word.length && font.widthOfTextAtSize(word.slice(0, length + 1), size) <= maxWidth) length++;
          lines.push(word.slice(0, length)); word = word.slice(length);
        }
        if (!word) continue;
        const next = line ? `${line} ${word}` : word;
        if (font.widthOfTextAtSize(next, size) > maxWidth && line) { lines.push(line); line = word; }
        else line = next;
      }
      if (line || !paragraph.trim()) lines.push(line);
    }
    return lines;
  }
  function newPage() {
    page = pdf.addPage([612, 792]);
    y = 742;
  }
  function ensure(height) { if (y - height < bottom) newPage(); }
  function text(value, { font = normal, size = 9, x = margin, maxWidth = width, color = black, gap = 7 } = {}) {
    const lines = linesFor(value, font, size, maxWidth);
    const leading = size * 1.3;
    // Keep ordinary paragraphs together, but safely paginate oversized content.
    if (lines.length * leading <= 580) ensure(lines.length * leading + gap);
    for (let i = 0; i < lines.length; i++) {
      ensure(leading);
      if (lines[i]) page.drawText(lines[i], { x, y, size, font, color });
      y -= leading;
    }
    y -= gap;
  }
  function bar(title, large = false, followingHeight = 35) {
    const size = large ? 11 : 10, height = large ? 26 : 22;
    const lines = linesFor(title, headingFont, size, width - 24);
    const extra = (lines.length - 1) * 14;
    ensure(height + extra + 10 + Math.min(followingHeight, 580));
    page.drawRectangle({ x: margin, y: y - height - extra, width, height: height + extra, color: darkGreen });
    lines.forEach((line, i) => page.drawText(line, { x: margin + 12, y: y - 17 - i * 14, size, font: headingFont, color: limeGreen }));
    y -= height + extra + 17;
  }
  function column(value, x, top, font = normal, size = 9) {
    const lines = linesFor(value, font, size, 246);
    lines.forEach((line, i) => page.drawText(line, { x, y: top - i * 11.7, size, font, color: black }));
    return top - lines.length * 11.7;
  }

  newPage();
  const logoDims = logo.scale(0.28);
  page.drawImage(logo, { x: margin, y: y - logoDims.height, ...logoDims });
  ['pappaslandscaping.com', 'hello@pappaslandscaping.com', '(440) 886-7318'].forEach((line, i) =>
    page.drawText(line, { x: 417, y: y - i * 13, size: 9, font: normal, color: gray }));
  y -= logoDims.height + 8;
  page.drawRectangle({ x: margin, y, width, height: 4, color: limeGreen });
  y -= 12;
  bar(`${doc.title}  #${doc.estimate_number}`, true, 120);
  y -= 16;

  const partiesY = y;
  page.drawText('SERVICE PROVIDER', { x: margin, y, size: 8, font: bold, color: gray });
  page.drawText('CLIENT', { x: 316, y, size: 8, font: bold, color: gray });
  let providerY = column('Pappas & Co. Landscaping', margin, partiesY - 14, bold, 10);
  providerY = column('T T Pappas Enterprises LLC\nPO Box 770057\nLakewood, OH 44107\n(440) 886-7318\nhello@pappaslandscaping.com', margin, providerY);
  let clientY = column(doc.customer.name, 316, partiesY - 14, bold, 10);
  const addresses = [...new Set(doc.services.map(s => s.property_address).filter(Boolean))];
  const address = addresses.length === 1 ? addresses[0] : 'Service addresses listed below';
  clientY = column([address, doc.customer.email, doc.customer.phone].filter(Boolean).join('\n'), 316, clientY);
  y = Math.min(providerY, clientY) - 20;

  function tableHeader() {
    bar('Services & Pricing', true, 55);
    page.drawRectangle({ x: margin, y: y - 15, width, height: 22, color: rgb(0.95, 0.95, 0.95) });
    page.drawText('Service', { x: margin + 10, y: y - 8, size: 9, font: bold, color: gray });
    page.drawText('Amount', { x: 500, y: y - 8, size: 9, font: bold, color: gray });
    y -= 22;
  }
  tableHeader();
  for (const [i, service] of doc.services.entries()) {
    const nameLines = linesFor(service.name, normal, 9, 330);
    const amountLines = linesFor(money(service.amount), bold, 9, 150);
    const count = Math.max(nameLines.length, amountLines.length);
    for (let offset = 0; offset < count;) {
      if (y - 24 < bottom) { newPage(); tableHeader(); }
      const fit = Math.max(1, Math.min(count - offset, Math.floor((y - bottom - 10) / 12)));
      const height = Math.max(22, fit * 12 + 10);
      page.drawRectangle({ x: margin, y: y - height, width, height, color: i % 2 ? light : rgb(1, 1, 1) });
      for (let row = 0; row < fit; row++) {
        const name = nameLines[offset + row], amount = amountLines[offset + row];
        if (name) page.drawText(name, { x: 60, y: y - 14 - row * 12, size: 9, font: normal, color: black });
        if (amount) page.drawText(amount, { x: 552 - bold.widthOfTextAtSize(amount, 9), y: y - 14 - row * 12, size: 9, font: bold, color: black });
      }
      y -= height; offset += fit;
    }
  }
  y -= 6; ensure(40);
  const totalLines = linesFor(`Accepted estimate total: ${money(doc.total)}`, bold, 11, width - 30);
  const totalHeight = 28 + (totalLines.length - 1) * 14;
  ensure(totalHeight + 20);
  page.drawRectangle({ x: margin, y: y - totalHeight, width, height: totalHeight, color: light, borderColor: limeGreen, borderWidth: 1 });
  totalLines.forEach((line, i) => page.drawText(line, { x: 65, y: y - 18 - i * 14, size: 11, font: bold, color: darkGreen }));
  y -= totalHeight + 22;

  bar('Accepted Service Details');
  for (const service of doc.services) {
    ensure(65);
    text(service.name, { font: bold, size: 9.5, color: darkGreen, gap: 3 });
    text(`Property: ${service.property_address}${service.quantity == null ? '' : `\nQuantity: ${service.quantity}`}`, { gap: 5 });
    text(service.description);
  }
  for (const [label, value] of [['Accepted Estimate Notes', doc.estimate_notes], ['Accepted Estimate Terms', doc.estimate_terms]]) {
    if (value) { bar(label); text(value); }
  }
  for (const [index, section] of doc.sections.entries()) {
    const firstHeight = linesFor(section.paragraphs[0] || '').length * 11.7 + 7;
    // Keep acceptance and its single signature box on the same page when possible.
    if (index === doc.sections.length - 1) ensure(firstHeight + 220);
    bar(section.title, false, firstHeight);
    section.paragraphs.forEach(paragraph => text(paragraph));
    y -= 4;
  }

  // Match the landscaping signature box without marking an unsigned preview as signed.
  const signed = Boolean(signature.data);
  const identityLines = linesFor(`Printed name: ${signature.name || '____________________________'}\nDate: ${signature.date || '__________________'}`, normal, 9, width - 30);
  const referenceLines = signature.hash ? linesFor(`Document reference: ${signature.hash}`, normal, 7, width - 30) : [];
  const signatureLines = signed && !/^data:image\//.test(signature.data) ? linesFor(signature.data, normal, 14, width - 30) : [];
  const signatureHeight = Math.max(45, signatureLines.length * 18);
  const boxHeight = 65 + signatureHeight + identityLines.length * 12 + referenceLines.length * 10;
  ensure(boxHeight + 14);
  const top = y;
  page.drawRectangle({ x: margin, y: top - boxHeight, width, height: boxHeight, color: light, borderColor: darkGreen, borderWidth: 2 });
  page.drawText(signed ? 'Agreement Accepted & Digitally Signed' : 'Client Signature', { x: 65, y: top - 22, size: 12, font: bold, color: darkGreen });
  page.drawText('CLIENT SIGNATURE:', { x: 65, y: top - 42, size: 8, font: bold, color: gray });
  if (/^data:image\/png;base64,[A-Za-z0-9+/=]+$/.test(signature.data || '')) {
    const signatureImage = await pdf.embedPng(Buffer.from(signature.data.split(',')[1], 'base64'));
    const dims = signatureImage.scaleToFit(260, 40);
    page.drawImage(signatureImage, { x: 65, y: top - 48 - dims.height, ...dims });
  } else signatureLines.forEach((line, i) => page.drawText(line, { x: 65, y: top - 65 - i * 18, size: 14, font: normal, color: black }));
  y = top - 48 - signatureHeight;
  page.drawLine({ start: { x: 65, y }, end: { x: 325, y }, thickness: 0.7, color: gray });
  y -= 17;
  identityLines.forEach(line => { page.drawText(line, { x: 65, y, size: 9, font: normal, color: black }); y -= 12; });
  y -= 3;
  referenceLines.forEach(line => { page.drawText(line, { x: 65, y, size: 7, font: normal, color: gray }); y -= 10; });

  const pages = pdf.getPages();
  pages.forEach((p, i) => {
    p.drawRectangle({ x: margin, y: 57, width, height: 3, color: limeGreen });
    p.drawText('Pappas & Co. Landscaping | T T Pappas Enterprises LLC | PO Box 770057, Lakewood, OH 44107', { x: margin, y: 43, size: 8, font: normal, color: gray });
    p.drawText('(440) 886-7318 | hello@pappaslandscaping.com | pappaslandscaping.com', { x: margin, y: 32, size: 8, font: normal, color: gray });
    const pageLabel = `${i + 1} of ${pages.length}`;
    p.drawText(pageLabel, { x: 562 - normal.widthOfTextAtSize(pageLabel, 8), y: 32, size: 8, font: normal, color: gray });
  });
  return pdf.save();
}

module.exports = { generateSnowAgreementPDF };
