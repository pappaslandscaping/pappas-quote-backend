const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');

// Render the stored document, never regenerated terms or catalog descriptions.
async function generateSnowAgreementPDF(doc, signature = {}) {
  const pdf = await PDFDocument.create();
  pdf.setTitle(doc.title);
  pdf.setSubject(`Estimate ${doc.estimate_number}; ${doc.version}`);
  const normal = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);
  const ink = rgb(0.15, 0.23, 0.21);
  const safe = value => String(value || '').replace(/[\u2018\u2019]/g, "'").replace(/[\u201c\u201d]/g, '"').replace(/[\u2013\u2014]/g, '-').replace(/[^\x20-\x7e\xa0-\xff\n]/g, '');
  let page, y;
  function nextPage() {
    page = pdf.addPage([612, 792]); y = 738;
    page.drawText('PAPPAS & CO. LANDSCAPING', { x: 48, y, size: 10, font: bold, color: ink });
    page.drawText('440-886-7318', { x: 468, y, size: 9, font: normal, color: ink });
    y -= 24;
  }
  function text(value, { heading = false, size = 10 } = {}) {
    const font = heading ? bold : normal;
    if (heading && y < 120) nextPage();
    for (const paragraph of safe(value).split('\n')) {
      const words = paragraph.split(/\s+/); let line = '';
      function flush() {
        if (y < 58) nextPage();
        if (line) page.drawText(line, { x: 48, y, size, font, color: ink });
        y -= size * 1.4; line = '';
      }
      for (let word of words) {
        while (font.widthOfTextAtSize(word, size) > 516) {
          if (line) flush();
          let length = 1;
          while (length < word.length && font.widthOfTextAtSize(word.slice(0, length + 1), size) <= 516) length++;
          line = word.slice(0, length); flush(); word = word.slice(length);
        }
        const candidate = line ? `${line} ${word}` : word;
        if (font.widthOfTextAtSize(candidate, size) > 516 && line) flush();
        line += (line ? ' ' : '') + word;
      }
      flush();
    }
    y -= heading ? 6 : 8;
  }
  nextPage();
  text(doc.title, { heading: true, size: 22 });
  text(`Estimate #${doc.estimate_number} | Version ${doc.version}`);
  text('Client', { heading: true, size: 12 });
  text([doc.customer.name, doc.customer.email, doc.customer.phone].filter(Boolean).join('\n'));
  text('Contractor', { heading: true, size: 12 });
  text('Pappas & Co. Landscaping\nT T Pappas Enterprises LLC\nPO Box 770057, Lakewood, OH 44107\n440-886-7318 | hello@pappaslandscaping.com');
  text('Accepted services and property details', { heading: true, size: 12 });
  const money = n => n == null ? '[From accepted estimate]' : Number(n).toLocaleString('en-US', { style: 'currency', currency: 'USD' });
  for (const service of doc.services) {
    text(service.name, { heading: true, size: 11 });
    text(`Property: ${service.property_address}\nQuoted amount: ${money(service.amount)}${service.quantity == null ? '' : ` | Quantity: ${service.quantity}`}`);
    text(service.description);
  }
  text(`Accepted estimate total: ${money(doc.total)}`, { heading: true, size: 11 });
  for (const [label, value] of [['Accepted estimate notes', doc.estimate_notes], ['Accepted estimate terms', doc.estimate_terms]]) {
    if (value) { text(label, { heading: true, size: 12 }); text(value); }
  }
  for (const section of doc.sections) {
    text(section.title, { heading: true, size: 12 });
    section.paragraphs.forEach(paragraph => text(paragraph));
  }
  if (y < 170) nextPage();
  text('Client signature', { heading: true, size: 12 });
  if (/^data:image\/png;base64,[A-Za-z0-9+/=]+$/.test(signature.data || '')) {
    const image = await pdf.embedPng(Buffer.from(signature.data.split(',')[1], 'base64'));
    const dims = image.scaleToFit(260, 65);
    page.drawImage(image, { x: 48, y: y - dims.height, ...dims }); y -= dims.height + 12;
  } else if (signature.data) text(signature.data, { size: 16 });
  text(`Printed name: ${signature.name || '____________________________'}\nDate: ${signature.date || '__________________'}`);
  if (signature.hash) text(`Document reference: ${signature.hash}`, { size: 8 });
  const pages = pdf.getPages();
  pages.forEach((p, i) => p.drawText(`${doc.version} | Estimate #${safe(doc.estimate_number)} | ${i + 1} of ${pages.length}`, { x: 48, y: 30, size: 8, font: normal, color: ink }));
  return pdf.save();
}

module.exports = { generateSnowAgreementPDF };
