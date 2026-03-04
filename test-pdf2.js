// Test the ACTUAL generateQuotePDF function from server.js
// by extracting it and running it standalone

const path = require('path');
const fs = require('fs');
const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
const fontkit = require('@pdf-lib/fontkit');

// Copy formatAddressLines from server.js
function formatAddressLines(addr) {
  if (!addr) return { line1: '', line2: '' };
  const trimmed = addr.trim();
  const clean = trimmed.replace(/,/g, ' ').replace(/\s+/g, ' ').trim();
  const stateZipMatch = clean.match(/^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
  if (stateZipMatch) {
    const beforeState = stateZipMatch[1];
    const state = stateZipMatch[2];
    const zip = stateZipMatch[3];
    const roadSuffixes = /^(.+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Court|Ct|Way|Place|Pl|Circle|Cir|Terrace|Ter|Trail|Trl|Parkway|Pkwy|Highway|Hwy)\.?)\s+(.+)$/i;
    const roadMatch = beforeState.match(roadSuffixes);
    if (roadMatch) return { line1: roadMatch[1], line2: roadMatch[2] + ', ' + state + ' ' + zip };
    return { line1: beforeState, line2: state + ' ' + zip };
  }
  const commaParts = trimmed.split(',').map(p => p.trim());
  if (commaParts.length >= 3) return { line1: commaParts[0], line2: commaParts.slice(1).join(', ') };
  if (commaParts.length === 2) return { line1: commaParts[0], line2: commaParts[1] };
  return { line1: trimmed, line2: '' };
}

const quote = {
  id: 1506,
  quote_number: '1506',
  customer_name: 'Exchange Street LLC',
  customer_email: 'test@example.com',
  customer_phone: '(440) 555-1234',
  customer_address: '7777 Exchange Road Valley View OH, 44125',
  quote_type: 'monthly_plan',
  services: JSON.stringify([
    { name: 'Mowing (Weekly)', amount: 200, description: 'Weekly mowing service for entire property. Includes: Edging along all walkways and driveways. Blowing: All clippings cleared from hard surfaces.' },
    { name: 'Fertilizing - Early Spring', amount: 85, description: 'Early spring fertilizer application to promote healthy growth.' },
    { name: 'Fertilizing - Late Spring', amount: 85, description: 'Late spring fertilizer application.' },
    { name: 'Fertilizing - Summer', amount: 85, description: 'Summer fertilizer application.' },
    { name: 'Fertilizing - Fall', amount: 85, description: 'Fall fertilizer application to prepare lawn for winter.' },
    { name: 'Weed Control (Per Visit)', amount: 45, description: 'Targeted weed control treatment.' },
    { name: 'Aeration', amount: 150, description: 'Core aeration to reduce soil compaction.' },
    { name: 'Overseeding', amount: 120, description: 'Overseeding with premium grass seed blend.' },
    { name: 'Spring Cleanup', amount: 275, description: 'Full spring cleanup including debris removal and bed edging.' },
    { name: 'Fall Cleanup', amount: 350, description: 'Complete fall leaf removal and cleanup.' },
    { name: 'Mulching', amount: 400, description: 'Premium mulch installation in all landscape beds.' },
    { name: 'Bush Trimming', amount: 200, description: 'Trimming and shaping of all ornamental bushes and shrubs.' }
  ]),
  subtotal: 5800,
  tax_rate: 8,
  tax_amount: 464,
  total: 6264,
  monthly_payment: 522,
  created_at: new Date().toISOString()
};

async function test() {
  console.log('=== Testing FULL generateQuotePDF logic ===\n');

  const pdfDoc = await PDFDocument.create();
  pdfDoc.registerFontkit(fontkit);
  const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const helveticaBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  let qualyFont = helveticaBold;
  const qualyPath = path.join(__dirname, 'public', 'Qualy.otf');
  if (fs.existsSync(qualyPath)) {
    qualyFont = await pdfDoc.embedFont(fs.readFileSync(qualyPath));
  }

  let logoImage = null;
  const logoPath = path.join(__dirname, 'public', 'logo.png');
  if (fs.existsSync(logoPath)) {
    logoImage = await pdfDoc.embedPng(fs.readFileSync(logoPath));
  }

  const pageWidth = 612;
  const pageHeight = 792;
  const margin = 50;
  const contentWidth = pageWidth - (margin * 2);

  const darkGreen = rgb(0.18, 0.25, 0.24);
  const limeGreen = rgb(0.79, 0.87, 0.50);
  const black = rgb(0, 0, 0);
  const gray = rgb(0.4, 0.45, 0.45);
  const midGray = rgb(0.55, 0.58, 0.58);
  const lightGray = rgb(0.97, 0.98, 0.96);

  let services = JSON.parse(quote.services);
  const quoteNumber = quote.quote_number;
  const quoteDate = new Date(quote.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });

  // wrapText helper
  function wrapText(page, text, x, y, maxWidth, font, size, color, lineHeight = 1.4) {
    const words = text.split(' ');
    let line = '';
    let curY = y;
    for (const word of words) {
      const test = line + (line ? ' ' : '') + word;
      if (font.widthOfTextAtSize(test, size) > maxWidth && line) {
        page.drawText(line, { x, y: curY, size, font, color });
        line = word;
        curY -= size * lineHeight;
      } else {
        line = test;
      }
    }
    if (line) { page.drawText(line, { x, y: curY, size, font, color }); curY -= size * lineHeight; }
    return curY;
  }

  // wrapHeight helper
  function wrapHeight(text, maxWidth, font, size, lineHeight = 1.4) {
    const words = text.split(' ');
    let line = '';
    let lines = 0;
    for (const word of words) {
      const test = line + (line ? ' ' : '') + word;
      if (font.widthOfTextAtSize(test, size) > maxWidth && line) {
        lines++;
        line = word;
      } else {
        line = test;
      }
    }
    if (line) lines++;
    return lines * size * lineHeight;
  }

  // addContinuationPage
  function addContinuationPage() {
    const newPage = pdfDoc.addPage([pageWidth, pageHeight]);
    let py = pageHeight - margin;
    try {
      if (logoImage) {
        const logoDims = logoImage.scale(0.18);
        newPage.drawImage(logoImage, { x: margin, y: py - logoDims.height, width: logoDims.width, height: logoDims.height });
        newPage.drawText('Quote ' + quoteNumber + ' continued', { x: margin + logoDims.width + 12, y: py - 14, size: 10, font: helveticaBold, color: darkGreen });
        newPage.drawText('pappaslandscaping.com', { x: pageWidth - margin - 130, y: py, size: 8, font: helvetica, color: gray });
        newPage.drawText('(440) 886-7318', { x: pageWidth - margin - 130, y: py - 11, size: 8, font: helvetica, color: gray });
        py -= logoDims.height + 8;
      } else {
        newPage.drawText('Quote ' + quoteNumber + ' continued', { x: margin, y: py - 10, size: 10, font: helveticaBold, color: darkGreen });
        py -= 30;
      }
      newPage.drawRectangle({ x: margin, y: py, width: contentWidth, height: 3, color: limeGreen });
    } catch (contErr) {
      console.error('continuation page header error:', contErr.message);
    }
    py -= 20;
    return { page: newPage, y: py };
  }

  let page = pdfDoc.addPage([pageWidth, pageHeight]);
  let y = pageHeight - margin;

  // === HEADER ===
  try {
    if (logoImage) {
      const logoDims = logoImage.scale(0.28);
      page.drawImage(logoImage, { x: margin, y: y - logoDims.height, width: logoDims.width, height: logoDims.height });
      const cx = pageWidth - margin - 145;
      page.drawText('pappaslandscaping.com', { x: cx, y, size: 9, font: helvetica, color: gray });
      page.drawText('hello@pappaslandscaping.com', { x: cx, y: y - 13, size: 9, font: helvetica, color: gray });
      page.drawText('(440) 886-7318', { x: cx, y: y - 26, size: 9, font: helvetica, color: gray });
      y -= logoDims.height + 8;
    }
    page.drawRectangle({ x: margin, y, width: contentWidth, height: 4, color: limeGreen });
    y -= 30;
    console.log('✅ Header OK, y=' + y);
  } catch (e) { console.error('❌ Header:', e.message); }

  // === BADGE ===
  try {
    page.drawRectangle({ x: margin, y: y - 8, width: 140, height: 26, color: darkGreen });
    page.drawText('QUOTE  #' + quoteNumber, { x: margin + 12, y: y - 1, size: 11, font: helveticaBold, color: limeGreen });
    y -= 46;
    console.log('✅ Badge OK');
  } catch (e) { console.error('❌ Badge:', e.message); }

  // === INFO BOX ===
  const infoBoxH = 95;
  try {
    page.drawRectangle({ x: margin, y: y - infoBoxH, width: 250, height: infoBoxH, color: lightGray, borderColor: limeGreen, borderWidth: 2 });
    page.drawText('PREPARED FOR', { x: margin + 14, y: y - 10, size: 8, font: helveticaBold, color: midGray });
    page.drawText(quote.customer_name || '', { x: margin + 14, y: y - 26, size: 13, font: helveticaBold, color: darkGreen });
    let infoY = y - 42;
    if (quote.customer_address) {
      const addrLines = formatAddressLines(quote.customer_address);
      console.log('  Address split:', JSON.stringify(addrLines));
      page.drawText(addrLines.line1, { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
      if (addrLines.line2) {
        infoY -= 12;
        page.drawText(addrLines.line2, { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
      }
      infoY -= 14;
    }
    if (quote.customer_email) {
      page.drawText(String(quote.customer_email), { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
    }
    const dx = margin + 275;
    page.drawText('QUOTE DETAILS', { x: dx, y: y - 10, size: 8, font: helveticaBold, color: midGray });
    page.drawText('Date:', { x: dx, y: y - 26, size: 9, font: helveticaBold, color: gray });
    page.drawText(String(quoteDate), { x: dx + 30, y: y - 26, size: 9, font: helvetica, color: black });
    page.drawText('Valid For:', { x: dx, y: y - 40, size: 9, font: helveticaBold, color: gray });
    page.drawText('30 Days', { x: dx + 48, y: y - 40, size: 9, font: helvetica, color: black });
    page.drawText('Quote #:', { x: dx, y: y - 54, size: 9, font: helveticaBold, color: gray });
    page.drawText(String(quoteNumber), { x: dx + 44, y: y - 54, size: 9, font: helvetica, color: black });
    page.drawText('Type:', { x: dx, y: y - 68, size: 9, font: helveticaBold, color: gray });
    page.drawText('Annual Care Plan', { x: dx + 28, y: y - 68, size: 9, font: helvetica, color: black });
    y -= infoBoxH + 18;
    console.log('✅ Info box OK');
  } catch (e) { console.error('❌ Info box:', e.message, e.stack); }

  // === SERVICES HEADER ===
  try {
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: darkGreen });
    page.drawText('Services Included', { x: margin + 14, y: y + 2, size: 12, font: qualyFont, color: rgb(1, 1, 1) });
    y -= 33;
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 20, color: rgb(0.93, 0.94, 0.93) });
    page.drawText('SERVICE / DESCRIPTION', { x: margin + 10, y: y - 1, size: 8, font: helveticaBold, color: gray });
    page.drawText('AMOUNT', { x: pageWidth - margin - 55, y: y - 1, size: 8, font: helveticaBold, color: gray });
    y -= 22;
    console.log('✅ Services header OK');
  } catch (e) { console.error('❌ Services header:', e.message); }

  // === SERVICE ROWS (exact same logic as server.js) ===
  for (let i = 0; i < services.length; i++) {
    const svc = services[i];
    if (!svc || typeof svc !== 'object') continue;
    const svcName = (svc.name || 'Service ' + (i + 1)).toString();
    const svcAmount = svc.amount != null ? parseFloat(svc.amount) : 0;
    const desc = (svc.description || '').toString();
    const descLineHeight = 1.35;
    const descSize = 8;
    const nameSize = 10;
    const descMaxWidth = contentWidth - 75;

    let rowH = nameSize * 1.6 + 6;
    if (desc) {
      try {
        const labelRegexH = /(?:^|\s)([A-Z][A-Za-z]*(?:\s+(?:[A-Z&\/][A-Za-z]*|\([A-Za-z]+\))){0,4}):\s*/g;
        const matchesH = [];
        let mh;
        while ((mh = labelRegexH.exec(desc)) !== null) {
          const adjIdx = (mh.index > 0 && /\s/.test(desc[mh.index])) ? mh.index + 1 : mh.index;
          matchesH.push({ index: adjIdx, end: mh.index + mh[0].length });
        }
        if (matchesH.length === 0) {
          rowH += wrapHeight(desc, descMaxWidth, helvetica, descSize, descLineHeight);
        } else {
          if (matchesH[0].index > 0) {
            const bef = desc.slice(0, matchesH[0].index).trim();
            if (bef) rowH += wrapHeight(bef, descMaxWidth, helvetica, descSize, descLineHeight);
          }
          for (let mi = 0; mi < matchesH.length; mi++) {
            const textEnd = mi + 1 < matchesH.length ? matchesH[mi + 1].index : desc.length;
            const part = desc.slice(matchesH[mi].end, textEnd).trim();
            rowH += wrapHeight(part || ' ', descMaxWidth, helvetica, descSize, descLineHeight);
          }
          rowH += (matchesH.length - 1) * 4;
        }
      } catch (hErr) {
        console.error('Height calc error:', hErr.message);
        rowH += wrapHeight(desc, descMaxWidth, helvetica, descSize, descLineHeight);
      }
      rowH += 8;
    }

    try {
      if (y - rowH < 100) {
        const cont = addContinuationPage();
        page = cont.page;
        y = cont.y;
        const cp2 = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];
        cp2.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 20, color: rgb(0.93, 0.94, 0.93) });
        cp2.drawText('SERVICE / DESCRIPTION', { x: margin + 10, y: y - 1, size: 8, font: helveticaBold, color: gray });
        cp2.drawText('AMOUNT', { x: pageWidth - margin - 55, y: y - 1, size: 8, font: helveticaBold, color: gray });
        y -= 22;
      }

      const cp = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];
      const bg = i % 2 === 0 ? rgb(1, 1, 1) : rgb(0.97, 0.98, 0.97);
      cp.drawRectangle({ x: margin, y: y - rowH + nameSize * 0.4, width: contentWidth, height: rowH, color: bg });
      cp.drawText(svcName, { x: margin + 10, y, size: nameSize, font: helveticaBold, color: darkGreen });
      const amtStr = '$' + svcAmount.toFixed(2);
      cp.drawText(amtStr, { x: pageWidth - margin - 55, y, size: nameSize, font: helveticaBold, color: black });

      if (desc) {
        let dy = y - nameSize * 1.5;
        try {
          const labelRegex = /(?:^|\s)([A-Z][A-Za-z]*(?:\s+(?:[A-Z&\/][A-Za-z]*|\([A-Za-z]+\))){0,4}):\s*/g;
          const matches = [];
          let m;
          while ((m = labelRegex.exec(desc)) !== null) {
            const adjIdx = (m.index > 0 && /\s/.test(desc[m.index])) ? m.index + 1 : m.index;
            matches.push({ index: adjIdx, end: m.index + m[0].length, label: m[1] + ':' });
          }

          const sections = [];
          if (matches.length === 0) {
            sections.push({ label: null, text: desc });
          } else {
            if (matches[0].index > 0) {
              const before = desc.slice(0, matches[0].index).trim();
              if (before) sections.push({ label: null, text: before });
            }
            for (let mi = 0; mi < matches.length; mi++) {
              const textEnd = mi + 1 < matches.length ? matches[mi + 1].index : desc.length;
              const sectionText = desc.slice(matches[mi].end, textEnd).trim();
              sections.push({ label: matches[mi].label, text: sectionText });
            }
          }

          for (let si = 0; si < sections.length; si++) {
            const sec = sections[si];
            if (si > 0) dy -= 4;
            if (sec.label) {
              cp.drawText(sec.label, { x: margin + 10, y: dy, size: descSize, font: helveticaBold, color: rgb(0.12, 0.16, 0.21) });
              const labelW = helveticaBold.widthOfTextAtSize(sec.label, descSize);
              if (sec.text) {
                const spaceW = helvetica.widthOfTextAtSize(' ', descSize);
                const firstLineMax = descMaxWidth - labelW - spaceW;
                const words = sec.text.split(' ');
                let line = '';
                let firstLine = true;
                for (const word of words) {
                  const test = line + (line ? ' ' : '') + word;
                  const maxW = firstLine ? firstLineMax : descMaxWidth;
                  if (helvetica.widthOfTextAtSize(test, descSize) > maxW && line) {
                    if (firstLine) {
                      cp.drawText(line, { x: margin + 10 + labelW + spaceW, y: dy, size: descSize, font: helvetica, color: midGray });
                      firstLine = false;
                    } else {
                      cp.drawText(line, { x: margin + 10, y: dy, size: descSize, font: helvetica, color: midGray });
                    }
                    line = word;
                    dy -= descSize * descLineHeight;
                  } else {
                    line = test;
                  }
                }
                if (line) {
                  if (firstLine) {
                    cp.drawText(line, { x: margin + 10 + labelW + spaceW, y: dy, size: descSize, font: helvetica, color: midGray });
                  } else {
                    cp.drawText(line, { x: margin + 10, y: dy, size: descSize, font: helvetica, color: midGray });
                  }
                  dy -= descSize * descLineHeight;
                }
              } else {
                dy -= descSize * descLineHeight;
              }
            } else {
              dy = wrapText(cp, sec.text, margin + 10, dy, descMaxWidth, helvetica, descSize, midGray, descLineHeight);
            }
          }
        } catch (descErr) {
          console.error('Desc error for ' + svcName + ':', descErr.message);
          dy = wrapText(cp, desc, margin + 10, dy, descMaxWidth, helvetica, descSize, midGray, descLineHeight);
        }
      }

      y -= rowH;
    } catch (svcErr) {
      console.error('❌ Service error (' + svcName + '):', svcErr.message, svcErr.stack);
      y -= 30;
    }
    console.log('✅ Service ' + (i+1) + '/' + services.length + ': ' + svcName + ' (y=' + y + ')');
  }

  // === TOTALS ===
  y -= 10;
  try {
    let cp = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];
    if (y < 180) {
      const cont = addContinuationPage();
      cp = cont.page;
      y = cont.y;
    }
    const safeSubtotal = (parseFloat(quote.subtotal) || 0).toFixed(2);
    const safeTax = (parseFloat(quote.tax_amount) || 0).toFixed(2);
    const safeTotal = (parseFloat(quote.total) || 0).toFixed(2);

    cp.drawRectangle({ x: margin, y: y - 100, width: contentWidth, height: 105, color: lightGray, borderColor: limeGreen, borderWidth: 2 });
    cp.drawText('Subtotal', { x: margin + 15, y: y - 16, size: 10, font: helvetica, color: gray });
    cp.drawText('$' + safeSubtotal, { x: pageWidth - margin - 80, y: y - 16, size: 10, font: helvetica, color: black });
    cp.drawText('Tax (' + (quote.tax_rate || 8) + '%)', { x: margin + 15, y: y - 33, size: 10, font: helvetica, color: gray });
    cp.drawText('$' + safeTax, { x: pageWidth - margin - 80, y: y - 33, size: 10, font: helvetica, color: black });
    cp.drawRectangle({ x: margin + 15, y: y - 48, width: contentWidth - 30, height: 2, color: limeGreen });
    cp.drawText('TOTAL', { x: margin + 15, y: y - 70, size: 14, font: helveticaBold, color: darkGreen });
    cp.drawText('$' + safeTotal, { x: pageWidth - margin - 95, y: y - 70, size: 18, font: helveticaBold, color: darkGreen });
    y -= 115;

    if (quote.monthly_payment) {
      cp.drawRectangle({ x: margin, y: y - 6, width: contentWidth, height: 32, color: darkGreen });
      cp.drawText('Monthly Payment Plan', { x: margin + 14, y: y + 3, size: 11, font: helveticaBold, color: rgb(1, 1, 1) });
      cp.drawText('$' + (parseFloat(quote.monthly_payment) || 0).toFixed(2) + '/mo', { x: pageWidth - margin - 100, y: y + 3, size: 14, font: helveticaBold, color: limeGreen });
      y -= 46;
    }

    y -= 10;
    cp.drawRectangle({ x: margin, y: y - 48, width: contentWidth, height: 52, color: rgb(0.97, 0.99, 0.97), borderColor: limeGreen, borderWidth: 1 });
    cp.drawText('How to Accept This Quote', { x: margin + 14, y: y - 10, size: 10, font: helveticaBold, color: darkGreen });
    cp.drawText('Review your quote email and click "View Your Quote" to accept online.', { x: margin + 14, y: y - 25, size: 8, font: helvetica, color: gray });
    cp.drawText('Questions? Call or text (440) 886-7318', { x: margin + 14, y: y - 38, size: 8, font: helvetica, color: gray });
    y -= 65;

    cp.drawRectangle({ x: margin, y: y + 5, width: contentWidth, height: 3, color: limeGreen });
    y -= 14;
    cp.drawText('Pappas & Co. Landscaping  |  PO Box 770057, Lakewood, OH 44107  |  (440) 886-7318', { x: margin, y, size: 8, font: helvetica, color: gray });
    console.log('✅ Totals + footer OK');
  } catch (e) {
    console.error('❌ Totals/footer:', e.message);
  }

  // === SAVE ===
  try {
    const pdfBytes = await pdfDoc.save();
    console.log('\n✅ FULL PDF SAVED! Size:', pdfBytes.length, 'bytes (' + Math.round(pdfBytes.length / 1024) + ' KB)');
    console.log('Pages:', pdfDoc.getPageCount());
    fs.writeFileSync('/tmp/test-quote-full.pdf', pdfBytes);
    console.log('Written to /tmp/test-quote-full.pdf');
  } catch (e) {
    console.error('\n❌ SAVE FAILED:', e.message);
    console.error(e.stack);
  }
}

test().catch(e => console.error('FATAL:', e));
