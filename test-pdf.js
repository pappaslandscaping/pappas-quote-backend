// Test script to reproduce the quote PDF generation issue
const path = require('path');
const fs = require('fs');

// Mock the quote data matching real data from the screenshots
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
  console.log('Loading generateQuotePDF from server.js...');

  // We can't easily import the function, so let's just copy the relevant parts
  const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
  const fontkit = require('@pdf-lib/fontkit');

  console.log('pdf-lib version:', require('pdf-lib/package.json').version);

  // Test Qualy font
  const qualyPath = path.join(__dirname, 'public', 'Qualy.otf');
  console.log('Qualy font exists:', fs.existsSync(qualyPath));
  if (fs.existsSync(qualyPath)) {
    console.log('Qualy font size:', fs.statSync(qualyPath).size, 'bytes');
  }

  // Test logo
  const logoPath = path.join(__dirname, 'public', 'logo.png');
  console.log('Logo exists:', fs.existsSync(logoPath));
  if (fs.existsSync(logoPath)) {
    console.log('Logo size:', fs.statSync(logoPath).size, 'bytes');
  }

  // Now actually run the function by extracting it
  // Let's do a minimal reproduction
  try {
    const pdfDoc = await PDFDocument.create();
    pdfDoc.registerFontkit(fontkit);

    const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);
    const helveticaBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
    console.log('Standard fonts OK');

    let qualyFont = helveticaBold;
    try {
      if (fs.existsSync(qualyPath)) {
        const qualyBytes = fs.readFileSync(qualyPath);
        qualyFont = await pdfDoc.embedFont(qualyBytes);
        console.log('Qualy embedded OK');
      }
    } catch (e) {
      console.error('Qualy embed error:', e.message);
    }

    let logoImage = null;
    try {
      if (fs.existsSync(logoPath)) {
        logoImage = await pdfDoc.embedPng(fs.readFileSync(logoPath));
        console.log('Logo embedded OK');
      }
    } catch (e) {
      console.error('Logo embed error:', e.message);
    }

    const page = pdfDoc.addPage([612, 792]);
    const darkGreen = rgb(0.18, 0.25, 0.24);
    const limeGreen = rgb(0.79, 0.87, 0.50);
    const black = rgb(0, 0, 0);
    const gray = rgb(0.4, 0.45, 0.45);
    let y = 742;

    // Test header
    try {
      if (logoImage) {
        const logoDims = logoImage.scale(0.28);
        page.drawImage(logoImage, { x: 50, y: y - logoDims.height, width: logoDims.width, height: logoDims.height });
        page.drawText('pappaslandscaping.com', { x: 417, y, size: 9, font: helvetica, color: gray });
        y -= logoDims.height + 8;
      }
      page.drawRectangle({ x: 50, y, width: 512, height: 4, color: limeGreen });
      y -= 30;
      console.log('✅ Header OK, y=' + y);
    } catch (e) {
      console.error('❌ Header error:', e.message);
    }

    // Test badge
    try {
      page.drawRectangle({ x: 50, y: y - 8, width: 140, height: 26, color: darkGreen });
      page.drawText('QUOTE  #1506', { x: 62, y: y - 1, size: 11, font: helveticaBold, color: limeGreen });
      y -= 46;
      console.log('✅ Badge OK');
    } catch (e) {
      console.error('❌ Badge error:', e.message);
    }

    // Test info box
    try {
      page.drawRectangle({ x: 50, y: y - 95, width: 250, height: 95, color: rgb(0.97, 0.98, 0.96), borderColor: limeGreen, borderWidth: 2 });
      page.drawText('PREPARED FOR', { x: 64, y: y - 10, size: 8, font: helveticaBold, color: rgb(0.55, 0.58, 0.58) });
      page.drawText('Exchange Street LLC', { x: 64, y: y - 26, size: 13, font: helveticaBold, color: darkGreen });
      page.drawText('7777 Exchange Road', { x: 64, y: y - 42, size: 9, font: helvetica, color: black });
      page.drawText('Valley View, OH 44125', { x: 64, y: y - 54, size: 9, font: helvetica, color: black });
      y -= 113;
      console.log('✅ Info box OK');
    } catch (e) {
      console.error('❌ Info box error:', e.message);
    }

    // Test services header with Qualy
    try {
      page.drawRectangle({ x: 50, y: y - 5, width: 512, height: 28, color: darkGreen });
      page.drawText('Services Included', { x: 64, y: y + 2, size: 12, font: qualyFont, color: rgb(1, 1, 1) });
      y -= 33;
      console.log('✅ Services header with Qualy OK');
    } catch (e) {
      console.error('❌ Services header error:', e.message);
    }

    // Test each service name with the actual font
    const services = JSON.parse(quote.services);
    for (const svc of services) {
      try {
        page.drawText(svc.name, { x: 60, y, size: 10, font: helveticaBold, color: darkGreen });
        // Test description too
        if (svc.description) {
          page.drawText(svc.description.substring(0, 80), { x: 60, y: y - 12, size: 8, font: helvetica, color: gray });
        }
        y -= 30;
        console.log('✅ Service OK: ' + svc.name);
      } catch (e) {
        console.error('❌ Service error (' + svc.name + '):', e.message);
        y -= 30;
      }
    }

    // Test save
    try {
      const pdfBytes = await pdfDoc.save();
      console.log('\n✅ PDF SAVED SUCCESSFULLY! Size:', pdfBytes.length, 'bytes');
      fs.writeFileSync('/tmp/test-quote.pdf', pdfBytes);
      console.log('Written to /tmp/test-quote.pdf');
    } catch (e) {
      console.error('\n❌ PDF SAVE FAILED:', e.message);
      console.error('Stack:', e.stack);
    }

  } catch (e) {
    console.error('FATAL:', e.message);
    console.error(e.stack);
  }
}

test();
