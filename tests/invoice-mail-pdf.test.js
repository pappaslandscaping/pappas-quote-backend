const assert = require('assert');
const { PDFDocument } = require('pdf-lib');
const { renderMailInvoicePdf } = require('../lib/invoice-mail-pdf');

async function runAssertions() {
  const pdfBytes = await renderMailInvoicePdf({
    id: 42,
    invoice_number: '10191',
    invoice_date_raw: 'Apr 03, 2026',
    customer_name: 'Bob Maclean',
    customer_address: '15646 Hocking Boulevard\nBrook Park OH 44142',
    property_address: '15646 Hocking Boulevard Brook Park OH, 44142',
    subtotal: 209,
    tax_amount: 16.4,
    total: 225.4,
    notes: 'Thank you for your business.',
    line_items: [
      {
        service_date_raw: 'Apr 02, 2026',
        name: 'Fertilizing',
        description: 'Early Spring\nFertilization & Pre-Emergent Crabgrass Control',
        quantity: 1,
        rate: 49,
        amount: 52.92,
      },
      {
        service_date_raw: 'Apr 27, 2026',
        name: 'Fuel Surcharge',
        description: '',
        quantity: 1,
        rate: 4,
        amount: 4,
      },
    ],
    metadata: {
      outstanding_balance: 18.23,
      this_invoice: 225.4,
      total_due_on_account: 243.63,
    },
  });

  assert(pdfBytes.length > 1000);

  const pdfDoc = await PDFDocument.load(pdfBytes);
  assert.strictEqual(pdfDoc.getPageCount(), 2);

  const longInvoiceBytes = await renderMailInvoicePdf({
    id: 43,
    invoice_number: '10300',
    invoice_date_raw: 'Apr 28, 2026',
    customer_name: 'Superior Industrial',
    customer_address: '3855 West 150th Street\nCleveland, OH 44111',
    property_address: '3855 West 150th Street\nCleveland, OH 44111',
    subtotal: 180,
    tax_amount: 14.08,
    total: 194.08,
    notes: 'Please return the remittance stub with your payment.',
    line_items: [
      { service_date_raw: 'Apr 01, 2026', name: 'Mowing (Weekly)', description: 'Weekly mowing, edging, trimming, and cleanup.', quantity: 1, rate: 44, line_total: 47.52 },
      { service_date_raw: 'Apr 08, 2026', name: 'Mowing (Weekly)', description: 'Weekly mowing, edging, trimming, and cleanup.', quantity: 1, rate: 44, line_total: 47.52 },
      { service_date_raw: 'Apr 15, 2026', name: 'Mowing (Weekly)', description: 'Weekly mowing, edging, trimming, and cleanup.', quantity: 1, rate: 44, line_total: 47.52 },
      { service_date_raw: 'Apr 22, 2026', name: 'Mowing (Weekly)', description: 'Weekly mowing, edging, trimming, and cleanup.', quantity: 1, rate: 44, line_total: 47.52 },
      { service_date_raw: 'Apr 29, 2026', name: 'Mowing (Weekly)', description: 'Weekly mowing, edging, trimming, and cleanup.', quantity: 1, rate: 44, line_total: 47.52 },
      { service_date_raw: 'Apr 29, 2026', name: 'Shrub Trimming', description: 'Trim front hedge and remove clippings from beds.', quantity: 1, rate: 65, line_total: 70.2 },
      { service_date_raw: 'Apr 29, 2026', name: 'Fuel Surcharge', description: '', quantity: 1, rate: 4, line_total: 4 },
    ],
    metadata: {
      outstanding_balance: 328.67,
      this_invoice: 194.08,
      total_due_on_account: 522.75,
    },
  });

  const longInvoiceDoc = await PDFDocument.load(longInvoiceBytes);
  assert.strictEqual(longInvoiceDoc.getPageCount(), 2);
}

if (typeof test === 'function') {
  test('renderMailInvoicePdf creates a valid two-page mailer PDF', runAssertions);
} else {
  runAssertions()
    .then(() => {
      console.log('invoice-mail-pdf.test.js passed');
    })
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}
