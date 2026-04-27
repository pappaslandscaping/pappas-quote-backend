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
