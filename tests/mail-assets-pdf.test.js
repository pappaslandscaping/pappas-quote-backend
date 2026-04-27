const assert = require('assert');
const { PDFDocument } = require('pdf-lib');
const {
  renderEnvelope10SingleWindowPdf,
  renderEnvelope9ReturnPdf,
  renderMailBatchInsertPdf,
} = require('../lib/mail-assets-pdf');

const sampleInvoice = {
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
  ],
  metadata: {
    outstanding_balance: 18.23,
    this_invoice: 225.4,
    total_due_on_account: 243.63,
  },
};

async function runAssertions() {
  const envelope10 = await renderEnvelope10SingleWindowPdf();
  const envelope9 = await renderEnvelope9ReturnPdf();
  const batch = await renderMailBatchInsertPdf([sampleInvoice, { ...sampleInvoice, id: 43, invoice_number: '10192' }]);

  const envelope10Doc = await PDFDocument.load(envelope10);
  const envelope9Doc = await PDFDocument.load(envelope9);
  const batchDoc = await PDFDocument.load(batch);

  assert.strictEqual(envelope10Doc.getPageCount(), 1);
  assert.strictEqual(envelope9Doc.getPageCount(), 1);
  assert.strictEqual(batchDoc.getPageCount(), 4);
}

if (typeof test === 'function') {
  test('mail asset PDFs render envelope templates and combined insert batches', runAssertions);
} else {
  runAssertions()
    .then(() => {
      console.log('mail-assets-pdf.test.js passed');
    })
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}
