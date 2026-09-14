const assert = require('assert');
const test = require('node:test');
const { PDFDocument } = require('pdf-lib');
const { renderStatementPdf, _internal } = require('../lib/statement-pdf');

test('renders a branded statement with invoice and payment reconciliation', async () => {
  const result = await renderStatementPdf({
    customer: {
      name: 'NEO Payee Solutions',
      street: '24600 Center Ridge Road',
      city: 'Westlake',
      state: 'OH',
      postal_code: '44145',
    },
    statementDate: '2026-09-14',
    sourceAsOf: '2026-09-14T20:00:00Z',
    dateRange: 'Jul 1 - Sep 14, 2026',
    invoices: [
      {
        id: 1,
        invoice_number: '12178',
        created_at: '2026-07-21',
        due_date: '2026-08-21',
        status: 'overdue',
        subtotal: 100,
        tax_amount: 7.36,
        total: 118.10,
        amount_paid: 61.52,
        line_items: [{ description: 'Mowing (Bi-Weekly)' }, { description: 'Fuel Surcharge' }],
      },
      {
        id: 2,
        invoice_number: '12411',
        created_at: '2026-08-18',
        due_date: '2026-10-01',
        status: 'pending',
        subtotal: 100,
        tax_amount: 7.36,
        total: 107.36,
        amount_paid: 0,
        line_items: [{ description: 'Mowing (Bi-Weekly)' }],
      },
    ],
    payments: [
      { paid_at: '2026-09-10', amount: 35.79, method: 'Check #358', invoice_number: '12178' },
      { paid_at: '2026-08-24', amount: 35.79, method: 'Check #10028', details: '$10.06 for Invoice #11858; $25.73 added to credit' },
    ],
  });

  assert(result.bytes.length > 5000);
  assert.strictEqual(result.summary.balance.toFixed(2), '163.94');
  assert.strictEqual(result.summary.pastDue.toFixed(2), '56.58');
  assert.strictEqual(result.summary.current.toFixed(2), '107.36');
  assert.strictEqual(result.summary.openCharges.toFixed(2), '225.46');
  assert.strictEqual(result.summary.applied.toFixed(2), '61.52');

  const pdf = await PDFDocument.load(result.bytes);
  assert(pdf.getPageCount() >= 1);
});

test('formats payment references without inventing check numbers', () => {
  assert.strictEqual(_internal.paymentReference({ method: 'Check', notes: 'Check #1964' }), 'Check #1964');
  assert.strictEqual(_internal.paymentReference({ method: 'Check' }), 'Check');
  assert.strictEqual(_internal.paymentInvoice({ details: '$25.73 for Invoice #12178' }), 'Invoice #12178');
  assert.strictEqual(
    _internal.paymentInvoice({ details: '$14.53 for Invoice #6202; $15.35 for Invoice #10483; $9.49 for Invoice #11501' }),
    'Invoices #6202, #10483, #11501'
  );
});

test('shows a paid invoice as payment activity when the account balance is zero', () => {
  const activity = _internal.buildPaymentActivity([
    {
      id: 10,
      invoice_number: '12083',
      total: 184.80,
      amount_paid: 184.80,
      paid_at: '2026-07-10T12:00:00Z',
      status: 'paid',
    },
  ], [], '2026-06-16', '2026-09-14');

  assert.strictEqual(activity.length, 1);
  assert.strictEqual(activity[0].amount, 184.80);
  assert.strictEqual(activity[0].invoice_number, '12083');
  assert.strictEqual(_internal.paymentInvoice(activity[0]), 'Invoice #12083');
});
