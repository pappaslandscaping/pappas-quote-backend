const assert = require('assert');
const { toDbValuesFromDetail } = require('../scripts/import-copilot-invoices');

function runAssertions() {
  const values = toDbValuesFromDetail({
    external_invoice_id: '555',
    invoice_number: '10951',
    customer_name: 'Superior Industrial',
    invoice_date: '2026-04-28',
    total: 194.08,
    tax_amount: 14.08,
    subtotal: 180,
    line_items: [],
    metadata: {
      prior_balance: 264.75,
      total_due_on_account: 458.83,
      customer_outstanding_balance: 458.83,
    },
  }, null);

  assert.strictEqual(values.metadata.prior_balance, 264.75);
  assert.strictEqual(values.metadata.total_due_on_account, 458.83);
  assert.strictEqual(values.metadata.customer_outstanding_balance, 458.83);
  assert.strictEqual(values.metadata.customer_name, 'Superior Industrial');
  assert.strictEqual(values.metadata.invoice_date, '2026-04-28');
}

if (typeof test === 'function') {
  test('toDbValuesFromDetail preserves derived detail metadata', runAssertions);
} else {
  runAssertions();
  console.log('import-copilot-invoice-detail-metadata.test.js passed');
}
