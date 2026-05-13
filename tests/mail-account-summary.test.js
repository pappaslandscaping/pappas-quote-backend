const assert = require('assert');
const { resolveMailAccountSummary } = require('../lib/mail-account-summary');

function parseMoney(value) {
  if (value === null || value === undefined || value === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function roundMoney(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function runAssertions() {
  const summary = resolveMailAccountSummary({
    invoiceTotal: 194.08,
    metadata: {
      outstanding_balance: 458.83,
      this_invoice: 194.08,
    },
    parseMoney,
    roundMoney,
  });

  assert.strictEqual(summary.priorBalance, 264.75);
  assert.strictEqual(summary.thisInvoice, 194.08);
  assert.strictEqual(summary.totalDueOnAccount, 458.83);

  const copilotStandingSummary = resolveMailAccountSummary({
    invoiceTotal: 194.08,
    metadata: {
      prior_balance: 29.11,
      past_due_balance: 29.11,
      customer_past_due_balance: 29.11,
      customer_outstanding_balance: 137.11,
      total_due_on_account: 137.11,
    },
    parseMoney,
    roundMoney,
  });

  assert.strictEqual(copilotStandingSummary.priorBalance, 137.11);
  assert.strictEqual(copilotStandingSummary.thisInvoice, 194.08);
  assert.strictEqual(copilotStandingSummary.totalDueOnAccount, 331.19);

  const sentInvoiceStandingSummary = resolveMailAccountSummary({
    invoiceTotal: 41.8,
    metadata: {
      prior_balance: 41.8,
      past_due_balance: 0,
      customer_past_due_balance: 0,
      customer_outstanding_balance: 41.8,
      total_due_on_account: 83.6,
    },
    parseMoney,
    roundMoney,
  });

  assert.strictEqual(sentInvoiceStandingSummary.priorBalance, 0);
  assert.strictEqual(sentInvoiceStandingSummary.thisInvoice, 41.8);
  assert.strictEqual(sentInvoiceStandingSummary.totalDueOnAccount, 41.8);

  const partialCreditSummary = resolveMailAccountSummary({
    invoiceTotal: 167.52,
    amountPaid: 12.12,
    metadata: {
      prior_balance: 155.4,
      outstanding_balance: 155.4,
      customer_outstanding_balance: 155.4,
      total_due_on_account: 322.92,
    },
    parseMoney,
    roundMoney,
  });

  assert.strictEqual(partialCreditSummary.paymentCredit, 12.12);
  assert.strictEqual(partialCreditSummary.priorBalance, 0);
  assert.strictEqual(partialCreditSummary.thisInvoice, 155.4);
  assert.strictEqual(partialCreditSummary.totalDueOnAccount, 155.4);
}

if (typeof test === 'function') {
  test('resolveMailAccountSummary treats outstanding_balance as account total when account due is absent', runAssertions);
} else {
  runAssertions();
  console.log('mail-account-summary.test.js passed');
}
