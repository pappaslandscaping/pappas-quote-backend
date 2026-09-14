const assert = require('assert');
const test = require('node:test');
const { matchesInvoiceCustomer } = require('../lib/copilot-customer-match');

test('matches a Copilot invoice to the requested statement customer', () => {
  const invoice = {
    customer_name: ' Kevin   Hopp ',
    customer_email: 'KDHOPP.KH@GMAIL.COM',
    copilot_customer_id: '1053401',
  };

  assert.strictEqual(matchesInvoiceCustomer(invoice, { customerName: 'Kevin Hopp' }), true);
  assert.strictEqual(matchesInvoiceCustomer(invoice, { customerEmail: 'kdhopp.kh@gmail.com' }), true);
  assert.strictEqual(matchesInvoiceCustomer(invoice, { copilotCustomerId: 1053401 }), true);
  assert.strictEqual(matchesInvoiceCustomer(invoice, { customerName: 'Someone Else' }), false);
});
