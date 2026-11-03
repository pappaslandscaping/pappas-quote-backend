const assert = require('node:assert/strict');
const { test } = require('node:test');
const { refreshHomeworksMailInvoices } = require('../lib/homeworks-mail-invoices');

const live = { id: 3908906, number: 12930, customerId: 1053541, status: 'PENDING', date: '2026-10-29',
  subtotal: '495', tax: '36.8', total: '531.8', paidAmount: '0', lineItems: [] };
const owner = { id: 1053541, outstanding: '531.8', pastDue: '0' };
const rows = [
  { id: 18564, external_invoice_id: '4095795', invoice_number: '13284', status: 'pending', total: '59.4' },
  { id: 18210, external_invoice_id: '3908906', invoice_number: '12930', status: 'pending', total: '0' },
];
function api({ deleted = true, active = live, customers = [owner], errors } = {}) {
  const calls = [];
  const query = async (name, text, variables) => {
    assert(!/\bmutation\b/.test(text));
    calls.push({ name, text, variables });
    if (name === 'MailInvoiceSnapshots') return { data: { invoices: active ? [active] : [] } };
    if (name === 'MailDeletedInvoices') return { errors, data: { invoices: deleted ? [{ id: 4095795, number: 13284, deletedState: 'DELETED' }] : [] } };
    if (name === 'MailCustomerStanding') return { data: { customers } };
    throw new Error(`Unexpected operation: ${name}`);
  };
  return { calls, query };
}

test('mixed batch excludes a verified deleted invoice and retains live financials without writes', async () => {
  const { query, calls } = api();
  const before = JSON.stringify(rows);
  const result = await refreshHomeworksMailInvoices(rows, query, { skipDeleted: true });
  assert.equal(result[0].homeworks_print_exclusion, 'Deleted in HomeWorks');
  assert.equal(result[1].total, '531.8');
  assert.equal(result[1].external_metadata.customer_outstanding_balance, '531.8');
  assert.deepEqual(calls.find(call => call.name === 'MailDeletedInvoices').variables.ids, [4095795]);
  assert.equal(JSON.stringify(rows), before);
});
test('single deleted invoice produces an actionable error', async () => {
  await assert.rejects(refreshHomeworksMailInvoices([rows[0]], api({ active: null }).query),
    error => error.code === 'HOMEWORKS_INVOICE_DELETED' && error.message.includes('#13284'));
});
test('all-deleted batch returns exclusions without requesting customer standing', async () => {
  const { query, calls } = api({ active: null });
  const result = await refreshHomeworksMailInvoices([rows[0]], query, { skipDeleted: true });
  assert.equal(result.length, 1);
  assert(result[0].homeworks_print_exclusion);
  assert(!calls.some(call => call.name === 'MailCustomerStanding'));
});
test('missing records are never silently treated as deleted', async () => {
  await assert.rejects(refreshHomeworksMailInvoices(rows, api({ deleted: false }).query, { skipDeleted: true }), /unavailable/);
});
test('partial deletion lookup failures stop printing', async () => {
  await assert.rejects(refreshHomeworksMailInvoices(rows, api({ errors: [{ message: 'unavailable' }] }).query, { skipDeleted: true }), /could not be verified/);
});
test('active records require current customer standing', async () => {
  await assert.rejects(refreshHomeworksMailInvoices([rows[1]], api({ customers: [] }).query), /unavailable/);
});
test('paid status comes from HomeWorks rather than stale YardDesk status', async () => {
  const [result] = await refreshHomeworksMailInvoices([rows[1]], api({ active: { ...live, status: 'PAID', paidAmount: '531.8' } }).query);
  assert.equal(result.status, 'paid');
  assert.equal(result.amount_paid, '531.8');
});
test('active-only batch does not perform deletion lookups', async () => {
  const { query, calls } = api();
  await refreshHomeworksMailInvoices([rows[1]], query);
  assert(!calls.some(call => call.name === 'MailDeletedInvoices'));
});
