const assert = require('assert');
const { refreshHomeworksMailInvoices, resolveHomeworksMailFinancials } = require('../lib/homeworks-mail-invoices');
const { renderMailInvoiceHtml } = require('../lib/invoice-mail-html');
const { renderMailInvoicePdf } = require('../lib/invoice-mail-pdf');
const createInvoiceRoutes = require('../routes/invoices');
createInvoiceRoutes({
  pool: { query: async () => ({ rows: [] }) },
  authenticateToken: (_req, _res, next) => next(),
  getCopilotToken: async () => ({ cookieHeader: 'other=value; copilotApiAccessToken=test-token' }),
});
const { buildMailInvoicePayload, refreshMailInvoiceRows } = createInvoiceRoutes._helpers;

async function run() {
  const row = { id: 1, external_invoice_id: '3123384', invoice_number: '11875',
    customer_name: 'Jackie Singleton', customer_address: '25825 Eaton Way\nBay Village, OH 44140',
    subtotal: 118, tax_amount: 8.8, total: 126.8, external_metadata: {} };
  const source = { id: 3123384, number: 11875, customerId: 2686070, date: '2026-06-25',
    subtotal: '118', tax: '8.8', total: '145.82', lateFee: '19.02',
    processingFee: '0', discountAmount: '0', paidAmount: '0',
    lineItems: [
      { name: 'Mowing (Weekly)', date: '2026-06-11', price: '55', quantity: '1', subtotal: '55', subtotalWithTax: '59.4', taxPrice1: '4.4', taxPrice2: '0', lineItemOrder: 1 },
      { name: 'Mowing (Weekly)', date: '2026-06-25', price: '55', quantity: '1', subtotal: '55', subtotalWithTax: '59.4', taxPrice1: '4.4', taxPrice2: '0', lineItemOrder: 2 },
      { name: 'Fuel Surcharge', date: '2026-06-30', price: '8', quantity: '1', subtotal: '8', subtotalWithTax: '8', taxPrice1: '0', taxPrice2: '0', lineItemOrder: 3 },
    ] };
  const calls = [];
  const refresh = async (invoice = source, outstanding = '333.02') => refreshHomeworksMailInvoices([row], async (name, query, variables) => {
    calls.push({ name, query, variables });
    assert(!query.includes('mutation'));
    return { data: name === 'MailInvoiceSnapshots' ? { invoices: [invoice] } : { customers: [{ id: 2686070, outstanding, pastDue: '145.82' }] } };
  });
  const [snapshot] = await refresh();
  const payload = buildMailInvoicePayload(snapshot);
  assert.strictEqual(payload.total, 145.82);
  assert.strictEqual(payload.metadata.outstanding_balance, 187.2);
  assert.strictEqual(payload.metadata.total_due, 333.02);
  assert.strictEqual(payload.invoice_date_raw, 'Jun 25, 2026');
  const html = renderMailInvoiceHtml(payload);
  assert(html.includes('<span>Late Fee</span><span>$19.02</span>'));
  assert(html.includes('<span>Prior Balance</span><span>$187.20</span>'));
  assert(html.includes('<span>Total Due</span><span>$333.02</span>'));
  assert(!html.includes('$352.04'));
  const pdf = await renderMailInvoicePdf(payload);
  assert(pdf.length > 1000);
  const [paid] = await refresh({ ...source, paidAmount: '20' }, '313.02');
  const paidPayload = buildMailInvoicePayload(paid);
  assert.strictEqual(paidPayload.metadata.total_due, 313.02);
  assert.strictEqual(paidPayload.metadata.outstanding_balance, 187.2);
  const [noFee] = await refresh({ ...source, lateFee: '0', total: '126.8' }, '314');
  assert(!renderMailInvoiceHtml(buildMailInvoicePayload(noFee)).includes('<span>Late Fee</span>'));
  await assert.rejects(refreshHomeworksMailInvoices([row], async () => ({ data: { invoices: [], customers: [] } })), /unavailable/);
  assert.strictEqual(row.total, 126.8); // Printing never mutates the stored charge.
  const originalFetch = global.fetch;
  try {
    global.fetch = async (url, options) => {
      assert.strictEqual(url, 'https://api.copilotcrm.com/graphql');
      assert.strictEqual(options.headers.Authorization, 'Bearer test-token');
      const body = JSON.parse(options.body);
      return { ok: true, json: async () => ({ data: body.operationName === 'MailInvoiceSnapshots'
        ? { invoices: [source] }
        : { customers: [{ id: 2686070, outstanding: '333.02', pastDue: '145.82' }] } }) };
    };
    const [live] = await refreshMailInvoiceRows([row]);
    const livePayload = buildMailInvoicePayload(live);
    assert.strictEqual(livePayload.total, 145.82);
    assert.strictEqual(livePayload.metadata.total_due, 333.02);
    assert.strictEqual(livePayload.metadata.outstanding_balance, 187.2);
    global.fetch = async () => ({ ok: false, status: 401 });
    await assert.rejects(refreshMailInvoiceRows([row]), /401/);
  } finally { global.fetch = originalFetch; }
  console.log('HomeWorks mail invoice regression checks passed');
}
run().catch(error => { console.error(error); process.exitCode = 1; });
