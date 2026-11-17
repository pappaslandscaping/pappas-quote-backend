const { mirrorInvoices } = require('../services/homeworks/invoice-cache');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const invoice = { external_invoice_id: '71', invoice_number: '9091', customer_name: 'Current owner', status: 'sent', total: 108, amount_paid: 0, metadata: {} };

test('a conflicting local link is visible read-only without reassigning its historical record', async () => {
  const pool = { query: jest.fn().mockResolvedValue({ rows: [{ id: 1, invoice_number: '9091', external_source: 'copilotcrm', external_invoice_id: '999' }] }) };
  const rows = await mirrorInvoices(pool, [invoice], { details: false, allowConflicts: true });
  expect(rows[0]).toMatchObject({ invoice_number: '9091', external_invoice_id: '71', total: 108, id: null, linkage_review_required: true });
  expect(rows[0].external_metadata.linkage_review_required).toBe(true);
  expect(pool.query).toHaveBeenCalledTimes(1);
});
test('a conflict does not prevent verified invoices from refreshing', async () => {
  const pool = { query: jest.fn()
    .mockResolvedValueOnce({ rows: [{ id: 1, invoice_number: '9091', external_source: 'copilotcrm', external_invoice_id: '999' }] })
    .mockResolvedValueOnce({ rows: [{ id: 2, invoice_number: '9092', external_invoice_id: '72' }] }) };
  const rows = await mirrorInvoices(pool, [invoice, { ...invoice, invoice_number: '9092', external_invoice_id: '72' }], { details: false, allowConflicts: true });
  expect(rows[0].linkage_review_required).toBe(true);
  expect(rows[1].id).toBe(2);
  const writes = JSON.parse(pool.query.mock.calls[1][1][0]);
  expect(writes.map(row => row.external_invoice_id)).toEqual(['72']);
});
test('an invoice needing linkage review has no local billing or mail actions in the list', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/invoices.html'), 'utf8');
  const start = html.indexOf('function renderInvoices()');
  const end = html.indexOf('async function loadStats()', start);
  const body = { innerHTML: '' }, count = { textContent: '' };
  const noop = () => {};
  const context = vm.createContext({ invoices: [{ ...invoice, id: null, linkage_review_required: true }], sortCol: 'invoice_number', sortDir: 'asc', selectedInvoiceIds: new Set(),
    document: { getElementById: id => id === 'invoicesBody' ? body : id === 'invoiceCount' ? count : { value: '' } },
    syncSelectedInvoicesToCurrentList: noop, updateInvoiceSelectionState: noop, getInvoiceNumberDisplay: row => row.invoice_number, esc: value => String(value ?? ''),
    dueMeta: () => ({ primary: '', sub: '', overdue: false }), formatCurrency: String, getMailStateMeta: () => ({ label: 'Review', pillClass: '', sub: '' }),
    getSentStatus: () => 'sent', getSentStatusLabel: () => 'Sent', canInvoiceBeMailed: () => false, getStatusPill: String, invoiceNeedsReview: () => true });
  vm.runInContext(html.slice(start, end), context);
  context.renderInvoices();
  expect(body.innerHTML).toContain('Review in Homeworks');
  expect(body.innerHTML).toContain('/finances/invoices/view/71');
  expect(body.innerHTML).not.toMatch(/onclick="(?:sendInvoice|markPaid|deleteInvoice|viewInvoice)/);
});
