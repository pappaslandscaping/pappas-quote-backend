const records = require('../services/homeworks/records');
const invoice = { id: 1, number: 3372, customerId: 9, date: '2020-01-01', status: 'PENDING', subtotal: '100', tax: '8', total: '108', paidAmount: '0', isSent: true, lineItems: [] };
const context = () => ({ queryImpl: jest.fn(async request => request.operationName === 'YardDeskInvoiceOwners'
  ? { customers: [{ id: 9, fullName: 'Verified customer' }] }
  : { invoices: request.variables.where.id ? [{ ...invoice, id: 2, customerId: 99 }] : [invoice, { ...invoice, id: 2, customerId: 99 }] }) });

test('historical invoices with unavailable owners remain visible without inventing contact information', async () => {
  const rows = await records.loadInvoices(context(), {}, { details: false, allowUnverifiedCustomers: true });
  expect(rows).toHaveLength(2);
  expect(rows[0]).toMatchObject({ customer_name: 'Verified customer', customer_verification_required: false });
  expect(rows[1]).toMatchObject({ customer_name: 'Customer unavailable (Homeworks #99)', customer_email: '', customer_phone: '', total: 108, total_due: 108, owner: null, customer_verification_required: true });
  expect(rows[1].metadata.customer_verification_required).toBe(true);
});
test('billing snapshots still reject an invoice whose owner cannot be verified', async () => {
  await expect(records.invoiceSnapshot(context(), 2)).rejects.toThrow('no verified customer');
});
test('a malformed customer response is unavailable rather than a set of historical orphan records', async () => {
  const queryImpl = jest.fn(async request => request.operationName === 'YardDeskInvoiceOwners' ? {} : { invoices: [invoice] });
  await expect(records.loadInvoices({ queryImpl }, {}, { details: false, allowUnverifiedCustomers: true })).rejects.toThrow('customer verification is unavailable');
});
