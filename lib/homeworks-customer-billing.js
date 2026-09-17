const cents = value => Math.round(Number(value || 0) * 100);
const normalize = value => String(value || '').trim().toLowerCase();
const billedStatuses = new Set(['pending', 'partially_paid', 'past_due', 'sent', 'viewed', 'overdue']);
function isBilledInvoice(invoice) {
  return invoice.is_sent === true && !invoice.is_archived && billedStatuses.has(normalize(invoice.status));
}
function summarizeBilling(invoices) {
  const billed = invoices.filter(isBilledInvoice);
  const balance = billed.reduce((sum, invoice) => sum + Math.max(0, cents(invoice.total) - cents(invoice.amount_paid)), 0);
  const pastDue = billed.filter(invoice => ['past_due', 'overdue'].includes(normalize(invoice.status)))
    .reduce((sum, invoice) => sum + Math.max(0, cents(invoice.total) - cents(invoice.amount_paid)), 0);
  return { balance: balance / 100, pastDue: pastDue / 100, current: (balance - pastDue) / 100 };
}
async function loadHomeworksCustomerBilling({ customer, getCopilotToken, fetchImpl = fetch }) {
  const tokenInfo = await getCopilotToken();
  const match = String(tokenInfo?.cookieHeader || '').match(/(?:^|;\s*)copilotApiAccessToken=([^;]+)/i);
  if (!match) throw new Error('Connect HomeWorks to load this account.');
  async function query(operationName, query, variables) {
    const response = await fetchImpl('https://api.copilotcrm.com/graphql', {
      method: 'POST', headers: { Authorization: 'Bearer ' + decodeURIComponent(match[1]), 'Content-Type': 'application/json' },
      body: JSON.stringify({ operationName, query, variables }),
    });
    if (!response.ok) throw new Error('HomeWorks returned ' + response.status);
    const result = await response.json();
    if (result.errors?.length || !result.data) throw new Error(result.errors?.map(error => error.message).join('; ') || 'HomeWorks returned no account data.');
    return result.data;
  }
  const name = customer.name || [customer.first_name, customer.last_name].filter(Boolean).join(' ');
  const identity = [];
  if (customer.copilot_customer_id) identity.push({ id: { equals: Number(customer.copilot_customer_id) } });
  else {
    if (String(name || '').trim()) identity.push({ fullName: { equals: name.trim() } });
    if (String(customer.email || '').trim()) identity.push({ email: { equals: customer.email.trim() } });
  }
  if (!identity.length) throw new Error('This customer has no verified HomeWorks identity.');
  const found = await query('CustomerBillingIdentity',
    'query CustomerBillingIdentity($where: CustomerFilter!) { customers(where: $where, take: 20) { id number fullName email phone cell outstanding pastDue } }',
    { where: { isDeleted: false, OR: identity } });
  let candidates = found.customers;
  const emailMatches = customer.email ? candidates.filter(item => normalize(item.email) === normalize(customer.email)) : [];
  if (customer.copilot_customer_id) candidates = candidates.filter(item => item.id === Number(customer.copilot_customer_id));
  else if (emailMatches.length) candidates = emailMatches;
  else candidates = candidates.filter(item => normalize(item.fullName) === normalize(name));
  if (candidates.length !== 1) throw new Error('Could not uniquely match this account in HomeWorks.');
  const owner = candidates[0];
  const result = await query('CustomerBillingRecords',
    'query CustomerBillingRecords($customerId: SafeInt!) { invoices(where: { customerId: { equals: $customerId }, isArchived: false, deletedState: { equals: ACTIVE } }, take: 1000, orderBy: [{ date: desc }]) { id number date status subtotal tax total paidAmount lateFee processingFee isSent isArchived dueDate lineItems { name description } } payments(where: { customerId: { equals: $customerId }, isDeleted: false }, take: 1000, orderBy: [{ date: desc }]) { id invoiceId date invoiceAmount totalAmount creditAmount isRefund methodDisplayName notes details } }',
    { customerId: owner.id });
  if (result.invoices.length === 1000 || result.payments.length === 1000) throw new Error('Account history exceeds the safe statement limit.');
  const invoices = result.invoices.map(invoice => ({
    id: null, homeworks_id: invoice.id, external_invoice_id: String(invoice.id), external_source: 'copilotcrm',
    invoice_number: String(invoice.number ?? invoice.id), invoice_date: invoice.date, created_at: invoice.date,
    status: normalize(invoice.status), subtotal: invoice.subtotal, tax_amount: invoice.tax, total: invoice.total,
    amount_paid: invoice.paidAmount, is_sent: invoice.isSent, is_archived: invoice.isArchived, due_date: invoice.dueDate,
    line_items: invoice.lineItems,
    external_metadata: { late_fee: invoice.lateFee, processing_fee: invoice.processingFee, homeworks_financials: true },
  }));
  const payments = result.payments.map(payment => ({
    id: payment.id, paid_at: payment.date, amount: payment.isRefund ? -Math.abs(Number(payment.totalAmount)) : payment.totalAmount,
    method: payment.isRefund ? 'Refund' : payment.methodDisplayName, notes: payment.notes, details: payment.details,
    invoice_number: invoices.find(invoice => invoice.homeworks_id === payment.invoiceId)?.invoice_number,
  }));
  const summary = summarizeBilling(invoices);
  if (!Number.isFinite(Number(owner.outstanding)) || cents(owner.outstanding) !== cents(summary.balance)) {
    throw new Error('HomeWorks account balance does not reconcile with its sent invoices. Please review credits and account adjustments.');
  }
  return { invoices, payments, ...summary, source: 'homeworks', sourceAsOf: new Date().toISOString(), homeworksCustomerId: owner.id };
}
module.exports = { loadHomeworksCustomerBilling, isBilledInvoice, summarizeBilling };
