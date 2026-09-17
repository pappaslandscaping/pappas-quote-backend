const { createHash } = require('node:crypto');
const { extractAccessToken, queryHomeWorksGraphql } = require('../services/homeworks/client');
const cents = value => {
  if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) throw new Error('Missing or invalid HomeWorks financial amount.');
  return Math.round(Number(value) * 100);
};
const normalize = value => String(value || '').trim().toLowerCase();
const normalizeName = value => normalize(value).normalize('NFKC').replace(/[^\p{L}\p{N}]/gu, '');
const phone = value => String(value || '').replace(/\D/g, '').replace(/^1(?=\d{10}$)/, '');
const street = value => normalize(value).replace(/\b(west|east|north|south|street|road|avenue|drive|boulevard|lane|court|place)\b/g,
  word => ({west:'w',east:'e',north:'n',south:'s',street:'st',road:'rd',avenue:'ave',drive:'dr',boulevard:'blvd',lane:'ln',court:'ct',place:'pl'})[word])
  .replace(/[^a-z0-9]/g, '');
function sameAddress(customer, owner) {
  const localStreet = street(customer.street);
  const sourceStreet = street(owner.address?.street1);
  const localZip = String(customer.postal_code || '').slice(0, 5);
  const sourceZip = String(owner.address?.zip || '').slice(0, 5);
  return !!localStreet && localStreet === sourceStreet && localZip.length === 5 && localZip === sourceZip;
}
const directoryCache = new Map();
const DIRECTORY_LIMIT = 10000;
const OPEN_INVOICE_LIMIT = 20000;
const DIRECTORY_QUERY = `query CustomerBillingDirectory {
  customers(where: { isDeleted: false }, take: 10000) { id number fullName email phone cell outstanding pastDue credit }
}`;
const billedStatuses = new Set(['pending', 'partially_paid', 'past_due', 'sent', 'viewed', 'overdue']);
function isBilledInvoice(invoice) {
  return invoice.is_sent === true && !invoice.is_archived && billedStatuses.has(normalize(invoice.status));
}
function isPastDueInvoice(invoice) {
  return ['past_due', 'overdue'].includes(normalize(invoice.status)) || Number(invoice.days_past_due || invoice.daysPastDue || 0) > 0;
}
function summarizeBilling(invoices) {
  const billed = invoices.filter(isBilledInvoice);
  const remaining = invoice => Math.max(0, cents(invoice.total) - cents(invoice.amount_paid));
  const balance = billed.reduce((sum, invoice) => sum + remaining(invoice), 0);
  const pastDue = billed.filter(isPastDueInvoice).reduce((sum, invoice) => sum + remaining(invoice), 0);
  return { balance: balance / 100, pastDue: pastDue / 100, current: (balance - pastDue) / 100 };
}
async function billingQuery({ getCopilotToken, fetchImpl = fetch, accessToken }) {
  const tokenInfo = accessToken || process.env.HOMEWORKS_API_TOKEN || process.env.HOMEWORKS_ACCESS_TOKEN
    || (await getCopilotToken())?.cookieHeader;
  const token = extractAccessToken(tokenInfo);
  if (!token) throw new Error('Connect HomeWorks to load this account.');
  return {
    key: createHash('sha256').update(token).digest('hex'), fetchImpl,
    query: (operationName, query, variables) => queryHomeWorksGraphql({ accessToken: token, fetchImpl, operationName, query, variables }),
  };
}
async function loadDirectory(context, fresh = false) {
  const cached = directoryCache.get(context.key);
  if (!fresh && cached && cached.fetchImpl === context.fetchImpl && cached.expires > Date.now()) return cached.customers;
  const result = await context.query('CustomerBillingDirectory', DIRECTORY_QUERY);
  if (!Array.isArray(result.customers) || result.customers.length >= DIRECTORY_LIMIT) throw new Error('Could not verify the complete HomeWorks customer directory.');
  if (directoryCache.size >= 4) directoryCache.clear();
  directoryCache.set(context.key, { customers: result.customers, fetchImpl: context.fetchImpl, expires: Date.now() + 60000 });
  return result.customers;
}
function matchHomeworksCustomer(customer, directory) {
  const name = normalizeName(customer.name || [customer.first_name, customer.last_name].filter(Boolean).join(' '));
  const email = normalize(customer.email);
  if (customer.copilot_customer_id) {
    const direct = directory.filter(item => item.id === Number(customer.copilot_customer_id));
    if (direct.length === 1) return direct[0];
    throw new Error('The linked HomeWorks customer is unavailable.');
  }
  const nameMatches = name ? directory.filter(item => normalizeName(item.fullName) === name) : [];
  const emailMatches = email ? directory.filter(item => normalize(item.email) === email) : [];
  const numbers = [phone(customer.phone), phone(customer.mobile)].filter(value => value.length >= 10);
  const phoneMatches = numbers.length ? directory.filter(item => [phone(item.phone),phone(item.cell)].some(value => numbers.includes(value))) : [];
  // Name + complete physical address, or phone + complete physical address,
  // can resolve stale email addresses and harmless imported name differences.
  const addressed = directory.filter(item => sameAddress(customer, item) && (nameMatches.includes(item) || phoneMatches.includes(item)));
  if (addressed.length === 1) return addressed[0];
  let candidates = nameMatches.length ? nameMatches : emailMatches;
  if (nameMatches.length && emailMatches.length) {
    candidates = nameMatches.filter(item => emailMatches.includes(item));
    if (!candidates.length) throw new Error('Customer name and email identify different HomeWorks accounts.');
  }
  const number = String(customer.customer_number || '').trim();
  // Legacy imports sometimes store a display number, sometimes a canonical ID.
  // Never trust either without corroborating the name or email.
  const numbered = number ? candidates.filter(item => String(item.number || '').trim() === number || String(item.id) === number) : [];
  if (numbered.length === 1) return numbered[0];
  if (candidates.length > 1 && numbers.length) candidates = candidates.filter(item => [phone(item.phone), phone(item.cell)].some(value => numbers.includes(value)));
  if (candidates.length !== 1) throw new Error(candidates.length ? 'Multiple HomeWorks accounts match this record.' : 'No verified HomeWorks account matches this record.');
  return candidates[0];
}
async function enrichMatchingAddresses(customers, directory, context) {
  const ids = new Set();
  for (const customer of customers) {
    if (!customer.street || String(customer.postal_code || '').length < 5 || customer.copilot_customer_id) continue;
    const name = normalizeName(customer.name || [customer.first_name, customer.last_name].filter(Boolean).join(' '));
    const named = directory.filter(owner => name && normalizeName(owner.fullName) === name);
    let needsReview = named.length > 1;
    try { matchHomeworksCustomer(customer, directory); } catch (_error) { needsReview = true; }
    if (!needsReview) continue;
    const email = normalize(customer.email);
    const numbers = [phone(customer.phone), phone(customer.mobile)].filter(value => value.length >= 10);
    directory.filter(owner => named.includes(owner) || (email && normalize(owner.email) === email)
      || [phone(owner.phone),phone(owner.cell)].some(value => numbers.includes(value))).forEach(owner => { if (!owner.address) ids.add(owner.id); });
  }
  const all = [...ids];
  for (let offset = 0; offset < all.length; offset += 100) {
    const result = await context.query('CustomerBillingIdentityAddresses', `query CustomerBillingIdentityAddresses($ids: [SafeInt!]!) {
      customers(where: { isDeleted: false, id: { in: $ids } }, take: 100) { id address { street1 zip } }
    }`, { ids: all.slice(offset, offset + 100) });
    if (!Array.isArray(result.customers) || result.customers.length !== all.slice(offset,offset+100).length) throw new Error('Could not verify billing identity addresses.');
    for (const item of result.customers) { const owner = directory.find(owner => owner.id === item.id); if (owner) owner.address = item.address; }
  }
}
async function loadHomeworksCustomerBilling({ customer, getCopilotToken, fetchImpl = fetch }) {
  const context = await billingQuery({ getCopilotToken, fetchImpl });
  const directory = await loadDirectory(context);
  await enrichMatchingAddresses([customer], directory, context);
  const matched = matchHomeworksCustomer(customer, directory);
  const result = await context.query('CustomerBillingRecords',
    'query CustomerBillingRecords($customerId: SafeInt!) { customer(customerId: $customerId) { id outstanding credit } invoices(where: { customerId: { equals: $customerId }, isArchived: false, deletedState: { equals: ACTIVE } }, take: 1000, orderBy: [{ date: desc }]) { id number date status subtotal tax total paidAmount lateFee processingFee isSent isArchived dueDate daysPastDue lineItems { name description } } payments(where: { customerId: { equals: $customerId }, isDeleted: false }, take: 1000, orderBy: [{ date: desc }]) { id invoiceId date invoiceAmount totalAmount creditAmount isRefund methodDisplayName notes details } }',
    { customerId: matched.id });
  const owner = result.customer;
  if (!owner || owner.id !== matched.id || !Array.isArray(result.invoices) || !Array.isArray(result.payments)) throw new Error('HomeWorks returned incomplete account records.');
  if (result.invoices.length === 1000 || result.payments.length === 1000) throw new Error('Account history exceeds the safe statement limit.');
  const invoices = result.invoices.map(invoice => ({
    id: null, homeworks_id: invoice.id, external_invoice_id: String(invoice.id), external_source: 'copilotcrm',
    invoice_number: String(invoice.number ?? invoice.id), invoice_date: invoice.date, created_at: invoice.date,
    status: normalize(invoice.status), subtotal: invoice.subtotal, tax_amount: invoice.tax, total: invoice.total,
    amount_paid: invoice.paidAmount, is_sent: invoice.isSent, is_archived: invoice.isArchived, due_date: invoice.dueDate,
    days_past_due: invoice.daysPastDue, is_past_due: invoice.status === 'PAST_DUE' || Number(invoice.daysPastDue) > 0,
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
  return { invoices, payments, ...summary, availableCredit: cents(owner.credit) / 100, source: 'homeworks', sourceAsOf: new Date().toISOString(), homeworksCustomerId: owner.id };
}
async function auditHomeworksCustomerBalances({ customers, getCopilotToken, fetchImpl = fetch, accessToken }) {
  const context = await billingQuery({ getCopilotToken, fetchImpl, accessToken });
  const directory = await loadDirectory(context, true);
  await enrichMatchingAddresses(customers, directory, context);
  const records = await context.query('CompanyOpenBillingRecords', `query CompanyOpenBillingRecords {
    invoices(take: 20000, where: { isArchived: false, deletedState: { equals: ACTIVE }, status: { in: [PENDING, PARTIALLY_PAID, PAST_DUE] } }) {
      id customerId number status total paidAmount isSent isArchived daysPastDue
    }
  }`);
  if (!Array.isArray(records.invoices) || records.invoices.length >= OPEN_INVOICE_LIMIT) throw new Error('Could not verify the complete HomeWorks open invoice history.');
  const grouped = new Map();
  for (const invoice of records.invoices) {
    const invoices = grouped.get(invoice.customerId) || [];
    invoices.push({ total: invoice.total, amount_paid: invoice.paidAmount, status: invoice.status, is_sent: invoice.isSent, is_archived: invoice.isArchived, days_past_due: invoice.daysPastDue });
    grouped.set(invoice.customerId, invoices);
  }
  const standing = new Map(directory.map(owner => {
    const summary = summarizeBilling(grouped.get(owner.id) || []);
    return [owner.id, { ...summary, availableCredit: cents(owner.credit) / 100, verified: cents(summary.balance) === cents(owner.outstanding) }];
  }));
  const accounts = customers.map(customer => {
    try {
      const owner = matchHomeworksCustomer(customer, directory);
      const summary = standing.get(owner.id);
      return { id: customer.id, name: customer.name || [customer.first_name, customer.last_name].filter(Boolean).join(' '), homeworksCustomerId: owner.id,
        ...summary, reviewReason: summary.verified ? null : 'Sent invoices do not reconcile with the HomeWorks account balance.' };
    } catch (error) {
      return { id: customer.id, name: customer.name || [customer.first_name, customer.last_name].filter(Boolean).join(' '), balance: null, verified: false, reviewReason: error.message,
        reviewIdentity: { number: customer.customer_number || '', email: customer.email || '', phone: customer.mobile || customer.phone || '' } };
    }
  });
  const verifiedSource = [...standing.values()].filter(item => item.verified);
  const represented = new Set(accounts.filter(account => account.verified).map(account => account.homeworksCustomerId));
  const missingBilledAccounts = directory.filter(owner => standing.get(owner.id).verified && standing.get(owner.id).balance > 0 && !represented.has(owner.id))
    .map(owner => ({ homeworksCustomerId: owner.id, name: owner.fullName, number: owner.number, ...standing.get(owner.id) }));
  const billed = records.invoices.filter(invoice => invoice.isSent === true && standing.has(invoice.customerId));
  return { source: 'homeworks', sourceAsOf: new Date().toISOString(), accounts, missingBilledAccounts,
    summary: { yarddeskCustomers: customers.length, verified: accounts.filter(item => item.verified).length, needsReview: accounts.filter(item => !item.verified).length,
      homeworksCustomers: directory.length, homeworksVerified: verifiedSource.length, homeworksNeedsReview: directory.length - verifiedSource.length,
      outstanding: verifiedSource.reduce((sum, item) => sum + cents(item.balance), 0) / 100,
      pastDue: verifiedSource.reduce((sum, item) => sum + cents(item.pastDue), 0) / 100,
      outstandingInvoices: billed.length, pastDueInvoices: billed.filter(isPastDueInvoice).length,
      customersPastDue: verifiedSource.filter(item => item.pastDue > 0).length } };
}
module.exports = { loadHomeworksCustomerBilling, auditHomeworksCustomerBalances, matchHomeworksCustomer, isBilledInvoice, isPastDueInvoice, summarizeBilling };
