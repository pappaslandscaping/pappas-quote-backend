'use strict';

const { queryHomeWorksGraphql } = require('./client');
const SOURCE = 'official_homeworks_graphql';
const OPEN = new Set(['PENDING', 'PARTIALLY_PAID', 'PAST_DUE']);
const cents = (value) => {
  if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value)))
    throw new Error('Homeworks returned an invalid amount.');
  return Math.round(Number(value) * 100);
};
const money = (value) => cents(value) / 100;
const id = (value) => {
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result <= 0) throw new Error('A verified Homeworks ID is required.');
  return result;
};
const dateKey = (value = new Date()) =>
  new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(value);
function call(context, operationName, query, variables) {
  return (context.queryImpl || queryHomeWorksGraphql)({ ...context, operationName, query, variables });
}

// A capped or repeated page is an error, never a successful empty/partial account.
async function listAll(
  context,
  { entity, filterType, where = {}, fields, orderBy = '[{ id: asc }]', pageSize = 250, maxRows = 50000 },
) {
  if (!/^[A-Za-z]+$/.test(entity) || !/^[A-Za-z]+Filter$/.test(filterType))
    throw new Error('Invalid Homeworks collection.');
  const rows = [],
    seen = new Set();
  for (let skip = 0; skip < maxRows; skip += pageSize) {
    const data = await call(
      context,
      'YardDeskRecords',
      `query YardDeskRecords($where: ${filterType}, $take: SafeInt!, $skip: SafeInt!) {
      ${entity}(where: $where, take: $take, skip: $skip, orderBy: ${orderBy}) { ${fields} }
    }`,
      { where, take: pageSize, skip },
    );
    const page = data[entity];
    if (!Array.isArray(page)) throw new Error(`Homeworks ${entity} are unavailable.`);
    for (const row of page) {
      if (seen.has(String(row.id)))
        throw new Error(`Homeworks ${entity} changed while loading. Refresh before continuing.`);
      seen.add(String(row.id));
      rows.push(row);
    }
    if (page.length < pageSize) return rows;
  }
  throw new Error(`Homeworks ${entity} exceed the verified result limit. Narrow the date range.`);
}

const INVOICE_FIELDS = `id number customerId propertyId date status title notes terms subtotal discountAmount tax total paidAmount
 isSent sentEmail sentSms sentSnailMail sentCopyLink sentAt isArchived deletedState daysPastDue dueDate lateFee processingFee createdAt updatedAt
 lineItems(orderBy: [{lineItemOrder: asc}]) { id name description date price quantity subtotal subtotalWithTax taxPrice1 taxPrice2 lineItemOrder propertyId }`;
const CUSTOMER_FIELDS =
  'id fullName firstName lastName email phone cell portalKey outstanding pastDue credit address { street1 street2 city state zip country }';
function mapInvoice(invoice, customer = {}) {
  const total = money(invoice.total),
    paid = money(invoice.paidAmount);
  const billed = invoice.isSent === true;
  const status =
    invoice.status === 'PAID'
      ? 'paid'
      : invoice.status === 'WRITE_OFF'
        ? 'void'
        : !billed || invoice.status === 'DRAFT'
          ? 'draft'
          : invoice.status === 'PARTIALLY_PAID'
            ? 'partial'
            : invoice.status === 'PAST_DUE' || invoice.daysPastDue > 0
              ? 'overdue'
              : 'sent';
  return {
    external_source: 'copilotcrm',
    external_invoice_id: String(invoice.id),
    copilot_customer_id: String(invoice.customerId),
    invoice_number: String(invoice.number ?? invoice.id),
    invoice_date: invoice.date,
    created_at: invoice.createdAt || invoice.date,
    updated_at: invoice.updatedAt,
    due_date: invoice.dueDate,
    title: invoice.title,
    status,
    raw_status: invoice.status,
    subtotal: money(invoice.subtotal),
    tax_amount: money(invoice.tax),
    total,
    amount_paid: paid,
    total_due: Math.max(0, cents(invoice.total) - cents(invoice.paidAmount)) / 100,
    customer_name: customer.fullName || '',
    customer_email: customer.email || '',
    customer_phone: customer.cell || customer.phone || '',
    customer_address: [
      customer.address?.street1,
      customer.address?.street2,
      customer.address?.city,
      customer.address?.state,
      customer.address?.zip,
    ]
      .filter(Boolean)
      .join(', '),
    notes: invoice.notes || '',
    terms: invoice.terms || '',
    is_sent: billed,
    sent_status: billed ? 'sent' : 'unsent',
    sent_at: invoice.sentAt,
    is_archived: invoice.isArchived,
    deleted_state: invoice.deletedState,
    days_past_due: invoice.daysPastDue,
    sent_email: invoice.sentEmail,
    sent_sms: invoice.sentSms,
    sent_snail_mail: invoice.sentSnailMail,
    line_items: (invoice.lineItems || []).map((line) => ({
      id: line.id,
      name: line.name,
      description: line.description,
      service_date: line.date,
      quantity: Number(line.quantity),
      rate: money(line.price),
      subtotal: money(line.subtotal),
      amount: money(line.subtotalWithTax),
      total: money(line.subtotalWithTax),
      tax_amount: (cents(line.taxPrice1) + cents(line.taxPrice2)) / 100,
    })),
    metadata: {
      homeworks_financials: true,
      source: SOURCE,
      copilot_customer_id: String(invoice.customerId),
      property_id: invoice.propertyId,
      is_sent: billed,
      sent_email: invoice.sentEmail,
      sent_sms: invoice.sentSms,
      sent_snail_mail: invoice.sentSnailMail,
      invoice_date: invoice.date,
      late_fee: invoice.lateFee,
      processing_fee: invoice.processingFee,
      discount_amount: invoice.discountAmount,
      customer_outstanding_balance: customer.outstanding,
      customer_past_due_balance: customer.pastDue,
      customer_credit_available: customer.credit,
    },
  };
}
async function loadInvoices(context, where = {}, { details = true } = {}) {
  const fields = details ? INVOICE_FIELDS : INVOICE_FIELDS.slice(0, INVOICE_FIELDS.indexOf(' lineItems'));
  const invoices = await listAll(context, {
    entity: 'invoices',
    filterType: 'InvoiceFilter',
    where: { isArchived: false, deletedState: { equals: 'ACTIVE' }, ...where },
    fields,
    pageSize: details ? 100 : 500,
  });
  const customers = new Map();
  const ids = [...new Set(invoices.map((invoice) => invoice.customerId))];
  for (let start = 0; start < ids.length; start += 200) {
    const data = await call(
      context,
      'YardDeskInvoiceOwners',
      `query YardDeskInvoiceOwners($ids: [SafeInt!]!) { customers(where: { id: { in: $ids } }, take: 200) { ${CUSTOMER_FIELDS} } }`,
      { ids: ids.slice(start, start + 200) },
    );
    for (const customer of data.customers || []) customers.set(customer.id, customer);
  }
  return invoices.map((invoice) => {
    const owner = customers.get(invoice.customerId);
    if (!owner) throw new Error(`Homeworks invoice ${invoice.number} has no verified customer.`);
    return { ...mapInvoice(invoice, owner), homeworks: invoice, owner };
  });
}

async function invoiceSnapshot(context, invoiceId) {
  const rows = await loadInvoices(context, { id: { equals: id(invoiceId) } });
  if (rows.length !== 1) throw new Error('The Homeworks invoice is unavailable.');
  return rows[0];
}
async function markInvoicesMailed(context, invoiceIds) {
  const ids = [...new Set(invoiceIds.map(id))];
  if (!ids.length) throw new Error('Select invoices to confirm as mailed.');
  const data = await call(
    context,
    'YardDeskMarkMailed',
    `mutation YardDeskMarkMailed($ids: [SafeInt!]!) {
    markInvoicesAsSent(invoiceIds: $ids, sendMethod: SNAIL_MAIL) { id }
  }`,
    { ids },
  );
  if (data.markInvoicesAsSent?.length !== ids.length)
    throw new Error('Homeworks did not confirm all mailed invoices. Refresh before retrying.');
  const check = await call(
    context,
    'YardDeskVerifyMailed',
    `query YardDeskVerifyMailed($ids: [SafeInt!]!) {
    invoices(where: { id: { in: $ids } }, take: 1000) { id isSent sentSnailMail }
  }`,
    { ids },
  );
  const verified = new Map((check.invoices || []).map((row) => [row.id, row]));
  return ids.map((invoiceId) => ({
    invoiceId,
    success: verified.get(invoiceId)?.isSent === true && verified.get(invoiceId)?.sentSnailMail === true,
  }));
}

async function loadPayments(context, where = {}) {
  const rows = await listAll(context, {
    entity: 'payments',
    filterType: 'PaymentFilter',
    where: { isDeleted: false, isExpensePayment: false, ...where },
    pageSize: 200,
    fields: `id customerId date method methodDisplayName invoiceId totalAmount invoiceAmount creditAmount tipAmount processFeeCharged paidAmount isRefund notes details
      customer { id fullName } invoice { id number date total tax subtotal discountAmount lateFee processingFee }`,
  });
  return rows.map((row) => ({
    id: row.id,
    paid_at: row.date,
    source_date_raw: row.date,
    customer_name: row.customer?.fullName || '',
    amount: (row.isRefund ? -1 : 1) * money(row.totalAmount),
    tip_amount: money(row.tipAmount),
    method: row.methodDisplayName || row.method,
    details: row.details,
    notes: row.notes,
    is_refund: row.isRefund,
    extracted_invoice_number: row.invoice?.number == null ? null : String(row.invoice.number),
    extracted_invoice_date: row.invoice?.date || null,
    external_source: 'copilotcrm',
    external_payment_key: `homeworks:${row.id}`,
    external_metadata: {
      source: SOURCE,
      homeworks_payment_id: row.id,
      homeworks_invoice_id: row.invoiceId,
      homeworks_customer_id: row.customerId,
      invoice_amount: row.invoiceAmount,
      credit_amount: row.creditAmount,
      processing_fee: row.processFeeCharged,
      is_refund: row.isRefund,
    },
    homeworks: row,
  }));
}

function invoiceAging(rows) {
  const buckets = Object.fromEntries(
    ['within_30', '31_60', '61_90', '90_plus'].map((key) => [key, { count: 0, total: 0, invoices: [] }]),
  );
  const totals = {};
  for (const row of rows) {
    const remote = row.homeworks;
    if (!remote.isSent || !OPEN.has(remote.status)) continue;
    const balanceCents = Math.max(0, cents(remote.total) - cents(remote.paidAmount));
    if (!balanceCents) continue;
    const days = Math.max(0, Number(remote.daysPastDue) || 0);
    const key = days <= 30 ? 'within_30' : days <= 60 ? '31_60' : days <= 90 ? '61_90' : '90_plus';
    totals[key] = (totals[key] || 0) + balanceCents;
    buckets[key].count++;
    buckets[key].total = totals[key] / 100;
    buckets[key].invoices.push({
      external_invoice_id: row.external_invoice_id,
      invoice_number: row.invoice_number,
      customer_name: row.customer_name,
      balance: balanceCents / 100,
      days_overdue: days,
      status: row.status,
    });
  }
  return { success: true, source: SOURCE, as_of: new Date().toISOString(), buckets };
}

async function accountStanding(context) {
  const rows = await call(
    context,
    'YardDeskAccountStanding',
    `query YardDeskAccountStanding {
    invoiceReport(where: {isArchived: false, deletedState: {equals: ACTIVE}}) { status isSent _count {id} }
    customerReport(where: {isDeleted: false}) { _count {id} }
    customers(where: {isDeleted:false},take:10000) {id credit}
  }`,
  );
  if (!Array.isArray(rows.invoiceReport) || !Array.isArray(rows.customers) || rows.customers.length >= 10000)
    throw new Error('Homeworks account standing is incomplete.');
  const standing = {
    total: 0,
    outstanding: 0,
    paid: 0,
    past_due: 0,
    credit: rows.customers.filter((row) => cents(row.credit) > 0).length,
  };
  for (const row of rows.invoiceReport) {
    const count = row._count.id;
    standing.total += count;
    if (row.status === 'PAID') standing.paid += count;
    if (row.isSent && OPEN.has(row.status)) standing.outstanding += count;
    if (row.isSent && row.status === 'PAST_DUE') standing.past_due += count;
  }
  return { ...standing, source: SOURCE, as_of: new Date().toISOString() };
}

async function collectedRevenue(context, startDate, endDate) {
  const data = await call(
    context,
    'YardDeskCollectedRevenue',
    `query YardDeskCollectedRevenue($where: PaymentFilter) {
    paymentReport(where: $where) { isRefund _sum {totalAmount tipAmount} }
  }`,
    {
      where: {
        isDeleted: false,
        isExpensePayment: false,
        isRefund: false,
        date: { gte: startDate, lte: endDate },
      },
    },
  );
  let collected = 0;
  for (const row of data.paymentReport || [])
    collected += (row.isRefund ? -1 : 1) * cents(row._sum.totalAmount || '0');
  return {
    source: SOURCE,
    as_of: new Date().toISOString(),
    period_start: startDate,
    period_end: endDate,
    total: collected / 100,
  };
}

async function uniqueCustomer(context, { customerId, name, phone, email } = {}) {
  const normalizePhone = (value) =>
    String(value || '')
      .replace(/\D/g, '')
      .replace(/^1(?=\d{10}$)/, '');
  const where = { isDeleted: false };
  if (customerId) where.id = { equals: id(customerId) };
  else if (name) where.fullName = { contains: name };
  else if (email) where.email = { equals: email };
  const rows = await listAll(context, {
    entity: 'customers',
    filterType: 'CustomerFilter',
    where,
    fields:
      CUSTOMER_FIELDS +
      ' description status tags emailSendability smsSendability customerType{name} createdAt',
  });
  const matches = phone
    ? rows.filter((row) =>
        [row.phone, row.cell].some((value) => normalizePhone(value) === normalizePhone(phone)),
      )
    : rows;
  if (matches.length !== 1)
    throw new Error(
      matches.length
        ? 'More than one Homeworks customer matches. Select the correct account.'
        : 'No verified Homeworks customer matches.',
    );
  return matches[0];
}

module.exports = {
  SOURCE,
  OPEN,
  cents,
  money,
  id,
  dateKey,
  call,
  listAll,
  INVOICE_FIELDS,
  CUSTOMER_FIELDS,
  mapInvoice,
  loadInvoices,
  invoiceSnapshot,
  markInvoicesMailed,
  loadPayments,
  invoiceAging,
  accountStanding,
  collectedRevenue,
  uniqueCustomer,
};
