'use strict';
const records = require('./records');
const { mirrorInvoices } = require('./invoice-cache');
async function index(context) {
  return records.loadInvoices(context, {}, { details: false, allowUnverifiedCustomers: true });
}
async function list(context, params = {}, mailState = (row) => row.mail_state) {
  let rows = await index(context);
  if (params.customer_id) {
    const { rows: local } = await context.pool.query('SELECT * FROM customers WHERE id=$1', [
      params.customer_id,
    ]);
    if (!local[0]) throw new Error('Customer not found.');
    const owner = await records.uniqueCustomer(context, {
      customerId: local[0].homeworks_customer_id || local[0].copilot_customer_id,
      email: local[0].email,
    });
    rows = rows.filter((row) => Number(row.copilot_customer_id) === owner.id);
  }
  const term = String(params.search || '')
    .trim()
    .toLowerCase();
  if (term)
    rows = rows.filter((row) =>
      [row.customer_name, row.invoice_number, row.customer_email].some((value) =>
        String(value || '')
          .toLowerCase()
          .includes(term),
      ),
    );
  if (params.status && params.status !== 'all') rows = rows.filter((row) => row.status === params.status);
  rows.sort(
    (a, b) =>
      String(b.invoice_date).localeCompare(String(a.invoice_date)) ||
      Number(b.external_invoice_id) - Number(a.external_invoice_id),
  );
  const total = rows.length,
    offset = Math.max(0, Number(params.offset) || 0),
    limit = Math.min(25000, Math.max(1, Number(params.limit) || 25000));
  const mirrored = await mirrorInvoices(context.pool, rows.slice(offset, offset + limit), { details: false });
  let selected = mirrored.map((row) => ({ ...row, mail_state: mailState(row) }));
  if (params.mailed === 'mailed') selected = selected.filter((row) => row.mailed_at);
  if (params.mailed === 'unmailed')
    selected = selected.filter((row) => !row.mailed_at && !row.sent_snail_mail);
  return {
    success: true,
    source: records.SOURCE,
    as_of: new Date().toISOString(),
    invoices: selected,
    total,
  };
}
async function detail(context, localId) {
  const local = await context.pool.query('SELECT * FROM invoices WHERE id=$1', [localId]);
  if (!local.rows[0]?.external_invoice_id)
    throw new Error('Link this invoice to its Homeworks record before using billing actions.');
  const invoice = await records.invoiceSnapshot(context, local.rows[0].external_invoice_id);
  const rows = await mirrorInvoices(context.pool, [invoice]);
  const payments = await records.loadPayments(context, {
    invoiceId: { equals: Number(invoice.external_invoice_id) },
  });
  return {
    success: true,
    source: records.SOURCE,
    invoice: rows[0],
    payments: payments.map((row) => ({
      ...row,
      payment_record_id: row.id,
      status: row.is_refund ? 'refunded' : 'paid',
    })),
    homeworksReadOnlyPayments: true,
  };
}
async function stats(context) {
  const invoices = await index(context),
    today = records.dateKey(),
    month = today.slice(0, 7) + '-01';
  const [standing, collected, lifetime] = await Promise.all([
    records.accountStanding(context),
    records.collectedRevenue(context, month, today),
    records.collectedRevenue(context, '1900-01-01', today),
  ]);
  const stats = {
    total: invoices.length,
    draft: 0,
    pending: 0,
    partial: 0,
    sent: 0,
    paid: 0,
    overdue: 0,
    void: 0,
    outstanding: 0,
    overdueAmount: 0,
    paidThisMonth: collected.total,
    totalRevenue: lifetime.total,
    account_standing: standing,
  };
  let outstanding = 0,
    overdue = 0;
  for (const row of invoices) {
    stats[row.status] = (stats[row.status] || 0) + 1;
    if (row.homeworks.isSent && records.OPEN.has(row.homeworks.status)) {
      const balance = Math.max(
        0,
        records.cents(row.homeworks.total) - records.cents(row.homeworks.paidAmount),
      );
      outstanding += balance;
      if (row.homeworks.daysPastDue > 0) overdue += balance;
    }
  }
  stats.outstanding = outstanding / 100;
  stats.overdueAmount = overdue / 100;
  return { success: true, source: records.SOURCE, stats };
}
async function payments(context, params = {}) {
  const where = {};
  if (/^\d{4}$/.test(String(params.year || ''))) {
    const year = Number(params.year),
      month = Number(params.month) || 0;
    where.date = {
      gte: month ? `${year}-${String(month).padStart(2, '0')}-01` : `${year}-01-01`,
      lt: month ? new Date(Date.UTC(year, month, 1)).toISOString().slice(0, 10) : `${year + 1}-01-01`,
    };
  }
  let rows = await records.loadPayments(context, where);
  const term = String(params.search || '')
    .trim()
    .toLowerCase();
  if (term)
    rows = rows.filter((row) =>
      [row.customer_name, row.extracted_invoice_number, row.notes].some((value) =>
        String(value || '')
          .toLowerCase()
          .includes(term),
      ),
    );
  if (params.linked === 'linked') rows = rows.filter((row) => row.homeworks.invoiceId);
  if (params.linked === 'unlinked') rows = rows.filter((row) => !row.homeworks.invoiceId);
  rows.sort((a, b) => String(b.paid_at).localeCompare(String(a.paid_at)) || b.id - a.id);
  const months = new Map();
  let total = 0;
  for (const row of rows) {
    if (row.is_refund) continue;
    total += records.cents(row.amount);
    const key = row.paid_at.slice(0, 7),
      value = months.get(key) || { month: key, count: 0, cents: 0 };
    value.count++;
    value.cents += records.cents(row.amount);
    months.set(key, value);
  }
  const offset = Math.max(0, Number(params.offset) || 0),
    limit = Math.min(1000, Math.max(1, Number(params.limit) || 200));
  return {
    success: true,
    source: records.SOURCE,
    payments: rows
      .slice(offset, offset + limit)
      .map((row) => ({
        ...row,
        payment_record_id: row.id,
        payment_id: row.id,
        invoice_id: null,
        external_invoice_id: row.homeworks.invoiceId,
        display_invoice_number: row.extracted_invoice_number,
        invoice_number: row.extracted_invoice_number,
        payment_date: row.paid_at,
        status: row.is_refund ? 'refunded' : 'paid',
        read_only: true,
      })),
    total: rows.length,
    totalReceived: total / 100,
    monthly: [...months.values()].map((row) => ({
      month: row.month,
      count: row.count,
      total: row.cents / 100,
    })),
  };
}
module.exports = { list, detail, stats, payments };
