// Read-only print snapshots. HomeWorks totals already include late/processing fees.
const { resolveMailAccountSummary } = require('./mail-account-summary');
function publicInvoiceText(value) {
  return String(value || '')
    .replace(/<(script|style|iframe|object)\b[^>]*>[\s\S]*?<\/\1>/gi, '')
    .replace(/<br\s*\/?>|<\/(?:p|div|li|tr|h[1-6])>/gi, '\n')
    .replace(/<[^>]*>/g, ' ')
    .replace(/&(?:nbsp|amp|lt|gt|quot|apos);/gi, entity => ({'&nbsp;':' ', '&amp;':'&', '&lt;':'<', '&gt;':'>', '&quot;':'"', '&apos;':"'"}[entity.toLowerCase()]))
    .replace(/&#(x[0-9a-f]+|[0-9]+);/gi, (entity, code) => {
      const n = code[0].toLowerCase() === 'x' ? parseInt(code.slice(1),16) : Number(code);
      return n > 0 && n <= 0x10ffff ? String.fromCodePoint(n) : '';
    })
    .split('\n').map(line => line.replace(/[\t ]+/g, ' ').trim()).filter(Boolean).join('\n');
}
async function refreshHomeworksInvoiceText(row, query) {
  const invoiceId = Number(row.external_invoice_id);
  if (!Number.isSafeInteger(invoiceId) || invoiceId <= 0) return row;
  const result = await query('InvoicePublicText', 'query InvoicePublicText($id: SafeInt!) { invoice(invoiceId: $id) { id notes terms } }', { id: invoiceId });
  const invoice = result.data?.invoice;
  if (invoice?.id !== invoiceId) throw new Error('HomeWorks invoice notes could not be verified.');
  return { ...row, notes: publicInvoiceText(invoice.notes), terms: publicInvoiceText(invoice.terms) };
}
function resolveHomeworksMailFinancials({ subtotal, tax_amount, total, amount_paid, metadata }, parseMoney, roundMoney) {
  const account = resolveMailAccountSummary({
    invoiceTotal: total, amountPaid: amount_paid, metadata, parseMoney, roundMoney,
  });
  return {
    subtotal: parseMoney(subtotal) ?? 0, tax_amount: parseMoney(tax_amount) ?? 0,
    total: parseMoney(total) ?? 0, paymentCredit: account.paymentCredit,
    priorBalance: account.priorBalance, invoiceBalance: account.thisInvoice,
    totalDueOnAccount: account.totalDueOnAccount,
  };
}
function applyHomeworksMailSnapshot(row, invoice, customer) {
  const metadata = typeof row.external_metadata === 'string'
    ? JSON.parse(row.external_metadata) : (row.external_metadata || row.metadata || {});
  return {
    ...row,
    invoice_number: String(invoice.number ?? row.invoice_number),
    subtotal: invoice.subtotal,
    tax_amount: invoice.tax,
    total: invoice.total,
    amount_paid: invoice.paidAmount,
    notes: invoice.notes === undefined ? row.notes : publicInvoiceText(invoice.notes),
    terms: invoice.terms === undefined ? row.terms : publicInvoiceText(invoice.terms),
    line_items: invoice.lineItems.slice().sort((a, b) => a.lineItemOrder - b.lineItemOrder).map(item => ({
      name: item.name, description: item.description, service_date: item.date,
      quantity: item.quantity, rate: item.price, subtotal: item.subtotal,
      total: item.subtotalWithTax,
      tax_amount: Number(item.taxPrice1) + Number(item.taxPrice2),
    })),
    external_metadata: {
      ...metadata,
      homeworks_financials: true,
      invoice_date: invoice.date,
      late_fee: invoice.lateFee,
      processing_fee: invoice.processingFee,
      discount_amount: invoice.discountAmount,
      copilot_customer_id: String(invoice.customerId),
      customer_outstanding_balance: customer.outstanding,
      customer_past_due_balance: customer.pastDue,
    },
  };
}

async function refreshHomeworksMailInvoices(rows, query, { skipDeleted = false } = {}) {
  const ids = [...new Set(rows.map(row => Number(row.external_invoice_id)).filter(Number.isSafeInteger))];
  const invoices = [];
  for (let offset = 0; offset < ids.length; offset += 100) {
    const result = await query('MailInvoiceSnapshots', `query MailInvoiceSnapshots($ids: [SafeInt!]!) {
      invoices(where: { id: { in: $ids } }, take: 100) {
        id number customerId date status subtotal tax total paidAmount lateFee processingFee discountAmount notes terms
        lineItems(orderBy: [{ lineItemOrder: asc }]) {
          name description date price quantity subtotal subtotalWithTax taxPrice1 taxPrice2 lineItemOrder
        }
      }
    }`, { ids: ids.slice(offset, offset + 100) });
    if (result.errors?.length || !Array.isArray(result.data?.invoices)) throw new Error('HomeWorks returned incomplete invoice print data.');
    invoices.push(...result.data.invoices);
  }
  // The active list omits deleted records. Verify deletion explicitly before
  // excluding a stale YardDesk row; unavailable invoices must still stop printing.
  const foundIds = new Set(invoices.map(invoice => invoice.id));
  const missingIds = ids.filter(id => !foundIds.has(id));
  const deletedIds = new Set();
  for (let offset = 0; offset < missingIds.length; offset += 100) {
    const result = await query('MailDeletedInvoices', `query MailDeletedInvoices($ids: [SafeInt!]!) {
      invoices(where: { id: { in: $ids }, deletedState: { equals: DELETED } }, take: 100) { id number deletedState }
    }`, { ids: missingIds.slice(offset, offset + 100) });
    if (result.errors?.length || !Array.isArray(result.data?.invoices)) throw new Error('HomeWorks invoice deletion status could not be verified.');
    for (const invoice of result.data.invoices) {
      if (invoice.deletedState === 'DELETED') deletedIds.add(invoice.id);
    }
  }
  const customers = [];
  const customerIds = [...new Set(invoices.map(invoice => invoice.customerId))];
  for (let offset = 0; offset < customerIds.length; offset += 100) {
    const result = await query('MailCustomerStanding', `query MailCustomerStanding($ids: [SafeInt!]!) {
      customers(where: { id: { in: $ids } }, take: 100) { id outstanding pastDue }
    }`, { ids: customerIds.slice(offset, offset + 100) });
    if (result.errors?.length || !Array.isArray(result.data?.customers)) throw new Error('HomeWorks returned incomplete customer print data.');
    customers.push(...result.data.customers);
  }
  return rows.map(row => {
    if (deletedIds.has(Number(row.external_invoice_id))) {
      const message = `Invoice #${row.invoice_number || row.id} was deleted in HomeWorks and cannot be printed.`;
      if (!skipDeleted) {
        const error = new Error(message);
        error.code = 'HOMEWORKS_INVOICE_DELETED';
        throw error;
      }
      return { ...row, homeworks_print_exclusion: 'Deleted in HomeWorks' };
    }
    const invoice = invoices.find(invoice => invoice.id === Number(row.external_invoice_id));
    const customer = customers.find(customer => customer.id === invoice?.customerId);
    if (!invoice || !customer) throw new Error(`HomeWorks print snapshot unavailable for invoice ${row.invoice_number || row.id}`);
    return applyHomeworksMailSnapshot({ ...row,
      status: invoice.status ? ({ PARTIALLY_PAID: 'partial', PAST_DUE: 'overdue', WRITE_OFF: 'void' }[invoice.status] || invoice.status.toLowerCase()) : row.status,
    }, invoice, customer);
  });
}

module.exports = { applyHomeworksMailSnapshot, refreshHomeworksMailInvoices, resolveHomeworksMailFinancials, publicInvoiceText, refreshHomeworksInvoiceText };
