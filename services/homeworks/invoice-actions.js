'use strict';
const records = require('./records');
const { mirrorInvoices } = require('./invoice-cache');
async function create(context, input, requestId) {
  if (!/^[a-zA-Z0-9-]{16,100}$/.test(String(requestId || '')))
    throw new Error('Refresh the invoice form before creating an invoice. A request ID is required.');
  const customer = input.homeworks_customer_id
    ? await records.uniqueCustomer(context, { customerId: input.homeworks_customer_id })
    : await records.uniqueCustomer(context, { email: input.customer_email });
  const lineItems = (input.line_items || []).map((line) => ({
    name: line.name || line.description,
    description: line.description || '',
    price: String(line.price ?? line.rate ?? line.unit_price),
    quantity: String(line.quantity ?? 1),
    taxPercent1: String(
      line.taxable === false
        ? 0
        : (line.tax_percent ?? (input.tax_rate != null ? Number(input.tax_rate) * 100 : 0)),
    ),
    ...(line.item_id ? { itemId: records.id(line.item_id) } : {}),
  }));
  if (
    !lineItems.length ||
    lineItems.some(
      (line) =>
        !line.name ||
        !Number.isFinite(Number(line.price)) ||
        !Number.isFinite(Number(line.quantity)) ||
        Number(line.quantity) <= 0 ||
        Number(line.price) < 0,
    )
  )
    throw new Error('Review invoice services, quantities, and prices.');
  const date = String(input.invoice_date || input.date || records.dateKey());
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('Choose a valid invoice date.');
  if (input.due_date && input.due_date !== date)
    throw new Error('Set payment terms in Homeworks before creating an invoice with a different due date.');
  let propertyId;
  if (input.property_id) {
    const { remote: property } = await require('./properties').resolve(context, input.property_id);
    if (property.customerId !== customer.id)
      throw new Error('The service property belongs to a different customer.');
    propertyId = property.id;
  }
  if (
    lineItems.some(
      (line) =>
        !Number.isFinite(Number(line.taxPercent1)) ||
        Number(line.taxPercent1) < 0 ||
        Number(line.taxPercent1) > 100,
    )
  )
    throw new Error('Review the invoice tax percentage.');
  const remote = {
    customerId: customer.id,
    ...(propertyId ? { propertyId } : {}),
    date,
    lineItems,
    ...(input.notes !== undefined ? { notes: input.notes } : {}),
    ...(input.terms !== undefined ? { terms: input.terms } : {}),
    title: input.title || '',
    discountPercent: String(input.discount_percent || 0),
  };
  const fingerprint = require('node:crypto')
    .createHash('sha256')
    .update(JSON.stringify(remote))
    .digest('hex');
  await context.pool.query(
    'CREATE TABLE IF NOT EXISTS homeworks_action_receipts(request_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,status TEXT NOT NULL,remote_id BIGINT,created_at TIMESTAMPTZ DEFAULT NOW())',
  );
  const claim = await context.pool.query(
    "INSERT INTO homeworks_action_receipts(request_id,fingerprint,status) VALUES($1,$2,'pending') ON CONFLICT DO NOTHING RETURNING request_id",
    [requestId, fingerprint],
  );
  let invoiceId;
  if (!claim.rows.length) {
    const previous = await context.pool.query('SELECT * FROM homeworks_action_receipts WHERE request_id=$1', [
      requestId,
    ]);
    const saved = previous.rows[0];
    if (saved?.fingerprint !== fingerprint)
      throw new Error('This invoice request changed. Open a new form and review it.');
    if (!saved.remote_id)
      throw new Error('The earlier invoice request needs verification in Homeworks. Do not submit it again.');
    invoiceId = saved.remote_id;
  } else {
    const result = await records.call(
      context,
      'YardDeskCreateInvoice',
      'mutation YardDeskCreateInvoice($input:CreateInvoiceInput!){createInvoice(input:$input){id}}',
      { input: remote },
    );
    invoiceId = records.id(result.createInvoice?.id);
    await context.pool.query(
      "UPDATE homeworks_action_receipts SET status='created',remote_id=$2 WHERE request_id=$1",
      [requestId, invoiceId],
    );
  }
  const invoice = await records.invoiceSnapshot(context, invoiceId),
    rows = await mirrorInvoices(context.pool, [invoice]);
  return {
    success: true,
    source: records.SOURCE,
    invoice: rows[0],
    id: rows[0].id,
    homeworks_invoice_id: invoiceId,
  };
}
async function fromQuote(context, quoteId) {
  const local = await context.pool.query('SELECT quote_number FROM sent_quotes WHERE id=$1', [quoteId]);
  if (!local.rows[0]) throw new Error('Quote not found.');
  const estimate = await require('./agreements').findEstimate(context, local.rows[0].quote_number);
  const current = await records.call(
    context,
    'YardDeskEstimateInvoice',
    'query YardDeskEstimateInvoice($id:SafeInt!){estimate(estimateId:$id){id status invoiceId}}',
    { id: estimate.id },
  );
  let invoiceId = current.estimate.invoiceId;
  if (!invoiceId) {
    if (current.estimate.status !== 'ACCEPTED')
      throw new Error('The Homeworks estimate must be accepted first.');
    await require('./schema').ensureHomeworksSchema(context.pool);
    const requestId = `estimate-invoice-${estimate.id}`;
    const claim = await context.pool.query(
      "INSERT INTO homeworks_action_receipts(request_id,fingerprint,status) VALUES($1,$2,'pending') ON CONFLICT DO NOTHING RETURNING request_id",
      [requestId, `estimate:${estimate.id}`],
    );
    if (!claim.rows.length)
      throw new Error('This estimate conversion needs verification in Homeworks. Refresh before retrying.');
    const result = await records.call(
      context,
      'YardDeskInvoiceEstimate',
      'mutation YardDeskInvoiceEstimate($id:SafeInt!){createInvoiceFromEstimate(estimateId:$id){id}}',
      { id: estimate.id },
    );
    invoiceId = records.id(result.createInvoiceFromEstimate?.id);
    await context.pool.query(
      "UPDATE homeworks_action_receipts SET status='created',remote_id=$2 WHERE request_id=$1",
      [requestId, invoiceId],
    );
  }
  const invoice = await records.invoiceSnapshot(context, invoiceId),
    rows = await mirrorInvoices(context.pool, [invoice]);
  return { success: true, source: records.SOURCE, invoice: rows[0], id: rows[0].id };
}
module.exports = { create, fromQuote };
