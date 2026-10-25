const express = require('express');
const crypto = require('crypto');
const { queryHomeWorksGraphql } = require('../services/homeworks/client');

const PAGE_SIZE = 500;
const MAX_PAGES = 40;
const MAX_SMS_LENGTH = 1600;
const TTL = 30 * 60 * 1000;
const OPEN_STATUSES = new Set(['PENDING', 'PARTIALLY_PAID', 'PAST_DUE']);
const money = value => '$' + Number(value).toFixed(2);
const cents = value => {
  if (value == null || String(value).trim() === '' || !Number.isFinite(Number(value))) {
    throw new Error('An invoice amount is unavailable. Refresh the customer details.');
  }
  return Math.round(Number(value) * 100);
};
function phoneNumber(value) {
  const digits = String(value || '').replace(/\D/g, '');
  if (digits.length === 10) return '+1' + digits;
  if (digits.length === 11 && digits[0] === '1') return '+' + digits;
  throw new Error('This customer needs a valid phone number in HomeWorks.');
}
function openInvoiceRows(rows, customerId, portalKey) {
  const seen = new Set();
  return rows.flatMap(row => {
    if (String(row.customerId) !== String(customerId)) throw new Error('Invoice owner could not be verified.');
    if (seen.has(String(row.id))) throw new Error('The invoice list changed while loading. Refresh the customer details.');
    seen.add(String(row.id));
    if (row.isSent !== true || row.isArchived !== false || row.deletedState !== 'ACTIVE'
      || !OPEN_STATUSES.has(row.status)) return [];
    const balanceCents = Math.max(0, cents(row.total) - cents(row.paidAmount));
    if (!balanceCents) return [];
    if (!Number.isSafeInteger(Number(row.id)) || !row.number) throw new Error('An invoice number is unavailable.');
    return [{
      id: String(row.id), number: String(row.number), balance: balanceCents / 100,
      past_due: row.status === 'PAST_DUE' || Number(row.daysPastDue) > 0,
      due_date: row.dueDate || null,
      url: 'https://secure.copilotcrm.com/client/invoices/view/' + encodeURIComponent(row.id) + '/' + encodeURIComponent(portalKey),
    }];
  }).sort((a, b) => Number(a.id) - Number(b.id));
}
function invoiceDetails(preview) {
  return 'Open balance: ' + money(preview.balance) + '\n'
    + preview.invoices.map(row => 'Invoice #' + row.number + ': ' + money(row.balance) + '\n' + row.url).join('\n');
}
function reminderBody(preview) {
  const firstName = preview.customer_name.trim().split(/\s+/)[0] || 'there';
  return 'Hi ' + firstName + '! This is Theresa from Pappas & Co. Landscaping. Following up on your open '
    + (preview.invoices.length === 1 ? 'invoice.' : 'invoices.')
    + '\n\n' + invoiceDetails(preview)
    + "\n\nIf you've already sent payment, please disregard this. Thank you!";
}
async function loadFollowup({ pool, invoiceId, scope = 'all', queryImpl = queryHomeWorksGraphql }) {
  if (!['all', 'single'].includes(scope)) throw new Error('Choose all open invoices or the selected invoice.');
  const liveId = String(invoiceId).match(/^homeworks:(\d+)$/);
  const local = liveId ? { id: invoiceId, external_invoice_id: liveId[1] }
    : (await pool.query('SELECT * FROM invoices WHERE id = $1', [invoiceId])).rows[0];
  if (!local) throw new Error('Invoice not found.');
  const sourceId = Number(local.external_invoice_id);
  if (!Number.isSafeInteger(sourceId) || sourceId <= 0) throw new Error('This invoice needs a HomeWorks invoice link before preparing a follow-up.');
  const query = (operationName, query, variables) => queryImpl({ pool, operationName, query, variables });
  const ownerData = await query('YardDeskFollowupOwner',
    'query YardDeskFollowupOwner($invoiceId: SafeInt!) { invoice(invoiceId: $invoiceId) { id customerId } }',
    { invoiceId: sourceId });
  const customerId = ownerData.invoice?.customerId;
  if (Number(ownerData.invoice?.id) !== sourceId || !Number.isSafeInteger(customerId) || customerId <= 0) {
    throw new Error('The HomeWorks invoice owner could not be verified.');
  }
  const recipient = await query('YardDeskFollowupCustomer',
    'query YardDeskFollowupCustomer($customerId: SafeInt!) { customers(where: { id: { equals: $customerId }, isDeleted: false }, take: 1) { id fullName cell phone portalKey } }',
    { customerId });
  const customer = recipient.customers?.[0];
  if (String(customer?.id) !== String(customerId) || !customer.portalKey) throw new Error('The customer payment link could not be verified.');
  const to = phoneNumber(customer.cell || customer.phone);
  const rows = [];
  let complete = false;
  for (let page = 0; page < MAX_PAGES; page++) {
    const data = await query('YardDeskFollowupInvoices',
      'query YardDeskFollowupInvoices($customerId: SafeInt!, $skip: SafeInt!, $take: SafeInt!) { invoices('
      + 'where: { customerId: { equals: $customerId }, isArchived: false, deletedState: { equals: ACTIVE }, status: { in: [PENDING, PARTIALLY_PAID, PAST_DUE] } },'
      + ' take: $take, skip: $skip, orderBy: [{ id: asc }]) { id number customerId status total paidAmount isSent isArchived deletedState daysPastDue dueDate } }',
      { customerId, skip: page * PAGE_SIZE, take: PAGE_SIZE });
    if (!Array.isArray(data.invoices)) throw new Error('The complete invoice list could not be loaded.');
    rows.push(...data.invoices);
    if (data.invoices.length < PAGE_SIZE) { complete = true; break; }
  }
  if (!complete) throw new Error('The complete invoice list could not be verified. No text was sent.');
  const all = openInvoiceRows(rows, customerId, customer.portalKey);
  const selected = scope === 'all' ? all : all.filter(row => row.id === String(sourceId));
  if (!selected.length) throw new Error(scope === 'all' ? 'This customer has no sent, open invoices.' : 'This invoice is no longer open.');
  const sum = list => list.reduce((total, row) => total + cents(row.balance), 0) / 100;
  const preview = {
    invoice_id: String(local.id), customer_id: String(customerId), customer_name: customer.fullName,
    scope, to, invoices: selected, invoice_number: selected.map(row => row.number).join(', '),
    balance: sum(selected), past_due: sum(selected.filter(row => row.past_due)),
    total_outstanding: sum(all), invoice_url: selected[0].url, source: 'official_homeworks_graphql',
  };
  return preview;
}
async function loadInvoiceChoices({ pool, queryImpl = queryHomeWorksGraphql }) {
  const rows = [];
  let complete = false;
  for (let page = 0; page < MAX_PAGES; page++) {
    const data = await queryImpl({ pool, operationName: 'YardDeskFollowupChoices',
      query: 'query YardDeskFollowupChoices($skip: SafeInt!, $take: SafeInt!) { invoices('
        + 'where: { isArchived: false, deletedState: { equals: ACTIVE }, status: { in: [PENDING, PARTIALLY_PAID, PAST_DUE] } },'
        + ' take: $take, skip: $skip, orderBy: [{ id: asc }]) { id number customerId status total paidAmount isSent isArchived deletedState } }',
      variables: { skip: page * PAGE_SIZE, take: PAGE_SIZE } });
    if (!Array.isArray(data.invoices)) throw new Error('Unable to load the current invoices.');
    rows.push(...data.invoices.filter(row => row.isSent === true && row.isArchived === false
      && row.deletedState === 'ACTIVE' && OPEN_STATUSES.has(row.status) && cents(row.total) > cents(row.paidAmount)));
    if (data.invoices.length < PAGE_SIZE) { complete = true; break; }
  }
  if (!complete) throw new Error('The complete invoice list could not be loaded.');
  const ids = [...new Set(rows.map(row => row.customerId))];
  const names = new Map();
  for (let offset = 0; offset < ids.length; offset += 200) {
    const data = await queryImpl({ pool, operationName: 'YardDeskFollowupChoiceCustomers',
      query: 'query YardDeskFollowupChoiceCustomers($ids: [SafeInt!]!) { customers(where: { id: { in: $ids }, isDeleted: false }, take: 200) { id fullName } }',
      variables: { ids: ids.slice(offset, offset + 200) } });
    if (!Array.isArray(data.customers)) throw new Error('Unable to load invoice customers.');
    data.customers.forEach(customer => names.set(customer.id, customer.fullName));
  }
  return rows.filter(row => names.has(row.customerId)).map(row => ({
    id: 'homeworks:' + row.id, invoice_number: String(row.number || row.id),
    customer_name: names.get(row.customerId), total: row.total, amount_paid: row.paidAmount,
    status: row.status.toLowerCase(),
  }));
}

function fingerprint(preview) {
  return crypto.createHash('sha256').update(JSON.stringify(preview)).digest('hex');
}
function signPreview(preview, secret) {
  if (!secret) throw new Error('Invoice text verification is unavailable.');
  const payload = Buffer.from(JSON.stringify({ hash: fingerprint(preview), at: Date.now() })).toString('base64url');
  return payload + '.' + crypto.createHmac('sha256', secret).update(payload).digest('base64url');
}
function verifyPreview(receipt, preview, secret) {
  if (!secret) return false;
  try {
    const [payload, signature, extra] = String(receipt || '').split('.');
    if (!payload || !signature || extra) return false;
    const expected = Buffer.from(crypto.createHmac('sha256', secret).update(payload).digest('base64url'));
    const actual = Buffer.from(signature);
    if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) return false;
    const saved = JSON.parse(Buffer.from(payload, 'base64url').toString());
    const age = Date.now() - saved.at;
    return age >= 0 && age <= TTL && saved.hash === fingerprint(preview);
  } catch (_) { return false; }
}
function validateBody(body, preview) {
  if (!body || body.length > MAX_SMS_LENGTH) throw new Error('Keep the reviewed text within ' + MAX_SMS_LENGTH + ' characters.');
  const urls = body.match(/https?:\/\/[^\s]+/g) || [];
  if (preview.invoices.some(row => !urls.includes(row.url) || !body.includes('#' + row.number))
    || urls.some(url => !preview.invoices.some(row => row.url === url))) {
    throw new Error('Keep each invoice number and its verified payment link in the reviewed text.');
  }
}
function createFollowupRoutes({ pool, authenticateToken, sendSms, isTwilioConfigured, twilioPhoneNumber,
  queryImpl = queryHomeWorksGraphql }) {
  const router = express.Router();
  const attempted = new Map();
  router.get('/api/invoice-followup/choices', authenticateToken, async (_req, res) => {
    try {
      res.json({ success: true, source: 'official_homeworks_graphql', invoices: await loadInvoiceChoices({ pool, queryImpl }) });
    } catch (error) { res.status(409).json({ success: false, error: error.message }); }
  });
  router.post('/api/invoices/:id/followup-preview', authenticateToken, async (req, res) => {
    try {
      const preview = await loadFollowup({ pool, invoiceId: req.params.id, scope: req.body?.scope, queryImpl });
      const verification_receipt = signPreview(preview, process.env.JWT_SECRET);
      res.json({ success: true, preview: { ...preview, verification_receipt, body: reminderBody(preview), max_length: MAX_SMS_LENGTH } });
    } catch (error) {
      res.status(409).json({ success: false, error: error.message });
    }
  });
  router.post('/api/invoices/:id/followup-sms', authenticateToken, async (req, res) => {
    let receipt = '';
    let providerAccepted = false;
    try {
      if (req.body?.confirm_send !== true) return res.status(400).json({ success: false, error: 'Review and confirm the text before sending.' });
      if (!isTwilioConfigured()) return res.status(400).json({ success: false, error: 'SMS is not configured.' });
      receipt = String(req.body.verification_receipt || '');
      if (!receipt) throw new Error('Refresh the customer details before sending.');
      for (const [key, time] of attempted) if (Date.now() - time > TTL) attempted.delete(key);
      if (attempted.has(receipt)) return res.status(409).json({ success: false, error: 'This text is already sending or was sent. Check the conversation before sending another.' });
      attempted.set(receipt, Date.now());
      const preview = await loadFollowup({ pool, invoiceId: req.params.id, scope: req.body.scope, queryImpl });
      if (!verifyPreview(receipt, preview, process.env.JWT_SECRET)) throw new Error('The invoices or customer details changed. Refresh and review the text again.');
      if (phoneNumber(req.body.phone) !== preview.to) throw new Error('The phone number changed. Refresh the customer details.');
      const body = String(req.body.body || '').trim();
      validateBody(body, preview);
      const message = await sendSms({ to: preview.to, body, from: twilioPhoneNumber, manualReviewedSend: true });
      providerAccepted = true;
      // Do not change invoice billing/sent state or trust a stale local customer link.
      try {
        await pool.query("INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read) VALUES ($1, 'outbound', $2, $3, $4, $5, NULL, true)",
          [message.sid, twilioPhoneNumber, preview.to, body, message.status]);
      } catch (error) {
        console.error('Invoice follow-up sent but message logging failed:', error.message);
        return res.json({ success: true, sid: message.sid, warning: 'Text sent, but the conversation log could not be updated. Do not resend.' });
      }
      res.json({ success: true, sid: message.sid });
    } catch (error) {
      if (!providerAccepted && receipt) attempted.delete(receipt);
      res.status(409).json({ success: false, error: error.message });
    }
  });
  return router;
}
module.exports = { createFollowupRoutes, loadInvoiceChoices, loadFollowup, openInvoiceRows, invoiceDetails, reminderBody,
  signPreview, verifyPreview, validateBody, phoneNumber, PAGE_SIZE, MAX_SMS_LENGTH };
