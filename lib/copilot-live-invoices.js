'use strict';
// Compatibility names preserve existing screens; all live operations use the official API.
const { getHomeWorksAccessToken, extractAccessToken, tokenExpiry } = require('../services/homeworks/auth');
const records = require('../services/homeworks/records');
const { matchesInvoiceCustomer, normalizedMatchValue } = require('./copilot-customer-match');
const COOKIE_SETTINGS_KEY = 'homeworks_api_token';
const DEFAULT_INVOICE_LIST_PATH = '/graphql';
async function loadCopilotSettings(pool) {
  const accessToken = await getHomeWorksAccessToken(pool);
  return {
    pool,
    accessToken,
    cookies: accessToken ? 'copilotApiAccessToken=' + accessToken : '',
    invoiceListPath: DEFAULT_INVOICE_LIST_PATH,
    ...tokenExpiry(accessToken),
    source: records.SOURCE,
  };
}
async function saveCopilotSettings(pool, { cookies, accessToken } = {}) {
  const token = extractAccessToken(accessToken ?? cookies ?? '');
  if (!token) throw new Error('An official Homeworks API token is required.');
  // Verify before replacing a working credential.
  await records.call(
    { pool, accessToken: token },
    'YardDeskVerifyConnection',
    'query YardDeskVerifyConnection {company{id name}}',
  );
  await pool.query(
    'INSERT INTO copilot_sync_settings(key,value,updated_at) VALUES($1,$2,NOW()) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()',
    [COOKIE_SETTINGS_KEY, token],
  );
  return loadCopilotSettings(pool);
}
function getJwtExpiryInfoFromCookieString(value) {
  return tokenExpiry(value);
}
async function discoverInvoicePdf(settings, invoice) {
  const parsedInvoice = await records.invoiceSnapshot(
    { pool: settings?.pool, accessToken: settings?.accessToken },
    invoice.external_invoice_id,
  );
  return { parsedInvoice, source: records.SOURCE, pdfUrl: null, errors: [] };
}
async function syncCopilotInvoices({
  pool,
  settings,
  invoiceIds,
  customerName,
  customerEmail,
  copilotCustomerId,
}) {
  const where = {};
  if (invoiceIds?.length) where.id = { in: invoiceIds.map(records.id) };
  if (copilotCustomerId) where.customerId = { equals: records.id(copilotCustomerId) };
  const invoices = await records.loadInvoices({ pool, accessToken: settings?.accessToken }, where);
  const selected = invoices.filter((invoice) =>
    matchesInvoiceCustomer(invoice, { customerName, customerEmail, copilotCustomerId }),
  );
  const mirrored = await require('../services/homeworks/invoice-cache').mirrorInvoices(pool, selected);
  return {
    success: true,
    source: records.SOURCE,
    pages: [{ count: selected.length }],
    synced: mirrored.map((row) => ({
      id: row.id,
      action: 'updated',
      external_invoice_id: row.external_invoice_id,
      invoice_number: row.invoice_number,
    })),
    errors: [],
    skipped: [],
    matched: mirrored.length,
  };
}
async function refreshCopilotInvoiceSnapshot({ pool, settings, invoiceRow }) {
  if (!invoiceRow?.external_invoice_id)
    throw new Error('This invoice needs a verified Homeworks invoice ID.');
  const invoice = await records.invoiceSnapshot(
    { pool, accessToken: settings?.accessToken },
    invoiceRow.external_invoice_id,
  );
  const rows = await require('../services/homeworks/invoice-cache').mirrorInvoices(pool, [invoice]);
  return { refreshed: true, source: records.SOURCE, action: 'updated', row: rows[0], settings };
}
async function markInvoiceRegularMailInCopilot(settings, invoice) {
  const results = await records.markInvoicesMailed(
    { pool: settings?.pool, accessToken: settings?.accessToken },
    [invoice.external_invoice_id],
  );
  return {
    success: results[0]?.success === true,
    errors: results[0]?.success ? [] : ['Homeworks mail flags are not confirmed.'],
  };
}
module.exports = {
  COOKIE_SETTINGS_KEY,
  DEFAULT_INVOICE_LIST_PATH,
  loadCopilotSettings,
  saveCopilotSettings,
  getJwtExpiryInfoFromCookieString,
  discoverInvoicePdf,
  syncCopilotInvoices,
  refreshCopilotInvoiceSnapshot,
  markInvoiceRegularMailInCopilot,
  _internal: { matchesInvoiceCustomer, normalizedMatchValue },
};
