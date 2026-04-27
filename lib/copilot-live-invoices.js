const cheerio = require('cheerio');
const { parseInvoiceListHtml } = require('../scripts/parse-copilot-invoices');
const { parseInvoiceDetailHtml } = require('../scripts/parse-copilot-invoice-detail');
const { parseCopilotInvoicePdfBuffer, cleanText } = require('./copilot-invoice-pdf');

const COPILOT_ORIGIN = 'https://secure.copilotcrm.com';
const DEFAULT_INVOICE_LIST_PATH = '/finances/invoices';
const COOKIE_SETTINGS_KEY = 'copilot_cookies';
const INVOICE_LIST_PATH_SETTINGS_KEY = 'copilot_invoice_list_path';

function normalizeCookieString(value) {
  return String(value || '')
    .split(';')
    .map((part) => part.trim())
    .filter(Boolean)
    .join('; ');
}

function decodeJwtPayload(token) {
  const parts = String(token || '').split('.');
  if (parts.length < 2) return null;

  const normalized = parts[1]
    .replace(/-/g, '+')
    .replace(/_/g, '/')
    .padEnd(Math.ceil(parts[1].length / 4) * 4, '=');

  try {
    const decoded = Buffer.from(normalized, 'base64').toString('utf8');
    const payload = JSON.parse(decoded);
    return payload && typeof payload === 'object' ? payload : null;
  } catch (_error) {
    return null;
  }
}

function getJwtExpiryInfoFromCookieString(cookieString) {
  const tokenMatch = normalizeCookieString(cookieString).match(/(?:^|;\s*)copilotApiAccessToken=([^;]+)/i);
  if (!tokenMatch) return { expiresAt: null, daysUntilExpiry: null };

  const decoded = decodeJwtPayload(tokenMatch[1]);
  const exp = decoded?.exp;
  if (!Number.isFinite(exp)) return { expiresAt: null, daysUntilExpiry: null };

  const expiresAt = new Date(exp * 1000);
  const daysUntilExpiry = Number(((expiresAt.getTime() - Date.now()) / 86400000).toFixed(1));
  return {
    expiresAt: expiresAt.toISOString(),
    daysUntilExpiry,
  };
}

function isCopilotLoginPage(html, url = '') {
  const body = String(html || '');
  const finalUrl = String(url || '');
  if (/\/login\b/i.test(finalUrl)) return true;
  return /<title[^>]*>.*(?:login|sign in|homeworks)/i.test(body) || /auth\/login/i.test(body);
}

async function saveCopilotSettings(pool, { cookies, invoiceListPath } = {}) {
  if (cookies !== undefined) {
    await pool.query(
      `INSERT INTO copilot_sync_settings (key, value, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
      [COOKIE_SETTINGS_KEY, normalizeCookieString(cookies)]
    );
  }

  if (invoiceListPath) {
    await pool.query(
      `INSERT INTO copilot_sync_settings (key, value, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
      [INVOICE_LIST_PATH_SETTINGS_KEY, invoiceListPath]
    );
  }

  return loadCopilotSettings(pool);
}

async function loadCopilotSettings(pool) {
  const result = await pool.query(
    `SELECT key, value
       FROM copilot_sync_settings
      WHERE key = ANY($1)`,
    [[COOKIE_SETTINGS_KEY, INVOICE_LIST_PATH_SETTINGS_KEY]]
  );

  const byKey = new Map(result.rows.map((row) => [row.key, row.value]));
  const cookies = byKey.get(COOKIE_SETTINGS_KEY) || '';
  const invoiceListPath = byKey.get(INVOICE_LIST_PATH_SETTINGS_KEY) || DEFAULT_INVOICE_LIST_PATH;
  const expiry = getJwtExpiryInfoFromCookieString(cookies);

  return {
    cookies,
    invoiceListPath,
    ...expiry,
  };
}

async function refreshCopilotCookiesWithCredentials(pool) {
  const username = process.env.COPILOT_USERNAME || process.env.COPILOTCRM_USERNAME;
  const password = process.env.COPILOT_PASSWORD || process.env.COPILOTCRM_PASSWORD;
  if (!username || !password) {
    throw new Error('Copilot credentials are not configured for automatic cookie refresh.');
  }

  const cookieJar = new Map();

  const loginRes = await fetch('https://api.copilotcrm.com/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: COPILOT_ORIGIN },
    body: JSON.stringify({ username, password }),
  });

  const loginText = await loginRes.text();
  let loginData;
  try {
    loginData = JSON.parse(loginText);
  } catch (_error) {
    throw new Error(`CopilotCRM login returned non-JSON (status ${loginRes.status}): ${loginText.slice(0, 200)}`);
  }

  if (!loginData?.accessToken) {
    throw new Error(`CopilotCRM login failed (status ${loginRes.status}): ${loginText.slice(0, 200)}`);
  }

  cookieJar.set('copilotApiAccessToken', loginData.accessToken);

  const collectSetCookies = (setCookies = []) => {
    for (const cookie of setCookies) {
      const [pair] = String(cookie || '').split(';');
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) cookieJar.set(pair.slice(0, eqIdx).trim(), pair.slice(eqIdx + 1).trim());
    }
  };

  collectSetCookies(loginRes.headers.getSetCookie?.() || []);

  const sessionRes = await fetch(`${COPILOT_ORIGIN}/dashboard`, {
    method: 'GET',
    headers: {
      Cookie: `copilotApiAccessToken=${loginData.accessToken}`,
      Origin: COPILOT_ORIGIN,
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    },
    redirect: 'manual',
  });
  collectSetCookies(sessionRes.headers.getSetCookie?.() || []);

  const schedulerRes = await fetch(`${COPILOT_ORIGIN}/scheduler`, {
    method: 'GET',
    headers: {
      Cookie: [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join('; '),
      Origin: COPILOT_ORIGIN,
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    },
    redirect: 'manual',
  });
  collectSetCookies(schedulerRes.headers.getSetCookie?.() || []);

  const cookieString = [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
  await saveCopilotSettings(pool, { cookies: cookieString });
  return loadCopilotSettings(pool);
}

function buildCopilotUrl(pathOrUrl) {
  if (!pathOrUrl) return new URL(DEFAULT_INVOICE_LIST_PATH, COPILOT_ORIGIN);
  if (/^https?:\/\//i.test(pathOrUrl)) return new URL(pathOrUrl);
  return new URL(pathOrUrl.startsWith('/') ? pathOrUrl : `/${pathOrUrl}`, COPILOT_ORIGIN);
}

async function fetchCopilot(settings, pathOrUrl, options = {}) {
  if (!settings?.cookies) {
    throw new Error('Copilot cookies are not configured. Use /api/copilot/settings first.');
  }

  const url = buildCopilotUrl(pathOrUrl);
  const response = await fetch(url, {
    method: options.method || 'GET',
    headers: {
      Cookie: settings.cookies,
      Accept: options.accept || '*/*',
      'Content-Type': options.contentType || 'application/x-www-form-urlencoded; charset=UTF-8',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
      'X-Requested-With': options.xRequestedWith || 'XMLHttpRequest',
      ...(options.headers || {}),
    },
    body: options.body,
    redirect: 'follow',
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Copilot request failed (${response.status}) ${text.slice(0, 200)}`);
  }

  return response;
}

function extractInvoicePaginationPaths(html, pageUrl) {
  const $ = cheerio.load(html || '');
  const base = buildCopilotUrl(pageUrl || DEFAULT_INVOICE_LIST_PATH);
  const paths = new Set();

  $('a[href]').each((_, link) => {
    const href = ($(link).attr('href') || '').trim();
    if (!href || href.startsWith('#') || href.startsWith('javascript:')) return;

    try {
      const resolved = new URL(href, base);
      const page = resolved.searchParams.get('p') || resolved.searchParams.get('page');
      if (!page) return;
      if (!/invoice/i.test(resolved.pathname) && !/customers/i.test(resolved.pathname)) return;
      paths.add(`${resolved.pathname}${resolved.search}`);
    } catch (_error) {
      // Ignore malformed links.
    }
  });

  return Array.from(paths);
}

function findInterestingLinks(html, pageUrl) {
  const $ = cheerio.load(html || '');
  const base = buildCopilotUrl(pageUrl);
  const links = new Set();

  $('a[href]').each((_, link) => {
    const href = ($(link).attr('href') || '').trim();
    if (!href || href.startsWith('#') || href.startsWith('javascript:')) return;
    const label = cleanText($(link).text()).toLowerCase();

    if (!/pdf|print|download|invoice/.test(`${href} ${label}`)) return;

    try {
      links.add(new URL(href, base).toString());
    } catch (_error) {
      // Ignore malformed links.
    }
  });

  return Array.from(links);
}

function isSyntheticCustomerEmail(value) {
  const email = String(value || '').trim().toLowerCase();
  if (!email) return false;
  return email.includes('.ingest.sentry.io') || email.endsWith('@sentry.io');
}

function normalizeCustomerEmail(value) {
  const email = String(value || '').trim();
  if (!email) return null;
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return null;
  if (isSyntheticCustomerEmail(email)) return null;
  return email;
}

function normalizeCustomerAddress(value) {
  const address = cleanText(value || '');
  if (!address) return null;
  if (/invoice\s*#|invoice\s+date|outstanding\s+balance|subtotal|taxes|total\s+due/i.test(address)) return null;
  return address;
}

function extractCustomerAddressFromHtml(html) {
  const $ = cheerio.load(html || '');
  const bodyText = cleanText($('body').text());
  const match = bodyText.match(/(?:Bill To|Customer|Property Address)\s+([A-Za-z0-9#&'. -]+)\s+([0-9]{1,6}[^]+?)(?=(?:Invoice|Description|Notes|Total|Subtotal|Tax)\b)/i);
  if (!match) return null;
  return normalizeCustomerAddress(`${match[1]}\n${match[2]}`);
}

function extractEmailFromHtml(html) {
  const match = String(html || '').match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? normalizeCustomerEmail(match[0]) : null;
}

function extractInvoiceNumberFromHtml(html) {
  const $ = cheerio.load(html || '');
  const bodyText = cleanText($('body').text());
  const match = bodyText.match(/Invoice\s*#\s*([A-Z0-9-]+)/i);
  return match ? match[1] : null;
}

function mergeInvoiceSnapshots(baseInvoice, overlayInvoice) {
  const merged = {
    ...baseInvoice,
    ...Object.fromEntries(
      Object.entries(overlayInvoice || {}).filter(([, value]) => value !== null && value !== undefined && value !== '')
    ),
  };

  merged.metadata = {
    ...(baseInvoice.metadata || {}),
    ...(overlayInvoice?.metadata || {}),
  };

  if ((!merged.line_items || merged.line_items.length === 0) && baseInvoice.line_items) {
    merged.line_items = baseInvoice.line_items;
  }

  return merged;
}

async function discoverInvoicePdf(settings, invoice) {
  const detailPath = invoice.view_path || `/finances/invoices/view/${invoice.external_invoice_id}`;
  const detailResponse = await fetchCopilot(settings, detailPath, {
    accept: 'text/html,application/xhtml+xml',
    xRequestedWith: '',
  });
  const detailHtml = await detailResponse.text();
  const pageUrl = detailResponse.url;
  if (isCopilotLoginPage(detailHtml, pageUrl)) {
    throw new Error(`Copilot session expired while loading invoice detail ${invoice.invoice_number || invoice.external_invoice_id}`);
  }
  const linkCandidates = findInterestingLinks(detailHtml, pageUrl);
  const errors = [];
  let parsedFromHtml = null;

  try {
    parsedFromHtml = parseInvoiceDetailHtml(detailHtml);
  } catch (error) {
    errors.push(`detail_html_parse: ${error.message}`);
  }

  const genericCandidates = [
    `${buildCopilotUrl(detailPath).toString()}?output=pdf`,
    `${buildCopilotUrl(detailPath).toString()}?format=pdf`,
    `${buildCopilotUrl(detailPath).toString()}?print=1`,
  ];

  const candidates = [...new Set([...linkCandidates, ...genericCandidates])];
  let parsedFromPdf = null;
  let pdfUrlUsed = null;

  for (const candidate of candidates) {
    try {
      const response = await fetchCopilot(settings, candidate, {
        accept: 'application/pdf,*/*',
        xRequestedWith: '',
      });
      const contentType = response.headers.get('content-type') || '';
      if (!/pdf/i.test(contentType) && !candidate.includes('pdf') && !candidate.includes('print')) continue;
      const bytes = Buffer.from(await response.arrayBuffer());
      parsedFromPdf = await parseCopilotInvoicePdfBuffer(bytes);
      pdfUrlUsed = candidate;
      break;
    } catch (error) {
      errors.push(`${candidate}: ${error.message}`);
    }
  }

  const htmlSnapshot = {
    external_invoice_id: invoice.external_invoice_id,
    invoice_number: parsedFromHtml?.invoice_number || extractInvoiceNumberFromHtml(detailHtml) || invoice.invoice_number,
    customer_email: normalizeCustomerEmail(parsedFromHtml?.customer_email) || extractEmailFromHtml(detailHtml) || normalizeCustomerEmail(invoice.customer_email) || null,
    customer_address: normalizeCustomerAddress(parsedFromHtml?.customer_address) || extractCustomerAddressFromHtml(detailHtml) || normalizeCustomerAddress(invoice.customer_address) || null,
    customer_name: parsedFromHtml?.customer_name || invoice.customer_name || null,
    status: parsedFromHtml?.status || invoice.status || null,
    subtotal: parsedFromHtml?.subtotal ?? invoice.subtotal ?? null,
    tax_amount: parsedFromHtml?.tax_amount ?? invoice.tax_amount ?? null,
    total: parsedFromHtml?.total ?? invoice.total ?? null,
    amount_paid: parsedFromHtml?.amount_paid ?? invoice.amount_paid ?? null,
    due_date: parsedFromHtml?.due_date || invoice.due_date || null,
    created_at: parsedFromHtml?.invoice_date || invoice.created_at || null,
    notes: parsedFromHtml?.notes || invoice.notes || null,
    terms: parsedFromHtml?.terms || invoice.terms || null,
    line_items: Array.isArray(parsedFromHtml?.line_items) ? parsedFromHtml.line_items : invoice.line_items,
    metadata: {
      ...(parsedFromHtml?.metadata || {}),
      detail_page_url: pageUrl,
      detail_pdf_url: pdfUrlUsed,
      detail_fetch_errors: errors,
    },
  };

  const htmlInvoice = mergeInvoiceSnapshots(invoice, htmlSnapshot);

  return {
    detailHtml,
    pageUrl,
    pdfUrlUsed,
    parsedInvoice: parsedFromPdf ? mergeInvoiceSnapshots(htmlInvoice, parsedFromPdf) : htmlInvoice,
  };
}

async function fetchInvoiceListPages(settings, { invoiceIds, maxPages = 1, listPath, pagePaths } = {}) {
  const seen = new Set();
  const queue = Array.isArray(pagePaths) && pagePaths.length
    ? [...pagePaths]
    : [listPath || settings.invoiceListPath || DEFAULT_INVOICE_LIST_PATH];
  const invoices = [];
  const summaries = [];

  while (queue.length && seen.size < maxPages) {
    const nextPath = queue.shift();
    if (!nextPath || seen.has(nextPath)) continue;
    seen.add(nextPath);

    const response = await fetchCopilot(settings, nextPath, {
      accept: 'text/html,application/xhtml+xml',
      xRequestedWith: '',
    });
    const html = await response.text();
    const pageInvoices = parseInvoiceListHtml(html);
    invoices.push(...pageInvoices);

    summaries.push({
      path: nextPath,
      count: pageInvoices.length,
    });

    if (invoiceIds?.length) continue;

    const paginationPaths = extractInvoicePaginationPaths(html, response.url);
    paginationPaths.forEach((path) => {
      if (!seen.has(path) && !queue.includes(path) && seen.size + queue.length < maxPages) {
        queue.push(path);
      }
    });
  }

  const filtered = Array.isArray(invoiceIds) && invoiceIds.length
    ? invoices.filter((invoice) => invoiceIds.includes(String(invoice.external_invoice_id)) || invoiceIds.includes(String(invoice.invoice_number)))
    : invoices;

  return {
    invoices: filtered,
    pages: summaries,
  };
}

async function syncCopilotInvoices({ pool, settings, invoiceIds, maxPages = 1, listPath, detail = true, linkCustomers = true }) {
  const listPayload = await fetchInvoiceListPages(settings, { invoiceIds, maxPages, listPath });
  const synced = [];
  const errors = [];

  for (const summaryInvoice of listPayload.invoices) {
    try {
      const detailedInvoice = detail
        ? (await discoverInvoicePdf(settings, summaryInvoice)).parsedInvoice
        : summaryInvoice;

      const upserted = await require('../scripts/import-copilot-invoices').upsert(pool, detailedInvoice, { linkCustomers });
      synced.push({
        id: upserted.id,
        action: upserted.action,
        external_invoice_id: summaryInvoice.external_invoice_id,
        invoice_number: detailedInvoice.invoice_number || summaryInvoice.invoice_number,
        customer_name: detailedInvoice.customer_name || summaryInvoice.customer_name,
      });
    } catch (error) {
      errors.push({
        external_invoice_id: summaryInvoice.external_invoice_id,
        invoice_number: summaryInvoice.invoice_number,
        message: error.message,
      });
    }
  }

  return {
    success: errors.length === 0,
    pages: listPayload.pages,
    synced,
    errors,
  };
}

function parseJsonObject(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch (_error) {
      return {};
    }
  }
  return {};
}

function normalizeLineItems(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  }
  return [];
}

function parseMoneyValue(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const cleaned = String(value).trim().replace(/[^0-9.\-]/g, '');
  if (!cleaned || cleaned === '-' || cleaned === '.') return null;
  const numeric = Number(cleaned);
  return Number.isFinite(numeric) ? numeric : null;
}

function roundMoney(value) {
  return Math.round(Number(value || 0) * 100) / 100;
}

function lineItemAmount(item) {
  const directCandidates = [
    item?.amount,
    item?.line_total,
    item?.total,
    item?.extended_amount,
    item?.extended_total,
    item?.lineAmount,
    item?.lineTotal,
  ];

  for (const candidate of directCandidates) {
    const numeric = parseMoneyValue(candidate);
    if (numeric !== null) return numeric;
  }

  const quantity = parseMoneyValue(item?.quantity ?? 0);
  const rate = parseMoneyValue(item?.rate ?? item?.unit_price ?? 0);
  if (quantity !== null && rate !== null) return quantity * rate;
  return 0;
}

function lineItemBaseAmount(item) {
  const directCandidates = [
    item?.subtotal,
    item?.base_amount,
    item?.pre_tax_amount,
    item?.extended_subtotal,
  ];

  for (const candidate of directCandidates) {
    const numeric = parseMoneyValue(candidate);
    if (numeric !== null) return numeric;
  }

  const quantity = parseMoneyValue(item?.quantity ?? 0);
  const rate = parseMoneyValue(item?.rate ?? item?.unit_price ?? 0);
  if (quantity !== null && rate !== null) return quantity * rate;
  return lineItemAmount(item);
}

function lineItemTaxAmount(item) {
  const explicit = parseMoneyValue(item?.tax_amount ?? item?.tax);
  if (explicit !== null) return explicit;

  const baseAmount = lineItemBaseAmount(item);
  const taxPercent = parseMoneyValue(item?.tax_percent ?? item?.taxPercent);
  if (taxPercent !== null) return baseAmount * (taxPercent / 100);

  return Math.max(0, lineItemAmount(item) - baseAmount);
}

function normalizeInvoiceFinancialRow(row) {
  if (!row) return row;

  const metadata = parseJsonObject(row.external_metadata || row.metadata);
  const lineItems = normalizeLineItems(row.line_items);
  const rowSubtotal = parseMoneyValue(row.subtotal);
  const rowTax = parseMoneyValue(row.tax_amount);
  const rowTotal = parseMoneyValue(row.total);

  const lineSubtotal = roundMoney(lineItems.reduce((sum, item) => sum + lineItemBaseAmount(item), 0));
  const lineTax = roundMoney(lineItems.reduce((sum, item) => sum + lineItemTaxAmount(item), 0));
  const lineTotal = roundMoney(lineItems.reduce((sum, item) => sum + lineItemAmount(item), 0));

  let subtotal = rowSubtotal ?? 0;
  let taxAmount = rowTax ?? 0;
  let total = rowTotal ?? roundMoney(subtotal + taxAmount);

  if (lineItems.length) {
    const rowMatchesLines = rowTotal !== null && Math.abs(rowTotal - lineTotal) <= 0.009;
    if (!rowMatchesLines && lineTotal > 0) {
      subtotal = lineSubtotal;
      taxAmount = lineTax;
      total = lineTotal;
    } else {
      if (rowSubtotal === null && lineSubtotal > 0) subtotal = lineSubtotal;
      if (rowTax === null && lineTax >= 0) taxAmount = lineTax;
      if (rowTotal === null && lineTotal > 0) total = lineTotal;
    }
  }

  subtotal = roundMoney(subtotal);
  taxAmount = roundMoney(taxAmount);
  total = roundMoney(total);

  if (Math.abs((subtotal + taxAmount) - total) > 0.009) {
    if (lineItems.length && Math.abs((lineSubtotal + lineTax) - lineTotal) <= 0.009) {
      subtotal = lineSubtotal;
      taxAmount = lineTax;
      total = lineTotal;
    } else {
      taxAmount = roundMoney(total - subtotal);
    }
  }

  const explicitPriorBalance = parseMoneyValue(
    metadata.prior_balance
    ?? metadata.previous_balance
    ?? metadata.past_due_balance
  );
  const metadataAccountDue = parseMoneyValue(metadata.total_due_on_account ?? metadata.total_due);
  const metadataOutstanding = parseMoneyValue(metadata.outstanding_balance);

  let priorBalance = 0;
  if (explicitPriorBalance !== null && explicitPriorBalance > 0) {
    priorBalance = roundMoney(explicitPriorBalance);
  } else if (metadataAccountDue !== null && metadataAccountDue > total + 0.009) {
    priorBalance = roundMoney(metadataAccountDue - total);
  } else if (metadataOutstanding !== null && metadataOutstanding > total + 0.009) {
    priorBalance = roundMoney(metadataOutstanding - total);
  }

  return {
    ...row,
    subtotal,
    tax_amount: taxAmount,
    total,
    line_items: lineItems,
    external_metadata: {
      ...metadata,
      outstanding_balance: priorBalance,
      this_invoice: total,
      total_due: roundMoney(total + priorBalance),
      total_due_on_account: roundMoney(total + priorBalance),
    },
  };
}

function buildSummaryInvoiceFromRow(row) {
  const metadata = parseJsonObject(row?.external_metadata || row?.metadata);
  const externalInvoiceId = row?.external_invoice_id || metadata.external_invoice_id || null;

  return {
    external_invoice_id: externalInvoiceId,
    invoice_number: row?.invoice_number || metadata.invoice_number || null,
    customer_name: row?.customer_name || null,
    customer_email: row?.customer_email || null,
    customer_address: row?.customer_address || null,
    status: row?.status || null,
    total: row?.total ?? null,
    amount_paid: row?.amount_paid ?? null,
    total_due: metadata.total_due ?? metadata.total_due_on_account ?? row?.total ?? null,
    line_items: normalizeLineItems(row?.line_items),
    view_path: metadata.view_path || (externalInvoiceId ? `/finances/invoices/view/${externalInvoiceId}` : null),
    metadata,
  };
}

const MAIL_DEBUG_INVOICE_NUMBER = '10273';

function isMailDebugInvoice(row) {
  const candidates = [
    row?.invoice_number,
    row?.external_invoice_id,
    row?.id,
  ]
    .map((value) => String(value ?? '').trim())
    .filter(Boolean);

  return candidates.includes(MAIL_DEBUG_INVOICE_NUMBER);
}

function debugLineItems(rawLineItems) {
  return normalizeLineItems(rawLineItems).map((item, index) => ({
    index,
    service: item?.name || item?.description || 'Service',
    service_date_raw: item?.service_date_raw ?? item?.service_date ?? item?.date ?? null,
    quantity: item?.quantity ?? null,
    rate: item?.rate ?? item?.unit_price ?? null,
    amount: item?.amount ?? null,
    line_total: item?.line_total ?? null,
    total: item?.total ?? null,
    extended_amount: item?.extended_amount ?? null,
    extended_total: item?.extended_total ?? null,
  }));
}

function logMailDebug(label, row, extra = {}) {
  if (!isMailDebugInvoice(row)) return;
  console.log('[mail-debug 10273]', JSON.stringify({
    label,
    rowId: row?.id ?? null,
    invoiceNumber: row?.invoice_number ?? null,
    externalInvoiceId: row?.external_invoice_id ?? null,
    lineItemCount: debugLineItems(row?.line_items).length,
    lineItems: debugLineItems(row?.line_items),
    extra,
  }));
}

async function refreshCopilotInvoiceSnapshot({ pool, settings, invoiceRow, linkCustomers = true }) {
  if (!invoiceRow) {
    throw new Error('invoiceRow is required');
  }

  logMailDebug('refreshCopilotInvoiceSnapshot:entered', invoiceRow, {
    hasCookies: Boolean(settings?.cookies),
    linkCustomers,
  });

  const summaryInvoice = buildSummaryInvoiceFromRow(invoiceRow);
  if (!summaryInvoice.external_invoice_id && !summaryInvoice.invoice_number) {
    return { refreshed: false, row: invoiceRow, reason: 'missing_external_identity' };
  }

  let activeSettings = settings;
  let discovery;
  try {
    discovery = await discoverInvoicePdf(activeSettings, summaryInvoice);
  } catch (error) {
    if (!/Copilot session expired/i.test(String(error?.message || ''))) throw error;
    activeSettings = await refreshCopilotCookiesWithCredentials(pool);
    discovery = await discoverInvoicePdf(activeSettings, summaryInvoice);
  }

  const detailedInvoice = discovery.parsedInvoice;
  logMailDebug('refreshCopilotInvoiceSnapshot:detail-fetched', {
    ...invoiceRow,
    line_items: detailedInvoice?.line_items || [],
  }, {
    detailInvoiceNumber: detailedInvoice?.invoice_number ?? null,
    detailExternalInvoiceId: detailedInvoice?.external_invoice_id ?? null,
  });
  const importer = require('../scripts/import-copilot-invoices');
  const upserted = await importer.upsert(pool, detailedInvoice, { linkCustomers });
  const refreshedResult = await pool.query('SELECT * FROM invoices WHERE id = $1 LIMIT 1', [upserted.id]);
  const normalizedRow = normalizeInvoiceFinancialRow(refreshedResult.rows[0] || invoiceRow);
  logMailDebug('refreshCopilotInvoiceSnapshot:row-reread', normalizedRow, {
    action: upserted?.action || null,
    rereadBeforeReturn: true,
  });

  return {
    refreshed: true,
    action: upserted.action,
    row: normalizedRow,
    settings: activeSettings,
  };
}

async function markInvoiceRegularMailInCopilot(settings, invoice, options = {}) {
  const targetPath = options.listPath || settings.invoiceListPath || DEFAULT_INVOICE_LIST_PATH;
  const invoiceId = invoice.external_invoice_id || invoice.invoice_number;
  const attempts = [
    new URLSearchParams({ action: 'markSendSnailmail', 'row[]': invoiceId }),
    new URLSearchParams({ bundleAction: 'markSendSnailmail', 'row[]': invoiceId }),
    new URLSearchParams({ markSendSnailmail: '1', 'row[]': invoiceId }),
  ];

  const failures = [];
  for (const body of attempts) {
    try {
      const response = await fetchCopilot(settings, targetPath, {
        method: 'POST',
        body: body.toString(),
      });
      const text = await response.text();
      if (/login|sign in/i.test(text)) {
        throw new Error('Copilot rejected the stored session.');
      }
      if (/error|invalid/i.test(text) && !/invoice/i.test(text)) {
        throw new Error(text.slice(0, 200));
      }
      return {
        success: true,
        attempt: body.toString(),
      };
    } catch (error) {
      failures.push(`${body.toString()}: ${error.message}`);
    }
  }

  return {
    success: false,
    errors: failures,
  };
}

module.exports = {
  COOKIE_SETTINGS_KEY,
  DEFAULT_INVOICE_LIST_PATH,
  getJwtExpiryInfoFromCookieString,
  loadCopilotSettings,
  markInvoiceRegularMailInCopilot,
  refreshCopilotInvoiceSnapshot,
  saveCopilotSettings,
  syncCopilotInvoices,
};
