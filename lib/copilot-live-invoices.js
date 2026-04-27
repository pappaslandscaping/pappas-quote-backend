const cheerio = require('cheerio');
const { parseInvoiceListHtml } = require('../scripts/parse-copilot-invoices');
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

function extractCustomerAddressFromHtml(html) {
  const $ = cheerio.load(html || '');
  const bodyText = cleanText($('body').text());
  const match = bodyText.match(/(?:Bill To|Customer|Property Address)\s+([A-Za-z0-9#&'. -]+)\s+([0-9]{1,6}[^]+?)(?=(?:Invoice|Description|Notes|Total|Subtotal|Tax)\b)/i);
  if (!match) return null;
  return cleanText(`${match[1]}\n${match[2]}`);
}

function extractEmailFromHtml(html) {
  const match = String(html || '').match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? match[0] : null;
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
  const linkCandidates = findInterestingLinks(detailHtml, pageUrl);

  const genericCandidates = [
    `${buildCopilotUrl(detailPath).toString()}?output=pdf`,
    `${buildCopilotUrl(detailPath).toString()}?format=pdf`,
    `${buildCopilotUrl(detailPath).toString()}?print=1`,
  ];

  const candidates = [...new Set([...linkCandidates, ...genericCandidates])];
  let parsedFromPdf = null;
  let pdfUrlUsed = null;
  const errors = [];

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
    invoice_number: extractInvoiceNumberFromHtml(detailHtml) || invoice.invoice_number,
    customer_email: extractEmailFromHtml(detailHtml) || invoice.customer_email || null,
    customer_address: extractCustomerAddressFromHtml(detailHtml) || invoice.customer_address || null,
    metadata: {
      detail_page_url: pageUrl,
      detail_pdf_url: pdfUrlUsed,
      detail_fetch_errors: errors,
    },
  };

  return {
    detailHtml,
    pageUrl,
    pdfUrlUsed,
    parsedInvoice: parsedFromPdf ? mergeInvoiceSnapshots(invoice, parsedFromPdf) : mergeInvoiceSnapshots(invoice, htmlSnapshot),
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
  saveCopilotSettings,
  syncCopilotInvoices,
};
