// ═══════════════════════════════════════════════════════════
// Invoice & Payment Routes — extracted from server.js
// Handles: invoices CRUD, payments, Square processing,
//          PDF generation, reminders, payment schedules
// ═══════════════════════════════════════════════════════════

const express = require('express');
const crypto = require('crypto');
const cheerio = require('cheerio');
const { validate, schemas } = require('../lib/validate');
const {
  normalizeStoredInvoiceStatus,
  isOutstandingInvoice,
  cleanStatusValue,
} = require('../lib/invoice-status');
const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
} = require('../lib/copilot-metric-sources');
const {
  AGING_BUCKET_KEYS,
  hasValidAgingBuckets,
  normalizeAgingSnapshot,
  getAgingSnapshotExpiry,
} = require('../lib/copilot-aging');
const {
  COPILOT_PAYMENTS_BASE_PATH,
  parseCopilotPaymentsHtml,
  normalizeCopilotPaymentsSnapshot,
} = require('../lib/copilot-payments');
const {
  COPILOT_TAX_SUMMARY_BASE_PATH,
  parseCopilotTaxSummaryHtml,
  normalizeTaxSummarySnapshot,
  buildDailyTaxRecommendation,
} = require('../lib/copilot-tax-summary');
const { buildInvoiceHistoryEvents } = require('../lib/invoice-history');
const { parseInvoiceListHtml } = require('../scripts/parse-copilot-invoices');
const {
  roundMoney,
  upsertCopilotPayments,
  hydratePaymentRecord,
} = require('../scripts/import-copilot-payments');

module.exports = function createInvoiceRoutes({ pool, sendEmail, emailTemplate, escapeHtml, serverError, authenticateToken, nextInvoiceNumber, squareClient, SQUARE_APP_ID, SQUARE_LOCATION_ID, SquareApiError, NOTIFICATION_EMAIL, LOGO_URL, FROM_EMAIL, COMPANY_NAME, getCopilotToken }) {
  const router = express.Router();

let cachedCopilotStanding = null;
let cachedCopilotAging = null;
let cachedCopilotAgingPromise = null;
let cachedCopilotPayments = null;
let cachedCopilotTaxSummaries = new Map();
const COPILOT_STANDING_SNAPSHOT_KEY = 'copilot_invoice_last_account_standing';
const COPILOT_AGING_SNAPSHOT_KEY = 'copilot_invoice_last_aging';
const COPILOT_PAYMENTS_SNAPSHOT_KEY = 'copilot_payments_snapshot';
const COPILOT_TAX_SUMMARY_CACHE_TTL_MS = 5 * 60 * 1000;
const ACCOUNT_STANDING_KEYS = ['total', 'outstanding', 'paid', 'past_due', 'credit'];
const COPILOT_AGING_CACHE_TTL_MS = 5 * 60 * 1000;
const COPILOT_PAYMENTS_CACHE_TTL_MS = 5 * 60 * 1000;

function outstandingBalance(inv) {
  const total = parseFloat(inv.total) || 0;
  const paid = parseFloat(inv.amount_paid) || 0;
  return Math.max(0, total - paid);
}

function normalizeCount(value) {
  const parsed = parseInt(String(value || '').replace(/[^0-9-]/g, ''), 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseStandingCount(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function buildTaxSummaryCacheKey({ startDate, endDate, basis = 'collected' }) {
  return `${basis}:${startDate}:${endDate}`;
}

function eachDayInclusive(startDate, endDate) {
  const dates = [];
  const cursor = new Date(`${startDate}T00:00:00Z`);
  const end = new Date(`${endDate}T00:00:00Z`);
  while (!Number.isNaN(cursor.getTime()) && cursor <= end) {
    dates.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
}

async function readPersistedTaxSummarySnapshot({ startDate, endDate, basis = 'collected' }) {
  const result = await pool.query(
    `SELECT *
       FROM copilot_tax_summary_snapshots
      WHERE external_source = 'copilotcrm'
        AND basis = $1
        AND start_date = $2
        AND end_date = $3
      LIMIT 1`,
    [basis, startDate, endDate]
  );
  if (!result.rows[0]) return null;
  return normalizeTaxSummarySnapshot({
    source: PERSISTED_COPILOT_SNAPSHOT_SOURCE,
    as_of: result.rows[0].imported_at || result.rows[0].updated_at,
    basis: result.rows[0].basis,
    start_date: result.rows[0].start_date?.toISOString
      ? result.rows[0].start_date.toISOString().slice(0, 10)
      : String(result.rows[0].start_date),
    end_date: result.rows[0].end_date?.toISOString
      ? result.rows[0].end_date.toISOString().slice(0, 10)
      : String(result.rows[0].end_date),
    rows: result.rows[0].rows || [],
    total_sales: result.rows[0].total_sales,
    taxable_amount: result.rows[0].taxable_amount,
    discount: result.rows[0].discount,
    tax_amount: result.rows[0].tax_amount,
    processing_fees: result.rows[0].processing_fees,
    tips: result.rows[0].tips,
    external_metadata: result.rows[0].external_metadata || {},
  }, PERSISTED_COPILOT_SNAPSHOT_SOURCE);
}

async function persistTaxSummarySnapshot(snapshot) {
  const normalized = normalizeTaxSummarySnapshot(snapshot, LIVE_COPILOT_SOURCE);
  if (!normalized) return null;
  await pool.query(
    `INSERT INTO copilot_tax_summary_snapshots (
       external_source, basis, start_date, end_date, rows,
       total_sales, taxable_amount, discount, tax_amount,
       processing_fees, tips, external_metadata, imported_at, updated_at
     ) VALUES (
       $1, $2, $3, $4, $5,
       $6, $7, $8, $9,
       $10, $11, $12, NOW(), NOW()
     )
     ON CONFLICT (external_source, basis, start_date, end_date) DO UPDATE SET
       rows = EXCLUDED.rows,
       total_sales = EXCLUDED.total_sales,
       taxable_amount = EXCLUDED.taxable_amount,
       discount = EXCLUDED.discount,
       tax_amount = EXCLUDED.tax_amount,
       processing_fees = EXCLUDED.processing_fees,
       tips = EXCLUDED.tips,
       external_metadata = EXCLUDED.external_metadata,
       imported_at = NOW(),
       updated_at = NOW()`,
    [
      LIVE_COPILOT_SOURCE,
      normalized.basis,
      normalized.start_date,
      normalized.end_date,
      JSON.stringify(normalized.rows),
      normalized.total_sales,
      normalized.taxable_amount,
      normalized.discount,
      normalized.tax_amount,
      normalized.processing_fees,
      normalized.tips,
      JSON.stringify(normalized.external_metadata || {}),
    ]
  );
  return normalized;
}

async function fetchCopilotTaxSummarySnapshot({ startDate, endDate, basis = 'collected', forceRefresh = false } = {}) {
  const cacheKey = buildTaxSummaryCacheKey({ startDate, endDate, basis });
  const cached = cachedCopilotTaxSummaries.get(cacheKey);
  if (!forceRefresh && cached?.expiresAt > Date.now()) return cached.value;

  if (!forceRefresh) {
    const persisted = await readPersistedTaxSummarySnapshot({ startDate, endDate, basis }).catch(() => null);
    if (persisted) {
      cachedCopilotTaxSummaries.set(cacheKey, {
        value: persisted,
        expiresAt: Date.now() + COPILOT_TAX_SUMMARY_CACHE_TTL_MS,
      });
      return persisted;
    }
  }

  const tokenInfo = await getCopilotToken();
  if (!tokenInfo?.cookieHeader) throw new Error('CopilotCRM authentication is not configured');

  const url = new URL(`https://secure.copilotcrm.com${COPILOT_TAX_SUMMARY_BASE_PATH}`);
  url.searchParams.set('type', basis);
  url.searchParams.set('sdate', startDate);
  url.searchParams.set('edate', endDate);

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      Cookie: tokenInfo.cookieHeader,
      Origin: 'https://secure.copilotcrm.com',
      Referer: `https://secure.copilotcrm.com${COPILOT_TAX_SUMMARY_BASE_PATH}`,
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    },
  });
  if (!response.ok) {
    throw new Error(`Copilot tax summary returned ${response.status}`);
  }

  const html = await response.text();
  const parsed = parseCopilotTaxSummaryHtml(html, {
    startDate,
    endDate,
    basis,
    pageUrl: `${COPILOT_TAX_SUMMARY_BASE_PATH}?type=${basis}&sdate=${startDate}&edate=${endDate}`,
  });
  if (parsed?.parser_warning) {
    throw new Error(parsed.parser_warning);
  }
  const normalized = normalizeTaxSummarySnapshot({
    ...parsed,
    as_of: new Date().toISOString(),
    basis,
    start_date: startDate,
    end_date: endDate,
  }, LIVE_COPILOT_SOURCE);
  if (!normalized) throw new Error('Unable to parse Copilot Tax Summary');

  await persistTaxSummarySnapshot(normalized);
  cachedCopilotTaxSummaries.set(cacheKey, {
    value: normalized,
    expiresAt: Date.now() + COPILOT_TAX_SUMMARY_CACHE_TTL_MS,
  });
  return normalized;
}

async function computeBackendReconstructedTaxForDay(date) {
  const result = await pool.query(
    `SELECT
       p.amount,
       p.tip_amount,
       i.total AS invoice_total,
       i.tax_amount AS invoice_tax_amount
     FROM payments p
     LEFT JOIN invoices i ON p.invoice_id = i.id
     WHERE COALESCE(p.paid_at, p.created_at) >= $1::date
       AND COALESCE(p.paid_at, p.created_at) < ($1::date + INTERVAL '1 day')
       AND p.invoice_id IS NOT NULL`,
    [date]
  );
  return roundMoney(result.rows.reduce((sum, row) => {
    const appliedAmount = Math.min(
      Math.max((Number(row.amount) || 0) - (Number(row.tip_amount) || 0), 0),
      Number(row.invoice_total) || 0
    );
    if ((Number(row.invoice_total) || 0) <= 0 || (Number(row.invoice_tax_amount) || 0) <= 0) return sum;
    return sum + ((appliedAmount / Number(row.invoice_total)) * Number(row.invoice_tax_amount));
  }, 0));
}

async function persistCopilotPaymentsSnapshot(snapshot) {
  const normalized = normalizeCopilotPaymentsSnapshot(snapshot, LIVE_COPILOT_SOURCE);
  if (!normalized) return null;
  await pool.query(
    `INSERT INTO copilot_sync_settings (key, value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [COPILOT_PAYMENTS_SNAPSHOT_KEY, JSON.stringify(normalized)]
  );
  return normalized;
}

async function fetchCopilotPaymentsSnapshot({ pageSize = 100, maxPages = 25, forceRefresh = false } = {}) {
  const now = Date.now();
  if (!forceRefresh && cachedCopilotPayments?.expiresAt > now) return cachedCopilotPayments.value;

  const tokenInfo = await getCopilotToken();
  if (!tokenInfo?.cookieHeader) throw new Error('CopilotCRM authentication is not configured');

  const baseUrl = `https://secure.copilotcrm.com${COPILOT_PAYMENTS_BASE_PATH}?p=1&iop=${Math.max(1, Number(pageSize) || 100)}`;
  const queue = [baseUrl];
  const visited = new Set();
  const seenKeys = new Set();
  const payments = [];
  let total = null;
  let pagesFetched = 0;

  while (queue.length && pagesFetched < Math.max(1, Number(maxPages) || 25)) {
    const url = queue.shift();
    if (!url || visited.has(url)) continue;
    visited.add(url);
    pagesFetched += 1;

    const copilotRes = await fetch(url, {
      method: 'GET',
      headers: {
        Cookie: tokenInfo.cookieHeader,
        Origin: 'https://secure.copilotcrm.com',
        Referer: `https://secure.copilotcrm.com${COPILOT_PAYMENTS_BASE_PATH}`,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    });
    if (!copilotRes.ok) {
      throw new Error(`Copilot payments returned ${copilotRes.status}`);
    }

    const html = await copilotRes.text();
    const parsed = parseCopilotPaymentsHtml(html, url);
    if (parsed?.parser_warning) {
      throw new Error(parsed.parser_warning);
    }
    if (total == null && Number.isFinite(Number(parsed.total))) {
      total = Number(parsed.total);
    }

    parsed.payments.forEach((payment) => {
      if (seenKeys.has(payment.external_payment_key)) return;
      seenKeys.add(payment.external_payment_key);
      payments.push(payment);
    });

    parsed.page_paths.forEach((pagePath) => {
      const absolute = pagePath.startsWith('http')
        ? pagePath
        : `https://secure.copilotcrm.com${pagePath}`;
      if (!visited.has(absolute)) queue.push(absolute);
    });

    if (total && payments.length >= total) break;
  }

  const normalized = normalizeCopilotPaymentsSnapshot({
    as_of: new Date().toISOString(),
    total: total || payments.length,
    payments,
  }, LIVE_COPILOT_SOURCE);
  if (!normalized) throw new Error('Unable to parse Copilot payments');

  await persistCopilotPaymentsSnapshot(normalized).catch((error) => {
    console.error('Error persisting Copilot payments snapshot:', error);
  });

  cachedCopilotPayments = {
    value: normalized,
    expiresAt: Date.now() + COPILOT_PAYMENTS_CACHE_TTL_MS,
  };

  return normalized;
}

function buildPaymentRecordWhere({ search, year, month, source, linked } = {}, params) {
  const where = [];
  if (search) {
    params.push(`%${search}%`);
    where.push(`(
      COALESCE(p.customer_name, '') ILIKE $${params.length}
      OR COALESCE(i.invoice_number, '') ILIKE $${params.length}
      OR COALESCE(p.details, '') ILIKE $${params.length}
      OR COALESCE(p.notes, '') ILIKE $${params.length}
    )`);
  }
  if (year) {
    params.push(parseInt(year, 10));
    where.push(`EXTRACT(YEAR FROM COALESCE(p.paid_at, p.created_at)) = $${params.length}`);
  }
  if (month) {
    params.push(parseInt(month, 10));
    where.push(`EXTRACT(MONTH FROM COALESCE(p.paid_at, p.created_at)) = $${params.length}`);
  }
  if (source) {
    params.push(String(source));
    where.push(`COALESCE(p.external_source, 'database') = $${params.length}`);
  }
  if (linked === 'true') where.push('p.invoice_id IS NOT NULL');
  if (linked === 'false') where.push('p.invoice_id IS NULL');
  return where;
}

function hasValidStandingShape(standing) {
  if (!standing || typeof standing !== 'object') return false;
  return ACCOUNT_STANDING_KEYS.every((key) => Number.isFinite(Number(standing[key])));
}

function isMeaningfulStanding(standing) {
  if (!hasValidStandingShape(standing)) return false;
  const total = parseStandingCount(standing.total);
  if (total > 0) return true;
  return ACCOUNT_STANDING_KEYS.some((key) => parseStandingCount(standing[key]) > 0);
}

function normalizeStandingSnapshot(standing, source) {
  if (!hasValidStandingShape(standing)) return null;
  const normalized = { source, as_of: standing.as_of || new Date().toISOString() };
  for (const key of ACCOUNT_STANDING_KEYS) normalized[key] = parseStandingCount(standing[key]);
  return normalized;
}

async function readPersistedCopilotStanding() {
  try {
    const result = await pool.query(
      `SELECT value
         FROM copilot_sync_settings
        WHERE key = $1`,
      [COPILOT_STANDING_SNAPSHOT_KEY]
    );
    if (!result.rows[0]?.value) return null;
    return normalizeStandingSnapshot(JSON.parse(result.rows[0].value), 'copilot_snapshot');
  } catch (error) {
    return null;
  }
}

async function persistCopilotStanding(standing) {
  const normalized = normalizeStandingSnapshot(standing, 'copilot');
  if (!normalized || !isMeaningfulStanding(normalized)) return;
  await pool.query(
    `INSERT INTO copilot_sync_settings (key, value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [COPILOT_STANDING_SNAPSHOT_KEY, JSON.stringify(normalized)]
  );
}

function usesInvoiceDateAsDueDate(terms) {
  const normalized = cleanStatusValue(terms).toLowerCase();
  if (!normalized) return false;
  return (
    normalized.includes('due upon receipt') ||
    normalized.includes('due on receipt') ||
    normalized.includes('payable upon receipt') ||
    normalized.includes('payment due upon receipt') ||
    normalized.includes('payment due on receipt')
  );
}

function startOfDay(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return null;
  const normalized = new Date(date);
  normalized.setHours(0, 0, 0, 0);
  return normalized;
}

function parseAgingAnchorDate(inv, referenceDate = new Date()) {
  const dueDate = startOfDay(inv.due_date ? new Date(inv.due_date) : null);
  if (dueDate) return dueDate;

  if (usesInvoiceDateAsDueDate(inv.terms)) {
    const invoiceDate = startOfDay(inv.created_at ? new Date(inv.created_at) : null);
    if (invoiceDate) return invoiceDate;
  }

  return startOfDay(referenceDate);
}

function calculateAgingDays(inv, referenceDate = new Date()) {
  const anchorDate = parseAgingAnchorDate(inv, referenceDate);
  const today = startOfDay(referenceDate) || new Date();
  return Math.floor((today - anchorDate) / (1000 * 60 * 60 * 24));
}

function initAgingBuckets() {
  return {
    within_30: { count: 0, total: 0, invoices: [] },
    '31_60': { count: 0, total: 0, invoices: [] },
    '61_90': { count: 0, total: 0, invoices: [] },
    '90_plus': { count: 0, total: 0, invoices: [] },
  };
}

async function readPersistedCopilotAging() {
  try {
    const result = await pool.query(
      `SELECT value
         FROM copilot_sync_settings
        WHERE key = $1`,
      [COPILOT_AGING_SNAPSHOT_KEY]
    );
    if (!result.rows[0]?.value) return null;
    return normalizeAgingSnapshot(
      JSON.parse(result.rows[0].value),
      PERSISTED_COPILOT_SNAPSHOT_SOURCE
    );
  } catch (error) {
    return null;
  }
}

async function persistCopilotAging(snapshot) {
  const normalized = normalizeAgingSnapshot(snapshot, LIVE_COPILOT_SOURCE);
  if (!normalized) return;
  await pool.query(
    `INSERT INTO copilot_sync_settings (key, value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [COPILOT_AGING_SNAPSHOT_KEY, JSON.stringify(normalized)]
  );
}

function getAgingBucketKey(daysAged) {
  if (daysAged <= 30) return 'within_30';
  if (daysAged <= 60) return '31_60';
  if (daysAged <= 90) return '61_90';
  return '90_plus';
}

function calculateDaysFromInvoiceDate(invoiceDate, referenceDate = new Date()) {
  const invoiceDay = startOfDay(invoiceDate ? new Date(invoiceDate) : null);
  if (!invoiceDay) return null;
  const today = startOfDay(referenceDate) || new Date();
  return Math.max(0, Math.floor((today - invoiceDay) / (1000 * 60 * 60 * 24)));
}

function parseFallbackAgingDays(inv, referenceDate = new Date()) {
  const rawStatus = cleanStatusValue(inv.raw_status).toLowerCase();
  if (
    inv.external_source === 'copilotcrm' &&
    !inv.due_date &&
    (rawStatus.includes('past due') || rawStatus.includes('overdue') || cleanStatusValue(inv.status).toLowerCase() === 'overdue')
  ) {
    const invoiceAge = calculateDaysFromInvoiceDate(inv.created_at, referenceDate);
    if (Number.isFinite(invoiceAge)) return invoiceAge;
  }
  return Math.max(0, calculateAgingDays(inv, referenceDate));
}

async function fetchCopilotAgingBuckets({ pageSize = 100, maxPages = 150 } = {}) {
  const nowMs = Date.now();
  if (!cachedCopilotAging) {
    const persistedSnapshot = await readPersistedCopilotAging();
    if (persistedSnapshot) {
      cachedCopilotAging = {
        value: persistedSnapshot,
        expiresAt: getAgingSnapshotExpiry(persistedSnapshot),
      };
    }
  }
  if (cachedCopilotAging?.expiresAt > nowMs) {
    return cachedCopilotAging.value;
  }
  if (cachedCopilotAgingPromise) {
    return cachedCopilotAging ? cachedCopilotAging.value : cachedCopilotAgingPromise;
  }

  const refreshPromise = (async () => {
  if (typeof getCopilotToken !== 'function') return null;

  const tokenInfo = await getCopilotToken();
  if (!tokenInfo?.cookieHeader) return null;

  const buckets = initAgingBuckets();
  const now = new Date();
  const seenIds = new Set();
  const seenPageLeads = new Set();

  for (let page = 1; page <= maxPages; page += 1) {
    const formData = new URLSearchParams();
    formData.append('pagination[]', `p=${page}`);
    formData.append('pagination[]', `iop=${pageSize}`);
    formData.append('pagination[]', 'sort=datedesc');

    const copilotRes = await fetch('https://secure.copilotcrm.com/finances/invoices/getInvoicesListAjax', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': tokenInfo.cookieHeader,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest',
      },
      body: formData.toString(),
    });

    if (!copilotRes.ok) break;
    const payload = await copilotRes.json();
    const rows = parseInvoiceListHtml(payload.html || '');
    if (!rows.length) break;

    const pageLeadId = rows[0].external_invoice_id || rows[0].invoice_number || `page-${page}`;
    if (seenPageLeads.has(pageLeadId)) break;
    seenPageLeads.add(pageLeadId);

    for (const row of rows) {
      const id = String(row.external_invoice_id || row.invoice_number || '');
      if (!id || seenIds.has(id)) continue;
      seenIds.add(id);

      const totalDue = parseFloat(row.total_due || 0) || 0;
      if (totalDue <= 0) continue;
      if (!isOutstandingInvoice(row.status, row.total ?? totalDue, row.amount_paid ?? 0)) continue;

      const daysAged = calculateDaysFromInvoiceDate(row.invoice_date, now);
      if (!Number.isFinite(daysAged)) continue;

      const bucket = getAgingBucketKey(daysAged);
      buckets[bucket].count += 1;
      buckets[bucket].total += totalDue;
      buckets[bucket].invoices.push({
        external_invoice_id: row.external_invoice_id || null,
        invoice_number: row.invoice_number || null,
        customer_name: row.customer_name || null,
        balance: totalDue,
        days_overdue: daysAged,
        status: row.status || null,
      });
    }

    if (rows.length < pageSize) break;
  }

  const value = {
    success: true,
    source: LIVE_COPILOT_SOURCE,
    as_of: now.toISOString(),
    buckets,
  };
  await persistCopilotAging(value).catch((error) => {
    console.error('Error persisting Copilot aging snapshot:', error);
  });
  cachedCopilotAging = {
    value,
    expiresAt: Date.now() + COPILOT_AGING_CACHE_TTL_MS,
  };
  return value;
  })();
  cachedCopilotAgingPromise = refreshPromise.finally(() => {
    cachedCopilotAgingPromise = null;
  });

  try {
    if (cachedCopilotAging) {
      return cachedCopilotAging.value;
    }
    return await cachedCopilotAgingPromise;
  } catch (error) {
    if (cachedCopilotAging) return cachedCopilotAging.value;
    throw error;
  }
}

function deriveDbAccountStanding(rows) {
  const standing = { total: 0, outstanding: 0, paid: 0, past_due: 0, credit: 0 };
  const creditCustomers = new Set();
  const now = new Date();

  for (const inv of rows || []) {
    const effectiveStatus = normalizeStoredInvoiceStatus(inv.status, inv.due_date, inv.total, inv.amount_paid);
    const balance = outstandingBalance(inv);
    const daysAged = calculateAgingDays(inv, now);
    const isOutstanding = isOutstandingInvoice(effectiveStatus, inv.total, inv.amount_paid);
    standing.total += 1;
    if (isOutstanding) standing.outstanding += 1;
    if (!isOutstanding && balance <= 0 && !['void', 'draft'].includes(effectiveStatus)) standing.paid += 1;
    if (balance > 0 && daysAged > 0 && !['paid', 'void', 'draft'].includes(effectiveStatus)) standing.past_due += 1;

    const creditAvailable = parseFloat(inv.credit_available || 0);
    if (creditAvailable > 0) {
      const customerKey = String(
        inv.copilot_customer_id ||
        inv.customer_id ||
        inv.customer_email ||
        inv.customer_name ||
        inv.id
      );
      creditCustomers.add(customerKey);
    }
  }

  standing.credit = creditCustomers.size;
  return standing;
}

async function fetchCopilotCreditCount(cookieHeader, { pageSize = 100, maxPages = 150 } = {}) {
  const creditedCustomers = new Set();
  const seenFirstIds = new Set();

  for (let page = 1; page <= maxPages; page += 1) {
    const formData = new URLSearchParams();
    formData.append('pagination[]', `p=${page}`);
    formData.append('pagination[]', `iop=${pageSize}`);
    formData.append('pagination[]', 'sort=datedesc');

    const copilotRes = await fetch('https://secure.copilotcrm.com/finances/invoices/getInvoicesListAjax', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': cookieHeader,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest',
      },
      body: formData.toString(),
    });

    if (!copilotRes.ok) break;
    const data = await copilotRes.json();
    const html = data.html || '';
    const rows = [];
    const $ = cheerio.load(html);
    $('tbody tr').each((_, row) => {
      const tds = $(row).find('td');
      if (tds.length < 12) return;
      const invoiceNumber = tds.eq(0).text().replace(/\s+/g, ' ').trim();
      const firstLink = $(row).find('a[href*="/finances/invoices/view/"]').first();
      const firstId = firstLink.attr('href')?.match(/\/view\/(\d+)/)?.[1] || invoiceNumber;
      rows.push({
        firstId,
        customerKey: $(row).find('a[href*="/customers/details/"]').first().attr('href')?.match(/\/details\/(\d+)/)?.[1]
          || tds.eq(2).text().replace(/\s+/g, ' ').trim(),
        creditAvailable: parseFloat((tds.eq(9).text() || '').replace(/[^0-9.-]/g, '')) || 0,
      });
    });

    if (!rows.length) break;
    if (rows[0].firstId && seenFirstIds.has(rows[0].firstId)) break;
    if (rows[0].firstId) seenFirstIds.add(rows[0].firstId);

    rows.forEach(row => {
      if (row.creditAvailable > 0 && row.customerKey) creditedCustomers.add(String(row.customerKey));
    });

    if (rows.length < pageSize) break;
  }

  return creditedCustomers.size;
}

async function fetchCopilotAccountStanding() {
  const persistedStanding = await readPersistedCopilotStanding();
  if (typeof getCopilotToken !== 'function') return persistedStanding;

  const now = Date.now();
  if (cachedCopilotStanding && cachedCopilotStanding.expiresAt > now) {
    return cachedCopilotStanding.value;
  }

  const tokenInfo = await getCopilotToken();
  if (!tokenInfo?.cookieHeader) return persistedStanding;

  const copilotRes = await fetch('https://secure.copilotcrm.com/finances/invoices', {
    headers: {
      'Cookie': tokenInfo.cookieHeader,
      'Referer': 'https://secure.copilotcrm.com/',
    },
  });
  if (!copilotRes.ok) return persistedStanding;

  const html = await copilotRes.text();
  const $ = cheerio.load(html);
  const pillValues = {};
  $('.page-stat-pill').each((_, pill) => {
    const title = $(pill).find('.page-stat-title').text().replace(/\s+/g, ' ').trim().toLowerCase();
    const value = normalizeCount($(pill).find('.page-stat-value').text());
    if (!title) return;
    pillValues[title] = value;
  });
  if (Object.keys(pillValues).length === 0) return persistedStanding;

  let credit = 0;
  if (Object.prototype.hasOwnProperty.call(pillValues, 'credit')) {
    credit = pillValues.credit;
  } else {
    try {
      credit = await fetchCopilotCreditCount(tokenInfo.cookieHeader);
    } catch (error) {
      credit = parseStandingCount(persistedStanding?.credit);
    }
  }

  const value = normalizeStandingSnapshot({
    source: 'copilot',
    as_of: new Date().toISOString(),
    total: pillValues.total || 0,
    outstanding: pillValues.outstanding || 0,
    paid: pillValues.paid || 0,
    past_due: pillValues['past due'] || 0,
    credit,
  }, 'copilot');
  if (!value) return persistedStanding;
  if (!isMeaningfulStanding(value) && isMeaningfulStanding(persistedStanding)) return persistedStanding;

  await persistCopilotStanding(value).catch((error) => {
    console.error('Error persisting Copilot account standing:', error);
  });

  cachedCopilotStanding = {
    value,
    expiresAt: now + (60 * 1000),
  };

  return value;
}

// GET /api/payments - List received payments (paid/partial invoices)
router.get('/api/payments', async (req, res) => {
  try {
    const { search, year, month, limit = 200, offset = 0 } = req.query;

    const params = [];
    const where = ['amount_paid > 0'];
    let p = 1;

    if (search) {
      where.push(`(customer_name ILIKE $${p} OR invoice_number ILIKE $${p})`);
      params.push('%' + search + '%'); p++;
    }
    if (year) {
      where.push(`EXTRACT(YEAR FROM COALESCE(paid_at, updated_at)) = $${p}`);
      params.push(parseInt(year)); p++;
    }
    if (month) {
      where.push(`EXTRACT(MONTH FROM COALESCE(paid_at, updated_at)) = $${p}`);
      params.push(parseInt(month)); p++;
    }

    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    params.push(parseInt(limit)); params.push(parseInt(offset));

    const [result, countResult, monthly] = await Promise.all([
      pool.query(
        `SELECT id, invoice_number, customer_id, customer_name, customer_email,
                total, amount_paid, status, paid_at, due_date, created_at, qb_invoice_id, payment_token
         FROM invoices ${whereClause}
         ORDER BY COALESCE(paid_at, updated_at) DESC
         LIMIT $${p} OFFSET $${p+1}`,
        params
      ),
      pool.query(
        `SELECT COUNT(*) as cnt, COALESCE(SUM(amount_paid),0) as total_received
         FROM invoices ${whereClause}`,
        params.slice(0, -2)
      ),
      pool.query(`
        SELECT to_char(COALESCE(paid_at, updated_at),'YYYY-MM') as month,
               COUNT(*) as count, SUM(amount_paid) as total
        FROM invoices
        WHERE amount_paid > 0 AND COALESCE(paid_at, updated_at) >= NOW() - INTERVAL '12 months'
        GROUP BY month ORDER BY month
      `)
    ]);

    res.json({
      success: true,
      payments: result.rows,
      total: parseInt(countResult.rows[0].cnt),
      totalReceived: parseFloat(countResult.rows[0].total_received),
      monthly: monthly.rows
    });
  } catch (e) {
    console.error('Payments API error:', e);
    serverError(res, e);
  }
});

// POST /api/copilot/payments/sync - Sync live Copilot payments into payment records
router.post('/api/copilot/payments/sync', authenticateToken, async (req, res) => {
  try {
    const snapshot = await fetchCopilotPaymentsSnapshot({
      pageSize: Number(req.body?.pageSize || req.query.pageSize || 100),
      maxPages: Number(req.body?.maxPages || req.query.maxPages || 25),
      forceRefresh: req.body?.force !== false && req.query.force !== 'false',
    });
    const syncResult = await upsertCopilotPayments({
      pool,
      payments: snapshot.payments,
    });

    res.json({
      success: true,
      source: snapshot.source,
      as_of: snapshot.as_of,
      total: snapshot.total,
      pages: Math.ceil((snapshot.total || snapshot.payments.length || 0) / Math.max(1, Number(req.body?.pageSize || req.query.pageSize || 100))),
      sync: {
        total: syncResult.total,
        inserted: syncResult.inserted,
        updated: syncResult.updated,
        linked: syncResult.linked,
        unresolved: syncResult.unresolved,
      },
      unresolved: syncResult.payments.filter((payment) => payment.link_status === 'unresolved'),
    });
  } catch (error) {
    console.error('Copilot payments sync error:', error);
    serverError(res, error);
  }
});

// GET /api/payment-records - True payment records joined to invoices
router.get('/api/payment-records', authenticateToken, async (req, res) => {
  try {
    const { search, year, month, source, linked, limit = 200, offset = 0 } = req.query;
    const params = [];
    const where = buildPaymentRecordWhere({ search, year, month, source, linked }, params);
    const whereClause = where.length ? `WHERE ${where.join(' AND ')}` : '';

    params.push(parseInt(limit, 10));
    params.push(parseInt(offset, 10));

    const selectSql = `
      SELECT
        p.id,
        p.payment_id,
        p.invoice_id,
        p.customer_id,
        p.customer_name,
        p.amount,
        p.tip_amount,
        p.method,
        p.status,
        p.details,
        p.notes,
        p.paid_at,
        p.created_at,
        p.source_date_raw,
        p.external_source,
        p.external_payment_key,
        p.external_metadata,
        p.imported_at,
        i.invoice_number,
        i.total AS invoice_total,
        i.tax_amount AS invoice_tax_amount,
        i.external_source AS invoice_external_source,
        i.external_invoice_id
      FROM payments p
      LEFT JOIN invoices i ON p.invoice_id = i.id
      ${whereClause}
      ORDER BY COALESCE(p.paid_at, p.created_at) DESC, p.id DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `;

    const [rowsResult, countResult] = await Promise.all([
      pool.query(selectSql, params),
      pool.query(
        `SELECT COUNT(*)::int AS count
           FROM payments p
           LEFT JOIN invoices i ON p.invoice_id = i.id
           ${whereClause}`,
        params.slice(0, -2)
      ),
    ]);

    const payments = rowsResult.rows.map(hydratePaymentRecord);
    res.json({
      success: true,
      payments,
      total: countResult.rows[0]?.count || 0,
    });
  } catch (error) {
    console.error('Payment records API error:', error);
    serverError(res, error);
  }
});

// GET /api/reports/tax-sweep - Reconciliation report from payment records only
router.get('/api/reports/tax-sweep', authenticateToken, async (req, res) => {
  try {
    const { start_date, end_date, source = 'copilotcrm' } = req.query;
    if (!start_date || !end_date) {
      return res.status(400).json({ success: false, error: 'start_date and end_date are required' });
    }

    const result = await pool.query(
      `SELECT
         p.id,
         p.payment_id,
         p.invoice_id,
         p.customer_name,
         p.amount,
         p.tip_amount,
         p.method,
         p.status,
         p.details,
         p.notes,
         p.paid_at,
         p.created_at,
         p.source_date_raw,
         p.external_source,
         p.external_payment_key,
         p.external_metadata,
         p.imported_at,
         i.invoice_number,
         i.total AS invoice_total,
         i.tax_amount AS invoice_tax_amount
       FROM payments p
       LEFT JOIN invoices i ON p.invoice_id = i.id
       WHERE COALESCE(p.paid_at, p.created_at) >= $1::date
         AND COALESCE(p.paid_at, p.created_at) < ($2::date + INTERVAL '1 day')
         AND COALESCE(p.external_source, 'database') = $3
       ORDER BY COALESCE(p.paid_at, p.created_at) DESC, p.id DESC`,
      [start_date, end_date, source]
    );

    const payments = result.rows.map(hydratePaymentRecord);
    const summary = payments.reduce((acc, payment) => {
      acc.payment_count += 1;
      acc.gross_collected = roundMoney(acc.gross_collected + payment.amount);
      acc.tip_amount = roundMoney(acc.tip_amount + payment.tip_amount);
      acc.applied_amount = roundMoney(acc.applied_amount + payment.applied_amount);
      acc.tax_portion_collected = roundMoney(acc.tax_portion_collected + payment.tax_portion_collected);
      if (payment.invoice_id) acc.linked_count += 1;
      else acc.unresolved_count += 1;
      return acc;
    }, {
      payment_count: 0,
      linked_count: 0,
      unresolved_count: 0,
      gross_collected: 0,
      tip_amount: 0,
      applied_amount: 0,
      tax_portion_collected: 0,
    });

    res.json({
      success: true,
      mode: 'reconciliation_only',
      start_date,
      end_date,
      source,
      summary,
      payments,
    });
  } catch (error) {
    console.error('Tax sweep report error:', error);
    serverError(res, error);
  }
});

// POST /api/copilot/tax-summary/sync - Persist daily Copilot Tax Summary collected snapshots
router.post('/api/copilot/tax-summary/sync', authenticateToken, async (req, res) => {
  try {
    const basis = 'collected';
    const startDate = String(req.body?.start_date || req.query.start_date || '').trim();
    const endDate = String(req.body?.end_date || req.query.end_date || startDate).trim();
    if (!startDate || !endDate) {
      return res.status(400).json({ success: false, error: 'start_date and end_date are required' });
    }

    const dates = eachDayInclusive(startDate, endDate);
    if (dates.length > 31) {
      return res.status(400).json({ success: false, error: 'date range must be 31 days or fewer' });
    }

    const snapshots = [];
    for (const date of dates) {
      const snapshot = await fetchCopilotTaxSummarySnapshot({
        startDate: date,
        endDate: date,
        basis,
        forceRefresh: req.body?.force !== false && req.query.force !== 'false',
      });
      snapshots.push({
        date,
        source: snapshot.source,
        snapshot_as_of: snapshot.as_of,
        total_sales: snapshot.total_sales,
        taxable_amount: snapshot.taxable_amount,
        discount: snapshot.discount,
        tax_amount: snapshot.tax_amount,
        processing_fees: snapshot.processing_fees,
        tips: snapshot.tips,
        row_count: snapshot.rows.length,
      });
    }

    res.json({
      success: true,
      basis,
      start_date: startDate,
      end_date: endDate,
      count: snapshots.length,
      snapshots,
    });
  } catch (error) {
    console.error('Copilot tax summary sync error:', error);
    serverError(res, error);
  }
});

// GET /api/reports/tax-transfer-daily - Daily Copilot-vs-backend tax reconciliation
router.get('/api/reports/tax-transfer-daily', authenticateToken, async (req, res) => {
  try {
    const basis = 'collected';
    const startDate = String(req.query.start_date || '').trim();
    const endDate = String(req.query.end_date || startDate).trim();
    if (!startDate || !endDate) {
      return res.status(400).json({ success: false, error: 'start_date and end_date are required' });
    }

    const dates = eachDayInclusive(startDate, endDate);
    if (dates.length > 31) {
      return res.status(400).json({ success: false, error: 'date range must be 31 days or fewer' });
    }

    const days = [];
    for (const date of dates) {
      let snapshot = await readPersistedTaxSummarySnapshot({ startDate: date, endDate: date, basis }).catch(() => null);
      if (!snapshot) {
        snapshot = await fetchCopilotTaxSummarySnapshot({ startDate: date, endDate: date, basis }).catch(() => null);
      }
      if (!snapshot) {
        const backendReconstructedTax = await computeBackendReconstructedTaxForDay(date);
        const recommendation = buildDailyTaxRecommendation({
          snapshot: { tax_amount: 0 },
          backendReconstructedTax,
        });
        days.push({
          date,
          ...recommendation,
          source: 'missing_copilot_snapshot',
          snapshot_as_of: null,
          total_sales: 0,
          taxable_amount: 0,
          discount: 0,
          processing_fees: 0,
          tips: 0,
          tax_rows: [],
        });
        continue;
      }

      const backendReconstructedTax = await computeBackendReconstructedTaxForDay(date);
      const recommendation = buildDailyTaxRecommendation({
        snapshot,
        backendReconstructedTax,
      });

      days.push({
        date,
        ...recommendation,
        source: snapshot.source,
        snapshot_as_of: snapshot.as_of,
        total_sales: snapshot.total_sales,
        taxable_amount: snapshot.taxable_amount,
        discount: snapshot.discount,
        processing_fees: snapshot.processing_fees,
        tips: snapshot.tips,
        tax_rows: snapshot.rows,
      });
    }

    const summary = days.reduce((acc, day) => {
      acc.recommended_transfer_amount = roundMoney(acc.recommended_transfer_amount + (day.recommended_transfer_amount || 0));
      acc.copilot_collected_tax = roundMoney(acc.copilot_collected_tax + (day.copilot_collected_tax || 0));
      acc.backend_reconstructed_tax = roundMoney(acc.backend_reconstructed_tax + (day.backend_reconstructed_tax || 0));
      acc.variance = roundMoney(acc.variance + (day.variance || 0));
      return acc;
    }, {
      recommended_transfer_amount: 0,
      copilot_collected_tax: 0,
      backend_reconstructed_tax: 0,
      variance: 0,
    });

    res.json({
      success: true,
      basis,
      start_date: startDate,
      end_date: endDate,
      days,
      summary,
    });
  } catch (error) {
    console.error('Tax transfer daily report error:', error);
    serverError(res, error);
  }
});

// GET /api/invoices - List invoices
router.get('/api/invoices', async (req, res) => {
  try {
    const { status, customer_id, search, limit = 25000, offset = 0, include_blank_drafts } = req.query;
    // Build a real display name. The customers table is inconsistent —
    // some rows have `name` populated, others only `first_name`+`last_name`,
    // and many invoices have a stale or missing customer_id. Walk the chain:
    //   1. customers.name (when non-empty)
    //   2. customers.first_name + ' ' + customers.last_name (when populated)
    //   3. invoices.customer_name (the value captured on the invoice itself)
    // We also expose the raw linked + invoice values separately so callers
    // can tell whether the invoice is linked to a real customer record.
    let q = `SELECT i.*,
                    COALESCE(
                      NULLIF(c.name, ''),
                      NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), ''),
                      NULLIF(i.customer_name, '')
                    ) AS customer_name,
                    c.name AS linked_customer_name,
                    i.customer_name AS invoice_customer_name,
                    CASE
                      WHEN COALESCE(NULLIF(TRIM(i.invoice_number), ''), '') = ''
                        OR i.invoice_number ~ '^[A-Z][a-z]{2} [0-9]{2}, [0-9]{4}$'
                        OR i.invoice_number ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$'
                        OR i.invoice_number ~ '^\\d{4}-\\d{2}-\\d{2}$'
                      THEN NULLIF(CASE WHEN i.external_source = 'copilotcrm' THEN i.external_invoice_id ELSE NULL END, '')
                      ELSE i.invoice_number
                    END AS display_invoice_number
             FROM invoices i
             LEFT JOIN customers c ON i.customer_id = c.id`;
    const params = [];
    const where = [];
    if (status) {
      if (status === 'overdue') {
        where.push(`COALESCE(i.total, 0) > COALESCE(i.amount_paid, 0)`);
        where.push(`(
          COALESCE(LOWER(TRIM(i.status)), '') = 'overdue'
          OR (
            COALESCE(LOWER(TRIM(i.status)), '') IN ('sent', 'pending', '')
            AND i.due_date IS NOT NULL
            AND i.due_date < CURRENT_DATE
          )
        )`);
      } else {
        params.push(status);
        where.push(`i.status = $${params.length}`);
      }
    }
    if (customer_id) { params.push(customer_id); where.push(`i.customer_id = $${params.length}`); }
    // Hide blank placeholder drafts (no customer name + no money + no
    // line items). They were dominating the top of the list and making
    // the page look broken. Pass include_blank_drafts=true to see them.
    // Doesn't apply when an explicit status filter is set, so drilling
    // into "Draft" still shows everything.
    if (!status && !include_blank_drafts) {
      where.push(`NOT (
        i.status = 'draft'
        AND COALESCE(TRIM(i.customer_name), '') = ''
        AND COALESCE(i.total, 0) = 0
        AND jsonb_array_length(COALESCE(i.line_items, '[]'::jsonb)) = 0
      )`);
    }
    if (search) {
      params.push(`%${search}%`);
      // Search the same name sources used to build the display name above:
      // invoice number, customers.name, customers.first_name+last_name, and
      // the invoice's own captured customer_name. This keeps search results
      // consistent with what the user actually sees in the list.
      const p = params.length;
      where.push(`(
        i.invoice_number ILIKE $${p}
        OR c.name ILIKE $${p}
        OR TRIM(CONCAT_WS(' ', c.first_name, c.last_name)) ILIKE $${p}
        OR i.customer_name ILIKE $${p}
      )`);
    }
    if (where.length) q += ' WHERE ' + where.join(' AND ');
    q += ' ORDER BY i.created_at DESC';
    params.push(limit); q += ` LIMIT $${params.length}`;
    params.push(offset); q += ` OFFSET $${params.length}`;
    const result = await pool.query(q, params);
    res.json({ success: true, invoices: result.rows });
  } catch (error) {
    console.error('Error fetching invoices:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/stats - Invoice statistics
router.get('/api/invoices/stats', async (req, res) => {
  try {
    const all = await pool.query(`
      SELECT status, total, amount_paid, due_date, paid_at, created_at, terms,
             customer_id, customer_name, customer_email,
             external_metadata->>'copilot_customer_id' AS copilot_customer_id,
             COALESCE((external_metadata->>'credit_available')::numeric, 0) AS credit_available
      FROM invoices
    `);
    const now = new Date();
    const thisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    let stats = { total: 0, draft: 0, pending: 0, partial: 0, sent: 0, paid: 0, overdue: 0, void: 0,
      outstanding: 0, overdueAmount: 0, paidThisMonth: 0, totalRevenue: 0 };
    all.rows.forEach(inv => {
      const effectiveStatus = normalizeStoredInvoiceStatus(inv.status, inv.due_date, inv.total, inv.amount_paid);
      const balance = outstandingBalance(inv);
      stats.total++;
      stats[effectiveStatus] = (stats[effectiveStatus] || 0) + 1;
      const t = parseFloat(inv.total) || 0;
      if (effectiveStatus === 'paid') {
        stats.totalRevenue += t;
        if (inv.paid_at && new Date(inv.paid_at) >= thisMonth) stats.paidThisMonth += t;
      }
      if (isOutstandingInvoice(effectiveStatus, inv.total, inv.amount_paid)) {
        stats.outstanding += balance;
      }
      if (effectiveStatus === 'overdue' && balance > 0) {
        stats.overdueAmount += balance;
      }
    });
    const fallbackStanding = {
      ...deriveDbAccountStanding(all.rows),
      source: 'database',
      as_of: new Date().toISOString(),
    };
    let accountStanding = fallbackStanding;
    try {
      const copilotStanding = await fetchCopilotAccountStanding();
      if (copilotStanding) accountStanding = copilotStanding;
    } catch (error) {
      console.error('Error fetching Copilot account standing:', error);
    }
    stats.account_standing = accountStanding;
    stats.account_standing_fallback = fallbackStanding;
    res.json({ success: true, stats });
  } catch (error) {
    console.error('Error fetching invoice stats:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/aging - Aging AR (must be before :id route)
router.get('/api/invoices/aging', async (req, res) => {
  try {
    try {
      const copilotAging = await fetchCopilotAgingBuckets();
      if (copilotAging?.buckets) {
        console.info('Invoice aging source', {
          source: copilotAging.source,
          asOf: copilotAging.as_of,
        });
        return res.json(copilotAging);
      }
    } catch (error) {
      console.error('Error fetching Copilot aging buckets:', error);
    }

    const result = await pool.query(`
      SELECT id, invoice_number, customer_name, total, amount_paid, due_date, created_at, terms,
             status, sent_status, external_source, external_metadata,
             external_metadata->>'raw_status' AS raw_status
      FROM invoices
      WHERE total > COALESCE(amount_paid, 0)
        AND COALESCE(status, 'draft') NOT IN ('paid', 'void', 'draft')
        AND (external_source = 'copilotcrm' OR external_invoice_id IS NOT NULL)
    `);
    const now = new Date();
    const buckets = initAgingBuckets();
    result.rows.forEach(inv => {
      const effectiveStatus = normalizeStoredInvoiceStatus(inv.status, inv.due_date, inv.total, inv.amount_paid);
      if (!isOutstandingInvoice(effectiveStatus, inv.total, inv.amount_paid)) return;
      const balance = outstandingBalance(inv);
      const daysOverdue = parseFallbackAgingDays(inv, now);
      const bucket = getAgingBucketKey(daysOverdue);
      buckets[bucket].count++;
      buckets[bucket].total += balance;
      buckets[bucket].invoices.push({
        id: inv.id,
        invoice_number: inv.invoice_number,
        customer_name: inv.customer_name,
        balance,
        days_overdue: daysOverdue,
        status: effectiveStatus,
      });
    });
    console.warn('Invoice aging fallback', {
      source: DATABASE_FALLBACK_SOURCE,
      asOf: now.toISOString(),
    });
    res.json({ success: true, source: DATABASE_FALLBACK_SOURCE, as_of: now.toISOString(), buckets });
  } catch (error) {
    console.error('Error fetching aging data:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/batch - Batch invoice from jobs (must be before :id route)
router.post('/api/invoices/batch', async (req, res) => {
  try {
    const { job_ids } = req.body;
    if (!job_ids || !Array.isArray(job_ids) || job_ids.length === 0) {
      return res.status(400).json({ success: false, error: 'job_ids array is required' });
    }
    const created = [];
    for (const jobId of job_ids) {
      const jobResult = await pool.query('SELECT * FROM scheduled_jobs WHERE id = $1', [jobId]);
      if (jobResult.rows.length === 0) continue;
      const job = jobResult.rows[0];
      if (job.invoice_id) continue;
      const invNum = await nextInvoiceNumber();
      const dueDate = new Date();
      dueDate.setDate(dueDate.getDate() + 30);
      const lineItems = [{ description: job.service_type || 'Service', amount: parseFloat(job.service_price || 0) }];
      const total = parseFloat(job.service_price || 0);
      let customerEmail = '';
      if (job.customer_id) {
        const custResult = await pool.query('SELECT email FROM customers WHERE id = $1', [job.customer_id]);
        if (custResult.rows.length > 0) customerEmail = custResult.rows[0].email || '';
      }
      const r = await pool.query(`INSERT INTO invoices
        (invoice_number, customer_id, customer_name, customer_email, customer_address, job_id,
         subtotal, total, due_date, line_items, notes)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
        [invNum, job.customer_id || null, job.customer_name || '', customerEmail,
         job.address || '', jobId, total, total, dueDate.toISOString().split('T')[0],
         JSON.stringify(lineItems), 'Generated from completed job #' + jobId]);
      try { await pool.query('UPDATE scheduled_jobs SET invoice_id = $1 WHERE id = $2', [r.rows[0].id, jobId]); } catch(e) {}
      created.push(r.rows[0]);
    }
    res.json({ success: true, invoices: created, count: created.length });
  } catch (error) {
    console.error('Error batch creating invoices:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/:id - Single invoice
router.get('/api/invoices/:id', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = r.rows[0];
    // Fetch payment history
    try {
      const payments = await pool.query(
        'SELECT id, payment_id, amount, method, status, card_brand, card_last4, square_receipt_url, ach_bank_name, notes, paid_at, created_at FROM payments WHERE invoice_id = $1 ORDER BY COALESCE(paid_at, created_at) DESC, id DESC',
        [inv.id]
      );
      inv.payment_history = payments.rows;
    } catch(e) { inv.payment_history = []; }
    try {
      const emailLog = await pool.query(
        `SELECT id, recipient_email, subject, email_type, status, error_message, sent_at
           FROM email_log
          WHERE invoice_id = $1
          ORDER BY sent_at DESC, id DESC`,
        [inv.id]
      );
      inv.email_history = emailLog.rows;
    } catch (e) { inv.email_history = []; }
    inv.history_events = buildInvoiceHistoryEvents(inv, {
      payments: inv.payment_history,
      emailLog: inv.email_history,
    });
    res.json({ success: true, invoice: inv });
  } catch (error) {
    serverError(res, error);
  }
});

// POST /api/invoices - Create invoice
router.post('/api/invoices', validate(schemas.createInvoice), async (req, res) => {
  try {
    const { customer_id, customer_name, customer_email, customer_address, sent_quote_id, job_id,
      subtotal, tax_rate, tax_amount, total, due_date, notes, line_items, draft_autosave } = req.body;

    // Reject blank invoices outright. The schema validator allows things
    // like customer_name=' ', total=0, line_items=[] which historically
    // produced "junk" placeholder drafts that dominated the list. A real
    // invoice needs a non-blank customer name AND either a positive total
    // OR at least one non-empty line item.
    const trimmedName = (customer_name || '').toString().trim();
    const items = Array.isArray(line_items) ? line_items : [];
    const meaningfulItems = items.filter(it =>
      it && (
        (typeof it.description === 'string' && it.description.trim() !== '') ||
        parseFloat(it.amount || 0) > 0
      )
    );
    const meaningfulTotal = parseFloat(total || 0) > 0;
    const isBlank = !trimmedName && !meaningfulTotal && meaningfulItems.length === 0;
    if (isBlank && !draft_autosave) {
      return res.status(400).json({
        success: false,
        error: 'Invoice is empty. Add a customer and at least one line item before saving. Pass draft_autosave: true to bypass for autosave flows.',
      });
    }

    const invNum = await nextInvoiceNumber();
    const r = await pool.query(`INSERT INTO invoices
      (invoice_number, customer_id, customer_name, customer_email, customer_address, sent_quote_id, job_id,
       subtotal, tax_rate, tax_amount, total, due_date, notes, line_items)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING *`,
      [invNum, customer_id||null, customer_name, customer_email, customer_address||'',
       sent_quote_id||null, job_id||null, subtotal||0, tax_rate||0, tax_amount||0, total||0,
       due_date||null, notes||'', JSON.stringify(line_items||[])]);
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    console.error('Error creating invoice:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/from-quote/:quoteId - Create invoice from signed quote
router.post('/api/invoices/from-quote/:quoteId', async (req, res) => {
  try {
    const q = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.quoteId]);
    if (q.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    const quote = q.rows[0];
    const services = typeof quote.services === 'string' ? JSON.parse(quote.services) : (quote.services || []);
    const lineItems = services.map(s => ({ description: s.name || s.description, amount: parseFloat(s.price || s.amount || 0) }));
    const invNum = await nextInvoiceNumber();
    const dueDate = new Date(); dueDate.setDate(dueDate.getDate() + 30);
    const r = await pool.query(`INSERT INTO invoices
      (invoice_number, customer_id, customer_name, customer_email, customer_address, sent_quote_id,
       subtotal, tax_rate, tax_amount, total, due_date, line_items, notes)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
      [invNum, quote.customer_id||null, quote.customer_name, quote.customer_email, quote.customer_address||'',
       quote.id, parseFloat(quote.subtotal)||0, 0, parseFloat(quote.tax_amount)||0,
       parseFloat(quote.total)||0, dueDate.toISOString().split('T')[0], JSON.stringify(lineItems),
       `Generated from Quote ${quote.quote_number || 'Q-'+quote.id}`]);
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    console.error('Error creating invoice from quote:', error);
    serverError(res, error);
  }
});

// PATCH /api/invoices/:id - Update invoice
router.patch('/api/invoices/:id', async (req, res) => {
  try {
    const fields = ['status','sent_status','customer_name','customer_email','customer_address','subtotal','tax_rate','tax_amount','total','amount_paid','due_date','paid_at','sent_at','qb_invoice_id','notes','terms','line_items'];
    const sets = []; const params = [];
    fields.forEach(f => {
      if (req.body[f] !== undefined) {
        params.push(f === 'line_items' ? JSON.stringify(req.body[f]) : req.body[f]);
        sets.push(`${f} = $${params.length}`);
      }
    });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields to update' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    params.push(req.params.id);
    const r = await pool.query(`UPDATE invoices SET ${sets.join(', ')} WHERE id = $${params.length} RETURNING *`, params);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    console.error('Error updating invoice:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/:id/send - Email invoice to customer
router.post('/api/invoices/:id/send', async (req, res) => {
  try {
    const r = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = r.rows[0];
    if (!inv.customer_email) return res.status(400).json({ success: false, error: 'No customer email' });

    // Generate payment token if needed
    let paymentToken = inv.payment_token;
    if (!paymentToken) {
      paymentToken = generateToken();
      await pool.query('UPDATE invoices SET payment_token = $1, payment_token_created_at = CURRENT_TIMESTAMP WHERE id = $2', [paymentToken, inv.id]);
    }
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const payUrl = `${baseUrl}/pay-invoice.html?token=${paymentToken}`;

    const firstName = (inv.customer_name || '').split(' ')[0] || 'there';
    const totalFormatted = '$' + parseFloat(inv.total).toFixed(2);
    const content = `
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 16px;">Hi ${firstName},</p>
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 16px;">Thank you for allowing <strong>Pappas & Co. Landscaping</strong> to care for your property!</p>
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 24px;"><strong>Your latest invoice is ready for review and payment.</strong> You can access your invoice and make an online payment by clicking the secure button below:</p>

      <div style="text-align:center;margin:28px 0 32px;">
        <a href="${payUrl}" style="display:inline-block;padding:16px 48px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">
          View &amp; Pay Invoice &mdash; ${totalFormatted}
        </a>
      </div>

      <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:20px;margin:0 0 24px;">
        <p style="font-weight:700;color:#1e293b;font-size:14px;margin:0 0 12px;">Payment Reminders:</p>
        <ul style="margin:0;padding:0 0 0 20px;color:#475569;font-size:14px;line-height:1.8;">
          <li><strong>Online Payment:</strong> The fastest and easiest way to pay is directly through the secure invoice link above. We accept <strong>credit/debit cards</strong>, <strong>Apple Pay</strong>, and <strong>bank transfers (ACH)</strong>.</li>
          <li><strong>Mail a Check:</strong> Checks can be made payable to <strong>Pappas & Co. Landscaping</strong> and mailed to our secure payment box: <strong>PO Box 770057, Lakewood, OH 44107</strong>.</li>
          <li><strong>Zelle Payments:</strong> If you prefer to pay via Zelle, please ensure you are sending funds to: <strong>hello@pappaslandscaping.com</strong>.</li>
        </ul>
      </div>

      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 16px;">We truly appreciate your business and look forward to continuing to provide top-quality service.</p>
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 4px;">If you have any questions or concerns about your service or the invoice, please don't hesitate to reach out.</p>
    `;

    let attachments = null;
    try {
      const pdfResult = await generateInvoicePDF(inv);
      if (pdfResult && pdfResult.bytes) {
        attachments = [{
          filename: `invoice-${inv.invoice_number || inv.id}.pdf`,
          content: Buffer.from(pdfResult.bytes).toString('base64'),
          type: 'application/pdf'
        }];
      }
    } catch (pdfErr) { console.error('Invoice PDF error:', pdfErr); }

    await sendEmail(inv.customer_email, `Invoice ${inv.invoice_number} from Pappas & Co.`, emailTemplate(content), attachments, { type: 'invoice', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });
    await pool.query("UPDATE invoices SET status = 'sent', sent_status = 'sent', sent_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1", [inv.id]);
    res.json({ success: true, message: 'Invoice sent' });
  } catch (error) {
    console.error('Error sending invoice:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/:id/mark-paid - Mark invoice as paid
router.post('/api/invoices/:id/mark-paid', async (req, res) => {
  try {
    const r = await pool.query(
      "UPDATE invoices SET status = 'paid', amount_paid = total, paid_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *",
      [req.params.id]
    );
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    serverError(res, error);
  }
});

// POST /api/invoices/cleanup-blank-drafts
// One-time cleanup of junk placeholder drafts. Targets ONLY rows where:
//   status = 'draft' AND blank customer_name AND total = 0 AND no line_items.
// Defaults to dry-run; pass { confirm: true } to actually delete.
router.post('/api/invoices/cleanup-blank-drafts', async (req, res) => {
  try {
    const confirm = req.body && req.body.confirm === true;
    const selectQ = `
      SELECT id, invoice_number, created_at
      FROM invoices
      WHERE status = 'draft'
        AND COALESCE(TRIM(customer_name), '') = ''
        AND COALESCE(total, 0) = 0
        AND jsonb_array_length(COALESCE(line_items, '[]'::jsonb)) = 0`;
    const candidates = await pool.query(selectQ);
    if (!confirm) {
      return res.json({
        success: true,
        dryRun: true,
        wouldDelete: candidates.rows.length,
        candidates: candidates.rows,
      });
    }
    const ids = candidates.rows.map(r => r.id);
    if (ids.length === 0) return res.json({ success: true, deleted: 0 });
    // Detach payments first to satisfy FK; junk drafts shouldn't have any
    // but be safe.
    try { await pool.query('DELETE FROM payments WHERE invoice_id = ANY($1::int[])', [ids]); } catch (e) {}
    const del = await pool.query('DELETE FROM invoices WHERE id = ANY($1::int[]) RETURNING id', [ids]);
    res.json({ success: true, deleted: del.rowCount });
  } catch (error) {
    console.error('Error cleaning up blank drafts:', error);
    serverError(res, error);
  }
});

// DELETE /api/invoices/:id - Delete invoice
router.delete('/api/invoices/:id', async (req, res) => {
  try {
    // Delete related payments first to avoid foreign key constraint
    try { await pool.query('DELETE FROM payments WHERE invoice_id = $1', [req.params.id]); } catch(e) { /* */ }
    const r = await pool.query('DELETE FROM invoices WHERE id = $1 RETURNING id', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// SQUARE PAYMENT ENDPOINTS
// ═══════════════════════════════════════════════════════════

// GET /api/pay/config - Public - Square frontend config
router.get('/api/pay/config', (req, res) => {
  res.json({
    appId: SQUARE_APP_ID || '',
    locationId: SQUARE_LOCATION_ID || '',
    environment: process.env.SQUARE_ENVIRONMENT || 'sandbox'
  });
});

// GET /api/square/status - Check Square connection
router.get('/api/square/status', async (req, res) => {
  if (!squareClient) {
    return res.json({ connected: false, error: 'Square not configured' });
  }
  try {
    const response = await squareClient.locationsApi.listLocations();
    const locations = response.result.locations || [];
    const location = locations.find(l => l.id === SQUARE_LOCATION_ID) || locations[0];
    res.json({
      connected: true,
      environment: process.env.SQUARE_ENVIRONMENT || 'sandbox',
      locationId: location?.id,
      locationName: location?.name,
      currency: location?.currency
    });
  } catch (error) {
    res.json({ connected: false, error: error.message });
  }
});

// GET /api/pay/:token - Public - Fetch invoice by payment token
router.get('/api/pay/:token', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, invoice_number, customer_id, customer_name, customer_email, customer_address,
              subtotal, tax_rate, tax_amount, total, amount_paid, status, due_date,
              line_items, notes, created_at, sent_at, paid_at
       FROM invoices WHERE payment_token = $1`,
      [req.params.token]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Invoice not found' });
    }
    const inv = result.rows[0];
    // Update viewed_at
    await pool.query("UPDATE invoices SET viewed_at = CURRENT_TIMESTAMP, sent_status = CASE WHEN sent_status = 'viewed' THEN sent_status ELSE 'viewed' END WHERE id = $1", [inv.id]);

    // Get payment history
    try {
      const payments = await pool.query(
        'SELECT amount, method, status, card_last4, square_receipt_url, paid_at, created_at FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC',
        [inv.id]
      );
      inv.payment_history = payments.rows;
    } catch(e) { inv.payment_history = []; }

    // Get processing fee config
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        inv.processing_fee_config = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
      }
    } catch(e) { /* no fee config */ }

    // Get saved cards for this customer
    try {
      const cards = await pool.query(
        'SELECT id, card_brand, last4, exp_month, exp_year, cardholder_name FROM customer_saved_cards WHERE customer_id = $1 AND enabled = true ORDER BY created_at DESC',
        [inv.customer_id]
      );
      inv.saved_cards = cards.rows;
    } catch(e) { inv.saved_cards = []; }

    res.json({ success: true, invoice: inv });
  } catch (error) {
    console.error('Pay token error:', error);
    serverError(res, error);
  }
});

// POST /api/pay/:token/card - Process card/Apple Pay payment
router.post('/api/pay/:token/card', async (req, res) => {
  try {
    if (!squareClient) return res.status(503).json({ success: false, error: 'Square payments not configured' });

    const { sourceId, verificationToken, save_card } = req.body;
    if (!sourceId) return res.status(400).json({ success: false, error: 'Payment source required' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = invResult.rows[0];

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice already paid' });

    // Check processing fee config
    let processingFee = 0;
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        const feeConfig = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
        if (feeConfig.enabled) {
          const pct = parseFloat(feeConfig.card_fee_percent) || 2.9;
          const fixed = parseFloat(feeConfig.card_fee_fixed) || 0.30;
          processingFee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;
        }
      }
    } catch(e) { /* no fee */ }

    const totalCharge = balance + processingFee;
    const amountCents = Math.round(totalCharge * 100);
    const idempotencyKey = crypto.randomUUID();
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();

    // If save_card requested, save the card first then charge the saved card ID
    let paymentSourceId = sourceId;
    let savedCardInfo = null;
    if (save_card && inv.customer_id) {
      try {
        // Ensure Square customer exists
        const custResult = await pool.query('SELECT square_customer_id, name, email FROM customers WHERE id = $1', [inv.customer_id]);
        const cust = custResult.rows[0];
        let squareCustomerId = cust?.square_customer_id;
        if (!squareCustomerId && cust) {
          const { result: sqCustResult } = await squareClient.customersApi.createCustomer({
            givenName: (cust.name || '').split(' ')[0],
            familyName: (cust.name || '').split(' ').slice(1).join(' '),
            emailAddress: cust.email
          });
          squareCustomerId = sqCustResult.customer.id;
          await pool.query('UPDATE customers SET square_customer_id = $1 WHERE id = $2', [squareCustomerId, inv.customer_id]);
        }

        if (squareCustomerId) {
          // Save card on file first (consumes the single-use token)
          const { result: cardResult } = await squareClient.cardsApi.createCard({
            idempotencyKey: crypto.randomUUID(),
            sourceId: sourceId,
            card: { customerId: squareCustomerId, cardholderName: inv.customer_name }
          });
          const savedCard = cardResult.card;

          // Save to our DB
          await pool.query(
            `INSERT INTO customer_saved_cards (customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [inv.customer_id, savedCard.id, savedCard.cardBrand, savedCard.last4, savedCard.expMonth, savedCard.expYear, inv.customer_name]
          );

          // Use saved card ID for the payment
          paymentSourceId = savedCard.id;
          savedCardInfo = { brand: savedCard.cardBrand, last4: savedCard.last4 };
        }
      } catch (saveErr) {
        console.error('Save card during payment error:', saveErr);
        // Fall back to charging with original token (card won't be saved but payment still works)
        paymentSourceId = sourceId;
      }
    }

    const paymentRequest = {
      sourceId: paymentSourceId,
      idempotencyKey,
      amountMoney: { amount: BigInt(amountCents), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: inv.invoice_number,
      note: `Invoice ${inv.invoice_number} - ${inv.customer_name}${processingFee > 0 ? ' (includes service fee)' : ''}`,
    };
    if (verificationToken) paymentRequest.verificationToken = verificationToken;

    const response = await squareClient.paymentsApi.createPayment(paymentRequest);
    const sqPayment = response.result.payment;

    // Determine method
    const method = sqPayment.sourceType === 'WALLET' ? 'apple_pay' : 'card';
    const cardDetails = sqPayment.cardDetails;

    // Record payment (amount = balance only, processing_fee tracked separately)
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, square_order_id, square_receipt_url, card_brand, card_last4, paid_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, totalCharge, method, sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
       sqPayment.id, sqPayment.orderId, sqPayment.receiptUrl,
       cardDetails?.card?.cardBrand, cardDetails?.card?.last4]
    );

    // Update invoice
    const newAmountPaid = parseFloat(inv.amount_paid || 0) + balance;
    const currentStatus = cleanStatusValue(inv.status).toLowerCase();
    const newStatus = newAmountPaid >= parseFloat(inv.total)
      ? 'paid'
      : (currentStatus === 'pending' || currentStatus === 'sent' || currentStatus === 'overdue' ? 'partial' : inv.status);
    if (processingFee > 0) {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP, processing_fee = $4, processing_fee_passed = true WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id, processingFee]
      );
    } else {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id]
      );
    }

    // Send confirmation emails
    const feeNote = processingFee > 0 ? `<p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Credit Card Service Fee:</strong> $${processingFee.toFixed(2)}</p>` : '';
    try {
      // Customer confirmation
      if (inv.customer_email) {
        const custContent = `
          <h2 style="color:#2e403d;margin:0 0 16px;">Payment Confirmation</h2>
          <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
          <p>We've received your payment. Thank you!</p>
          <div style="background:#ecfdf5;border:1px solid #bbf7d0;border-radius:8px;padding:20px;margin:20px 0;">
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice amount:</strong> $${balance.toFixed(2)}</p>
            ${feeNote}
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Total charged:</strong> $${totalCharge.toFixed(2)}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice:</strong> ${inv.invoice_number}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Method:</strong> ${method === 'apple_pay' ? 'Apple Pay' : 'Card'} ${cardDetails?.card?.last4 ? '•••• ' + cardDetails.card.last4 : ''}</p>
            ${sqPayment.receiptUrl ? `<p style="margin:0;"><a href="${sqPayment.receiptUrl}" style="color:#059669;font-weight:600;">View Receipt</a></p>` : ''}
          </div>
        `;
        await sendEmail(inv.customer_email, `Payment received — ${inv.invoice_number}`, emailTemplate(custContent, { showSignature: false }), null, { type: 'payment_receipt', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });
      }
      // Admin notification
      const adminContent = `
        <h2 style="color:#2e403d;margin:0 0 16px;">Payment Received</h2>
        <p><strong>${inv.customer_name}</strong> paid <strong>$${totalCharge.toFixed(2)}</strong> on invoice <strong>${inv.invoice_number}</strong>.</p>
        ${processingFee > 0 ? `<p>Credit Card Service Fee passed to customer: $${processingFee.toFixed(2)} (Invoice balance: $${balance.toFixed(2)})</p>` : ''}
        <p>Method: ${method === 'apple_pay' ? 'Apple Pay' : 'Card'} ${cardDetails?.card?.last4 ? '•••• ' + cardDetails.card.last4 : ''}</p>
        <p>Square ID: ${sqPayment.id}</p>
        ${sqPayment.receiptUrl ? `<p><a href="${sqPayment.receiptUrl}">View Receipt</a></p>` : ''}
      `;
      await sendEmail(NOTIFICATION_EMAIL, `Payment: $${totalCharge.toFixed(2)} from ${inv.customer_name}`, emailTemplate(adminContent, { showSignature: false }), null, { type: 'admin_notification', customer_name: inv.customer_name });
    } catch (emailErr) { console.error('Payment email error:', emailErr); }

    res.json({
      success: true,
      payment: {
        id: paymentId,
        amount: totalCharge,
        invoiceAmount: balance,
        processingFee,
        status: sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
        receiptUrl: sqPayment.receiptUrl,
        cardBrand: cardDetails?.card?.cardBrand,
        cardLast4: cardDetails?.card?.last4,
        cardSaved: !!savedCardInfo
      }
    });
  } catch (error) {
    console.error('Card payment error:', error);
    const errorMessage = error instanceof SquareApiError ? (error.result?.errors?.[0]?.detail || error.message) : error.message;
    res.status(500).json({ success: false, error: errorMessage });
  }
});

// POST /api/pay/:token/saved-card - Pay invoice with saved card on file
router.post('/api/pay/:token/saved-card', async (req, res) => {
  try {
    if (!squareClient) return res.status(503).json({ success: false, error: 'Square payments not configured' });
    const { card_id } = req.body;
    if (!card_id) return res.status(400).json({ success: false, error: 'card_id required' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = invResult.rows[0];

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice already paid' });

    // Look up saved card
    const cardResult = await pool.query('SELECT square_card_id, card_brand, last4 FROM customer_saved_cards WHERE id = $1 AND customer_id = $2 AND enabled = true', [card_id, inv.customer_id]);
    if (cardResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Saved card not found' });
    const savedCard = cardResult.rows[0];

    // Check processing fee config
    let processingFee = 0;
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        const feeConfig = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
        if (feeConfig.enabled) {
          const pct = parseFloat(feeConfig.card_fee_percent) || 2.9;
          const fixed = parseFloat(feeConfig.card_fee_fixed) || 0.30;
          processingFee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;
        }
      }
    } catch(e) { /* no fee */ }

    const totalCharge = balance + processingFee;
    const amountCents = Math.round(totalCharge * 100);
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();

    const response = await squareClient.paymentsApi.createPayment({
      sourceId: savedCard.square_card_id,
      idempotencyKey: crypto.randomUUID(),
      amountMoney: { amount: BigInt(amountCents), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: inv.invoice_number,
      note: `Invoice ${inv.invoice_number} - ${inv.customer_name} (card on file)${processingFee > 0 ? ' (includes service fee)' : ''}`
    });
    const sqPayment = response.result.payment;

    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, square_order_id, square_receipt_url, card_brand, card_last4, paid_at)
       VALUES ($1, $2, $3, $4, 'card', $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, totalCharge, sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
       sqPayment.id, sqPayment.orderId, sqPayment.receiptUrl, savedCard.card_brand, savedCard.last4]
    );

    const newAmountPaid = parseFloat(inv.amount_paid || 0) + balance;
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : inv.status;
    if (processingFee > 0) {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP, processing_fee = $4, processing_fee_passed = true WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id, processingFee]
      );
    } else {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id]
      );
    }

    // Send confirmation emails
    try {
      if (inv.customer_email) {
        const custContent = `
          <h2 style="color:#2e403d;margin:0 0 16px;">Payment Confirmation</h2>
          <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
          <p>We've received your payment. Thank you!</p>
          <div style="background:#ecfdf5;border:1px solid #bbf7d0;border-radius:8px;padding:20px;margin:20px 0;">
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice amount:</strong> $${balance.toFixed(2)}</p>
            ${processingFee > 0 ? `<p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Service Fee:</strong> $${processingFee.toFixed(2)}</p>` : ''}
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Total charged:</strong> $${totalCharge.toFixed(2)}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice:</strong> ${inv.invoice_number}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Method:</strong> Card on file •••• ${savedCard.last4}</p>
            ${sqPayment.receiptUrl ? `<p style="margin:0;"><a href="${sqPayment.receiptUrl}" style="color:#059669;font-weight:600;">View Receipt</a></p>` : ''}
          </div>`;
        await sendEmail(inv.customer_email, `Payment received — ${inv.invoice_number}`, emailTemplate(custContent, { showSignature: false }), null, { type: 'payment_receipt', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });
      }
      const adminContent = `
        <h2 style="color:#2e403d;margin:0 0 16px;">Payment Received</h2>
        <p><strong>${inv.customer_name}</strong> paid <strong>$${totalCharge.toFixed(2)}</strong> on invoice <strong>${inv.invoice_number}</strong>.</p>
        ${processingFee > 0 ? `<p>Service Fee: $${processingFee.toFixed(2)}</p>` : ''}
        <p>Method: Card on file •••• ${savedCard.last4}</p>
        <p>Square ID: ${sqPayment.id}</p>`;
      await sendEmail(NOTIFICATION_EMAIL, `Payment: $${totalCharge.toFixed(2)} from ${inv.customer_name}`, emailTemplate(adminContent, { showSignature: false }), null, { type: 'admin_notification', customer_name: inv.customer_name });
    } catch (emailErr) { console.error('Saved card payment email error:', emailErr); }

    res.json({
      success: true,
      payment: { id: paymentId, amount: totalCharge, invoiceAmount: balance, processingFee, status: sqPayment.status === 'COMPLETED' ? 'completed' : 'pending', receiptUrl: sqPayment.receiptUrl, cardBrand: savedCard.card_brand, cardLast4: savedCard.last4 }
    });
  } catch (error) {
    console.error('Saved card payment error:', error);
    const errorMessage = error instanceof SquareApiError ? (error.result?.errors?.[0]?.detail || error.message) : error.message;
    res.status(500).json({ success: false, error: errorMessage });
  }
});

// POST /api/pay/:token/ach - Process ACH bank transfer
router.post('/api/pay/:token/ach', async (req, res) => {
  try {
    if (!squareClient) return res.status(503).json({ success: false, error: 'Square payments not configured' });

    const { sourceId } = req.body;
    if (!sourceId) return res.status(400).json({ success: false, error: 'Payment source required' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = invResult.rows[0];

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice already paid' });

    // Check processing fee config for ACH
    let processingFee = 0;
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        const feeConfig = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
        if (feeConfig.enabled) {
          const pct = parseFloat(feeConfig.ach_fee_percent) || 1.0;
          const fixed = parseFloat(feeConfig.ach_fee_fixed) || 0;
          processingFee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;
        }
      }
    } catch(e) { /* no fee */ }

    const totalCharge = balance + processingFee;
    const amountCents = Math.round(totalCharge * 100);
    const idempotencyKey = crypto.randomUUID();
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();

    const response = await squareClient.paymentsApi.createPayment({
      sourceId,
      idempotencyKey,
      amountMoney: { amount: BigInt(amountCents), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: inv.invoice_number,
      note: `Invoice ${inv.invoice_number} - ${inv.customer_name}${processingFee > 0 ? ' (includes service fee)' : ''}`,
      acceptPartialAuthorization: false,
    });
    const sqPayment = response.result.payment;
    const bankDetails = sqPayment.bankAccountDetails;

    // ACH payments are typically PENDING until they clear
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, square_order_id, square_receipt_url, ach_bank_name, paid_at)
       VALUES ($1, $2, $3, $4, 'ach', $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, totalCharge, sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
       sqPayment.id, sqPayment.orderId, sqPayment.receiptUrl, bankDetails?.bankName]
    );

    // Update invoice
    const newAmountPaid = parseFloat(inv.amount_paid || 0) + balance;
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : inv.status;
    if (processingFee > 0) {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP, processing_fee = $4, processing_fee_passed = true WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id, processingFee]
      );
    } else {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id]
      );
    }

    // Send emails
    const feeNote = processingFee > 0 ? `<p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>ACH Service Fee:</strong> $${processingFee.toFixed(2)}</p>` : '';
    try {
      if (inv.customer_email) {
        const custContent = `
          <h2 style="color:#2e403d;margin:0 0 16px;">Payment Confirmation</h2>
          <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
          <p>We've received your ACH bank transfer. It may take 3-5 business days to clear.</p>
          <div style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:8px;padding:20px;margin:20px 0;">
            <p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>Invoice amount:</strong> $${balance.toFixed(2)}</p>
            ${feeNote}
            <p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>Total charged:</strong> $${totalCharge.toFixed(2)}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>Invoice:</strong> ${inv.invoice_number}</p>
            <p style="margin:0;font-size:14px;color:#5b21b6;"><strong>Method:</strong> ACH Bank Transfer${bankDetails?.bankName ? ' (' + bankDetails.bankName + ')' : ''}</p>
          </div>
        `;
        await sendEmail(inv.customer_email, `Payment received — ${inv.invoice_number}`, emailTemplate(custContent, { showSignature: false }));
      }
      await sendEmail(NOTIFICATION_EMAIL, `ACH Payment: $${totalCharge.toFixed(2)} from ${inv.customer_name}`, emailTemplate(`
        <h2 style="color:#2e403d;margin:0 0 16px;">ACH Payment Received</h2>
        <p><strong>${inv.customer_name}</strong> paid <strong>$${totalCharge.toFixed(2)}</strong> via ACH on invoice <strong>${inv.invoice_number}</strong>.</p>
        ${processingFee > 0 ? `<p>ACH Service Fee passed to customer: $${processingFee.toFixed(2)} (Invoice balance: $${balance.toFixed(2)})</p>` : ''}
        <p>Status: ${sqPayment.status} (ACH payments may take 3-5 days to clear)</p>
        <p>Square ID: ${sqPayment.id}</p>
      `, { showSignature: false }));
    } catch (emailErr) { console.error('ACH email error:', emailErr); }

    res.json({
      success: true,
      payment: {
        id: paymentId,
        amount: totalCharge,
        invoiceAmount: balance,
        processingFee,
        status: sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
        receiptUrl: sqPayment.receiptUrl,
        bankName: bankDetails?.bankName,
        note: sqPayment.status !== 'COMPLETED' ? 'ACH transfers typically take 3-5 business days to clear' : undefined
      }
    });
  } catch (error) {
    console.error('ACH payment error:', error);
    const errorMessage = error instanceof SquareApiError ? (error.result?.errors?.[0]?.detail || error.message) : error.message;
    res.status(500).json({ success: false, error: errorMessage });
  }
});

// GET /api/pay/:token/pdf - Download invoice PDF (public)
router.get('/api/pay/:token/pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = result.rows[0];
    const pdfResult = await generateInvoicePDF(inv);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="invoice-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Pay PDF error:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/:id/pdf - Download invoice PDF (admin)
router.get('/api/invoices/:id/pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = result.rows[0];
    const pdfResult = await generateInvoicePDF(inv);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="invoice-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Invoice PDF error:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/:id/receipt-pdf - Download payment receipt PDF (admin)
router.get('/api/invoices/:id/receipt-pdf', async (req, res) => {
  try {
    const invResult = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = invResult.rows[0];
    // Get most recent payment for this invoice
    let payment = {};
    try {
      const payResult = await pool.query('SELECT * FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC LIMIT 1', [inv.id]);
      if (payResult.rows.length > 0) payment = payResult.rows[0];
    } catch (e) { /* no payments table or no payment */ }
    if (!payment.paid_at) payment.paid_at = inv.paid_at;
    if (!payment.amount) payment.amount = inv.amount_paid || inv.total;
    const pdfResult = await generateReceiptPDF(inv, payment);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'Receipt PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="receipt-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Receipt PDF error:', error);
    serverError(res, error);
  }
});

// GET /api/pay/:token/receipt-pdf - Download payment receipt PDF (public)
router.get('/api/pay/:token/receipt-pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = result.rows[0];
    let payment = {};
    try {
      const payResult = await pool.query('SELECT * FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC LIMIT 1', [inv.id]);
      if (payResult.rows.length > 0) payment = payResult.rows[0];
    } catch (e) { /* no payment */ }
    if (!payment.paid_at) payment.paid_at = inv.paid_at;
    if (!payment.amount) payment.amount = inv.amount_paid || inv.total;
    const pdfResult = await generateReceiptPDF(inv, payment);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'Receipt PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="receipt-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Pay receipt PDF error:', error);
    serverError(res, error);
  }
});

router.post('/api/invoices/:id/record-payment', validate(schemas.recordPayment), async (req, res) => {
  try {
    const { amount, method, notes } = req.body;
    if (!amount || amount <= 0) return res.status(400).json({ success: false, error: 'Invalid amount' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = invResult.rows[0];

    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, notes, paid_at)
       VALUES ($1, $2, $3, $4, $5, 'completed', $6, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, amount, method || 'cash', notes]
    );

    const newAmountPaid = parseFloat(inv.amount_paid || 0) + parseFloat(amount);
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : inv.status;
    await pool.query(
      `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2 = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
      [newAmountPaid, newStatus, inv.id]
    );

    res.json({ success: true, paymentId, newAmountPaid, newStatus });
  } catch (error) {
    console.error('Record payment error:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/:id/send-reminder - Send payment reminder email
router.post('/api/invoices/:id/send-reminder', async (req, res) => {
  try {
    const invResult = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = invResult.rows[0];
    if (!inv.customer_email) return res.status(400).json({ success: false, error: 'No customer email' });

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const payUrl = inv.payment_token ? `${baseUrl}/pay-invoice.html?token=${inv.payment_token}` : '';

    const content = `
      <h2 style="color:#2e403d;margin:0 0 16px;">Payment Reminder</h2>
      <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
      <p>This is a friendly reminder that your invoice <strong>${inv.invoice_number}</strong> has a balance of <strong>$${balance.toFixed(2)}</strong>${inv.due_date ? ' due by <strong>' + new Date(inv.due_date).toLocaleDateString('en-US', {month:'long',day:'numeric',year:'numeric'}) + '</strong>' : ''}.</p>
      ${payUrl ? `
        <div style="text-align:center;margin:28px 0;">
          <a href="${payUrl}" style="display:inline-block;padding:16px 40px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">
            Pay Now — $${balance.toFixed(2)}
          </a>
        </div>
        <p style="text-align:center;font-size:12px;color:#9ca09c;">Secure payment powered by Square</p>
      ` : ''}
      <p>If you've already sent payment, please disregard this reminder.</p>
    `;
    await sendEmail(inv.customer_email, `Reminder: Invoice ${inv.invoice_number} — $${balance.toFixed(2)} due`, emailTemplate(content), null, { type: 'invoice_reminder', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });

    await pool.query(
      'UPDATE invoices SET reminder_sent_at = CURRENT_TIMESTAMP, reminder_count = COALESCE(reminder_count, 0) + 1, updated_at = CURRENT_TIMESTAMP WHERE id = $1',
      [inv.id]
    );

    res.json({ success: true, message: 'Reminder sent' });
  } catch (error) {
    console.error('Send reminder error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════

// POST /api/invoices/:id/charge-card - Admin: charge a saved card for an invoice
router.post('/api/invoices/:id/charge-card', authenticateToken, async (req, res) => {
  try {
    const { card_id } = req.body;
    if (!card_id) return res.status(400).json({ success: false, error: 'card_id required' });
    if (!squareClient) return res.status(500).json({ success: false, error: 'Square not configured' });

    const inv = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (inv.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const invoice = inv.rows[0];

    const cardResult = await pool.query(
      'SELECT square_card_id FROM customer_saved_cards WHERE id = $1 AND customer_id = $2 AND enabled = true',
      [card_id, invoice.customer_id]
    );
    if (cardResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Card not found' });

    const balance = Math.round((parseFloat(invoice.total) - parseFloat(invoice.amount_paid || 0)) * 100);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice has no balance due' });

    const { result: payResult } = await squareClient.paymentsApi.createPayment({
      idempotencyKey: crypto.randomUUID(),
      sourceId: cardResult.rows[0].square_card_id,
      amountMoney: { amount: BigInt(balance), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: invoice.invoice_number,
      note: `Invoice ${invoice.invoice_number} — admin charge card-on-file`
    });
    const payment = payResult.payment;
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, card_brand, card_last4, paid_at)
       VALUES ($1, $2, $3, $4, 'card', 'completed', $5, $6, $7, NOW())`,
      [paymentId, req.params.id, invoice.customer_id, balance / 100, payment.id, payment.cardDetails?.card?.cardBrand, payment.cardDetails?.card?.last4]
    );
    await pool.query("UPDATE invoices SET status = 'paid', amount_paid = total, paid_at = NOW(), updated_at = NOW() WHERE id = $1", [req.params.id]);
    res.json({ success: true, paymentId, receiptUrl: payment.receiptUrl });
  } catch (error) {
    console.error('Admin charge card error:', error);
    serverError(res, error);
  }
});

// ─── Payment Schedule Splitting ────────────────────────────────────────────

// POST /api/invoices/:id/payment-schedule - Split invoice into installments
router.post('/api/invoices/:id/payment-schedule', async (req, res) => {
  try {
    const { installments } = req.body; // Array of { amount, due_date, label }
    if (!installments || !Array.isArray(installments) || installments.length < 2) {
      return res.status(400).json({ success: false, error: 'Need at least 2 installments' });
    }
    const inv = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (!inv.rows.length) return res.status(404).json({ success: false, error: 'Invoice not found' });

    const total = parseFloat(inv.rows[0].total);
    const scheduleTotal = installments.reduce((sum, i) => sum + parseFloat(i.amount), 0);
    if (Math.abs(scheduleTotal - total) > 0.01) {
      return res.status(400).json({ success: false, error: `Installments total $${scheduleTotal.toFixed(2)} doesn't match invoice total $${total.toFixed(2)}` });
    }

    const schedule = installments.map((inst, idx) => ({
      number: idx + 1,
      amount: parseFloat(inst.amount),
      due_date: inst.due_date,
      label: inst.label || `Payment ${idx + 1} of ${installments.length}`,
      status: 'pending'
    }));

    await pool.query(
      `UPDATE invoices SET payment_schedule = $1, installment_count = $2, updated_at = NOW() WHERE id = $3`,
      [JSON.stringify(schedule), installments.length, req.params.id]
    );

    res.json({ success: true, schedule, installment_count: installments.length });
  } catch (error) { serverError(res, error); }
});

// GET /api/invoices/:id/payment-schedule
router.get('/api/invoices/:id/payment-schedule', async (req, res) => {
  try {
    const inv = await pool.query('SELECT id, total, amount_paid, payment_schedule, installment_count FROM invoices WHERE id = $1', [req.params.id]);
    if (!inv.rows.length) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const schedule = inv.rows[0].payment_schedule || [];
    // Mark paid installments based on amount_paid
    let remaining = parseFloat(inv.rows[0].amount_paid) || 0;
    const updated = (Array.isArray(schedule) ? schedule : []).map(inst => {
      if (remaining >= inst.amount) { remaining -= inst.amount; return { ...inst, status: 'paid' }; }
      if (remaining > 0) { const partial = remaining; remaining = 0; return { ...inst, status: 'partial', paid: partial }; }
      return { ...inst, status: 'pending' };
    });
    res.json({ success: true, schedule: updated, total: parseFloat(inv.rows[0].total), amount_paid: parseFloat(inv.rows[0].amount_paid) || 0 });
  } catch (error) { serverError(res, error); }
});

// ─── Job Detail / Profitability ────────────────────────────────────────────

  return router;
};
