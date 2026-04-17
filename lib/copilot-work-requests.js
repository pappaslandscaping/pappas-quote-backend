const crypto = require('crypto');
const cheerio = require('cheerio');
const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
} = require('./copilot-metric-sources');

const COPILOT_WORK_REQUESTS_BASE_PATH = '/customers/work_requests';
const COPILOT_WORK_REQUESTS_BASE_PATH_WITH_SLASH = '/customers/work_requests/';

function cleanText(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeHeader(value) {
  return cleanText(value).toLowerCase();
}

function parsePreferredWorkDate(value) {
  const raw = cleanText(value);
  if (!raw || raw === '—' || raw === '-') return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toISOString().slice(0, 10);
}

function buildRequestKey(parts) {
  return crypto
    .createHash('sha1')
    .update(parts.map((part) => cleanText(part)).join('|'))
    .digest('hex')
    .slice(0, 16);
}

function findWorkRequestsTable($) {
  return $('table').toArray().find((table) => {
    const headerRow = $(table).find('thead tr').first().length
      ? $(table).find('thead tr').first()
      : $(table).find('tr').first();
    const headers = headerRow.find('th,td').toArray()
      .map((cell) => normalizeHeader($(cell).text()));
    if (!headers.length) return false;
    const required = [
      'customer name',
      'phone',
      'email',
      'address',
      'preferred work date',
      'work requested',
      'source',
    ];
    const matches = required.filter((header) => headers.includes(header)).length;
    return matches >= 5;
  }) || null;
}

function extractPaginationTotal($) {
  const text = cleanText($('body').text());
  const patterns = [
    /\b\d+\s*-\s*\d+\s+of\s+(\d+)\b/i,
    /\bof\s+(\d+)\b/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const total = parseInt(match[1], 10);
    if (Number.isFinite(total)) return total;
  }
  return null;
}

function extractPaginationPaths($, pageUrl) {
  const base = new URL(pageUrl || `https://secure.copilotcrm.com${COPILOT_WORK_REQUESTS_BASE_PATH}`);
  const paths = new Set();
  $('a[href]').each((_, link) => {
    const href = ($(link).attr('href') || '').trim();
    if (!href || href.startsWith('#') || href.startsWith('javascript:')) return;
    try {
      const resolved = new URL(href, base);
      if (
        resolved.pathname !== COPILOT_WORK_REQUESTS_BASE_PATH
        && resolved.pathname !== COPILOT_WORK_REQUESTS_BASE_PATH_WITH_SLASH
      ) return;
      const page = resolved.searchParams.get('page') || resolved.searchParams.get('p');
      if (page && page !== '1') {
        const normalizedPath = resolved.pathname === COPILOT_WORK_REQUESTS_BASE_PATH_WITH_SLASH
          ? COPILOT_WORK_REQUESTS_BASE_PATH
          : resolved.pathname;
        paths.add(`${normalizedPath}${resolved.search}`);
      }
    } catch (_error) {
      // ignore malformed links
    }
  });
  return Array.from(paths);
}

function parseCopilotWorkRequestsHtml(html, pageUrl) {
  const $ = cheerio.load(html || '');
  const table = findWorkRequestsTable($);
  if (!table) {
    return {
      requests: [],
      total: extractPaginationTotal($) || 0,
      page_paths: extractPaginationPaths($, pageUrl),
    };
  }

  const headerRow = $(table).find('thead tr').first().length
    ? $(table).find('thead tr').first()
    : $(table).find('tr').first();
  const headers = headerRow.find('th,td').toArray()
    .map((cell) => normalizeHeader($(cell).text()));
  const headerIndex = Object.fromEntries(headers.map((header, index) => [header, index]));
  const rows = [];

  const bodyRows = $(table).find('tbody tr').length
    ? $(table).find('tbody tr').toArray()
    : $(table).find('tr').slice(1).toArray();

  bodyRows.forEach((row, rowIndex) => {
    const cells = $(row).find('td,th').toArray();
    if (!cells.length) return;

    function cellText(header) {
      const idx = headerIndex[header];
      if (!Number.isInteger(idx)) return '';
      return cleanText($(cells[idx]).text());
    }

    function cellNode(header) {
      const idx = headerIndex[header];
      if (!Number.isInteger(idx)) return null;
      return cells[idx] || null;
    }

    const customerNode = cellNode('customer name');
    const customerLink = customerNode ? $(customerNode).find('a').first() : null;
    const customerName = cellText('customer name');
    const phone = cellText('phone');
    const emailNode = cellNode('email');
    const emailLink = emailNode ? $(emailNode).find('a[href^="mailto:"]').first() : null;
    const email = cleanText(emailLink?.text() || cellText('email'));
    const address = cellText('address');
    const preferredWorkDateRaw = cellText('preferred work date');
    const workRequested = cellText('work requested');
    const source = cellText('source');
    const customerPath = customerLink?.attr('href') || null;
    const rowId = $(row).attr('id') || $(row).attr('data-id') || null;
    const requestKey = rowId || buildRequestKey([
      customerName,
      phone,
      email,
      address,
      preferredWorkDateRaw,
      workRequested,
      source,
      rowIndex,
    ]);

    if (!customerName && !workRequested) return;

    rows.push({
      id: requestKey,
      external_source: 'copilotcrm',
      customer_name: customerName || null,
      customer_phone: phone || null,
      customer_email: email || null,
      customer_address: address || null,
      preferred_work_date: parsePreferredWorkDate(preferredWorkDateRaw),
      preferred_work_date_raw: preferredWorkDateRaw || null,
      work_requested: workRequested || null,
      source: source || null,
      customer_path: customerPath || null,
    });
  });

  return {
    requests: rows,
    total: extractPaginationTotal($) || rows.length,
    page_paths: extractPaginationPaths($, pageUrl),
  };
}

function buildCopilotWorkRequestStats(requests) {
  const stats = {
    total: 0,
    open_total: 0,
    client: 0,
    lead: 0,
    with_preferred_date: 0,
  };
  (requests || []).forEach((request) => {
    stats.total += 1;
    stats.open_total += 1;
    const source = cleanText(request.source).toLowerCase();
    if (source === 'client') stats.client += 1;
    if (source === 'lead') stats.lead += 1;
    if (request.preferred_work_date || request.preferred_work_date_raw) stats.with_preferred_date += 1;
  });
  return stats;
}

function normalizeCopilotWorkRequestsSnapshot(snapshot, sourceOverride = LIVE_COPILOT_SOURCE) {
  if (!snapshot || typeof snapshot !== 'object' || !Array.isArray(snapshot.requests)) return null;
  const requests = snapshot.requests.map((request) => ({
    id: cleanText(request.id) || buildRequestKey([
      request.customer_name,
      request.customer_phone,
      request.customer_email,
      request.customer_address,
      request.preferred_work_date_raw || request.preferred_work_date,
      request.work_requested,
      request.source,
    ]),
    external_source: 'copilotcrm',
    customer_name: cleanText(request.customer_name) || null,
    customer_phone: cleanText(request.customer_phone) || null,
    customer_email: cleanText(request.customer_email) || null,
    customer_address: cleanText(request.customer_address) || null,
    preferred_work_date: request.preferred_work_date || null,
    preferred_work_date_raw: cleanText(request.preferred_work_date_raw) || null,
    work_requested: cleanText(request.work_requested) || null,
    source: cleanText(request.source) || null,
    customer_path: cleanText(request.customer_path) || null,
  }));
  const total = Number.isFinite(Number(snapshot.total)) ? Number(snapshot.total) : requests.length;
  const stats = buildCopilotWorkRequestStats(requests);
  return {
    success: true,
    source: sourceOverride || snapshot.source || LIVE_COPILOT_SOURCE,
    as_of: snapshot.as_of || new Date().toISOString(),
    mode: 'copilot',
    total: Math.max(total, requests.length),
    requests,
    stats,
  };
}

function getWorkRequestsSnapshotExpiry(snapshot, ttlMs) {
  const asOfMs = snapshot?.as_of ? new Date(snapshot.as_of).getTime() : NaN;
  if (!Number.isFinite(asOfMs)) return 0;
  return asOfMs + ttlMs;
}

module.exports = {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
  COPILOT_WORK_REQUESTS_BASE_PATH,
  cleanText,
  parsePreferredWorkDate,
  parseCopilotWorkRequestsHtml,
  buildCopilotWorkRequestStats,
  normalizeCopilotWorkRequestsSnapshot,
  getWorkRequestsSnapshotExpiry,
};
