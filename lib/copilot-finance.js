const cheerio = require('cheerio');
const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
} = require('./copilot-metric-sources');

function formatEasternDate(date = new Date()) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
  const parts = formatter.formatToParts(date);
  const mapped = Object.fromEntries(parts.filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
  return `${mapped.year}-${mapped.month}-${mapped.day}`;
}

function getCopilotRevenueWindow(now = new Date()) {
  const end = formatEasternDate(now);
  const [year, month] = end.split('-');
  return {
    start: `${year}-${month}-01`,
    end,
  };
}

function parseCurrencyAmount(value) {
  const normalized = String(value || '').replace(/[^0-9.-]/g, '');
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function extractCurrencyValues(text) {
  return (String(text || '').match(/-?\$?\d[\d,]*\.\d{2}/g) || [])
    .map(parseCurrencyAmount)
    .filter((value) => Number.isFinite(value));
}

function extractTableRowAmounts($, row) {
  const cellTexts = $(row).find('td,th').toArray().map((cell) => $(cell).text().replace(/\s+/g, ' ').trim());
  const rowText = cellTexts.join(' | ').trim();
  const amounts = cellTexts
    .flatMap((text) => extractCurrencyValues(text))
    .filter((value) => Number.isFinite(value));
  return { rowText, cellTexts, amounts };
}

function extractRevenueByCrewTableTotal($) {
  const reportTable = $('table').toArray().find((table) => {
    const headers = $(table).find('thead tr').first().find('th,td').toArray()
      .map((cell) => $(cell).text().replace(/\s+/g, ' ').trim());
    if (!headers.length) return false;
    const hasCrewColumn = /^crew$/i.test(headers[0] || '');
    const hasTotalColumn = headers.some((text) => /^total$/i.test(text));
    if (!hasCrewColumn || !hasTotalColumn) return false;

    return $(table).find('tbody tr').toArray().some((row) => {
      const firstCell = $(row).find('td,th').first().text().replace(/\s+/g, ' ').trim();
      return /^total$/i.test(firstCell);
    });
  });

  if (!reportTable) return null;

  const totalRow = $(reportTable).find('tbody tr').toArray().find((row) => {
    const firstCell = $(row).find('td,th').first().text().replace(/\s+/g, ' ').trim();
    return /^total$/i.test(firstCell);
  });
  if (!totalRow) return null;

  const { amounts } = extractTableRowAmounts($, totalRow);
  const positiveAmounts = amounts.filter((value) => value > 0);
  if (!positiveAmounts.length) return null;
  return Math.max(...positiveAmounts);
}

function extractCopilotRevenueReportTotal(html) {
  const $ = cheerio.load(html || '');
  const directReportTableTotal = extractRevenueByCrewTableTotal($);
  if (Number.isFinite(directReportTableTotal)) return directReportTableTotal;

  const rows = $('table tr, .grand-total, .summary-row, .report-total').toArray()
    .map((row) => extractTableRowAmounts($, row))
    .filter((row) => row.rowText && row.amounts.length);

  const explicitTotalRows = rows.filter((row) => /(grand total|total collected|collected total)/i.test(row.rowText));
  const positiveExplicit = explicitTotalRows.flatMap((row) => row.amounts.filter((value) => value > 0));
  if (positiveExplicit.length) return Math.max(...positiveExplicit);
  if (explicitTotalRows.some((row) => row.amounts.some((value) => value === 0))) return 0;

  const footerRows = $('tfoot tr').toArray()
    .map((row) => extractTableRowAmounts($, row))
    .filter((row) => row.rowText && row.amounts.length);
  const positiveFooter = footerRows.flatMap((row) => row.amounts.filter((value) => value > 0));
  if (positiveFooter.length) return Math.max(...positiveFooter);

  const bottomRows = rows.slice(-8).filter((row) => row.amounts.length >= 2);
  const positiveBottom = bottomRows.flatMap((row) => row.amounts.filter((value) => value > 0));
  if (positiveBottom.length) return Math.max(...positiveBottom);

  const pageText = $('body').text().replace(/\s+/g, ' ').trim();
  const regexes = [
    /Grand Total[^$0-9-]*\$?([0-9,]+\.\d{2})/i,
    /Total Collected[^$0-9-]*\$?([0-9,]+\.\d{2})/i,
    /Collected Total[^$0-9-]*\$?([0-9,]+\.\d{2})/i,
    /Revenue by Crew[\s\S]{0,400}\$?([0-9,]+\.\d{2})/i,
  ];
  for (const regex of regexes) {
    const match = pageText.match(regex);
    if (match) {
      const amount = parseCurrencyAmount(match[1]);
      if (Number.isFinite(amount)) return amount;
    }
  }

  return null;
}

function normalizeCopilotRevenueSnapshot(snapshot, sourceOverride = LIVE_COPILOT_SOURCE) {
  if (!snapshot || typeof snapshot !== 'object') return null;
  const total = Number(snapshot.total);
  if (!Number.isFinite(total)) return null;
  return {
    source: sourceOverride || snapshot.source || LIVE_COPILOT_SOURCE,
    as_of: snapshot.as_of || new Date().toISOString(),
    period_start: snapshot.period_start,
    period_end: snapshot.period_end,
    total,
    type: 'collected',
  };
}

function getRevenueSnapshotExpiry(snapshot, currentWindow, ttlMs) {
  const asOfMs = snapshot?.as_of ? new Date(snapshot.as_of).getTime() : NaN;
  if (!Number.isFinite(asOfMs)) return 0;
  return snapshot.period_end === currentWindow.end
    ? asOfMs + ttlMs
    : 0;
}

function isRevenueSnapshotForWindow(snapshot, window) {
  return !!snapshot
    && snapshot.period_start === window.start
    && snapshot.period_end === window.end;
}

function hasUsableCopilotRevenueSnapshot(snapshot, revenueMonth) {
  const total = Number(snapshot?.total);
  return Number.isFinite(total) && (total > 0 || revenueMonth <= 0);
}

function buildRevenueMetric({
  copilotRevenueSnapshot,
  revenueMonth,
  now = new Date(),
}) {
  const revenueWindow = getCopilotRevenueWindow(now);
  const useCopilot = hasUsableCopilotRevenueSnapshot(copilotRevenueSnapshot, revenueMonth);

  return {
    revenue: useCopilot ? Number(copilotRevenueSnapshot.total) : revenueMonth,
    revenue_source: useCopilot
      ? (copilotRevenueSnapshot?.source || LIVE_COPILOT_SOURCE)
      : DATABASE_FALLBACK_SOURCE,
    revenue_as_of: useCopilot ? (copilotRevenueSnapshot?.as_of || null) : null,
    revenue_period_start: useCopilot ? (copilotRevenueSnapshot?.period_start || revenueWindow.start) : revenueWindow.start,
    revenue_period_end: useCopilot ? (copilotRevenueSnapshot?.period_end || revenueWindow.end) : revenueWindow.end,
  };
}

module.exports = {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
  formatEasternDate,
  getCopilotRevenueWindow,
  parseCurrencyAmount,
  extractCurrencyValues,
  extractTableRowAmounts,
  extractRevenueByCrewTableTotal,
  extractCopilotRevenueReportTotal,
  normalizeCopilotRevenueSnapshot,
  getRevenueSnapshotExpiry,
  isRevenueSnapshotForWindow,
  hasUsableCopilotRevenueSnapshot,
  buildRevenueMetric,
};
