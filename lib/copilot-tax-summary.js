const cheerio = require('cheerio');
const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
} = require('./copilot-metric-sources');

const COPILOT_TAX_SUMMARY_BASE_PATH = '/reports/tax_summary';

function cleanText(value) {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeHeader(value) {
  return cleanText(value).toLowerCase();
}

function parseCurrencyAmount(value) {
  const normalized = String(value || '').replace(/[^0-9.-]/g, '');
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function findTaxSummaryTable($) {
  return $('table').toArray().find((table) => {
    const headerRow = $(table).find('thead tr').first().length
      ? $(table).find('thead tr').first()
      : $(table).find('tr').first();
    const headers = headerRow.find('th,td').toArray()
      .map((cell) => normalizeHeader($(cell).text()));
    if (!headers.length) return false;
    const required = [
      'tax rate',
      'total sales',
      'taxable amount',
      'discount',
      'tax amount',
    ];
    const matches = required.filter((header) => headers.includes(header)).length;
    return matches >= 4;
  }) || null;
}

function parseTaxSummaryRows($, table) {
  const headerRow = $(table).find('thead tr').first().length
    ? $(table).find('thead tr').first()
    : $(table).find('tr').first();
  const headers = headerRow.find('th,td').toArray()
    .map((cell) => normalizeHeader($(cell).text()));
  const headerIndex = Object.fromEntries(headers.map((header, index) => [header, index]));

  const bodyRows = $(table).find('tbody tr').length
    ? $(table).find('tbody tr').toArray()
    : $(table).find('tr').slice(1).toArray();

  const rows = [];
  bodyRows.forEach((row) => {
    const cells = $(row).find('td,th').toArray();
    if (!cells.length) return;

    const rowTexts = cells.map((cell) => cleanText($(cell).text()));
    const firstCell = normalizeHeader(rowTexts[0] || '');
    if (!firstCell || firstCell === 'total') return;

    function cellText(header) {
      const idx = headerIndex[header];
      if (!Number.isInteger(idx)) return '';
      return rowTexts[idx] || '';
    }

    const taxRateRaw = cellText('tax rate');
    const taxRate = parseFloat(String(taxRateRaw || '').replace(/[^0-9.-]/g, '')) || 0;
    rows.push({
      tax_rate: taxRate,
      total_sales: parseCurrencyAmount(cellText('total sales')),
      taxable_amount: parseCurrencyAmount(cellText('taxable amount')),
      discount: parseCurrencyAmount(cellText('discount')),
      tax_amount: parseCurrencyAmount(cellText('tax amount')),
      raw_tax_rate: taxRateRaw || null,
    });
  });

  return rows;
}

function extractNotTaxableValues($) {
  const text = cleanText($('body').text());
  const processingFeesMatch = text.match(/processing fees[^$0-9-]*\$?([0-9,]+\.\d{2})/i);
  const tipsMatch = text.match(/\btips\b[^$0-9-]*\$?([0-9,]+\.\d{2})/i);
  return {
    processing_fees: processingFeesMatch ? parseCurrencyAmount(processingFeesMatch[1]) : 0,
    tips: tipsMatch ? parseCurrencyAmount(tipsMatch[1]) : 0,
  };
}

function parseCopilotTaxSummaryHtml(html, { startDate, endDate, basis = 'collected', pageUrl } = {}) {
  const $ = cheerio.load(html || '');
  const table = findTaxSummaryTable($);
  if (!table) {
    return {
      start_date: startDate || null,
      end_date: endDate || null,
      basis,
      rows: [],
      total_sales: 0,
      taxable_amount: 0,
      discount: 0,
      tax_amount: 0,
      processing_fees: 0,
      tips: 0,
      parser_warning: 'Tax Summary table not found',
      external_metadata: {
        page_path: pageUrl || COPILOT_TAX_SUMMARY_BASE_PATH,
        parser_warning: 'Tax Summary table not found',
      },
    };
  }

  const rows = parseTaxSummaryRows($, table);
  const totals = rows.reduce((acc, row) => {
    acc.total_sales += row.total_sales;
    acc.taxable_amount += row.taxable_amount;
    acc.discount += row.discount;
    acc.tax_amount += row.tax_amount;
    return acc;
  }, {
    total_sales: 0,
    taxable_amount: 0,
    discount: 0,
    tax_amount: 0,
  });
  const notTaxable = extractNotTaxableValues($);

  return {
    start_date: startDate || null,
    end_date: endDate || null,
    basis,
    rows,
    total_sales: totals.total_sales,
    taxable_amount: totals.taxable_amount,
    discount: totals.discount,
    tax_amount: totals.tax_amount,
    processing_fees: notTaxable.processing_fees,
    tips: notTaxable.tips,
    parser_warning: null,
    external_metadata: {
      page_path: pageUrl || COPILOT_TAX_SUMMARY_BASE_PATH,
      row_count: rows.length,
    },
  };
}

function normalizeTaxSummarySnapshot(snapshot, sourceOverride = LIVE_COPILOT_SOURCE) {
  if (!snapshot || typeof snapshot !== 'object' || !Array.isArray(snapshot.rows)) return null;
  return {
    success: true,
    source: sourceOverride || snapshot.source || LIVE_COPILOT_SOURCE,
    as_of: snapshot.as_of || new Date().toISOString(),
    basis: snapshot.basis || 'collected',
    start_date: snapshot.start_date,
    end_date: snapshot.end_date,
    rows: snapshot.rows.map((row) => ({
      tax_rate: Number(row.tax_rate) || 0,
      total_sales: Number(row.total_sales) || 0,
      taxable_amount: Number(row.taxable_amount) || 0,
      discount: Number(row.discount) || 0,
      tax_amount: Number(row.tax_amount) || 0,
      raw_tax_rate: cleanText(row.raw_tax_rate) || null,
    })),
    total_sales: Number(snapshot.total_sales) || 0,
    taxable_amount: Number(snapshot.taxable_amount) || 0,
    discount: Number(snapshot.discount) || 0,
    tax_amount: Number(snapshot.tax_amount) || 0,
    processing_fees: Number(snapshot.processing_fees) || 0,
    tips: Number(snapshot.tips) || 0,
    external_metadata: snapshot.external_metadata && typeof snapshot.external_metadata === 'object'
      ? snapshot.external_metadata
      : {},
  };
}

function buildDailyTaxRecommendation({ snapshot, backendReconstructedTax }) {
  const copilotCollectedTax = Number(snapshot?.tax_amount) || 0;
  const reconstructed = Number(backendReconstructedTax) || 0;
  return {
    recommended_transfer_amount: copilotCollectedTax,
    copilot_collected_tax: copilotCollectedTax,
    backend_reconstructed_tax: reconstructed,
    variance: Math.round((copilotCollectedTax - reconstructed) * 100) / 100,
  };
}

module.exports = {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
  COPILOT_TAX_SUMMARY_BASE_PATH,
  cleanText,
  parseCurrencyAmount,
  parseCopilotTaxSummaryHtml,
  normalizeTaxSummarySnapshot,
  buildDailyTaxRecommendation,
};
