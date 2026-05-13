#!/usr/bin/env node

const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { parse } = require('csv-parse/sync');

const DEFAULT_ZIP = path.join(
  os.homedir(),
  'Downloads',
  '251020_1206_68f57cff5dff0 (1).zip'
);
const ZIP_PATH = process.argv[2] || process.env.COPILOT_EXPORT_ZIP || DEFAULT_ZIP;
const OUTPUT_DIR = path.join(process.cwd(), 'analysis', 'mowing-pricing');
const PAPPAS_MINIMUM_MOWING_PRICE = 40;

const REQUIRED_FILES = [
  'invoices_lines.csv',
  'estimates_lines.csv',
  'assets.csv',
  'customs.csv',
  'srvitems.csv',
  'invoices.csv',
  'estimates.csv',
];

function readCsvFromZip(zipPath, fileName) {
  const raw = execFileSync('unzip', ['-p', zipPath, fileName], {
    maxBuffer: 1024 * 1024 * 128,
  });
  return parse(raw, {
    columns: true,
    bom: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_empty_lines: true,
  });
}

function indexBy(rows, field) {
  return new Map(rows.filter((row) => row[field]).map((row) => [row[field], row]));
}

function toNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  const cleaned = String(value).replace(/[$,]/g, '').trim();
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}

function round(value, places = 2) {
  if (!Number.isFinite(value)) return '';
  return Number(value.toFixed(places));
}

function cleanText(value) {
  return String(value || '').trim();
}

function isRealDate(value) {
  const text = cleanText(value);
  return text && text !== '0000-00-00' && text !== '0000-00-00 00:00:00';
}

function median(values) {
  const sorted = values
    .map(toNumber)
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (sorted.length === 0) return 0;
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[middle];
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function average(values) {
  const numeric = values.map(toNumber).filter((value) => Number.isFinite(value));
  if (numeric.length === 0) return 0;
  return numeric.reduce((sum, value) => sum + value, 0) / numeric.length;
}

function csvEscape(value) {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (/[",\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function writeCsv(filePath, rows, columns) {
  const header = columns.join(',');
  const body = rows.map((row) => columns.map((column) => csvEscape(row[column])).join(','));
  fs.writeFileSync(filePath, `${header}\n${body.join('\n')}\n`);
}

function linePrice(row, prefix) {
  const total = toNumber(row[`${prefix}_total`]);
  if (total > 0) return total;
  const cost = toNumber(row[`${prefix}_cost`]);
  const qty = toNumber(row[`${prefix}_qty`]) || 1;
  return cost * qty;
}

function mowingCadence(serviceName) {
  const normalized = String(serviceName || '').toLowerCase();
  if (normalized.includes('mowing (bi-weekly)')) return 'biweekly';
  if (normalized.includes('mowing (weekly)')) return 'weekly';
  return '';
}

function isObviousContractOrMonthly(serviceName) {
  const normalized = String(serviceName || '').toLowerCase();
  return (
    normalized.includes('contract') ||
    normalized.includes('monthly') ||
    normalized.includes('season') ||
    normalized.includes('bi-monthly')
  );
}

function assetSizeFields(asset) {
  const fields = {
    asset_size: toNumber(asset.assets_size),
    asset_size_type: asset.assets_size_type || '',
    paved_size: toNumber(asset.assets_paved_size),
    house_size: toNumber(asset.assets_house_size),
    planting_size: toNumber(asset.assets_planting_size),
    roof_area_size: toNumber(asset.assets_roof_area_size),
    roof_perimeter_size: toNumber(asset.assets_roof_perimeter_size),
    driveway_walkway_size: toNumber(asset.assets_driveway_walkway_size),
  };
  fields.has_nonzero_size = Object.entries(fields).some(([key, value]) => {
    return key.endsWith('_size') && toNumber(value) > 0;
  })
    ? 'yes'
    : 'no';
  return fields;
}

function serviceCatalogSummary(srvItems) {
  return srvItems
    .filter((row) => /mowing/i.test(row.item_name || ''))
    .map((row) => `${row.item_id}: ${row.item_name} ($${round(toNumber(row.item_price))})`)
    .sort();
}

function buildLine(row, options) {
  const { source, prefix, parentsById, assetsById, customersById } = options;
  const serviceName = row[`${prefix}_srv_name`] || '';
  const cadence = mowingCadence(serviceName);
  if (!cadence) return null;
  if (isObviousContractOrMonthly(serviceName)) return null;

  const price = linePrice(row, prefix);
  if (price <= 0) return null;

  const parentId = row[`${prefix}_${source === 'invoice' ? 'inv_id' : 'est_id'}`] || '';
  const parent = parentsById.get(parentId) || {};
  const assetId = row[`${prefix}_asset_id`] || parent[`${source === 'invoice' ? 'inv' : 'est'}_asset_id`] || '';
  const asset = assetsById.get(assetId) || {};
  const customerId =
    asset.assets_custom_id ||
    parent[`${source === 'invoice' ? 'inv' : 'est'}_customer_id`] ||
    '';
  const customer = customersById.get(customerId) || {};
  const sizes = assetSizeFields(asset);

  return {
    source,
    record_id: row[`${prefix}_id`] || '',
    parent_id: parentId,
    service_date: row[`${prefix}_date`] || parent[`${source === 'invoice' ? 'inv' : 'est'}_date`] || '',
    service_id: row[`${prefix}_srv_id`] || '',
    service_name: serviceName,
    cadence,
    price: round(price),
    cost: round(toNumber(row[`${prefix}_cost`])),
    quantity: round(toNumber(row[`${prefix}_qty`])),
    asset_id: assetId,
    customer_id: customerId,
    customer_name: cleanText(
      customer.custom_company_name ||
        [customer.custom_firstname, customer.custom_lastname].filter(Boolean).join(' ')
    ),
    property_name: cleanText(asset.assets_name),
    address: cleanText(asset.assets_addr),
    city: cleanText(asset.assets_city),
    state: cleanText(asset.assets_state),
    zip: cleanText(asset.assets_zip),
    ...sizes,
  };
}

function summarizeProperties(lines) {
  const groups = new Map();
  for (const line of lines) {
    const key = line.asset_id || `missing-asset:${line.customer_id}:${line.address}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(line);
  }

  return [...groups.values()]
    .map((group) => {
      const exemplar = group[0];
      const prices = group.map((line) => line.price);
      const latestServiceDate = group
        .map((line) => line.service_date)
        .filter(isRealDate)
        .sort()
        .at(-1);

      return {
        asset_id: exemplar.asset_id,
        customer_id: exemplar.customer_id,
        customer_name: exemplar.customer_name,
        property_name: exemplar.property_name,
        address: exemplar.address,
        city: exemplar.city,
        state: exemplar.state,
        zip: exemplar.zip,
        line_count: group.length,
        weekly_line_count: group.filter((line) => line.cadence === 'weekly').length,
        biweekly_line_count: group.filter((line) => line.cadence === 'biweekly').length,
        latest_service_date: latestServiceDate || '',
        min_mowing_price: round(Math.min(...prices)),
        median_mowing_price: round(median(prices)),
        avg_mowing_price: round(average(prices)),
        max_mowing_price: round(Math.max(...prices)),
        asset_size: exemplar.asset_size,
        asset_size_type: exemplar.asset_size_type,
        paved_size: exemplar.paved_size,
        house_size: exemplar.house_size,
        planting_size: exemplar.planting_size,
        roof_area_size: exemplar.roof_area_size,
        roof_perimeter_size: exemplar.roof_perimeter_size,
        driveway_walkway_size: exemplar.driveway_walkway_size,
        has_nonzero_size: exemplar.has_nonzero_size,
      };
    })
    .sort((a, b) => {
      if (b.line_count !== a.line_count) return b.line_count - a.line_count;
      return String(a.property_name).localeCompare(String(b.property_name));
    });
}

function formatMoney(value) {
  return `$${round(value).toFixed(2)}`;
}

function markdownSummary({ lines, propertySummary, srvItems }) {
  const weekly = lines.filter((line) => line.cadence === 'weekly');
  const biweekly = lines.filter((line) => line.cadence === 'biweekly');
  const invoiceLines = lines.filter((line) => line.source === 'invoice');
  const estimateLines = lines.filter((line) => line.source === 'estimate');
  const missingAssetLines = lines.filter((line) => !line.asset_id);
  const placeholderDateLines = lines.filter((line) => !isRealDate(line.service_date));
  const propertiesWithNonzeroSize = propertySummary.filter((row) => row.has_nonzero_size === 'yes');
  const needsSqft = propertySummary
    .filter((row) => row.has_nonzero_size !== 'yes' && row.asset_id)
    .slice(0, 20);

  const catalog = serviceCatalogSummary(srvItems);
  const enoughSizeData = propertiesWithNonzeroSize.length >= Math.max(10, propertySummary.length * 0.25);

  const linesMd = [
    '# Mowing Price Summary',
    '',
    `Generated from: \`${ZIP_PATH}\``,
    '',
    '## Scope',
    '',
    '- Included service names containing `Mowing (Weekly)`.',
    '- Reported service names containing `Mowing (Bi-Weekly)` separately.',
    '- Excluded zero-dollar lines.',
    '- Excluded obvious contract, monthly, season, and bi-monthly lines for this first pass.',
    `- Business rule noted: Pappas minimum mowing price is ${formatMoney(PAPPAS_MINIMUM_MOWING_PRICE)}.`,
    '',
    '## Totals',
    '',
    `- Total mowing lines found: ${lines.length}`,
    `- Invoice mowing line count: ${invoiceLines.length}`,
    `- Estimate mowing line count: ${estimateLines.length}`,
    `- Weekly mowing line count: ${weekly.length}`,
    `- Biweekly mowing line count: ${biweekly.length}`,
    `- Median weekly price: ${formatMoney(median(weekly.map((line) => line.price)))}`,
    `- Average weekly price: ${formatMoney(average(weekly.map((line) => line.price)))}`,
    `- Median biweekly price: ${formatMoney(median(biweekly.map((line) => line.price)))}`,
    `- Average biweekly price: ${formatMoney(average(biweekly.map((line) => line.price)))}`,
    `- Properties with nonzero size fields: ${propertiesWithNonzeroSize.length} of ${propertySummary.length}`,
    `- Lines missing asset IDs: ${missingAssetLines.length}`,
    `- Lines with placeholder or missing service dates: ${placeholderDateLines.length}`,
    '',
    '## Pricing Readiness',
    '',
    enoughSizeData
      ? '- There is some asset size data available, but review the CSV before building recommendations.'
      : '- Not enough sqft data is available for reliable pricing recommendations yet.',
    '',
    '## Top Recurring Mowing Properties Needing Sqft Enrichment',
    '',
    '| Rank | Asset ID | Customer ID | Property | Address | Lines | Latest Service | Median Price |',
    '| ---: | --- | --- | --- | --- | ---: | --- | ---: |',
    ...needsSqft.map((row, index) => {
      const property = row.property_name || row.customer_name || '(unnamed)';
      const address = [row.address, row.city, row.zip].filter(Boolean).join(', ');
      return `| ${index + 1} | ${row.asset_id || ''} | ${row.customer_id || ''} | ${property} | ${address} | ${row.line_count} | ${row.latest_service_date} | ${formatMoney(row.median_mowing_price)} |`;
    }),
    '',
    '## Mowing Service Catalog Entries Observed',
    '',
    ...catalog.map((entry) => `- ${entry}`),
    '',
  ];

  return linesMd.join('\n');
}

function main() {
  if (!fs.existsSync(ZIP_PATH)) {
    throw new Error(`Copilot export ZIP not found: ${ZIP_PATH}`);
  }

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });

  const files = Object.fromEntries(
    REQUIRED_FILES.map((fileName) => [fileName, readCsvFromZip(ZIP_PATH, fileName)])
  );

  const assetsById = indexBy(files['assets.csv'], 'assets_id');
  const customersById = indexBy(files['customs.csv'], 'custom_id');
  const invoicesById = indexBy(files['invoices.csv'], 'inv_id');
  const estimatesById = indexBy(files['estimates.csv'], 'est_id');

  const invoiceLines = files['invoices_lines.csv']
    .map((row) =>
      buildLine(row, {
        source: 'invoice',
        prefix: 'invrec',
        parentsById: invoicesById,
        assetsById,
        customersById,
      })
    )
    .filter(Boolean);

  const estimateLines = files['estimates_lines.csv']
    .map((row) =>
      buildLine(row, {
        source: 'estimate',
        prefix: 'estrec',
        parentsById: estimatesById,
        assetsById,
        customersById,
      })
    )
    .filter(Boolean);

  const lines = [...invoiceLines, ...estimateLines].sort((a, b) => {
    if (a.asset_id !== b.asset_id) return String(a.asset_id).localeCompare(String(b.asset_id));
    return String(a.service_date).localeCompare(String(b.service_date));
  });
  const propertySummary = summarizeProperties(lines);

  const lineColumns = [
    'source',
    'record_id',
    'parent_id',
    'service_date',
    'service_id',
    'service_name',
    'cadence',
    'price',
    'cost',
    'quantity',
    'asset_id',
    'customer_id',
    'customer_name',
    'property_name',
    'address',
    'city',
    'state',
    'zip',
    'asset_size',
    'asset_size_type',
    'paved_size',
    'house_size',
    'planting_size',
    'roof_area_size',
    'roof_perimeter_size',
    'driveway_walkway_size',
    'has_nonzero_size',
  ];

  const propertyColumns = [
    'asset_id',
    'customer_id',
    'customer_name',
    'property_name',
    'address',
    'city',
    'state',
    'zip',
    'line_count',
    'weekly_line_count',
    'biweekly_line_count',
    'latest_service_date',
    'min_mowing_price',
    'median_mowing_price',
    'avg_mowing_price',
    'max_mowing_price',
    'asset_size',
    'asset_size_type',
    'paved_size',
    'house_size',
    'planting_size',
    'roof_area_size',
    'roof_perimeter_size',
    'driveway_walkway_size',
    'has_nonzero_size',
  ];

  writeCsv(path.join(OUTPUT_DIR, 'mowing-lines.csv'), lines, lineColumns);
  writeCsv(
    path.join(OUTPUT_DIR, 'mowing-property-summary.csv'),
    propertySummary,
    propertyColumns
  );
  fs.writeFileSync(
    path.join(OUTPUT_DIR, 'mowing-price-summary.md'),
    markdownSummary({
      lines,
      propertySummary,
      srvItems: files['srvitems.csv'],
    })
  );

  console.log(`Wrote ${lines.length} mowing lines across ${propertySummary.length} properties.`);
  console.log(path.join(OUTPUT_DIR, 'mowing-lines.csv'));
  console.log(path.join(OUTPUT_DIR, 'mowing-property-summary.csv'));
  console.log(path.join(OUTPUT_DIR, 'mowing-price-summary.md'));
}

main();
