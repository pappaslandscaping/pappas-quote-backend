#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');

const ANALYSIS_DIR = path.join(process.cwd(), 'analysis', 'mowing-pricing');
const INPUT_PATH = path.join(ANALYSIS_DIR, 'mowing-property-summary.csv');
const OUTPUT_PATH = path.join(ANALYSIS_DIR, 'mowing-size-enrichment-candidates.csv');
const DEFAULT_LIMIT = 100;

function toNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  const parsed = Number(String(value).replace(/[$,]/g, '').trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function cleanText(value) {
  return String(value || '').trim();
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

function hasAnyNonzeroSize(row) {
  const sizeFields = [
    'asset_size',
    'paved_size',
    'house_size',
    'planting_size',
    'roof_area_size',
    'roof_perimeter_size',
    'driveway_walkway_size',
  ];

  return (
    cleanText(row.has_nonzero_size).toLowerCase() === 'yes' ||
    sizeFields.some((field) => toNumber(row[field]) > 0)
  );
}

function reasonFor(row) {
  const reasons = ['missing/zero available size fields'];
  const lineCount = toNumber(row.line_count);
  if (lineCount >= 100) {
    reasons.push(`${lineCount} mowing lines`);
  } else {
    reasons.push(`high recurring volume: ${lineCount} mowing lines`);
  }
  reasons.push(`latest service ${cleanText(row.latest_service_date) || 'unknown'}`);
  return reasons.join('; ');
}

function main() {
  if (!fs.existsSync(INPUT_PATH)) {
    throw new Error(
      `Missing ${INPUT_PATH}. Run npm run analyze:mowing-pricing before selecting enrichment candidates.`
    );
  }

  const limitArg = process.argv[2] || process.env.MOWING_ENRICHMENT_LIMIT;
  const limit = Math.max(1, toNumber(limitArg) || DEFAULT_LIMIT);
  const rows = parse(fs.readFileSync(INPUT_PATH), {
    columns: true,
    bom: true,
    skip_empty_lines: true,
  });

  const candidates = rows
    .filter((row) => toNumber(row.line_count) > 0)
    .filter((row) => !hasAnyNonzeroSize(row))
    .filter((row) => cleanText(row.address))
    .sort((a, b) => {
      const lineDiff = toNumber(b.line_count) - toNumber(a.line_count);
      if (lineDiff !== 0) return lineDiff;
      const dateDiff = cleanText(b.latest_service_date).localeCompare(cleanText(a.latest_service_date));
      if (dateDiff !== 0) return dateDiff;
      return cleanText(a.property_name).localeCompare(cleanText(b.property_name));
    })
    .slice(0, limit)
    .map((row, index) => ({
      rank: index + 1,
      asset_id: cleanText(row.asset_id),
      customer_id: cleanText(row.customer_id),
      property_name: cleanText(row.property_name),
      address: cleanText(row.address),
      city: cleanText(row.city),
      state: cleanText(row.state),
      zip: cleanText(row.zip),
      line_count: toNumber(row.line_count),
      latest_service_date: cleanText(row.latest_service_date),
      median_mowing_price: toNumber(row.median_mowing_price),
      avg_mowing_price: toNumber(row.avg_mowing_price),
      reason: reasonFor(row),
    }));

  const columns = [
    'rank',
    'asset_id',
    'customer_id',
    'property_name',
    'address',
    'city',
    'state',
    'zip',
    'line_count',
    'latest_service_date',
    'median_mowing_price',
    'avg_mowing_price',
    'reason',
  ];

  writeCsv(OUTPUT_PATH, candidates, columns);
  console.log(`Wrote ${candidates.length} mowing size enrichment candidates.`);
  console.log(OUTPUT_PATH);
}

main();
