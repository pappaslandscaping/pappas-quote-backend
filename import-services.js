require('dotenv').config({ path: __dirname + '/.env' });
const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { Pool } = require('pg');

const CSV_PATH = '/Users/theresapappas/Downloads/file (4).csv';

// --- Skip lists ---
const SKIP_NAMES = new Set([
  'credit adjustment',
  'balance adjustment',
  'late fee',
  'returned check fee',
  'returned payment fee',
  'sales discount',
  'tip',
  'credit card/ach processing fee',
  'fuel surcharge',
  'product / service',
  'sales',
]);

function looksLikeAddress(name) {
  // Pattern: starts with digits followed by a street-like word
  return /^\d+\s+\w+/.test(name.trim());
}

// --- Category mapping ---
function mapCategory(cat) {
  const c = (cat || '').trim();
  if (c === 'Landscaping Services') return 'Landscaping';
  if (c === 'Services') return 'General';
  if (c === 'Billable Expense Income') return 'Billing';
  if (c === 'Sales Discounts') return 'Billing';
  if (c === 'Fuel Surcharge') return 'Billing';
  if (c === '' || !c) return 'General';
  return c; // fallback: keep as-is
}

// --- Clean HTML entities ---
function cleanHtml(str) {
  if (!str) return '';
  return str
    .replace(/&amp;/g, '&')
    .replace(/&nbsp;/g, ' ')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

async function main() {
  console.log('Reading CSV file...');
  const csvData = fs.readFileSync(CSV_PATH, 'utf-8');

  console.log('Parsing CSV...');
  const records = parse(csvData, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  console.log(`Parsed ${records.length} total rows from CSV.`);

  // --- Filter and deduplicate ---
  const seenNames = new Set();
  const services = [];

  for (const row of records) {
    const name = (row['Name'] || '').trim();
    const category = (row['Category'] || '').trim();
    const nameLower = name.toLowerCase();

    // Skip Common category
    if (category === 'Common') {
      continue;
    }

    // Skip addresses
    if (looksLikeAddress(name)) {
      continue;
    }

    // Skip financial adjustments and special items
    if (SKIP_NAMES.has(nameLower)) {
      continue;
    }

    // Skip duplicates (case-insensitive), keep first occurrence
    if (seenNames.has(nameLower)) {
      continue;
    }
    seenNames.add(nameLower);

    const rate = parseFloat(row['Rate Charged to Client'] || '0') || 0;
    const tax1 = parseFloat(row['Tax1 %'] || '0') || 0;
    const description = cleanHtml((row['Description'] || '').trim());
    const mappedCategory = mapCategory(category);

    services.push({
      name,
      default_rate: rate,
      category: mappedCategory,
      description,
      tax_rate: tax1,
    });
  }

  console.log(`Filtered to ${services.length} unique service items.`);

  // --- Connect to database ---
  console.log('Connecting to database...');
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });

  try {
    // Create table if not exists
    console.log('Ensuring service_items table exists...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS service_items (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        default_rate DECIMAL(10,2) DEFAULT 0,
        duration_minutes INTEGER,
        category VARCHAR(100),
        active BOOLEAN DEFAULT true,
        description TEXT,
        tax_rate DECIMAL(5,2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);
    // Add columns if table already existed without them
    await pool.query('ALTER TABLE service_items ADD COLUMN IF NOT EXISTS description TEXT').catch(() => {});
    await pool.query('ALTER TABLE service_items ADD COLUMN IF NOT EXISTS tax_rate DECIMAL(5,2) DEFAULT 0').catch(() => {});

    // Insert services, skipping duplicates by lowercase name
    let inserted = 0;
    let skipped = 0;

    for (const svc of services) {
      // Check if already exists (case-insensitive)
      const existing = await pool.query(
        'SELECT id FROM service_items WHERE LOWER(name) = LOWER($1)',
        [svc.name]
      );

      if (existing.rows.length > 0) {
        console.log(`  SKIP (already exists): ${svc.name}`);
        skipped++;
        continue;
      }

      await pool.query(
        `INSERT INTO service_items (name, default_rate, category, description, tax_rate)
         VALUES ($1, $2, $3, $4, $5)`,
        [svc.name, svc.default_rate, svc.category, svc.description, svc.tax_rate]
      );
      console.log(`  INSERTED: ${svc.name} [${svc.category}] rate=$${svc.default_rate} tax=${svc.tax_rate}%`);
      inserted++;
    }

    console.log('\n--- Import Complete ---');
    console.log(`Total unique services from CSV: ${services.length}`);
    console.log(`Inserted: ${inserted}`);
    console.log(`Skipped (already in DB): ${skipped}`);

    // Show final count
    const countResult = await pool.query('SELECT COUNT(*) FROM service_items');
    console.log(`Total service_items in database: ${countResult.rows[0].count}`);
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error('Import failed:', err);
  process.exit(1);
});
