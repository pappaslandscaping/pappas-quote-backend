// ═══════════════════════════════════════════════════════════
// Startup Schema & Bootstrap — extracted from server.js
// Table DDLs, ALTER migrations, seed data, and table init
//
// Usage:
//   As module:     require('./lib/startup-schema')
//   Standalone:    node lib/startup-schema.js          (runs all migrations)
//   Dry-run:       DRY_RUN=1 node lib/startup-schema.js
// ═══════════════════════════════════════════════════════════

const crypto = require('crypto');

// ─── Auth bootstrap helpers ──────────────────────────────

const ADMIN_USERS_TABLE = `CREATE TABLE IF NOT EXISTS admin_users (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  name VARCHAR(255),
  role VARCHAR(50) DEFAULT 'admin',
  last_login TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

function hashPassword(password, existingSalt) {
  const salt = existingSalt || crypto.randomBytes(32).toString('hex');
  const hash = crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex');
  return salt + ':' + hash;
}

// ─── Table DDL constants ─────────────────────────────────

const PAYMENTS_TABLE = `CREATE TABLE IF NOT EXISTS payments (
  id SERIAL PRIMARY KEY,
  payment_id VARCHAR(100) UNIQUE,
  invoice_id INTEGER NOT NULL REFERENCES invoices(id),
  customer_id INTEGER,
  amount DECIMAL(10,2) NOT NULL,
  method VARCHAR(50),
  status VARCHAR(30) DEFAULT 'pending',
  square_payment_id VARCHAR(100),
  square_order_id VARCHAR(100),
  square_receipt_url TEXT,
  card_brand VARCHAR(30),
  card_last4 VARCHAR(4),
  ach_bank_name VARCHAR(100),
  qb_payment_id VARCHAR(100),
  notes TEXT,
  failure_reason TEXT,
  refund_amount DECIMAL(10,2) DEFAULT 0,
  paid_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const PORTAL_TOKENS_TABLE = `CREATE TABLE IF NOT EXISTS customer_portal_tokens (
  id SERIAL PRIMARY KEY,
  token VARCHAR(64) UNIQUE NOT NULL,
  customer_id INTEGER,
  email VARCHAR(255) NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const BUSINESS_SETTINGS_TABLE = `CREATE TABLE IF NOT EXISTS business_settings (
  id SERIAL PRIMARY KEY,
  key VARCHAR(100) UNIQUE NOT NULL,
  value JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const LATE_FEES_TABLE = `CREATE TABLE IF NOT EXISTS late_fees (
  id SERIAL PRIMARY KEY,
  invoice_id INTEGER NOT NULL,
  fee_amount DECIMAL(10,2) NOT NULL,
  fee_type VARCHAR(20) DEFAULT 'percentage',
  fee_percentage DECIMAL(5,2),
  days_overdue INTEGER,
  waived BOOLEAN DEFAULT false,
  waived_at TIMESTAMP,
  waived_by VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const RECURRING_INVOICE_LOG_TABLE = `CREATE TABLE IF NOT EXISTS recurring_invoice_log (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  billing_month VARCHAR(7) NOT NULL,
  invoice_id INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(customer_id, billing_month)
)`;

const RECURRING_JOB_LOG_TABLE = `CREATE TABLE IF NOT EXISTS recurring_job_log (
  id SERIAL PRIMARY KEY,
  source_job_id INTEGER NOT NULL,
  generated_for_date DATE NOT NULL,
  generated_job_id INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(source_job_id, generated_for_date)
)`;

const CUSTOMER_SAVED_CARDS_TABLE = `CREATE TABLE IF NOT EXISTS customer_saved_cards (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  square_card_id VARCHAR(255) NOT NULL,
  card_brand VARCHAR(30),
  last4 VARCHAR(4),
  exp_month INTEGER,
  exp_year INTEGER,
  cardholder_name VARCHAR(255),
  is_default BOOLEAN DEFAULT false,
  enabled BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const SERVICE_REQUESTS_TABLE = `CREATE TABLE IF NOT EXISTS service_requests (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  type VARCHAR(50) DEFAULT 'service',
  service_type VARCHAR(100),
  description TEXT,
  preferred_date DATE,
  urgency VARCHAR(20) DEFAULT 'normal',
  status VARCHAR(20) DEFAULT 'pending',
  admin_notes TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const CUSTOMER_COMMUNICATION_PREFS_TABLE = `CREATE TABLE IF NOT EXISTS customer_communication_prefs (
  id SERIAL PRIMARY KEY,
  customer_id INTEGER UNIQUE NOT NULL,
  email_invoices BOOLEAN DEFAULT true,
  email_reminders BOOLEAN DEFAULT true,
  email_marketing BOOLEAN DEFAULT false,
  sms_reminders BOOLEAN DEFAULT false,
  sms_marketing BOOLEAN DEFAULT false,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const EMAIL_TEMPLATES_TABLE = `CREATE TABLE IF NOT EXISTS email_templates (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  slug VARCHAR(100) UNIQUE NOT NULL,
  category VARCHAR(50) DEFAULT 'system',
  subject TEXT,
  body TEXT,
  sms_body TEXT,
  channel VARCHAR(10) DEFAULT 'email',
  variables JSONB DEFAULT '[]',
  is_default BOOLEAN DEFAULT false,
  is_active BOOLEAN DEFAULT true,
  options JSONB DEFAULT '{}',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const EMAIL_LOG_TABLE = `CREATE TABLE IF NOT EXISTS email_log (
  id SERIAL PRIMARY KEY,
  recipient_email VARCHAR(255) NOT NULL,
  subject VARCHAR(500),
  email_type VARCHAR(50) DEFAULT 'general',
  customer_id INTEGER,
  customer_name VARCHAR(255),
  invoice_id INTEGER,
  quote_id INTEGER,
  status VARCHAR(20) DEFAULT 'sent',
  error_message TEXT,
  html_body TEXT,
  open_token VARCHAR(255),
  sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

const CAMPAIGN_SENDS_TABLE = `CREATE TABLE IF NOT EXISTS campaign_sends (
  id SERIAL PRIMARY KEY,
  campaign_id INTEGER,
  template_id INTEGER,
  customer_id INTEGER,
  customer_email VARCHAR(255),
  status VARCHAR(20) DEFAULT 'sent',
  sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  opened_at TIMESTAMP,
  clicked_at TIMESTAMP,
  tracking_id VARCHAR(64) UNIQUE,
  error_message TEXT
)`;

// ─── Exported helper ─────────────────────────────────────

async function ensurePaymentsTables(pool) {
  await pool.query(PAYMENTS_TABLE);
  await pool.query(PORTAL_TOKENS_TABLE);
}

// ─── Main startup migrations ─────────────────────────────

async function runStartupMigrations(pool) {
  // Widen contract columns that are too narrow for signature data
  try {
    await pool.query(`ALTER TABLE sent_quotes ALTER COLUMN contract_signature_data TYPE TEXT`);
    console.log('✅ Widened contract_signature_data to TEXT');
  } catch(e) { /* column may already be TEXT or not exist */ }
  try {
    await pool.query(`ALTER TABLE sent_quotes ALTER COLUMN contract_signer_ip TYPE VARCHAR(255)`);
    console.log('✅ Widened contract_signer_ip to VARCHAR(255)');
  } catch(e) { /* already wide enough or not exist */ }
  try {
    await pool.query(`ALTER TABLE sent_quotes ALTER COLUMN contract_signer_name TYPE VARCHAR(255)`);
    console.log('✅ Widened contract_signer_name to VARCHAR(255)');
  } catch(e) { /* already wide enough or not exist */ }
  try {
    await pool.query(`ALTER TABLE sent_quotes ALTER COLUMN contract_signature_type TYPE VARCHAR(50)`);
    console.log('✅ Widened contract_signature_type to VARCHAR(50)');
  } catch(e) { /* already wide enough or not exist */ }

  // Fix quotes stuck at 'signed' that already have contract signed
  try {
    const fixed = await pool.query(`UPDATE sent_quotes SET status = 'contracted' WHERE status = 'signed' AND contract_signed_at IS NOT NULL RETURNING id, quote_number`);
    if (fixed.rowCount > 0) console.log('✅ Fixed ' + fixed.rowCount + ' quotes signed→contracted (had contract_signed_at):', fixed.rows.map(r => r.quote_number).join(', '));
  } catch(e) { console.error('Migration fix error:', e.message); }

  // Fix quotes stuck at 'signed' from before the VARCHAR(45) fix
  try {
    const fixed2 = await pool.query(`UPDATE sent_quotes SET status = 'contracted', contract_signed_at = updated_at WHERE status = 'signed' AND contract_signed_at IS NULL AND created_at < '2026-03-04' RETURNING id, quote_number`);
    if (fixed2.rowCount > 0) console.log('✅ Fixed ' + fixed2.rowCount + ' old signed quotes→contracted (pre-fix):', fixed2.rows.map(r => r.quote_number).join(', '));
  } catch(e) { console.error('Migration fix error:', e.message); }

  // Create payments & portal tokens tables
  try {
    await pool.query(PAYMENTS_TABLE);
    await pool.query(PORTAL_TOKENS_TABLE);
    console.log('✅ Payments & portal tokens tables ready');
  } catch(e) { console.error('Payments table error:', e.message); }

  // Add payment columns to invoices
  const invoicePaymentCols = [
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_token VARCHAR(64)',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_token_created_at TIMESTAMP',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS square_invoice_id VARCHAR(100)',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS viewed_at TIMESTAMP',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS reminder_sent_at TIMESTAMP',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS reminder_count INTEGER DEFAULT 0',
  ];
  for (const sql of invoicePaymentCols) {
    try { await pool.query(sql); } catch(e) { /* column may already exist */ }
  }
  console.log('✅ Invoice payment columns ready');

  // CopilotCRM Feature Parity Tables
  const newTables = [
    BUSINESS_SETTINGS_TABLE, LATE_FEES_TABLE, RECURRING_INVOICE_LOG_TABLE,
    RECURRING_JOB_LOG_TABLE, CUSTOMER_SAVED_CARDS_TABLE, SERVICE_REQUESTS_TABLE,
    CUSTOMER_COMMUNICATION_PREFS_TABLE, EMAIL_TEMPLATES_TABLE, CAMPAIGN_SENDS_TABLE
  ];
  for (const sql of newTables) {
    try { await pool.query(sql); } catch(e) { console.error('Table create error:', e.message); }
  }
  console.log('✅ CopilotCRM feature tables ready');

  // Add channel column to email_templates
  try { await pool.query("ALTER TABLE email_templates ADD COLUMN IF NOT EXISTS channel VARCHAR(10) DEFAULT 'email'"); } catch(e) {}

  // Add customer_type column for lead/customer pipeline
  try { await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS customer_type VARCHAR(20) DEFAULT 'customer'"); } catch(e) {}
  console.log('✅ Customer type column ready');

  // Add new columns to invoices
  const invoiceNewCols = [
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS late_fee_total DECIMAL(10,2) DEFAULT 0',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS is_auto_generated BOOLEAN DEFAULT false',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS auto_gen_source VARCHAR(50)',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS billing_month VARCHAR(7)',
  ];
  for (const sql of invoiceNewCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // Add new columns to scheduled_jobs
  const jobNewCols = [
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS recurring_end_date DATE',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS parent_job_id INTEGER',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS is_recurring BOOLEAN DEFAULT false',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS recurring_pattern VARCHAR(50)',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS recurring_day_of_week INTEGER',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS recurring_start_date DATE',
    "ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS pipeline_stage VARCHAR(30) DEFAULT 'pending'",
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS material_cost DECIMAL(10,2) DEFAULT 0',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS labor_cost DECIMAL(10,2) DEFAULT 0',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS expense_total DECIMAL(10,2) DEFAULT 0',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS invoice_id INTEGER',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS property_id INTEGER',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS lat DECIMAL(10,7)',
    'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS lng DECIMAL(10,7)',
    "ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS geocode_quality VARCHAR(20)",
  ];
  for (const sql of jobNewCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // Add new columns to invoices for payment schedules
  const invoicePayCols = [
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_schedule JSONB',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS installment_count INTEGER DEFAULT 1',
  ];
  for (const sql of invoicePayCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // Add processing fee columns to invoices
  const invoiceFeeCols = [
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS processing_fee DECIMAL(10,2) DEFAULT 0',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS processing_fee_passed BOOLEAN DEFAULT false',
  ];
  for (const sql of invoiceFeeCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // External-source bookkeeping for the CopilotCRM importer.
  //   external_invoice_id stores the source record's primary id (e.g. the
  //     <tr id="..."> from CopilotCRM's invoice list HTML) so re-imports
  //     upsert cleanly instead of duplicating.
  //   external_source records where the row originated ('copilotcrm', etc).
  //   external_metadata holds source-specific fields we don't want as
  //     dedicated columns yet (copilot_customer_id, property_name, crew,
  //     sent_status, view_path, edit_path, raw_status, ...).
  //   imported_at marks the most recent import that touched the row.
  const invoiceImportCols = [
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS external_invoice_id VARCHAR(64)',
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS external_source VARCHAR(40)',
    "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS external_metadata JSONB DEFAULT '{}'::jsonb",
    'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS imported_at TIMESTAMP',
    'CREATE UNIQUE INDEX IF NOT EXISTS invoices_external_uidx ON invoices(external_source, external_invoice_id) WHERE external_invoice_id IS NOT NULL',
  ];
  for (const sql of invoiceImportCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // Quotes table (incoming quote requests from website)
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS quotes (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255),
      email VARCHAR(255),
      phone VARCHAR(50),
      address TEXT,
      package VARCHAR(100),
      services TEXT[],
      questions JSONB,
      notes TEXT,
      source VARCHAR(100),
      status VARCHAR(30) DEFAULT 'new',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    console.log('✅ Quotes table ready');
  } catch(e) { console.error('Quotes table error:', e.message); }

  // Cancellations table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS cancellations (
      id SERIAL PRIMARY KEY,
      customer_name VARCHAR(255),
      customer_email VARCHAR(255),
      customer_address TEXT,
      cancellation_reason TEXT,
      original_email_body TEXT,
      status VARCHAR(30) DEFAULT 'pending',
      copilot_crm_updated BOOLEAN DEFAULT false,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    console.log('✅ Cancellations table ready');
  } catch(e) { console.error('Cancellations table error:', e.message); }

  // Campaigns table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS campaigns (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      description TEXT,
      form_url TEXT,
      status VARCHAR(30) DEFAULT 'active',
      template_id INTEGER,
      send_count INTEGER DEFAULT 0,
      open_count INTEGER DEFAULT 0,
      click_count INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    console.log('✅ Campaigns table ready');
  } catch(e) { console.error('Campaigns table error:', e.message); }

  // Campaign submissions table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS campaign_submissions (
      id SERIAL PRIMARY KEY,
      campaign_id VARCHAR(255),
      name VARCHAR(255),
      email VARCHAR(255),
      phone VARCHAR(50),
      address TEXT,
      status VARCHAR(30) DEFAULT 'new',
      notes TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    console.log('✅ Campaign submissions table ready');
  } catch(e) { console.error('Campaign submissions table error:', e.message); }

  // Internal notes table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS internal_notes (
      id SERIAL PRIMARY KEY,
      entity_type VARCHAR(30) NOT NULL,
      entity_id INTEGER NOT NULL,
      author_name VARCHAR(255),
      author_id INTEGER,
      content TEXT NOT NULL,
      pinned BOOLEAN DEFAULT false,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_notes_entity ON internal_notes (entity_type, entity_id)`);
  } catch(e) {}

  // Referrals table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS referrals (
      id SERIAL PRIMARY KEY,
      referrer_id INTEGER NOT NULL,
      referred_name VARCHAR(255) NOT NULL,
      referred_customer_id INTEGER,
      status VARCHAR(20) DEFAULT 'pending',
      credited_at TIMESTAMP,
      notes TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals (referrer_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_referrals_status ON referrals (status)`);
  } catch(e) {}

  // Job expenses table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS job_expenses (
      id SERIAL PRIMARY KEY,
      job_id INTEGER NOT NULL,
      description VARCHAR(500),
      category VARCHAR(100),
      amount DECIMAL(10,2) NOT NULL,
      receipt_url TEXT,
      created_by VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
  } catch(e) {}

  // Add new columns to customers
  const customerNewCols = [
    'ALTER TABLE customers ADD COLUMN IF NOT EXISTS square_customer_id VARCHAR(255)',
    'ALTER TABLE customers ADD COLUMN IF NOT EXISTS monthly_plan_amount DECIMAL(10,2) DEFAULT 0',
    'ALTER TABLE customers ADD COLUMN IF NOT EXISTS tax_exempt BOOLEAN DEFAULT false',
  ];
  for (const sql of customerNewCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // Add tax columns to properties
  const propertyNewCols = [
    'ALTER TABLE properties ADD COLUMN IF NOT EXISTS county_tax DECIMAL(5,3) DEFAULT NULL',
    'ALTER TABLE properties ADD COLUMN IF NOT EXISTS city_tax DECIMAL(5,3) DEFAULT NULL',
    'ALTER TABLE properties ADD COLUMN IF NOT EXISTS state_tax DECIMAL(5,3) DEFAULT NULL',
  ];
  for (const sql of propertyNewCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // Add new columns to campaigns
  const campaignNewCols = [
    'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS template_id INTEGER',
    'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_count INTEGER DEFAULT 0',
    'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS open_count INTEGER DEFAULT 0',
    'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS click_count INTEGER DEFAULT 0',
  ];
  for (const sql of campaignNewCols) {
    try { await pool.query(sql); } catch(e) { /* */ }
  }

  // Time tracking table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS time_entries (
      id SERIAL PRIMARY KEY,
      crew_id INTEGER,
      crew_name VARCHAR(255),
      job_id INTEGER,
      customer_name VARCHAR(255),
      address VARCHAR(500),
      service_type VARCHAR(100),
      clock_in TIMESTAMP NOT NULL,
      clock_out TIMESTAMP,
      break_minutes INTEGER DEFAULT 0,
      notes TEXT,
      status VARCHAR(20) DEFAULT 'active',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
  } catch(e) {}

  // Dispatch templates table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS dispatch_templates (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      zip_codes TEXT,
      crew_id INTEGER,
      service_type VARCHAR(100),
      default_duration INTEGER DEFAULT 30,
      notes TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
  } catch(e) {}

  // Service programs table (multi-step)
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS service_programs (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      description TEXT,
      status VARCHAR(20) DEFAULT 'active',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS program_steps (
      id SERIAL PRIMARY KEY,
      program_id INTEGER NOT NULL REFERENCES service_programs(id) ON DELETE CASCADE,
      step_order INTEGER NOT NULL,
      service_type VARCHAR(100) NOT NULL,
      description TEXT,
      estimated_duration INTEGER DEFAULT 30,
      offset_days INTEGER DEFAULT 0,
      price DECIMAL(10,2),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS customer_programs (
      id SERIAL PRIMARY KEY,
      customer_id INTEGER NOT NULL,
      program_id INTEGER NOT NULL,
      property_id INTEGER,
      start_date DATE NOT NULL,
      current_step INTEGER DEFAULT 1,
      status VARCHAR(20) DEFAULT 'active',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
  } catch(e) {}

  // Email log table
  try { await pool.query(EMAIL_LOG_TABLE); } catch(e) {}

  // Season kickoff responses table
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS season_kickoff_responses (
      id SERIAL PRIMARY KEY,
      token VARCHAR(64) UNIQUE NOT NULL,
      customer_id INTEGER,
      customer_name VARCHAR(255),
      customer_email VARCHAR(255),
      services JSONB,
      properties JSONB,
      status VARCHAR(20) DEFAULT 'pending',
      notes TEXT,
      viewed_at TIMESTAMP,
      view_count INTEGER DEFAULT 0,
      responded_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      email_opened_at TIMESTAMP,
      email_open_count INTEGER DEFAULT 0
    )`);
  } catch(e) {}

  console.log('✅ Time tracking, dispatch templates, service programs, and email log tables ready');
  console.log('✅ All CopilotCRM column migrations ready');

  // Seed default late fee settings (matches contract terms at Section V)
  try {
    await pool.query(`
      INSERT INTO business_settings (key, value) VALUES
        ('late_fee_rules', '{"grace_period_days": 30, "initial_fee_percent": 10, "recurring_fee_percent": 5, "recurring_interval_days": 30, "max_fees": 3, "enabled": true}'),
        ('recurring_invoice_config', '{"auto_send": true, "due_date_offset_days": 0, "include_payment_link": true}'),
        ('company_info', '{"name": "Pappas & Co. Landscaping", "phone": "(440) 886-7318", "email": "hello@pappaslandscaping.com", "website": "pappaslandscaping.com", "address": "PO Box 770057, Lakewood, Ohio 44107"}')
      ON CONFLICT (key) DO NOTHING
    `);
    console.log('✅ Default business settings seeded');
  } catch(e) { console.error('Settings seed error:', e.message); }

  // Seed default invoice settings
  try {
    await pool.query(`
      INSERT INTO business_settings (key, value) VALUES
        ('invoice_creation_mode', '{"mode": "per_visit"}'),
        ('invoice_closing_mode', '{"mode": "per_visit"}'),
        ('invoice_start_number', '{"number": 1001}'),
        ('invoice_date_mode', '{"mode": "date_sent"}'),
        ('invoice_defaults', '{"set_service_date_today": true, "notes": "", "terms": "Due upon receipt.", "show_status_stamp": true, "show_property_name": "address_only", "show_custom_fields": false, "due_date_visibility": "show_all", "event_date_mode": "scheduled", "visible_qty": true, "visible_rate": true, "visible_budgeted_hours": false}'),
        ('invoice_email_settings', '{"attach_pdf": true, "template": "default"}'),
        ('invoice_sms_settings', '{"template": "default"}'),
        ('invoice_send_method', '{"preferred": "email"}'),
        ('invoice_auto_send', '{"enabled": false, "frequency": "weekly", "day": "friday"}'),
        ('tax_defaults', '{"default_rate": 8}'),
        ('processing_fee_config', '{"enabled": false, "card_fee_percent": 2.9, "card_fee_fixed": 0.30, "ach_fee_percent": 1.0, "ach_fee_fixed": 0}')
      ON CONFLICT (key) DO NOTHING
    `);
    console.log('✅ Default invoice settings seeded');
  } catch(e) { console.error('Invoice settings seed error:', e.message); }

  // Create admin_users table and seed default admin accounts
  try {
    await pool.query(ADMIN_USERS_TABLE);
    const adminUsers = [
      { email: 'hello@pappaslandscaping.com', password: process.env.ADMIN_PASSWORD || 'changeme', name: 'Theresa Pappas', role: 'owner' },
      { email: 'tim@pappaslandscaping.com', password: process.env.ADMIN_PASSWORD || 'changeme', name: 'Tim Pappas', role: 'owner' },
    ];
    for (const u of adminUsers) {
      const hash = hashPassword(u.password);
      await pool.query(
        // Only set the seed password on first creation. ON CONFLICT DO NOTHING
        // preserves any manual password changes done via /api/auth/change-password.
        `INSERT INTO admin_users (email, password_hash, name, role) VALUES ($1, $2, $3, $4) ON CONFLICT (email) DO NOTHING`,
        [u.email, hash, u.name, u.role]
      );
    }
    console.log('✅ Admin users ready');
  } catch(e) { console.error('Admin users error:', e.message); }

  // Ensure crews table exists and seed defaults
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS crews (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      members TEXT,
      crew_type VARCHAR(50),
      notes TEXT,
      is_active BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    try { await pool.query('ALTER TABLE crews ADD COLUMN IF NOT EXISTS crew_type VARCHAR(50)'); } catch(e2) {}
    try { await pool.query('ALTER TABLE crews ADD COLUMN IF NOT EXISTS members TEXT'); } catch(e2) {}
    try { await pool.query('ALTER TABLE crews ADD COLUMN IF NOT EXISTS notes TEXT'); } catch(e2) {}
    try { await pool.query('ALTER TABLE crews ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true'); } catch(e2) {}
    try { await pool.query('ALTER TABLE crews ADD COLUMN IF NOT EXISTS color VARCHAR(20)'); } catch(e2) {}
    try { await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS crews_name_unique ON crews (name)'); } catch(e2) {}
    // Migrate old placeholder crews to real crew names
    try {
      await pool.query(`UPDATE scheduled_jobs SET crew_assigned = 'Mowing' WHERE crew_assigned = 'Crew A'`);
      await pool.query(`UPDATE scheduled_jobs SET crew_assigned = 'Jobs' WHERE crew_assigned = 'Crew B'`);
      await pool.query(`UPDATE scheduled_jobs SET crew_assigned = 'Rob Mowing Crew' WHERE crew_assigned = 'Crew C'`);
      await pool.query(`DELETE FROM crews WHERE name IN ('Crew A', 'Crew B', 'Crew C')`);
    } catch(e2) { console.error('Crew migration error:', e2.message); }
    // Upsert real crews
    const realCrews = [
      ['Chris Snow', 'Christopher Redarowicz', 'snow', '#2e403d'],
      ['Jobs', 'Christopher Redarowicz, Robert Ellison, Timothy Pappas, Wilkyn Camacho', 'landscaping', '#4a6741'],
      ['Mowing', 'Timothy Pappas', 'mowing', '#059669'],
      ['Rob Mowing Crew', 'Robert Ellison, Wilkyn Camacho', 'mowing', '#6b8f3c'],
      ['Rob Snow', 'Robert Ellison', 'snow', '#3d5a4c'],
      ['Tim Mowing Crew', 'Christopher Redarowicz, Timothy Pappas', 'mowing', '#7aab55'],
      ['Tim Snow', 'Timothy Pappas', 'snow', '#335c44'],
      ['Wilkyn Snow', 'Wilkyn Camacho', 'snow', '#4e7a5e']
    ];
    for (const [name, members, crewType, color] of realCrews) {
      await pool.query(
        `INSERT INTO crews (name, members, crew_type, color) VALUES ($1, $2, $3, $4)
         ON CONFLICT (name) DO UPDATE SET members = $2, crew_type = $3, color = $4`,
        [name, members, crewType, color]
      );
    }
    console.log('✅ Crews table ready');
  } catch(e) { console.error('Crews table error:', e.message); }

  // Ensure employees table exists
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS employees (
        id SERIAL PRIMARY KEY,
        title VARCHAR(100),
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        birth_date DATE,
        hire_date DATE,
        salary_amount DECIMAL(10,2),
        pay_type VARCHAR(20) DEFAULT 'hourly',
        chemical_license VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        address TEXT,
        notes TEXT,
        login_email VARCHAR(255) UNIQUE,
        password_hash VARCHAR(255),
        permissions JSONB DEFAULT '[]',
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('✅ Employees table ready');
  } catch(e) { console.error('Employees table error:', e.message); }

  // Add extra property detail columns if missing
  try {
    const propCols = ['stories VARCHAR(20)', 'fence_type VARCHAR(100)', 'assigned_crew VARCHAR(255)', 'default_services TEXT', "photos JSONB DEFAULT '[]'", 'access_instructions TEXT', 'equipment_notes TEXT'];
    for (const col of propCols) {
      await pool.query(`ALTER TABLE properties ADD COLUMN IF NOT EXISTS ${col}`).catch(() => {});
    }
    console.log('✅ Property detail columns ready');
  } catch(e) { console.error('Property columns error:', e.message); }

  // Add completion columns to scheduled_jobs if missing
  try {
    await pool.query(`ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS completion_notes TEXT`).catch(() => {});
    await pool.query(`ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS completion_photos JSONB`).catch(() => {});
    console.log('✅ Job completion columns ready');
  } catch(e) { console.error('Job completion columns error:', e.message); }
}

// ─── Table ensure functions (also called lazily from routes) ───

const INVOICES_TABLE = `CREATE TABLE IF NOT EXISTS invoices (
  id SERIAL PRIMARY KEY,
  invoice_number VARCHAR(50) UNIQUE,
  customer_id INTEGER,
  customer_name VARCHAR(255),
  customer_email VARCHAR(255),
  customer_address TEXT,
  sent_quote_id INTEGER,
  job_id INTEGER,
  status VARCHAR(20) DEFAULT 'draft',
  subtotal DECIMAL(10,2) DEFAULT 0,
  tax_rate DECIMAL(5,3) DEFAULT 0,
  tax_amount DECIMAL(10,2) DEFAULT 0,
  total DECIMAL(10,2) DEFAULT 0,
  amount_paid DECIMAL(10,2) DEFAULT 0,
  due_date DATE,
  paid_at TIMESTAMP,
  sent_at TIMESTAMP,
  qb_invoice_id VARCHAR(100),
  notes TEXT,
  line_items JSONB DEFAULT '[]',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

async function ensureInvoicesTable(pool) {
  await pool.query(INVOICES_TABLE);
}

async function ensureQuoteEventsTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS quote_events (
    id SERIAL PRIMARY KEY,
    sent_quote_id INTEGER NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    description TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);
}

async function ensureCustomerReviewsTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS customer_reviews (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    created_at TIMESTAMP DEFAULT NOW()
  )`);
}

async function ensureQBTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS qb_tokens (
    id SERIAL PRIMARY KEY,
    realm_id VARCHAR(100) NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    token_type VARCHAR(50) DEFAULT 'bearer',
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS qb_sync_log (
    id SERIAL PRIMARY KEY,
    sync_type VARCHAR(50),
    customers_synced INTEGER DEFAULT 0,
    invoices_synced INTEGER DEFAULT 0,
    payments_synced INTEGER DEFAULT 0,
    expenses_synced INTEGER DEFAULT 0,
    errors TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
  )`);
  // QB-driven columns on existing tables
  try { await pool.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS qb_id VARCHAR(100)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ADD COLUMN IF NOT EXISTS qb_id VARCHAR(100)`); } catch(e) {}

  // Payment columns/constraints needed for QB payment sync
  try { await pool.query(`ALTER TABLE payments ALTER COLUMN invoice_id DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE payments ADD COLUMN IF NOT EXISTS qb_payment_id VARCHAR(100)`); } catch(e) {}

  // Expense columns/constraints needed for QB expense sync
  // (some columns may have been created with stricter NOT NULL or narrower types)
  const expCols = [
    ['description', 'TEXT'], ['amount', 'NUMERIC(10,2) DEFAULT 0'], ['category', 'VARCHAR(100)'],
    ['vendor', 'VARCHAR(255)'], ['expense_date', 'DATE'], ['receipt_url', 'TEXT'],
    ['notes', 'TEXT'], ['qb_id', 'VARCHAR(100)']
  ];
  for (const [col, type] of expCols) {
    try { await pool.query(`ALTER TABLE expenses ADD COLUMN IF NOT EXISTS ${col} ${type}`); } catch(e) {}
  }
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN vendor DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN description DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN category DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN expense_date DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN vendor TYPE VARCHAR(500)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN category TYPE VARCHAR(500)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN qb_id TYPE VARCHAR(255)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN description TYPE TEXT`); } catch(e) {}
}

async function ensureCopilotSyncTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS copilot_sync_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS copilot_sync_jobs (
    id SERIAL PRIMARY KEY,
    sync_date DATE NOT NULL,
    event_id TEXT NOT NULL,
    customer_name TEXT,
    customer_id TEXT,
    crew_name TEXT,
    employees TEXT,
    address TEXT,
    status TEXT,
    visit_total TEXT,
    job_title TEXT,
    stop_order INT,
    raw_data JSONB,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sync_date, event_id)
  )`);
}

// ─── Second startup init (table ensures) ─────────────────

async function runStartupTableInit(pool) {
  try {
    await ensureInvoicesTable(pool);
    await ensureQuoteEventsTable(pool);
    await ensureQBTables(pool);
    await ensureCopilotSyncTables(pool);
    await pool.query(`CREATE TABLE IF NOT EXISTS quote_views (
      id SERIAL PRIMARY KEY, sent_quote_id INTEGER NOT NULL,
      viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      ip_address VARCHAR(45), user_agent TEXT
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS sent_quotes (
      id SERIAL PRIMARY KEY, quote_number VARCHAR(50), customer_name VARCHAR(255),
      customer_email VARCHAR(255), status VARCHAR(50) DEFAULT 'draft',
      sign_token VARCHAR(255), services JSONB, total DECIMAL(10,2),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    // Service items, message templates, automations
    await pool.query(`CREATE TABLE IF NOT EXISTS service_items (
      id SERIAL PRIMARY KEY, name VARCHAR(255), default_rate DECIMAL(10,2),
      duration_minutes INTEGER, category VARCHAR(100), active BOOLEAN DEFAULT true,
      description TEXT, tax_rate DECIMAL(5,2) DEFAULT 0, taxable BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW()
    )`);
    await pool.query(`ALTER TABLE service_items ADD COLUMN IF NOT EXISTS description TEXT`).catch(() => {});
    await pool.query(`ALTER TABLE service_items ADD COLUMN IF NOT EXISTS tax_rate DECIMAL(5,2) DEFAULT 0`).catch(() => {});
    await pool.query(`ALTER TABLE service_items ADD COLUMN IF NOT EXISTS taxable BOOLEAN DEFAULT true`).catch(() => {});
    await pool.query(`CREATE TABLE IF NOT EXISTS message_templates (
      id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, type VARCHAR(20) DEFAULT 'email',
      subject VARCHAR(500), html_content TEXT, text_content TEXT,
      category VARCHAR(100), tags TEXT[] DEFAULT '{}', active BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS automations (
      id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, description TEXT,
      trigger_type VARCHAR(100) NOT NULL, trigger_config JSONB DEFAULT '{}',
      conditions JSONB DEFAULT '[]', actions JSONB DEFAULT '[]',
      active BOOLEAN DEFAULT true, review_before_exec BOOLEAN DEFAULT false,
      run_count INTEGER DEFAULT 0, last_run_at TIMESTAMP,
      created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS automation_history (
      id SERIAL PRIMARY KEY, automation_id INTEGER REFERENCES automations(id) ON DELETE CASCADE,
      triggered_by VARCHAR(255), trigger_data JSONB DEFAULT '{}',
      actions_taken JSONB DEFAULT '[]', status VARCHAR(50) DEFAULT 'completed',
      created_at TIMESTAMP DEFAULT NOW()
    )`);
    console.log('✅ Startup table initialization complete');
  } catch (err) {
    console.error('⚠️ Startup table initialization error:', err.message);
  }
}

module.exports = {
  ADMIN_USERS_TABLE,
  hashPassword,
  ensurePaymentsTables,
  ensureInvoicesTable,
  ensureQuoteEventsTable,
  ensureCustomerReviewsTable,
  ensureQBTables,
  ensureCopilotSyncTables,
  runStartupMigrations,
  runStartupTableInit,
};

// ─── Standalone runner ───────────────────────────────────
// Usage: DATABASE_URL=... node lib/startup-schema.js
//        DRY_RUN=1 DATABASE_URL=... node lib/startup-schema.js

if (require.main === module) {
  require('dotenv').config({ path: require('path').resolve(__dirname, '..', '.env') });

  const { Pool } = require('pg');
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  });

  const isDryRun = process.env.DRY_RUN === '1';

  (async () => {
    console.log('─── YardDesk Startup Schema ───');
    console.log(`Database: ${process.env.DATABASE_URL ? '(connected)' : '⚠️  DATABASE_URL not set'}`);
    console.log(`Mode: ${isDryRun ? 'DRY RUN (no changes)' : 'LIVE'}`);
    console.log('');

    if (!process.env.DATABASE_URL) {
      console.error('❌ DATABASE_URL is required. Set it in .env or pass it directly.');
      process.exit(1);
    }

    if (isDryRun) {
      console.log('✅ Dry run complete — connection verified, no migrations executed.');
      await pool.end();
      process.exit(0);
    }

    try {
      console.log('Running startup migrations...');
      await runStartupMigrations(pool);
      console.log('');
      console.log('Running table initialization...');
      await runStartupTableInit(pool);
      console.log('');
      console.log('═══ All migrations complete ═══');
    } catch (err) {
      console.error('❌ Migration failed:', err.message);
      process.exit(1);
    } finally {
      await pool.end();
    }
  })();
}
