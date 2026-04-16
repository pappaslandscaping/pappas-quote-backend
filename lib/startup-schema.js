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

function createStartupSchemaTools({
  pool,
  config,
  hashPassword,
  logAuditEvent = () => {},
  adminUsersTable,
}) {
  async function ensurePaymentsTables() {
    await pool.query(PAYMENTS_TABLE);
    await pool.query(PORTAL_TOKENS_TABLE);
  }

  async function runStatements(statements) {
    for (const statement of statements) {
      try {
        await pool.query(statement);
      } catch (error) {
        // These migrations are intentionally idempotent.
      }
    }
  }

  async function runCoreStartupMigrations() {
    try {
      await runStatements([
        'ALTER TABLE sent_quotes ALTER COLUMN contract_signature_data TYPE TEXT',
        'ALTER TABLE sent_quotes ALTER COLUMN contract_signer_ip TYPE VARCHAR(255)',
        'ALTER TABLE sent_quotes ALTER COLUMN contract_signer_name TYPE VARCHAR(255)',
        'ALTER TABLE sent_quotes ALTER COLUMN contract_signature_type TYPE VARCHAR(50)',
      ]);

      try {
        const fixedSignedQuotes = await pool.query(
          `UPDATE sent_quotes
             SET status = 'contracted'
           WHERE status = 'signed' AND contract_signed_at IS NOT NULL
           RETURNING quote_number`
        );
        if (fixedSignedQuotes.rowCount > 0) {
          console.log(
            '✅ Fixed signed quotes with contract_signed_at:',
            fixedSignedQuotes.rows.map((row) => row.quote_number).join(', ')
          );
        }
      } catch (error) {
        console.error('Migration fix error:', error.message);
      }

      try {
        const fixedLegacySignedQuotes = await pool.query(
          `UPDATE sent_quotes
             SET status = 'contracted', contract_signed_at = updated_at
           WHERE status = 'signed'
             AND contract_signed_at IS NULL
             AND created_at < '2026-03-04'
           RETURNING quote_number`
        );
        if (fixedLegacySignedQuotes.rowCount > 0) {
          console.log(
            '✅ Fixed legacy signed quotes without contract timestamps:',
            fixedLegacySignedQuotes.rows.map((row) => row.quote_number).join(', ')
          );
        }
      } catch (error) {
        console.error('Migration fix error:', error.message);
      }

      try {
        await ensurePaymentsTables();
        console.log('✅ Payments & portal tokens tables ready');
      } catch (error) {
        console.error('Payments table error:', error.message);
      }

      await runStatements([
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_token VARCHAR(64)',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_token_created_at TIMESTAMP',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS square_invoice_id VARCHAR(100)',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS viewed_at TIMESTAMP',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS reminder_sent_at TIMESTAMP',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS reminder_count INTEGER DEFAULT 0',
      ]);
      console.log('✅ Invoice payment columns ready');

      for (const statement of [
        BUSINESS_SETTINGS_TABLE,
        LATE_FEES_TABLE,
        RECURRING_INVOICE_LOG_TABLE,
        RECURRING_JOB_LOG_TABLE,
        CUSTOMER_SAVED_CARDS_TABLE,
        SERVICE_REQUESTS_TABLE,
        CUSTOMER_COMMUNICATION_PREFS_TABLE,
        EMAIL_TEMPLATES_TABLE,
        CAMPAIGN_SENDS_TABLE,
      ]) {
        try {
          await pool.query(statement);
        } catch (error) {
          console.error('Table create error:', error.message);
        }
      }
      console.log('✅ Core business settings tables ready');

      await runStatements([
        "ALTER TABLE email_templates ADD COLUMN IF NOT EXISTS channel VARCHAR(10) DEFAULT 'email'",
        "ALTER TABLE customers ADD COLUMN IF NOT EXISTS customer_type VARCHAR(20) DEFAULT 'customer'",
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS late_fee_total DECIMAL(10,2) DEFAULT 0',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS is_auto_generated BOOLEAN DEFAULT false',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS auto_gen_source VARCHAR(50)',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS billing_month VARCHAR(7)',
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
        'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS geocode_quality VARCHAR(20)',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payment_schedule JSONB',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS installment_count INTEGER DEFAULT 1',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS processing_fee DECIMAL(10,2) DEFAULT 0',
        'ALTER TABLE invoices ADD COLUMN IF NOT EXISTS processing_fee_passed BOOLEAN DEFAULT false',
      ]);
      console.log('✅ Invoice and job migration columns ready');

      for (const statement of [
        `CREATE TABLE IF NOT EXISTS quotes (
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
        )`,
        `CREATE TABLE IF NOT EXISTS cancellations (
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
        )`,
        `CREATE TABLE IF NOT EXISTS campaigns (
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
        )`,
        `CREATE TABLE IF NOT EXISTS campaign_submissions (
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
        )`,
        `CREATE TABLE IF NOT EXISTS internal_notes (
          id SERIAL PRIMARY KEY,
          entity_type VARCHAR(30) NOT NULL,
          entity_id INTEGER NOT NULL,
          author_name VARCHAR(255),
          author_id INTEGER,
          content TEXT NOT NULL,
          pinned BOOLEAN DEFAULT false,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS referrals (
          id SERIAL PRIMARY KEY,
          referrer_id INTEGER NOT NULL,
          referred_name VARCHAR(255) NOT NULL,
          referred_customer_id INTEGER,
          status VARCHAR(20) DEFAULT 'pending',
          credited_at TIMESTAMP,
          notes TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS job_expenses (
          id SERIAL PRIMARY KEY,
          job_id INTEGER NOT NULL,
          description VARCHAR(500),
          category VARCHAR(100),
          amount DECIMAL(10,2) NOT NULL,
          receipt_url TEXT,
          created_by VARCHAR(255),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS time_entries (
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
        )`,
        `CREATE TABLE IF NOT EXISTS dispatch_templates (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          zip_codes TEXT,
          crew_id INTEGER,
          service_type VARCHAR(100),
          default_duration INTEGER DEFAULT 30,
          notes TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS service_programs (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          description TEXT,
          status VARCHAR(20) DEFAULT 'active',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS program_steps (
          id SERIAL PRIMARY KEY,
          program_id INTEGER NOT NULL REFERENCES service_programs(id) ON DELETE CASCADE,
          step_order INTEGER NOT NULL,
          service_type VARCHAR(100) NOT NULL,
          description TEXT,
          estimated_duration INTEGER DEFAULT 30,
          offset_days INTEGER DEFAULT 0,
          price DECIMAL(10,2),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS customer_programs (
          id SERIAL PRIMARY KEY,
          customer_id INTEGER NOT NULL,
          program_id INTEGER NOT NULL,
          property_id INTEGER,
          start_date DATE NOT NULL,
          current_step INTEGER DEFAULT 1,
          status VARCHAR(20) DEFAULT 'active',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS season_kickoff_responses (
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
        )`,
        `CREATE TABLE IF NOT EXISTS crews (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          members TEXT,
          crew_type VARCHAR(50),
          notes TEXT,
          is_active BOOLEAN DEFAULT true,
          color VARCHAR(20),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS employees (
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
        )`,
      ]) {
        try {
          await pool.query(statement);
        } catch (error) {
          console.error('Table create error:', error.message);
        }
      }

      await runStatements([
        'CREATE INDEX IF NOT EXISTS idx_notes_entity ON internal_notes (entity_type, entity_id)',
        'CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals (referrer_id)',
        'CREATE INDEX IF NOT EXISTS idx_referrals_status ON referrals (status)',
        'ALTER TABLE customers ADD COLUMN IF NOT EXISTS square_customer_id VARCHAR(255)',
        'ALTER TABLE customers ADD COLUMN IF NOT EXISTS monthly_plan_amount DECIMAL(10,2) DEFAULT 0',
        'ALTER TABLE customers ADD COLUMN IF NOT EXISTS tax_exempt BOOLEAN DEFAULT false',
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS county_tax DECIMAL(5,3) DEFAULT NULL',
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS city_tax DECIMAL(5,3) DEFAULT NULL',
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS state_tax DECIMAL(5,3) DEFAULT NULL',
        'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS template_id INTEGER',
        'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_count INTEGER DEFAULT 0',
        'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS open_count INTEGER DEFAULT 0',
        'ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS click_count INTEGER DEFAULT 0',
      ]);

      try {
        await pool.query(EMAIL_LOG_TABLE);
      } catch (error) {
        console.error('Email log table error:', error.message);
      }

      try {
        await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS crews_name_unique ON crews (name)');
        await runStatements([
          'ALTER TABLE crews ADD COLUMN IF NOT EXISTS crew_type VARCHAR(50)',
          'ALTER TABLE crews ADD COLUMN IF NOT EXISTS members TEXT',
          'ALTER TABLE crews ADD COLUMN IF NOT EXISTS notes TEXT',
          'ALTER TABLE crews ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true',
          'ALTER TABLE crews ADD COLUMN IF NOT EXISTS color VARCHAR(20)',
          "UPDATE scheduled_jobs SET crew_assigned = 'Mowing' WHERE crew_assigned = 'Crew A'",
          "UPDATE scheduled_jobs SET crew_assigned = 'Jobs' WHERE crew_assigned = 'Crew B'",
          "UPDATE scheduled_jobs SET crew_assigned = 'Rob Mowing Crew' WHERE crew_assigned = 'Crew C'",
          "DELETE FROM crews WHERE name IN ('Crew A', 'Crew B', 'Crew C')",
        ]);

        const realCrews = [
          ['Chris Snow', 'Christopher Redarowicz', 'snow', '#2e403d'],
          ['Jobs', 'Christopher Redarowicz, Robert Ellison, Timothy Pappas, Wilkyn Camacho', 'landscaping', '#4a6741'],
          ['Mowing', 'Timothy Pappas', 'mowing', '#059669'],
          ['Rob Mowing Crew', 'Robert Ellison, Wilkyn Camacho', 'mowing', '#6b8f3c'],
          ['Rob Snow', 'Robert Ellison', 'snow', '#3d5a4c'],
          ['Tim Mowing Crew', 'Christopher Redarowicz, Timothy Pappas', 'mowing', '#7aab55'],
          ['Tim Snow', 'Timothy Pappas', 'snow', '#335c44'],
          ['Wilkyn Snow', 'Wilkyn Camacho', 'snow', '#4e7a5e'],
        ];

        for (const [name, members, crewType, color] of realCrews) {
          await pool.query(
            `INSERT INTO crews (name, members, crew_type, color)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE
             SET members = $2, crew_type = $3, color = $4`,
            [name, members, crewType, color]
          );
        }
        console.log('✅ Crews table ready');
      } catch (error) {
        console.error('Crews table error:', error.message);
      }

      await runStatements([
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS stories VARCHAR(20)',
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS fence_type VARCHAR(100)',
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS assigned_crew VARCHAR(255)',
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS default_services TEXT',
        "ALTER TABLE properties ADD COLUMN IF NOT EXISTS photos JSONB DEFAULT '[]'",
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS access_instructions TEXT',
        'ALTER TABLE properties ADD COLUMN IF NOT EXISTS equipment_notes TEXT',
        'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS completion_notes TEXT',
        'ALTER TABLE scheduled_jobs ADD COLUMN IF NOT EXISTS completion_photos JSONB',
      ]);
      console.log('✅ Property and job detail columns ready');

      try {
        await pool.query(`
          INSERT INTO business_settings (key, value) VALUES
            ('late_fee_rules', '{"grace_period_days": 30, "initial_fee_percent": 10, "recurring_fee_percent": 5, "recurring_interval_days": 30, "max_fees": 3, "enabled": true}'),
            ('recurring_invoice_config', '{"auto_send": true, "due_date_offset_days": 0, "include_payment_link": true}'),
            ('company_info', '{"name": "Pappas & Co. Landscaping", "phone": "(440) 886-7318", "email": "hello@pappaslandscaping.com", "website": "pappaslandscaping.com", "address": "PO Box 770057, Lakewood, Ohio 44107"}')
          ON CONFLICT (key) DO NOTHING
        `);
        console.log('✅ Default business settings seeded');
      } catch (error) {
        console.error('Settings seed error:', error.message);
      }

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
      } catch (error) {
        console.error('Invoice settings seed error:', error.message);
      }

      try {
        await pool.query(adminUsersTable);
        const bootstrapPassword = config.auth.bootstrapAdminPassword;
        if (!bootstrapPassword) {
          console.log('ℹ️ Admin bootstrap skipped (BOOTSTRAP_ADMIN_PASSWORD not set)');
        } else {
          const adminUsers = [
            { email: 'hello@pappaslandscaping.com', name: 'Theresa Pappas', role: 'owner' },
            { email: 'tim@pappaslandscaping.com', name: 'Tim Pappas', role: 'owner' },
          ];
          for (const user of adminUsers) {
            await pool.query(
              `INSERT INTO admin_users (email, password_hash, name, role)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (email) DO NOTHING`,
              [user.email, hashPassword(bootstrapPassword), user.name, user.role]
            );
          }
          logAuditEvent('auth.bootstrap_admin.completed', {
            seededUsers: adminUsers.map((user) => user.email),
          });
        }
        console.log('✅ Admin users ready');
      } catch (error) {
        console.error('Admin users error:', error.message);
      }
    } catch (error) {
      console.error('⚠️ Core startup migration error:', error.message);
      throw error;
    }
  }

  return {
    ensurePaymentsTables,
    runCoreStartupMigrations,
  };
}

module.exports = {
  createStartupSchemaTools,
};
