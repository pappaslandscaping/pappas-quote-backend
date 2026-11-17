'use strict';
const pending = new WeakMap();
async function ensureHomeworksSchema(pool) {
  if (!pending.has(pool))
    pending.set(
      pool,
      (async () => {
        await pool.query('ALTER TABLE customers ADD COLUMN IF NOT EXISTS homeworks_customer_id BIGINT');
        await pool.query(
          'CREATE UNIQUE INDEX IF NOT EXISTS customers_homeworks_id_uidx ON customers(homeworks_customer_id) WHERE homeworks_customer_id IS NOT NULL',
        );
        await pool.query('ALTER TABLE properties ADD COLUMN IF NOT EXISTS homeworks_property_id BIGINT');
        await pool.query(
          'CREATE UNIQUE INDEX IF NOT EXISTS properties_homeworks_id_uidx ON properties(homeworks_property_id) WHERE homeworks_property_id IS NOT NULL',
        );
        await pool.query(
          'CREATE TABLE IF NOT EXISTS yarddesk_customer_notes(id BIGSERIAL PRIMARY KEY,homeworks_customer_id BIGINT NOT NULL,note TEXT NOT NULL,created_at TIMESTAMPTZ DEFAULT NOW())',
        );
        await pool.query(
          "CREATE TABLE IF NOT EXISTS homeworks_document_attachments(quote_id BIGINT NOT NULL,property_id BIGINT NOT NULL,document_hash TEXT NOT NULL,upload_key TEXT,media_id BIGINT,status TEXT NOT NULL DEFAULT 'pending',error TEXT,updated_at TIMESTAMPTZ DEFAULT NOW(),PRIMARY KEY(quote_id,property_id,document_hash))",
        );
        await pool.query(
          'CREATE TABLE IF NOT EXISTS homeworks_action_receipts(request_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,status TEXT NOT NULL,remote_id BIGINT,created_at TIMESTAMPTZ DEFAULT NOW())',
        );
      })().catch((error) => {
        pending.delete(pool);
        throw error;
      }),
    );
  return pending.get(pool);
}
module.exports = { ensureHomeworksSchema };
