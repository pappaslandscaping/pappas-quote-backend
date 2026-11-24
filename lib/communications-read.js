'use strict';

// Communications are records in their own right. Never make displaying them
// conditional on a live Homeworks customer search or on enrichment metadata.
function normalizePhone(phone) {
  const digits = String(phone || '').replace(/\D/g, '').replace(/^1(?=\d{10}$)/, '');
  return /^\d{10}$/.test(digits) ? digits : '';
}

async function optionalRows(pool, sql, values = []) {
  let timer;
  try {
    const result = await Promise.race([
      pool.query({ text: sql, values, query_timeout: 4000 }),
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('local read timed out')), 4500); }),
    ]);
    return { rows: result.rows || [], available: true };
  } catch (error) {
    console.warn('Optional communications enrichment unavailable:', error.message);
    return { rows: [], available: false };
  } finally {
    clearTimeout(timer);
  }
}

async function cachedCustomerMatches(pool, phones) {
  const normalized = [...new Set(phones.map(normalizePhone).filter(Boolean))];
  if (!normalized.length) return new Map();
  const { rows } = await optionalRows(pool, `
    SELECT normalized_phone, MIN(homeworks_customer_id) AS id, MIN(customer_name) AS name
    FROM homeworks_phone_matches
    WHERE normalized_phone = ANY($1::text[])
    GROUP BY normalized_phone
    HAVING COUNT(DISTINCT homeworks_customer_id) = 1
  `, [normalized]);
  return new Map(rows.map(row => [row.normalized_phone, { id: row.id, name: row.name }]));
}

async function loadVoicemailMetadata(pool, sourceIds, phones) {
  const [matches, sync, intake, start] = await Promise.all([
    cachedCustomerMatches(pool, phones),
    optionalRows(pool, "SELECT source_id, status, homeworks_customer_id, error FROM homeworks_communication_sync WHERE source_type = 'voicemail' AND source_id = ANY($1::text[])", [sourceIds]),
    optionalRows(pool, 'SELECT source_id, status, error FROM homeworks_voicemail_intake WHERE source_id = ANY($1::text[])', [sourceIds]),
    optionalRows(pool, 'SELECT started_at FROM homeworks_voicemail_intake_state WHERE singleton = true'),
  ]);
  return {
    matches,
    sync: new Map(sync.rows.map(row => [row.source_id, { ...row, customerId: row.homeworks_customer_id || null }])),
    intake: new Map(intake.rows.map(row => [row.source_id, row])),
    syncAvailable: sync.available,
    intakeAvailable: intake.available && start.available,
    intakeStartedAt: start.rows[0]?.started_at || null,
  };
}

module.exports = { normalizePhone, cachedCustomerMatches, loadVoicemailMetadata };
