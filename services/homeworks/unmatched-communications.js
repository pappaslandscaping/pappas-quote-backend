const { queryHomeWorksGraphql } = require('./client');
const { normalizePhone } = require('./mobile-app');
const {
  buildCommunicationCallNote,
  ensureMatchTable,
  ensureSyncTable,
  saveHomeWorksCallNote,
} = require('./call-notes');

async function listUnmatchedCommunications(pool, limit = 100) {
  await Promise.all([ensureSyncTable(pool), ensureMatchTable(pool)]);
  const result = await pool.query(`
    SELECT sync.source_type, sync.source_id, sync.customer_phone AS phone,
      sync.status AS sync_status, sync.error, sync.created_at,
      COALESCE(m.direction, c.direction) AS direction,
      COALESCE(m.status, c.status) AS communication_status,
      m.body, c.duration, c.transcription,
      COALESCE(m.created_at, c.created_at, sync.created_at) AS occurred_at
    FROM homeworks_communication_sync sync
    LEFT JOIN messages m ON sync.source_type = 'sms' AND sync.source_id = m.twilio_sid
    LEFT JOIN calls c ON sync.source_type IN ('call', 'voicemail') AND sync.source_id = c.twilio_sid
    WHERE sync.status IN ('skipped_no_customer', 'failed')
    ORDER BY COALESCE(m.created_at, c.created_at, sync.created_at) DESC
    LIMIT $1
  `, [Math.max(1, Math.min(Number(limit) || 100, 200))]);
  return result.rows;
}

async function loadSourceCommunication(pool, sourceType, sourceId) {
  if (sourceType === 'sms') {
    const result = await pool.query(`
      SELECT twilio_sid, direction, from_number, to_number, body, status, created_at
      FROM messages WHERE twilio_sid = $1 LIMIT 1
    `, [sourceId]);
    const row = result.rows[0];
    return row && { kind: 'text', sourceId: row.twilio_sid, occurredAt: row.created_at, from: row.from_number, to: row.to_number, ...row };
  }
  if (sourceType === 'call' || sourceType === 'voicemail') {
    const result = await pool.query(`
      SELECT twilio_sid, direction, from_number, to_number, status, duration, recording_url, transcription, created_at
      FROM calls WHERE twilio_sid = $1 LIMIT 1
    `, [sourceId]);
    const row = result.rows[0];
    return row && { kind: sourceType === 'voicemail' ? 'voicemail' : 'call', sourceId: row.twilio_sid, occurredAt: row.created_at, from: row.from_number, to: row.to_number, recordingUrl: row.recording_url, ...row };
  }
  return null;
}

async function matchCommunication({ pool, getCopilotToken, sourceType, sourceId, customerId, customerName, fetchImpl = fetch }) {
  const numericCustomerId = Number(customerId);
  if (!['sms', 'call', 'voicemail'].includes(sourceType)) throw new Error('Communication type must be sms, call, or voicemail');
  if (!sourceId) throw new Error('Communication ID is required');
  if (!Number.isInteger(numericCustomerId) || numericCustomerId <= 0) throw new Error('A valid HomeWorks customer is required');
  await Promise.all([ensureSyncTable(pool), ensureMatchTable(pool)]);
  const communication = await loadSourceCommunication(pool, sourceType, sourceId);
  if (!communication) throw new Error('Communication could not be found');
  const remotePhone = normalizePhone(communication.direction === 'inbound' ? communication.from_number : communication.to_number);
  if (!remotePhone) throw new Error('Communication has no usable phone number');

  await saveHomeWorksCallNote({
    pool,
    getCopilotToken,
    customerId: numericCustomerId,
    note: buildCommunicationCallNote(communication),
    fetchImpl,
  });
  await pool.query(`
    INSERT INTO homeworks_phone_matches (normalized_phone, homeworks_customer_id, customer_name, updated_at)
    VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
    ON CONFLICT (normalized_phone) DO UPDATE SET
      homeworks_customer_id = EXCLUDED.homeworks_customer_id,
      customer_name = EXCLUDED.customer_name,
      updated_at = CURRENT_TIMESTAMP
  `, [remotePhone, numericCustomerId, String(customerName || '').trim() || null]);
  await pool.query(`
    UPDATE homeworks_communication_sync
    SET status = 'synced', homeworks_customer_id = $3, synced_at = CURRENT_TIMESTAMP, error = NULL
    WHERE source_type = $1 AND source_id = $2
  `, [sourceType, sourceId, numericCustomerId]);
  return { success: true, customerId: numericCustomerId, phone: remotePhone, status: 'synced' };
}

async function createLeadForPhone({ pool, fullName, phone, fetchImpl = fetch }) {
  const name = String(fullName || '').trim();
  const normalized = normalizePhone(phone);
  if (!name) throw new Error('A lead name is required');
  if (!normalized) throw new Error('A valid phone number is required');
  const lookup = await queryHomeWorksGraphql({ pool, fetchImpl, operationName: 'TwilioConnectLeadType', query: `query TwilioConnectLeadType { customerTypes { id name } }` });
  const leadType = (lookup.customerTypes || []).find((type) => String(type.name).trim().toLowerCase() === 'lead');
  if (!leadType) throw new Error('The HomeWorks Lead customer type was not found');
  const result = await queryHomeWorksGraphql({
    pool,
    fetchImpl,
    operationName: 'TwilioConnectCreateLead',
    query: `mutation TwilioConnectCreateLead($input: CustomerInput!) { createCustomer(input: $input) { id fullName phone cell } }`,
    variables: { input: {
      fullName: name,
      phone: normalized,
      cell: normalized,
      customerTypeId: leadType.id,
      description: 'Created from the Twilio Connect unmatched communications inbox.',
      isReceivePhone: true,
      isReceiveText: false,
    } },
  });
  return result.createCustomer;
}

module.exports = {
  createLeadForPhone,
  ensureMatchTable,
  listUnmatchedCommunications,
  matchCommunication,
};
