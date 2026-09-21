const { syncCommunicationToHomeWorks, ensureSyncTable } = require('./call-notes');

async function ensureVoicemailSyncState(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS homeworks_voicemail_sync_state (
      singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton = true),
      started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `);
  await pool.query(`INSERT INTO homeworks_voicemail_sync_state (singleton) VALUES (true) ON CONFLICT (singleton) DO NOTHING`);
  const result = await pool.query(`SELECT started_at FROM homeworks_voicemail_sync_state WHERE singleton = true`);
  return result.rows[0]?.started_at || new Date();
}

function voicemailSourceId(voicemail) {
  return String(voicemail?.twilio_sid || `voicemail-${voicemail?.id || ''}`);
}

async function mirrorVoicemail(pool, voicemail) {
  const sourceId = voicemailSourceId(voicemail);
  const result = await pool.query(`
    INSERT INTO calls (twilio_sid, direction, from_number, to_number, status, duration, recording_url, transcription, read, created_at, updated_at)
    VALUES ($1, 'inbound', $2, $3, 'voicemail', $4, $5, $6, $7, COALESCE($8::timestamp, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP)
    ON CONFLICT (twilio_sid) DO UPDATE SET
      duration = EXCLUDED.duration,
      recording_url = COALESCE(EXCLUDED.recording_url, calls.recording_url),
      transcription = COALESCE(EXCLUDED.transcription, calls.transcription),
      read = EXCLUDED.read,
      updated_at = CURRENT_TIMESTAMP
    RETURNING *
  `, [sourceId, voicemail.from_number || '', voicemail.to_number || '', Number(voicemail.duration || 0), voicemail.recording_url || null, voicemail.transcription || null, Boolean(voicemail.read), voicemail.created_at || null]);
  return result.rows[0];
}

async function getVoicemailSyncStatus(pool, sourceId) {
  await ensureSyncTable(pool);
  const result = await pool.query(`
    SELECT status, homeworks_customer_id, error, synced_at
    FROM homeworks_communication_sync
    WHERE source_type = 'voicemail' AND source_id = $1
    LIMIT 1
  `, [sourceId]);
  const row = result.rows[0];
  return row ? { ...row, customerId: row.homeworks_customer_id || null } : null;
}

async function syncVoicemailToHomeWorks({ pool, getCopilotToken, voicemail, automatic = false, recordingUrl, fetchImpl = fetch }) {
  const startedAt = await ensureVoicemailSyncState(pool);
  const sourceId = voicemailSourceId(voicemail);
  await mirrorVoicemail(pool, voicemail);
  const occurredAt = new Date(voicemail.created_at || Date.now());
  if (automatic && occurredAt < new Date(startedAt)) {
    const previous = await getVoicemailSyncStatus(pool, sourceId);
    return previous || { success: false, status: 'legacy_untracked' };
  }
  const result = await syncCommunicationToHomeWorks({
    pool,
    getCopilotToken,
    sourceType: 'voicemail',
    sourceId,
    phone: voicemail.from_number,
    communication: {
      kind: 'voicemail',
      direction: 'inbound',
      occurredAt: voicemail.created_at,
      from: voicemail.from_number,
      to: voicemail.to_number,
      status: 'voicemail',
      duration: voicemail.duration,
      transcription: voicemail.transcription,
      recordingUrl,
    },
    fetchImpl,
  });
  if (result.status === 'already_processed') {
    const previous = await getVoicemailSyncStatus(pool, sourceId);
    if (previous) return { success: previous.status === 'synced', ...previous };
  }
  if (result.customerId) await pool.query(`UPDATE calls SET customer_id = $2 WHERE twilio_sid = $1`, [sourceId, result.customerId]);
  return result;
}

module.exports = {
  ensureVoicemailSyncState,
  getVoicemailSyncStatus,
  mirrorVoicemail,
  syncVoicemailToHomeWorks,
  voicemailSourceId,
};
