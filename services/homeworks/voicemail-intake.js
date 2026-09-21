const { mirrorVoicemail, voicemailSourceId } = require('./voicemail-sync');

async function ensureVoicemailIntakeTables(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS homeworks_voicemail_intake_state (
      singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton = true),
      started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `);
  await pool.query(`INSERT INTO homeworks_voicemail_intake_state (singleton) VALUES (true) ON CONFLICT (singleton) DO NOTHING`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS homeworks_voicemail_intake (
      source_id VARCHAR(120) PRIMARY KEY,
      status VARCHAR(30) NOT NULL DEFAULT 'capturing',
      error TEXT,
      attempts INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `);
  const result = await pool.query(`SELECT started_at FROM homeworks_voicemail_intake_state WHERE singleton = true`);
  if (!result.rows[0]?.started_at) throw new Error('Voicemail intake start time is unavailable');
  return result.rows[0].started_at;
}

async function getVoicemailIntakeStatus(pool, sourceId) {
  const result = await pool.query(`SELECT status, error FROM homeworks_voicemail_intake WHERE source_id = $1`, [sourceId]);
  return result.rows[0] || null;
}

async function getVoicemailIntakeStartedAt(pool) {
  const result = await pool.query(`SELECT started_at FROM homeworks_voicemail_intake_state WHERE singleton = true`);
  return result.rows[0]?.started_at || null;
}

async function captureNewVoicemails({ pool, voicemails }) {
  const startedAt = await ensureVoicemailIntakeTables(pool);
  const results = { captured: 0, retried: 0, skipped: 0, failed: 0 };
  for (const voicemail of voicemails) {
    const occurredAt = new Date(voicemail?.created_at);
    if ((!voicemail?.twilio_sid && !voicemail?.id) || !Number.isFinite(occurredAt.getTime()) || occurredAt < new Date(startedAt)) {
      results.skipped += 1;
      continue;
    }
    const sourceId = voicemailSourceId(voicemail);
    const claim = await pool.query(`
      INSERT INTO homeworks_voicemail_intake (source_id, status, attempts)
      VALUES ($1, 'capturing', 1)
      ON CONFLICT (source_id) DO UPDATE SET
        status = 'capturing', error = NULL,
        attempts = homeworks_voicemail_intake.attempts + 1,
        updated_at = CURRENT_TIMESTAMP
      WHERE homeworks_voicemail_intake.status = 'intake_failed'
        OR (homeworks_voicemail_intake.status = 'capturing' AND homeworks_voicemail_intake.updated_at < CURRENT_TIMESTAMP - INTERVAL '5 minutes')
      RETURNING attempts
    `, [sourceId]);
    if (!claim.rows[0]) {
      results.skipped += 1;
      continue;
    }
    try {
      await mirrorVoicemail(pool, voicemail);
      await pool.query(`UPDATE homeworks_voicemail_intake SET status = 'pending_homeworks', error = NULL, updated_at = CURRENT_TIMESTAMP WHERE source_id = $1`, [sourceId]);
      if (Number(claim.rows[0].attempts) > 1) results.retried += 1;
      else results.captured += 1;
    } catch (error) {
      await pool.query(`UPDATE homeworks_voicemail_intake SET status = 'intake_failed', error = $2, updated_at = CURRENT_TIMESTAMP WHERE source_id = $1`, [sourceId, error.message]).catch(() => {});
      results.failed += 1;
    }
  }
  return results;
}

module.exports = { captureNewVoicemails, ensureVoicemailIntakeTables, getVoicemailIntakeStartedAt, getVoicemailIntakeStatus };
