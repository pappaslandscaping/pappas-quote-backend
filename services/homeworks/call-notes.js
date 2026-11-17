const cheerio = require('cheerio');
const { fetchMobileCustomerSnapshot, normalizePhone } = require('./mobile-app');
const { stripHtml } = require('./client');


function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function noteTextToHtml(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line) => escapeHtml(line))
    .join('<br>');
}

function formatEasternTimestamp(value = new Date()) {
  const date = value instanceof Date ? value : new Date(value);
  return date.toLocaleString('en-US', {
    timeZone: 'America/New_York',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function formatDuration(seconds) {
  const total = Math.max(0, Number.parseInt(seconds, 10) || 0);
  const minutes = Math.floor(total / 60);
  const remainder = total % 60;
  if (!minutes) return `${remainder}s`;
  return `${minutes}m ${remainder}s`;
}

function parseHomeWorksCallNotesHtml(html, limit = 5) {
  const $ = cheerio.load(String(html || ''));
  const notes = [];
  $('#custom_timeline_communicat_tab li').each((index, element) => {
    if (notes.length >= limit) return false;
    const item = $(element);
    const body = stripHtml(item.find('.text-dark').first().html());
    if (!body) return undefined;
    const date = item.find('a[href*="/customers/details/"]').first().text().replace(/\s+/g, ' ').trim();
    const author = item.find('a[href*="/resources/employees/edit/"]').first().text().replace(/\s+/g, ' ').trim();
    notes.push({
      id: `${date || 'note'}-${index}-${body.slice(0, 32)}`,
      date,
      author,
      body,
    });
    return undefined;
  });
  return notes;
}

async function fetchHomeWorksCallNotes({pool,customerId,limit=5}){
 await pool.query('CREATE TABLE IF NOT EXISTS yarddesk_customer_notes(id BIGSERIAL PRIMARY KEY,homeworks_customer_id BIGINT NOT NULL,note TEXT NOT NULL,created_at TIMESTAMPTZ DEFAULT NOW())');
 const result=await pool.query("SELECT id,note AS body,created_at AS date,'YardDesk' AS author FROM yarddesk_customer_notes WHERE homeworks_customer_id=$1 ORDER BY created_at DESC LIMIT $2",[Number(customerId),Math.min(100,Number(limit)||5)]);
 return result.rows.map(row=>({...row,source:'yarddesk',destination:'office_notes'}));
}

function buildCommunicationCallNote({
  kind,
  direction,
  occurredAt,
  from,
  to,
  body,
  status,
  duration,
  transcription,
  recordingUrl,
  sourceId,
  employee,
}) {
  const normalizedKind = kind === 'voicemail' ? 'Voicemail' : kind === 'call' ? 'Call' : 'Text';
  const normalizedDirection = String(direction || '').toLowerCase().includes('inbound') ? 'received' : 'sent';
  const lines = [
    `Twilio Connect · ${normalizedKind} ${normalizedDirection}`,
    formatEasternTimestamp(occurredAt),
  ];
  if (employee) lines.push(`Handled by: ${employee}`);
  if (from) lines.push(`From: ${from}`);
  if (to) lines.push(`To: ${to}`);
  if (status) lines.push(`Status: ${status}`);
  if ((kind === 'call' || kind === 'voicemail') && duration != null) lines.push(`Duration: ${formatDuration(duration)}`);
  if (body) lines.push('', 'Message:', String(body).trim());
  if (transcription) lines.push('', 'Transcription:', String(transcription).trim());
  if (recordingUrl) lines.push('', `Recording: ${recordingUrl}`);
  if (sourceId) lines.push('', `Twilio ID: ${sourceId}`);
  return lines.join('\n');
}

async function saveHomeWorksCallNote({pool,customerId,note,notifyUser=false,createFollowUp=false,followUpDate}){
 const records=require('./records');const numericId=records.id(customerId),text=String(note||'').trim();
 if(!text)throw new Error('The office note cannot be empty.');
 if(notifyUser||createFollowUp||followUpDate)throw new Error('Save this office note without a notification or follow-up. Manage those actions in Homeworks.');
 await records.uniqueCustomer({pool},{customerId:numericId});
 await pool.query('CREATE TABLE IF NOT EXISTS yarddesk_customer_notes(id BIGSERIAL PRIMARY KEY,homeworks_customer_id BIGINT NOT NULL,note TEXT NOT NULL,created_at TIMESTAMPTZ DEFAULT NOW())');
 const result=await pool.query('INSERT INTO yarddesk_customer_notes(homeworks_customer_id,note) VALUES($1,$2) RETURNING id',[numericId,text]);
 return {success:true,customerId:numericId,id:result.rows[0].id,status:'saved_local',source:'yarddesk',destination:'office_notes',homeworksSynced:false,message:'Saved to YardDesk office notes. Homeworks call-note access is not available through its official API.',followUpRequested:createFollowUp,notificationRequested:notifyUser};
}

async function ensureSyncTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS homeworks_communication_sync (
      id BIGSERIAL PRIMARY KEY,
      source_type VARCHAR(20) NOT NULL,
      source_id VARCHAR(120) NOT NULL,
      homeworks_customer_id BIGINT,
      customer_phone VARCHAR(30),
      status VARCHAR(30) NOT NULL DEFAULT 'pending',
      error TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      synced_at TIMESTAMP,
      UNIQUE(source_type, source_id)
    )
  `);
}

async function ensureMatchTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS homeworks_phone_matches (
      normalized_phone VARCHAR(20) PRIMARY KEY,
      homeworks_customer_id BIGINT NOT NULL,
      customer_name TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

async function syncCommunicationToHomeWorks({
  pool,
  getCopilotToken,
  sourceType,
  sourceId,
  phone,
  communication,
  fetchImpl = fetch,
}) {
  if (!sourceId) throw new Error('sourceId is required for HomeWorks communication sync');
  const normalizedPhone = normalizePhone(phone);
  if (!normalizedPhone) return { success: false, status: 'skipped_no_phone' };
  await Promise.all([ensureSyncTable(pool), ensureMatchTable(pool)]);

  const claimed = await pool.query(`
    INSERT INTO homeworks_communication_sync (source_type, source_id, customer_phone, status)
    VALUES ($1, $2, $3, 'pending')
    ON CONFLICT (source_type, source_id) DO UPDATE SET
      status = 'pending', error = NULL
    WHERE homeworks_communication_sync.status = 'failed'
    RETURNING id
  `, [sourceType, String(sourceId), normalizedPhone]);
  if (!claimed.rows[0]) return { success: true, status: 'already_processed' };

  try {
    const matched = await pool.query(`SELECT homeworks_customer_id FROM homeworks_phone_matches WHERE normalized_phone = $1 LIMIT 1`, [normalizedPhone]);
    const matchedCustomerId = matched.rows[0]?.homeworks_customer_id;
    const snapshot = await fetchMobileCustomerSnapshot({ pool, ...(matchedCustomerId ? { customerId: matchedCustomerId } : { phone: normalizedPhone }) });
    if (!snapshot?.customer?.id) {
      await pool.query(`UPDATE homeworks_communication_sync SET status = 'skipped_no_customer', error = $2 WHERE id = $1`, [claimed.rows[0].id, 'No matching HomeWorks customer']);
      return { success: false, status: 'skipped_no_customer' };
    }
    const note = buildCommunicationCallNote({ ...communication, sourceId });
    await saveHomeWorksCallNote({
      pool,
      getCopilotToken,
      customerId: snapshot.customer.id,
      note,
      fetchImpl,
    });
    await pool.query(`
      UPDATE homeworks_communication_sync
      SET status = 'saved_local', homeworks_customer_id = $2, synced_at = CURRENT_TIMESTAMP, error = NULL
      WHERE id = $1
    `, [claimed.rows[0].id, snapshot.customer.id]);
    return { success: true, status: 'saved_local', homeworksSynced: false, customerId: snapshot.customer.id };
  } catch (error) {
    await pool.query(`UPDATE homeworks_communication_sync SET status = 'failed', error = $2 WHERE id = $1`, [claimed.rows[0].id, error.message]).catch(() => {});
    throw error;
  }
}

module.exports = {
  buildCommunicationCallNote,
  ensureMatchTable,
  ensureSyncTable,
  fetchHomeWorksCallNotes,
  formatEasternTimestamp,
  parseHomeWorksCallNotesHtml,
  saveHomeWorksCallNote,
  syncCommunicationToHomeWorks,
};
