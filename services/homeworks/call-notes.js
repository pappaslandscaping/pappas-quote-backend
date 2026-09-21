const cheerio = require('cheerio');
const { fetchMobileCustomerSnapshot, normalizePhone } = require('./mobile-app');
const { stripHtml } = require('./client');

const COPILOT_WEB_BASE = 'https://secure.copilotcrm.com';

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

async function fetchHomeWorksCallNotes({ pool, getCopilotToken, customerId, fetchImpl = fetch, limit = 5 }) {
  const numericCustomerId = Number(customerId);
  if (!Number.isInteger(numericCustomerId) || numericCustomerId <= 0) return [];
  const tokenInfo = await getCopilotToken(pool);
  if (!tokenInfo?.cookieHeader) throw new Error('HomeWorks web session is not configured');
  const response = await fetchImpl(`${COPILOT_WEB_BASE}/customers/details/${numericCustomerId}`, {
    headers: {
      Cookie: tokenInfo.cookieHeader,
      Accept: 'text/html,application/xhtml+xml',
    },
  });
  if (!response.ok) throw new Error(`HomeWorks customer page returned ${response.status}`);
  return parseHomeWorksCallNotesHtml(await response.text(), limit);
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
  sourceId,
  employee,
}) {
  const normalizedKind = kind === 'call' ? 'Call' : 'Text';
  const normalizedDirection = String(direction || '').toLowerCase().includes('inbound') ? 'received' : 'sent';
  const lines = [
    `Twilio Connect · ${normalizedKind} ${normalizedDirection}`,
    formatEasternTimestamp(occurredAt),
  ];
  if (employee) lines.push(`Handled by: ${employee}`);
  if (from) lines.push(`From: ${from}`);
  if (to) lines.push(`To: ${to}`);
  if (status) lines.push(`Status: ${status}`);
  if (kind === 'call' && duration != null) lines.push(`Duration: ${formatDuration(duration)}`);
  if (body) lines.push('', 'Message:', String(body).trim());
  if (transcription) lines.push('', 'Transcription:', String(transcription).trim());
  if (sourceId) lines.push('', `Twilio ID: ${sourceId}`);
  return lines.join('\n');
}

async function saveHomeWorksCallNote({
  pool,
  getCopilotToken,
  customerId,
  note,
  notifyUser = false,
  createFollowUp = false,
  followUpDate,
  fetchImpl = fetch,
}) {
  const numericCustomerId = Number(customerId);
  if (!Number.isInteger(numericCustomerId) || numericCustomerId <= 0) {
    throw new Error('A valid HomeWorks customer ID is required');
  }
  const cleanNote = String(note || '').trim();
  if (!cleanNote) throw new Error('Call note cannot be empty');

  const tokenInfo = await getCopilotToken(pool);
  if (!tokenInfo?.cookieHeader) throw new Error('HomeWorks web session is not configured');

  const body = new URLSearchParams({
    customer_id: String(numericCustomerId),
    call_notes: noteTextToHtml(cleanNote),
  });
  if (notifyUser) body.set('notify_user', '1');
  if (createFollowUp) {
    body.set('add_followup', '1');
    if (followUpDate) body.set('follow_up_date', String(followUpDate));
  }

  const response = await fetchImpl(`${COPILOT_WEB_BASE}/customers/details/communicationSaveCall`, {
    method: 'POST',
    headers: {
      Cookie: tokenInfo.cookieHeader,
      Origin: COPILOT_WEB_BASE,
      Referer: `${COPILOT_WEB_BASE}/customers/details/${numericCustomerId}`,
      'X-Requested-With': 'XMLHttpRequest',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      Accept: 'application/json, text/javascript, */*; q=0.01',
    },
    body: body.toString(),
  });
  const raw = await response.text();
  let payload = null;
  try { payload = JSON.parse(raw); } catch { /* handled below */ }
  if (!response.ok || !payload?.status) {
    const detail = payload?.msg || raw.slice(0, 240) || `HTTP ${response.status}`;
    throw new Error(`HomeWorks Call Note save failed: ${detail}`);
  }
  return { success: true, customerId: numericCustomerId, response: payload };
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
  await ensureSyncTable(pool);

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
    const snapshot = await fetchMobileCustomerSnapshot({ pool, phone: normalizedPhone });
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
      SET status = 'synced', homeworks_customer_id = $2, synced_at = CURRENT_TIMESTAMP, error = NULL
      WHERE id = $1
    `, [claimed.rows[0].id, snapshot.customer.id]);
    return { success: true, status: 'synced', customerId: snapshot.customer.id };
  } catch (error) {
    await pool.query(`UPDATE homeworks_communication_sync SET status = 'failed', error = $2 WHERE id = $1`, [claimed.rows[0].id, error.message]).catch(() => {});
    throw error;
  }
}

module.exports = {
  buildCommunicationCallNote,
  fetchHomeWorksCallNotes,
  formatEasternTimestamp,
  parseHomeWorksCallNotesHtml,
  saveHomeWorksCallNote,
  syncCommunicationToHomeWorks,
};
