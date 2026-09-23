const TIME_ZONE = 'America/New_York';
const INBOX_URL = 'https://app.pappaslandscaping.com/live-chat.html';

function easternParts(date) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: TIME_ZONE, year: 'numeric', month: '2-digit', day: '2-digit',
    weekday: 'short', hour: '2-digit', hourCycle: 'h23',
  }).formatToParts(date);
  return Object.fromEntries(parts.map(({ type, value }) => [type, value]));
}

function addDays(date, count) {
  const value = new Date(`${date}T12:00:00Z`);
  value.setUTCDate(value.getUTCDate() + count);
  return value.toISOString().slice(0, 10);
}

function reportWeek(now = new Date()) {
  const parts = easternParts(now);
  const today = `${parts.year}-${parts.month}-${parts.day}`;
  const weekday = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(parts.weekday);
  const monday = addDays(today, -(weekday + 6) % 7);
  return { start: addDays(monday, -7), end: monday, due: weekday !== 0 && (weekday !== 1 || Number(parts.hour) >= 9) };
}

function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
}

function isBusinessHours(date) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: TIME_ZONE, weekday: 'short', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map(({ type, value }) => [type, value]));
  const minute = Number(values.hour) * 60 + Number(values.minute);
  if (values.weekday === 'Sat') return minute >= 9 * 60 && minute < 16 * 60;
  if (values.weekday === 'Sun') return false;
  return minute >= 9 * 60 && minute < 18 * 60;
}

async function loadReport(pool, week) {
  const result = await pool.query(`
    SELECT c.id, c.visitor_name, c.mode, c.status, c.created_at, c.joined_at,
      COUNT(m.id) FILTER (WHERE m.sender = 'visitor')::int AS visitor_messages,
      COUNT(m.id) FILTER (WHERE m.sender = 'assistant')::int AS assistant_messages,
      COUNT(m.id) FILTER (WHERE m.sender = 'staff')::int AS staff_messages,
      MIN(m.created_at) FILTER (WHERE m.sender = 'staff') AS first_staff_at,
      MAX(m.created_at) FILTER (WHERE m.sender = 'visitor') AS last_visitor_at,
      MAX(m.created_at) FILTER (WHERE m.sender = 'staff') AS last_staff_at
    FROM site_chats c LEFT JOIN site_chat_messages m ON m.chat_id = c.id
    WHERE c.created_at >= ($1::date::timestamp AT TIME ZONE 'America/New_York')
      AND c.created_at < ($2::date::timestamp AT TIME ZONE 'America/New_York')
    GROUP BY c.id ORDER BY c.created_at DESC`, [week.start, week.end]);
  const chats = result.rows;
  const needsReply = chats.filter((chat) => chat.mode === 'human' && chat.status === 'open' &&
    (!chat.last_staff_at || new Date(chat.last_visitor_at) > new Date(chat.last_staff_at)));
  return {
    week, total: chats.length,
    aiOnly: chats.filter((chat) => Number(chat.assistant_messages) > 0 && chat.mode === 'assistant').length,
    staffReplied: chats.filter((chat) => Number(chat.staff_messages) > 0).length,
    handoffs: chats.filter((chat) => Number(chat.assistant_messages) > 0 && chat.mode === 'human').length,
    afterHours: chats.filter((chat) => !isBusinessHours(new Date(chat.created_at))).length,
    needsReply,
  };
}

function renderReport(report) {
  const { week, total, aiOnly, staffReplied, handoffs, afterHours, needsReply } = report;
  const item = (label, count) => `<tr><td style="padding:9px 0;border-bottom:1px solid #e5ebe2">${label}</td><td style="padding:9px 0;text-align:right;border-bottom:1px solid #e5ebe2"><strong>${count}</strong></td></tr>`;
  const followUps = needsReply.slice(0, 20).map((chat) =>
    `<li style="margin:8px 0"><a href="${INBOX_URL}?chat=${encodeURIComponent(chat.id)}">${escapeHtml(chat.visitor_name || 'Website visitor')}</a></li>`).join('');
  return `<div style="font-family:Arial,sans-serif;color:#263b37;max-width:600px;margin:auto;line-height:1.5">
    <h1 style="font-size:24px">Your weekly website chat report</h1>
    <p>${escapeHtml(week.start)} through ${escapeHtml(addDays(week.end, -1))} (Eastern time)</p>
    <table style="width:100%;border-collapse:collapse">${item('Conversations started', total)}${item('AI-only conversations', aiOnly)}${item('Conversations with a staff reply', staffReplied)}${item('AI chats handed to the team', handoffs)}${item('Started after office hours', afterHours)}${item('Open team chats awaiting a reply', needsReply.length)}</table>
    ${needsReply.length ? `<h2 style="font-size:18px">Needs a reply</h2><ul>${followUps}</ul>${needsReply.length > 20 ? `<p>Plus ${needsReply.length - 20} more in YardDesk.</p>` : ''}` : '<p>No open team chats from this week are waiting for a reply.</p>'}
    <p><a href="${INBOX_URL}">Open the private chat inbox in YardDesk</a></p>
    <p style="color:#66766c;font-size:13px">Counts cover chats started during this week. “Awaiting a reply” reflects their status when this report was prepared. AI-only conversations may still have open questions; review them in YardDesk.</p>
  </div>`;
}

function createWeeklyChatReporter({ pool, fetchImpl = fetch, apiKey, from, to, now = () => new Date() }) {
  async function run() {
    const week = reportWeek(now());
    if (!week.due) return { skipped: 'not due', week: week.start };
    if (!apiKey || !to) return { skipped: 'email not configured', week: week.start };
    const claim = await pool.query(`
      INSERT INTO site_chat_weekly_reports (week_start, status, attempted_at)
      VALUES ($1, 'sending', NOW())
      ON CONFLICT (week_start) DO UPDATE SET status = 'sending', attempted_at = NOW()
      WHERE (site_chat_weekly_reports.status = 'failed'
        AND site_chat_weekly_reports.attempted_at < NOW() - INTERVAL '1 hour')
        OR (site_chat_weekly_reports.status = 'sending'
        AND site_chat_weekly_reports.attempted_at < NOW() - INTERVAL '2 hours')
      RETURNING week_start`, [week.start]);
    if (!claim.rows.length) return { skipped: 'already sent or in progress', week: week.start };
    try {
      const report = await loadReport(pool, week);
      const response = await fetchImpl('https://api.resend.com/emails', {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from, to: [to], subject: `Website chat report: ${week.start} to ${addDays(week.end, -1)}`, html: renderReport(report) }),
      });
      if (!response.ok) throw new Error(`Email provider returned ${response.status}`);
      await pool.query(`UPDATE site_chat_weekly_reports SET status = 'sent', sent_at = NOW(), error = NULL WHERE week_start = $1`, [week.start]);
      return { sent: true, week: week.start, total: report.total, needsReply: report.needsReply.length };
    } catch (error) {
      await pool.query(`UPDATE site_chat_weekly_reports SET status = 'failed', error = $2 WHERE week_start = $1`, [week.start, String(error.message || error).slice(0, 500)]);
      throw error;
    }
  }
  function startScheduler() {
    const check = () => run().catch((error) => console.error('Weekly website chat report failed:', error));
    const timer = setInterval(check, 60 * 60 * 1000);
    timer.unref?.();
    check();
    return timer;
  }
  return { run, startScheduler };
}

module.exports = { reportWeek, loadReport, renderReport, createWeeklyChatReporter };
