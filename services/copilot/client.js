// ═══════════════════════════════════════════════════════════
// CopilotCRM Client
// Centralizes auth/session/token handling and HTML parsing
// for CopilotCRM. Routes and other callers use this module
// instead of constructing API calls inline.
// ═══════════════════════════════════════════════════════════

const cheerio = require('cheerio');

const COPILOT_API_BASE = 'https://api.copilotcrm.com';
const COPILOT_WEB_BASE = 'https://secure.copilotcrm.com';

/**
 * Read stored cookies/token from copilot_sync_settings.
 * Returns { cookieHeader, expiresAt, daysUntilExpiry } or null if not configured.
 */
async function getCopilotToken(pool) {
  const result = await pool.query(
    "SELECT key, value FROM copilot_sync_settings WHERE key IN ('copilot_token', 'copilot_cookies')"
  );
  const settings = {};
  for (const row of result.rows) settings[row.key] = row.value;

  const cookies = settings.copilot_cookies || null;
  const token = settings.copilot_token || null;

  if (!cookies && !token) return null;

  const cookieHeader = cookies || `copilotApiAccessToken=${token}`;

  let expiresAt = null, daysUntilExpiry = null;
  const jwtMatch = (cookies || '').match(/copilotApiAccessToken=([^;]+)/) || (token ? [null, token] : null);
  if (jwtMatch && jwtMatch[1]) {
    try {
      const payload = JSON.parse(Buffer.from(jwtMatch[1].split('.')[1], 'base64').toString());
      expiresAt = new Date(payload.exp * 1000);
      daysUntilExpiry = (expiresAt - new Date()) / (1000 * 60 * 60 * 24);
    } catch { /* not a valid JWT — that's ok */ }
  }

  return { cookieHeader, expiresAt, daysUntilExpiry };
}

/**
 * Parse a CopilotCRM route schedule HTML page into job rows.
 * Used by the daily route sync to import Copilot's published schedule.
 */
function parseCopilotRouteHtml(html, employeesArray) {
  const $ = cheerio.load(html);
  const jobs = [];
  $('tr[data-row-event-id]').each((i, row) => {
    const $row = $(row);
    const eventId = $row.attr('data-row-event-id');

    // Customer name + ID from link
    const customerLink = $row.find('td.column-3 a');
    const customerName = customerLink.text().trim();
    const customerHref = customerLink.attr('href') || '';
    const customerIdMatch = customerHref.match(/\/(\d+)/);
    const customerId = customerIdMatch ? customerIdMatch[1] : null;

    // Crew name — first text node of span.row-crew-label (before the <small>)
    const crewLabel = $row.find('span.row-crew-label');
    const crewName = crewLabel.contents().filter(function() { return this.nodeType === 3; }).first().text().trim();

    // Employees from small tag
    const employeesText = $row.find('span.row-crew-label small').text().trim();

    // Address
    const address = $row.find('td.column-13').text().trim();

    // Status
    const status = $row.find('span.status-label').text().trim();

    // Visit total — column 17 (price)
    const visitTotal = $row.find('td.column-17').text().trim();

    // Job title — column 8 (service type / job description)
    const jobTitle = $row.find('td.column-8').text().trim();

    // Stop order — first column
    const stopOrder = parseInt($row.find('td.column-1').text().trim()) || null;

    if (eventId && customerName) {
      jobs.push({
        event_id: eventId,
        customer_id: customerId,
        customer_name: customerName,
        crew_name: crewName,
        employees: employeesText,
        address,
        status,
        visit_total: visitTotal,
        job_title: jobTitle,
        stop_order: stopOrder,
      });
    }
  });
  return jobs;
}

/**
 * Login with username/password and return Set-Cookie headers.
 * Used by the contract-sign flow when long-lived cookies are not configured.
 */
async function loginWithCredentials(username, password) {
  const res = await fetch(`${COPILOT_API_BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Origin': COPILOT_WEB_BASE },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`CopilotCRM login failed (${res.status}): ${body.slice(0, 200)}`);
  }
  return {
    setCookies: res.headers.getSetCookie?.() || [],
    body: await res.json().catch(() => ({})),
  };
}

module.exports = {
  COPILOT_API_BASE,
  COPILOT_WEB_BASE,
  getCopilotToken,
  parseCopilotRouteHtml,
  loginWithCredentials,
};
