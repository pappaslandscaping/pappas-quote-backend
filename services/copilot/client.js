// ═══════════════════════════════════════════════════════════
// CopilotCRM Client
// Centralizes auth/session/token handling and HTML parsing
// for CopilotCRM. Routes and other callers use this module
// instead of constructing API calls inline.
// ═══════════════════════════════════════════════════════════

const cheerio = require('cheerio');

const COPILOT_API_BASE = 'https://api.copilotcrm.com';
const COPILOT_WEB_BASE = 'https://secure.copilotcrm.com';
const COPILOT_ROUTE_LIST_PATH = '/scheduler/all/list';
const COPILOT_SCHEDULE_GRID_DAY_PATH = '/scheduler/grid/day/';

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
    const jobId = $row.attr('data-row-job-id') || $row.attr('data-job-id') || null;

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
        job_id: jobId,
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

function parseCopilotScheduleCrewCell($cell) {
  const crewLabel = $cell.find('span.row-crew-label');
  if (crewLabel.length > 0) {
    const crewName = crewLabel.contents().filter(function () {
      return this.nodeType === 3;
    }).first().text().trim();
    const employeesText = crewLabel.find('small').text().trim();
    return {
      crew_name: crewName || null,
      employees: employeesText || null,
    };
  }

  const text = $cell.text().replace(/\s+/g, ' ').trim();
  return {
    crew_name: text || null,
    employees: null,
  };
}

function parseCopilotScheduleGridDayHtml(html) {
  const $ = cheerio.load(html);
  const noEventsFound = /No Events Found/i.test(html);
  const gridTable = $('table.copilot-table.table--with-hide-options').first();
  if (!gridTable.length) {
    if (noEventsFound) return [];
    throw new Error('Copilot Schedule grid table not found');
  }

  const jobs = [];
  gridTable.find('tbody tr[data-row-event-id]').each((_, row) => {
    const $row = $(row);
    const eventId = String($row.attr('data-row-event-id') || '').trim();
    if (!eventId) return;

    const cells = $row.find('td');
    if (cells.length < 16) return;

    const titleCell = cells.eq(2);
    const crewCell = cells.eq(3);
    const customerCell = cells.eq(4);
    const propertyCell = cells.eq(5);
    const addressCell = cells.eq(6);
    const typeCell = cells.eq(7);
    const invoiceableCell = cells.eq(8);
    const frequencyCell = cells.eq(9);
    const lastServicedCell = cells.eq(10);
    const statusCell = cells.eq(11);
    const visitNotesCell = cells.eq(12);
    const trackedTimeCell = cells.eq(13);
    const bhCell = cells.eq(14);
    const visitTotalCell = cells.eq(15);

    const customerLink = customerCell.find('a').first();
    const propertyLink = propertyCell.find('a').first();
    const titleLink = titleCell.find('a.getEventDetails').first();

    const customerHref = customerLink.attr('href') || '';
    const customerIdMatch = customerHref.match(/\/customers\/details\/(\d+)/);
    const propertyHref = propertyLink.attr('href') || '';
    const propertyIdMatch = propertyHref.match(/\/assets\/details\/edit\/(\d+)/);
    const crewInfo = parseCopilotScheduleCrewCell(crewCell);
    const statusLabel = statusCell.find('span.status-label').first().text().trim();
    const invoiceLink = statusCell.find('a').first();

    jobs.push({
      event_id: eventId,
      job_id: titleLink.attr('data-id') || eventId,
      customer_id: customerIdMatch ? customerIdMatch[1] : null,
      customer_name: customerLink.text().trim() || customerCell.text().replace(/\s+/g, ' ').trim(),
      property_id: propertyIdMatch ? propertyIdMatch[1] : null,
      property_name: propertyLink.text().trim() || propertyCell.text().replace(/\s+/g, ' ').trim(),
      crew_name: crewInfo.crew_name,
      employees: crewInfo.employees,
      address: addressCell.text().replace(/\s+/g, ' ').trim() || null,
      status: statusLabel || statusCell.text().replace(/\s+/g, ' ').trim() || null,
      invoice_number: invoiceLink.text().trim() || null,
      invoiceable: invoiceableCell.text().replace(/\s+/g, ' ').trim() || null,
      visit_total: visitTotalCell.text().replace(/\s+/g, ' ').trim() || null,
      job_title: titleLink.text().trim() || titleCell.text().replace(/\s+/g, ' ').trim() || null,
      stop_order: null,
      service_date_text: cells.eq(1).text().replace(/\s+/g, ' ').trim() || null,
      event_type: typeCell.text().replace(/\s+/g, ' ').trim() || null,
      frequency: frequencyCell.text().replace(/\s+/g, ' ').trim() || null,
      last_serviced: lastServicedCell.text().replace(/\s+/g, ' ').trim() || null,
      visit_notes: visitNotesCell.text().replace(/\s+/g, ' ').trim() || null,
      tracked_time: trackedTimeCell.text().replace(/\s+/g, ' ').trim() || null,
      budgeted_hours: bhCell.text().replace(/\s+/g, ' ').trim() || null,
      raw_data: {
        source_surface: 'schedule_grid',
      },
    });
  });

  return jobs;
}

function formatCopilotRouteDate(dateStr) {
  const date = new Date(`${dateStr}T00:00:00`);
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function buildCopilotRouteFormData(dateStr) {
  const formattedDate = formatCopilotRouteDate(dateStr);
  const formData = new URLSearchParams();
  formData.append('accessFrom', 'route');
  formData.append('bs4', '1');
  formData.append('sDate', formattedDate);
  formData.append('eDate', formattedDate);
  formData.append('optimizationFlag', '1');
  formData.append('count', '-1');
  for (const type of ['1', '2', '3', '4', '5', '0']) {
    formData.append('evtypes_route[]', type);
  }
  formData.append('isdate', '0');
  formData.append('sdate', formattedDate);
  formData.append('edate', formattedDate);
  formData.append('erec', 'all');
  formData.append('estatus', 'any');
  formData.append('esort', '');
  formData.append('einvstatus', 'any');
  return formData;
}

async function fetchCopilotRouteJobsForDate({
  cookieHeader,
  syncDate,
  fetchImpl = fetch,
} = {}) {
  if (!cookieHeader) throw new Error('CopilotCRM cookies are required');
  if (!syncDate) throw new Error('syncDate is required');

  const response = await fetchImpl(`${COPILOT_WEB_BASE}${COPILOT_ROUTE_LIST_PATH}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Cookie': cookieHeader,
      'Origin': COPILOT_WEB_BASE,
      'Referer': `${COPILOT_WEB_BASE}/`,
      'X-Requested-With': 'XMLHttpRequest',
    },
    body: buildCopilotRouteFormData(syncDate).toString(),
  });

  if (!response.ok) {
    const errBody = await response.text().catch(() => '');
    throw new Error(`CopilotCRM returned ${response.status}: ${errBody.substring(0, 200)}`);
  }

  const data = await response.json();
  if (data.status !== undefined && data.status !== 1 && data.status !== '1' && data.status !== true) {
    throw new Error(`CopilotCRM returned non-success status: ${data.status}`);
  }

  return {
    sync_date: syncDate,
    source_surface: 'route_day',
    raw: data,
    jobs: parseCopilotRouteHtml(data.html || '', data.employees || []),
  };
}

async function fetchCopilotScheduleGridJobsForDate({
  cookieHeader,
  syncDate,
  fetchImpl = fetch,
} = {}) {
  if (!cookieHeader) throw new Error('CopilotCRM cookies are required');
  if (!syncDate) throw new Error('syncDate is required');

  const url = new URL(`${COPILOT_WEB_BASE}${COPILOT_SCHEDULE_GRID_DAY_PATH}`);
  url.searchParams.set('d', syncDate);

  const response = await fetchImpl(url.toString(), {
    method: 'GET',
    headers: {
      'Cookie': cookieHeader,
      'Origin': COPILOT_WEB_BASE,
      'Referer': `${COPILOT_WEB_BASE}/scheduler/grid/day/`,
      'User-Agent': 'Mozilla/5.0',
      'Accept': 'text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8',
    },
  });

  const html = await response.text();
  if (!response.ok) {
    throw new Error(`Copilot Schedule grid returned ${response.status}: ${html.substring(0, 200)}`);
  }
  if (!html || response.url.includes('/login')) {
    throw new Error('Copilot Schedule grid did not return an authenticated HTML page');
  }

  const jobs = parseCopilotScheduleGridDayHtml(html);
  const noEventsFound = /No Events Found/i.test(html);
  if (jobs.length === 0 && !noEventsFound) {
    throw new Error(`Copilot Schedule grid parse mismatch for ${syncDate}`);
  }

  return {
    sync_date: syncDate,
    source_surface: 'schedule_grid',
    raw: {
      html_length: html.length,
      no_events_found: noEventsFound,
      url: response.url,
    },
    jobs,
  };
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
  COPILOT_ROUTE_LIST_PATH,
  COPILOT_SCHEDULE_GRID_DAY_PATH,
  formatCopilotRouteDate,
  buildCopilotRouteFormData,
  fetchCopilotRouteJobsForDate,
  fetchCopilotScheduleGridJobsForDate,
  getCopilotToken,
  parseCopilotScheduleGridDayHtml,
  parseCopilotRouteHtml,
  loginWithCredentials,
};
