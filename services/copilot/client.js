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

function normalizeCellText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function parseCopilotScheduleCrewCell($cell) {
  const text = normalizeCellText($cell.text());
  if (!text) return { crew_name: '', employees_text: '' };

  const smallText = normalizeCellText($cell.find('small').text());
  if (!smallText) {
    return { crew_name: text, employees_text: '' };
  }

  const crewName = normalizeCellText(text.replace(smallText, '').trim());
  return {
    crew_name: crewName || text,
    employees_text: smallText,
  };
}

function parseCopilotScheduleGridDayHtml(html) {
  const sourceHtml = String(html || '');
  const $ = cheerio.load(sourceHtml);

  const diagnostics = {
    html_source_length: sourceHtml.length,
    contains_expected_grid_table_markers:
      sourceHtml.includes('copilot-table') &&
      sourceHtml.includes('data-row-event-id'),
    marker_hits: {
      grid_table_count: $('table.copilot-table.table--with-hide-options').length,
      event_row_count: $('tr[data-row-event-id]').length,
      tbody_row_count: $('tbody tr').length,
    },
    parsed_row_count_before_filtering: 0,
    parsed_row_count_after_filtering: 0,
    no_events_found: /No Events Found/i.test(sourceHtml),
  };

  const table = $('table.copilot-table.table--with-hide-options').first();
  if (!table.length) {
    return { jobs: [], diagnostics };
  }

  const jobs = [];
  table.find('tbody tr[data-row-event-id]').each((_, row) => {
    diagnostics.parsed_row_count_before_filtering += 1;
    const $row = $(row);
    const eventId = normalizeCellText($row.attr('data-row-event-id'));
    const jobId = normalizeCellText($row.attr('data-row-job-id') || $row.attr('data-job-id'));
    const cells = $row.find('td');

    const crewCell = parseCopilotScheduleCrewCell(cells.eq(2));
    const customerCell = cells.eq(3);
    const customerLink = customerCell.find('a').first();
    const customerName = normalizeCellText(customerLink.text() || customerCell.text());
    const customerHref = customerLink.attr('href') || '';
    const customerIdMatch = customerHref.match(/\/(\d+)(?:\/)?$/);
    const customerId = customerIdMatch ? customerIdMatch[1] : null;

    if (!eventId || !customerName) {
      return;
    }

    diagnostics.parsed_row_count_after_filtering += 1;
    jobs.push({
      job_id: jobId || null,
      event_id: eventId,
      customer_id: customerId,
      customer_name: customerName,
      crew_name: crewCell.crew_name || null,
      employees: crewCell.employees_text || null,
      address: normalizeCellText(cells.eq(5).text()) || null,
      status: normalizeCellText(cells.eq(10).text()) || null,
      visit_total: normalizeCellText(cells.eq(14).text()) || null,
      job_title: normalizeCellText(cells.eq(1).text()) || null,
      stop_order: null,
      raw_data: {
        source_surface: 'schedule_grid',
        service_date_label: normalizeCellText(cells.eq(0).text()) || null,
        property_name: normalizeCellText(cells.eq(4).text()) || null,
        event_type: normalizeCellText(cells.eq(6).text()) || null,
        invoiceable: normalizeCellText(cells.eq(7).text()) || null,
        frequency: normalizeCellText(cells.eq(8).text()) || null,
        last_serviced: normalizeCellText(cells.eq(9).text()) || null,
        notes: normalizeCellText(cells.eq(11).text()) || null,
        tracked_time: normalizeCellText(cells.eq(12).text()) || null,
        budgeted_hours: normalizeCellText(cells.eq(13).text()) || null,
      },
    });
  });

  return { jobs, diagnostics };
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
  const html = data.html || '';
  const jobs = parseCopilotRouteHtml(html, data.employees || []);
  const $ = cheerio.load(html);

  return {
    sync_date: syncDate,
    raw: data,
    jobs,
    diagnostics: {
      html_source_length: html.length,
      contains_expected_grid_table_markers:
        html.includes('copilot-table') && html.includes('data-row-event-id'),
      parsed_row_count_before_filtering: $('tr[data-row-event-id]').length,
      parsed_row_count_after_filtering: jobs.length,
      total_event_count: Number.parseInt(data.totalEventCount, 10) || 0,
    },
  };
}

async function fetchCopilotScheduleGridJobsForDate({
  cookieHeader,
  syncDate,
  fetchImpl = fetch,
} = {}) {
  if (!cookieHeader) throw new Error('CopilotCRM cookies are required');
  if (!syncDate) throw new Error('syncDate is required');

  const url = `${COPILOT_WEB_BASE}${COPILOT_SCHEDULE_GRID_DAY_PATH}?d=${encodeURIComponent(syncDate)}`;
  const response = await fetchImpl(url, {
    headers: {
      'Cookie': cookieHeader,
      'Referer': `${COPILOT_WEB_BASE}${COPILOT_SCHEDULE_GRID_DAY_PATH}`,
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    },
  });

  if (!response.ok) {
    const errBody = await response.text().catch(() => '');
    throw new Error(`CopilotCRM schedule grid/day returned ${response.status}: ${errBody.substring(0, 200)}`);
  }

  const html = await response.text();
  const parsed = parseCopilotScheduleGridDayHtml(html);

  return {
    sync_date: syncDate,
    source_surface: 'schedule_grid',
    raw: {
      html_length: html.length,
      url,
      no_events_found: parsed.diagnostics.no_events_found,
    },
    jobs: parsed.jobs,
    diagnostics: parsed.diagnostics,
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
