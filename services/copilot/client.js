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
  const auth = require('../homeworks/auth');
  const accessToken = await auth.getHomeWorksAccessToken(pool);
  return accessToken ? {accessToken,cookieHeader:'copilotApiAccessToken='+accessToken,...auth.tokenExpiry(accessToken),source:'official_homeworks_graphql'} : null;
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

    // Grid/day rows include a leading checkbox column at index 0.
    // Every business field is shifted by +1 compared to the route/day parser.
    const dateCell = cells.eq(1);
    const titleCell = cells.eq(2);
    const crewCell = parseCopilotScheduleCrewCell(cells.eq(3));
    const customerCell = cells.eq(4);
    const propertyCell = cells.eq(5);
    const addressCell = cells.eq(6);
    const typeCell = cells.eq(7);
    const invoiceableCell = cells.eq(8);
    const frequencyCell = cells.eq(9);
    const lastServicedCell = cells.eq(10);
    const statusCell = cells.eq(11);
    const notesCell = cells.eq(12);
    const trackedTimeCell = cells.eq(13);
    const budgetedHoursCell = cells.eq(14);
    const visitTotalCell = cells.eq(15);
    const customerLink = customerCell.find('a').first();
    const customerName = normalizeCellText(customerLink.text() || customerCell.text());
    const customerHref = customerLink.attr('href') || '';
    const customerIdMatch = customerHref.match(/\/(\d+)(?:\/)?$/);
    const customerId = customerIdMatch ? customerIdMatch[1] : null;
    const statusLabel = normalizeCellText(statusCell.find('.status-label').first().text());

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
      address: normalizeCellText(addressCell.text()) || null,
      status: statusLabel || normalizeCellText(statusCell.text()) || null,
      visit_total: normalizeCellText(visitTotalCell.text()) || null,
      job_title: normalizeCellText(titleCell.text()) || null,
      stop_order: null,
      raw_data: {
        source_surface: 'schedule_grid',
        service_date_label: normalizeCellText(dateCell.text()) || null,
        property_name: normalizeCellText(propertyCell.text()) || null,
        event_type: normalizeCellText(typeCell.text()) || null,
        invoiceable: normalizeCellText(invoiceableCell.text()) || null,
        frequency: normalizeCellText(frequencyCell.text()) || null,
        last_serviced: normalizeCellText(lastServicedCell.text()) || null,
        notes: normalizeCellText(notesCell.text()) || null,
        tracked_time: normalizeCellText(trackedTimeCell.text()) || null,
        budgeted_hours: normalizeCellText(budgetedHoursCell.text()) || null,
      },
    });
  });

  return { jobs, diagnostics };
}

async function fetchCopilotRouteJobsForDate({pool,cookieHeader,syncDate,fetchImpl=fetch}) {
  const client=require('../homeworks/client');
  const jobs=await client.fetchHomeWorksEvents({pool,accessToken:cookieHeader?client.extractAccessToken(cookieHeader):undefined,startDate:syncDate,endDate:syncDate,fetchImpl});
  return {jobs,diagnostics:{source:'official_homeworks_graphql',count:jobs.length}};
}

async function fetchCopilotScheduleGridJobsForDate(options) {
  return fetchCopilotRouteJobsForDate(options);
}

/**
 * Login with username/password and return Set-Cookie headers.
 * Used by the contract-sign flow when long-lived cookies are not configured.
 */
async function loginWithCredentials() {
  throw new Error('Browser-session login was retired. Configure the official Homeworks API token in integration settings.');
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
