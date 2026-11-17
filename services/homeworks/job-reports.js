'use strict';
const records = require('./records');
const jobs = require('./jobs');

function period(params = {}) {
  const today = records.dateKey();
  const start = params.date || params.start_date || `${today.slice(0, 4)}-01-01`;
  const end = params.date || params.end_date || today;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(start) || !/^\d{4}-\d{2}-\d{2}$/.test(end) || start > end) {
    throw new Error('Choose a valid visit date range.');
  }
  return { start_date: start, end_date: end };
}

async function stats(context, params) {
  const scope = period(params);
  const { jobs: visits } = await jobs.list(context, scope);
  const byStatus = {},
    byCrew = {};
  let value = 0;
  for (const visit of visits) {
    byStatus[visit.status] = (byStatus[visit.status] || 0) + 1;
    const crew = visit.crew_assigned || 'Unassigned';
    byCrew[crew] = (byCrew[crew] || 0) + 1;
    value += records.cents(visit.service_price);
  }
  return {
    success: true,
    source: records.SOURCE,
    scope,
    stats: {
      total: visits.length,
      byStatus,
      byCrew,
      totalRevenue: value / 100,
      revenue_basis: 'Scheduled visit value; this is not collected revenue.',
    },
  };
}

async function dashboard(context) {
  const today = records.dateKey();
  const week = new Date(`${today}T12:00:00Z`);
  week.setUTCDate(week.getUTCDate() + 7);
  const scope = { start_date: today, end_date: week.toISOString().slice(0, 10) };
  const { jobs: visits } = await jobs.list(context, scope);
  const ordered = [...visits].sort(
    (a, b) => a.job_date.localeCompare(b.job_date) || a.homeworks_event_id - b.homeworks_event_id,
  );
  return {
    success: true,
    source: records.SOURCE,
    scope,
    stats: {
      today: visits.filter((row) => row.job_date === today).length,
      thisWeek: visits.length,
      pending: visits.filter((row) => row.status === 'pending').length,
    },
    upcoming: ordered.slice(0, 5),
  };
}

async function calendar(context, month) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(month || '')) throw new Error('Provide month as YYYY-MM.');
  const [year, number] = month.split('-').map(Number);
  const scope = {
    start_date: month + '-01',
    end_date: month + '-' + new Date(Date.UTC(year, number, 0)).getUTCDate(),
  };
  const { jobs: visits } = await jobs.list(context, scope);
  const grouped = new Map();
  for (const visit of visits) {
    const day = grouped.get(visit.job_date) || {
      day: visit.job_date,
      total_jobs: 0,
      completed: 0,
      pending: 0,
      in_progress: 0,
      cents: 0,
      crews: {},
    };
    day.total_jobs++;
    if (visit.status === 'completed') day.completed++;
    if (visit.status === 'pending') day.pending++;
    day.cents += records.cents(visit.service_price);
    const crew = visit.crew_assigned || 'Unassigned';
    day.crews[crew] = (day.crews[crew] || 0) + 1;
    grouped.set(day.day, day);
  }
  return {
    success: true,
    source: records.SOURCE,
    scope,
    month,
    revenue_basis: 'Scheduled visit value',
    days: [...grouped.values()]
      .sort((a, b) => a.day.localeCompare(b.day))
      .map(({ cents, ...day }) => ({ ...day, revenue: cents / 100 })),
  };
}

async function completedUninvoiced(context, params) {
  const scope = period(params);
  const { jobs: visits } = await jobs.list(context, { ...scope, status: 'completed' });
  return {
    success: true,
    source: records.SOURCE,
    scope,
    jobs: visits.filter((row) => !row.invoice_id).map((row) => ({ ...row, billing_review_required: true })),
    note: 'A completed visit without an invoice is not permission to invoice it. Review is_invoiceable and customer billing rules in Homeworks.',
  };
}

async function pipeline(context, params) {
  const scope = period(params),
    { jobs: visits } = await jobs.list(context, scope);
  const stages = ['new', 'quoted', 'scheduled', 'in_progress', 'completed', 'invoiced'];
  const pipeline = Object.fromEntries(stages.map((stage) => [stage, []]));
  for (const visit of visits) {
    const stage = visit.invoice_id ? 'invoiced' : visit.status === 'completed' ? 'completed' : 'scheduled';
    pipeline[stage].push({ ...visit, stage });
  }
  return {
    success: true,
    source: records.SOURCE,
    scope,
    stages,
    pipeline,
    counts: Object.fromEntries(stages.map((stage) => [stage, pipeline[stage].length])),
  };
}

module.exports = { period, stats, dashboard, calendar, completedUninvoiced, pipeline };
