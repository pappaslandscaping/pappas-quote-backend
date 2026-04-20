// ═══════════════════════════════════════════════════════════
// Jobs, Crews & Dispatch Routes — extracted from server.js
// Handles: jobs CRUD, crews, dispatch board, route optimization,
//          recurring scheduling, import, profitability, templates
// ═══════════════════════════════════════════════════════════

const express = require('express');
const crypto = require('crypto');
const { validate, schemas } = require('../lib/validate');
const { DISPATCH_ROUTE_TEMPLATE_SEEDS } = require('../lib/dispatch-route-template-seeds');
const {
  getCopilotToken,
  fetchCopilotRouteJobsForDate,
} = require('../services/copilot/client');
const {
  applyDispatchRouteTemplate,
  buildDispatchRouteTemplateStop,
  buildDispatchBoardPayload,
  fetchResolvedLiveJobs,
  getCopilotLiveJobs,
  isDispatchRouteTemplateApplicable,
  mapResolvedLiveJobToScheduleJob,
  patchDispatchPlanItems,
} = require('../services/copilot/live-jobs');

const VALID_JOB_STATUSES = new Set(['pending', 'in_progress', 'completed', 'skipped', 'cancelled']);
const COPILOT_SYNCABLE_STATUSES = new Map([
  ['scheduled', 'pending'],
  ['assigned', 'pending'],
  ['not_started', 'pending'],
  ['pending', 'pending'],
  ['en_route', 'in_progress'],
  ['started', 'in_progress'],
  ['active', 'in_progress'],
  ['in_progress', 'in_progress'],
  ['in-progress', 'in_progress'],
  ['completed', 'completed'],
  ['closed', 'completed'],
  ['skipped', 'skipped'],
  ['unable_to_complete', 'skipped'],
  ['no_access', 'skipped'],
  ['cancelled', 'cancelled'],
  ['canceled', 'cancelled'],
]);

class JobStatusTransitionError extends Error {
  constructor(message, statusCode = 400) {
    super(message);
    this.name = 'JobStatusTransitionError';
    this.statusCode = statusCode;
  }
}

function normalizeJobStatus(status) {
  if (typeof status !== 'string') return null;
  const normalized = status.trim().toLowerCase().replace(/[\s-]+/g, '_');
  if (normalized === 'done') return 'completed';
  if (normalized === 'canceled') return 'cancelled';
  if (VALID_JOB_STATUSES.has(normalized)) return normalized;
  return null;
}

function hasCopilotDispatchSyncAccess(user) {
  if (!user || user.isEmployee) return false;
  return !!(user.isAdmin || user.role === 'admin' || user.accountType === 'admin' || user.isServiceToken);
}

function normalizeCopilotExecutionStatus(status) {
  if (typeof status !== 'string') return null;
  const normalized = status.trim().toLowerCase().replace(/[\s-]+/g, '_');
  return COPILOT_SYNCABLE_STATUSES.get(normalized) || null;
}

function isValidIsoDate(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function toIsoTimestamp(value) {
  if (value === undefined) return undefined;
  if (value === null || value === '') return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return undefined;
  return date.toISOString();
}

function toNullableString(value) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  const stringValue = String(value).trim();
  return stringValue || null;
}

function toNullableDecimal(value) {
  if (value === undefined) return undefined;
  if (value === null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function toJsonArray(value) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (Array.isArray(value)) return value;
  return [value];
}

function firstDefined(...values) {
  return values.find((value) => value !== undefined);
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const nextChar = line[index + 1];
    if (char === '"' && !inQuotes) inQuotes = true;
    else if (char === '"' && inQuotes) {
      if (nextChar === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = false;
      }
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current);
  return result;
}

function normalizeCopilotLinkText(value) {
  if (value === undefined || value === null) return null;
  const normalized = String(value)
    .trim()
    .toLowerCase()
    .replace(/[.,]/g, ' ')
    .replace(/\s+/g, ' ');
  return normalized || null;
}

function normalizeCopilotLinkAddress(value) {
  if (value === undefined || value === null) return null;
  const normalized = String(value)
    .trim()
    .toLowerCase()
    .replace(/[.,]/g, ' ')
    .replace(/\s+/g, ' ');
  return normalized || null;
}

function normalizeCopilotLinkMoney(value) {
  if (value === undefined || value === null || value === '') return null;
  const numeric = Number.parseFloat(String(value).replace(/[^0-9.-]/g, ''));
  if (!Number.isFinite(numeric)) return null;
  return numeric.toFixed(2);
}

function buildCopilotImportMatchFingerprint({
  job_date,
  customer_name,
  service_type,
  address,
  service_price,
} = {}) {
  return {
    job_date: isValidIsoDate(job_date) ? job_date : null,
    customer_name: normalizeCopilotLinkText(customer_name),
    service_type: normalizeCopilotLinkText(service_type),
    address: normalizeCopilotLinkAddress(address),
    service_price: normalizeCopilotLinkMoney(service_price),
  };
}

function buildCopilotSyncJobFingerprint(row = {}) {
  return {
    job_date: isValidIsoDate(row.sync_date) ? row.sync_date : null,
    customer_name: normalizeCopilotLinkText(row.customer_name),
    service_type: normalizeCopilotLinkText(row.job_title),
    address: normalizeCopilotLinkAddress(row.address),
    service_price: normalizeCopilotLinkMoney(row.visit_total),
  };
}

function findStrictCopilotImportCandidates(importRow, copilotRows = []) {
  const importFingerprint = buildCopilotImportMatchFingerprint(importRow);
  return (copilotRows || []).filter((row) => {
    const candidateFingerprint = buildCopilotSyncJobFingerprint(row);
    if (!importFingerprint.job_date || candidateFingerprint.job_date !== importFingerprint.job_date) return false;
    if (!importFingerprint.customer_name || candidateFingerprint.customer_name !== importFingerprint.customer_name) return false;
    if (!importFingerprint.service_type || candidateFingerprint.service_type !== importFingerprint.service_type) return false;
    if (!importFingerprint.address || candidateFingerprint.address !== importFingerprint.address) return false;
    if (importFingerprint.service_price && candidateFingerprint.service_price && candidateFingerprint.service_price !== importFingerprint.service_price) {
      return false;
    }
    return true;
  });
}

function planCopilotImportLinkage(importRows = [], copilotRowsByDate = new Map()) {
  const candidateRowsByImportIndex = new Map();
  const importIndicesByVisitId = new Map();
  const diagnostics = [];
  const summary = {
    attempted: importRows.length,
    linkable: 0,
    unmatched: 0,
    ambiguous: 0,
  };

  importRows.forEach((row, index) => {
    const copilotRows = copilotRowsByDate.get(row.job_date) || [];
    const candidates = findStrictCopilotImportCandidates(row, copilotRows);
    candidateRowsByImportIndex.set(index, candidates);
    for (const candidate of candidates) {
      const visitId = candidate.event_id || null;
      if (!visitId) continue;
      if (!importIndicesByVisitId.has(visitId)) importIndicesByVisitId.set(visitId, []);
      importIndicesByVisitId.get(visitId).push(index);
    }
  });

  const plan = importRows.map((row, index) => {
    const candidates = candidateRowsByImportIndex.get(index) || [];
    const base = {
      row_index: index + 1,
      job_date: row.job_date || null,
      customer_name: row.customer_name || null,
      service_type: row.service_type || null,
      address: row.address || null,
      service_price: row.service_price ?? null,
      candidate_visit_ids: candidates.map((candidate) => candidate.event_id).filter(Boolean),
      candidate_count: candidates.length,
    };

    if (candidates.length === 0) {
      summary.unmatched += 1;
      diagnostics.push({ ...base, status: 'unmatched', reason: 'no_strict_match' });
      return { status: 'unmatched', reason: 'no_strict_match', candidate: null, ...base };
    }

    if (candidates.length > 1) {
      summary.ambiguous += 1;
      diagnostics.push({ ...base, status: 'ambiguous', reason: 'multiple_copilot_candidates' });
      return { status: 'ambiguous', reason: 'multiple_copilot_candidates', candidate: null, ...base };
    }

    const candidate = candidates[0];
    const importMatches = importIndicesByVisitId.get(candidate.event_id) || [];
    if (importMatches.length > 1) {
      summary.ambiguous += 1;
      diagnostics.push({ ...base, status: 'ambiguous', reason: 'multiple_import_rows_match_same_visit' });
      return { status: 'ambiguous', reason: 'multiple_import_rows_match_same_visit', candidate: null, ...base };
    }

    summary.linkable += 1;
    return { status: 'linkable', reason: null, candidate, ...base };
  });

  return { plan, summary, diagnostics };
}

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function hashCopilotExecutionPayload(payload) {
  return crypto.createHash('sha256').update(stableStringify(payload)).digest('hex');
}

function canonicalizeCopilotExecutionMirror(mirror) {
  const payload = {};
  for (const [key, value] of Object.entries(mirror)) {
    if (value !== undefined) payload[key] = value;
  }
  return payload;
}

function mapCopilotExecutionMirror(rawRecord = {}) {
  const mirror = {
    copilot_job_id: toNullableString(firstDefined(
      rawRecord.job_id,
      rawRecord.copilot_job_id,
      rawRecord.external_job_id
    )),
    copilot_visit_id: toNullableString(firstDefined(
      rawRecord.visit_id,
      rawRecord.copilot_visit_id,
      rawRecord.event_id,
      rawRecord.external_visit_id
    )),
    copilot_assigned_crew_name: toNullableString(firstDefined(
      rawRecord.assigned_crew_name,
      rawRecord.copilot_assigned_crew_name,
      rawRecord.crew_name
    )),
    copilot_execution_status_raw: toNullableString(firstDefined(
      rawRecord.execution_status_raw,
      rawRecord.copilot_execution_status_raw,
      rawRecord.status
    )),
    copilot_execution_reason: toNullableString(firstDefined(
      rawRecord.execution_reason,
      rawRecord.copilot_execution_reason,
      rawRecord.status_reason,
      rawRecord.skip_reason,
      rawRecord.reason
    )),
    copilot_started_at: toIsoTimestamp(firstDefined(
      rawRecord.started_at,
      rawRecord.copilot_started_at,
      rawRecord.start_time
    )),
    copilot_started_by: toNullableString(firstDefined(
      rawRecord.started_by,
      rawRecord.copilot_started_by,
      rawRecord.start_actor,
      rawRecord.start_technician
    )),
    copilot_completed_at: toIsoTimestamp(firstDefined(
      rawRecord.completed_at,
      rawRecord.copilot_completed_at,
      rawRecord.closed_at,
      rawRecord.completion_time
    )),
    copilot_completed_by: toNullableString(firstDefined(
      rawRecord.completed_by,
      rawRecord.copilot_completed_by,
      rawRecord.closed_by,
      rawRecord.completion_actor
    )),
    copilot_completion_notes: toNullableString(firstDefined(
      rawRecord.completion_notes,
      rawRecord.copilot_completion_notes,
      rawRecord.notes,
      rawRecord.close_notes
    )),
    copilot_completion_photos: toJsonArray(firstDefined(
      rawRecord.completion_photos,
      rawRecord.copilot_completion_photos,
      rawRecord.photos,
      rawRecord.media_urls
    )),
    copilot_completion_lat: toNullableDecimal(firstDefined(
      rawRecord.completion_lat,
      rawRecord.copilot_completion_lat,
      rawRecord.gps_lat
    )),
    copilot_completion_lng: toNullableDecimal(firstDefined(
      rawRecord.completion_lng,
      rawRecord.copilot_completion_lng,
      rawRecord.gps_lng
    )),
    copilot_event_updated_at: toIsoTimestamp(firstDefined(
      rawRecord.event_updated_at,
      rawRecord.copilot_event_updated_at,
      rawRecord.updated_at,
      rawRecord.last_updated_at
    )),
    copilot_execution_locked: true,
  };

  const normalizedStatus = normalizeCopilotExecutionStatus(firstDefined(
    rawRecord.execution_status,
    rawRecord.copilot_execution_status,
    mirror.copilot_execution_status_raw
  ));
  if (normalizedStatus !== null) {
    mirror.copilot_execution_status = normalizedStatus;
  }

  return mirror;
}

function* eachDateInRange(dateFrom, dateTo) {
  const cursor = new Date(`${dateFrom}T00:00:00`);
  const end = new Date(`${dateTo}T00:00:00`);
  while (cursor <= end) {
    yield cursor.toISOString().slice(0, 10);
    cursor.setDate(cursor.getDate() + 1);
  }
}

async function fetchCopilotDispatchExecutionRecords({
  poolClient,
  dateFrom,
  dateTo,
  fetchImpl = fetch,
} = {}) {
  const tokenInfo = await getCopilotToken(poolClient);
  if (!tokenInfo || !tokenInfo.cookieHeader) {
    throw new Error('No CopilotCRM cookies configured.');
  }

  const records = [];
  for (const syncDate of eachDateInRange(dateFrom, dateTo)) {
    const { jobs } = await fetchCopilotRouteJobsForDate({
      cookieHeader: tokenInfo.cookieHeader,
      syncDate,
      fetchImpl,
    });
    for (const job of jobs) {
      records.push({
        visit_id: job.event_id,
        assigned_crew_name: job.crew_name,
        execution_status_raw: job.status,
        job_date: syncDate,
      });
    }
  }

  return records;
}

async function findScheduledJobForCopilotMirror({ mirror, jobDate, poolClient }) {
  if (mirror.copilot_visit_id) {
    const byVisit = await poolClient.query(
      'SELECT * FROM scheduled_jobs WHERE copilot_visit_id = $1 ORDER BY id ASC LIMIT 1',
      [mirror.copilot_visit_id]
    );
    if (byVisit.rows[0]) return byVisit.rows[0];
  }

  if (mirror.copilot_job_id && jobDate) {
    const byJob = await poolClient.query(
      'SELECT * FROM scheduled_jobs WHERE copilot_job_id = $1 AND job_date = $2 ORDER BY id ASC LIMIT 1',
      [mirror.copilot_job_id, jobDate]
    );
    if (byJob.rows[0]) return byJob.rows[0];
  }

  return null;
}

async function updateScheduledJobCopilotMirror({
  jobId,
  mirror,
  payloadHash,
  syncedAt,
  poolClient,
} = {}) {
  const sets = [];
  const values = [];
  let index = 1;

  for (const [column, value] of Object.entries(mirror)) {
    if (value === undefined) continue;
    if (column === 'copilot_completion_photos') {
      sets.push(`${column} = $${index++}::jsonb`);
      values.push(value === null ? null : JSON.stringify(value));
      continue;
    }
    sets.push(`${column} = $${index++}`);
    values.push(value);
  }

  sets.push(`copilot_payload_hash = $${index++}`);
  values.push(payloadHash);
  sets.push(`copilot_last_synced_at = $${index++}`);
  values.push(syncedAt);
  sets.push('updated_at = CURRENT_TIMESTAMP');

  values.push(jobId);
  const result = await poolClient.query(
    `UPDATE scheduled_jobs SET ${sets.join(', ')} WHERE id = $${index} RETURNING *`,
    values
  );
  return result.rows[0];
}

async function syncCopilotDispatchExecutionRecords({
  records,
  poolClient,
  dryRun = false,
  force = false,
  syncedAt = new Date().toISOString(),
} = {}) {
  const summary = {
    fetched: Array.isArray(records) ? records.length : 0,
    matched: 0,
    updated: 0,
    skipped_unmatched: 0,
    skipped_unchanged: 0,
    skipped_stale: 0,
  };

  for (const record of records || []) {
    const jobDate = isValidIsoDate(record?.job_date) ? record.job_date : null;
    const mirror = mapCopilotExecutionMirror(record);
    const match = await findScheduledJobForCopilotMirror({ mirror, jobDate, poolClient });
    if (!match) {
      summary.skipped_unmatched += 1;
      continue;
    }

    summary.matched += 1;
    const canonicalPayload = canonicalizeCopilotExecutionMirror(mirror);
    const payloadHash = hashCopilotExecutionPayload(canonicalPayload);

    if (!force && mirror.copilot_event_updated_at && match.copilot_event_updated_at) {
      const incomingUpdatedAt = Date.parse(mirror.copilot_event_updated_at);
      const storedUpdatedAt = Date.parse(match.copilot_event_updated_at);
      if (!Number.isNaN(incomingUpdatedAt) && !Number.isNaN(storedUpdatedAt) && incomingUpdatedAt < storedUpdatedAt) {
        summary.skipped_stale += 1;
        continue;
      }
    }

    if (!force && match.copilot_payload_hash && match.copilot_payload_hash === payloadHash) {
      summary.skipped_unchanged += 1;
      continue;
    }

    summary.updated += 1;
    if (!dryRun) {
      await updateScheduledJobCopilotMirror({
        jobId: match.id,
        mirror,
        payloadHash,
        syncedAt,
        poolClient,
      });
    }
  }

  return summary;
}

async function persistCopilotSyncJobs({
  syncDate,
  jobs,
  poolClient,
} = {}) {
  const summary = { inserted: 0, updated: 0 };
  for (const job of jobs || []) {
    const result = await poolClient.query(
      `INSERT INTO copilot_sync_jobs (sync_date, event_id, customer_name, customer_id, crew_name, employees, address, status, visit_total, job_title, stop_order, raw_data, synced_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
       ON CONFLICT (sync_date, event_id) DO UPDATE SET
         customer_name = EXCLUDED.customer_name,
         customer_id = EXCLUDED.customer_id,
         crew_name = EXCLUDED.crew_name,
         employees = EXCLUDED.employees,
         address = EXCLUDED.address,
         status = EXCLUDED.status,
         visit_total = EXCLUDED.visit_total,
         job_title = EXCLUDED.job_title,
         stop_order = EXCLUDED.stop_order,
         raw_data = EXCLUDED.raw_data,
         synced_at = NOW()
       RETURNING (xmax = 0) AS is_insert`,
      [
        syncDate,
        job.event_id,
        job.customer_name,
        job.customer_id,
        job.crew_name,
        job.employees,
        job.address,
        job.status,
        job.visit_total,
        job.job_title,
        job.stop_order,
        JSON.stringify(job),
      ]
    );
    if (result.rows[0]?.is_insert) summary.inserted += 1;
    else summary.updated += 1;
  }
  return summary;
}

async function hydrateCopilotScheduleDates({
  syncDates,
  poolClient,
  fetchImpl = fetch,
  logger = console,
} = {}) {
  const summary = {
    attempted: Array.isArray(syncDates) ? syncDates.length : 0,
    hydrated_dates: 0,
    fetched: 0,
    inserted: 0,
    updated: 0,
    failed_dates: [],
    skipped: false,
    reason: null,
  };
  const rowsByDate = new Map();

  if (!Array.isArray(syncDates) || syncDates.length === 0) {
    summary.skipped = true;
    summary.reason = 'no_dates';
    return { rowsByDate, summary };
  }

  const tokenInfo = await getCopilotToken(poolClient);
  if (!tokenInfo || !tokenInfo.cookieHeader) {
    summary.skipped = true;
    summary.reason = 'missing_copilot_cookies';
    logger.warn('[import-scheduling] Copilot linkage skipped: no CopilotCRM cookies configured');
    return { rowsByDate, summary };
  }

  for (const syncDate of syncDates) {
    try {
      const { jobs } = await fetchCopilotRouteJobsForDate({
        cookieHeader: tokenInfo.cookieHeader,
        syncDate,
        fetchImpl,
      });
      const jobsForDate = (jobs || []).map((job) => ({
        ...job,
        sync_date: isValidIsoDate(job?.sync_date) ? job.sync_date : syncDate,
      }));
      rowsByDate.set(syncDate, jobsForDate);
      summary.hydrated_dates += 1;
      summary.fetched += jobsForDate.length;
      const persisted = await persistCopilotSyncJobs({ syncDate, jobs: jobsForDate, poolClient });
      summary.inserted += persisted.inserted;
      summary.updated += persisted.updated;
    } catch (error) {
      summary.failed_dates.push({ sync_date: syncDate, error: error.message });
      logger.warn(`[import-scheduling] Copilot hydration failed for ${syncDate}: ${error.message}`);
    }
  }

  return { rowsByDate, summary };
}

async function applyCopilotImportLinkage({
  scheduledJobId,
  currentJob,
  linkagePlan,
  poolClient,
  logger = console,
} = {}) {
  const result = {
    status: linkagePlan?.status || 'unmatched',
    reason: linkagePlan?.reason || null,
    copilot_visit_id: null,
    copilot_job_id: null,
  };

  if (!linkagePlan || linkagePlan.status !== 'linkable' || !linkagePlan.candidate?.event_id) {
    return result;
  }

  const visitId = linkagePlan.candidate.event_id;
  const explicitJobId = toNullableString(firstDefined(
    linkagePlan.candidate.job_id,
    linkagePlan.candidate.raw_data?.job_id,
    linkagePlan.candidate.raw_data?.copilot_job_id,
    linkagePlan.candidate.raw_data?.external_job_id
  ));

  if (currentJob?.copilot_visit_id && currentJob.copilot_visit_id !== visitId) {
    logger.warn(`[import-scheduling] Job ${scheduledJobId} already linked to Copilot visit ${currentJob.copilot_visit_id}; skipping ${visitId}`);
    return {
      status: 'conflict',
      reason: 'job_already_linked_to_different_visit',
      copilot_visit_id: currentJob.copilot_visit_id,
      copilot_job_id: currentJob.copilot_job_id || null,
    };
  }

  const existingByVisit = await poolClient.query(
    'SELECT id FROM scheduled_jobs WHERE copilot_visit_id = $1 AND id <> $2 LIMIT 1',
    [visitId, scheduledJobId]
  );
  if (existingByVisit.rows[0]) {
    logger.warn(`[import-scheduling] Copilot visit ${visitId} already linked to scheduled_jobs.id=${existingByVisit.rows[0].id}; skipping job ${scheduledJobId}`);
    return {
      status: 'conflict',
      reason: 'visit_already_linked_elsewhere',
      copilot_visit_id: null,
      copilot_job_id: null,
    };
  }

  if (currentJob?.copilot_visit_id === visitId) {
    return {
      status: 'already_linked',
      reason: null,
      copilot_visit_id: visitId,
      copilot_job_id: currentJob.copilot_job_id || explicitJobId || null,
    };
  }

  const values = [visitId];
  const sets = ['copilot_visit_id = $1'];
  let parameterIndex = 2;
  if (explicitJobId) {
    sets.push(`copilot_job_id = COALESCE(copilot_job_id, $${parameterIndex})`);
    values.push(explicitJobId);
    parameterIndex += 1;
  }
  values.push(scheduledJobId);
  await poolClient.query(
    `UPDATE scheduled_jobs SET ${sets.join(', ')}, updated_at = CURRENT_TIMESTAMP WHERE id = $${parameterIndex}`,
    values
  );
  logger.log(`[import-scheduling] Linked scheduled_jobs.id=${scheduledJobId} to Copilot visit ${visitId}`);
  return {
    status: 'linked',
    reason: null,
    copilot_visit_id: visitId,
    copilot_job_id: explicitJobId,
  };
}

function validateJobStatusTransition(currentStatus, nextStatus) {
  if (!nextStatus) return;
  if (currentStatus === nextStatus) {
    if (currentStatus === 'in_progress' || currentStatus === 'completed') return;
    throw new JobStatusTransitionError(`Status ${currentStatus} cannot be updated in place`);
  }

  const allowedTransitions = {
    pending: new Set(['in_progress', 'completed', 'skipped', 'cancelled']),
    in_progress: new Set(['completed', 'skipped', 'cancelled']),
    completed: new Set([]),
    skipped: new Set([]),
    cancelled: new Set([]),
  };

  if (!allowedTransitions[currentStatus]?.has(nextStatus)) {
    throw new JobStatusTransitionError(`Invalid status transition from ${currentStatus} to ${nextStatus}`);
  }
}

async function calculateInvoiceTax({ poolClient, customerId, propertyId, lineItems }) {
  try {
    if (customerId) {
      const cust = await poolClient.query('SELECT tax_exempt FROM customers WHERE id = $1', [customerId]);
      if (cust.rows[0] && cust.rows[0].tax_exempt) {
        const subtotal = lineItems.reduce((sum, item) => sum + (parseFloat(item.amount) || 0), 0);
        return {
          lineItems: lineItems.map(item => ({ ...item, tax: 0, taxRate: 0 })),
          subtotal,
          taxAmount: 0,
          total: subtotal,
          effectiveRate: 0,
        };
      }
    }

    let propertyTaxRate = null;
    if (propertyId) {
      const prop = await poolClient.query('SELECT county_tax, city_tax, state_tax FROM properties WHERE id = $1', [propertyId]);
      if (prop.rows[0]) {
        const row = prop.rows[0];
        if (row.county_tax !== null || row.city_tax !== null || row.state_tax !== null) {
          propertyTaxRate = (parseFloat(row.county_tax) || 0) + (parseFloat(row.city_tax) || 0) + (parseFloat(row.state_tax) || 0);
        }
      }
    }

    let defaultRate = 0;
    const settingsResult = await poolClient.query("SELECT value FROM business_settings WHERE key = 'tax_defaults'");
    if (settingsResult.rows[0]) defaultRate = parseFloat(settingsResult.rows[0].value.default_rate) || 0;

    let taxTotal = 0;
    let subtotal = 0;
    const processedItems = lineItems.map(item => {
      const amount = parseFloat(item.amount) || 0;
      subtotal += amount;

      if (item.taxable === false) return { ...item, tax: 0, taxRate: 0 };

      if (propertyTaxRate !== null) {
        const tax = Math.round(amount * propertyTaxRate) / 100;
        taxTotal += tax;
        return { ...item, tax, taxRate: propertyTaxRate };
      }

      if (item.service_tax_rate !== undefined && item.service_tax_rate !== null && parseFloat(item.service_tax_rate) > 0) {
        const rate = parseFloat(item.service_tax_rate);
        const tax = Math.round(amount * rate) / 100;
        taxTotal += tax;
        return { ...item, tax, taxRate: rate };
      }

      const tax = Math.round(amount * defaultRate) / 100;
      taxTotal += tax;
      return { ...item, tax, taxRate: defaultRate };
    });

    const taxAmount = Math.round(taxTotal * 100) / 100;
    return {
      lineItems: processedItems,
      subtotal,
      taxAmount,
      total: subtotal + taxAmount,
      effectiveRate: subtotal > 0 ? Math.round((taxTotal / subtotal) * 10000) / 100 : 0,
    };
  } catch (error) {
    console.error('Tax calculation error:', error);
    const subtotal = lineItems.reduce((sum, item) => sum + (parseFloat(item.amount) || 0), 0);
    return { lineItems, subtotal, taxAmount: 0, total: subtotal, effectiveRate: 0 };
  }
}

async function applyCompletedJobInvoiceSideEffects(completedJob, {
  poolClient,
  calculateTaxFn = calculateInvoiceTax,
  nextInvoiceNumberFn,
} = {}) {
  const custId = completedJob.customer_id;
  const custName = completedJob.customer_name;
  if (!custId) return;

  const now = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split('T')[0];
  const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().split('T')[0];

  const existingInv = await poolClient.query(
    `SELECT id, line_items, subtotal, tax_rate, tax_amount, total FROM invoices
     WHERE customer_id = $1 AND status = 'draft'
     AND created_at >= $2 AND created_at <= ($3::date + interval '1 day')
     ORDER BY created_at DESC LIMIT 1`,
    [custId, monthStart, monthEnd]
  );

  const propertyId = completedJob.property_id || null;
  let propertyName = null;
  if (propertyId) {
    const propRow = await poolClient.query('SELECT property_name, street FROM properties WHERE id = $1', [propertyId]);
    if (propRow.rows[0]) propertyName = propRow.rows[0].property_name || propRow.rows[0].street || null;
  }

  const newItem = {
    name: completedJob.service_type || 'Service',
    description: 'Job #' + completedJob.id + (completedJob.job_date ? ' - ' + new Date(completedJob.job_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : ''),
    quantity: 1,
    rate: parseFloat(completedJob.service_price) || 0,
    amount: parseFloat(completedJob.service_price) || 0,
    service_date: completedJob.completed_at ? new Date(completedJob.completed_at).toISOString().split('T')[0] : (completedJob.job_date ? new Date(completedJob.job_date).toISOString().split('T')[0] : null),
    property_name: propertyName,
  };

  if (existingInv.rows.length > 0) {
    const inv = existingInv.rows[0];
    let items = inv.line_items || [];
    if (typeof items === 'string') items = JSON.parse(items);
    items.push(newItem);
    const taxResult = await calculateTaxFn({ poolClient, customerId: custId, propertyId, lineItems: items });

    await poolClient.query(
      `UPDATE invoices SET line_items = $1, subtotal = $2, tax_amount = $3, total = $4, updated_at = CURRENT_TIMESTAMP WHERE id = $5`,
      [JSON.stringify(taxResult.lineItems), taxResult.subtotal, taxResult.taxAmount, taxResult.total, inv.id]
    );
    await poolClient.query('UPDATE scheduled_jobs SET invoice_id = $1 WHERE id = $2', [inv.id, completedJob.id]);
    return;
  }

  const custRow = await poolClient.query('SELECT email, address FROM customers WHERE id = $1', [custId]);
  const custEmail = custRow.rows[0]?.email || '';
  const custAddress = custRow.rows[0]?.address || completedJob.address || '';
  const invNum = await nextInvoiceNumberFn();
  const dueDate = new Date(now.getFullYear(), now.getMonth() + 1, 0);
  const taxResult = await calculateTaxFn({ poolClient, customerId: custId, propertyId, lineItems: [newItem] });

  const invResult = await poolClient.query(
    `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, customer_address, job_id, status, subtotal, tax_rate, tax_amount, total, due_date, notes, line_items)
     VALUES ($1, $2, $3, $4, $5, $6, 'draft', $7, 0, $8, $9, $10, $11, $12) RETURNING id`,
    [invNum, custId, custName, custEmail, custAddress, completedJob.id, taxResult.subtotal, taxResult.taxAmount, taxResult.total, dueDate.toISOString().split('T')[0],
      'Monthly invoice - ' + now.toLocaleDateString('en-US', { month: 'long', year: 'numeric' }),
      JSON.stringify(taxResult.lineItems)]
  );
  await poolClient.query('UPDATE scheduled_jobs SET invoice_id = $1 WHERE id = $2', [invResult.rows[0].id, completedJob.id]);
}

async function transitionScheduledJobStatus({
  jobId,
  nextStatus,
  actorName = null,
  source = 'system',
  completionNotes,
  completionPhotos,
  completionLat,
  completionLng,
  dispatchIssue,
  dispatchIssueReason,
  poolClient,
  invoiceSideEffectFn,
} = {}) {
  const normalizedStatus = nextStatus === undefined ? undefined : normalizeJobStatus(nextStatus);
  if (nextStatus !== undefined && !normalizedStatus) {
    throw new JobStatusTransitionError('Invalid status value');
  }

  const hasIssueUpdate = dispatchIssue !== undefined;
  if (normalizedStatus === undefined && !hasIssueUpdate) {
    throw new JobStatusTransitionError('status or dispatch_issue is required');
  }

  const currentResult = await poolClient.query('SELECT * FROM scheduled_jobs WHERE id = $1', [jobId]);
  if (currentResult.rows.length === 0) {
    throw new JobStatusTransitionError('Job not found', 404);
  }

  const currentJob = currentResult.rows[0];
  const currentStatus = normalizeJobStatus(currentJob.status) || 'pending';
  validateJobStatusTransition(currentStatus, normalizedStatus);

  const sets = [];
  const vals = [];
  let idx = 1;
  const statusWillChange = normalizedStatus !== undefined && normalizedStatus !== currentStatus;
  const touchesCompletion = normalizedStatus === 'completed' || currentStatus === 'completed';
  const statusActor = actorName || currentJob.last_status_by || null;
  const statusSource = source || currentJob.last_status_source || 'system';

  if (statusWillChange) {
    sets.push(`status = $${idx++}`);
    vals.push(normalizedStatus);
    sets.push('last_status_at = CURRENT_TIMESTAMP');
    sets.push(`last_status_by = $${idx++}`);
    vals.push(statusActor);
    sets.push(`last_status_source = $${idx++}`);
    vals.push(statusSource);
  }

  if (normalizedStatus === 'in_progress') {
    sets.push('started_at = COALESCE(started_at, CURRENT_TIMESTAMP)');
    if (actorName) {
      sets.push(`started_by = COALESCE(started_by, $${idx++})`);
      vals.push(actorName);
    }
  }

  if (touchesCompletion) {
    sets.push('completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)');
    if (actorName) {
      sets.push(`completed_by = $${idx++}`);
      vals.push(actorName);
    }
    if (completionNotes !== undefined) {
      sets.push(`completion_notes = COALESCE($${idx++}, completion_notes)`);
      vals.push(completionNotes || null);
    }
    if (completionPhotos !== undefined) {
      sets.push(`completion_photos = COALESCE($${idx++}::jsonb, completion_photos)`);
      vals.push(completionPhotos == null ? null : JSON.stringify(completionPhotos));
    }
    if (completionLat !== undefined) {
      sets.push(`completion_lat = COALESCE($${idx++}, completion_lat)`);
      vals.push(completionLat ?? null);
    }
    if (completionLng !== undefined) {
      sets.push(`completion_lng = COALESCE($${idx++}, completion_lng)`);
      vals.push(completionLng ?? null);
    }
  }

  if (hasIssueUpdate) {
    if (dispatchIssue) {
      sets.push(`dispatch_issue = $${idx++}`);
      vals.push(true);
      sets.push(`dispatch_issue_reason = COALESCE($${idx++}, dispatch_issue_reason)`);
      vals.push(dispatchIssueReason || null);
      sets.push('dispatch_issue_reported_at = CURRENT_TIMESTAMP');
      sets.push(`dispatch_issue_reported_by = COALESCE($${idx++}, dispatch_issue_reported_by)`);
      vals.push(actorName || null);
    } else {
      sets.push(`dispatch_issue = $${idx++}`);
      vals.push(false);
      sets.push('dispatch_issue_reason = NULL');
      sets.push('dispatch_issue_reported_at = NULL');
      sets.push('dispatch_issue_reported_by = NULL');
    }
  }

  if (sets.length === 0) {
    return currentJob;
  }

  sets.push('updated_at = CURRENT_TIMESTAMP');
  vals.push(jobId);
  const updateResult = await poolClient.query(
    `UPDATE scheduled_jobs SET ${sets.join(', ')} WHERE id = $${idx} RETURNING *`,
    vals
  );
  const updatedJob = updateResult.rows[0];

  if (statusWillChange && normalizedStatus === 'completed') {
    try {
      await invoiceSideEffectFn(updatedJob, { poolClient });
    } catch (autoInvErr) {
      console.error('Auto-invoice error (non-fatal):', autoInvErr.message);
    }
    const refreshedResult = await poolClient.query('SELECT * FROM scheduled_jobs WHERE id = $1', [jobId]);
    return refreshedResult.rows[0] || updatedJob;
  }

  return updatedJob;
}

function createJobRoutes({ pool, serverError, authenticateToken, nextInvoiceNumber, upload, fetchImpl = fetch }) {
  const router = express.Router();

  function normalizeLiveDispatchJobKey(value) {
    if (value == null) return null;
    const normalized = String(value).trim();
    return normalized.startsWith('copilot:') ? normalized : null;
  }

  const CANONICAL_ROUTE_TEMPLATE_SEED_KEYS = DISPATCH_ROUTE_TEMPLATE_SEEDS.map((seed) => seed.seed_key);
  const DISPATCH_AUTO_APPLY_NAME = 'YardDesk Auto Apply';

  function getDispatchActor(req) {
    const user = req?.user || {};
    return {
      updatedByUserId: Number.isFinite(Number(user.id)) ? Number(user.id) : null,
      updatedByName: user.name || user.email || user.username || 'YardDesk Dispatch',
    };
  }

  function stableRouteOrderCompare(left, right) {
    const leftOrder = left?.route_order ?? Number.MAX_SAFE_INTEGER;
    const rightOrder = right?.route_order ?? Number.MAX_SAFE_INTEGER;
    if (leftOrder !== rightOrder) return leftOrder - rightOrder;
    const leftName = left?.customer_name || '';
    const rightName = right?.customer_name || '';
    if (leftName !== rightName) return leftName.localeCompare(rightName);
    return String(left?.id || '').localeCompare(String(right?.id || ''));
  }

  async function fetchLiveDispatchJobsForDate(targetDate) {
    const livePayload = await getCopilotLiveJobs({
      poolClient: pool,
      date: targetDate,
      startDate: targetDate,
      endDate: targetDate,
      fetchImpl,
    });

    return (livePayload.jobs || []).filter((job) => !job?.hold_from_dispatch && !job?.source_deleted);
  }

  async function fetchApplicableCanonicalTemplates(targetDate) {
    if (!targetDate || CANONICAL_ROUTE_TEMPLATE_SEED_KEYS.length === 0) return [];
    const result = await pool.query(
      `SELECT *
         FROM dispatch_route_templates
        WHERE active = true
          AND seed_key = ANY($1::text[])
        ORDER BY crew_name ASC, name ASC`,
      [CANONICAL_ROUTE_TEMPLATE_SEED_KEYS]
    );
    return result.rows.filter((row) => isDispatchRouteTemplateApplicable(row, targetDate));
  }

  async function getRouteOverrideStateForCrewDate(targetDate, crewName) {
    const result = await pool.query(
      `SELECT dpi.updated_by_name
         FROM copilot_live_jobs clj
         JOIN dispatch_plan_items dpi ON dpi.job_key = clj.job_key
        WHERE clj.service_date = $1::date
          AND dpi.route_order_override IS NOT NULL
          AND COALESCE(dpi.crew_override_name, clj.source_crew_name) = $2`,
      [targetDate, crewName]
    );

    const rows = result.rows || [];
    return {
      hasOverrides: rows.length > 0,
      autoAppliedOnly: rows.length > 0 && rows.every((row) => (row.updated_by_name || '') === DISPATCH_AUTO_APPLY_NAME),
    };
  }

  async function maybeAutoApplyCanonicalTemplates({ targetDate, startDate, endDate, livePayload }) {
    if (!targetDate || targetDate !== startDate || targetDate !== endDate) {
      return { livePayload, statuses: [] };
    }

    const templates = await fetchApplicableCanonicalTemplates(targetDate);
    if (!templates.length) return { livePayload, statuses: [] };

    const statuses = [];
    let didApply = false;

    for (const template of templates) {
      const liveJobs = livePayload.jobs || [];
      const candidateCount = liveJobs.filter((job) => (job.crew_assigned || null) === template.crew_name || !job.crew_assigned).length;
      if (candidateCount === 0) continue;

      const overrideState = await getRouteOverrideStateForCrewDate(targetDate, template.crew_name);
      if (overrideState.hasOverrides) {
        statuses.push({
          crew_name: template.crew_name,
          template_name: template.name,
          status: overrideState.autoAppliedOnly ? 'already_applied' : 'manual_overrides',
          message: overrideState.autoAppliedOnly
            ? `Canonical Monday pattern already applied for ${template.crew_name}.`
            : `Manual route overrides already exist for ${template.crew_name}; canonical Monday auto-apply skipped.`,
        });
        continue;
      }

      const stopsResult = await pool.query(
        `SELECT * FROM dispatch_route_template_stops WHERE template_id = $1 ORDER BY position ASC`,
        [template.id]
      );
      const applyResult = applyDispatchRouteTemplate({
        template,
        templateStops: stopsResult.rows,
        liveJobs,
      });

      if (applyResult.patches.length === 0) {
        statuses.push({
          crew_name: template.crew_name,
          template_name: template.name,
          status: 'partial_match',
          message: `Canonical Monday pattern exists for ${template.crew_name}, but no live stops could be matched automatically.`,
          matched_count: 0,
          appended_count: 0,
          unmatched_count: applyResult.unmatched_template_stops.length,
          ambiguous_count: applyResult.ambiguous.length,
          ambiguous: applyResult.ambiguous,
          unmatched_template_stops: applyResult.unmatched_template_stops,
        });
        continue;
      }

      await patchDispatchPlanItems(pool, {
        patches: applyResult.patches,
        updatedByUserId: null,
        updatedByName: DISPATCH_AUTO_APPLY_NAME,
      });
      didApply = true;
      const partial = applyResult.unmatched_template_stops.length > 0 || applyResult.ambiguous.length > 0;
      statuses.push({
        crew_name: template.crew_name,
        template_name: template.name,
        status: partial ? 'partial_match' : 'applied',
        message: partial
          ? `Canonical Monday pattern auto-applied for ${template.crew_name} with ${applyResult.unmatched_template_stops.length} unmatched and ${applyResult.ambiguous.length} ambiguous stop(s).`
          : `Canonical Monday pattern auto-applied for ${template.crew_name}.`,
        matched_count: applyResult.matched.length,
        appended_count: applyResult.appended.length,
        unmatched_count: applyResult.unmatched_template_stops.length,
        ambiguous_count: applyResult.ambiguous.length,
        ambiguous: applyResult.ambiguous,
        unmatched_template_stops: applyResult.unmatched_template_stops,
      });
    }

    if (!didApply) return { livePayload, statuses };

    const refreshedPayload = await getCopilotLiveJobs({
      poolClient: pool,
      date: targetDate,
      startDate,
      endDate,
      fetchImpl,
    });
    return { livePayload: refreshedPayload, statuses };
  }

  function getCrewDispatchJobs(jobs, crewName) {
    return jobs
      .filter((job) => (job.crew_assigned || null) === crewName)
      .sort(stableRouteOrderCompare);
  }

  function escapeXml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&apos;');
  }

  function buildDispatchRouteSheetComments(job) {
    const notes = [];
    const service = String(job?.service_type || '').trim();
    if (service && !/^mowing(?:\s*\(weekly\))?$/i.test(service)) notes.push(service);
    if (job?.print_note) notes.push(job.print_note);
    if (job?.special_notes) notes.push(job.special_notes);
    if (job?.property_notes) notes.push(job.property_notes);
    return [...new Set(notes.filter(Boolean))].join(' / ');
  }

  function buildDispatchRouteSheetAddress(job) {
    if (job?.cust_street && String(job.cust_street).trim()) {
      const parts = [String(job.cust_street).trim()];
      if (job.cust_city) parts.push(job.cust_city);
      if (job.cust_state) parts.push(job.cust_state);
      if (job.cust_zip) parts.push(job.cust_zip);
      return parts.join(', ');
    }
    return job?.address || 'No address';
  }

  function isBiWeeklyDispatchRouteJob(job) {
    return /bi-?\s*weekly/i.test([job?.service_frequency, job?.service_type, job?.service_title].filter(Boolean).join(' '));
  }

  function sanitizeWorksheetName(name, index) {
    const cleaned = String(name || `Sheet ${index + 1}`)
      .replace(/[\[\]\*\/\\\?:]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    return cleaned.slice(0, 31) || `Sheet ${index + 1}`;
  }

  function buildDispatchRouteSheetWorkbookXml({ targetDate, groups }) {
    const displayDate = new Date(`${targetDate}T00:00:00`).toLocaleDateString('en-US', {
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
    const worksheets = groups.map((group, groupIndex) => {
      const sheetName = sanitizeWorksheetName(group.crewName, groupIndex);
      const totalPrice = group.jobs.reduce((sum, job) => sum + (Number(job.service_price) || 0), 0);
      const rows = group.jobs.map((job, index) => `
        <Row>
          <Cell ss:StyleID="cell-center"><Data ss:Type="Number">${index + 1}</Data></Cell>
          <Cell ss:StyleID="cell-center"><Data ss:Type="String"></Data></Cell>
          <Cell ss:StyleID="cell-center"><Data ss:Type="String"></Data></Cell>
          <Cell ss:StyleID="cell-name"><Data ss:Type="String">${escapeXml(`${job.customer_name || ''}${isBiWeeklyDispatchRouteJob(job) ? ' (BW)' : ''}`)}</Data></Cell>
          <Cell ss:StyleID="cell-wrap"><Data ss:Type="String">${escapeXml(buildDispatchRouteSheetAddress(job))}</Data></Cell>
          <Cell ss:StyleID="cell-wrap"><Data ss:Type="String">${escapeXml(buildDispatchRouteSheetComments(job))}</Data></Cell>
        </Row>
      `).join('');

      return `
        <Worksheet ss:Name="${escapeXml(sheetName)}">
          <Table>
            <Column ss:Width="34"/>
            <Column ss:Width="32"/>
            <Column ss:Width="52"/>
            <Column ss:Width="155"/>
            <Column ss:Width="220"/>
            <Column ss:Width="260"/>
            <Row>
              <Cell ss:MergeAcross="5" ss:StyleID="title"><Data ss:Type="String">Pappas &amp; Co. Landscaping</Data></Cell>
            </Row>
            <Row>
              <Cell ss:MergeAcross="5" ss:StyleID="subtitle"><Data ss:Type="String">${escapeXml(group.crewName)} — Route Sheet</Data></Cell>
            </Row>
            <Row>
              <Cell ss:MergeAcross="5" ss:StyleID="meta"><Data ss:Type="String">${escapeXml(displayDate)}</Data></Cell>
            </Row>
            <Row/>
            <Row>
              <Cell ss:StyleID="header"><Data ss:Type="String">#</Data></Cell>
              <Cell ss:StyleID="header"><Data ss:Type="String">Done</Data></Cell>
              <Cell ss:StyleID="header"><Data ss:Type="String">Time</Data></Cell>
              <Cell ss:StyleID="header"><Data ss:Type="String">Name</Data></Cell>
              <Cell ss:StyleID="header"><Data ss:Type="String">Address</Data></Cell>
              <Cell ss:StyleID="header"><Data ss:Type="String">Comments</Data></Cell>
            </Row>
            ${rows}
            <Row/>
            <Row>
              <Cell ss:MergeAcross="5" ss:StyleID="meta"><Data ss:Type="String">${escapeXml(`${group.jobs.length} stops · $${Number(totalPrice || 0).toFixed(0)}`)}</Data></Cell>
            </Row>
          </Table>
        </Worksheet>
      `;
    }).join('');

    return `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:x="urn:schemas-microsoft-com:office:excel"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:html="http://www.w3.org/TR/REC-html40">
 <Styles>
  <Style ss:ID="Default" ss:Name="Normal">
   <Alignment ss:Vertical="Top"/>
   <Font ss:FontName="DM Sans" ss:Size="10" ss:Color="#2e403d"/>
  </Style>
  <Style ss:ID="title">
   <Font ss:FontName="DM Sans" ss:Bold="1" ss:Size="14" ss:Color="#2e403d"/>
  </Style>
  <Style ss:ID="subtitle">
   <Font ss:FontName="DM Sans" ss:Bold="1" ss:Size="12" ss:Color="#2e403d"/>
  </Style>
  <Style ss:ID="meta">
   <Font ss:FontName="DM Sans" ss:Size="10" ss:Color="#6e726e"/>
  </Style>
  <Style ss:ID="header">
   <Font ss:FontName="DM Sans" ss:Bold="1" ss:Size="10" ss:Color="#6e726e"/>
   <Borders>
    <Border ss:Position="Bottom" ss:LineStyle="Continuous" ss:Weight="1"/>
   </Borders>
  </Style>
  <Style ss:ID="cell-center">
   <Alignment ss:Horizontal="Center" ss:Vertical="Top"/>
  </Style>
  <Style ss:ID="cell-name">
   <Font ss:FontName="DM Sans" ss:Bold="1" ss:Size="10" ss:Color="#2e403d"/>
  </Style>
  <Style ss:ID="cell-wrap">
   <Alignment ss:Vertical="Top" ss:WrapText="1"/>
  </Style>
 </Styles>
 ${worksheets}
</Workbook>`;
  }

  function summarizeDispatchRouteTemplate(templateRow, stopCount = 0, { targetDate = null } = {}) {
    return {
      id: templateRow.id,
      name: templateRow.name,
      crew_name: templateRow.crew_name,
      cadence: templateRow.cadence,
      anchor_date: templateRow.anchor_date instanceof Date
        ? templateRow.anchor_date.toISOString().slice(0, 10)
        : templateRow.anchor_date,
      day_of_week: templateRow.day_of_week,
      active: templateRow.active !== false,
      notes: templateRow.notes || null,
      stop_count: Number(stopCount) || 0,
      applies_to_date: targetDate ? isDispatchRouteTemplateApplicable(templateRow, targetDate) : null,
      created_at: templateRow.created_at || null,
      updated_at: templateRow.updated_at || null,
    };
  }

  // Haversine distance (for route optimization)
  function haversineDistance(lat1, lon1, lat2, lon2) {
    const R = 3959;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  // ────────────────────────────────────────────────────────────
  // Address-quality helpers (shared across geocode + optimize paths).
  //
  // The single biggest cause of "all jobs collapse to one pin" was
  // geocoding `job.address` directly, which is often only "Bay Village OH
  // 44140" with no street. Google then returns the city centroid for every
  // such job and they all overlap. Use buildBestJobGeocodeAddress() to get
  // the strongest possible street-level address before geocoding, and
  // isStreetLevelGoogleResult() to gate stored coordinates on real
  // street-level results.
  // ────────────────────────────────────────────────────────────
  function extractStreetAddress(text /*, cityAddress */) {
    if (!text) return null;
    const match = text.match(/(\d+\s+(?:[A-Za-z0-9]+\s+)*?(?:Street|Drive|Road|Avenue|Boulevard|Lane|Court|Circle|Place|Way|Pike|Trail|Parkway|Row|St|Dr|Rd|Ave|Blvd|Ln|Ct|Cir|Pl|Tr|Pkwy))\b/i);
    return match ? match[1].trim() : null;
  }

  // Returns { address, source } where source is 'street' or 'city'.
  // 'city' means we could not find a street number anywhere — geocoding it
  // would only produce a city centroid, so callers should NOT trust the
  // result as a stop coordinate.
  function buildBestJobGeocodeAddress(job) {
    if (!job) return { address: '', source: 'city' };

    // Best: customer.street with a real house number.
    if (job.cust_street && /^\d+/.test(String(job.cust_street).trim())) {
      const street = String(job.cust_street).trim();
      const city = job.cust_city || '';
      const state = job.cust_state || 'OH';
      const zip = job.cust_zip || '';
      return {
        address: `${street}, ${city} ${state} ${zip}`.replace(/\s+/g, ' ').trim(),
        source: 'street',
      };
    }

    // Fallback: try to extract a street out of customer_name or service_type
    // (some Copilot exports stuff "JOHN SMITH 1234 MAIN ST" into the name).
    const fromName = extractStreetAddress(job.customer_name);
    if (fromName) {
      return { address: fromName + (job.address ? ', ' + job.address : ''), source: 'street' };
    }
    const fromService = extractStreetAddress(job.service_type);
    if (fromService) {
      return { address: fromService + (job.address ? ', ' + job.address : ''), source: 'street' };
    }

    // Last resort: bare job.address. If it has a leading street number, we
    // call it street-level; otherwise it's just city/state/zip and we mark
    // it as 'city' so the optimizer skips it.
    const addr = (job.address || '').trim();
    if (/^\d+\s/.test(addr)) return { address: addr, source: 'street' };
    return { address: addr, source: 'city' };
  }

  // Google geocode result is "street-level" only when it identifies a real
  // property pin. Crucially we do NOT accept 'route' — that means Google
  // matched the street but not the house number, which still resolves to
  // a point on the street centerline that many addresses on the same
  // street will collapse onto. Those collapsed pins were the root cause
  // of the "all jobs show as one marker" bug.
  function isStreetLevelGoogleResult(result) {
    if (!result || !Array.isArray(result.types)) return false;
    return result.types.some(t => ['street_address', 'premise', 'subpremise', 'intersection'].includes(t));
  }

  // ────────────────────────────────────────────────────────────
  // Best-forward-route picker — Google Directions + corridor sweep scoring.
  //
  // Why this exists: when you give Google Directions a set of stops and ask
  // it to optimize a forward route, it returns the mathematically shortest
  // sequence — but "shortest" routes through a 2D cluster often zig-zag
  // east/west or jump back across the cluster. Dispatchers want a route
  // that *sweeps* through the area in one direction.
  //
  // Strategy:
  //  1. Detect the cluster's dominant axis (east-west vs north-south) from
  //     its bounding box, with a longitude correction at this latitude.
  //  2. Pick destination candidates from the EXTREME ENDS of that axis on
  //     both sides (e.g. for an east-west cluster, the easternmost AND
  //     westernmost stops). The route can sweep either way.
  //  3. For each candidate, call Google Directions with optimize:true.
  //  4. Score each route with a composite that adds an axis-reversal penalty
  //     (the route moving against its dominant sweep direction) plus a small
  //     radial backtrack penalty as a secondary signal.
  //  5. Lowest composite score wins.
  //
  // `stops` is an array of { id, lat, lng } (already filtered to geocoded jobs).
  // Returns { orderedIds, totalDistance, totalDuration, legs } or null on
  // failure (caller should fall back to nearest-neighbor).
  // ────────────────────────────────────────────────────────────

  // 1° latitude ≈ 69 miles. 1° longitude ≈ 69 * cos(lat) miles.
  const MI_PER_LAT_DEG = 69;
  function miPerLngDeg(lat) { return MI_PER_LAT_DEG * Math.cos(lat * Math.PI / 180); }

  // Pick the axis (lat or lng) with greater real-world spread for this cluster.
  function getDominantAxis(stops) {
    const lats = stops.map(s => s.lat);
    const lngs = stops.map(s => s.lng);
    const latRange = Math.max(...lats) - Math.min(...lats);
    const lngRange = Math.max(...lngs) - Math.min(...lngs);
    const meanLat = (Math.max(...lats) + Math.min(...lats)) / 2;
    const latMiles = latRange * MI_PER_LAT_DEG;
    const lngMiles = lngRange * miPerLngDeg(meanLat);
    return lngMiles >= latMiles ? 'lng' : 'lat';
  }

  function axisValue(stop, axis) { return axis === 'lng' ? stop.lng : stop.lat; }

  // Convert an axis-degree delta into miles for this cluster.
  function axisDeltaMiles(deltaDeg, axis, refLat) {
    return axis === 'lng' ? Math.abs(deltaDeg) * miPerLngDeg(refLat) : Math.abs(deltaDeg) * MI_PER_LAT_DEG;
  }

  // Pick edge candidates from BOTH ends of the dominant axis (a route can
  // sweep east-to-west or west-to-east; we test both endings).
  function pickEdgeCandidates(stops, axis) {
    const sorted = [...stops].sort((a, b) => axisValue(a, axis) - axisValue(b, axis));
    const perSide = Math.max(2, Math.ceil(stops.length * 0.20));
    const lowEnd = sorted.slice(0, perSide);
    const highEnd = sorted.slice(-perSide);
    const seen = new Set();
    const out = [];
    for (const s of [...lowEnd, ...highEnd]) {
      if (!seen.has(s.id)) { seen.add(s.id); out.push(s); }
    }
    return out;
  }

  // ────────────────────────────────────────────────────────────
  // Road-network travel time helpers (Google Distance Matrix).
  //
  // For real driving routes, geometric heuristics aren't enough — one-way
  // streets, freeway access, and bridges produce drive times that differ
  // sharply from straight-line distance. We pull pairwise driving durations
  // from Google Distance Matrix once per optimization, then build the route
  // by nearest-neighbor + 2-opt on those durations.
  //
  // Matrix shape:
  //   matrix.get(fromId).get(toId) = { duration: seconds, distance: meters }
  // 'start' is a synthetic node id for the home base.
  // ────────────────────────────────────────────────────────────
  async function buildTravelTimeMatrix(stops, startLat, startLng, GMAPS_KEY) {
    if (!GMAPS_KEY || stops.length === 0) return null;

    const nodes = [
      { id: 'start', lat: startLat, lng: startLng },
      ...stops.map(s => ({ id: s.id, lat: s.lat, lng: s.lng })),
    ];
    const matrix = new Map();
    for (const n of nodes) matrix.set(n.id, new Map());

    // Distance Matrix limits: ≤25 origins, ≤25 destinations, ≤100 elements per request.
    // Chunk so origin × destination ≤ 100. With 10×10 batches, a 25-stop route
    // (26 nodes) needs (3×3) = 9 calls.
    const ORIGIN_BATCH = 10;
    const DEST_BATCH = 10;

    let okCount = 0;
    let totalEdges = 0;
    for (let oi = 0; oi < nodes.length; oi += ORIGIN_BATCH) {
      const originBatch = nodes.slice(oi, oi + ORIGIN_BATCH);
      const originStr = originBatch.map(n => `${n.lat},${n.lng}`).join('|');
      for (let di = 0; di < nodes.length; di += DEST_BATCH) {
        const destBatch = nodes.slice(di, di + DEST_BATCH);
        const destStr = destBatch.map(n => `${n.lat},${n.lng}`).join('|');
        const url = `https://maps.googleapis.com/maps/api/distancematrix/json?origins=${encodeURIComponent(originStr)}&destinations=${encodeURIComponent(destStr)}&mode=driving&key=${GMAPS_KEY}`;
        try {
          const res = await fetch(url);
          const data = await res.json();
          if (data.status !== 'OK' || !Array.isArray(data.rows)) continue;
          for (let i = 0; i < data.rows.length; i++) {
            const fromId = originBatch[i].id;
            const fromRow = matrix.get(fromId);
            const cells = data.rows[i].elements || [];
            for (let j = 0; j < cells.length; j++) {
              const toId = destBatch[j].id;
              if (fromId === toId) {
                fromRow.set(toId, { duration: 0, distance: 0 });
                continue;
              }
              totalEdges++;
              const cell = cells[j];
              if (cell.status === 'OK') {
                fromRow.set(toId, {
                  duration: cell.duration?.value || 0,
                  distance: cell.distance?.value || 0,
                });
                okCount++;
              }
            }
          }
        } catch (e) {
          console.error('Distance Matrix call failed:', e.message);
        }
      }
    }

    // If Google succeeded for fewer than 60% of edges, the matrix is unreliable
    // and the geometric fallback is a safer choice.
    if (totalEdges === 0 || okCount / totalEdges < 0.6) return null;

    // Fill any missing edges with a haversine estimate (~30 mph average) so
    // nearest-neighbor doesn't trip on undefined lookups.
    function estimateEdge(from, to) {
      const miles = haversineDistance(from.lat, from.lng, to.lat, to.lng);
      return { duration: Math.round((miles / 30) * 3600), distance: Math.round(miles * 1609.34) };
    }
    for (const fromNode of nodes) {
      const row = matrix.get(fromNode.id);
      for (const toNode of nodes) {
        if (fromNode.id === toNode.id) continue;
        if (!row.has(toNode.id)) row.set(toNode.id, estimateEdge(fromNode, toNode));
      }
    }
    return matrix;
  }

  function routeDurationFromMatrix(orderedIds, matrix) {
    if (orderedIds.length === 0) return 0;
    let total = matrix.get('start')?.get(orderedIds[0])?.duration || 0;
    for (let i = 1; i < orderedIds.length; i++) {
      total += matrix.get(orderedIds[i - 1])?.get(orderedIds[i])?.duration || 0;
    }
    return total;
  }

  function routeDistanceFromMatrix(orderedIds, matrix) {
    if (orderedIds.length === 0) return 0;
    let total = matrix.get('start')?.get(orderedIds[0])?.distance || 0;
    for (let i = 1; i < orderedIds.length; i++) {
      total += matrix.get(orderedIds[i - 1])?.get(orderedIds[i])?.distance || 0;
    }
    return total;
  }

  // Nearest-neighbor seed using actual road-time matrix.
  function buildRoadTimeRoute(stops, matrix) {
    if (stops.length === 0) return [];
    if (stops.length === 1) return [stops[0].id];
    const remaining = new Set(stops.map(s => s.id));
    const order = [];
    let currentId = 'start';
    while (remaining.size > 0) {
      let nextId = null;
      let nextDur = Infinity;
      const row = matrix.get(currentId);
      for (const id of remaining) {
        const dur = row?.get(id)?.duration;
        if (dur != null && dur < nextDur) { nextDur = dur; nextId = id; }
      }
      if (nextId === null) {
        // Edge missing entirely (shouldn't happen with fallback fill) — pick any.
        nextId = remaining.values().next().value;
      }
      order.push(nextId);
      remaining.delete(nextId);
      currentId = nextId;
    }
    return order;
  }

  // 2-opt improvement on the road-time matrix. Iteratively reverses
  // segments arr[i..j] when doing so lowers total drive duration.
  function improveRouteByTravelTime(orderedIds, matrix) {
    const arr = [...orderedIds];
    if (arr.length < 3) return arr;

    function edgeDur(fromId, toId) {
      return matrix.get(fromId)?.get(toId)?.duration || 0;
    }
    function segmentCost(seg, prevId, nextId) {
      let cost = edgeDur(prevId, seg[0]);
      for (let k = 1; k < seg.length; k++) cost += edgeDur(seg[k - 1], seg[k]);
      if (nextId !== null) cost += edgeDur(seg[seg.length - 1], nextId);
      return cost;
    }

    let improved = true;
    let pass = 0;
    while (improved && pass < 5) {
      improved = false;
      pass++;
      for (let i = 0; i < arr.length - 1; i++) {
        for (let j = i + 1; j < arr.length; j++) {
          const prevId = i === 0 ? 'start' : arr[i - 1];
          const nextId = j + 1 < arr.length ? arr[j + 1] : null;
          const oldSeg = arr.slice(i, j + 1);
          const newSeg = [...oldSeg].reverse();
          // 1-second epsilon to avoid floating churn from rounding.
          if (segmentCost(newSeg, prevId, nextId) + 1 < segmentCost(oldSeg, prevId, nextId)) {
            arr.splice(i, oldSeg.length, ...newSeg);
            improved = true;
          }
        }
      }
    }
    return arr;
  }

  // Total haversine distance for an ordered route, including the home→first leg.
  function totalRouteHaversine(orderedStops, startLat, startLng) {
    if (orderedStops.length === 0) return 0;
    let total = haversineDistance(startLat, startLng, orderedStops[0].lat, orderedStops[0].lng);
    for (let i = 1; i < orderedStops.length; i++) {
      total += haversineDistance(orderedStops[i - 1].lat, orderedStops[i - 1].lng, orderedStops[i].lat, orderedStops[i].lng);
    }
    return total;
  }

  // ────────────────────────────────────────────────────────────
  // PCA-based corridor projection.
  //
  // Real field-service routes follow road corridors and shoreline geography
  // — they're rarely lined up with pure lat or pure lng. Axis-aligned bucket
  // snakes treat the area as a rectangle and produce visible jumps when the
  // real cluster is a diagonal or curved corridor.
  //
  // Instead we:
  //   1. Convert lat/lng to local "miles east, miles north" so axes are
  //      directly comparable.
  //   2. Compute the principal axis of the cluster via 2x2 PCA — that's
  //      the dominant direction the stops actually run along.
  //   3. Project every stop onto that axis to get a corridor coordinate.
  //   4. Sort by corridor coordinate. Forward = start of corridor → end.
  //   5. Build the route in both directions; pick whichever has shorter
  //      total haversine length (this favors starting near home).
  //   6. Run a small adjacent-swap pass: if two neighbors in the projected
  //      order are physically out of place (e.g. side-of-corridor noise),
  //      a swap that reduces the local route length is accepted.
  // ────────────────────────────────────────────────────────────

  // Returns { vx, vy, meanLat, meanLng } where (vx, vy) is a unit vector
  // in local-miles space (vx = east, vy = north) along the principal axis.
  function computePrincipalAxis(stops) {
    if (stops.length < 2) {
      return { vx: 1, vy: 0, meanLat: stops[0]?.lat || 0, meanLng: stops[0]?.lng || 0 };
    }
    const meanLat = stops.reduce((s, p) => s + p.lat, 0) / stops.length;
    const meanLng = stops.reduce((s, p) => s + p.lng, 0) / stops.length;
    const lngScale = miPerLngDeg(meanLat);
    const xs = stops.map(p => (p.lng - meanLng) * lngScale);
    const ys = stops.map(p => (p.lat - meanLat) * MI_PER_LAT_DEG);
    let sxx = 0, sxy = 0, syy = 0;
    for (let i = 0; i < xs.length; i++) {
      sxx += xs[i] * xs[i];
      sxy += xs[i] * ys[i];
      syy += ys[i] * ys[i];
    }
    const n = xs.length;
    sxx /= n; sxy /= n; syy /= n;

    // Larger eigenvalue / eigenvector of the 2x2 symmetric covariance matrix.
    const trace = sxx + syy;
    const det = sxx * syy - sxy * sxy;
    const disc = Math.sqrt(Math.max(0, (trace * trace) / 4 - det));
    const lambda = trace / 2 + disc;
    let vx, vy;
    if (Math.abs(sxy) > 1e-9) {
      vx = lambda - syy;
      vy = sxy;
    } else {
      // Diagonal covariance — pick the axis with larger variance.
      if (sxx >= syy) { vx = 1; vy = 0; } else { vx = 0; vy = 1; }
    }
    const mag = Math.sqrt(vx * vx + vy * vy);
    if (mag < 1e-9) { vx = 1; vy = 0; }
    else { vx /= mag; vy /= mag; }
    return { vx, vy, meanLat, meanLng };
  }

  // Project a stop onto the principal axis.
  // `along` = miles along the corridor, `perp` = miles off it (signed).
  function projectStopOntoAxis(stop, axis) {
    const x = (stop.lng - axis.meanLng) * miPerLngDeg(axis.meanLat);
    const y = (stop.lat - axis.meanLat) * MI_PER_LAT_DEG;
    return {
      along: x * axis.vx + y * axis.vy,
      perp: -x * axis.vy + y * axis.vx,
    };
  }

  // Adjacent-swap improvement pass. Walks the route and swaps neighbors when
  // doing so reduces the local 3-leg cost (prev→a→b→next vs prev→b→a→next).
  // Only neighbors get touched, so the corridor order is preserved.
  function localSwapImprove(route, startLat, startLng) {
    const arr = [...route];
    let improved = true;
    let pass = 0;
    while (improved && pass < 4) {
      improved = false;
      pass++;
      for (let i = 0; i < arr.length - 1; i++) {
        const a = arr[i];
        const b = arr[i + 1];
        const prev = i === 0 ? { lat: startLat, lng: startLng } : arr[i - 1];
        const next = i + 2 < arr.length ? arr[i + 2] : null;
        const orig =
          haversineDistance(prev.lat, prev.lng, a.lat, a.lng) +
          haversineDistance(a.lat, a.lng, b.lat, b.lng) +
          (next ? haversineDistance(b.lat, b.lng, next.lat, next.lng) : 0);
        const swap =
          haversineDistance(prev.lat, prev.lng, b.lat, b.lng) +
          haversineDistance(b.lat, b.lng, a.lat, a.lng) +
          (next ? haversineDistance(a.lat, a.lng, next.lat, next.lng) : 0);
        if (swap < orig - 0.0005) {
          arr[i] = b;
          arr[i + 1] = a;
          improved = true;
        }
      }
    }
    return arr;
  }

  function buildCorridorRoute(stops, startLat, startLng) {
    if (stops.length === 0) return [];
    if (stops.length === 1) return [stops[0].id];

    const axis = computePrincipalAxis(stops);
    const projected = stops.map(s => ({ stop: s, ...projectStopOntoAxis(s, axis) }));

    // Sort primarily by corridor projection. Use absolute perpendicular
    // distance as a small tiebreaker so stops near the corridor centerline
    // come before far-off-corridor stops at the same projection.
    projected.sort((a, b) => {
      if (a.along !== b.along) return a.along - b.along;
      return Math.abs(a.perp) - Math.abs(b.perp);
    });

    const ascendingOrder = projected.map(p => p.stop);
    const descendingOrder = [...ascendingOrder].reverse();

    const ascImproved = localSwapImprove(ascendingOrder, startLat, startLng);
    const descImproved = localSwapImprove(descendingOrder, startLat, startLng);

    const ascLen = totalRouteHaversine(ascImproved, startLat, startLng);
    const descLen = totalRouteHaversine(descImproved, startLat, startLng);
    return (ascLen <= descLen ? ascImproved : descImproved).map(s => s.id);
  }

  // Backwards-compatible alias for the old call site.
  function buildSweepRoute(stops, startLat, startLng) {
    return buildCorridorRoute(stops, startLat, startLng);
  }

  // Calls Google Directions for the chosen order (no optimize:true) so we
  // get accurate legs/polyline/totals for the response.
  async function fetchDirectionsStats(orderedStops, startLat, startLng, GMAPS_KEY) {
    if (!GMAPS_KEY || orderedStops.length === 0) return { totalDistance: 0, totalDuration: 0, legs: [] };
    try {
      const origin = `${startLat},${startLng}`;
      const last = orderedStops[orderedStops.length - 1];
      const middle = orderedStops.slice(0, -1);
      const destination = `${last.lat},${last.lng}`;
      const waypoints = middle.map(s => `${s.lat},${s.lng}`).join('|');
      const url = `https://maps.googleapis.com/maps/api/directions/json?origin=${origin}&destination=${destination}${waypoints ? `&waypoints=${waypoints}` : ''}&key=${GMAPS_KEY}`;
      const res = await fetch(url);
      const data = await res.json();
      if (data.status === 'OK' && data.routes && data.routes[0]) {
        const legs = data.routes[0].legs || [];
        return {
          legs,
          totalDistance: legs.reduce((s, l) => s + (l.distance?.value || 0), 0),
          totalDuration: legs.reduce((s, l) => s + (l.duration?.value || 0), 0),
        };
      }
    } catch (e) { /* fall through */ }
    return { totalDistance: 0, totalDuration: 0, legs: [] };
  }

  // ────────────────────────────────────────────────────────────
  // pickBestForwardRoute — road-time matrix primary, geometric fallback.
  //
  //  1. Pull a Google Distance Matrix for { home, ...stops } (real driving
  //     durations, accounts for one-ways/freeways/bridges).
  //  2. Nearest-neighbor seed using the matrix.
  //  3. 2-opt improvement using matrix durations (segment-reversal accepts
  //     any change that lowers total drive time).
  //  4. Optional Google Directions call (no optimize:true) for stats/legs.
  //
  // Falls back to the geometric PCA corridor route if the matrix is
  // unavailable or unreliable.
  // ────────────────────────────────────────────────────────────
  async function pickBestForwardRoute(stops, startLat, startLng, GMAPS_KEY) {
    if (stops.length < 2) return null;

    // ── 2-stop edge case: closer one first by road time when available ─
    if (stops.length === 2) {
      let ordered = stops;
      if (GMAPS_KEY) {
        const matrix = await buildTravelTimeMatrix(stops, startLat, startLng, GMAPS_KEY);
        if (matrix) {
          const seed = buildRoadTimeRoute(stops, matrix);
          if (seed.length === 2) {
            const stopById = new Map(stops.map(s => [s.id, s]));
            ordered = seed.map(id => stopById.get(id));
          }
        }
      }
      if (ordered === stops) {
        const dA = haversineDistance(startLat, startLng, stops[0].lat, stops[0].lng);
        const dB = haversineDistance(startLat, startLng, stops[1].lat, stops[1].lng);
        ordered = dA <= dB ? stops : [stops[1], stops[0]];
      }
      const stats = await fetchDirectionsStats(ordered, startLat, startLng, GMAPS_KEY);
      return { orderedIds: ordered.map(s => s.id), ...stats };
    }

    const stopById = new Map(stops.map(s => [s.id, s]));

    // ── PRIMARY: road-time matrix optimization ──────────────────────
    if (GMAPS_KEY) {
      try {
        const matrix = await buildTravelTimeMatrix(stops, startLat, startLng, GMAPS_KEY);
        if (matrix) {
          const seedOrder = buildRoadTimeRoute(stops, matrix);
          const optimizedOrder = improveRouteByTravelTime(seedOrder, matrix);
          if (optimizedOrder.length === stops.length) {
            // Use matrix totals as a baseline (already reflects real drive time)
            let totalDuration = routeDurationFromMatrix(optimizedOrder, matrix);
            let totalDistance = routeDistanceFromMatrix(optimizedOrder, matrix);
            let legs = [];

            // Refine with a Directions call so the displayed polyline/legs match.
            const orderedStops = optimizedOrder.map(id => stopById.get(id));
            const stats = await fetchDirectionsStats(orderedStops, startLat, startLng, GMAPS_KEY);
            if (stats.legs.length > 0) {
              legs = stats.legs;
              if (stats.totalDistance > 0) totalDistance = stats.totalDistance;
              if (stats.totalDuration > 0) totalDuration = stats.totalDuration;
            }
            return { orderedIds: optimizedOrder, totalDistance, totalDuration, legs };
          }
        }
        console.error('Road-time matrix unavailable or unreliable; using geometric fallback.');
      } catch (e) {
        console.error('Road-time optimization failed, falling back to geometric:', e.message);
      }
    }

    // ── FALLBACK: PCA corridor route + Directions stats ─────────────
    const orderedIds = buildCorridorRoute(stops, startLat, startLng);
    if (orderedIds.length !== stops.length) return null;
    const orderedStops = orderedIds.map(id => stopById.get(id));
    const stats = await fetchDirectionsStats(orderedStops, startLat, startLng, GMAPS_KEY);
    return { orderedIds, ...stats };
  }

router.get('/api/jobs', async (req, res) => {
  try {
    const { date, status, crew, start_date, end_date, search, limit } = req.query;
    // LEFT JOIN customers so we can decide server-side whether each job has
    // a real street-level address. The frontend uses this to decide whether
    // it's safe to client-geocode (skips bare city/state/zip).
    let query = `SELECT sj.*,
                        c.street AS cust_street, c.city AS cust_city,
                        c.state AS cust_state, c.postal_code AS cust_zip
                 FROM scheduled_jobs sj
                 LEFT JOIN customers c ON sj.customer_id = c.id
                 WHERE 1=1`;
    const params = [];
    let p = 1;
    if (date) { query += ` AND sj.job_date::date = $${p++}::date`; params.push(date); }
    if (start_date && end_date) { query += ` AND sj.job_date::date BETWEEN $${p++}::date AND $${p++}::date`; params.push(start_date, end_date); }
    if (status) { query += ` AND sj.status = $${p++}`; params.push(status); }
    if (crew) { query += ` AND sj.crew_assigned = $${p++}`; params.push(crew); }
    if (search) { query += ` AND (sj.customer_name ILIKE $${p} OR sj.service_type ILIKE $${p} OR sj.address ILIKE $${p})`; params.push(`%${search}%`); p++; }
    query += ' ORDER BY sj.job_date ASC, sj.route_order ASC NULLS LAST';
    if (limit) { query += ` LIMIT $${p++}`; params.push(parseInt(limit)); }
    const result = await pool.query(query, params);
    // Strip the joined customer columns from the response and replace them
    // with two derived fields so the client and backend agree on what to
    // geocode:
    //   has_street_address: true only when a real street number is available
    //     somewhere (customer.street, customer_name, service_type, or a
    //     leading number on job.address).
    //   geocode_address: the exact same string the backend would feed to
    //     Google. The frontend MUST use this when client-geocoding so its
    //     input matches the backend's quality gate. Empty when the job has
    //     no usable street-level address.
    const jobs = result.rows.map(row => {
      const { address, source } = buildBestJobGeocodeAddress(row);
      const { cust_street, cust_city, cust_state, cust_zip, ...clean } = row;
      return {
        ...clean,
        has_street_address: source === 'street',
        geocode_address: source === 'street' ? address : '',
      };
    });
    res.json({ success: true, jobs });
  } catch (error) { serverError(res, error); }
});

router.get('/api/jobs/stats', async (req, res) => {
  try {
    const { date } = req.query;
    let filter = '';
    const params = [];
    if (date) { filter = ' WHERE job_date::date = $1::date'; params.push(date); }
    const [total, byStatus, revenue, byCrew] = await Promise.all([
      pool.query(`SELECT COUNT(*) FROM scheduled_jobs${filter}`, params),
      pool.query(`SELECT status, COUNT(*) FROM scheduled_jobs${filter} GROUP BY status`, params),
      pool.query(`SELECT COALESCE(SUM(service_price), 0) as total FROM scheduled_jobs${filter}`, params),
      pool.query(`SELECT COALESCE(crew_assigned, 'Unassigned') as crew, COUNT(*) FROM scheduled_jobs${filter} GROUP BY crew_assigned`, params)
    ]);
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), byStatus: Object.fromEntries(byStatus.rows.map(r => [r.status, parseInt(r.count)])), totalRevenue: parseFloat(revenue.rows[0].total), byCrew: Object.fromEntries(byCrew.rows.map(r => [r.crew, parseInt(r.count)])) } });
  } catch (error) { serverError(res, error); }
});

router.get('/api/jobs/dashboard', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const [todayCount, weekCount, pending, upcoming] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM scheduled_jobs WHERE job_date::date = $1::date', [today]),
      pool.query("SELECT COUNT(*) FROM scheduled_jobs WHERE job_date::date BETWEEN $1::date AND ($1::date + interval '7 days')", [today]),
      pool.query('SELECT COUNT(*) FROM scheduled_jobs WHERE status = $1 AND job_date::date >= $2::date', ['pending', today]),
      pool.query(`SELECT id, job_date, customer_name, service_type, address, status, service_price FROM scheduled_jobs WHERE job_date::date >= $1::date ORDER BY job_date ASC LIMIT 5`, [today])
    ]);
    res.json({ success: true, stats: { today: parseInt(todayCount.rows[0].count), thisWeek: parseInt(weekCount.rows[0].count), pending: parseInt(pending.rows[0].count) }, upcoming: upcoming.rows });
  } catch (error) { serverError(res, error); }
});

// GET /api/jobs/calendar-summary?month=YYYY-MM - Day-by-day job counts with crew colors (Phase 5)
router.get('/api/jobs/calendar-summary', async (req, res) => {
  try {
    const { month } = req.query;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ success: false, error: 'Provide month as YYYY-MM' });
    }
    const startDate = month + '-01';
    const [year, mon] = month.split('-').map(Number);
    const lastDay = new Date(year, mon, 0).getDate();
    const endDate = month + '-' + String(lastDay).padStart(2, '0');

    const result = await pool.query(
      `SELECT
         job_date::date AS day,
         COUNT(*) AS total_jobs,
         COUNT(*) FILTER (WHERE status = 'completed') AS completed,
         COUNT(*) FILTER (WHERE status = 'pending') AS pending,
         COUNT(*) FILTER (WHERE status IN ('in_progress')) AS in_progress,
         COALESCE(SUM(service_price), 0) AS revenue,
         json_agg(json_build_object(
           'crew', COALESCE(crew_assigned, 'Unassigned'),
           'count', 1
         )) AS crew_details
       FROM scheduled_jobs
       WHERE job_date::date BETWEEN $1::date AND $2::date
       GROUP BY job_date::date
       ORDER BY job_date::date`,
      [startDate, endDate]
    );

    const days = result.rows.map(row => {
      const crewCounts = {};
      (row.crew_details || []).forEach(d => {
        crewCounts[d.crew] = (crewCounts[d.crew] || 0) + d.count;
      });
      return {
        day: row.day,
        total_jobs: parseInt(row.total_jobs),
        completed: parseInt(row.completed),
        pending: parseInt(row.pending),
        in_progress: parseInt(row.in_progress),
        revenue: parseFloat(row.revenue),
        crews: crewCounts
      };
    });

    res.json({ success: true, month, days });
  } catch (error) {
    serverError(res, error);
  }
});

// GET /api/jobs/completed-uninvoiced - Completed jobs without invoices (must be before :id)
router.get('/api/jobs/completed-uninvoiced', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, job_date, customer_name, customer_id, service_type, service_price, address
      FROM scheduled_jobs
      WHERE status IN ('completed', 'done')
        AND (invoice_id IS NULL OR invoice_id = 0)
      ORDER BY job_date DESC LIMIT 200
    `);
    res.json({ success: true, jobs: result.rows });
  } catch (error) {
    console.error('Error fetching completed uninvoiced jobs:', error);
    serverError(res, error);
  }
});

// GET /api/jobs/pipeline - Jobs grouped by pipeline stage (must be before :id route)
router.get('/api/jobs/pipeline', async (req, res) => {
  try {
    const stages = ['new', 'quoted', 'scheduled', 'in_progress', 'completed', 'invoiced'];
    // Map existing statuses to pipeline stages
    const result = await pool.query(`
      SELECT *,
        CASE
          WHEN pipeline_stage IS NOT NULL AND pipeline_stage != '' THEN pipeline_stage
          WHEN status = 'completed' THEN 'completed'
          WHEN status = 'in-progress' THEN 'in_progress'
          WHEN status = 'confirmed' THEN 'scheduled'
          ELSE 'new'
        END as stage
      FROM scheduled_jobs
      WHERE status != 'cancelled'
      ORDER BY job_date DESC
    `);
    const grouped = {};
    stages.forEach(s => grouped[s] = []);
    result.rows.forEach(j => {
      const s = stages.includes(j.stage) ? j.stage : 'new';
      grouped[s].push(j);
    });
    const counts = {};
    stages.forEach(s => counts[s] = grouped[s].length);
    res.json({ success: true, stages, pipeline: grouped, counts });
  } catch (error) { serverError(res, error); }
});

router.get('/api/jobs/:id', async (req, res) => {
  try {
    if (!/^\d+$/.test(req.params.id)) return res.status(400).json({ success: false, error: 'Invalid job ID' });
    const result = await pool.query('SELECT * FROM scheduled_jobs WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.post('/api/jobs', validate(schemas.createJob), async (req, res) => {
  try {
    const { job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned, latitude, longitude } = req.body;
    const result = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING *`,
      [job_date, customer_name, customer_id, service_type, service_frequency, service_price || 0, address, phone, special_notes, property_notes, status || 'pending', route_order, estimated_duration || 30, crew_assigned, latitude, longitude]
    );
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.post('/api/jobs/bulk', async (req, res) => {
  try {
    const { jobs } = req.body;
    if (!jobs || !Array.isArray(jobs)) return res.status(400).json({ success: false, error: 'Must provide jobs array' });
    const created = [], errors = [];
    for (const job of jobs) {
      try {
        const result = await pool.query(
          `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING *`,
          [job.job_date, job.customer_name, job.customer_id, job.service_type, job.service_frequency, job.service_price || 0, job.address, job.phone, job.special_notes, job.property_notes, job.status || 'pending', job.route_order, job.estimated_duration || 30, job.crew_assigned]
        );
        created.push(result.rows[0]);
      } catch (err) { errors.push({ customer: job.customer_name, error: err.message }); }
    }
    res.json({ success: true, created: created.length, errors: errors.length, jobs: created, errorDetails: errors });
  } catch (error) { serverError(res, error); }
});

router.patch('/api/jobs/:id', async (req, res) => {
  try {
    const allowed = ['job_date', 'customer_name', 'service_type', 'service_price', 'address', 'phone', 'special_notes', 'property_notes', 'status', 'route_order', 'crew_assigned', 'completed_at', 'pipeline_stage', 'is_recurring', 'recurring_pattern', 'recurring_day_of_week', 'recurring_start_date', 'recurring_end_date', 'material_cost', 'labor_cost', 'expense_total', 'invoice_id', 'property_id', 'estimated_duration'];
    const sets = [], vals = [];
    let p = 1;
    Object.keys(req.body).forEach(k => { if (allowed.includes(k)) { sets.push(`${k} = $${p++}`); vals.push(req.body[k]); } });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE scheduled_jobs SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.patch('/api/jobs/:id/complete', async (req, res) => {
  try {
    if (!/^\d+$/.test(req.params.id)) {
      return res.status(400).json({ success: false, error: 'Invalid job ID' });
    }

    const actorName = req.body.actor_name || req.body.completed_by || null;
    const job = await transitionScheduledJobStatus({
      jobId: req.params.id,
      nextStatus: 'completed',
      actorName,
      source: req.body.source || 'api',
      completionNotes: req.body.completion_notes,
      completionPhotos: req.body.completion_photos,
      completionLat: req.body.completion_lat,
      completionLng: req.body.completion_lng,
      dispatchIssue: req.body.dispatch_issue,
      dispatchIssueReason: req.body.dispatch_issue_reason,
      poolClient: pool,
      invoiceSideEffectFn: (completedJob, { poolClient }) => applyCompletedJobInvoiceSideEffects(completedJob, {
        poolClient,
        nextInvoiceNumberFn: nextInvoiceNumber,
      }),
    });

    res.json({ success: true, job });
  } catch (error) {
    if (error instanceof JobStatusTransitionError) {
      return res.status(error.statusCode).json({ success: false, error: error.message });
    }
    serverError(res, error, 'Job completion failed');
  }
});

router.patch('/api/jobs/:id/status', async (req, res) => {
  try {
    if (!/^\d+$/.test(req.params.id)) {
      return res.status(400).json({ success: false, error: 'Invalid job ID' });
    }

    const actorName = req.body.actor_name || req.body.completed_by || null;
    const job = await transitionScheduledJobStatus({
      jobId: req.params.id,
      nextStatus: req.body.status,
      actorName,
      source: req.body.source || 'api',
      completionNotes: req.body.completion_notes,
      completionPhotos: req.body.completion_photos,
      completionLat: req.body.completion_lat,
      completionLng: req.body.completion_lng,
      dispatchIssue: req.body.dispatch_issue,
      dispatchIssueReason: req.body.dispatch_issue_reason,
      poolClient: pool,
      invoiceSideEffectFn: (completedJob, { poolClient }) => applyCompletedJobInvoiceSideEffects(completedJob, {
        poolClient,
        nextInvoiceNumberFn: nextInvoiceNumber,
      }),
    });

    res.json({ success: true, job });
  } catch (error) {
    if (error instanceof JobStatusTransitionError) {
      return res.status(error.statusCode).json({ success: false, error: error.message });
    }
    serverError(res, error, 'Job status transition failed');
  }
});

router.post('/api/copilot/dispatch-execution/sync', authenticateToken, async (req, res) => {
  try {
    if (!hasCopilotDispatchSyncAccess(req.user)) {
      return res.status(403).json({ success: false, error: 'Admin access required' });
    }

    const today = new Date().toISOString().slice(0, 10);
    const dateFrom = req.body.date_from || today;
    const dateTo = req.body.date_to || dateFrom;
    if (!isValidIsoDate(dateFrom) || !isValidIsoDate(dateTo)) {
      return res.status(400).json({ success: false, error: 'date_from and date_to must be YYYY-MM-DD' });
    }
    if (dateFrom > dateTo) {
      return res.status(400).json({ success: false, error: 'date_from must be on or before date_to' });
    }

    const dryRun = req.body.dry_run === true;
    const force = req.body.force === true;
    const records = await fetchCopilotDispatchExecutionRecords({
      poolClient: pool,
      dateFrom,
      dateTo,
    });
    const result = await syncCopilotDispatchExecutionRecords({
      records,
      poolClient: pool,
      dryRun,
      force,
    });

    res.json({
      success: true,
      dry_run: dryRun,
      date_from: dateFrom,
      date_to: dateTo,
      ...result,
    });
  } catch (error) {
    serverError(res, error, 'Copilot dispatch execution sync failed');
  }
});

router.patch('/api/jobs/reorder', async (req, res) => {
  try {
    const { jobs } = req.body;
    for (const job of jobs) { await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]); }
    res.json({ success: true, updated: jobs.length });
  } catch (error) { serverError(res, error); }
});

router.post('/api/jobs/optimize-route', async (req, res) => {
  try {
    const { date, crew, jobIds } = req.body;

    // Build the candidate set:
    //   1. If the client passed an explicit jobIds list, optimize only those.
    //   2. Otherwise fall back to date + optional crew filter.
    let jobs;
    if (Array.isArray(jobIds) && jobIds.length > 0) {
      const intIds = jobIds.map(id => parseInt(id)).filter(id => !isNaN(id));
      if (intIds.length < 2) {
        return res.status(400).json({ success: false, error: 'Need at least 2 jobs to optimize' });
      }
      // JOIN customers so the geocoder can build street-level addresses.
      const r = await pool.query(
        `SELECT sj.*,
                c.street AS cust_street, c.city AS cust_city,
                c.state AS cust_state, c.postal_code AS cust_zip
         FROM scheduled_jobs sj
         LEFT JOIN customers c ON sj.customer_id = c.id
         WHERE sj.id = ANY($1::int[]) AND sj.status != 'completed'
         ORDER BY sj.route_order ASC NULLS LAST`,
        [intIds]
      );
      jobs = r.rows;
    } else {
      let query = `SELECT sj.*,
                          c.street AS cust_street, c.city AS cust_city,
                          c.state AS cust_state, c.postal_code AS cust_zip
                   FROM scheduled_jobs sj
                   LEFT JOIN customers c ON sj.customer_id = c.id
                   WHERE sj.job_date::date = $1::date AND sj.status != $2`;
      const params = [date, 'completed'];
      if (crew) { query += ' AND sj.crew_assigned = $3'; params.push(crew); }
      query += ' ORDER BY sj.route_order ASC NULLS LAST';
      const r = await pool.query(query, params);
      jobs = r.rows;
    }

    if (jobs.length < 2) {
      return res.json({ success: true, message: 'Not enough jobs to optimize', jobs, stats: { totalStops: jobs.length, optimized: 0, skipped: 0 } });
    }

    // The optimizer is a READ-ONLY consumer of stored coordinates.
    // POST /api/dispatch/geocode is the ONLY endpoint that writes
    // scheduled_jobs.lat/lng anywhere in the app. Jobs without trusted
    // street-level coords are bucketed into `skippedJobs` so the user can
    // fix them via the "Fix Map Pins" UI before re-running optimize.
    const GMAPS_KEY = process.env.GOOGLE_MAPS_API_KEY;
    const isUsableCoord = (j) =>
      j.lat && j.lng && j.geocode_quality !== 'city' && j.geocode_quality !== 'failed' && j.geocode_quality !== 'no_street';
    const geocodedJobs = jobs.filter(isUsableCoord);
    const skippedJobs = jobs.filter(j => !isUsableCoord(j));
    if (geocodedJobs.length < 2) {
      return res.status(400).json({
        success: false,
        error: 'Not enough geocoded jobs to build a route. Run "Fix Map Pins" on the schedule map view to populate coordinates, or update customer street addresses.',
        skipped: skippedJobs.length,
      });
    }

    // Home base — used as the route's starting point only (forward route, no return).
    let startLat = 41.4268;
    let startLng = -81.7356;
    try {
      const hbResult = await pool.query("SELECT value FROM business_settings WHERE key = 'home_base'");
      if (hbResult.rows.length > 0) {
        const hb = hbResult.rows[0].value;
        if (hb.lat) startLat = parseFloat(hb.lat);
        if (hb.lng) startLng = parseFloat(hb.lng);
      }
    } catch(e) { /* defaults */ }

    // ─── Google Directions: try every job as the endpoint, pick the best ───
    // Forward route, no return-to-origin loop. We compare N candidate routes
    // (one per possible destination) and pick the lowest total drive time.
    if (GMAPS_KEY && geocodedJobs.length >= 2) {
      try {
        const stops = geocodedJobs.map(j => ({
          id: j.id,
          lat: parseFloat(j.lat),
          lng: parseFloat(j.lng),
        }));
        const best = await pickBestForwardRoute(stops, startLat, startLng, GMAPS_KEY);
        if (best && best.orderedIds.length === stops.length) {
          // Map ordered IDs back to full job records and assign route_order.
          const byId = new Map(geocodedJobs.map(j => [j.id, j]));
          const optimizedJobs = best.orderedIds.map((id, i) => ({
            ...byId.get(id),
            route_order: i + 1,
          }));
          // Persist route_order ONLY for the optimized jobs.
          for (const job of optimizedJobs) {
            await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]);
          }
          return res.json({
            success: true,
            message: 'Route optimized via Google Directions',
            jobs: optimizedJobs,
            stats: {
              totalStops: optimizedJobs.length,
              optimized: optimizedJobs.length,
              skipped: skippedJobs.length,
              totalDistance: (best.totalDistance / 1609.34).toFixed(1) + ' miles',
              totalDriveTime: Math.round(best.totalDuration / 60) + ' minutes',
            },
          });
        }
        console.error('Google Directions returned no usable candidate routes; falling back to nearest-neighbor.');
      } catch (e) {
        console.error('Google Directions optimize failed, using fallback:', e.message);
      }
    }

    // ─── Fallback: nearest-neighbor (forward, no return) ──────
    const stops = geocodedJobs.map(j => ({ ...j, lat: parseFloat(j.lat), lng: parseFloat(j.lng) }));
    const visited = new Set();
    const order = [];
    let curLat = startLat, curLng = startLng;
    while (order.length < stops.length) {
      let nearest = null, nearestDist = Infinity;
      for (const s of stops) {
        if (visited.has(s.id)) continue;
        const d = haversineDistance(curLat, curLng, s.lat, s.lng);
        if (d < nearestDist) { nearestDist = d; nearest = s; }
      }
      if (!nearest) break;
      visited.add(nearest.id);
      order.push(nearest);
      curLat = nearest.lat; curLng = nearest.lng;
    }
    const optimizedJobs = order.map((j, i) => ({ ...j, route_order: i + 1 }));
    for (const job of optimizedJobs) {
      await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]);
    }
    res.json({
      success: true,
      message: 'Route optimized (nearest-neighbor)',
      jobs: optimizedJobs,
      stats: {
        totalStops: optimizedJobs.length,
        optimized: optimizedJobs.length,
        skipped: skippedJobs.length,
      },
    });
  } catch (error) { serverError(res, error); }
});

router.delete('/api/jobs/bulk', async (req, res) => {
  try {
    const { ids } = req.body;
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ success: false, error: 'ids must be a non-empty array' });
    }
    const intIds = ids.map(id => parseInt(id)).filter(id => !isNaN(id));
    if (intIds.length === 0) {
      return res.status(400).json({ success: false, error: 'No valid job IDs provided' });
    }
    const result = await pool.query('DELETE FROM scheduled_jobs WHERE id = ANY($1::int[]) RETURNING id', [intIds]);
    res.json({ success: true, deleted: result.rowCount });
  } catch (error) { serverError(res, error, 'Bulk delete jobs'); }
});

router.delete('/api/jobs/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM scheduled_jobs WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// CREWS ENDPOINTS
// ═══════════════════════════════════════════════════════════

router.get('/api/crews', async (req, res) => {
  try {
    const { active_only } = req.query;
    let query = 'SELECT * FROM crews';
    if (active_only === 'true') query += ' WHERE is_active = true';
    query += ' ORDER BY name ASC';
    const result = await pool.query(query);
    res.json({ success: true, crews: result.rows });
  } catch (error) { serverError(res, error); }
});

router.post('/api/crews', validate(schemas.createCrew), async (req, res) => {
  try {
    const { name, members, crew_type, notes } = req.body;
    const result = await pool.query('INSERT INTO crews (name, members, crew_type, notes) VALUES ($1, $2, $3, $4) RETURNING *', [name, members, crew_type, notes]);
    res.json({ success: true, crew: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.patch('/api/crews/:id', async (req, res) => {
  try {
    const { name, members, crew_type, notes, is_active } = req.body;
    const sets = [], vals = [];
    let p = 1;
    if (name !== undefined) { sets.push(`name = $${p++}`); vals.push(name); }
    if (members !== undefined) { sets.push(`members = $${p++}`); vals.push(members); }
    if (crew_type !== undefined) { sets.push(`crew_type = $${p++}`); vals.push(crew_type); }
    if (notes !== undefined) { sets.push(`notes = $${p++}`); vals.push(notes); }
    if (is_active !== undefined) { sets.push(`is_active = $${p++}`); vals.push(is_active); }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE crews SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, crew: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.delete('/api/crews/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM crews WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.post('/api/import-scheduling', upload.single('csvfile'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No CSV file' });
    const csvContent = req.file.buffer.toString('utf-8');
    const lines = csvContent.split('\n');
    const headers = parseCSVLine(lines[0]);
    
    const jobs = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      try {
        const values = parseCSVLine(line);
        const job = {};
        headers.forEach((h, idx) => { job[h] = values[idx] || ''; });
        job.__row_index = i;
        jobs.push(job);
      } catch (e) {}
    }
    
    function parseDate(dateStr) {
      if (!dateStr) return null;
      let cleaned = dateStr.replace(/^="?"?/, '').replace(/"?"?$/, '').trim();
      try { const d = new Date(cleaned); return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0]; } catch { return null; }
    }
    
    function parseNameAddress(details) {
      if (!details) return { name: '', address: '' };
      // Format: "FirstName LastName 1234 Street Name  City State Zip, Country"
      // Split on 2+ spaces first to separate street from city
      const parts = details.split(/\s{2,}/);
      const nameAndStreet = parts[0] || '';
      const cityStateZip = parts.slice(1).join(' ').trim();
      // Split name from street: name ends before the first house number (digits)
      const match = nameAndStreet.match(/^(.*?)\s+(\d+\s+.*)$/);
      if (match) {
        const street = cityStateZip ? match[2] + ', ' + cityStateZip : match[2];
        return { name: match[1].trim(), address: street };
      }
      return { name: nameAndStreet.trim(), address: cityStateZip };
    }
    
    let skipped = 0;
    const importRows = [];
    for (const job of jobs) {
      const jobDate = parseDate(job['Date of Service']);
      if (!jobDate) {
        skipped += 1;
        continue;
      }
      const { name, address } = parseNameAddress(job['Name / Details']);
      const rawTitle = job['Title'] || 'Service';
      const crewMatch = rawTitle.match(/^(.+?)\s{1,2}(\w+\s+(?:Mowing|Cleanup|Lawn|Landscape|Plow|Snow)\s+Crew)$/i);
      importRows.push({
        source_row: job.__row_index || null,
        job_date: jobDate,
        customer_name: name,
        address,
        raw_title: rawTitle.trim(),
        service_type: crewMatch ? crewMatch[1].trim() : rawTitle.trim(),
        crew_assigned: crewMatch ? crewMatch[2].trim() : null,
        service_price: parseFloat((job['Visit Total'] || '0').replace(/[^0-9.]/g, '')) || 0,
      });
    }

    const uniqueSyncDates = [...new Set(importRows.map((row) => row.job_date).filter(isValidIsoDate))].sort();
    const copilotHydration = await hydrateCopilotScheduleDates({
      syncDates: uniqueSyncDates,
      poolClient: pool,
      fetchImpl,
    });
    const linkagePlan = planCopilotImportLinkage(importRows, copilotHydration.rowsByDate);
    for (const diagnostic of linkagePlan.diagnostics) {
      console.warn(
        `[import-scheduling] Copilot linkage ${diagnostic.status} for row ${diagnostic.row_index} (${diagnostic.job_date} ${diagnostic.customer_name || 'Unknown'}): ${diagnostic.reason}`
      );
    }

    let imported = 0, updated = 0;
    const importedJobs = [];
    const linkageSummary = {
      attempted: linkagePlan.summary.attempted,
      linked: 0,
      already_linked: 0,
      unmatched: 0,
      ambiguous: 0,
      conflict: 0,
    };
    const linkageDiagnostics = [];

    for (const [rowIndex, row] of importRows.entries()) {
      try {
        const jobDate = row.job_date;
        const name = row.customer_name;
        const address = row.address;
        const serviceType = row.service_type;
        const crewAssigned = row.crew_assigned;
        const price = row.service_price;

        // Try to match customer by name
        const nameParts = name.split(' ');
        const firstName = nameParts[0] || '';
        const lastName = nameParts.slice(1).join(' ') || '';
        let customerId = null, customerPhone = null, customerMobile = null;

        const custMatch = await pool.query(
          `SELECT id, phone, mobile FROM customers
           WHERE LOWER(TRIM(name)) = LOWER($1)
              OR (LOWER(TRIM(first_name)) = LOWER($2) AND LOWER(TRIM(last_name)) = LOWER($3))
           LIMIT 1`,
          [name, firstName, lastName]
        );
        if (custMatch.rows.length > 0) {
          customerId = custMatch.rows[0].id;
          customerPhone = custMatch.rows[0].phone;
          customerMobile = custMatch.rows[0].mobile;
        }

        // Check for existing by date + customer (ignore old service_type with crew name)
        const existing = await pool.query(
          'SELECT id, copilot_visit_id, copilot_job_id FROM scheduled_jobs WHERE job_date = $1 AND customer_name = $2',
          [jobDate, name]
        );
        let jobId;
        let currentJobRow = null;
        if (existing.rows.length > 0) {
          jobId = existing.rows[0].id;
          currentJobRow = existing.rows[0];
          await pool.query('UPDATE scheduled_jobs SET address = $1, service_price = $2, customer_id = COALESCE($3, customer_id), service_type = $4, crew_assigned = COALESCE($5, crew_assigned) WHERE id = $6', [address, price, customerId, serviceType, crewAssigned, jobId]);
          updated++;
        } else {
          const insertResult = await pool.query(
            'INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_price, address, status, crew_assigned) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, copilot_visit_id, copilot_job_id',
            [jobDate, name, customerId, serviceType, price, address, 'pending', crewAssigned]
          );
          jobId = insertResult.rows[0].id;
          currentJobRow = insertResult.rows[0];
          imported++;
        }

        const rowPlan = linkagePlan.plan[rowIndex];
        const linkageResult = await applyCopilotImportLinkage({
          scheduledJobId: jobId,
          currentJob: currentJobRow,
          linkagePlan: rowPlan,
          poolClient: pool,
        });
        if (linkageResult.status === 'linked') linkageSummary.linked += 1;
        else if (linkageResult.status === 'already_linked') linkageSummary.already_linked += 1;
        else if (linkageResult.status === 'conflict') linkageSummary.conflict += 1;
        else if (linkageResult.status === 'ambiguous') linkageSummary.ambiguous += 1;
        else linkageSummary.unmatched += 1;

        if (linkageResult.status !== 'linked' && linkageResult.status !== 'already_linked') {
          linkageDiagnostics.push({
            row_index: rowPlan?.row_index || row.source_row || null,
            job_date: jobDate,
            customer_name: name,
            service_type: serviceType,
            address,
            status: linkageResult.status,
            reason: linkageResult.reason,
            candidate_visit_ids: rowPlan?.candidate_visit_ids || [],
          });
        }

        importedJobs.push({
          id: jobId,
          job_date: jobDate,
          customer_name: name,
          customer_id: customerId,
          service_type: serviceType,
          service_price: price,
          address,
          phone: customerMobile || customerPhone || null,
          copilot_visit_id: linkageResult.copilot_visit_id,
          copilot_job_id: linkageResult.copilot_job_id,
          copilot_linkage_status: linkageResult.status,
          copilot_linkage_reason: linkageResult.reason,
        });
      } catch (e) { skipped++; }
    }
    res.json({
      success: true,
      message: 'Import complete',
      stats: { total: jobs.length, imported, updated, skipped },
      copilot_hydration: copilotHydration.summary,
      copilot_linkage: {
        ...linkageSummary,
        diagnostics: linkageDiagnostics,
      },
      jobs: importedJobs,
    });
  } catch (error) { serverError(res, error); }
});

router.get('/api/jobs/recurring', async (req, res) => {
  try {
    // Check if is_recurring column exists before querying
    const colCheck = await pool.query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'scheduled_jobs' AND column_name = 'is_recurring'`);
    if (colCheck.rows.length === 0) {
      return res.json({ success: true, jobs: [] });
    }
    const result = await pool.query(`SELECT * FROM scheduled_jobs WHERE is_recurring = true ORDER BY customer_name ASC`);
    res.json({ success: true, jobs: result.rows });
  } catch (error) { serverError(res, error); }
});

// PATCH /api/jobs/:id/recurring - Configure recurring pattern
router.patch('/api/jobs/:id/recurring', async (req, res) => {
  try {
    const { is_recurring, recurring_pattern, recurring_end_date } = req.body;
    const result = await pool.query(
      `UPDATE scheduled_jobs SET is_recurring = COALESCE($1, is_recurring), recurring_pattern = COALESCE($2, recurring_pattern), recurring_end_date = COALESCE($3, recurring_end_date), updated_at = NOW() WHERE id = $4 RETURNING *`,
      [is_recurring, recurring_pattern, recurring_end_date, req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Job not found' });
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.get('/api/dispatch/board', async (req, res) => {
  try {
    const { date, view = 'day' } = req.query;
    const targetDate = date || new Date().toISOString().split('T')[0];
    let startDate = null;
    let endDate = null;
    if (view === 'week') {
      // Get Monday of the week
      const d = new Date(targetDate);
      const day = d.getDay();
      const monday = new Date(d);
      monday.setDate(d.getDate() - (day === 0 ? 6 : day - 1));
      const sunday = new Date(monday);
      sunday.setDate(monday.getDate() + 6);
      startDate = monday.toISOString().split('T')[0];
      endDate = sunday.toISOString().split('T')[0];
    } else {
      startDate = targetDate;
      endDate = targetDate;
    }

    let livePayload = await getCopilotLiveJobs({
      poolClient: pool,
      date: startDate === endDate ? targetDate : null,
      startDate,
      endDate,
      fetchImpl,
    });
    const autoApply = await maybeAutoApplyCanonicalTemplates({
      targetDate,
      startDate,
      endDate,
      livePayload,
    });
    livePayload = autoApply.livePayload;
    const crews = await pool.query('SELECT * FROM crews ORDER BY name');
    const payload = buildDispatchBoardPayload({
      targetDate,
      view,
      jobs: livePayload.jobs || [],
      crews: crews.rows || [],
      freshness: livePayload.freshness || null,
    });
    payload.canonical_template_statuses = autoApply.statuses;
    res.json(payload);
  } catch (error) { serverError(res, error); }
});

router.get('/api/dispatch/route-sheet-export', async (req, res) => {
  try {
    const targetDate = req.query.date || new Date().toISOString().split('T')[0];
    const visibleCrews = String(req.query.visible_crews || '')
      .split(',')
      .map((value) => String(value || '').trim())
      .filter(Boolean);
    const visibleSet = new Set(visibleCrews);

    let livePayload = await getCopilotLiveJobs({
      poolClient: pool,
      date: targetDate,
      startDate: targetDate,
      endDate: targetDate,
      fetchImpl,
    });
    const autoApply = await maybeAutoApplyCanonicalTemplates({
      targetDate,
      startDate: targetDate,
      endDate: targetDate,
      livePayload,
    });
    livePayload = autoApply.livePayload;
    const crews = await pool.query('SELECT * FROM crews ORDER BY name');
    const boardPayload = buildDispatchBoardPayload({
      targetDate,
      view: 'day',
      jobs: livePayload.jobs || [],
      crews: crews.rows || [],
      freshness: livePayload.freshness || null,
    });

    const groups = [];
    for (const crew of boardPayload.crews || []) {
      if (visibleSet.size > 0 && !visibleSet.has(crew.name)) continue;
      const jobs = (crew.jobs || []).slice().sort(stableRouteOrderCompare);
      if (jobs.length === 0) continue;
      groups.push({ crewName: crew.name, jobs });
    }
    if ((boardPayload.unassigned || []).length > 0 && (visibleSet.size === 0 || visibleSet.has('__unassigned'))) {
      groups.push({
        crewName: 'Unassigned',
        jobs: (boardPayload.unassigned || []).slice().sort(stableRouteOrderCompare),
      });
    }

    if (groups.length === 0) {
      return res.status(404).json({ success: false, error: 'No visible routes available for export' });
    }

    const workbookXml = buildDispatchRouteSheetWorkbookXml({
      targetDate,
      groups,
    });
    res.setHeader('Content-Type', 'application/vnd.ms-excel; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="dispatch-route-sheets-${targetDate}.xml"`);
    res.status(200).send(workbookXml);
  } catch (error) { serverError(res, error); }
});

router.patch('/api/dispatch/route-order', async (req, res) => {
  try {
    const { date, crew_name, ordered_job_keys } = req.body;
    if (!date || !crew_name || !Array.isArray(ordered_job_keys) || ordered_job_keys.length === 0) {
      return res.status(400).json({ success: false, error: 'date, crew_name, and ordered_job_keys are required' });
    }

    const actor = getDispatchActor(req);
    const liveJobs = await fetchLiveDispatchJobsForDate(date);
    const crewJobs = getCrewDispatchJobs(liveJobs, crew_name);
    const expectedKeys = crewJobs.map((job) => normalizeLiveDispatchJobKey(job.job_key || job.id)).filter(Boolean);
    const submittedKeys = ordered_job_keys.map((jobKey) => normalizeLiveDispatchJobKey(jobKey)).filter(Boolean);

    if (expectedKeys.length === 0) {
      return res.status(404).json({ success: false, error: 'No live jobs found for that crew and date' });
    }
    if (submittedKeys.length !== ordered_job_keys.length) {
      return res.status(400).json({ success: false, error: 'ordered_job_keys must be live Copilot job keys' });
    }
    if (expectedKeys.length !== submittedKeys.length) {
      return res.status(400).json({ success: false, error: 'ordered_job_keys must include every current live stop for this crew and date' });
    }

    const expectedSet = new Set(expectedKeys);
    const submittedSet = new Set(submittedKeys);
    if (submittedSet.size !== submittedKeys.length || expectedKeys.some((key) => !submittedSet.has(key)) || submittedKeys.some((key) => !expectedSet.has(key))) {
      return res.status(400).json({ success: false, error: 'ordered_job_keys do not match the current live crew job set' });
    }

    const updated = await patchDispatchPlanItems(pool, {
      patches: submittedKeys.map((jobKey, index) => ({
        jobKey,
        patch: { route_order_override: index + 1 },
      })),
      ...actor,
    });

    res.json({
      success: true,
      crew_name,
      date,
      updated: updated.length,
      ordered: submittedKeys.map((jobKey, index) => ({ job_key: jobKey, route_order_override: index + 1 })),
    });
  } catch (error) { serverError(res, error); }
});

router.post('/api/dispatch/reverse-route', async (req, res) => {
  try {
    const { date, crew_name } = req.body;
    if (!date || !crew_name) {
      return res.status(400).json({ success: false, error: 'date and crew_name are required' });
    }

    const actor = getDispatchActor(req);
    const liveJobs = await fetchLiveDispatchJobsForDate(date);
    const crewJobs = getCrewDispatchJobs(liveJobs, crew_name);
    if (crewJobs.length < 2) {
      return res.json({ success: true, crew_name, date, updated: 0, ordered: [] });
    }

    const reversedKeys = crewJobs
      .map((job) => normalizeLiveDispatchJobKey(job.job_key || job.id))
      .filter(Boolean)
      .reverse();

    const updated = await patchDispatchPlanItems(pool, {
      patches: reversedKeys.map((jobKey, index) => ({
        jobKey,
        patch: { route_order_override: index + 1 },
      })),
      ...actor,
    });

    res.json({
      success: true,
      crew_name,
      date,
      updated: updated.length,
      ordered: reversedKeys.map((jobKey, index) => ({ job_key: jobKey, route_order_override: index + 1 })),
    });
  } catch (error) { serverError(res, error); }
});

// PATCH /api/dispatch/assign - Persist crew / route overrides for live Dispatch jobs.
router.patch('/api/dispatch/assign', async (req, res) => {
  try {
    const { assignments } = req.body;
    if (!assignments || !Array.isArray(assignments)) return res.status(400).json({ success: false, error: 'assignments array required' });
    const actor = getDispatchActor(req);
    const patches = [];

    for (const assignment of assignments) {
      const jobKey = normalizeLiveDispatchJobKey(
        assignment.job_key || assignment.jobId || assignment.job_id || assignment.id
      );
      if (!jobKey) {
        return res.status(400).json({ success: false, error: 'Live Dispatch assignments require a Copilot job_key' });
      }
      if (assignment.status !== undefined || assignment.job_date !== undefined) {
        return res.status(409).json({
          success: false,
          error: 'Live Dispatch assignments only support crew and route overrides. Status and date remain source-of-truth fields from Copilot.',
        });
      }

      const patch = {};
      if (Object.prototype.hasOwnProperty.call(assignment, 'crew_assigned')) patch.crew_override_name = assignment.crew_assigned;
      if (Object.prototype.hasOwnProperty.call(assignment, 'route_order')) patch.route_order_override = assignment.route_order;
      if (Object.keys(patch).length === 0) continue;
      patches.push({ jobKey, patch });
    }

    const updated = await patchDispatchPlanItems(pool, {
      patches,
      ...actor,
    });

    res.json({
      success: true,
      updated: updated.length,
      jobs: updated.map((entry) => entry.dispatchJob),
    });
  } catch (error) { serverError(res, error); }
});

// GET /api/dispatch/crew-availability - Crew workload summary
router.get('/api/dispatch/crew-availability', async (req, res) => {
  try {
    const date = req.query.date || new Date().toISOString().split('T')[0];
    const result = await pool.query(`
      SELECT crew_assigned as crew_name, COUNT(*) as job_count, COALESCE(SUM(estimated_duration), 0) / 60.0 as total_hours
      FROM scheduled_jobs WHERE job_date::date = $1::date AND crew_assigned IS NOT NULL
      GROUP BY crew_assigned ORDER BY crew_assigned
    `, [date]);
    res.json({ success: true, date, crews: result.rows });
  } catch (error) { serverError(res, error); }
});

// POST /api/dispatch/geocode - Geocode live Dispatch jobs into dispatch_plan_items.
//
// Body:
//   jobKeys: string[]  → geocode these exact live jobs. Implies force-clear of
//                        their dispatch-plan map overrides before re-geocoding.
//   jobKey: string     → single-job alias for jobKeys.
//   date: 'YYYY-MM-DD' (default: today) → date-mode scope.
//   force: bool        → re-geocode the whole in-scope live job set even when
//                        map pins already exist in the dispatch plan.
router.post('/api/dispatch/geocode', async (req, res) => {
  try {
    const { date, force, jobKey, jobKeys } = req.body;
    const targetDate = date || new Date().toISOString().split('T')[0];
    const actor = getDispatchActor(req);
    const explicitJobKeys = Array.isArray(jobKeys)
      ? jobKeys.map((value) => normalizeLiveDispatchJobKey(value)).filter(Boolean)
      : (normalizeLiveDispatchJobKey(jobKey) ? [normalizeLiveDispatchJobKey(jobKey)] : null);
    const isExplicit = !!(explicitJobKeys && explicitJobKeys.length > 0);

    let jobs = [];
    if (isExplicit) {
      const resolved = await fetchResolvedLiveJobs(pool, { jobKeys: explicitJobKeys, includeDeleted: false });
      jobs = resolved.map((job) => mapResolvedLiveJobToScheduleJob(job));
    } else {
      const livePayload = await getCopilotLiveJobs({
        poolClient: pool,
        date: targetDate,
        startDate: targetDate,
        endDate: targetDate,
        fetchImpl,
      });
      jobs = livePayload.jobs || [];
    }

    const targetJobs = jobs.filter((job) => {
      if (job?.hold_from_dispatch || job?.source_deleted) return false;
      if (!job?.address) return false;
      if (force === true || isExplicit) return true;
      return job.lat == null || job.lng == null;
    });

    let geocoded = 0;
    const GMAPS_KEY = process.env.GOOGLE_MAPS_API_KEY;
    let skippedNoStreet = 0;
    let failed = 0;
    for (const job of targetJobs) {
      try {
        const { address: fullAddress, source: addressSource } = buildBestJobGeocodeAddress(job);
        if (addressSource !== 'street' || !fullAddress) {
          await patchDispatchPlanItems(pool, {
            patches: [{
              jobKey: normalizeLiveDispatchJobKey(job.job_key || job.id),
              patch: {
                map_lat: null,
                map_lng: null,
                map_source: 'geocoded',
                map_quality: 'missing',
                map_address_input: fullAddress || job.address || null,
              },
            }],
            ...actor,
          });
          skippedNoStreet++;
          continue;
        }

        const q = encodeURIComponent(fullAddress);
        if (GMAPS_KEY) {
          const gRes = await fetchImpl(`https://maps.googleapis.com/maps/api/geocode/json?address=${q}&key=${GMAPS_KEY}`);
          const gData = await gRes.json();
          if (gData.status === 'OK' && gData.results && gData.results.length > 0) {
            const result = gData.results[0];
            if (isStreetLevelGoogleResult(result)) {
              const loc = result.geometry.location;
              await patchDispatchPlanItems(pool, {
                patches: [{
                  jobKey: normalizeLiveDispatchJobKey(job.job_key || job.id),
                  patch: {
                    map_lat: loc.lat,
                    map_lng: loc.lng,
                    map_source: 'geocoded',
                    map_quality: 'street',
                    map_address_input: fullAddress,
                  },
                }],
                ...actor,
              });
              geocoded++;
            } else {
              await patchDispatchPlanItems(pool, {
                patches: [{
                  jobKey: normalizeLiveDispatchJobKey(job.job_key || job.id),
                  patch: {
                    map_lat: null,
                    map_lng: null,
                    map_source: 'geocoded',
                    map_quality: 'ambiguous',
                    map_address_input: fullAddress,
                  },
                }],
                ...actor,
              });
            }
          } else {
            await patchDispatchPlanItems(pool, {
              patches: [{
                jobKey: normalizeLiveDispatchJobKey(job.job_key || job.id),
                patch: {
                  map_lat: null,
                  map_lng: null,
                  map_source: 'geocoded',
                  map_quality: 'failed',
                  map_address_input: fullAddress,
                },
              }],
              ...actor,
            });
            failed++;
          }
        } else {
          const gRes = await fetchImpl(`https://nominatim.openstreetmap.org/search?format=json&q=${q}&limit=1&countrycodes=us`);
          const gData = await gRes.json();
          if (gData && gData.length > 0) {
            await patchDispatchPlanItems(pool, {
              patches: [{
                jobKey: normalizeLiveDispatchJobKey(job.job_key || job.id),
                patch: {
                  map_lat: parseFloat(gData[0].lat),
                  map_lng: parseFloat(gData[0].lon),
                  map_source: 'geocoded',
                  map_quality: 'street',
                  map_address_input: fullAddress,
                },
              }],
              ...actor,
            });
            geocoded++;
          } else {
            await patchDispatchPlanItems(pool, {
              patches: [{
                jobKey: normalizeLiveDispatchJobKey(job.job_key || job.id),
                patch: {
                  map_lat: null,
                  map_lng: null,
                  map_source: 'geocoded',
                  map_quality: 'failed',
                  map_address_input: fullAddress,
                },
              }],
              ...actor,
            });
            failed++;
          }
          await new Promise(r => setTimeout(r, 1100));
        }
      } catch (e) { failed += 1; }
    }

    res.json({
      success: true,
      geocoded,
      skippedNoStreet,
      failed,
      cleared: 0,
      forceCleared: 0,
      suspiciousCleared: 0,
      ambiguousCleared: 0,
      total: targetJobs.length,
    });
  } catch (error) { serverError(res, error); }
});

// extractStreetAddress moved to top of closure as a shared helper.

// POST /api/dispatch/optimize-route - Optimize live route order for a crew
router.post('/api/dispatch/optimize-route', async (req, res) => {
  try {
    const { date, crew_name, start_lat, start_lng } = req.body;
    if (!date || !crew_name) return res.status(400).json({ success: false, error: 'date and crew_name required' });
    const actor = getDispatchActor(req);
    const crewJobs = getCrewDispatchJobs(await fetchLiveDispatchJobsForDate(date), crew_name)
      .filter((job) => job.lat != null && job.lng != null);

    if (crewJobs.length === 0) return res.json({ success: true, message: 'No geocoded jobs found for this crew', optimized: [] });

    const stops = crewJobs.map((job) => ({
      id: normalizeLiveDispatchJobKey(job.job_key || job.id),
      lat: parseFloat(job.lat),
      lng: parseFloat(job.lng),
      duration: parseInt(job.estimated_duration, 10) || 30,
    }));
    // Get home base from settings (default: Pappas HQ)
    let defaultLat = 41.4268, defaultLng = -81.7356;
    try {
      const hbResult = await pool.query("SELECT value FROM business_settings WHERE key = 'home_base'");
      if (hbResult.rows.length > 0) {
        const hb = hbResult.rows[0].value;
        if (hb.lat) defaultLat = parseFloat(hb.lat);
        if (hb.lng) defaultLng = parseFloat(hb.lng);
      }
    } catch(e) { /* use defaults */ }
    const sLat = start_lat ? parseFloat(start_lat) : defaultLat;
    const sLng = start_lng ? parseFloat(start_lng) : defaultLng;

    const GMAPS_KEY = process.env.GOOGLE_MAPS_API_KEY;
    if (GMAPS_KEY && stops.length >= 2) {
      try {
        // Use the shared best-forward-route picker so this endpoint matches
        // /api/jobs/optimize-route (no destination = origin loop).
        const best = await pickBestForwardRoute(stops, sLat, sLng, GMAPS_KEY);
        if (best && best.orderedIds.length === stops.length) {
          await patchDispatchPlanItems(pool, {
            patches: best.orderedIds.map((id, index) => ({
              jobKey: id,
              patch: { route_order_override: index + 1 },
            })),
            ...actor,
          });
          return res.json({
            success: true,
            optimized: best.orderedIds.map((id, i) => ({ job_key: id, job_id: id, route_order: i + 1 })),
            stats: {
              totalDistance: (best.totalDistance / 1609.34).toFixed(1) + ' miles',
              totalDriveTime: Math.round(best.totalDuration / 60) + ' minutes',
            },
          });
        }
      } catch (e) { console.error('Google Directions optimize failed, using fallback:', e.message); }
    }

    // Fallback: nearest-neighbor TSP
    const visited = new Set();
    const order = [];
    let curLat = sLat, curLng = sLng;
    while (order.length < stops.length) {
      let nearest = null, nearestDist = Infinity;
      for (const s of stops) {
        if (visited.has(s.id)) continue;
        const d = haversine(curLat, curLng, s.lat, s.lng);
        if (d < nearestDist) { nearestDist = d; nearest = s; }
      }
      if (!nearest) break;
      visited.add(nearest.id);
      order.push(nearest.id);
      curLat = nearest.lat; curLng = nearest.lng;
    }
    await patchDispatchPlanItems(pool, {
      patches: order.map((id, index) => ({
        jobKey: id,
        patch: { route_order_override: index + 1 },
      })),
      ...actor,
    });
    res.json({ success: true, optimized: order.map((id, i) => ({ job_key: id, job_id: id, route_order: i + 1 })) });
  } catch (error) { serverError(res, error); }
});

router.get('/api/dispatch/route-templates', async (req, res) => {
  try {
    const { date = null, crew_name = null, include_inactive = 'false' } = req.query;
    const includeInactive = String(include_inactive) === 'true';
    const result = await pool.query(
      `SELECT drt.*, COUNT(drts.id) AS stop_count
         FROM dispatch_route_templates drt
         LEFT JOIN dispatch_route_template_stops drts ON drts.template_id = drt.id
        WHERE ($1::text IS NULL OR drt.crew_name = $1)
          AND ($2::boolean OR drt.active = true)
        GROUP BY drt.id
        ORDER BY drt.crew_name ASC, drt.name ASC`,
      [crew_name || null, includeInactive]
    );

    res.json({
      success: true,
      templates: result.rows.map((row) => summarizeDispatchRouteTemplate(row, row.stop_count, { targetDate: date || null })),
    });
  } catch (error) { serverError(res, error); }
});

router.post('/api/dispatch/route-templates/save-from-live', async (req, res) => {
  const client = await pool.connect();
  try {
    const { date, crew_name, name, cadence = 'weekly', anchor_date = null, notes = null } = req.body;
    if (!date || !crew_name || !name) {
      return res.status(400).json({ success: false, error: 'date, crew_name, and name are required' });
    }
    if (!['weekly', 'biweekly'].includes(cadence)) {
      return res.status(400).json({ success: false, error: 'cadence must be weekly or biweekly' });
    }
    const anchorDate = anchor_date || date;
    if (!isValidIsoDate(date) || !isValidIsoDate(anchorDate)) {
      return res.status(400).json({ success: false, error: 'date and anchor_date must be YYYY-MM-DD values' });
    }

    const actor = getDispatchActor(req);
    const liveJobs = await fetchLiveDispatchJobsForDate(date);
    const crewJobs = getCrewDispatchJobs(liveJobs, crew_name);
    if (crewJobs.length === 0) {
      return res.status(404).json({ success: false, error: 'No live jobs found for that crew and date' });
    }

    await client.query('BEGIN');
    const templateResult = await client.query(
      `INSERT INTO dispatch_route_templates (
         name, crew_name, cadence, anchor_date, day_of_week, active, notes,
         created_by_user_id, created_by_name, updated_by_user_id, updated_by_name
       ) VALUES ($1, $2, $3, $4::date, $5, true, $6, $7, $8, $7, $8)
       RETURNING *`,
      [
        String(name).trim(),
        String(crew_name).trim(),
        cadence,
        anchorDate,
        new Date(`${anchorDate}T00:00:00.000Z`).getUTCDay(),
        notes || null,
        actor.updatedByUserId,
        actor.updatedByName,
      ]
    );
    const template = templateResult.rows[0];

    for (const [index, job] of crewJobs.entries()) {
      const stop = buildDispatchRouteTemplateStop(job, index + 1);
      await client.query(
        `INSERT INTO dispatch_route_template_stops (
           template_id, position, source_customer_id, customer_link_id, property_link_id,
           customer_name, address_fingerprint, service_title, service_frequency, source_event_type
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [
          template.id,
          stop.position,
          stop.source_customer_id,
          stop.customer_link_id,
          stop.property_link_id,
          stop.customer_name,
          stop.address_fingerprint,
          stop.service_title,
          stop.service_frequency,
          stop.source_event_type,
        ]
      );
    }
    await client.query('COMMIT');

    res.json({
      success: true,
      template: summarizeDispatchRouteTemplate(template, crewJobs.length, { targetDate: date }),
    });
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    serverError(res, error);
  } finally {
    client.release();
  }
});

router.post('/api/dispatch/route-templates/:id/apply', async (req, res) => {
  try {
    const templateId = Number.parseInt(req.params.id, 10);
    const { date, force = false } = req.body;
    if (!Number.isFinite(templateId) || !date) {
      return res.status(400).json({ success: false, error: 'template id and date are required' });
    }
    if (!isValidIsoDate(date)) {
      return res.status(400).json({ success: false, error: 'date must be YYYY-MM-DD' });
    }

    const templateResult = await pool.query(
      `SELECT * FROM dispatch_route_templates WHERE id = $1`,
      [templateId]
    );
    if (templateResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Route template not found' });
    }
    const template = templateResult.rows[0];
    if (!template.active) {
      return res.status(409).json({ success: false, error: 'Route template is inactive' });
    }
    if (!force && !isDispatchRouteTemplateApplicable(template, date)) {
      return res.status(409).json({ success: false, error: 'Route template does not align with the selected date cadence' });
    }

    const stopsResult = await pool.query(
      `SELECT * FROM dispatch_route_template_stops WHERE template_id = $1 ORDER BY position ASC`,
      [templateId]
    );
    const liveJobs = await fetchLiveDispatchJobsForDate(date);
    const applyResult = applyDispatchRouteTemplate({
      template,
      templateStops: stopsResult.rows,
      liveJobs,
    });
    const actor = getDispatchActor(req);
    const updated = await patchDispatchPlanItems(pool, {
      patches: applyResult.patches,
      ...actor,
    });

    res.json({
      success: true,
      template: summarizeDispatchRouteTemplate(template, stopsResult.rows.length, { targetDate: date }),
      applied_date: date,
      updated: updated.length,
      matched_count: applyResult.matched.length,
      appended_count: applyResult.appended.length,
      ambiguous: applyResult.ambiguous,
      unmatched_template_stops: applyResult.unmatched_template_stops,
      ordered: applyResult.patches.map((entry) => ({
        job_key: entry.jobKey,
        route_order_override: entry.patch.route_order_override,
        crew_override_name: entry.patch.crew_override_name || null,
      })),
    });
  } catch (error) { serverError(res, error); }
});

// POST /api/dispatch/apply-future-weeks - Apply route order to future recurring visits
router.post('/api/dispatch/apply-future-weeks', async (req, res) => {
  try {
    const { date, crew_name, frequency } = req.body;
    if (!date || !crew_name) return res.status(400).json({ success: false, error: 'date and crew_name required' });

    const sourceJobs = await pool.query(
      `SELECT id, route_order, parent_job_id, is_recurring, customer_id, service_type, address
       FROM scheduled_jobs
       WHERE job_date::date = $1::date AND crew_assigned = $2 AND route_order IS NOT NULL
       ORDER BY route_order ASC`,
      [date, crew_name]
    );

    if (sourceJobs.rows.length === 0) {
      return res.json({ success: false, error: 'No ordered jobs found for this date and crew. Save the route order for today first.' });
    }

    const sourceDate = new Date(date + 'T00:00:00');
    const sourceDayOfWeek = sourceDate.getDay();
    let totalUpdated = 0;

    for (const job of sourceJobs.rows) {
      const seriesRootId = job.parent_job_id || (job.is_recurring ? job.id : null);
      if (!seriesRootId) continue;

      const futureJobs = await pool.query(
        `SELECT id, job_date FROM scheduled_jobs
         WHERE (parent_job_id = $1 OR (id = $1 AND is_recurring = true))
           AND crew_assigned = $2
           AND job_date::date > $3::date
           AND EXTRACT(DOW FROM job_date::date) = $4
         ORDER BY job_date ASC`,
        [seriesRootId, crew_name, date, sourceDayOfWeek]
      );

      for (const futureJob of futureJobs.rows) {
        if (frequency === 'biweekly') {
          const futureDate = new Date(futureJob.job_date);
          const weeksDiff = Math.round((futureDate - sourceDate) / (7 * 24 * 60 * 60 * 1000));
          if (weeksDiff % 2 !== 0) continue;
        }
        await pool.query(
          'UPDATE scheduled_jobs SET route_order = $1, updated_at = NOW() WHERE id = $2',
          [job.route_order, futureJob.id]
        );
        totalUpdated++;
      }
    }

    res.json({ success: true, updated: totalUpdated, message: `Applied route order to ${totalUpdated} future visits (${frequency})` });
  } catch (err) {
    console.error('Apply future weeks error:', err);
    serverError(res, err);
  }
});

// Haversine distance in miles
function haversine(lat1, lon1, lat2, lon2) {
  const R = 3959; // Earth radius in miles
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

router.patch('/api/jobs/:id/pipeline', async (req, res) => {
  try {
    const { stage } = req.body;
    const validStages = ['new', 'quoted', 'scheduled', 'in_progress', 'completed', 'invoiced'];
    if (!validStages.includes(stage)) return res.status(400).json({ success: false, error: 'Invalid stage' });
    const statusMap = { new: 'pending', quoted: 'pending', scheduled: 'confirmed', in_progress: 'in-progress', completed: 'completed', invoiced: 'completed' };
    const result = await pool.query(
      `UPDATE scheduled_jobs SET pipeline_stage = $1, status = $2, updated_at = NOW() WHERE id = $3 RETURNING *`,
      [stage, statusMap[stage], req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, error: 'Job not found' });
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ─── Recurring Job Scheduling (Enhanced) ───────────────────────────────────

// POST /api/jobs/:id/setup-recurring - Configure recurring schedule
router.post('/api/jobs/:id/setup-recurring', async (req, res) => {
  try {
    const { pattern, day_of_week, start_date, end_date, auto_generate_weeks } = req.body;
    // pattern: weekly, biweekly, monthly, custom
    const validPatterns = ['weekly', 'biweekly', 'monthly', 'custom'];
    if (!validPatterns.includes(pattern)) return res.status(400).json({ success: false, error: 'Invalid pattern' });

    const result = await pool.query(
      `UPDATE scheduled_jobs SET is_recurring = true, recurring_pattern = $1, recurring_day_of_week = $2, recurring_start_date = $3, recurring_end_date = $4, updated_at = NOW() WHERE id = $5 RETURNING *`,
      [pattern, day_of_week, start_date, end_date, req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, error: 'Job not found' });

    // Auto-generate upcoming jobs if requested
    let generated = [];
    if (auto_generate_weeks && auto_generate_weeks > 0) {
      const job = result.rows[0];
      const startDt = new Date(start_date || job.job_date);
      const endDt = end_date ? new Date(end_date) : new Date(startDt.getTime() + auto_generate_weeks * 7 * 86400000);

      let current = new Date(startDt);
      const intervalDays = pattern === 'weekly' ? 7 : pattern === 'biweekly' ? 14 : 30;
      current.setDate(current.getDate() + intervalDays); // skip first (it's the parent)

      while (current <= endDt) {
        const dateStr = current.toISOString().split('T')[0];
        // Check for duplicates
        const exists = await pool.query('SELECT id FROM recurring_job_log WHERE source_job_id = $1 AND generated_for_date = $2', [job.id, dateStr]);
        if (!exists.rows.length) {
          const newJob = await pool.query(
            `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, estimated_duration, crew_assigned, parent_job_id, property_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11,$12,$13,$14) RETURNING *`,
            [dateStr, job.customer_name, job.customer_id, job.service_type, job.service_frequency, job.service_price, job.address, job.phone, job.special_notes, job.property_notes, job.estimated_duration, job.crew_assigned, job.id, job.property_id]
          );
          await pool.query('INSERT INTO recurring_job_log (source_job_id, generated_for_date, generated_job_id) VALUES ($1,$2,$3)', [job.id, dateStr, newJob.rows[0].id]);
          generated.push(newJob.rows[0]);
        }
        current.setDate(current.getDate() + intervalDays);
      }
    }

    res.json({ success: true, job: result.rows[0], generated_jobs: generated.length, jobs: generated });
  } catch (error) { serverError(res, error); }
});

router.get('/api/jobs/:id/profitability', async (req, res) => {
  try {
    const job = await pool.query('SELECT * FROM scheduled_jobs WHERE id = $1', [req.params.id]);
    if (!job.rows.length) return res.status(404).json({ success: false, error: 'Job not found' });
    const j = job.rows[0];

    // Get expenses
    let expenses = [];
    try { expenses = (await pool.query('SELECT * FROM job_expenses WHERE job_id = $1 ORDER BY created_at DESC', [j.id])).rows; } catch(e) {}

    // Get time entries for labor cost
    let timeEntries = [];
    try { timeEntries = (await pool.query('SELECT * FROM time_entries WHERE job_id = $1', [j.id])).rows; } catch(e) {}

    const laborHours = timeEntries.reduce((sum, t) => {
      if (!t.clock_in || !t.clock_out) return sum;
      return sum + (new Date(t.clock_out) - new Date(t.clock_in)) / 3600000 - (t.break_minutes || 0) / 60;
    }, 0);
    const laborRate = 35; // TODO: configurable per crew
    const laborCost = parseFloat(j.labor_cost) || (laborHours * laborRate);
    const materialCost = parseFloat(j.material_cost) || 0;
    const expenseTotal = expenses.reduce((sum, e) => sum + parseFloat(e.amount), 0);
    const revenue = parseFloat(j.service_price) || 0;
    const totalCost = laborCost + materialCost + expenseTotal;
    const profit = revenue - totalCost;
    const margin = revenue > 0 ? (profit / revenue * 100) : 0;

    res.json({
      success: true,
      profitability: {
        revenue,
        labor_cost: laborCost,
        labor_hours: Math.round(laborHours * 100) / 100,
        material_cost: materialCost,
        expense_total: expenseTotal,
        total_cost: totalCost,
        profit,
        margin: Math.round(margin * 10) / 10
      },
      expenses,
      time_entries: timeEntries
    });
  } catch (error) { serverError(res, error); }
});

// POST /api/jobs/:id/expenses - Add expense to job
router.post('/api/jobs/:id/expenses', async (req, res) => {
  try {
    const { description, category, amount, receipt_url, created_by } = req.body;
    if (!amount) return res.status(400).json({ success: false, error: 'Amount required' });
    const result = await pool.query(
      `INSERT INTO job_expenses (job_id, description, category, amount, receipt_url, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [req.params.id, description, category, parseFloat(amount), receipt_url, created_by]
    );
    // Update job expense_total
    const total = await pool.query('SELECT COALESCE(SUM(amount),0) as total FROM job_expenses WHERE job_id = $1', [req.params.id]);
    await pool.query('UPDATE scheduled_jobs SET expense_total = $1 WHERE id = $2', [total.rows[0].total, req.params.id]);
    res.json({ success: true, expense: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// DELETE /api/jobs/:id/expenses/:expenseId
router.delete('/api/jobs/:id/expenses/:expenseId', async (req, res) => {
  try {
    await pool.query('DELETE FROM job_expenses WHERE id = $1 AND job_id = $2', [req.params.expenseId, req.params.id]);
    const total = await pool.query('SELECT COALESCE(SUM(amount),0) as total FROM job_expenses WHERE job_id = $1', [req.params.id]);
    await pool.query('UPDATE scheduled_jobs SET expense_total = $1 WHERE id = $2', [total.rows[0].total, req.params.id]);
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

// ─── Internal Notes ────────────────────────────────────────────────────────


router.post('/api/jobs/from-quote/:quoteId', async (req, res) => {
  try {
    const { quoteId } = req.params;
    const { job_date, crew_assigned } = req.body;

    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [quoteId]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    const quote = quoteResult.rows[0];

    let serviceType = 'Service';
    let totalPrice = parseFloat(quote.total) || 0;
    if (quote.services) {
      const services = typeof quote.services === 'string' ? JSON.parse(quote.services) : quote.services;
      if (Array.isArray(services) && services.length > 0) {
        serviceType = services.map(s => s.name || s.service || s.description || 'Service').join(', ');
        if (serviceType.length > 100) serviceType = serviceType.substring(0, 97) + '...';
      }
    }

    const jobDate = job_date || new Date().toISOString().split('T')[0];

    const result = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_price, address, phone, special_notes, status, crew_assigned, estimated_duration)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, 60)
       RETURNING *`,
      [
        jobDate,
        quote.customer_name,
        quote.customer_id || null,
        serviceType,
        totalPrice,
        quote.customer_address || '',
        quote.customer_phone || '',
        'Created from Quote #' + (quote.quote_number || quote.id),
        crew_assigned || null
      ]
    );

    res.json({ success: true, job: result.rows[0], quote_id: quote.id });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// PHASE 6 — FINANCIAL SUITE ENDPOINTS
// (aging + batch routes are registered above before /api/invoices/:id)

router.get('/api/crews/:id/performance', async (req, res) => {
  try {
    const { id } = req.params;
    const crew = await pool.query('SELECT name FROM crews WHERE id = $1', [id]);
    if (crew.rows.length === 0) return res.status(404).json({ success: false, error: 'Crew not found' });
    const crewName = crew.rows[0].name;
    const stats = await pool.query(`
      SELECT COUNT(*) as total_jobs,
        COUNT(*) FILTER (WHERE status = 'completed') as completed_jobs,
        COALESCE(SUM(service_price) FILTER (WHERE status = 'completed'), 0) as total_revenue,
        COUNT(*) FILTER (WHERE status = 'completed' AND job_date >= NOW() - INTERVAL '30 days') as completed_last_30
      FROM scheduled_jobs WHERE crew_assigned = $1
    `, [crewName]);
    const s = stats.rows[0];
    const totalJobs = parseInt(s.total_jobs) || 0;
    const completedJobs = parseInt(s.completed_jobs) || 0;
    const onTimeRate = totalJobs > 0 ? (completedJobs / totalJobs) : 0;
    res.json({
      success: true,
      crew_name: crewName,
      total_jobs: totalJobs,
      completed_jobs: completedJobs,
      on_time_rate: Math.round(onTimeRate * 100),
      total_revenue: parseFloat(s.total_revenue) || 0,
      completed_last_30: parseInt(s.completed_last_30) || 0
    });
  } catch (error) {
    console.error('Crew performance error:', error);
    serverError(res, error);
  }
});

router.get('/api/crews/:id/schedule', async (req, res) => {
  try {
    const { id } = req.params;
    const crew = await pool.query('SELECT name FROM crews WHERE id = $1', [id]);
    if (crew.rows.length === 0) return res.status(404).json({ success: false, error: 'Crew not found' });
    const crewName = crew.rows[0].name;
    const jobs = await pool.query(`
      SELECT id, customer_name, service_type, service_price, address, job_date, job_date AS scheduled_date, status
      FROM scheduled_jobs WHERE crew_assigned = $1 AND job_date >= CURRENT_DATE
      ORDER BY job_date ASC LIMIT 20
    `, [crewName]);
    res.json({ success: true, jobs: jobs.rows });
  } catch (error) {
    console.error('Crew schedule error:', error);
    serverError(res, error);
  }
});

// 8.2 Reports: Job Costing & Customer Value

router.get('/api/dispatch-templates', async (req, res) => {
  try {
    const result = await pool.query('SELECT dt.*, c.name as crew_display_name FROM dispatch_templates dt LEFT JOIN crews c ON dt.crew_id = c.id ORDER BY dt.name');
    res.json({ success: true, templates: result.rows });
  } catch (error) { serverError(res, error); }
});

router.post('/api/dispatch-templates', async (req, res) => {
  try {
    const { name, zip_codes, crew_id, service_type, default_duration, notes } = req.body;
    if (!name) return res.status(400).json({ success: false, error: 'Name required' });
    const result = await pool.query(
      'INSERT INTO dispatch_templates (name, zip_codes, crew_id, service_type, default_duration, notes) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
      [name, zip_codes, crew_id, service_type, default_duration || 30, notes]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.put('/api/dispatch-templates/:id', async (req, res) => {
  try {
    const { name, zip_codes, crew_id, service_type, default_duration, notes } = req.body;
    const result = await pool.query(
      'UPDATE dispatch_templates SET name=$1, zip_codes=$2, crew_id=$3, service_type=$4, default_duration=$5, notes=$6 WHERE id=$7 RETURNING *',
      [name, zip_codes, crew_id, service_type, default_duration, notes, req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.delete('/api/dispatch-templates/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM dispatch_templates WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// Quick dispatch: find template by zip code, create job
router.post('/api/dispatch-templates/quick-dispatch', async (req, res) => {
  try {
    const { address, zip_code, customer_name, customer_id, job_date } = req.body;
    if (!zip_code || !job_date) return res.status(400).json({ success: false, error: 'Zip code and date required' });
    // Find matching template
    const templates = await pool.query("SELECT * FROM dispatch_templates WHERE zip_codes LIKE $1", [`%${zip_code}%`]);
    if (templates.rows.length === 0) return res.status(404).json({ success: false, error: 'No template found for zip ' + zip_code });
    const t = templates.rows[0];
    const job = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, address, crew_assigned, estimated_duration, status) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending') RETURNING *`,
      [job_date, customer_name, customer_id, t.service_type, address, t.crew_id, t.default_duration]
    );
    res.json({ success: true, job: job.rows[0], template: t });
  } catch (error) { serverError(res, error); }
});

  return router;
}

module.exports = createJobRoutes;
module.exports.__testables = {
  JobStatusTransitionError,
  normalizeJobStatus,
  normalizeCopilotExecutionStatus,
  normalizeCopilotLinkText,
  normalizeCopilotLinkAddress,
  normalizeCopilotLinkMoney,
  buildCopilotImportMatchFingerprint,
  buildCopilotSyncJobFingerprint,
  findStrictCopilotImportCandidates,
  planCopilotImportLinkage,
  mapCopilotExecutionMirror,
  hashCopilotExecutionPayload,
  canonicalizeCopilotExecutionMirror,
  hasCopilotDispatchSyncAccess,
  findScheduledJobForCopilotMirror,
  syncCopilotDispatchExecutionRecords,
  fetchCopilotDispatchExecutionRecords,
  persistCopilotSyncJobs,
  hydrateCopilotScheduleDates,
  applyCopilotImportLinkage,
  validateJobStatusTransition,
  transitionScheduledJobStatus,
  applyCompletedJobInvoiceSideEffects,
  calculateInvoiceTax,
};
