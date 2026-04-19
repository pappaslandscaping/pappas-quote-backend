const {
  fetchCopilotScheduleGridJobsForDate,
  getCopilotToken,
} = require('./client');

const COPILOT_SOURCE_SYSTEM = 'copilot';
const SCHEDULE_SOURCE_KIND = 'live_schedule';
const LIVE_JOB_FETCH_TIMEOUT_MS = 12 * 1000;

const LIVE_JOB_STATUS_MAP = new Map([
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

function buildCopilotJobKey(serviceDate, sourceEventId) {
  if (!serviceDate) throw new Error('serviceDate is required');
  if (!sourceEventId) throw new Error('sourceEventId is required');
  return `${COPILOT_SOURCE_SYSTEM}:${serviceDate}:${String(sourceEventId).trim()}`;
}

function parseVisitTotal(value) {
  if (value == null) return null;
  const numeric = String(value).replace(/[^0-9.-]/g, '');
  if (!numeric) return null;
  const parsed = Number.parseFloat(numeric);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function parseJsonObject(value) {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }
  return {};
}

function isValidIsoDate(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function* eachDateInRange(startDate, endDate) {
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  while (cursor <= end) {
    yield cursor.toISOString().slice(0, 10);
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
}

function normalizeCopilotLiveStatus(status) {
  if (typeof status !== 'string') return 'pending';
  const normalized = status.trim().toLowerCase().replace(/[\s-]+/g, '_');
  return LIVE_JOB_STATUS_MAP.get(normalized) || 'pending';
}

function withTimeout(promise, timeoutMs, message) {
  let timeoutHandle;
  return Promise.race([
    promise.finally(() => clearTimeout(timeoutHandle)),
    new Promise((_, reject) => {
      timeoutHandle = setTimeout(() => reject(new Error(message)), timeoutMs);
    }),
  ]);
}

function normalizeScheduleAddress(address) {
  return typeof address === 'string' ? address.trim() : '';
}

function buildScheduleGeocodeFields(address) {
  const normalizedAddress = normalizeScheduleAddress(address);
  const hasStreetAddress = /^\d+/.test(normalizedAddress);
  return {
    has_street_address: hasStreetAddress,
    geocode_address: hasStreetAddress ? normalizedAddress : '',
  };
}

function normalizeResolvedRow(row) {
  const overlayExists = Boolean(row.overlay_job_key);
  const dispatchPlanExists = Boolean(row.dispatch_plan_job_key);
  const localTags = parseJsonArray(row.local_tags);
  const sourceVisitTotal =
    row.visit_total == null ? null : Number.parseFloat(row.visit_total);
  const effectiveAddress = row.address_override || row.address_raw || null;
  const effectiveCrewName = row.crew_override_name || row.source_crew_name || null;
  const effectiveRouteOrder = row.route_order_override ?? row.source_stop_order ?? null;
  const mapLat = row.map_lat == null ? null : Number.parseFloat(row.map_lat);
  const mapLng = row.map_lng == null ? null : Number.parseFloat(row.map_lng);
  const sourceDeleted = !!row.source_deleted_at;
  const rawPayload = parseJsonObject(row.raw_payload);
  const rawData = parseJsonObject(rawPayload.raw_data);
  const sourceSurface = rawData.source_surface || rawPayload.source_surface || null;

  return {
    job_key: row.job_key,
    service_date: row.service_date instanceof Date
      ? row.service_date.toISOString().slice(0, 10)
      : row.service_date,
    source_system: COPILOT_SOURCE_SYSTEM,
    source: {
      event_id: row.source_event_id,
      synced_at: row.source_synced_at,
      deleted_at_source: row.source_deleted_at || null,
      customer_id: row.source_customer_id || null,
      customer_name: row.customer_name,
      job_title: row.job_title || null,
      status: row.source_status || null,
      visit_total: Number.isFinite(sourceVisitTotal) ? sourceVisitTotal : null,
      crew_name: row.source_crew_name || null,
      employees_text: row.source_employees_text || null,
      stop_order: row.source_stop_order ?? null,
      address: row.address_raw || null,
      property_name: rawData.property_name || null,
      event_type: rawData.event_type || null,
      invoiceable: rawData.invoiceable || null,
      frequency: rawData.frequency || null,
      last_serviced: rawData.last_serviced || null,
      notes: rawData.notes || null,
      tracked_time: rawData.tracked_time || null,
      budgeted_hours: rawData.budgeted_hours || null,
      service_date_label: rawData.service_date_label || null,
      source_surface: sourceSurface,
    },
    overlay: {
      exists: overlayExists,
      review_state: row.review_state || 'new',
      office_note: row.office_note || null,
      hold_from_dispatch: !!row.hold_from_dispatch,
      local_tags: localTags,
      address_override: row.address_override || null,
      customer_link_id: row.customer_link_id ?? null,
      property_link_id: row.property_link_id ?? null,
      updated_at: overlayExists ? row.overlay_updated_at : null,
      updated_by_name: overlayExists ? (row.overlay_updated_by_name || null) : null,
    },
    dispatch_plan: {
      exists: dispatchPlanExists,
      crew_override_name: row.crew_override_name || null,
      route_order_override: row.route_order_override ?? null,
      route_locked: !!row.route_locked,
      map_lat: mapLat,
      map_lng: mapLng,
      map_source: row.map_source || 'none',
      map_quality: row.map_quality || 'missing',
      print_group_key: row.print_group_key || null,
      print_note: row.print_note || null,
      updated_at: dispatchPlanExists ? row.dispatch_updated_at : null,
      updated_by_name: dispatchPlanExists ? (row.dispatch_updated_by_name || null) : null,
    },
    resolved: {
      effective_address: effectiveAddress,
      effective_crew_name: effectiveCrewName,
      effective_route_order: effectiveRouteOrder,
      included_in_dispatch: !row.hold_from_dispatch && !sourceDeleted,
      map_lat: mapLat,
      map_lng: mapLng,
      map_quality: row.map_quality || 'missing',
    },
    flags: {
      needs_address_review: !effectiveAddress,
      needs_crew_review: !effectiveCrewName,
      needs_route_review: effectiveRouteOrder == null,
      source_deleted: sourceDeleted,
    },
  };
}

async function fetchResolvedLiveJobs(pool, {
  date,
  startDate,
  endDate,
  includeDeleted = false,
  jobKeys,
} = {}) {
  const params = [];
  const where = [`clj.source_system = '${COPILOT_SOURCE_SYSTEM}'`];
  let paramIndex = 1;

  if (date) {
    where.push(`clj.service_date = $${paramIndex++}::date`);
    params.push(date);
  } else {
    if (startDate) {
      where.push(`clj.service_date >= $${paramIndex++}::date`);
      params.push(startDate);
    }
    if (endDate) {
      where.push(`clj.service_date <= $${paramIndex++}::date`);
      params.push(endDate);
    }
  }

  if (Array.isArray(jobKeys) && jobKeys.length > 0) {
    where.push(`clj.job_key = ANY($${paramIndex++}::text[])`);
    params.push(jobKeys);
  }

  if (!includeDeleted) {
    where.push('clj.source_deleted_at IS NULL');
  }

  const result = await pool.query(
    `SELECT
       clj.*,
       yjo.job_key AS overlay_job_key,
       yjo.review_state,
       yjo.office_note,
       yjo.hold_from_dispatch,
       yjo.local_tags,
       yjo.address_override,
       yjo.customer_link_id,
       yjo.property_link_id,
       yjo.updated_at AS overlay_updated_at,
       yjo.updated_by_name AS overlay_updated_by_name,
       dpi.job_key AS dispatch_plan_job_key,
       dpi.crew_override_name,
       dpi.route_order_override,
       dpi.route_locked,
       dpi.map_lat,
       dpi.map_lng,
       dpi.map_source,
       dpi.map_quality,
       dpi.print_group_key,
       dpi.print_note,
       dpi.updated_at AS dispatch_updated_at,
       dpi.updated_by_name AS dispatch_updated_by_name
     FROM copilot_live_jobs clj
     LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key
     LEFT JOIN dispatch_plan_items dpi ON dpi.job_key = clj.job_key
     WHERE ${where.join(' AND ')}
     ORDER BY
       clj.service_date ASC,
       COALESCE(dpi.route_order_override, clj.source_stop_order) ASC NULLS LAST,
       clj.customer_name ASC`,
    params
  );

  return result.rows.map(normalizeResolvedRow);
}

async function fetchResolvedLiveJob(pool, jobKey, { includeDeleted = true } = {}) {
  const jobs = await fetchResolvedLiveJobs(pool, { jobKeys: [jobKey], includeDeleted });
  return jobs[0] || null;
}

async function upsertCopilotLiveJobs(pool, { serviceDate, jobs, syncedAt = new Date() }) {
  if (!serviceDate) throw new Error('serviceDate is required');
  if (!Array.isArray(jobs)) throw new Error('jobs must be an array');

  const eventIds = [];
  let inserted = 0;
  let updated = 0;

  for (const job of jobs) {
    if (!job || !job.event_id) continue;
    const sourceEventId = String(job.event_id).trim();
    if (!sourceEventId) continue;
    eventIds.push(sourceEventId);

    const result = await pool.query(
      `INSERT INTO copilot_live_jobs (
         job_key,
         source_system,
         service_date,
         source_event_id,
         source_customer_id,
         customer_name,
         job_title,
         source_status,
         visit_total,
         source_crew_name,
         source_employees_text,
         source_stop_order,
         address_raw,
         raw_payload,
         source_synced_at,
         source_deleted_at,
         first_seen_at,
         last_seen_at
       ) VALUES (
         $1, $2, $3::date, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15, NULL, NOW(), NOW()
       )
       ON CONFLICT (job_key) DO UPDATE SET
         source_customer_id = EXCLUDED.source_customer_id,
         customer_name = EXCLUDED.customer_name,
         job_title = EXCLUDED.job_title,
         source_status = EXCLUDED.source_status,
         visit_total = EXCLUDED.visit_total,
         source_crew_name = EXCLUDED.source_crew_name,
         source_employees_text = EXCLUDED.source_employees_text,
         source_stop_order = EXCLUDED.source_stop_order,
         address_raw = EXCLUDED.address_raw,
         raw_payload = EXCLUDED.raw_payload,
         source_synced_at = EXCLUDED.source_synced_at,
         source_deleted_at = NULL,
         last_seen_at = NOW()
       RETURNING (xmax = 0) AS is_insert`,
      [
        buildCopilotJobKey(serviceDate, sourceEventId),
        COPILOT_SOURCE_SYSTEM,
        serviceDate,
        sourceEventId,
        job.customer_id || null,
        job.customer_name || '',
        job.job_title || null,
        job.status || null,
        parseVisitTotal(job.visit_total),
        job.crew_name || null,
        job.employees || null,
        job.stop_order ?? null,
        job.address || null,
        JSON.stringify(job.raw_data || job),
        syncedAt,
      ]
    );

    if (result.rows[0] && result.rows[0].is_insert) inserted += 1;
    else updated += 1;
  }

  let markedDeleted = 0;
  if (eventIds.length > 0) {
    const deletedResult = await pool.query(
      `UPDATE copilot_live_jobs
          SET source_deleted_at = $3,
              source_synced_at = $3
        WHERE source_system = $1
          AND service_date = $2::date
          AND source_event_id <> ALL($4::text[])
          AND source_deleted_at IS NULL`,
      [COPILOT_SOURCE_SYSTEM, serviceDate, syncedAt, eventIds]
    );
    markedDeleted = deletedResult.rowCount || 0;
  } else {
    const deletedResult = await pool.query(
      `UPDATE copilot_live_jobs
          SET source_deleted_at = $3,
              source_synced_at = $3
        WHERE source_system = $1
          AND service_date = $2::date
          AND source_deleted_at IS NULL`,
      [COPILOT_SOURCE_SYSTEM, serviceDate, syncedAt]
    );
    markedDeleted = deletedResult.rowCount || 0;
  }

  return {
    serviceDate,
    total: jobs.length,
    inserted,
    updated,
    marked_deleted: markedDeleted,
  };
}

function mapResolvedLiveJobToScheduleJob(job) {
  return mapResolvedLiveJobToScheduleJobWithFreshness(job, {});
}

function mapResolvedLiveJobToScheduleJobWithFreshness(job, {
  freshnessSource = 'mirror',
  fetchedAt = null,
} = {}) {
  const address = normalizeScheduleAddress(job.resolved.effective_address || job.source.address || '');
  const geocodeFields = buildScheduleGeocodeFields(address);
  const effectiveFetchedAt = fetchedAt || job.source.synced_at || null;
  const routeOrder = job.resolved.effective_route_order ?? null;
  const mapLat = job.resolved.map_lat ?? null;
  const mapLng = job.resolved.map_lng ?? null;

  return {
    id: job.job_key,
    source_system: COPILOT_SOURCE_SYSTEM,
    source_kind: SCHEDULE_SOURCE_KIND,
    freshness_source: freshnessSource,
    fetched_at: effectiveFetchedAt,
    is_read_only: true,
    can_edit: false,
    can_complete: false,
    can_delete: false,
    job_date: job.service_date,
    service_date: job.service_date,
    visit_id: job.source.event_id || null,
    copilot_visit_id: job.source.event_id || null,
    job_id: null,
    copilot_job_id: null,
    copilot_customer_id: job.source.customer_id || null,
    customer_id: job.overlay.customer_link_id ?? null,
    local_customer_id: job.overlay.customer_link_id ?? null,
    customer_name: job.source.customer_name || 'Unknown',
    phone: null,
    email: null,
    address,
    service_type: job.source.job_title || 'Service',
    service_title: job.source.job_title || 'Service',
    service_frequency: job.source.frequency || null,
    property_name: job.source.property_name || null,
    copilot_event_type: job.source.event_type || null,
    copilot_invoiceable_status: job.source.invoiceable || null,
    service_price: job.source.visit_total ?? null,
    service_price_raw: job.source.visit_total ?? null,
    crew_assigned: job.resolved.effective_crew_name || null,
    crew_name: job.resolved.effective_crew_name || null,
    crew_members_text: job.source.employees_text || null,
    status: normalizeCopilotLiveStatus(job.source.status),
    status_raw: job.source.status || null,
    last_serviced: job.source.last_serviced || null,
    route_order: routeOrder,
    stop_order: routeOrder,
    estimated_duration: 30,
    tracked_time: job.source.tracked_time || null,
    budgeted_hours: job.source.budgeted_hours || null,
    start_time: null,
    end_time: null,
    special_notes: job.overlay.office_note || null,
    property_notes: null,
    completion_notes: null,
    completion_photos: [],
    completion_lat: null,
    completion_lng: null,
    lat: mapLat,
    lng: mapLng,
    geocode_quality: job.resolved.map_quality || null,
    overlay_review_state: job.overlay.review_state || 'new',
    hold_from_dispatch: !!job.overlay.hold_from_dispatch,
    source_deleted: !!job.flags.source_deleted,
    ...geocodeFields,
  };
}

function buildLiveJobStats(jobs = []) {
  const byStatus = {};
  const byCrew = {};
  let totalRevenue = 0;

  for (const job of jobs) {
    const status = job.status || 'pending';
    const crew = job.crew_assigned || 'Unassigned';
    byStatus[status] = (byStatus[status] || 0) + 1;
    byCrew[crew] = (byCrew[crew] || 0) + 1;
    totalRevenue += Number(job.service_price) || 0;
  }

  return {
    total: jobs.length,
    byStatus,
    totalRevenue,
    byCrew,
  };
}

function buildLiveJobDaySummaries(jobs = []) {
  const byDay = new Map();

  for (const job of jobs) {
    const day = job.job_date;
    if (!byDay.has(day)) {
      byDay.set(day, {
        day,
        total_jobs: 0,
        completed: 0,
        pending: 0,
        in_progress: 0,
        skipped: 0,
        cancelled: 0,
        revenue: 0,
        crews: {},
      });
    }

    const summary = byDay.get(day);
    summary.total_jobs += 1;
    summary.revenue += Number(job.service_price) || 0;
    summary.crews[job.crew_assigned || 'Unassigned'] = (summary.crews[job.crew_assigned || 'Unassigned'] || 0) + 1;
    if (summary[job.status] !== undefined) summary[job.status] += 1;
  }

  return [...byDay.values()].sort((left, right) => left.day.localeCompare(right.day));
}

function buildAggregateFreshness(perDate = []) {
  const uniqueSources = [...new Set(perDate.map((entry) => entry.source))];
  const fetchedAt = perDate.reduce((latest, entry) => {
    if (!entry.fetched_at) return latest;
    return !latest || entry.fetched_at > latest ? entry.fetched_at : latest;
  }, null);

  return {
    source: uniqueSources.length === 1 ? uniqueSources[0] : 'mixed',
    fetched_at: fetchedAt,
    stale: uniqueSources.includes('mirror'),
    per_date: perDate,
  };
}

function detectLiveParseMismatch(liveResult, syncDate) {
  const jobCount = Array.isArray(liveResult?.jobs) ? liveResult.jobs.length : 0;
  const noEventsFound = !!liveResult?.diagnostics?.no_events_found;
  if (jobCount === 0 && !noEventsFound) {
    throw new Error(`Copilot schedule grid/day parse mismatch for ${syncDate}: parsed 0 jobs without an explicit no-events marker`);
  }
}

async function fetchLiveCopilotScheduleDate({
  poolClient,
  syncDate,
  cookieHeader,
  fetchImpl = fetch,
  timeoutMs = LIVE_JOB_FETCH_TIMEOUT_MS,
} = {}) {
  if (!cookieHeader) throw new Error('No CopilotCRM cookies configured');

  const liveResult = await withTimeout(
    fetchCopilotScheduleGridJobsForDate({ cookieHeader, syncDate, fetchImpl }),
    timeoutMs,
    `Copilot live schedule timed out for ${syncDate}`
  );
  detectLiveParseMismatch(liveResult, syncDate);

  const fetchedAt = new Date().toISOString();
  await upsertCopilotLiveJobs(poolClient, {
    serviceDate: syncDate,
    jobs: liveResult.jobs || [],
    syncedAt: new Date(fetchedAt),
  });

  return {
    date: syncDate,
    source: 'live',
    source_surface: liveResult.source_surface || 'schedule_grid',
    fetched_at: fetchedAt,
    error: null,
    diagnostics: liveResult.diagnostics || null,
  };
}

async function getCopilotLiveJobs({
  poolClient,
  date,
  startDate,
  endDate,
  fetchImpl = fetch,
  timeoutMs = LIVE_JOB_FETCH_TIMEOUT_MS,
} = {}) {
  const resolvedStartDate = startDate || date;
  const resolvedEndDate = endDate || date || startDate;

  if (!isValidIsoDate(resolvedStartDate) || !isValidIsoDate(resolvedEndDate)) {
    throw new Error('Provide date or start_date/end_date in YYYY-MM-DD format');
  }
  if (resolvedStartDate > resolvedEndDate) {
    throw new Error('start_date cannot be after end_date');
  }

  const tokenInfo = await getCopilotToken(poolClient).catch(() => null);
  const cookieHeader = tokenInfo?.cookieHeader || null;
  const liveAttemptByDate = new Map();

  for (const syncDate of eachDateInRange(resolvedStartDate, resolvedEndDate)) {
    try {
      const liveAttempt = await fetchLiveCopilotScheduleDate({
        poolClient,
        syncDate,
        cookieHeader,
        fetchImpl,
        timeoutMs,
      });
      liveAttemptByDate.set(syncDate, liveAttempt);
    } catch (error) {
      liveAttemptByDate.set(syncDate, {
        date: syncDate,
        source: 'mirror',
        fetched_at: null,
        error,
      });
    }
  }

  const resolvedJobs = await fetchResolvedLiveJobs(poolClient, {
    startDate: resolvedStartDate,
    endDate: resolvedEndDate,
    includeDeleted: false,
  });
  const resolvedJobsByDate = new Map();
  for (const job of resolvedJobs) {
    const bucket = resolvedJobsByDate.get(job.service_date) || [];
    bucket.push(job);
    resolvedJobsByDate.set(job.service_date, bucket);
  }

  const jobs = [];
  const perDate = [];

  for (const syncDate of eachDateInRange(resolvedStartDate, resolvedEndDate)) {
    const liveAttempt = liveAttemptByDate.get(syncDate);
    const dateJobs = resolvedJobsByDate.get(syncDate) || [];

    if (liveAttempt?.source === 'live') {
      dateJobs.forEach((job) => {
        jobs.push(mapResolvedLiveJobToScheduleJobWithFreshness(job, {
          freshnessSource: 'live',
          fetchedAt: liveAttempt.fetched_at,
        }));
      });
      perDate.push({
        date: syncDate,
        source: 'live',
        source_surface: liveAttempt.source_surface || 'schedule_grid',
        fetched_at: liveAttempt.fetched_at,
        error: null,
        diagnostics: liveAttempt.diagnostics || null,
      });
      continue;
    }

    if (dateJobs.length > 0) {
      const mirrorFetchedAt = dateJobs.reduce((latest, job) => {
        const value = job.source.synced_at || null;
        if (!value) return latest;
        return !latest || value > latest ? value : latest;
      }, null);
      dateJobs.forEach((job) => {
        jobs.push(mapResolvedLiveJobToScheduleJobWithFreshness(job, {
          freshnessSource: 'mirror',
          fetchedAt: mirrorFetchedAt,
        }));
      });
      perDate.push({
        date: syncDate,
        source: 'mirror',
        source_surface: liveAttempt?.source_surface || null,
        fetched_at: mirrorFetchedAt,
        error: liveAttempt?.error ? liveAttempt.error.message : null,
        diagnostics: liveAttempt?.diagnostics || null,
      });
      continue;
    }

    if (liveAttempt?.error) {
      throw liveAttempt.error;
    }

    perDate.push({
      date: syncDate,
      source: 'live',
      source_surface: liveAttempt?.source_surface || 'schedule_grid',
      fetched_at: liveAttempt?.fetched_at || null,
      error: null,
      diagnostics: liveAttempt?.diagnostics || null,
    });
  }

  return {
    start_date: resolvedStartDate,
    end_date: resolvedEndDate,
    freshness: buildAggregateFreshness(perDate),
    stats: buildLiveJobStats(jobs),
    days: buildLiveJobDaySummaries(jobs),
    jobs,
  };
}

module.exports = {
  COPILOT_SOURCE_SYSTEM,
  buildCopilotJobKey,
  buildLiveJobDaySummaries,
  buildLiveJobStats,
  buildScheduleGeocodeFields,
  buildAggregateFreshness,
  fetchResolvedLiveJob,
  fetchResolvedLiveJobs,
  fetchLiveCopilotScheduleDate,
  getCopilotLiveJobs,
  isValidIsoDate,
  mapResolvedLiveJobToScheduleJob,
  normalizeCopilotLiveStatus,
  normalizeResolvedRow,
  parseVisitTotal,
  upsertCopilotLiveJobs,
};
