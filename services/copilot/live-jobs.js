const COPILOT_SOURCE_SYSTEM = 'copilot';

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

async function fetchResolvedLiveJobs(pool, { date, includeDeleted = false, jobKeys } = {}) {
  const params = [];
  const where = [`clj.source_system = '${COPILOT_SOURCE_SYSTEM}'`];
  let paramIndex = 1;

  if (date) {
    where.push(`clj.service_date = $${paramIndex++}::date`);
    params.push(date);
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

module.exports = {
  COPILOT_SOURCE_SYSTEM,
  buildCopilotJobKey,
  fetchResolvedLiveJob,
  fetchResolvedLiveJobs,
  normalizeResolvedRow,
  parseVisitTotal,
  upsertCopilotLiveJobs,
};
