// ═══════════════════════════════════════════════════════════
// CopilotCRM Routes — sync, settings, cookie refresh
// Vendor-specific logic lives in services/copilot/client.js
// ═══════════════════════════════════════════════════════════

const express = require('express');
const {
  fetchCopilotRouteJobsForDate,
  fetchCopilotScheduleGridJobsForDate,
  getCopilotToken,
  parseCopilotRouteHtml,
} = require('../services/copilot/client');
const { getCopilotLiveJobs, upsertCopilotLiveJobs } = require('../services/copilot/live-jobs');

module.exports = function createCopilotRoutes({ pool, serverError, authenticateToken, fetchImpl = fetch }) {
  const router = express.Router();

  router.get('/api/copilot/live-jobs', authenticateToken, async (req, res) => {
    try {
      const payload = await getCopilotLiveJobs({
        poolClient: pool,
        date: req.query.date || null,
        startDate: req.query.start_date || null,
        endDate: req.query.end_date || null,
        fetchImpl,
      });
      res.json({
        success: true,
        ...payload,
      });
    } catch (error) {
      serverError(res, error, 'Copilot live jobs fetch failed');
    }
  });

  router.get('/api/copilot/schedule-live-debug', authenticateToken, async (req, res) => {
    try {
      const syncDate = req.query.date || new Date().toISOString().slice(0, 10);
      const tokenInfo = await getCopilotToken(pool);
      if (!tokenInfo || !tokenInfo.cookieHeader) {
        return res.status(500).json({
          success: false,
          error: 'No CopilotCRM cookies configured.',
        });
      }

      const [routeDay, scheduleGrid] = await Promise.all([
        fetchCopilotRouteJobsForDate({
          cookieHeader: tokenInfo.cookieHeader,
          syncDate,
          fetchImpl,
        }),
        fetchCopilotScheduleGridJobsForDate({
          cookieHeader: tokenInfo.cookieHeader,
          syncDate,
          fetchImpl,
        }),
      ]);

      res.json({
        success: true,
        date: syncDate,
        route_day: {
          total_jobs: routeDay.jobs.length,
          diagnostics: routeDay.diagnostics,
        },
        schedule_grid_day: {
          total_jobs: scheduleGrid.jobs.length,
          diagnostics: scheduleGrid.diagnostics,
        },
      });
    } catch (error) {
      serverError(res, error, 'Copilot live schedule debug failed');
    }
  });

  router.post('/api/copilot/sync', authenticateToken, async (req, res) => {
    try {

    // Date range — defaults to today
    const today = new Date().toISOString().slice(0, 10);
    const startDate = req.body.startDate || today;
    const endDate = req.body.endDate || startDate;

    // Get token
    const tokenInfo = await getCopilotToken(pool);
    if (!tokenInfo || !tokenInfo.cookieHeader) {
      return res.status(500).json({ success: false, error: 'No CopilotCRM cookies configured. Insert full browser cookie string into copilot_sync_settings with key=copilot_cookies.' });
    }

    // Warn if expiring soon
    let tokenWarning = null;
    if (tokenInfo.daysUntilExpiry !== null && tokenInfo.daysUntilExpiry < 7) {
      tokenWarning = `CopilotCRM token expires in ${Math.round(tokenInfo.daysUntilExpiry)} days (${tokenInfo.expiresAt.toISOString().slice(0, 10)}). Refresh soon.`;
    }

    // Fetch from CopilotCRM
    // Format dates as "Mar 26, 2026" to match CopilotCRM's expected format
    function formatCopilotDate(dateStr) {
      const d = new Date(dateStr + 'T00:00:00');
      return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }
    const sDateFormatted = formatCopilotDate(startDate);
    const eDateFormatted = formatCopilotDate(endDate);

    const formData = new URLSearchParams();
    formData.append('accessFrom', 'route');
    formData.append('bs4', '1');
    formData.append('sDate', sDateFormatted);
    formData.append('eDate', eDateFormatted);
    formData.append('optimizationFlag', '1');
    formData.append('count', '-1');
    // Event types: 1-5 + 0 (all route event types)
    for (const t of ['1', '2', '3', '4', '5', '0']) {
      formData.append('evtypes_route[]', t);
    }
    formData.append('isdate', '0');
    formData.append('sdate', sDateFormatted);
    formData.append('edate', eDateFormatted);
    formData.append('erec', 'all');
    formData.append('estatus', 'any');
    formData.append('esort', '');
    formData.append('einvstatus', 'any');

    const copilotRes = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': tokenInfo.cookieHeader,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest'
      },
      body: formData.toString()
    });

    const debug = req.query.debug === '1' || req.body.debug === true;

    if (!copilotRes.ok) {
      const errBody = await copilotRes.text().catch(() => '');
      return res.status(502).json({ success: false, error: `CopilotCRM returned ${copilotRes.status}`, ...(debug && { responseBody: errBody.substring(0, 1000) }) });
    }

    const data = await copilotRes.json();

    if (debug) {
      console.log(`🔍 CopilotCRM sync debug: status=${data.status}, totalEventCount=${data.totalEventCount}, htmlLength=${(data.html || '').length}, employeesCount=${(data.employees || []).length}`);
    }

    if (data.status !== undefined && data.status !== 1 && data.status !== '1' && data.status !== true) {
      return res.status(502).json({ success: false, error: 'CopilotCRM returned non-success status', copilot_status: data.status, ...(debug && { rawKeys: Object.keys(data) }) });
    }

    // Parse HTML
    const jobs = parseCopilotRouteHtml(data.html || '', data.employees || []);

    // Check for parse mismatch
    const expectedCount = data.totalEventCount || 0;
    if (expectedCount > 0 && jobs.length === 0) {
      return res.status(500).json({ success: false, error: 'parse_mismatch', expected: expectedCount, got: 0 });
    }

    // Upsert each job
    let inserted = 0;
    let updated = 0;
    const syncedEventIds = [];

    for (const job of jobs) {
      if (job && job.event_id) {
        syncedEventIds.push(String(job.event_id));
      }
      const result = await pool.query(
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
        [startDate, job.event_id, job.customer_name, job.customer_id, job.crew_name, job.employees, job.address, job.status, job.visit_total, job.job_title, job.stop_order, JSON.stringify(job)]
      );
      if (result.rows[0].is_insert) inserted++;
      else updated++;
    }

    let markedDeletedFromSnapshot = null;
    if (startDate === endDate) {
      const deleteResult = syncedEventIds.length > 0
        ? await pool.query(
            `DELETE FROM copilot_sync_jobs
              WHERE sync_date = $1
                AND event_id <> ALL($2::text[])`,
            [startDate, syncedEventIds]
          )
        : await pool.query(
            `DELETE FROM copilot_sync_jobs
              WHERE sync_date = $1`,
            [startDate]
          );
      markedDeletedFromSnapshot = deleteResult.rowCount || 0;
    }

    let liveMirror = null;
    if (startDate === endDate) {
      liveMirror = await upsertCopilotLiveJobs(pool, {
        serviceDate: startDate,
        jobs,
        syncedAt: new Date(),
      });
    } else {
      liveMirror = {
        skipped: true,
        reason: 'multi_date_range_sync_not_supported_for_live_mirror',
      };
    }

    const response = {
      success: true,
      startDate,
      endDate,
      total: jobs.length,
      inserted,
      updated,
      snapshotDeleted: markedDeletedFromSnapshot,
      totalEventCount: expectedCount,
      overallVisitTotal: data.overallVisitTotal || null,
      liveMirror,
    };
    if (tokenWarning) response.tokenWarning = tokenWarning;
    if (debug) {
      response.debug = {
        copilotStatus: data.status,
        htmlLength: (data.html || '').length,
        htmlPreview: (data.html || '').substring(0, 500),
        employeesCount: (data.employees || []).length,
        rawKeys: Object.keys(data),
        cookieHeaderLength: tokenInfo.cookieHeader.length,
        cookieHeaderPreview: tokenInfo.cookieHeader.substring(0, 80) + '...',
      };
    }

      res.json(response);
    } catch (error) {
      serverError(res, error, 'CopilotCRM sync failed');
    }
  });

// GET/POST CopilotCRM settings — view and update auth cookies
router.get('/api/copilot/settings', authenticateToken, async (req, res) => {
  try {
    const tokenInfo = await getCopilotToken(pool);
    res.json({
      success: true,
      hasCookies: !!tokenInfo,
      expiresAt: tokenInfo?.expiresAt || null,
      daysUntilExpiry: tokenInfo?.daysUntilExpiry ? Math.round(tokenInfo.daysUntilExpiry) : null,
    });
  } catch (error) {
    serverError(res, error, 'CopilotCRM settings fetch failed');
  }
});

router.post('/api/copilot/settings', authenticateToken, async (req, res) => {
  const { cookies } = req.body;
  if (!cookies || typeof cookies !== 'string') {
    return res.status(400).json({ success: false, error: 'cookies string is required' });
  }
  try {
    await pool.query(
      `INSERT INTO copilot_sync_settings (key, value, updated_at) VALUES ('copilot_cookies', $1, NOW()) ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()`,
      [cookies.trim()]
    );
    // Verify the token works
    const tokenInfo = await getCopilotToken(pool);
    console.log(`✅ CopilotCRM cookies updated. Expires: ${tokenInfo?.expiresAt || 'unknown'}`);
    res.json({
      success: true,
      message: 'CopilotCRM cookies updated',
      expiresAt: tokenInfo?.expiresAt || null,
      daysUntilExpiry: tokenInfo?.daysUntilExpiry ? Math.round(tokenInfo.daysUntilExpiry) : null,
    });
  } catch (error) {
    serverError(res, error, 'CopilotCRM settings update failed');
  }
});

// ═══════════════════════════════════════════════════════════════
// COPILOT — automated cookie refresh via API login
// ═══════════════════════════════════════════════════════════════

router.post('/api/copilot/refresh-cookies', authenticateToken, async (req, res) => {
  const username = process.env.COPILOT_USERNAME || process.env.COPILOTCRM_USERNAME;
  const password = process.env.COPILOT_PASSWORD || process.env.COPILOTCRM_PASSWORD;
  if (!username || !password) {
    return res.status(500).json({ success: false, error: 'COPILOT_USERNAME and COPILOT_PASSWORD env vars are not set' });
  }

  try {
    console.log('🔄 CopilotCRM cookie refresh: logging in via API...');
    const cookieJar = new Map(); // name → value

    // Step 1: API login to get accessToken (same as contract signing)
    const loginRes = await fetch('https://api.copilotcrm.com/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
      body: JSON.stringify({ username, password }),
    });

    const loginText = await loginRes.text();
    let loginData;
    try { loginData = JSON.parse(loginText); } catch (e) {
      throw new Error(`CopilotCRM login returned non-JSON (status ${loginRes.status}): ${loginText.substring(0, 200)}`);
    }

    if (!loginData.accessToken) {
      throw new Error(`CopilotCRM login failed (status ${loginRes.status}): ${loginText.substring(0, 200)}`);
    }

    cookieJar.set('copilotApiAccessToken', loginData.accessToken);

    // Capture any Set-Cookie headers from the API login
    const apiSetCookies = loginRes.headers.getSetCookie?.() || [];
    for (const sc of apiSetCookies) {
      const [pair] = sc.split(';');
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) cookieJar.set(pair.substring(0, eqIdx).trim(), pair.substring(eqIdx + 1).trim());
    }

    // Step 2: Hit secure.copilotcrm.com with the token to establish a full session
    // The scheduler endpoint may require session cookies that only come from the web app domain
    const sessionCookie = `copilotApiAccessToken=${loginData.accessToken}`;
    const sessionRes = await fetch('https://secure.copilotcrm.com/dashboard', {
      method: 'GET',
      headers: {
        'Cookie': sessionCookie,
        'Origin': 'https://secure.copilotcrm.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
      redirect: 'manual', // Don't follow redirects — we want the Set-Cookie headers
    });

    const webSetCookies = sessionRes.headers.getSetCookie?.() || [];
    for (const sc of webSetCookies) {
      const [pair] = sc.split(';');
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) cookieJar.set(pair.substring(0, eqIdx).trim(), pair.substring(eqIdx + 1).trim());
    }
    console.log(`🔑 CopilotCRM session: API login cookies=${apiSetCookies.length}, web session cookies=${webSetCookies.length}, total unique=${cookieJar.size}`);

    // Step 3: Also try the scheduler page to grab any scheduler-specific session cookies
    const fullCookieSoFar = [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
    const schedRes = await fetch('https://secure.copilotcrm.com/scheduler', {
      method: 'GET',
      headers: {
        'Cookie': fullCookieSoFar,
        'Origin': 'https://secure.copilotcrm.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
      redirect: 'manual',
    });

    const schedSetCookies = schedRes.headers.getSetCookie?.() || [];
    for (const sc of schedSetCookies) {
      const [pair] = sc.split(';');
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) cookieJar.set(pair.substring(0, eqIdx).trim(), pair.substring(eqIdx + 1).trim());
    }
    if (schedSetCookies.length > 0) console.log(`🔑 CopilotCRM scheduler page added ${schedSetCookies.length} more cookies`);

    // Build final cookie string
    const cookieString = [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');

    // Store in copilot_sync_settings
    await pool.query(
      `INSERT INTO copilot_sync_settings (key, value, updated_at) VALUES ('copilot_cookies', $1, NOW()) ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()`,
      [cookieString]
    );

    // Quick verification: try the actual scheduler endpoint
    const testFormData = new URLSearchParams();
    testFormData.append('accessFrom', 'route');
    testFormData.append('bs4', '1');
    const today = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    testFormData.append('sDate', today);
    testFormData.append('eDate', today);
    testFormData.append('count', '-1');
    for (const t of ['1', '2', '3', '4', '5', '0']) testFormData.append('evtypes_route[]', t);
    testFormData.append('erec', 'all');
    testFormData.append('estatus', 'any');
    testFormData.append('einvstatus', 'any');

    const verifyRes = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': cookieString,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest',
      },
      body: testFormData.toString(),
    });
    let verifyEventCount = null;
    if (verifyRes.ok) {
      try {
        const verifyData = await verifyRes.json();
        verifyEventCount = verifyData.totalEventCount || 0;
        console.log(`✅ CopilotCRM scheduler verification: ${verifyEventCount} events for today`);
      } catch { /* non-JSON response */ }
    } else {
      console.log(`⚠️ CopilotCRM scheduler verification returned ${verifyRes.status}`);
    }

    const tokenInfo = await getCopilotToken(pool);
    console.log(`✅ CopilotCRM cookies refreshed. ${cookieJar.size} cookies stored. Expires: ${tokenInfo?.expiresAt || 'unknown'}`);

    res.json({
      success: true,
      message: 'CopilotCRM cookies refreshed via API login',
      cookieCount: cookieJar.size,
      verifyEventCount,
      expiresAt: tokenInfo?.expiresAt || null,
      daysUntilExpiry: tokenInfo?.daysUntilExpiry ? Math.round(tokenInfo.daysUntilExpiry) : null,
    });
  } catch (error) {
    console.error('❌ CopilotCRM cookie refresh failed:', error.message);
    serverError(res, error, 'CopilotCRM cookie refresh failed');
  }
});
  return router;
};
