'use strict';
const express = require('express');
const { getHomeWorksAccessToken, tokenExpiry } = require('../services/homeworks/auth');
const { fetchHomeWorksEvents, fetchHomeWorksIntegrationStatus } = require('../services/homeworks/client');
const { getCopilotLiveJobs, upsertCopilotLiveJobs } = require('../services/copilot/live-jobs');
const {
  loadCopilotSettings,
  saveCopilotSettings,
  syncCopilotInvoices,
} = require('../lib/copilot-live-invoices');
module.exports = function ({ pool, serverError, authenticateToken, fetchImpl = fetch }) {
  const router = express.Router();
  const wrap = (fn) => async (req, res) => {
    try {
      await fn(req, res);
    } catch (error) {
      serverError(res, error, 'Homeworks integration failed');
    }
  };
  router.get(
    '/api/copilot/live-jobs',
    authenticateToken,
    wrap(async (req, res) =>
      res.json({
        success: true,
        ...(await getCopilotLiveJobs({
          poolClient: pool,
          date: req.query.date,
          startDate: req.query.start_date || null,
          endDate: req.query.end_date || null,
          fetchImpl,
        })),
      }),
    ),
  );
  router.get(
    '/api/copilot/schedule-live-debug',
    authenticateToken,
    wrap(async (req, res) => {
      const date =
        req.query.date ||
        new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date());
      const jobs = await fetchHomeWorksEvents({ pool, startDate: date, endDate: date, fetchImpl });
      res.json({ success: true, source: 'official_homeworks_graphql', date, total_jobs: jobs.length });
    }),
  );
  router.post(
    '/api/copilot/sync',
    authenticateToken,
    wrap(async (req, res) => {
      const startDate =
          req.body.startDate ||
          new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York' }).format(new Date()),
        endDate = req.body.endDate || startDate;
      const rows = await fetchHomeWorksEvents({ pool, startDate, endDate, fetchImpl });
      const dates = [];
      for (
        let day = new Date(startDate + 'T12:00:00Z');
        day <= new Date(endDate + 'T12:00:00Z');
        day.setUTCDate(day.getUTCDate() + 1)
      ) {
        dates.push(day.toISOString().slice(0, 10));
        if (dates.length > 62) throw new Error('Sync at most 62 days at a time.');
      }
      for (const date of dates)
        await upsertCopilotLiveJobs(pool, {
          serviceDate: date,
          jobs: rows.filter((row) => row.service_date === date),
          syncedAt: new Date(),
        });
      res.json({
        success: true,
        source: 'official_homeworks_graphql',
        startDate,
        endDate,
        total: rows.length,
        liveMirror: { dates: dates.length },
      });
    }),
  );
  router.post(
    '/api/copilot/invoices/sync',
    authenticateToken,
    wrap(async (req, res) => {
      const result = await syncCopilotInvoices({
        pool,
        settings: await loadCopilotSettings(pool),
        linkCustomers: req.body.linkCustomers !== false,
      });
      res.json({
        ...result,
        total: result.matched,
        inserted: result.synced.filter((row) => row.action === 'inserted').length,
        updated: result.synced.filter((row) => row.action === 'updated').length,
      });
    }),
  );
  router.get(
    '/api/copilot/settings',
    authenticateToken,
    wrap(async (req, res) => {
      const token = await getHomeWorksAccessToken(pool);
      res.json({
        success: true,
        source: 'official_homeworks_graphql',
        hasCookies: !!token,
        hasApiToken: !!token,
        ...tokenExpiry(token),
      });
    }),
  );
  router.post(
    '/api/copilot/settings',
    authenticateToken,
    wrap(async (req, res) => {
      const settings = await saveCopilotSettings(pool, {
        accessToken: req.body.accessToken || req.body.token,
        cookies: req.body.cookies,
      });
      res.json({ success: true, hasApiToken: !!settings.accessToken, source: settings.source });
    }),
  );
  router.post('/api/copilot/refresh-cookies', authenticateToken, (req, res) =>
    res
      .status(410)
      .json({
        success: false,
        error:
          'Browser-session refresh has been retired. Update the official Homeworks API token in Settings.',
      }),
  );
  return router;
};
