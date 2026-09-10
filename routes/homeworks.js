const express = require('express');
const {
  fetchHomeWorksBusinessSummary,
  fetchHomeWorksIntegrationStatus,
} = require('../services/homeworks/client');

function createHomeWorksRoutes({
  pool,
  authenticateToken,
  serverError,
  fetchImpl = fetch,
  summaryProvider = fetchHomeWorksBusinessSummary,
  statusProvider = fetchHomeWorksIntegrationStatus,
}) {
  const router = express.Router();
  let summaryCache = null;
  const summaryCacheMs = 2 * 60 * 1000;

  router.get('/api/homeworks/summary', authenticateToken, async (req, res) => {
    try {
      const now = Date.now();
      if (!summaryCache || now - summaryCache.cachedAt >= summaryCacheMs) {
        summaryCache = {
          cachedAt: now,
          value: await summaryProvider({ pool, fetchImpl }),
        };
      }
      res.json({ success: true, ...summaryCache.value });
    } catch (error) {
      serverError(res, error);
    }
  });

  router.get('/api/homeworks/status', authenticateToken, async (req, res) => {
    try {
      const status = await statusProvider({ pool, fetchImpl });
      res.json({ success: true, ...status });
    } catch (error) {
      serverError(res, error);
    }
  });

  return router;
}

module.exports = createHomeWorksRoutes;
