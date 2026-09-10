const express = require('express');
const { fetchHomeWorksIntegrationStatus } = require('../services/homeworks/client');

function createHomeWorksRoutes({
  pool,
  authenticateToken,
  serverError,
  fetchImpl = fetch,
  statusProvider = fetchHomeWorksIntegrationStatus,
}) {
  const router = express.Router();

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

