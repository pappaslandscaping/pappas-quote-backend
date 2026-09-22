const express = require('express');

function createAppVoicemailBadgeRoutes({ authenticateToken, webhookBase, fetchImpl = fetch }) {
  const router = express.Router();

  router.get('/api/app/voicemails/unheard-count', authenticateToken, async (_req, res) => {
    try {
      const response = await fetchImpl(`${webhookBase}/api/calls?status=voicemail&limit=1000`, {
        signal: AbortSignal.timeout(10000),
      });
      if (!response.ok) throw new Error(`Voicemail feed returned ${response.status}`);
      const payload = await response.json();
      if (!Array.isArray(payload.calls)) throw new Error('Voicemail feed was incomplete');
      res.json({ count: payload.calls.filter((call) => !call.read).length });
    } catch (error) {
      console.warn('Voicemail badge unavailable:', error.message);
      res.status(502).json({ error: 'Voicemail count unavailable' });
    }
  });

  return router;
}

module.exports = { createAppVoicemailBadgeRoutes };
