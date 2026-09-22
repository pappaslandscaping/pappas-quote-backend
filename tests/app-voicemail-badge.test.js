const { createAppVoicemailBadgeRoutes } = require('../routes/app-voicemail-badge');

function handlerFor(fetchImpl) {
  const router = createAppVoicemailBadgeRoutes({
    authenticateToken: (_req, _res, next) => next(),
    webhookBase: 'https://example.test',
    fetchImpl,
  });
  return router.stack.find((layer) => layer.route?.path === '/api/app/voicemails/unheard-count').route.stack.at(-1).handle;
}

function fakeResponse() {
  return {
    statusCode: 200,
    body: null,
    status(code) { this.statusCode = code; return this; },
    json(payload) { this.body = payload; return payload; },
  };
}

test('counts unheard active voicemails without customer enrichment', async () => {
  const fetchImpl = jest.fn(async () => ({ ok: true, json: async () => ({ calls: [{ read: false }, { read: true }, { read: false }] }) }));
  const response = fakeResponse();
  await handlerFor(fetchImpl)({}, response);
  expect(response.statusCode).toBe(200);
  expect(response.body).toEqual({ count: 2 });
  expect(fetchImpl).toHaveBeenCalledWith('https://example.test/api/calls?status=voicemail&limit=1000', expect.any(Object));
});

test('does not show a misleading zero when the voicemail feed fails', async () => {
  const warning = jest.spyOn(console, 'warn').mockImplementation(() => {});
  const response = fakeResponse();
  await handlerFor(async () => ({ ok: false, status: 503 }))({}, response);
  expect(response.statusCode).toBe(502);
  expect(response.body).toEqual({ error: 'Voicemail count unavailable' });
  warning.mockRestore();
});
