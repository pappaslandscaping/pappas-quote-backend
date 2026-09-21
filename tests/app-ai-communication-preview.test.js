const { createCommunicationPreviewRoutes } = require('../routes/app-ai-communication-preview');

function handlerFor(options) {
  const router = createCommunicationPreviewRoutes({
    pool: { query: jest.fn(async () => ({ rows: [] })) },
    authenticateToken: (_req, _res, next) => next(),
    serverError: (res) => res.status(500).json({ success: false }),
    fetchVoicemail: async () => null,
    generateJson: jest.fn(),
    ...options,
  });
  return router.stack.find((layer) => layer.route?.path === '/api/app/ai/communication-preview').route.stack.at(-1).handle;
}

function fakeResponse() {
  return { statusCode: 200, status(code) { this.statusCode = code; return this; }, json(payload) { return payload; } };
}

test('requires a real source and does not use AI when a transcript is missing', async () => {
  const generateJson = jest.fn();
  const handler = handlerFor({ fetchVoicemail: async () => ({ id: 7, from_number: '+12165550100', transcription: null, created_at: '2026-09-21T10:00:00Z' }), generateJson });
  const response = fakeResponse();
  const result = await handler({ body: { sourceType: 'voicemail', sourceId: '7' } }, response);
  expect(response.statusCode).toBe(200);
  expect(result.needsDetails).toBe(true);
  expect(result.callNoteDraft).toBeNull();
  expect(result.saved).toBe(false);
  expect(generateJson).not.toHaveBeenCalled();
});

test('returns a review-only preview of verified voicemail content', async () => {
  const generateJson = jest.fn(async () => ({ json: {
    summary: 'Customer asks about shrub trimming.', urgency: 'normal', intent: 'service request',
    missingDetails: ['Preferred date'], suggestedAction: 'Ask for preferred date.',
    callNoteDraft: 'Customer requested shrub trimming; date not yet confirmed.',
    replyDraft: 'Thanks for calling. What date works for you?', confidence: 0.9,
  } }));
  const handler = handlerFor({ fetchVoicemail: async () => ({ id: 7, from_number: '+12165550100', transcription: 'I need shrub trimming.', created_at: '2026-09-21T10:00:00Z' }), generateJson });
  const result = await handler({ body: { sourceType: 'voicemail', sourceId: '7' } }, fakeResponse());
  expect(result.summary).toMatch(/shrub trimming/);
  expect(result.callNoteDraft).toMatch(/date not yet confirmed/);
  expect(result.saved).toBe(false);
  expect(result.sent).toBe(false);
  expect(generateJson).toHaveBeenCalledTimes(1);
});

test('rejects invalid source and does not guess when a call is not found', async () => {
  const handler = handlerFor({});
  const invalidResponse = fakeResponse();
  await handler({ body: { sourceType: 'text', sourceId: '7' } }, invalidResponse);
  expect(invalidResponse.statusCode).toBe(400);
  const missingResponse = fakeResponse();
  const missing = await handler({ body: { sourceType: 'call', sourceId: 'CA000' } }, missingResponse);
  expect(missingResponse.statusCode).toBe(404);
  expect(missing.error).toBe('Communication not found.');
});
