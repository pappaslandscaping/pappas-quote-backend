const { buildAssistantContext, localDate } = require('../services/homeworks/assistant-context');
const { createTrustedAssistantRoutes } = require('../routes/app-ai-trusted');

test('uses New York date and excludes cancelled jobs from a verified schedule', async () => {
  expect(localDate(new Date('2026-09-22T02:00:00Z'))).toBe('2026-09-21');
  const context = await buildAssistantContext({
    pool: {}, question: 'Who is scheduled today?', now: new Date('2026-09-21T15:00:00Z'),
    fetchToday: async () => ({ jobs: [{ id: 1, status: 'OPEN' }, { id: 2, status: 'CANCELLED' }] }),
  });
  expect(context.context.schedule.date).toBe('2026-09-21');
  expect(context.context.schedule.jobs).toEqual([{ id: 1, status: 'OPEN' }]);
});

test('preserves invoice balance and marks truncated customer data', async () => {
  const context = await buildAssistantContext({
    pool: {}, question: 'What does Mary Shamray owe on invoices?',
    fetchCustomers: async () => [{ id: 3, name: 'Mary Shamray' }],
    fetchSnapshot: async () => ({ customer: { name: 'Mary Shamray' }, jobs: [], invoices: Array.from({ length: 25 }, (_, i) => ({ number: i + 1, total: 100, paidAmount: 40, balance: 60 })) }),
  });
  expect(context.context.customer.invoices[0].balance).toBe(60);
  expect(context.context.customer.invoicesLimited).toBe(true);
  expect(context.sources[0].status).toBe('verified');
});

test('ambiguous customers and unavailable HomeWorks do not produce invented facts', async () => {
  const ambiguous = await buildAssistantContext({ pool: {}, question: 'Invoice for John Smith', fetchCustomers: async () => [{ id: 1, name: 'John Smith' }, { id: 2, name: 'John Smith' }] });
  expect(ambiguous.context.customerAmbiguity).toHaveLength(2);
  const unavailable = await buildAssistantContext({ pool: {}, question: 'Jobs today', fetchToday: async () => { throw new Error('offline'); } });
  expect(unavailable.sources[0].status).toBe('unavailable');
  expect(unavailable.requiresVerification).toBe(true);
  const vagueDate = await buildAssistantContext({ pool: {}, question: 'What jobs are on Friday?', fetchToday: jest.fn() });
  expect(vagueDate.context.dateNeedsClarification).toBe(true);
});

test('assistant route abstains when official records are unavailable', async () => {
  const generateAppAiText = jest.fn();
  const router = createTrustedAssistantRoutes({
    pool: {}, authenticateToken: (_req, _res, next) => next(),
    serverError: (res) => res.status(500).json({ success: false }), generateAppAiText,
    buildContext: async () => ({ checkedAt: '2026-09-21T15:00:00Z', context: {}, sources: [{ name: 'HomeWorks schedule', status: 'unavailable' }], requiresVerification: true }),
  });
  const handler = router.stack.find((layer) => layer.route?.path === '/api/app/ai/assistant-v2').route.stack.at(-1).handle;
  const response = { json: jest.fn((payload) => payload), status: jest.fn(function status() { return this; }) };
  const payload = await handler({ body: { question: 'Jobs today?' } }, response);
  expect(payload.answer).toMatch(/could not verify/i);
  expect(generateAppAiText).not.toHaveBeenCalled();
});
