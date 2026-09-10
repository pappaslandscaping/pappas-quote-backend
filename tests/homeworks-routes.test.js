const createHomeWorksRoutes = require('../routes/homeworks');

describe('HomeWorks integration routes', () => {
  test('registers an authenticated official business summary route', async () => {
    const summaryProvider = jest.fn().mockResolvedValue({
      source: 'official_homeworks_graphql',
      financials: { outstanding: 100 },
      counts: { customers: 3 },
    });
    const authenticateToken = jest.fn((req, res, next) => next());
    const router = createHomeWorksRoutes({
      pool: {},
      authenticateToken,
      serverError: (res, error) => res.status(500).json({ success: false, error: error.message }),
      summaryProvider,
    });
    const layer = router.stack.find((entry) => entry.route?.path === '/api/homeworks/summary');
    expect(layer.route.stack[0].handle).toBe(authenticateToken);

    const handler = layer.route.stack[1].handle;
    const json = jest.fn();
    await handler({}, { json });
    expect(json).toHaveBeenCalledWith(expect.objectContaining({
      success: true,
      source: 'official_homeworks_graphql',
      financials: { outstanding: 100 },
    }));
  });

  test('registers an authenticated official connection status route', async () => {
    const statusProvider = jest.fn().mockResolvedValue({
      connected: true,
      source: 'official_homeworks_graphql',
      company: { id: 5261, name: 'Pappas & Co. Landscaping' },
    });
    const authenticateToken = jest.fn((req, res, next) => next());
    const router = createHomeWorksRoutes({
      pool: {},
      authenticateToken,
      serverError: (res, error) => res.status(500).json({ success: false, error: error.message }),
      statusProvider,
    });
    const layer = router.stack.find((entry) => entry.route?.path === '/api/homeworks/status');
    expect(layer.route.stack[0].handle).toBe(authenticateToken);

    const handler = layer.route.stack[1].handle;
    const json = jest.fn();
    await handler({}, { json });
    expect(json).toHaveBeenCalledWith(expect.objectContaining({
      success: true,
      connected: true,
      source: 'official_homeworks_graphql',
    }));
  });
});
