jest.mock('../services/copilot/live-jobs', () => ({
  getCopilotLiveJobs: jest.fn(),
}));

const { getCopilotLiveJobs } = require('../services/copilot/live-jobs');
const createCopilotRoutes = require('../routes/copilot');

function makeResponse() {
  return {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
}

function findRoute(router, path, method) {
  return router.stack.find((layer) => layer.route?.path === path && layer.route.methods?.[method]);
}

async function invokeRoute(router, path, method, reqOverrides = {}) {
  const layer = findRoute(router, path, method);
  if (!layer) throw new Error(`Route not found: ${method.toUpperCase()} ${path}`);

  const req = {
    method: method.toUpperCase(),
    headers: {},
    query: {},
    body: {},
    user: null,
    get(name) {
      const key = Object.keys(this.headers).find((candidate) => candidate.toLowerCase() === String(name).toLowerCase());
      return key ? this.headers[key] : undefined;
    },
    ...reqOverrides,
  };
  const res = makeResponse();

  for (const stackItem of layer.route.stack) {
    let nextCalled = false;
    await new Promise((resolve, reject) => {
      const next = (error) => {
        nextCalled = true;
        if (error) reject(error);
        else resolve();
      };

      Promise.resolve(stackItem.handle(req, res, next))
        .then(() => {
          if (!nextCalled) resolve();
        })
        .catch(reject);
    });

    if (!nextCalled) break;
  }

  return res;
}

describe('copilot live jobs route', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('returns the shared live-job payload for schedule consumers', async () => {
    const noop = (_req, _res, next) => next();
    const pool = { query: jest.fn() };
    getCopilotLiveJobs.mockResolvedValue({
      start_date: '2026-04-18',
      end_date: '2026-04-18',
      freshness: {
        source: 'live',
        fetched_at: '2026-04-18T13:30:00.000Z',
        stale: false,
        per_date: [{ date: '2026-04-18', source: 'live', fetched_at: '2026-04-18T13:30:00.000Z', error: null }],
      },
      stats: {
        total: 1,
        byStatus: { pending: 1 },
        byCrew: { 'Crew A': 1 },
        totalRevenue: 75,
      },
      days: [{
        day: '2026-04-18',
        total_jobs: 1,
        completed: 0,
        pending: 1,
        in_progress: 0,
        skipped: 0,
        cancelled: 0,
        revenue: 75,
        crews: { 'Crew A': 1 },
      }],
      jobs: [{
        id: 'copilot:2026-04-18:visit:visit-88',
        source_system: 'copilot',
        service_date: '2026-04-18',
      }],
    });

    const router = createCopilotRoutes({
      pool,
      serverError: jest.fn(),
      authenticateToken: noop,
      fetchImpl: jest.fn(),
    });

    const res = await invokeRoute(router, '/api/copilot/live-jobs', 'get', {
      query: { date: '2026-04-18' },
    });

    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({
      success: true,
      start_date: '2026-04-18',
      end_date: '2026-04-18',
      freshness: { source: 'live', stale: false },
      stats: { total: 1 },
      jobs: [{ id: 'copilot:2026-04-18:visit:visit-88' }],
    });
    expect(getCopilotLiveJobs).toHaveBeenCalledWith(expect.objectContaining({
      poolClient: pool,
      date: '2026-04-18',
      startDate: null,
      endDate: null,
    }));
  });
});
