jest.mock('../services/copilot/client', () => ({
  getCopilotToken: jest.fn(),
  fetchCopilotRouteJobsForDate: jest.fn(),
}));

jest.mock('../services/copilot/live-jobs', () => {
  const actual = jest.requireActual('../services/copilot/live-jobs');
  return {
    ...actual,
    getCopilotLiveJobs: jest.fn(),
  };
});

const { getCopilotLiveJobs } = require('../services/copilot/live-jobs');
const createJobRoutes = require('../routes/jobs');

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

describe('dispatch live board route', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('derives the dispatch board from the shared live Copilot job model', async () => {
    const pool = {
      query: jest.fn().mockResolvedValueOnce({
        rows: [{ id: 7, name: 'Crew A', members: 'Tim, Rob', color: '#059669' }],
      }),
    };

    getCopilotLiveJobs.mockResolvedValue({
      freshness: {
        source: 'live',
        stale: false,
        fetched_at: '2026-04-20T13:30:00.000Z',
        per_date: [{ date: '2026-04-20', source: 'live', fetched_at: '2026-04-20T13:30:00.000Z', error: null }],
      },
      jobs: [
        {
          id: 'copilot:2026-04-20:45002620',
          source_system: 'copilot',
          source_kind: 'live_schedule',
          job_date: '2026-04-20',
          service_date: '2026-04-20',
          visit_id: '45002620',
          customer_name: 'Angela Ziegler',
          address: '3445 West 131st Street Cleveland OH 44111, US',
          service_type: 'Mowing',
          service_price: 36,
          crew_assigned: 'Crew A',
          route_order: 1,
          estimated_duration: 30,
          status: 'pending',
          lat: 41.45,
          lng: -81.78,
          geocode_quality: 'street',
          hold_from_dispatch: false,
        },
        {
          id: 'copilot:2026-04-20:45002621',
          source_system: 'copilot',
          source_kind: 'live_schedule',
          job_date: '2026-04-20',
          service_date: '2026-04-20',
          visit_id: '45002621',
          customer_name: 'Unassigned Customer',
          address: '1 Main St, Lakewood OH',
          service_type: 'Cleanup',
          service_price: 120,
          crew_assigned: null,
          route_order: null,
          estimated_duration: 45,
          status: 'completed',
          hold_from_dispatch: false,
        },
      ],
    });

    const router = createJobRoutes({
      pool,
      serverError: jest.fn(),
      authenticateToken: (_req, _res, next) => next(),
      nextInvoiceNumber: jest.fn(),
      upload: { single: () => (_req, _res, next) => next() },
      fetchImpl: jest.fn(),
    });

    const res = await invokeRoute(router, '/api/dispatch/board', 'get', {
      query: { date: '2026-04-20', view: 'day' },
    });

    expect(getCopilotLiveJobs).toHaveBeenCalledWith(expect.objectContaining({
      poolClient: pool,
      date: '2026-04-20',
      startDate: '2026-04-20',
      endDate: '2026-04-20',
    }));
    expect(pool.query).toHaveBeenCalledWith('SELECT * FROM crews ORDER BY name');
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({
      success: true,
      source_kind: 'live_dispatch',
      read_only: true,
      crews: [{
        name: 'Crew A',
        jobCount: 1,
        jobs: [{ id: 'copilot:2026-04-20:45002620' }],
      }],
      unassigned: [{ id: 'copilot:2026-04-20:45002621' }],
    });
  });
});
