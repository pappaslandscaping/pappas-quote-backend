jest.mock('../services/copilot/client', () => ({
  getCopilotToken: jest.fn(),
  fetchCopilotRouteJobsForDate: jest.fn(),
}));

jest.mock('../services/copilot/live-jobs', () => {
  const actual = jest.requireActual('../services/copilot/live-jobs');
  return {
    ...actual,
    fetchResolvedLiveJobs: jest.fn(),
    getCopilotLiveJobs: jest.fn(),
    patchDispatchPlanItems: jest.fn(),
  };
});

const {
  fetchResolvedLiveJobs,
  getCopilotLiveJobs,
  patchDispatchPlanItems,
} = require('../services/copilot/live-jobs');
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
      read_only: false,
      write_capabilities: {
        assign: true,
        route_order: true,
        geocode: true,
        optimize: true,
        complete: false,
        add_job: false,
      },
      crews: [{
        name: 'Crew A',
        jobCount: 1,
        jobs: [{ id: 'copilot:2026-04-20:45002620' }],
      }],
      unassigned: [{ id: 'copilot:2026-04-20:45002621' }],
    });
  });

  test('persists live dispatch assignment overrides by Copilot job key', async () => {
    patchDispatchPlanItems.mockResolvedValue([
      {
        dispatchJob: {
          id: 'copilot:2026-04-20:45002620',
          crew_assigned: 'Crew A',
          route_order: 2,
        },
      },
    ]);

    const router = createJobRoutes({
      pool: { query: jest.fn() },
      serverError: jest.fn(),
      authenticateToken: (_req, _res, next) => next(),
      nextInvoiceNumber: jest.fn(),
      upload: { single: () => (_req, _res, next) => next() },
      fetchImpl: jest.fn(),
    });

    const res = await invokeRoute(router, '/api/dispatch/assign', 'patch', {
      body: {
        assignments: [{
          job_key: 'copilot:2026-04-20:45002620',
          crew_assigned: 'Crew A',
          route_order: 2,
        }],
      },
      user: { id: 17, name: 'Theresa' },
    });

    expect(patchDispatchPlanItems).toHaveBeenCalledWith(expect.any(Object), expect.objectContaining({
      updatedByUserId: 17,
      updatedByName: 'Theresa',
      patches: [{
        jobKey: 'copilot:2026-04-20:45002620',
        patch: {
          crew_override_name: 'Crew A',
          route_order_override: 2,
        },
      }],
    }));
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({
      success: true,
      updated: 1,
      jobs: [{ id: 'copilot:2026-04-20:45002620' }],
    });
  });

  test('geocodes live jobs into dispatch plan items instead of scheduled_jobs', async () => {
    const originalMapsKey = process.env.GOOGLE_MAPS_API_KEY;
    process.env.GOOGLE_MAPS_API_KEY = 'test-key';
    fetchResolvedLiveJobs.mockResolvedValue([
      {
        job_key: 'copilot:2026-04-20:45002620',
        service_date: '2026-04-20',
        source: {
          event_id: '45002620',
          synced_at: '2026-04-20T14:00:00.000Z',
          deleted_at_source: null,
          customer_id: '9001',
          customer_name: 'Angela Ziegler',
          job_title: 'Mowing',
          status: 'Open',
          visit_total: 36,
          crew_name: 'Crew A',
          employees_text: null,
          stop_order: 1,
          address: '3445 West 131st Street Cleveland OH 44111, US',
          property_name: null,
          event_type: null,
          invoiceable: null,
          frequency: null,
          last_serviced: null,
          notes: null,
          tracked_time: null,
          budgeted_hours: null,
          service_date_label: null,
          source_surface: 'schedule_grid',
        },
        overlay: {
          exists: false,
          review_state: 'new',
          office_note: null,
          hold_from_dispatch: false,
          local_tags: [],
          address_override: null,
          customer_link_id: null,
          property_link_id: null,
          updated_at: null,
          updated_by_name: null,
        },
        dispatch_plan: {
          exists: false,
          crew_override_name: null,
          route_order_override: null,
          route_locked: false,
          map_lat: null,
          map_lng: null,
          map_source: 'none',
          map_quality: 'missing',
          print_group_key: null,
          print_note: null,
          updated_at: null,
          updated_by_name: null,
        },
        resolved: {
          effective_address: '3445 West 131st Street Cleveland OH 44111, US',
          effective_crew_name: 'Crew A',
          effective_route_order: 1,
          included_in_dispatch: true,
          map_lat: null,
          map_lng: null,
          map_quality: 'missing',
        },
        flags: {
          needs_address_review: false,
          needs_crew_review: false,
          needs_route_review: false,
          source_deleted: false,
        },
      },
    ]);
    patchDispatchPlanItems.mockResolvedValue([]);
    const fetchImpl = jest.fn().mockResolvedValue({
      json: async () => ({
        status: 'OK',
        results: [{
          types: ['street_address'],
          geometry: { location: { lat: 41.45, lng: -81.78 } },
        }],
      }),
    });

    const router = createJobRoutes({
      pool: { query: jest.fn() },
      serverError: jest.fn(),
      authenticateToken: (_req, _res, next) => next(),
      nextInvoiceNumber: jest.fn(),
      upload: { single: () => (_req, _res, next) => next() },
      fetchImpl,
    });

    try {
      const res = await invokeRoute(router, '/api/dispatch/geocode', 'post', {
        body: { jobKeys: ['copilot:2026-04-20:45002620'] },
        user: { id: 17, name: 'Theresa' },
      });

      expect(fetchResolvedLiveJobs).toHaveBeenCalledWith(expect.any(Object), {
        jobKeys: ['copilot:2026-04-20:45002620'],
        includeDeleted: false,
      });
      expect(fetchImpl).toHaveBeenCalledWith(expect.stringContaining('maps.googleapis.com/maps/api/geocode/json'));
      expect(patchDispatchPlanItems).toHaveBeenCalledWith(expect.any(Object), expect.objectContaining({
        updatedByName: 'Theresa',
        patches: [{
          jobKey: 'copilot:2026-04-20:45002620',
          patch: {
            map_lat: 41.45,
            map_lng: -81.78,
            map_source: 'geocoded',
            map_quality: 'street',
            map_address_input: '3445 West 131st Street Cleveland OH 44111, US',
          },
        }],
      }));
      expect(res.body).toMatchObject({
        success: true,
        geocoded: 1,
        total: 1,
      });
    } finally {
      if (originalMapsKey === undefined) delete process.env.GOOGLE_MAPS_API_KEY;
      else process.env.GOOGLE_MAPS_API_KEY = originalMapsKey;
    }
  });

  test('optimizes live dispatch routes and persists route_order_override by job key', async () => {
    getCopilotLiveJobs.mockResolvedValue({
      jobs: [
        {
          id: 'copilot:2026-04-20:job-1',
          job_key: 'copilot:2026-04-20:job-1',
          crew_assigned: 'Crew A',
          estimated_duration: 30,
          lat: 41.45,
          lng: -81.78,
        },
        {
          id: 'copilot:2026-04-20:job-2',
          job_key: 'copilot:2026-04-20:job-2',
          crew_assigned: 'Crew A',
          estimated_duration: 45,
          lat: 41.46,
          lng: -81.79,
        },
      ],
    });
    patchDispatchPlanItems.mockResolvedValue([]);
    const pool = {
      query: jest.fn().mockResolvedValue({ rows: [] }),
    };
    const router = createJobRoutes({
      pool,
      serverError: jest.fn(),
      authenticateToken: (_req, _res, next) => next(),
      nextInvoiceNumber: jest.fn(),
      upload: { single: () => (_req, _res, next) => next() },
      fetchImpl: jest.fn(),
    });

    const res = await invokeRoute(router, '/api/dispatch/optimize-route', 'post', {
      body: { date: '2026-04-20', crew_name: 'Crew A' },
      user: { id: 17, name: 'Theresa' },
    });

    expect(getCopilotLiveJobs).toHaveBeenCalledWith(expect.objectContaining({
      poolClient: pool,
      date: '2026-04-20',
    }));
    expect(patchDispatchPlanItems).toHaveBeenCalledWith(pool, expect.objectContaining({
      updatedByName: 'Theresa',
      patches: [
        { jobKey: 'copilot:2026-04-20:job-1', patch: { route_order_override: 1 } },
        { jobKey: 'copilot:2026-04-20:job-2', patch: { route_order_override: 2 } },
      ],
    }));
    expect(res.body).toMatchObject({
      success: true,
      optimized: [
        { job_key: 'copilot:2026-04-20:job-1', route_order: 1 },
        { job_key: 'copilot:2026-04-20:job-2', route_order: 2 },
      ],
    });
  });
});
