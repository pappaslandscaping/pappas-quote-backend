jest.mock('../services/copilot/client', () => ({
  getCopilotToken: jest.fn(),
  fetchCopilotRouteJobsForDate: jest.fn(),
}));

const createJobRoutes = require('../routes/jobs');
const { getCopilotToken, fetchCopilotRouteJobsForDate } = require('../services/copilot/client');

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

function csvBuffer(lines) {
  return Buffer.from(lines.join('\n'), 'utf8');
}

function makeServerError() {
  return jest.fn((res, error) => {
    res.status(500).json({
      success: false,
      error: error?.message || String(error),
    });
  });
}

describe('schedule import Copilot linkage', () => {
  let consoleLogSpy;
  let consoleWarnSpy;

  beforeEach(() => {
    jest.clearAllMocks();
    consoleLogSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleLogSpy.mockRestore();
    consoleWarnSpy.mockRestore();
  });

  test('hydrates Copilot rows and links imported jobs when there is exactly one strict match', async () => {
    getCopilotToken.mockResolvedValue({ cookieHeader: 'copilot=abc' });
    fetchCopilotRouteJobsForDate.mockResolvedValue({
      sync_date: '2026-04-15',
      jobs: [{
        event_id: 'visit-101',
        customer_name: 'Jane Doe',
        job_title: 'Spring Cleanup',
        address: '123 Main St, Lakewood OH',
        visit_total: '$100.00',
        customer_id: 'crm-1',
      }],
    });

    const pool = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [{ id: 55, copilot_visit_id: null, copilot_job_id: null }] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] }),
    };

    const noop = (_req, _res, next) => next();
    const upload = { single: () => noop, array: () => noop, none: () => noop };
    const serverError = makeServerError();
    const router = createJobRoutes({
      pool,
      serverError,
      authenticateToken: noop,
      nextInvoiceNumber: jest.fn(),
      upload,
      fetchImpl: jest.fn(),
    });

    const res = await invokeRoute(router, '/api/import-scheduling', 'post', {
      file: {
        buffer: csvBuffer([
          'Date of Service,Title,Name / Details,Visit Total',
          '2026-04-15,Spring Cleanup,"Jane Doe 123 Main St  Lakewood OH","$100.00"',
        ]),
      },
    });

    expect(res.statusCode).toBe(200);
    expect(serverError).not.toHaveBeenCalled();
    expect(res.body.success).toBe(true);
    expect(res.body.copilot_hydration).toMatchObject({
      attempted: 1,
      hydrated_dates: 1,
      fetched: 1,
      inserted: 1,
      updated: 0,
      skipped: false,
    });
    expect(res.body.copilot_linkage).toMatchObject({
      attempted: 1,
      linked: 1,
      already_linked: 0,
      unmatched: 0,
      ambiguous: 0,
      conflict: 0,
    });
    expect(res.body.jobs[0]).toMatchObject({
      id: 55,
      copilot_visit_id: 'visit-101',
      copilot_linkage_status: 'linked',
      copilot_linkage_reason: null,
    });

    const [linkSql, linkVals] = pool.query.mock.calls[5];
    expect(linkSql).toContain('UPDATE scheduled_jobs SET copilot_visit_id = $1');
    expect(linkSql).not.toContain('status =');
    expect(linkVals).toEqual(['visit-101', 55]);
  });

  test('reports ambiguous Copilot matches and leaves imported jobs unlinked', async () => {
    getCopilotToken.mockResolvedValue({ cookieHeader: 'copilot=abc' });
    fetchCopilotRouteJobsForDate.mockResolvedValue({
      sync_date: '2026-04-15',
      jobs: [
        {
          event_id: 'visit-201',
          customer_name: 'Jane Doe',
          job_title: 'Spring Cleanup',
          address: '123 Main St, Lakewood OH',
          visit_total: '$100.00',
        },
        {
          event_id: 'visit-202',
          customer_name: 'Jane Doe',
          job_title: 'Spring Cleanup',
          address: '123 Main St, Lakewood OH',
          visit_total: '$100.00',
        },
      ],
    });

    const pool = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
        .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [{ id: 77, copilot_visit_id: null, copilot_job_id: null }] }),
    };

    const noop = (_req, _res, next) => next();
    const upload = { single: () => noop, array: () => noop, none: () => noop };
    const serverError = makeServerError();
    const router = createJobRoutes({
      pool,
      serverError,
      authenticateToken: noop,
      nextInvoiceNumber: jest.fn(),
      upload,
      fetchImpl: jest.fn(),
    });

    const res = await invokeRoute(router, '/api/import-scheduling', 'post', {
      file: {
        buffer: csvBuffer([
          'Date of Service,Title,Name / Details,Visit Total',
          '2026-04-15,Spring Cleanup,"Jane Doe 123 Main St  Lakewood OH","$100.00"',
        ]),
      },
    });

    expect(res.statusCode).toBe(200);
    expect(serverError).not.toHaveBeenCalled();
    expect(res.body.success).toBe(true);
    expect(res.body.copilot_linkage).toMatchObject({
      attempted: 1,
      linked: 0,
      already_linked: 0,
      unmatched: 0,
      ambiguous: 1,
      conflict: 0,
    });
    expect(res.body.copilot_linkage.diagnostics).toEqual([
      expect.objectContaining({
        row_index: 1,
        status: 'ambiguous',
        reason: 'multiple_copilot_candidates',
        candidate_visit_ids: ['visit-201', 'visit-202'],
      }),
    ]);
    expect(res.body.jobs[0]).toMatchObject({
      copilot_visit_id: null,
      copilot_linkage_status: 'ambiguous',
      copilot_linkage_reason: 'multiple_copilot_candidates',
    });

    expect(pool.query).toHaveBeenCalledTimes(5);
    const sqlCalls = pool.query.mock.calls.map(([sql]) => sql);
    expect(sqlCalls.some((sql) => sql.includes('copilot_visit_id = $1'))).toBe(false);
  });
});
