const createCommunicationRoutes = require('../routes/communications');

function makeResponse() {
  return {
    statusCode: 200,
    body: null,
    headers: {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
    send(payload) {
      this.body = payload;
      return this;
    },
    type(value) {
      this.headers['Content-Type'] = value;
      return this;
    },
    setHeader(name, value) {
      this.headers[name] = value;
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
    params: {},
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

function replaceTemplateVars(str, data) {
  if (!str) return str;
  return String(str).replace(/\{(\w+)\}/g, (match, key) => (
    data[key] !== undefined ? data[key] : match
  ));
}

function buildRouterWithQuerySpy({
  liveJobsPayload = { jobs: [], freshness: null },
  pool = { query: jest.fn().mockResolvedValue({ rows: [] }) },
  sendEmail = jest.fn(),
  twilioCreate = jest.fn().mockResolvedValue({ sid: 'SM123', status: 'queued' }),
} = {}) {
  const liveJobsProvider = jest.fn().mockResolvedValue(liveJobsPayload);

  const router = createCommunicationRoutes({
    pool,
    sendEmail,
    emailTemplate: (content) => content,
    renderManagedEmail: async (content) => content,
    escapeHtml: (value) => value,
    serverError: jest.fn(),
    twilioClient: { messages: { create: twilioCreate } },
    TWILIO_PHONE_NUMBER: '+14405550000',
    NOTIFICATION_EMAIL: 'hello@pappaslandscaping.com',
    replaceTemplateVars,
    liveJobsProvider,
  });

  return { pool, router, liveJobsProvider, sendEmail, twilioCreate };
}

describe('broadcast preview targeting query', () => {
  test('uses live-job activity first for active_since_months with scheduled_jobs fallback', async () => {
    const { pool, router } = buildRouterWithQuerySpy();

    const res = await invokeRoute(router, '/api/broadcasts/preview', 'post', {
      body: {
        channel: 'email',
        filters: { active_since_months: 6 },
      },
    });

    expect(res.statusCode).toBe(200);
    const [sql, params] = pool.query.mock.calls[0];
    expect(sql).toContain('FROM copilot_live_jobs clj');
    expect(sql).toContain('LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key');
    expect(sql).toContain('live_customer.customer_number = clj.source_customer_id');
    expect(sql).toContain("clj.service_date >= CURRENT_DATE - ($1::text || ' months')::INTERVAL");
    expect(sql).toContain("COALESCE(sj.job_date::date, sj.created_at::date) >= CURRENT_DATE - ($2::text || ' months')::INTERVAL");
    expect(params).toEqual([6, 6]);
  });

  test('uses live-job service_date first for job_date with scheduled_jobs fallback', async () => {
    const { pool, router, liveJobsProvider } = buildRouterWithQuerySpy();

    const res = await invokeRoute(router, '/api/broadcasts/preview', 'post', {
      body: {
        channel: 'sms',
        filters: { job_date: '2026-04-20' },
      },
    });

    expect(res.statusCode).toBe(200);
    expect(liveJobsProvider).toHaveBeenCalledWith(expect.objectContaining({
      poolClient: pool,
      date: '2026-04-20',
      startDate: '2026-04-20',
      endDate: '2026-04-20',
    }));
    const [sql, params] = pool.query.mock.calls[0];
    expect(sql).toContain('FROM copilot_live_jobs clj');
    expect(sql).toContain('SELECT fallback_customer.id');
    expect(sql).toContain('clj.service_date = $1::date');
    expect(sql).toContain('sj.job_date::date = $2::date');
    expect(params).toEqual(['2026-04-20', '2026-04-20']);
  });

  test('live customer name fallback resolves to one backend customer instead of broad name matches', async () => {
    const { pool, router } = buildRouterWithQuerySpy({
      liveJobsPayload: {
        jobs: [{ id: 'copilot:2026-04-30:visit-1', service_date: '2026-04-30' }],
        freshness: { source: 'live' },
      },
    });

    const res = await invokeRoute(router, '/api/broadcasts/preview', 'post', {
      body: {
        channel: 'sms',
        filters: { job_date: '2026-04-30' },
      },
    });

    expect(res.statusCode).toBe(200);
    const [sql] = pool.query.mock.calls.find(([query]) => query.includes('FROM customers c'));
    expect(sql).toContain('SELECT fallback_customer.id');
    expect(sql).toContain('ORDER BY fallback_customer.id ASC');
    expect(sql).not.toContain("LOWER(BTRIM(COALESCE(clj.customer_name, ''))) = LOWER(BTRIM(COALESCE(c.name, '')))");
  });

  test('does not fall back to stale scheduled_jobs when live dispatch has jobs for job_date', async () => {
    const { pool, router } = buildRouterWithQuerySpy({
      liveJobsPayload: {
        jobs: [{ id: 'copilot:2026-04-30:visit-1', service_date: '2026-04-30' }],
        freshness: { source: 'live' },
      },
    });

    const res = await invokeRoute(router, '/api/broadcasts/preview', 'post', {
      body: {
        channel: 'sms',
        filters: { job_date: '2026-04-30' },
      },
    });

    expect(res.statusCode).toBe(200);
    const [sql, params] = pool.query.mock.calls.find(([query]) => query.includes('FROM customers c'));
    expect(sql).toContain('FROM copilot_live_jobs clj');
    expect(sql).toContain('clj.service_date = $1::date');
    expect(sql).not.toContain('FROM scheduled_jobs sj');
    expect(params).toEqual(['2026-04-30']);
  });

  test('renders broadcast service_type merge tag lowercase for SMS', async () => {
    const pool = {
      query: jest.fn()
        .mockResolvedValueOnce({
          rows: [{
            id: 42,
            name: 'Jane Customer',
            first_name: 'Jane',
            email: null,
            mobile: '4405551212',
            phone: null,
            street: '123 Main St',
            city: 'Lakewood',
            state: 'OH',
            postal_code: '44107',
          }],
        })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            service_type: 'Mowing (Bi-Weekly)',
            address: '123 Main St, Lakewood, OH 44107',
            service_price: '55.00',
            job_date: '2026-04-30',
          }],
        })
        .mockResolvedValueOnce({ rows: [] }),
    };
    const { router, twilioCreate } = buildRouterWithQuerySpy({
      pool,
      liveJobsPayload: {
        jobs: [{ id: 'copilot:2026-04-30:visit-1', service_date: '2026-04-30' }],
      },
    });

    const res = await invokeRoute(router, '/api/broadcasts/send', 'post', {
      body: {
        channel: 'sms',
        sms_body: 'Reminder: {service_type} tomorrow',
        customer_ids: [42],
        job_date: '2026-04-30',
      },
    });

    expect(res.statusCode).toBe(200);
    expect(twilioCreate).toHaveBeenCalledWith(expect.objectContaining({
      body: 'Reminder: mowing (bi-weekly) tomorrow',
    }));
  });

  test('renders one SMS recipient with all service_type values when customer has multiple live jobs', async () => {
    const pool = {
      query: jest.fn()
        .mockResolvedValueOnce({
          rows: [{
            id: 42,
            name: 'Jane Customer',
            first_name: 'Jane',
            email: null,
            mobile: '4405551212',
            phone: null,
            street: '123 Main St',
            city: 'Lakewood',
            state: 'OH',
            postal_code: '44107',
          }],
        })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              service_type: 'Mowing',
              address: '123 Main St, Lakewood, OH 44107',
              service_price: '55.00',
              job_date: '2026-04-30',
            },
            {
              service_type: 'Weed Control',
              address: '123 Main St, Lakewood, OH 44107',
              service_price: '35.00',
              job_date: '2026-04-30',
            },
          ],
        })
        .mockResolvedValueOnce({ rows: [] }),
    };
    const { router, twilioCreate } = buildRouterWithQuerySpy({
      pool,
      liveJobsPayload: {
        jobs: [
          { id: 'copilot:2026-04-30:visit-1', service_date: '2026-04-30' },
          { id: 'copilot:2026-04-30:visit-2', service_date: '2026-04-30' },
        ],
      },
    });

    const res = await invokeRoute(router, '/api/broadcasts/send', 'post', {
      body: {
        channel: 'sms',
        sms_body: 'Reminder: {service_type} | {services_list}',
        customer_ids: [42],
        job_date: '2026-04-30',
      },
    });

    expect(res.statusCode).toBe(200);
    expect(twilioCreate).toHaveBeenCalledTimes(1);
    expect(twilioCreate).toHaveBeenCalledWith(expect.objectContaining({
      body: 'Reminder: mowing & weed control | mowing at 123 Main St and weed control at 123 Main St',
    }));
  });
});
