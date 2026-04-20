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

function buildRouterWithQuerySpy() {
  const pool = {
    query: jest.fn().mockResolvedValue({ rows: [] }),
  };

  const router = createCommunicationRoutes({
    pool,
    sendEmail: jest.fn(),
    emailTemplate: (content) => content,
    escapeHtml: (value) => value,
    serverError: jest.fn(),
    twilioClient: { messages: { create: jest.fn() } },
    TWILIO_PHONE_NUMBER: '+14405550000',
    NOTIFICATION_EMAIL: 'hello@pappaslandscaping.com',
  });

  return { pool, router };
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
    const { pool, router } = buildRouterWithQuerySpy();

    const res = await invokeRoute(router, '/api/broadcasts/preview', 'post', {
      body: {
        channel: 'sms',
        filters: { job_date: '2026-04-20' },
      },
    });

    expect(res.statusCode).toBe(200);
    const [sql, params] = pool.query.mock.calls[0];
    expect(sql).toContain('FROM copilot_live_jobs clj');
    expect(sql).toContain('COALESCE(yjo.customer_link_id, live_customer.id) = c.id');
    expect(sql).toContain('clj.service_date = $1::date');
    expect(sql).toContain('sj.job_date::date = $2::date');
    expect(params).toEqual(['2026-04-20', '2026-04-20']);
  });
});
