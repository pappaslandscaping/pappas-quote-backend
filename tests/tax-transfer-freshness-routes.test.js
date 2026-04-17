const assert = require('assert');
const createInvoiceRoutes = require('../routes/invoices');

const tests = [];
let failures = 0;

function it(name, fn) {
  tests.push({ name, fn });
}

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

function createPool(handler) {
  return {
    async query(sql, params = []) {
      return handler(String(sql), params);
    },
  };
}

function createRouter({ pool, getCopilotToken } = {}) {
  return createInvoiceRoutes({
    pool: pool || createPool(async () => ({ rows: [] })),
    sendEmail: async () => {},
    emailTemplate: () => '',
    escapeHtml: (value) => String(value ?? ''),
    serverError: (res, error) => res.status(500).json({ success: false, error: error.message }),
    authenticateToken: (_req, _res, next) => next(),
    nextInvoiceNumber: async () => 1,
    squareClient: {},
    SQUARE_APP_ID: '',
    SQUARE_LOCATION_ID: '',
    SquareApiError: Error,
    NOTIFICATION_EMAIL: '',
    LOGO_URL: '',
    FROM_EMAIL: '',
    COMPANY_NAME: 'Pappas & Co. Landscaping',
    getCopilotToken: getCopilotToken || (async () => null),
  });
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
      const match = Object.keys(this.headers).find((key) => key.toLowerCase() === String(name).toLowerCase());
      return match ? this.headers[match] : undefined;
    },
    ...reqOverrides,
  };
  req.headers = reqOverrides.headers || req.headers;
  req.query = reqOverrides.query || req.query;
  req.body = reqOverrides.body || req.body;

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

it('rejects the cron endpoint when CRON_SECRET is missing', async () => {
  const previousSecret = process.env.CRON_SECRET;
  const previousApiKey = process.env.CRON_API_KEY;
  delete process.env.CRON_SECRET;
  delete process.env.CRON_API_KEY;

  try {
    const router = createRouter();
    const res = await invokeRoute(router, '/api/cron/tax-transfer-freshness-sync', 'get');
    assert.strictEqual(res.statusCode, 503);
    assert.deepStrictEqual(res.body, {
      success: false,
      error: 'CRON_SECRET is not configured',
    });
  } finally {
    if (previousSecret == null) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = previousSecret;
    if (previousApiKey == null) delete process.env.CRON_API_KEY;
    else process.env.CRON_API_KEY = previousApiKey;
  }
});

it('rejects the cron endpoint when the cron secret is invalid', async () => {
  const previousSecret = process.env.CRON_SECRET;
  process.env.CRON_SECRET = 'expected-secret';

  try {
    const router = createRouter();
    const res = await invokeRoute(router, '/api/cron/tax-transfer-freshness-sync', 'post', {
      headers: { 'x-cron-secret': 'wrong-secret' },
    });
    assert.strictEqual(res.statusCode, 401);
    assert.deepStrictEqual(res.body, {
      success: false,
      error: 'Invalid cron secret',
    });
  } finally {
    if (previousSecret == null) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = previousSecret;
  }
});

it('returns failed status for a valid cron request when both sync components fail', async () => {
  const previousSecret = process.env.CRON_SECRET;
  process.env.CRON_SECRET = 'expected-secret';
  const storedStatuses = [];
  const pool = createPool(async (sql, params) => {
    if (sql.includes('SELECT value') && sql.includes('FROM copilot_sync_settings')) {
      return { rows: [] };
    }
    if (sql.includes('INSERT INTO copilot_sync_settings')) {
      storedStatuses.push(JSON.parse(params[1]));
      return { rows: [] };
    }
    throw new Error(`Unexpected SQL in cron test: ${sql}`);
  });

  try {
    const router = createRouter({
      pool,
      getCopilotToken: async () => null,
    });
    const res = await invokeRoute(router, '/api/cron/tax-transfer-freshness-sync', 'post', {
      headers: { 'x-cron-secret': 'expected-secret' },
    });

    assert.strictEqual(res.statusCode, 500);
    assert.strictEqual(res.body.success, false);
    assert.strictEqual(res.body.status, 'failed');
    assert.strictEqual(res.body.components.tax_summary.status, 'failed');
    assert.strictEqual(res.body.components.payments.status, 'failed');
    assert.strictEqual(storedStatuses.length, 1);
    assert.strictEqual(storedStatuses[0].status, 'failed');
  } finally {
    if (previousSecret == null) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = previousSecret;
  }
});

it('reports same-day freshness from stored automation and snapshot timestamps', async () => {
  const today = '2026-04-17';
  const storedStatus = {
    status: 'degraded',
    trigger: 'cron',
    time_zone: 'America/New_York',
    days_back: 1,
    start_date: '2026-04-16',
    end_date: today,
    today,
    last_attempt_at: '2026-04-17T20:15:00.000Z',
    last_success_at: '2026-04-16T20:15:00.000Z',
    last_error: 'Copilot payments sync failed',
    components: {
      tax_summary: {
        status: 'success',
        start_date: '2026-04-16',
        end_date: today,
      },
      payments: {
        status: 'failed',
      },
    },
  };

  const pool = createPool(async (sql, params) => {
    if (sql.includes('SELECT value') && sql.includes('FROM copilot_sync_settings')) {
      return { rows: [{ value: JSON.stringify(storedStatus) }] };
    }
    if (sql.includes('SELECT MAX(imported_at) AS last_imported_at') && sql.includes('FROM payments')) {
      return { rows: [{ last_imported_at: '2026-04-17T19:45:00.000Z' }] };
    }
    if (sql.includes('FROM copilot_tax_summary_snapshots')) {
      assert.deepStrictEqual(params, [today]);
      return {
        rows: [{
          imported_at: '2026-04-17T19:30:00.000Z',
          updated_at: '2026-04-17T19:30:00.000Z',
          tax_amount: '18.42',
          total_sales: '230.25',
        }],
      };
    }
    throw new Error(`Unexpected SQL in freshness status test: ${sql}`);
  });

  const RealDate = Date;
  global.Date = class extends RealDate {
    constructor(value) {
      super(value || '2026-04-17T20:30:00.000Z');
    }
    static now() {
      return new RealDate('2026-04-17T20:30:00.000Z').getTime();
    }
    static parse(value) {
      return RealDate.parse(value);
    }
    static UTC(...args) {
      return RealDate.UTC(...args);
    }
  };

  try {
    const router = createRouter({ pool });
    const res = await invokeRoute(router, '/api/reports/tax-transfer-freshness-status', 'get');
    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.body.success, true);
    assert.strictEqual(res.body.today, today);
    assert.strictEqual(res.body.automation.status, 'degraded');
    assert.strictEqual(res.body.freshness.today_tax_summary_fresh, true);
    assert.strictEqual(res.body.freshness.payments_fresh_today, true);
    assert.strictEqual(res.body.freshness.today_tax_summary_tax_amount, 18.42);
    assert.strictEqual(res.body.freshness.today_tax_summary_total_sales, 230.25);
  } finally {
    global.Date = RealDate;
  }
});

(async () => {
  console.log('tax-transfer-freshness-routes');
  for (const { name, fn } of tests) {
    try {
      await fn();
      console.log(`  \u2713 ${name}`);
    } catch (error) {
      failures += 1;
      console.error(`  \u2717 ${name}\n    ${error.message}`);
    }
  }

  if (failures > 0) {
    console.error(`\n${failures} failure(s)`);
    process.exit(1);
  } else {
    console.log('\nAll tests passed');
  }
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
