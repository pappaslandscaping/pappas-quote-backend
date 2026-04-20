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

function createRouter({ pool } = {}) {
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
    getCopilotToken: async () => null,
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

it('prefers extracted customer-facing invoice numbers over Copilot external ids in /api/payments', async () => {
  const pool = createPool(async (sql) => {
    if (sql.includes('FROM invoices WHERE amount_paid > 0') && sql.includes('ORDER BY COALESCE(paid_at, updated_at, created_at) DESC')) {
      return {
        rows: [{
          id: 91,
          invoice_number: 'Sep 19, 2025',
          external_source: 'copilotcrm',
          external_invoice_id: '2329272',
          external_metadata: {
            extracted_invoice_number: '9835',
          },
          customer_id: 10,
          customer_name: 'Linda Scamaldo',
          customer_email: 'linda@example.com',
          total: 120,
          amount_paid: 9.6,
          status: 'paid',
          paid_at: '2026-04-16T21:14:00.000Z',
          due_date: null,
          created_at: '2025-09-19T12:00:00.000Z',
          updated_at: '2026-04-16T21:14:00.000Z',
          payment_date: '2026-04-16T21:14:00.000Z',
          qb_invoice_id: null,
          payment_token: null,
        }],
      };
    }
    if (sql.includes('COUNT(*) as cnt')) {
      return { rows: [{ cnt: '1', total_received: '9.6' }] };
    }
    if (sql.includes('SELECT to_char(COALESCE(paid_at, updated_at)')) {
      return { rows: [] };
    }
    throw new Error(`Unexpected SQL in payments display test: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/payments', 'get');

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.success, true);
  assert.strictEqual(res.body.payments.length, 1);
  assert.strictEqual(res.body.payments[0].display_invoice_number, '9835');
});

it('does not fall back to Copilot external invoice ids in /api/payments when no real invoice number exists', async () => {
  const pool = createPool(async (sql) => {
    if (sql.includes('FROM invoices WHERE amount_paid > 0') && sql.includes('ORDER BY COALESCE(paid_at, updated_at, created_at) DESC')) {
      return {
        rows: [{
          id: 92,
          invoice_number: 'Sep 19, 2025',
          external_source: 'copilotcrm',
          external_invoice_id: '1234269',
          external_metadata: {},
          customer_id: 11,
          customer_name: 'Example Customer',
          customer_email: 'example@example.com',
          total: 50,
          amount_paid: 50,
          status: 'paid',
          paid_at: '2026-04-16T12:00:00.000Z',
          due_date: null,
          created_at: '2025-09-19T12:00:00.000Z',
          updated_at: '2026-04-16T12:00:00.000Z',
          payment_date: '2026-04-16T12:00:00.000Z',
          qb_invoice_id: null,
          payment_token: null,
        }],
      };
    }
    if (sql.includes('COUNT(*) as cnt')) {
      return { rows: [{ cnt: '1', total_received: '50' }] };
    }
    if (sql.includes('SELECT to_char(COALESCE(paid_at, updated_at)')) {
      return { rows: [] };
    }
    throw new Error(`Unexpected SQL in payments display test: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/payments', 'get');

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.success, true);
  assert.strictEqual(res.body.payments.length, 1);
  assert.strictEqual(res.body.payments[0].display_invoice_number, null);
});

(async () => {
  console.log('payments-display-routes');
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
