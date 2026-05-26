const assert = require('assert');
const createInvoiceRoutes = require('../routes/invoices');

const tests = [];

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
    if (sql.includes('FROM payments p') && sql.includes('ORDER BY COALESCE(p.paid_at, p.created_at) DESC')) {
      return {
        rows: [{
          payment_record_id: 301,
          id: 91,
          invoice_id: 91,
          payment_id: null,
          invoice_number: 'Sep 19, 2025',
          external_source: 'copilotcrm',
          external_invoice_id: '2329272',
          external_metadata: {
            extracted_invoice_number: '9835',
          },
          customer_id: 10,
          customer_name: 'Linda Scamaldo',
          customer_email: 'linda@example.com',
          amount: 9.6,
          method: 'Check',
          total: 120,
          amount_paid: 9.6,
          status: 'completed',
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
    if (sql.includes('SELECT to_char(COALESCE(paid_at, created_at)')) {
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
  assert.strictEqual(res.body.payments[0].method, 'check');
  assert.strictEqual(res.body.payments[0].status, 'paid');
});

it('does not fall back to Copilot external invoice ids in /api/payments when no real invoice number exists', async () => {
  const pool = createPool(async (sql) => {
    if (sql.includes('FROM payments p') && sql.includes('ORDER BY COALESCE(p.paid_at, p.created_at) DESC')) {
      return {
        rows: [{
          payment_record_id: 302,
          id: 92,
          invoice_id: 92,
          payment_id: null,
          invoice_number: 'Sep 19, 2025',
          external_source: 'copilotcrm',
          external_invoice_id: '1234269',
          external_metadata: {},
          customer_id: 11,
          customer_name: 'Example Customer',
          customer_email: 'example@example.com',
          amount: 50,
          method: 'Check',
          total: 50,
          amount_paid: 50,
          status: 'completed',
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
    if (sql.includes('SELECT to_char(COALESCE(paid_at, created_at)')) {
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

it('shows one Copilot payment row even when Copilot details include invoice and account credit allocations', async () => {
  const pool = createPool(async (sql) => {
    if (sql.includes('FROM payments p') && sql.includes('ORDER BY COALESCE(p.paid_at, p.created_at) DESC')) {
      return {
        rows: [{
          payment_record_id: 303,
          id: 10511,
          invoice_id: 10511,
          payment_id: null,
          invoice_number: '10511',
          external_source: 'copilotcrm',
          external_invoice_id: 'copilot-10511',
          external_metadata: {
            extracted_invoice_number: '10511',
            raw_details: '$79.76 for Invoice #10511 Apr 28, 2026 $2.00 added to credit of Martia Paolettto account',
          },
          customer_id: 1053620,
          customer_name: 'Martia Paolettto',
          customer_email: 'martia@example.com',
          amount: 81.76,
          method: 'Check',
          details: '$79.76 for Invoice #10511 Apr 28, 2026 $2.00 added to credit of Martia Paolettto account',
          total: 79.76,
          amount_paid: 79.76,
          status: 'completed',
          paid_at: '2026-05-05T16:00:00.000Z',
          due_date: null,
          created_at: '2026-05-05T16:00:00.000Z',
          updated_at: '2026-05-05T16:00:00.000Z',
          payment_date: '2026-05-05T16:00:00.000Z',
          qb_invoice_id: null,
        }],
      };
    }
    if (sql.includes('COUNT(*) as cnt')) {
      return { rows: [{ cnt: '1', total_received: '81.76' }] };
    }
    if (sql.includes('SELECT to_char(COALESCE(paid_at, created_at)')) {
      return { rows: [] };
    }
    throw new Error(`Unexpected SQL in payments display test: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/payments', 'get', {
    query: { search: 'martia', year: '2026', month: '5' },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.payments.length, 1);
  assert.strictEqual(res.body.payments[0].amount, 81.76);
  assert.strictEqual(res.body.payments[0].display_invoice_number, '10511');
  assert.strictEqual(res.body.totalReceived, 81.76);
});

describe('payments-display-routes', () => {
  for (const { name, fn } of tests) {
    test(name, fn);
  }
});
