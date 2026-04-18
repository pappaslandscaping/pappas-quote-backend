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

it('normalizes legacy linked Copilot invoice references in tax sweep detail rows', async () => {
  const pool = createPool(async (sql, params) => {
    if (sql.includes('FROM payments p') && sql.includes("COALESCE(p.external_source, 'database') = $3")) {
      assert.deepStrictEqual(params, ['2026-04-16', '2026-04-16', 'copilotcrm']);
      return {
        rows: [{
          id: 31,
          payment_id: null,
          invoice_id: 9835,
          customer_name: 'Linda Scamaldo',
          amount: 9.6,
          tip_amount: 0,
          method: 'Card',
          status: 'completed',
          details: '$9.60 for Invoice #9835 Sep 19, 2025 Payment Added Apr 16, 2026 9:14 pm by Theresa Pappas (Linda Scamaldo)',
          notes: '',
          paid_at: '2026-04-16T21:14:00.000Z',
          created_at: '2026-04-16T21:14:00.000Z',
          source_date_raw: 'Apr 16, 2026',
          external_source: 'copilotcrm',
          external_payment_key: 'hash:legacy9835',
          external_metadata: {
            extracted_invoice_number: '9835',
            extracted_invoice_date: '2025-09-19',
          },
          imported_at: '2026-04-16T21:15:00.000Z',
          invoice_number: 'Sep 19, 2025',
          invoice_external_source: 'copilotcrm',
          external_invoice_id: '9835',
          invoice_total: 120,
          invoice_tax_amount: 9.6,
        }],
      };
    }
    throw new Error(`Unexpected SQL in tax sweep display test: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/reports/tax-sweep', 'get', {
    query: {
      start_date: '2026-04-16',
      end_date: '2026-04-16',
      source: 'copilotcrm',
    },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.success, true);
  assert.strictEqual(res.body.summary.linked_count, 1);
  assert.strictEqual(res.body.payments.length, 1);
  assert.strictEqual(res.body.payments[0].invoice_number, 'Sep 19, 2025');
  assert.strictEqual(res.body.payments[0].display_invoice_number, '9835');
});

(async () => {
  console.log('tax-sweep-display-routes');
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
