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

it('requires a start date for Copilot payment review', async () => {
  const router = createRouter();
  const res = await invokeRoute(router, '/api/copilot/payment-review', 'get');
  assert.strictEqual(res.statusCode, 400);
  assert.deepStrictEqual(res.body, {
    success: false,
    error: 'start_date and end_date are required',
  });
});

it('returns unresolved Copilot payment review rows by default without writing data', async () => {
  let paymentQueryCount = 0;
  let invoiceQueryCount = 0;
  const pool = createPool(async (sql, params) => {
    if (sql.includes('FROM payments p') && sql.includes("COALESCE(p.external_source, 'database') = 'copilotcrm'")) {
      paymentQueryCount += 1;
      assert.deepStrictEqual(params, ['2026-04-17', '2026-04-17']);
      return {
        rows: [
          {
            id: 10,
            payment_id: null,
            invoice_id: null,
            customer_id: null,
            customer_name: 'Alice Adams',
            amount: 172.8,
            tip_amount: 0,
            method: 'Card',
            status: 'completed',
            details: '$172.80 for Invoice #10239',
            notes: '',
            paid_at: '2026-04-17T12:00:00.000Z',
            created_at: '2026-04-17T12:00:00.000Z',
            source_date_raw: 'Apr 17, 2026',
            external_source: 'copilotcrm',
            external_payment_key: 'row:payment_10',
            external_metadata: {},
            imported_at: '2026-04-17T12:05:00.000Z',
            invoice_number: null,
            invoice_total: null,
            invoice_tax_amount: null,
            invoice_external_source: null,
            external_invoice_id: null,
          },
          {
            id: 11,
            payment_id: null,
            invoice_id: 42,
            customer_id: 9,
            customer_name: 'Bob Brown',
            amount: 89.64,
            tip_amount: 0,
            method: 'ACH',
            status: 'completed',
            details: '$89.64 for Invoice #10001',
            notes: 'Imported cleanly',
            paid_at: '2026-04-17T15:00:00.000Z',
            created_at: '2026-04-17T15:00:00.000Z',
            source_date_raw: 'Apr 17, 2026',
            external_source: 'copilotcrm',
            external_payment_key: 'row:payment_11',
            external_metadata: { extracted_invoice_number: '10001' },
            imported_at: '2026-04-17T15:05:00.000Z',
            invoice_number: '10001',
            invoice_total: 89.64,
            invoice_tax_amount: 7.17,
            invoice_external_source: 'copilotcrm',
            external_invoice_id: '10001',
          },
        ],
      };
    }
    if (sql.includes('FROM invoices') && sql.includes('WHERE invoice_number = ANY')) {
      invoiceQueryCount += 1;
      assert.deepStrictEqual(params, [['10239']]);
      return {
        rows: [{
          id: 77,
          invoice_number: '10239',
          customer_id: 15,
          customer_name: 'Alice Adams',
          total: 172.8,
          tax_amount: 13.82,
          external_source: 'copilotcrm',
          external_invoice_id: '10239',
          external_metadata: {},
          imported_at: '2026-04-17T12:01:00.000Z',
          updated_at: '2026-04-17T12:01:00.000Z',
        }],
      };
    }
    throw new Error(`Unexpected SQL in payment review route test: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/copilot/payment-review', 'get', {
    query: { start_date: '2026-04-17' },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(paymentQueryCount, 1);
  assert.strictEqual(invoiceQueryCount, 1);
  assert.strictEqual(res.body.success, true);
  assert.strictEqual(res.body.unresolved_only, true);
  assert.strictEqual(res.body.total, 1);
  assert.deepStrictEqual(res.body.summary, {
    total_rows: 2,
    linked_count: 1,
    unresolved_count: 1,
  });
  assert.strictEqual(res.body.payments.length, 1);
  assert.strictEqual(res.body.payments[0].customer_name, 'Alice Adams');
  assert.strictEqual(res.body.payments[0].extracted_invoice_number, '10239');
  assert.strictEqual(res.body.payments[0].current_invoice_match_number, '10239');
  assert.strictEqual(
    res.body.payments[0].link_failure_reason,
    'Invoice #10239 exists in YardDesk, but this payment row is still unresolved.'
  );
});

it('shows linked rows when unresolved_only is false', async () => {
  const pool = createPool(async (sql) => {
    if (sql.includes('FROM payments p') && sql.includes("COALESCE(p.external_source, 'database') = 'copilotcrm'")) {
      return {
        rows: [{
          id: 11,
          payment_id: null,
          invoice_id: 42,
          customer_id: 9,
          customer_name: 'Bob Brown',
          amount: 89.64,
          tip_amount: 0,
          method: 'ACH',
          status: 'completed',
          details: '$89.64 for Invoice #10001',
          notes: '',
          paid_at: '2026-04-17T15:00:00.000Z',
          created_at: '2026-04-17T15:00:00.000Z',
          source_date_raw: 'Apr 17, 2026',
          external_source: 'copilotcrm',
          external_payment_key: 'row:payment_11',
          external_metadata: { extracted_invoice_number: '10001' },
          imported_at: '2026-04-17T15:05:00.000Z',
          invoice_number: '10001',
          invoice_total: 89.64,
          invoice_tax_amount: 7.17,
          invoice_external_source: 'copilotcrm',
          external_invoice_id: '10001',
        }],
      };
    }
    if (sql.includes('FROM invoices') && sql.includes('WHERE invoice_number = ANY')) {
      return { rows: [] };
    }
    throw new Error(`Unexpected SQL in payment review filter test: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/copilot/payment-review', 'get', {
    query: {
      start_date: '2026-04-17',
      end_date: '2026-04-17',
      unresolved_only: 'false',
    },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.unresolved_only, false);
  assert.strictEqual(res.body.total, 1);
  assert.strictEqual(res.body.payments.length, 1);
  assert.strictEqual(res.body.payments[0].link_status, 'linked');
  assert.strictEqual(res.body.payments[0].link_failure_reason, null);
});

(async () => {
  console.log('copilot-payment-review-routes');
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
