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

function createRouter({ pool, sendSms, isTwilioConfigured = () => true, normalizePhone } = {}) {
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
    sendSms: sendSms || (async () => ({ sid: 'SM-default', status: 'sent' })),
    isTwilioConfigured,
    normalizePhone: normalizePhone || ((value) => {
      const digits = String(value || '').replace(/\D/g, '');
      return digits.length === 10 ? `+1${digits}` : `+${digits}`;
    }),
    twilioPhoneNumber: '+14408867318',
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
    params: {},
    get(name) {
      const match = Object.keys(this.headers).find((key) => key.toLowerCase() === String(name).toLowerCase());
      return match ? this.headers[match] : undefined;
    },
    ...reqOverrides,
  };
  req.headers = reqOverrides.headers || req.headers;
  req.query = reqOverrides.query || req.query;
  req.body = reqOverrides.body || req.body;
  req.params = reqOverrides.params || req.params;

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

it('blocks backend invoice SMS sends', async () => {
  const sent = [];
  const inserts = [];
  const updates = [];
  const pool = createPool(async (sql, params) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 17,
          invoice_number: '10528',
          customer_id: 44,
          customer_name: 'Theresa Pappas',
          total: '48.60',
          amount_paid: '0',
          payment_token: 'paytok123',
          status: 'draft',
        }],
      };
    }
    if (sql === 'SELECT mobile, phone FROM customers WHERE id = $1') {
      return { rows: [{ mobile: '(440) 555-0100', phone: null }] };
    }
    if (sql.includes('INSERT INTO messages')) {
      inserts.push(params);
      return { rows: [] };
    }
    if (sql.includes('UPDATE invoices') && sql.includes('sent_status = \'sent\'')) {
      updates.push(params);
      return { rows: [] };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  const sendSms = async (payload) => {
    sent.push(payload);
    return { sid: 'SM123', status: 'queued' };
  };

  const router = createRouter({ pool, sendSms });
  const res = await invokeRoute(router, '/api/invoices/:id/send-sms', 'post', {
    params: { id: '17' },
  });

  assert.strictEqual(res.statusCode, 403);
  assert.strictEqual(res.body.success, false);
  assert.strictEqual(res.body.clientCommunicationsDisabled, true);
  assert.strictEqual(sent.length, 0);
  assert.strictEqual(inserts.length, 0);
  assert.strictEqual(updates.length, 0);
});

it('blocks invoice SMS before resolving customer phone numbers', async () => {
  const pool = createPool(async (sql) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 18,
          invoice_number: '10529',
          customer_id: 45,
          customer_name: 'No Phone Customer',
          total: '99.00',
          amount_paid: '0',
          payment_token: 'paytok456',
          status: 'draft',
        }],
      };
    }
    if (sql === 'SELECT mobile, phone FROM customers WHERE id = $1') {
      return { rows: [{ mobile: null, phone: null }] };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/invoices/:id/send-sms', 'post', {
    params: { id: '18' },
  });

  assert.strictEqual(res.statusCode, 403);
  assert.strictEqual(res.body.success, false);
  assert.strictEqual(res.body.clientCommunicationsDisabled, true);
});

it('does not generate payment tokens when backend invoice SMS is blocked', async () => {
  let generatedToken = null;
  let sentBody = null;
  const pool = createPool(async (sql, params) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 19,
          invoice_number: '10530',
          customer_id: 46,
          customer_name: 'Token Test',
          total: '120.00',
          amount_paid: '20.00',
          payment_token: null,
          status: 'sent',
        }],
      };
    }
    if (sql.includes('SET payment_token = $1')) {
      generatedToken = params[0];
      return { rows: [] };
    }
    if (sql === 'SELECT mobile, phone FROM customers WHERE id = $1') {
      return { rows: [{ mobile: '(216) 555-0199', phone: null }] };
    }
    if (sql.includes('INSERT INTO messages')) return { rows: [] };
    if (sql.includes('sent_status = \'sent\'')) return { rows: [] };
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  const sendSms = async ({ body }) => {
    sentBody = body;
    return { sid: 'SM456', status: 'sent' };
  };

  const router = createRouter({ pool, sendSms });
  const res = await invokeRoute(router, '/api/invoices/:id/send-sms', 'post', {
    params: { id: '19' },
  });

  assert.strictEqual(res.statusCode, 403);
  assert.strictEqual(generatedToken, null);
  assert.strictEqual(sentBody, null);
});

describe('invoice-sms-route', () => {
  for (const { name, fn } of tests) {
    test(name, fn);
  }
});
