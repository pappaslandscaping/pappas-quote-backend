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

it('previews an invoice SMS without sending', async () => {
  const sent = [];
  const pool = createPool(async (sql) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 17,
          invoice_number: '10528',
          customer_id: 44,
          customer_name: 'Theresa Pappas',
          total: '48.60',
          amount_paid: '0',
          external_metadata: { client_invoice_url: 'https://secure.copilotcrm.com/client/invoices/view/3254566/66183bad3d2c0' },
          status: 'overdue',
        }],
      };
    }
    if (sql === 'SELECT mobile, phone FROM customers WHERE id = $1') {
      return { rows: [{ mobile: '(440) 555-0100', phone: null }] };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  const sendSms = async (payload) => {
    sent.push(payload);
    return { sid: 'SM123', status: 'queued' };
  };

  const router = createRouter({ pool, sendSms });
  const res = await invokeRoute(router, '/api/invoices/:id/sms-preview', 'post', {
    params: { id: '17' },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.success, true);
  assert.strictEqual(res.body.preview.customer_name, 'Theresa Pappas');
  assert.match(res.body.preview.body, /\$48\.60/);
  assert.match(res.body.preview.body, /secure\.copilotcrm\.com\/client\/invoices\/view/);
  assert.strictEqual(sent.length, 0);
});

it('requires an explicit manual confirmation before sending', async () => {
  let sendCount = 0;
  const pool = createPool(async (sql) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 18,
          invoice_number: '10529',
          customer_id: 45,
          customer_name: 'Manual Confirmation',
          total: '99.00',
          amount_paid: '0',
          status: 'overdue',
        }],
      };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });

  const router = createRouter({ pool, sendSms: async () => {
    sendCount += 1;
    return { sid: 'SM-nope', status: 'queued' };
  } });
  const res = await invokeRoute(router, '/api/invoices/:id/send-sms', 'post', {
    params: { id: '18' },
    body: {
      invoice_url: 'https://secure.copilotcrm.com/client/invoices/view/3254566/66183bad3d2c0',
      body: 'Reviewed reminder https://secure.copilotcrm.com/client/invoices/view/3254566/66183bad3d2c0',
    },
  });

  assert.strictEqual(res.statusCode, 400);
  assert.strictEqual(res.body.success, false);
  assert.match(res.body.error, /explicitly confirm/i);
  assert.strictEqual(sendCount, 0);
});

it('sends exactly once after manual confirmation with a reviewed Copilot link', async () => {
  let sentBody = null;
  const inserts = [];
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
          status: 'overdue',
        }],
      };
    }
    if (sql === 'SELECT mobile, phone FROM customers WHERE id = $1') {
      return { rows: [{ mobile: '(216) 555-0199', phone: null }] };
    }
    if (sql.includes('INSERT INTO messages')) {
      inserts.push(params);
      return { rows: [] };
    }
    if (sql.includes('sent_status = \'sent\'')) return { rows: [] };
    throw new Error(`Unexpected SQL: ${sql}`);
  });
  const sendSms = async ({ body }) => {
    sentBody = body;
    return { sid: 'SM456', status: 'sent' };
  };

  const router = createRouter({ pool, sendSms });
  const invoiceUrl = 'https://secure.copilotcrm.com/client/invoices/view/3254566/66183bad3d2c0';
  const res = await invokeRoute(router, '/api/invoices/:id/send-sms', 'post', {
    params: { id: '19' },
    body: {
      confirm_send: true,
      invoice_url: invoiceUrl,
      body: `Hi Token! Your balance is $100.00. ${invoiceUrl}`,
    },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.success, true);
  assert.match(sentBody, /\$100\.00/);
  assert.match(sentBody, /secure\.copilotcrm\.com\/client\/invoices\/view/);
  assert.strictEqual(inserts.length, 1);
});

it('finds a phone by an exact customer match when a Copilot invoice is not linked', async () => {
  const updates = [];
  const pool = createPool(async (sql, params) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 11959,
          invoice_number: '11959',
          customer_id: null,
          customer_name: 'Kevin Hopp',
          customer_email: '',
          total: '528.99',
          amount_paid: '0',
          external_metadata: {
            copilot_customer_id: '1053401',
            client_invoice_url: 'https://secure.copilotcrm.com/client/invoices/view/1053401/example',
          },
          status: 'overdue',
        }],
      };
    }
    if (sql.includes('FROM customers') && sql.includes('match_priority')) {
      assert.deepStrictEqual(params, ['1053401', '', 'Kevin Hopp']);
      return { rows: [{ id: 77, mobile: '(440) 555-0119', phone: null, match_priority: 1 }] };
    }
    if (sql.startsWith('UPDATE invoices SET customer_id')) {
      updates.push(params);
      return { rows: [] };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/invoices/:id/sms-preview', 'post', {
    params: { id: '11959' },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.preview.to, '+14405550119');
  assert.deepStrictEqual(updates, [[77, 11959]]);
});

it('accepts a manually reviewed phone when no customer record is linked', async () => {
  const pool = createPool(async (sql) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 12000,
          invoice_number: '12000',
          customer_id: null,
          customer_name: 'Manual Phone',
          total: '25.00',
          amount_paid: '0',
          external_metadata: {
            client_invoice_url: 'https://secure.copilotcrm.com/client/invoices/view/12000/example',
          },
          status: 'overdue',
        }],
      };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });

  const router = createRouter({ pool });
  const res = await invokeRoute(router, '/api/invoices/:id/sms-preview', 'post', {
    params: { id: '12000' },
    body: { phone: '(216) 555-0123' },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.preview.to, '+12165550123');
});

describe('invoice-sms-route', () => {
  for (const { name, fn } of tests) {
    test(name, fn);
  }
});
