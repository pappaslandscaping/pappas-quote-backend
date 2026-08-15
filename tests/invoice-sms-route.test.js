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

function createRouter({ pool, sendSms, isTwilioConfigured = () => true, normalizePhone, getCopilotToken } = {}) {
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
          external_metadata: {
            client_invoice_url: 'https://secure.copilotcrm.com/client/invoices/view/3254566/66183bad3d2c0',
            customer_past_due_balance: '417.22',
            customer_outstanding_balance: '614.98',
          },
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
  assert.strictEqual(res.body.preview.balance, 417.22);
  assert.strictEqual(res.body.preview.total_outstanding, 614.98);
  assert.match(res.body.preview.body, /\$417\.22/);
  assert.doesNotMatch(res.body.preview.body, /\$48\.60/);
  assert.match(res.body.preview.body, /secure\.copilotcrm\.com\/client\/invoices\/view/);
  assert.strictEqual(sent.length, 0);
});

it('reconstructs the selected Copilot invoice link from this customer’s prior messages', async () => {
  const sent = [];
  const pool = createPool(async (sql, params) => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') {
      return {
        rows: [{
          id: 11476,
          external_invoice_id: '2891687',
          invoice_number: '11476',
          customer_id: 44,
          customer_name: 'Dianne Daugherty',
          customer_phone: '(216) 392-4969',
          total: '174.02',
          amount_paid: '0',
          external_source: '',
          external_metadata: {
            customer_past_due_balance: '417.22',
            customer_outstanding_balance: '614.98',
          },
          status: 'overdue',
        }],
      };
    }
    if (sql.includes('SELECT body') && sql.includes('FROM messages')) {
      assert.deepStrictEqual(params, [44, '2163924969']);
      return {
        rows: [{
          body: 'Prior invoice: https://secure.copilotcrm.com/client/invoices/view/3030244/661d81b79f4c4',
        }],
      };
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  });

  const router = createRouter({
    pool,
    sendSms: async (payload) => {
      sent.push(payload);
      return { sid: 'SM-never', status: 'queued' };
    },
  });
  const res = await invokeRoute(router, '/api/invoices/:id/sms-preview', 'post', {
    params: { id: '11476' },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(
    res.body.preview.invoice_url,
    'https://secure.copilotcrm.com/client/invoices/view/2891687/661d81b79f4c4'
  );
  assert.match(res.body.preview.body, /\/client\/invoices\/view\/2891687\/661d81b79f4c4/);
  assert.strictEqual(sent.length, 0);
});

it('uses the selected Copilot invoice owner instead of stale customer data', async () => {
  const originalFetch = global.fetch;
  const sent = [];
  const graphqlRequests = [];
  global.fetch = async (url, options) => {
    const body = JSON.parse(options.body);
    graphqlRequests.push({ url, options, body });
    if (body.operationName === 'ResolveInvoiceOwner') {
      return {
        ok: true,
        status: 200,
        async json() {
          return { data: { invoice: { customerId: 1072139 } } };
        },
      };
    }
    if (body.operationName === 'ResolveInvoiceRecipient') {
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            data: {
              customers: [{
                id: 1072139,
                fullName: 'Donna Martin',
                cell: '330-240-1681',
                phone: '',
                portalKey: '662adf33d6a27',
                outstanding: '303.48',
                pastDue: '251.28',
              }],
            },
          };
        },
      };
    }
    throw new Error(`Unexpected GraphQL operation: ${body.operationName}`);
  };

  try {
    const pool = createPool(async (sql) => {
      if (sql === 'SELECT * FROM invoices WHERE id = $1') {
        return {
          rows: [{
            id: 17001,
            external_invoice_id: '2907095',
            invoice_number: '11508',
            customer_id: 88,
            customer_name: 'Donna Martin',
            customer_phone: '(216) 544-9095',
            total: '128.43',
            amount_paid: '3.64',
            external_source: 'copilotcrm',
            external_metadata: {
              copilot_customer_id: '9999999',
              client_invoice_url: 'https://secure.copilotcrm.com/client/invoices/view/2907095/69bd5e0eaf59a',
              customer_past_due_balance: '247.64',
              customer_outstanding_balance: '303.48',
            },
            status: 'overdue',
          }],
        };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });

    const router = createRouter({
      pool,
      getCopilotToken: async () => ({
        cookieHeader: 'copilotApiAccessToken=test-access-token; other=value',
      }),
      sendSms: async (payload) => {
        sent.push(payload);
        return { sid: 'SM-never', status: 'queued' };
      },
    });
    const res = await invokeRoute(router, '/api/invoices/:id/sms-preview', 'post', {
      params: { id: '17001' },
      body: {
        phone: '+12165449095',
        invoice_url: 'https://secure.copilotcrm.com/client/invoices/view/2907095/69bd5e0eaf59a',
      },
    });

    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.body.preview.customer_name, 'Donna Martin');
    assert.strictEqual(res.body.preview.to, '+13302401681');
    assert.strictEqual(res.body.preview.balance, 251.28);
    assert.strictEqual(res.body.preview.total_outstanding, 303.48);
    assert.strictEqual(
      res.body.preview.invoice_url,
      'https://secure.copilotcrm.com/client/invoices/view/2907095/662adf33d6a27'
    );
    assert.strictEqual(graphqlRequests.length, 2);
    assert.strictEqual(graphqlRequests[0].body.variables.invoiceId, 2907095);
    assert.strictEqual(graphqlRequests[1].body.variables.customerId, 1072139);
    assert.strictEqual(sent.length, 0);
  } finally {
    global.fetch = originalFetch;
  }
});

it('refuses to send when the submitted phone does not match the selected invoice owner', async () => {
  const originalFetch = global.fetch;
  let sendCount = 0;
  global.fetch = async (_url, options) => {
    const body = JSON.parse(options.body);
    if (body.operationName === 'ResolveInvoiceOwner') {
      return {
        ok: true,
        status: 200,
        async json() {
          return { data: { invoice: { customerId: 1053505 } } };
        },
      };
    }
    if (body.operationName === 'ResolveInvoiceRecipient') {
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            data: {
              customers: [{
                id: 1053505,
                fullName: 'Jennifer Wright',
                cell: '216-702-4262',
                phone: '',
                portalKey: '66183bb31069b',
                outstanding: '118.80',
                pastDue: '118.80',
              }],
            },
          };
        },
      };
    }
    throw new Error(`Unexpected GraphQL operation: ${body.operationName}`);
  };

  try {
    const pool = createPool(async (sql) => {
      if (sql === 'SELECT * FROM invoices WHERE id = $1') {
        return {
          rows: [{
            id: 17002,
            external_invoice_id: '3133706',
            invoice_number: '11897',
            customer_id: 99,
            customer_name: 'Jennifer Wright',
            customer_phone: '(440) 886-7318',
            total: '118.80',
            amount_paid: '0',
            external_source: 'copilotcrm',
            external_metadata: {
              copilot_customer_id: '9999999',
            },
            status: 'overdue',
          }],
        };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });
    const router = createRouter({
      pool,
      getCopilotToken: async () => ({
        cookieHeader: 'copilotApiAccessToken=test-access-token',
      }),
      sendSms: async () => {
        sendCount += 1;
        return { sid: 'SM-never', status: 'queued' };
      },
    });
    const correctUrl = 'https://secure.copilotcrm.com/client/invoices/view/3133706/66183bb31069b';
    const res = await invokeRoute(router, '/api/invoices/:id/send-sms', 'post', {
      params: { id: '17002' },
      body: {
        confirm_send: true,
        phone: '(440) 886-7318',
        invoice_url: correctUrl,
        body: `Hi Jennifer! ${correctUrl}`,
      },
    });

    assert.strictEqual(res.statusCode, 409);
    assert.match(res.body.error, /does not match the selected invoice owner/i);
    assert.strictEqual(sendCount, 0);
  } finally {
    global.fetch = originalFetch;
  }
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
  let sentPayload = null;
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
  const sendSms = async (payload) => {
    sentPayload = payload;
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
  assert.match(sentPayload.body, /\$100\.00/);
  assert.match(sentPayload.body, /secure\.copilotcrm\.com\/client\/invoices\/view/);
  assert.strictEqual(sentPayload.manualReviewedSend, true);
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
