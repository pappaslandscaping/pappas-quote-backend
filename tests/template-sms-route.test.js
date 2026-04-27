const assert = require('assert');
const createCommunicationRoutes = require('../routes/communications');

const tests = [];
let failures = 0;

function it(name, fn) {
  tests.push({ name, fn });
}

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

function replaceTemplateVars(str, data) {
  if (!str) return str;
  return String(str).replace(/\{(\w+)\}/g, (match, key) => (
    data[key] !== undefined ? data[key] : match
  ));
}

function createRouter({ pool, getTemplate, twilioCreate } = {}) {
  return createCommunicationRoutes({
    pool,
    sendEmail: async () => {},
    emailTemplate: (html) => html,
    renderWithBaseLayout: async (html) => html,
    renderManagedEmail: async (html) => html,
    getTemplate: getTemplate || (async () => null),
    escapeHtml: (value) => String(value ?? ''),
    serverError: (res, error) => res.status(500).json({ success: false, error: error.message }),
    twilioClient: { messages: { create: twilioCreate || (async () => ({ sid: 'SM-default', status: 'queued' })) } },
    TWILIO_PHONE_NUMBER: '+14408867318',
    NOTIFICATION_EMAIL: 'hello@pappaslandscaping.com',
    replaceTemplateVars,
  });
}

it('sends a backend template SMS with invoice context and generates a payment token when needed', async () => {
  const sent = [];
  const messageInserts = [];
  let generatedToken = null;

  const pool = {
    async query(sql, params = []) {
      if (sql === 'SELECT * FROM invoices WHERE id = $1 LIMIT 1') {
        return {
          rows: [{
            id: 91,
            invoice_number: '10528',
            customer_id: 42,
            customer_name: 'Theresa Pappas',
            customer_email: 'theresa@example.com',
            total: '48.60',
            amount_paid: '0',
            due_date: '2026-04-30',
            payment_token: null,
          }],
        };
      }
      if (sql.includes('SELECT id, name, email, phone, mobile FROM customers WHERE id = $1')) {
        return {
          rows: [{
            id: 42,
            name: 'Theresa Pappas',
            email: 'theresa@example.com',
            phone: null,
            mobile: '(440) 555-0100',
          }],
        };
      }
      if (sql.includes('SET payment_token = $1')) {
        generatedToken = params[0];
        return { rows: [] };
      }
      if (sql.includes('INSERT INTO messages')) {
        messageInserts.push(params);
        return { rows: [] };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    },
  };

  const router = createRouter({
    pool,
    getTemplate: async (slug) => slug === 'invoice_sms' ? {
      slug,
      sms_body: 'Hi {customer_first_name}, your invoice #{invoice_number} is ready. Pay here: {payment_link}',
    } : null,
    twilioCreate: async (payload) => {
      sent.push(payload);
      return { sid: 'SM123', status: 'sent' };
    },
  });

  const res = await invokeRoute(router, '/api/messages/send-template', 'post', {
    body: { slug: 'invoice_sms', invoice_id: 91 },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.success, true);
  assert.strictEqual(res.body.template, 'invoice_sms');
  assert.strictEqual(typeof generatedToken, 'string');
  assert.strictEqual(generatedToken.length, 48);
  assert.strictEqual(sent.length, 1);
  assert.strictEqual(sent[0].to, '+14405550100');
  assert(sent[0].body.includes('Hi Theresa, your invoice #10528 is ready.'));
  assert(sent[0].body.includes(`pay-invoice.html?token=${generatedToken}`));
  assert.strictEqual(messageInserts.length, 1);
  assert.strictEqual(messageInserts[0][0], 'SM123');
  assert.strictEqual(messageInserts[0][2], '+14405550100');
});

it('renders Copilot-style uppercase double-brace tags for quote templates', async () => {
  const sent = [];
  const pool = {
    async query(sql, params = []) {
      if (sql.includes('SELECT * FROM email_templates WHERE id = $1')) {
        return {
          rows: [{
            id: 77,
            slug: 'quote_sms',
            sms_body: 'Hi {{CUSTOMER_FIRST_NAME}}, your estimate {{ESTIMATE_NUMBER}} is ready: {{ESTIMATE_LINK}}',
            is_active: true,
          }],
        };
      }
      if (sql === 'SELECT * FROM sent_quotes WHERE id = $1 LIMIT 1') {
        return {
          rows: [{
            id: 12,
            quote_number: '5678',
            sign_token: 'quote-token-abc',
            customer_name: 'Jane Smith',
            customer_email: 'jane@example.com',
            customer_phone: '(216) 555-0111',
            total_amount: '1250.00',
          }],
        };
      }
      if (sql.includes('SELECT id, name, first_name, last_name, email, phone, mobile, street, city, state, postal_code')) {
        return { rows: [] };
      }
      if (sql.includes('INSERT INTO messages')) {
        return { rows: [] };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    },
  };

  const router = createRouter({
    pool,
    twilioCreate: async (payload) => {
      sent.push(payload);
      return { sid: 'SM456', status: 'queued' };
    },
  });

  const res = await invokeRoute(router, '/api/messages/send-template', 'post', {
    body: { template_id: 77, quote_id: 12 },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.success, true);
  assert.strictEqual(sent.length, 1);
  assert.strictEqual(sent[0].to, '+12165550111');
  assert.strictEqual(
    sent[0].body,
    'Hi Jane, your estimate 5678 is ready: https://app.pappaslandscaping.com/sign-quote.html?token=quote-token-abc'
  );
});

it('returns 400 when a rendered template has no phone destination', async () => {
  const pool = {
    async query(sql) {
      if (sql === 'SELECT * FROM sent_quotes WHERE id = $1 LIMIT 1') {
        return {
          rows: [{
            id: 99,
            quote_number: '9999',
            sign_token: 'missing-phone',
            customer_name: 'No Phone',
            customer_email: '',
            customer_phone: '',
            total_amount: '10.00',
          }],
        };
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    },
  };

  const router = createRouter({
    pool,
    getTemplate: async () => ({ slug: 'quote_sms', sms_body: 'Hi {customer_first_name}' }),
  });

  const res = await invokeRoute(router, '/api/messages/send-template', 'post', {
    body: { slug: 'quote_sms', quote_id: 99 },
  });

  assert.strictEqual(res.statusCode, 400);
  assert.strictEqual(res.body.success, false);
  assert.strictEqual(res.body.error, 'No phone number available for this message');
});

(async () => {
  console.log('template-sms-route');
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
