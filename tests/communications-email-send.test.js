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

describe('communications inbox email send route', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('resolves recipient from phone-linked customer context and sends via email log path', async () => {
    const pool = {
      query: jest.fn().mockResolvedValue({
        rows: [{ id: 42, name: 'Jane Smith', email: 'jane@example.com' }],
      }),
    };
    const sendEmail = jest.fn().mockResolvedValue(undefined);
    const emailTemplate = jest.fn((content) => `WRAPPED:${content}`);
    const serverError = jest.fn();

    const router = createCommunicationRoutes({
      pool,
      sendEmail,
      emailTemplate,
      escapeHtml: (value) => String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;'),
      serverError,
      twilioClient: { messages: { create: jest.fn() } },
      TWILIO_PHONE_NUMBER: '+14405550000',
      NOTIFICATION_EMAIL: 'hello@pappaslandscaping.com',
    });

    const res = await invokeRoute(router, '/api/communications/email/send', 'post', {
      body: {
        phone_number: '(440) 555-0100',
        subject: 'Service follow-up',
        body: 'Thanks for reaching out.\nWe will follow up tomorrow.',
      },
    });

    expect(pool.query).toHaveBeenCalledWith(
      expect.stringContaining('SELECT id, name, email, phone, mobile'),
      ['%4405550100']
    );
    expect(emailTemplate).toHaveBeenCalledWith('Thanks for reaching out.<br>We will follow up tomorrow.');
    expect(sendEmail).toHaveBeenCalledWith(
      'jane@example.com',
      'Service follow-up',
      'WRAPPED:Thanks for reaching out.<br>We will follow up tomorrow.',
      null,
      { type: 'communication', customer_id: 42, customer_name: 'Jane Smith' }
    );
    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({
      success: true,
      recipient_email: 'jane@example.com',
      customer: { id: 42, name: 'Jane Smith', email: 'jane@example.com' },
    });
    expect(serverError).not.toHaveBeenCalled();
  });

  test('fails cleanly when no recipient email can be resolved', async () => {
    const pool = {
      query: jest.fn().mockResolvedValue({ rows: [{ id: 7, name: 'No Email', email: null }] }),
    };
    const sendEmail = jest.fn().mockResolvedValue(undefined);

    const router = createCommunicationRoutes({
      pool,
      sendEmail,
      emailTemplate: (content) => content,
      escapeHtml: (value) => value,
      serverError: jest.fn(),
      twilioClient: { messages: { create: jest.fn() } },
      TWILIO_PHONE_NUMBER: '+14405550000',
      NOTIFICATION_EMAIL: 'hello@pappaslandscaping.com',
    });

    const res = await invokeRoute(router, '/api/communications/email/send', 'post', {
      body: {
        phone_number: '(440) 555-0101',
        subject: 'Hello',
        body: 'Test message',
      },
    });

    expect(res.statusCode).toBe(400);
    expect(res.body).toEqual({
      success: false,
      error: 'No recipient email available for this contact',
    });
    expect(sendEmail).not.toHaveBeenCalled();
  });
});
