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

  test('blocks backend customer email sends before resolving recipient context', async () => {
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
      authenticateToken: (req, res, next) => next(),
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

    expect(pool.query).not.toHaveBeenCalled();
    expect(emailTemplate).not.toHaveBeenCalled();
    expect(sendEmail).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(403);
    expect(res.body).toMatchObject({
      success: false,
      clientCommunicationsDisabled: true,
    });
    expect(serverError).not.toHaveBeenCalled();
  });

  test('blocks backend customer email sends before validating recipient availability', async () => {
    const pool = {
      query: jest.fn().mockResolvedValue({ rows: [{ id: 7, name: 'No Email', email: null }] }),
    };
    const sendEmail = jest.fn().mockResolvedValue(undefined);

    const router = createCommunicationRoutes({
      pool,
      authenticateToken: (req, res, next) => next(),
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

    expect(res.statusCode).toBe(403);
    expect(res.body).toMatchObject({ success: false, clientCommunicationsDisabled: true });
    expect(sendEmail).not.toHaveBeenCalled();
  });
});
