const {
  buildSmsReplyAddress,
  parseSmsReplyTarget,
  extractReplyBody,
  handleSmsEmailReplyWebhook,
} = require('../lib/sms-email-replies');

describe('SMS email replies', () => {
  test('builds and parses a signed SMS reply address', () => {
    const address = buildSmsReplyAddress({
      customerPhone: '+1 (440) 552-1044',
      businessPhone: '+1 440 886 7318',
      domain: 'reply.pappaslandscaping.com',
      secret: 'test-secret',
    });

    expect(address).toMatch(/^sms\+14405521044\.14408867318\.[a-f0-9]{16}@reply\.pappaslandscaping\.com$/);
    expect(parseSmsReplyTarget([`Pappas Replies <${address}>`], 'test-secret')).toEqual({
      to: '+14405521044',
      from: '+14408867318',
    });
  });

  test('rejects tampered SMS reply addresses', () => {
    const address = buildSmsReplyAddress({
      customerPhone: '+14405521044',
      businessPhone: '+14408867318',
      domain: 'reply.pappaslandscaping.com',
      secret: 'test-secret',
    }).replace('14405521044', '14405521045');

    expect(parseSmsReplyTarget([address], 'test-secret')).toBeNull();
  });

  test('extracts only the fresh reply text', () => {
    const body = extractReplyBody(`Yes weekly mowing sounds good.

On Wed, May 27, 2026 at 10:12 AM Pappas & Co. Landscaping wrote:
> Original message`);

    expect(body).toBe('Yes weekly mowing sounds good.');
  });

  test('handles a Resend inbound webhook by sending SMS and logging outbound message', async () => {
    const replyTo = buildSmsReplyAddress({
      customerPhone: '+14405521044',
      businessPhone: '+14408867318',
      domain: 'reply.pappaslandscaping.com',
      secret: 'test-secret',
    });
    const queries = [];
    const pool = {
      query: jest.fn(async (sql, params) => {
        queries.push({ sql, params });
        if (sql.includes('SELECT id, name FROM customers')) {
          return { rows: [{ id: 42, name: 'Deborah Calaway' }] };
        }
        return { rows: [] };
      }),
    };
    const twilioClient = {
      messages: {
        create: jest.fn(async (payload) => ({ sid: 'SM-email-reply', status: 'queued', ...payload })),
      },
    };
    const fetchImpl = jest.fn(async () => ({
      ok: true,
      json: async () => ({
        text: 'Yes weekly mowing sounds good.\n\nOn Wed, May 27, 2026 at 10:12 AM Pappas wrote:',
      }),
      text: async () => '',
    }));

    const result = await handleSmsEmailReplyWebhook({
      event: {
        type: 'email.received',
        data: { email_id: 'email_123', to: [replyTo], from: 'hello@pappaslandscaping.com' },
      },
      pool,
      twilioClient,
      resendApiKey: 're_test',
      replySecret: 'test-secret',
      allowedSenders: ['hello@pappaslandscaping.com'],
      fetchImpl,
    });

    expect(result).toEqual({ handled: true, sid: 'SM-email-reply' });
    expect(fetchImpl).toHaveBeenCalledWith(
      'https://api.resend.com/emails/receiving/email_123',
      expect.objectContaining({ headers: expect.objectContaining({ Authorization: 'Bearer re_test' }) })
    );
    expect(twilioClient.messages.create).toHaveBeenCalledWith({
      to: '+14405521044',
      from: '+14408867318',
      body: 'Yes weekly mowing sounds good.',
    });
    expect(queries.some((q) => q.sql.includes('INSERT INTO messages'))).toBe(true);
  });

  test('ignores email replies from unapproved senders', async () => {
    const replyTo = buildSmsReplyAddress({
      customerPhone: '+14405521044',
      businessPhone: '+14408867318',
      domain: 'reply.pappaslandscaping.com',
      secret: 'test-secret',
    });
    const twilioClient = { messages: { create: jest.fn() } };

    const result = await handleSmsEmailReplyWebhook({
      event: {
        type: 'email.received',
        data: { email_id: 'email_123', to: [replyTo], from: 'stranger@example.com' },
      },
      pool: { query: jest.fn() },
      twilioClient,
      resendApiKey: 're_test',
      replySecret: 'test-secret',
      allowedSenders: ['hello@pappaslandscaping.com'],
      fetchImpl: jest.fn(),
    });

    expect(result).toEqual({ handled: false, reason: 'sender_not_allowed' });
    expect(twilioClient.messages.create).not.toHaveBeenCalled();
  });
});
