const fs = require('fs');
const path = require('path');
const { normalizeUsPhone, findLocalMessageCustomer } = require('../lib/local-message-customer');

const serverSource = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const communicationsSource = fs.readFileSync(path.join(__dirname, '..', 'routes', 'communications.js'), 'utf8');

function section(source, begin, finish) {
  const start = source.indexOf(begin);
  expect(start).toBeGreaterThan(-1);
  const end = source.indexOf(finish, start);
  expect(end).toBeGreaterThan(start);
  return source.slice(start, end);
}

describe('unknown phone numbers in TwilioConnect', () => {
  test('normalizes US numbers without mistaking extensions and short numbers for customers', () => {
    expect(normalizeUsPhone('+1 (440) 555-0123')).toBe('4405550123');
    expect(normalizeUsPhone('440-555-0123')).toBe('4405550123');
    expect(normalizeUsPhone('12345')).toBeNull();
    expect(normalizeUsPhone('4405550123 x555')).toBeNull();
  });

  test('returns null for unmatched and shared numbers, without creating a contact', async () => {
    const pool = { query: jest.fn().mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ id: 1 }, { id: 2 }] }) };
    expect(await findLocalMessageCustomer(pool, '+14405550123')).toBeNull();
    expect(await findLocalMessageCustomer(pool, '4405550123')).toBeNull();
    expect(pool.query).toHaveBeenCalledTimes(2);
    expect(pool.query.mock.calls[0][0]).toContain('FROM customers');
  });

  test('links a message only to one existing local customer', async () => {
    const pool = { query: jest.fn().mockResolvedValue({ rows: [{ id: 17, name: 'Existing customer' }] }) };
    expect(await findLocalMessageCustomer(pool, '4405550123')).toEqual({ id: 17, name: 'Existing customer' });
    expect(pool.query.mock.calls[0][1]).toEqual(['4405550123']);
  });

  test('thread history is never gated on remote Homeworks matching', () => {
    const thread = section(serverSource,
      "app.get('/api/app/messages/thread/:phoneNumber'",
      "// AI reply suggestion");
    expect(thread).toContain('SELECT * FROM messages');
    expect(thread).toContain('homeworks_phone_matches');
    expect(thread).not.toContain('await lookupAppCustomerByPhone');
    expect(thread).toContain('messages: result.rows');
  });

  test('inbound and app-originated SMS use local IDs for local foreign keys', () => {
    const inbound = section(communicationsSource,
      "router.post('/api/sms/webhook'",
      "router.post('/api/email/sms-reply'");
    const outbound = section(serverSource,
      "app.post('/api/app/messages/send'",
      "app.get('/api/app/messages/unread-count'");
    expect(inbound).toContain('findLocalMessageCustomer(pool, From)');
    expect(inbound).toContain('messageCustomerId = localMessageCustomer?.id || null');
    expect(inbound).toContain('[MessageSid, From, To, Body, mediaUrls, messageCustomerId]');
    expect(outbound).toContain('findLocalMessageCustomer(pool, formattedTo)');
    expect(outbound).toContain('localMessageCustomer?.id || null');
    expect(serverSource).not.toContain('backfillTwilioConnectContacts()');
  });
});
