const {
  listUnmatchedCommunications,
  matchCommunication,
} = require('../services/homeworks/unmatched-communications');

describe('HomeWorks unmatched communications inbox', () => {
  test('lists only unresolved sync records', async () => {
    const pool = { query: jest.fn().mockResolvedValue({ rows: [{ source_type: 'sms', source_id: 'SM1' }] }) };
    const rows = await listUnmatchedCommunications(pool, 25);
    expect(rows).toEqual([{ source_type: 'sms', source_id: 'SM1' }]);
    const listCall = pool.query.mock.calls.find(([sql]) => sql.includes('FROM homeworks_communication_sync sync'));
    expect(listCall[0]).toContain("'skipped_no_customer', 'failed'");
    expect(listCall[1]).toEqual([25]);
  });

  test('matches a text, saves its Call Note, and remembers the phone mapping', async () => {
    const pool = {
      query: jest.fn().mockImplementation(async (sql) => {
        if (sql.includes('FROM messages WHERE twilio_sid')) return { rows: [{
          twilio_sid: 'SM1', direction: 'inbound', from_number: '+12165012439',
          to_number: '+12165551212', body: 'Please call me', status: 'received',
          created_at: '2026-09-21T15:00:00.000Z',
        }] };
        return { rows: [] };
      }),
    };
    const fetchImpl = jest.fn().mockResolvedValue({ ok: true, status: 200, text: async () => JSON.stringify({ status: true }) });
    const result = await matchCommunication({
      pool,
      getCopilotToken: jest.fn().mockResolvedValue({ cookieHeader: 'session=test' }),
      sourceType: 'sms',
      sourceId: 'SM1',
      customerId: 42,
      customerName: 'Jane Smith',
      fetchImpl,
    });
    expect(result).toMatchObject({ success: true, customerId: 42, phone: '2165012439', status: 'synced' });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(pool.query.mock.calls.some(([sql]) => sql.includes('INSERT INTO homeworks_phone_matches'))).toBe(true);
    expect(pool.query.mock.calls.some(([sql]) => sql.includes("SET status = 'synced'"))).toBe(true);
  });
});
