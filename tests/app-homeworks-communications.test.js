const { loadLocalCommunications } = require('../routes/app-homeworks');

describe('TwilioConnect customer communication timeline', () => {
  test('returns messages and calls with their HomeWorks sync status', async () => {
    const pool = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [{ id: 1, homeworks_sync_status: 'synced' }] })
        .mockResolvedValueOnce({ rows: [{ id: 2, homeworks_sync_status: 'failed' }] }),
    };

    const result = await loadLocalCommunications(pool, '(216) 501-2439');

    expect(result.messages[0].homeworks_sync_status).toBe('synced');
    expect(result.calls[0].homeworks_sync_status).toBe('failed');
    expect(pool.query.mock.calls[1][0]).toContain("sync.source_type = 'sms'");
    expect(pool.query.mock.calls[2][0]).toContain("sync.source_type = 'call'");
    expect(pool.query.mock.calls[1][1]).toEqual(['2165012439']);
  });

  test('does not query when the customer has no usable phone number', async () => {
    const pool = { query: jest.fn() };
    await expect(loadLocalCommunications(pool, '')).resolves.toEqual({ messages: [], calls: [] });
    expect(pool.query).not.toHaveBeenCalled();
  });
});
