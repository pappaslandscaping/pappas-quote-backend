const { normalizePhone, cachedCustomerMatches, loadVoicemailMetadata } = require('../lib/communications-read');

describe('TwilioConnect resilient communications reads', () => {
  test('normalizes NANP numbers but ignores non-phone SDK identities', () => {
    expect(normalizePhone('+1 (216) 555-0100')).toBe('2165550100');
    expect(normalizePhone('client:tim@example.com')).toBe('');
  });
  test('deduplicates numbers and reads only cached matches', async () => {
    const pool = { query: jest.fn().mockResolvedValue({ rows: [{ normalized_phone: '2165550100', id: 42, name: 'Customer' }] }) };
    const matches = await cachedCustomerMatches(pool, ['+12165550100', '2165550100', 'unknown']);
    expect(pool.query).toHaveBeenCalledTimes(1);
    expect(pool.query.mock.calls[0][0].values).toEqual([['2165550100']]);
    expect(matches.get('2165550100')).toEqual({ id: 42, name: 'Customer' });
  });
  test('unavailable enrichment cannot hide voicemail records', async () => {
    const pool = { query: jest.fn().mockRejectedValue(new Error('unavailable')) };
    const result = await loadVoicemailMetadata(pool, ['CA1'], ['2165550100']);
    expect(result.matches.size).toBe(0);
    expect(result.sync.size).toBe(0);
    expect(result.syncAvailable).toBe(false);
    expect(result.intakeAvailable).toBe(false);
  });
  test('metadata is batched, not queried once per recording', async () => {
    const pool = { query: jest.fn().mockResolvedValue({ rows: [] }) };
    const result = await loadVoicemailMetadata(pool, Array.from({length: 500}, (_, i) => 'CA' + i), ['2165550100']);
    expect(pool.query).toHaveBeenCalledTimes(4);
    expect(result.syncAvailable).toBe(true);
  });
});
