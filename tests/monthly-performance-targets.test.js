const { parseTarget, parseCustomerId, loadCustomerTargets, saveCustomerTarget } = require('../lib/monthly-performance-targets');

describe('customer pricing targets', () => {
  test('validates HomeWorks IDs and hourly target amounts', () => {
    expect(parseCustomerId(123)).toBe('123');
    expect(() => parseCustomerId('customer 123')).toThrow();
    expect(parseTarget('125.50')).toBe(125.5);
    expect(parseTarget('')).toBeNull();
    expect(() => parseTarget(0)).toThrow();
    expect(() => parseTarget(12.345)).toThrow();
  });

  test('loads valid persisted targets and atomically saves one customer', async () => {
    const pool = { query: jest.fn()
      .mockResolvedValueOnce({ rows: [{ value: { '123': 150, 'bad': 9, '456': '125.50' } }] })
      .mockResolvedValue({ rows: [] }) };
    expect(await loadCustomerTargets(pool)).toEqual({ '123': 150, '456': 125.5 });
    expect(await saveCustomerTarget(pool, 123, 175)).toEqual({ customerId: '123', targetPerHour: 175 });
    expect(pool.query.mock.calls[1][0]).toContain('ON CONFLICT (key) DO UPDATE');
    expect(pool.query.mock.calls[1][1]).toEqual(['monthly_performance_customer_targets', '123', 175]);
    expect(await saveCustomerTarget(pool, 123, null)).toEqual({ customerId: '123', targetPerHour: null });
    expect(pool.query.mock.calls[2][0]).toContain(' - $2::text');
  });
});
