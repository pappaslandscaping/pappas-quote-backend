const {
  fetchMobileCustomerSnapshot,
  fetchMobileCustomers,
  fetchMobileToday,
  normalizePhone,
} = require('../services/homeworks/mobile-app');

const response = (data) => ({ ok: true, json: async () => ({ data }) });

describe('TwilioConnect HomeWorks data', () => {
  test('normalizes US customer phone numbers', () => {
    expect(normalizePhone('+1 (440) 555-1212')).toBe('4405551212');
  });

  test('maps the official customer directory contract', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(response({
      customers: [{
        id: 91,
        fullName: 'Jane Smith',
        email: 'jane@example.com',
        phone: '',
        cell: '440-555-1212',
        status: 'ACTIVE',
        tags: ['Mowing'],
        outstanding: '125.50',
        pastDue: '25.50',
        address: { street1: '123 Main St', city: 'Lakewood', state: 'OH', zip: '44107' },
      }],
    }));
    const customers = await fetchMobileCustomers({ accessToken: 'token', fetchImpl });
    expect(customers).toEqual([expect.objectContaining({
      id: 91,
      name: 'Jane Smith',
      phone: '440-555-1212',
      address: '123 Main St, Lakewood OH 44107',
      outstanding: 125.5,
      pastDue: 25.5,
      source: 'official_homeworks_graphql',
    })]);
  });

  test('builds a customer snapshot by exact normalized phone match', async () => {
    const fetchImpl = jest.fn()
      .mockResolvedValueOnce(response({ customers: [
        { id: 10, phone: '', cell: '(440) 555-1212' },
        { id: 11, phone: '216-555-0000', cell: '' },
      ] }))
      .mockResolvedValueOnce(response({ customers: [{
        id: 10,
        fullName: 'Jane Smith',
        phone: '',
        cell: '(440) 555-1212',
        email: 'jane@example.com',
        status: 'ACTIVE',
        tags: [],
        outstanding: '80',
        pastDue: '10',
        portalKey: 'portal-key',
        address: {},
        properties: [],
        events: [{ id: 20, title: 'Mowing', startDate: '2026-09-21', status: 'OPEN', total: '55', property: {}, timeEntryTotals: { runningTimers: 1 } }],
        estimates: [],
        invoices: [
          { id: 30, number: 12001, status: 'PAST_DUE', total: '100', paidAmount: '20', daysPastDue: 5, isSent: true },
          { id: 31, number: 12000, status: 'PAID', total: '215.35', paidAmount: '215.35', daysPastDue: 21, isSent: true },
        ],
        payments: [],
      }] }))
      .mockResolvedValueOnce(response({ smsMessages: [{ id: 40, text: 'Hello', direction: 'IN' }] }));

    const snapshot = await fetchMobileCustomerSnapshot({
      accessToken: 'token',
      fetchImpl,
      phone: '+1 440-555-1212',
    });
    expect(snapshot.customer.id).toBe(10);
    expect(snapshot.jobs[0]).toMatchObject({ title: 'Mowing', runningTimers: 1 });
    expect(snapshot.invoices[0]).toMatchObject({ balance: 80, portalUrl: expect.stringContaining('/30/portal-key') });
    expect(snapshot.invoices[1]).toMatchObject({ total: 215.35, balance: 0, daysPastDue: 0, status: 'PAID' });
    expect(snapshot.homeWorksMessages).toHaveLength(1);
  });

  test('summarizes today and accepted unscheduled services', async () => {
    const fetchImpl = jest.fn().mockResolvedValue(response({
      events: [
        { id: 1, title: 'Mowing', startDate: '2026-09-21', status: 'OPEN', total: '50', property: {}, customer: { fullName: 'A' }, users: [], timeEntryTotals: { runningTimers: 1 } },
        { id: 2, title: 'Cleanup', startDate: '2026-09-21', status: 'CLOSED', total: '100', property: {}, customer: { fullName: 'B' }, users: [], timeEntryTotals: { runningTimers: 0 } },
        { id: 6, title: 'Cancelled Visit', startDate: '2026-09-21', status: 'CANCELLED', total: '250', property: {}, customer: { fullName: 'D' }, users: [], timeEntryTotals: { runningTimers: 1 } },
        { id: 7, title: 'Canceled Visit', startDate: '2026-09-21', status: 'CANCELED', total: '300', property: {}, customer: { fullName: 'E' }, users: [], timeEntryTotals: { runningTimers: 1 } },
      ],
      routes: [{ id: 3, routeStopCount: 2 }],
      estimateLineItems: [{ id: 4, name: 'Aeration', price: '75', quantity: '1', property: {}, estimate: { id: 5, number: 1005, customerId: 7, customer: { fullName: 'C' } } }],
    }));
    const today = await fetchMobileToday({ accessToken: 'token', fetchImpl, date: '2026-09-21' });
    expect(today.summary).toMatchObject({ total: 2, open: 1, completed: 1, activeTimers: 1, scheduledRevenue: 150 });
    expect(today.jobs.map((job) => job.id)).toEqual([1, 2]);
    expect(today.unscheduledAcceptedServices[0]).toMatchObject({ name: 'Aeration', customerName: 'C' });
  });
});
