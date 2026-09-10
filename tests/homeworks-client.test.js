const {
  extractAccessToken,
  fetchHomeWorksEvents,
  fetchHomeWorksIntegrationStatus,
  mapHomeWorksEventToLiveJob,
  queryHomeWorksGraphql,
} = require('../services/homeworks/client');

describe('official HomeWorks API client', () => {
  test('extracts the official API token from either an environment value or compatibility cookie', () => {
    expect(extractAccessToken('direct-token')).toBe('direct-token');
    expect(extractAccessToken('other=1; copilotApiAccessToken=encoded%20token; last=2')).toBe('encoded token');
  });

  test('rejects GraphQL errors even when the HTTP request succeeds', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ errors: [{ message: 'Permission denied' }] }),
    });
    await expect(queryHomeWorksGraphql({
      accessToken: 'token',
      operationName: 'Test',
      query: 'query Test { ping }',
      fetchImpl,
    })).rejects.toThrow('Permission denied');
  });

  test('maps official event fields to the existing YardDesk schedule contract', () => {
    expect(mapHomeWorksEventToLiveJob({
      id: 501,
      customerId: 9001,
      title: 'Spring Cleanup',
      description: '<p>Gate code is 1234</p>',
      type: 'VISIT',
      status: 'OPEN',
      total: '245.50',
      budgetedHours: '2.5',
      sortOrder: 4,
      recurringEventId: 81,
      isInvoiceable: true,
      customer: { fullName: 'Jane Smith', cell: '440-555-1212', email: 'jane@example.com' },
      property: {
        id: 700,
        name: 'Home',
        address: { street1: '123 Main Street', city: 'Lakewood', state: 'OH', zip: '44107' },
      },
      crew: { name: 'Jobs Crew' },
      users: [{ firstName: 'Tim', lastName: 'Pappas' }],
    })).toMatchObject({
      event_id: '501',
      customer_id: '9001',
      customer_name: 'Jane Smith',
      crew_name: 'Jobs Crew',
      employees: 'Tim Pappas',
      address: '123 Main Street, Lakewood OH 44107',
      status: 'OPEN',
      visit_total: '245.50',
      stop_order: 4,
      raw_data: {
        source_surface: 'homeworks_graphql',
        property_id: 700,
        event_type: 'VISIT',
        frequency: 'Recurring',
        notes: 'Gate code is 1234',
        customer_phone: '440-555-1212',
        customer_email: 'jane@example.com',
      },
    });
  });

  test('fetches a date range from the official GraphQL events query', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ data: { events: [{
        id: 501,
        startDate: '2026-09-10',
        title: 'Mowing',
        status: 'OPEN',
        total: '54',
        customerId: 9001,
        customer: { fullName: 'Jane Smith' },
        property: { address: { street1: '123 Main St', city: 'Lakewood', state: 'OH', zip: '44107' } },
        users: [],
      }] } }),
    });
    const jobs = await fetchHomeWorksEvents({
      accessToken: 'token',
      startDate: '2026-09-10',
      endDate: '2026-09-12',
      fetchImpl,
    });
    expect(jobs).toEqual([expect.objectContaining({
      service_date: '2026-09-10',
      event_id: '501',
      customer_name: 'Jane Smith',
    })]);
    const request = JSON.parse(fetchImpl.mock.calls[0][1].body);
    expect(request.operationName).toBe('YardDeskSchedule');
    expect(request.variables).toEqual({ start: '2026-09-10', end: '2026-09-12' });
    expect(request.query).toContain('events(');
  });

  test('summarizes official catalog, payment labels, and QuickBooks failures', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ data: {
        company: { id: 5261, name: 'Pappas & Co. Landscaping', quickbooks: null },
        items: [{ id: 1 }, { id: 2 }],
        payments: [{ id: 3, date: '2026-09-10', method: 'CREDIT_CARD', methodDisplayName: 'Credit Card', totalAmount: '100' }],
        invoices: [
          { id: 4, quickbooksSyncFailedCount: 3 },
          { id: 5, quickbooksSyncFailedCount: 0 },
        ],
      } }),
    });
    const status = await fetchHomeWorksIntegrationStatus({ accessToken: 'token', fetchImpl });
    expect(status).toMatchObject({
      connected: true,
      source: 'official_homeworks_graphql',
      catalog: { itemCount: 2 },
      payments: [{ method: 'Credit Card' }],
      quickbooks: { recentInvoiceFailures: 1, recentInvoiceFailureIds: [4] },
    });
  });
});

