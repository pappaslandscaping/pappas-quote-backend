const {
  extractAccessToken,
  fetchHomeWorksBusinessSummary,
  fetchHomeWorksEvents,
  fetchHomeWorksIntegrationStatus,
  mapHomeWorksEventToLiveJob,
  queryHomeWorksGraphql,
  summarizeHomeWorksBusinessData,
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

  test('summarizes official HomeWorks balances, customers, invoices, and current-month payments', () => {
    const summary = summarizeHomeWorksBusinessData({
      now: new Date('2026-09-10T20:00:00Z'),
      customers: [
        { id: 1, outstanding: '100.25', pastDue: '20.00' },
        { id: 2, outstanding: '49.75', pastDue: '0' },
      ],
      invoices: [
        { id: 11, status: 'PENDING', total: '130', paidAmount: '0', isSent: true, isArchived: false, isDeleted: false },
        { id: 12, status: 'PAST_DUE', total: '30', paidAmount: '10', daysPastDue: 4, isSent: true, isArchived: false, isDeleted: false },
        { id: 13, status: 'DRAFT', isSent: false, isArchived: false, isDeleted: false },
        { id: 14, status: 'PENDING', total: '25', paidAmount: '0', isSent: false, isArchived: false, isDeleted: false },
      ],
      payments: [
        { id: 21, date: '2026-09-08', totalAmount: '75.50' },
        { id: 22, date: '2026-08-30', totalAmount: '20.00' },
      ],
    });

    expect(summary).toMatchObject({
      source: 'official_homeworks_graphql',
      financials: { outstanding: 150, pastDue: 20, collectedThisMonth: 75.5 },
      counts: { customers: 2, customersPastDue: 1, outstandingInvoices: 2, pastDueInvoices: 1, paymentsThisMonth: 1 },
    });
  });

  test('fetches company balances from sent invoice records after payments, not aggregate original charges', async () => {
    const fetchImpl = jest.fn()
      .mockResolvedValueOnce({ok:true,json:async()=>({data:{customers:[{id:1,fullName:'Example',outstanding:'80',credit:'0'}]}})})
      .mockResolvedValueOnce({ok:true,json:async()=>({data:{invoices:[
        {id:11,customerId:1,status:'PAST_DUE',total:'15',paidAmount:'5',isSent:true,daysPastDue:4},
        {id:12,customerId:1,status:'PENDING',total:'70',paidAmount:'0',isSent:true},
        {id:13,customerId:1,status:'PENDING',total:'200',paidAmount:'0',isSent:false},
      ]}})})
      .mockResolvedValueOnce({ok:true,json:async()=>({data:{payments:[{id:3,date:'2026-09-10',totalAmount:'25'}]}})});
    const summary = await fetchHomeWorksBusinessSummary({ accessToken:'token',fetchImpl,now:new Date('2026-09-10T20:00:00Z') });
    expect(summary.financials).toMatchObject({ outstanding:80,pastDue:10,collectedThisMonth:25,balanceBasis:'homeworks_sent_invoice_balances' });
    expect(summary.counts).toMatchObject({customers:1,outstandingInvoices:2,pastDueInvoices:1});
    const operations=fetchImpl.mock.calls.map(call=>JSON.parse(call[1].body));
    expect(operations[0].operationName).toBe('CustomerBillingDirectory');
    expect(operations[1].operationName).toBe('CompanyOpenBillingRecords');
    expect(operations[1].query).toContain('isSent');
    expect(operations[2].variables).toEqual({monthStart:'2026-09-01'});
    expect(operations.every(operation=>!operation.query.includes('invoiceReport'))).toBe(true);
  });

  test('keeps reconciled billing available if optional monthly payments fail', async () => {
    const fetchImpl=jest.fn()
      .mockResolvedValueOnce({ok:true,json:async()=>({data:{customers:[{id:1,outstanding:'80',credit:'0'}]}})})
      .mockResolvedValueOnce({ok:true,json:async()=>({data:{invoices:[{id:1,customerId:1,total:'80',paidAmount:'0',status:'PENDING',isSent:true}]}})})
      .mockResolvedValueOnce({ok:true,json:async()=>({errors:[{message:'Payments unavailable'}]})});
    const summary=await fetchHomeWorksBusinessSummary({accessToken:'token',fetchImpl,now:new Date('2026-09-10T20:00:00Z')});
    expect(summary.financials).toMatchObject({outstanding:80,pastDue:0,collectedThisMonth:null});
  });

  test('does not fall back to unverified totals if the sent invoice audit fails', async () => {
    const fetchImpl=jest.fn()
      .mockResolvedValueOnce({ok:true,json:async()=>({data:{customers:[{id:1,outstanding:'80',credit:'0'}]}})})
      .mockResolvedValueOnce({ok:true,json:async()=>({data:{invoices:[]}})});
    await expect(fetchHomeWorksBusinessSummary({accessToken:'token',fetchImpl})).rejects.toThrow('do not reconcile');
  });
});
