const { buildOperationalReport, fetchOperationalReport } = require('../lib/operational-reports');

const visit = (id, options = {}) => ({
  id, title: 'Mowing', startDate: '2026-09-05', subtotal: '60', budgetedHours: '0.5',
  customer: { id: 11, fullName: 'Customer A' },
  property: { id: 21, address: { street1: '1 Main St' } },
  crew: { name: 'Crew A' }, timeEntryTotals: { pastTimeEntrySeconds: 0 }, ...options,
});

test('pricing gaps use a saved customer target and exclude repeated season face values', () => {
  const events = [visit(1), visit(2),
    visit(3, { subtotal: '2310', recurringEventId: 55 }),
    visit(4, { subtotal: '2310', recurringEventId: 55 })];
  const report = buildOperationalReport({ month: '2026-09', events, targets: { 11: 150 } });
  expect(report.pricing[0]).toMatchObject({ visits: 2, value: 120, hours: 1, perHour: 120, target: 150, gap: 30 });
  expect(report.issues.repeatedContractAmount).toBe(2);
  expect(report.profitability.actualProfitAvailable).toBe(false);
});

test('open balances age by due status without mixing in the selected work month', () => {
  const report = buildOperationalReport({ month: '2026-09', events: [], invoices: [
    { id: 1, customerId: 11, total: '100', paidAmount: '25', isSent: true, dueDate: '2026-10-01', daysPastDue: 40 },
    { id: 2, customerId: 11, total: '200', paidAmount: '0', isSent: true, dueDate: '2026-08-21', daysPastDue: 0 },
    { id: 3, customerId: 11, total: '999', paidAmount: '0', isSent: false, dueDate: '2026-08-01' },
  ], invoiceCustomers: { 11: 'Customer A' }, asOf: new Date('2026-09-30T16:00:00Z') });
  expect(report.receivables.total).toBe(275);
  expect(report.receivables.aging).toMatchObject({ current: 75, days31to60: 200 });
  expect(report.receivables.rows[0].customer).toBe('Customer A');
});

test('peer comparisons are labeled separately from saved customer targets', () => {
  const events = Array.from({ length: 10 }, (_, index) => visit(index + 1, {
    subtotal: index === 0 ? '30' : '60',
    customer: { id: index + 1, fullName: `Customer ${index + 1}` },
    property: { id: index + 1, address: { street1: `${index + 1} Main St` } },
  }));
  const report = buildOperationalReport({ month: '2026-09', events });
  expect(report.pricing.every((row) => row.target === null)).toBe(true);
  expect(report.peerPricing[0]).toMatchObject({ customer: 'Customer 1', peerCount: 10, perHour: 60, peerMedian: 120 });
});

test('contract tracker uses the full series counts and prioritizes actionable data issues', () => {
  const report = buildOperationalReport({ month: '2026-09', events: [
    visit(1, { recurringEventId: 55 }),
    visit(2, { subtotal: '0', budgetedHours: '0', crew: null }),
  ], recurring: [{ id: 55, title: 'Mowing', eventCount: 33, lineItems: [] }],
  contractProgress: [
    { recurringEventId: 55, status: 'CLOSED', _count: { id: 25 } },
    { recurringEventId: 55, status: 'OPEN', _count: { id: 8 } },
  ] });
  expect(report.contracts[0]).toMatchObject({ completedVisits: 25, remainingVisits: 8, monthCompleted: 1 });
  expect(report.issueExamples[0].reasons).toEqual(expect.arrayContaining(['No visit price', 'No budgeted duration', 'No crew']));
});

test('an optional HomeWorks section failure is visible without hiding completed visits', async () => {
  const queryImpl = jest.fn(async ({ operationName }) => {
    if (operationName === 'YardDeskOperationalVisits') return { events: [visit(1)] };
    if (operationName === 'YardDeskOperationalRoutes') throw new Error('unavailable');
    if (operationName === 'YardDeskOpenInvoices') return { invoices: [] };
    return { recurringEvents: [] };
  });
  const pool = { query: jest.fn(async () => ({ rows: [] })) };
  const report = await fetchOperationalReport({ pool, month: '2026-09', queryImpl });
  expect(report.profitability.completedVisits).toBe(1);
  expect(report.warnings[0]).toMatch(/Routes: unavailable/);
});
