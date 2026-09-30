const { buildTargetProposals, proposalBounds } = require('../lib/customer-target-proposals');
const { saveCustomerTargetsBulk } = require('../lib/monthly-performance-targets');

const visit = (id, customerId, price = 60, options = {}) => ({
  id, title: 'Mowing', startDate: '2026-09-12', subtotal: String(price), budgetedHours: '0.5',
  customer: { id: customerId, fullName: `Customer ${customerId}` },
  property: { id: customerId, name: `${customerId} Main St` }, ...options,
});

test('proposals use other comparable properties and hold large increases for review', () => {
  const events = [];
  for (let id = 1; id <= 12; id++) {
    for (let n = 0; n < 3; n++) events.push(visit(id * 10 + n, id, id === 1 ? 30 : 60));
  }
  const report = buildTargetProposals({ month: '2026-09', events });
  expect(report.summary.customers).toBe(12);
  const low = report.proposals.find((row) => row.customerId === '1');
  expect(low).toMatchObject({ currentPerHour: 60, peerPerHour: 120, suggestedTarget: 120,
    reviewStatus: 'review', comparableProperties: 1 });
  const normal = report.proposals.find((row) => row.customerId === '2');
  expect(normal).toMatchObject({ currentPerHour: 120, suggestedTarget: 120, reviewStatus: 'ready' });
});

test('proposals exclude repeated season amounts and require an adequate peer group', () => {
  const events = [visit(1, 1), visit(2, 1, 2310, { recurringEventId: 7 }),
    visit(3, 1, 2310, { recurringEventId: 7 })];
  const report = buildTargetProposals({ month: '2026-09', events });
  expect(report.excluded.repeatedSeasonAmount).toBe(2);
  expect(report.proposals[0]).toMatchObject({ visits: 1, suggestedTarget: null,
    reviewStatus: 'review' });
  expect(proposalBounds('2026-09')).toMatchObject({ start: '2026-07-01', end: '2026-09-30' });
});

test('bulk target save does not overwrite existing customer goals', async () => {
  const calls = [];
  const client = { query: jest.fn(async (sql, params) => {
    calls.push({ sql, params });
    if (sql.includes('FOR UPDATE')) return { rows: [{ value: { 1: 150 } }] };
    return { rows: [] };
  }), release: jest.fn() };
  const result = await saveCustomerTargetsBulk({ connect: async () => client }, { 1: 160, 2: 125 });
  expect(result).toEqual({ saved: 1, skippedExisting: 1 });
  const update = calls.find((call) => call.sql.includes('UPDATE business_settings'));
  expect(JSON.parse(update.params[1])).toEqual({ 2: 125 });
  expect(calls.at(-1).sql).toBe('COMMIT');
  expect(client.release).toHaveBeenCalled();
});
