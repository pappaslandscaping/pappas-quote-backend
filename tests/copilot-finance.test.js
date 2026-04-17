const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  DATABASE_FALLBACK_SOURCE,
  extractCopilotRevenueReportTotal,
  getCopilotRevenueWindow,
  getRevenueSnapshotExpiry,
  isRevenueSnapshotForWindow,
  buildRevenueMetric,
} = require('../lib/copilot-finance');
const {
  normalizeAgingSnapshot,
  hasValidAgingBuckets,
} = require('../lib/copilot-aging');

describe('Copilot-backed finance hardening', () => {
  test('parses Revenue by Crew live table totals row directly', () => {
    const html = `
      <html>
        <body>
          <h4>Revenue by Crew</h4>
          <table class="table copilot-table table2excel">
            <thead>
              <tr class="headers">
                <th width="20%">Crew</th>
                <th>Apr-26</th>
                <th>Total</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Rob Mowing Crew</td>
                <td style="text-align:center;color:#2EB26E;">6,271.08</td>
                <td style="text-align:right;">$6,271.08</td>
              </tr>
              <tr>
                <td>Tim Mowing Crew</td>
                <td style="text-align:center;color:#2EB26E;">2,977.91</td>
                <td style="text-align:right;">$2,977.91</td>
              </tr>
              <tr>
                <td><strong>Total </strong></td>
                <td style="text-align:center;"><strong>$9,248.99</strong></td>
                <td style="text-align:right;"><strong>$9,248.99</strong></td>
              </tr>
            </tbody>
          </table>
        </body>
      </html>
    `;

    expect(extractCopilotRevenueReportTotal(html)).toBe(9248.99);
  });

  test('uses Copilot revenue snapshot when it is meaningful', () => {
    const metric = buildRevenueMetric({
      revenueMonth: 5678.95,
      now: new Date('2026-04-17T13:09:00-04:00'),
      copilotRevenueSnapshot: {
        source: LIVE_COPILOT_SOURCE,
        as_of: '2026-04-17T13:10:00-04:00',
        period_start: '2026-04-01',
        period_end: '2026-04-17',
        total: 9248.99,
      },
    });

    expect(metric).toEqual({
      revenue: 9248.99,
      revenue_source: LIVE_COPILOT_SOURCE,
      revenue_as_of: '2026-04-17T13:10:00-04:00',
      revenue_period_start: '2026-04-01',
      revenue_period_end: '2026-04-17',
    });
  });

  test('falls back to database when Copilot parsing yields a bogus zero and local revenue is positive', () => {
    const metric = buildRevenueMetric({
      revenueMonth: 5678.95,
      now: new Date('2026-04-17T13:09:00-04:00'),
      copilotRevenueSnapshot: {
        source: LIVE_COPILOT_SOURCE,
        as_of: '2026-04-17T13:10:00-04:00',
        period_start: '2026-04-01',
        period_end: '2026-04-17',
        total: 0,
      },
    });

    expect(metric).toEqual({
      revenue: 5678.95,
      revenue_source: DATABASE_FALLBACK_SOURCE,
      revenue_as_of: null,
      revenue_period_start: '2026-04-01',
      revenue_period_end: '2026-04-17',
    });
  });

  test('same-day snapshot validity fails across day rollover', () => {
    const currentWindow = getCopilotRevenueWindow(new Date('2026-04-17T09:00:00-04:00'));
    const staleSnapshot = {
      source: PERSISTED_COPILOT_SNAPSHOT_SOURCE,
      as_of: '2026-04-16T23:58:00-04:00',
      period_start: '2026-04-01',
      period_end: '2026-04-16',
      total: 8100.12,
    };

    expect(isRevenueSnapshotForWindow(staleSnapshot, currentWindow)).toBe(false);
    expect(getRevenueSnapshotExpiry(staleSnapshot, currentWindow, 5 * 60 * 1000)).toBe(0);
  });

  test('current-window persisted snapshot remains valid until TTL expiry', () => {
    const currentWindow = getCopilotRevenueWindow(new Date('2026-04-17T09:00:00-04:00'));
    const snapshot = {
      source: PERSISTED_COPILOT_SNAPSHOT_SOURCE,
      as_of: '2026-04-17T12:00:00.000Z',
      period_start: '2026-04-01',
      period_end: '2026-04-17',
      total: 9248.99,
    };

    expect(isRevenueSnapshotForWindow(snapshot, currentWindow)).toBe(true);
    expect(getRevenueSnapshotExpiry(snapshot, currentWindow, 300000)).toBe(new Date('2026-04-17T12:00:00.000Z').getTime() + 300000);
  });

  test('normalizes the Copilot aging snapshot shape used by the invoice page', () => {
    const snapshot = normalizeAgingSnapshot({
      source: LIVE_COPILOT_SOURCE,
      as_of: '2026-04-17T13:12:00.000Z',
      buckets: {
        within_30: { count: 254, total: 23088.65, invoices: [{ invoice_number: '10448' }] },
        '31_60': { count: 0, total: 0, invoices: [] },
        '61_90': { count: 1, total: 48.60, invoices: [{ invoice_number: '10023' }] },
        '90_plus': { count: 40, total: 5462.96, invoices: [{ invoice_number: '9297' }] },
      },
    }, PERSISTED_COPILOT_SNAPSHOT_SOURCE);

    expect(hasValidAgingBuckets(snapshot.buckets)).toBe(true);
    expect(snapshot.source).toBe(PERSISTED_COPILOT_SNAPSHOT_SOURCE);
    expect(snapshot.buckets.within_30.total).toBe(23088.65);
    expect(snapshot.buckets['90_plus'].count).toBe(40);
  });

  test('rejects malformed aging snapshots before they can be served or persisted', () => {
    const badSnapshot = normalizeAgingSnapshot({
      source: LIVE_COPILOT_SOURCE,
      buckets: {
        within_30: { count: 1, total: 10, invoices: [] },
      },
    });

    expect(badSnapshot).toBeNull();
  });
});
