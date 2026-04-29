const createCommunicationRoutes = require('../routes/communications');
const createCampaignRoutes = require('../routes/campaigns');

const {
  ensureBroadcastCustomersForLiveDate,
  lookupBroadcastJobsForCustomerOnDate,
} = createCommunicationRoutes._helpers;

const {
  buildActiveCampaignCustomerQuery,
} = createCampaignRoutes._helpers;

describe('marketing live-model helpers', () => {
  test('creates minimal backend customers for unmatched live Copilot dispatch customers', async () => {
    const pool = {
      query: jest
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              source_customer_id: '2684725',
              customer_name: 'Ron Wacker',
              address: '11508 Lake Avenue Cleveland OH 44102, US',
            },
            {
              source_customer_id: '2640566',
              customer_name: 'Sue Moody',
              address: '1617 Riverside Drive Lakewood OH 44107, US',
            },
          ],
        })
        .mockResolvedValueOnce({ rowCount: 1, rows: [] })
        .mockResolvedValueOnce({ rowCount: 1, rows: [] }),
    };

    const result = await ensureBroadcastCustomersForLiveDate(pool, '2026-04-30');

    expect(result).toEqual({ inserted: 2, candidates: 2 });
    expect(pool.query).toHaveBeenCalledTimes(3);
    expect(pool.query.mock.calls[0][0]).toContain('FROM copilot_live_jobs clj');
    expect(pool.query.mock.calls[1][0]).toContain('INSERT INTO customers');
    expect(pool.query.mock.calls[1][0]).toContain('customer_number = $1::text');
    expect(pool.query.mock.calls[1][1]).toEqual([
      '2684725',
      'Ron Wacker',
      '11508 Lake Avenue Cleveland OH 44102, US',
      'Created from live Copilot dispatch job scheduled 2026-04-30',
    ]);
  });

  test('broadcast send-time job lookup prefers live jobs before scheduled_jobs fallback', async () => {
    const pool = {
      query: jest
        .fn()
        .mockResolvedValueOnce({
          rows: [
            {
              service_type: 'Weekly Mowing',
              address: '123 Main St, Lakewood, OH 44107',
              service_price: '55.00',
              job_date: '2026-04-21',
            },
          ],
        }),
    };

    const jobs = await lookupBroadcastJobsForCustomerOnDate(pool, 42, '2026-04-21');

    expect(jobs).toHaveLength(1);
    expect(jobs[0]).toMatchObject({
      service_type: 'Weekly Mowing',
      address: '123 Main St, Lakewood, OH 44107',
      service_price: '55.00',
      job_date: '2026-04-21',
    });
    expect(pool.query).toHaveBeenCalledTimes(1);
    expect(pool.query.mock.calls[0][0]).toContain('FROM copilot_live_jobs clj');
    expect(pool.query.mock.calls[0][0]).toContain('SELECT fallback_customer.id');
    expect(pool.query.mock.calls[0][0]).toContain('ORDER BY fallback_customer.id ASC');
  });

  test('broadcast send-time job lookup falls back to scheduled_jobs when live linkage is missing', async () => {
    const pool = {
      query: jest
        .fn()
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [
            {
              service_type: 'Spring Cleanup',
              address: '456 Oak Ave, Rocky River, OH 44116',
              service_price: '210.00',
              job_date: '2026-04-21',
            },
          ],
        }),
    };

    const jobs = await lookupBroadcastJobsForCustomerOnDate(pool, 77, '2026-04-21');

    expect(jobs).toHaveLength(1);
    expect(pool.query).toHaveBeenCalledTimes(2);
    expect(pool.query.mock.calls[1][0]).toContain('FROM scheduled_jobs');
  });

  test('broadcast send-time job lookup skips scheduled_jobs fallback when live date is authoritative', async () => {
    const pool = {
      query: jest
        .fn()
        .mockResolvedValueOnce({ rows: [] }),
    };

    const jobs = await lookupBroadcastJobsForCustomerOnDate(pool, 77, '2026-04-30', {
      hasLiveJobsForDate: true,
    });

    expect(jobs).toEqual([]);
    expect(pool.query).toHaveBeenCalledTimes(1);
    expect(pool.query.mock.calls[0][0]).toContain('FROM copilot_live_jobs clj');
  });

  test('campaign active-segment query prefers live jobs with scheduled fallback', () => {
    const sql = buildActiveCampaignCustomerQuery();

    expect(sql).toContain('FROM copilot_live_jobs clj');
    expect(sql).toContain('LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key');
    expect(sql).toContain('live_customer.customer_number = clj.source_customer_id');
    expect(sql).toContain("clj.service_date >= CURRENT_DATE - ($1::text || ' months')::INTERVAL");
    expect(sql).toContain("COALESCE(sj.job_date::date, sj.created_at::date) >= CURRENT_DATE - ($2::text || ' months')::INTERVAL");
  });
});
