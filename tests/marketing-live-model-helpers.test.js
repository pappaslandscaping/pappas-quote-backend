const createCommunicationRoutes = require('../routes/communications');
const createCampaignRoutes = require('../routes/campaigns');

const {
  lookupBroadcastJobsForCustomerOnDate,
} = createCommunicationRoutes._helpers;

const {
  buildActiveCampaignCustomerQuery,
} = createCampaignRoutes._helpers;

describe('marketing live-model helpers', () => {
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
    expect(pool.query.mock.calls[0][0]).toContain('COALESCE(yjo.customer_link_id, live_customer.id) = $1');
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

  test('campaign active-segment query prefers live jobs with scheduled fallback', () => {
    const sql = buildActiveCampaignCustomerQuery();

    expect(sql).toContain('FROM copilot_live_jobs clj');
    expect(sql).toContain('LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key');
    expect(sql).toContain('live_customer.customer_number = clj.source_customer_id');
    expect(sql).toContain("clj.service_date >= CURRENT_DATE - ($1::text || ' months')::INTERVAL");
    expect(sql).toContain("COALESCE(sj.job_date::date, sj.created_at::date) >= CURRENT_DATE - ($2::text || ' months')::INTERVAL");
  });
});
