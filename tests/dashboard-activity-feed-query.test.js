const { buildDashboardActivityFeedQuery } = require('../lib/dashboard-activity-feed-query');

describe('dashboard activity feed query', () => {
  test('payment activity uses actual paid_at dates only', () => {
    const query = buildDashboardActivityFeedQuery();
    const paymentBranch = query.match(/SELECT 'payment_received'[\s\S]*?ORDER BY paid_at DESC LIMIT 10/);

    expect(paymentBranch).toBeTruthy();
    expect(paymentBranch[0]).toContain('paid_at as timestamp');
    expect(paymentBranch[0]).toContain('paid_at IS NOT NULL');
    expect(paymentBranch[0]).not.toContain('updated_at');
  });
});
