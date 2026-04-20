const createCommunicationRoutes = require('../routes/communications');

const {
  getBroadcastEligibility,
  getBroadcastInclusionReasons,
  getBroadcastFilterSummary
} = createCommunicationRoutes._helpers;

describe('broadcast preview helpers', () => {
  test('marks recipients blocked for missing email on email sends', () => {
    const eligibility = getBroadcastEligibility(
      { email: '', mobile: '5551234567' },
      null,
      'email'
    );

    expect(eligibility.channel_eligible).toBe(false);
    expect(eligibility.channel_label).toBe('Email blocked');
    expect(eligibility.channel_blocked_reason).toBe('Missing email');
  });

  test('marks recipients limited to email when both-channel send only has email path', () => {
    const eligibility = getBroadcastEligibility(
      { email: 'hello@example.com', mobile: '' },
      null,
      'both'
    );

    expect(eligibility.channel_eligible).toBe(true);
    expect(eligibility.channel_label).toBe('Email only');
    expect(eligibility.email_eligible).toBe(true);
    expect(eligibility.sms_eligible).toBe(false);
  });

  test('builds inclusion reasons from applied filters', () => {
    const reasons = getBroadcastInclusionReasons(
      {
        tags: 'spring cleanup, vip',
        city: 'Lakewood',
        postal_code: '44107',
        status: 'active',
        customer_type: 'customer'
      },
      {
        tags: ['VIP'],
        cities: ['lakewood'],
        postal_codes: ['44107'],
        status: 'active',
        customer_type: 'customer',
        monthly_plan: true,
        active_since_months: 6,
        job_date: '2026-04-20'
      }
    );

    expect(reasons).toEqual([
      'Tag: VIP',
      'ZIP: 44107',
      'City: Lakewood',
      'Status: active',
      'Type: customer',
      'Monthly plan customer',
      'Active in last 6 months',
      'Scheduled on 2026-04-20'
    ]);
  });

  test('summarizes applied filters for audience state copy', () => {
    const summary = getBroadcastFilterSummary({
      tags: ['VIP'],
      exclude_tags: ['Do Not Market'],
      cities: ['Lakewood'],
      job_date: '2026-04-20'
    });

    expect(summary).toEqual([
      'Tags: VIP',
      'Exclude tags: Do Not Market',
      'Cities: Lakewood',
      'Scheduled on 2026-04-20'
    ]);
  });
});
