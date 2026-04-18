const {
  buildCopilotJobKey,
  normalizeResolvedRow,
  parseVisitTotal,
  upsertCopilotLiveJobs,
} = require('../services/copilot/live-jobs');

describe('copilot live jobs service', () => {
  test('buildCopilotJobKey composes a stable live-job key', () => {
    expect(buildCopilotJobKey('2026-04-18', '12345')).toBe('copilot:2026-04-18:12345');
  });

  test('parseVisitTotal parses Copilot money strings', () => {
    expect(parseVisitTotal('$145.50')).toBe(145.5);
    expect(parseVisitTotal('')).toBeNull();
    expect(parseVisitTotal(null)).toBeNull();
  });

  test('normalizeResolvedRow returns the PR1 ResolvedLiveJob contract', () => {
    const resolved = normalizeResolvedRow({
      job_key: 'copilot:2026-04-18:88',
      service_date: '2026-04-18',
      source_event_id: '88',
      source_synced_at: '2026-04-18T12:00:00.000Z',
      source_deleted_at: null,
      source_customer_id: '501',
      customer_name: 'Jane Smith',
      job_title: 'Spring Cleanup',
      source_status: 'Scheduled',
      visit_total: '150.00',
      source_crew_name: 'Jobs',
      source_employees_text: 'Tim, Rob',
      source_stop_order: 3,
      address_raw: '123 Main St, Lakewood, OH',
      overlay_job_key: 'copilot:2026-04-18:88',
      review_state: 'reviewed',
      office_note: 'Call before arrival',
      hold_from_dispatch: false,
      local_tags: ['vip'],
      address_override: null,
      customer_link_id: 21,
      property_link_id: 44,
      overlay_updated_at: '2026-04-18T12:05:00.000Z',
      overlay_updated_by_name: 'Theresa',
      dispatch_plan_job_key: 'copilot:2026-04-18:88',
      crew_override_name: 'Rob Mowing Crew',
      route_order_override: 1,
      route_locked: true,
      map_lat: '41.4767000',
      map_lng: '-81.8123000',
      map_source: 'manual_override',
      map_quality: 'street',
      print_group_key: 'rob-am',
      print_note: 'Front gate sticks',
      dispatch_updated_at: '2026-04-18T12:06:00.000Z',
      dispatch_updated_by_name: 'Theresa',
    });

    expect(resolved).toEqual({
      job_key: 'copilot:2026-04-18:88',
      service_date: '2026-04-18',
      source_system: 'copilot',
      source: {
        event_id: '88',
        synced_at: '2026-04-18T12:00:00.000Z',
        deleted_at_source: null,
        customer_id: '501',
        customer_name: 'Jane Smith',
        job_title: 'Spring Cleanup',
        status: 'Scheduled',
        visit_total: 150,
        crew_name: 'Jobs',
        employees_text: 'Tim, Rob',
        stop_order: 3,
        address: '123 Main St, Lakewood, OH',
      },
      overlay: {
        exists: true,
        review_state: 'reviewed',
        office_note: 'Call before arrival',
        hold_from_dispatch: false,
        local_tags: ['vip'],
        address_override: null,
        customer_link_id: 21,
        property_link_id: 44,
        updated_at: '2026-04-18T12:05:00.000Z',
        updated_by_name: 'Theresa',
      },
      dispatch_plan: {
        exists: true,
        crew_override_name: 'Rob Mowing Crew',
        route_order_override: 1,
        route_locked: true,
        map_lat: 41.4767,
        map_lng: -81.8123,
        map_source: 'manual_override',
        map_quality: 'street',
        print_group_key: 'rob-am',
        print_note: 'Front gate sticks',
        updated_at: '2026-04-18T12:06:00.000Z',
        updated_by_name: 'Theresa',
      },
      resolved: {
        effective_address: '123 Main St, Lakewood, OH',
        effective_crew_name: 'Rob Mowing Crew',
        effective_route_order: 1,
        included_in_dispatch: true,
        map_lat: 41.4767,
        map_lng: -81.8123,
        map_quality: 'street',
      },
      flags: {
        needs_address_review: false,
        needs_crew_review: false,
        needs_route_review: false,
        source_deleted: false,
      },
    });
  });

  test('upsertCopilotLiveJobs upserts rows and marks missing jobs deleted for a sync date', async () => {
    const query = jest
      .fn()
      .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
      .mockResolvedValueOnce({ rows: [{ is_insert: false }] })
      .mockResolvedValueOnce({ rowCount: 4 });
    const pool = { query };

    const result = await upsertCopilotLiveJobs(pool, {
      serviceDate: '2026-04-18',
      syncedAt: new Date('2026-04-18T13:00:00.000Z'),
      jobs: [
        {
          event_id: '101',
          customer_id: '5001',
          customer_name: 'First Customer',
          job_title: 'Mowing',
          status: 'Scheduled',
          visit_total: '$45.00',
          crew_name: 'Mowing',
          employees: 'Tim',
          stop_order: 1,
          address: '1 Main St',
        },
        {
          event_id: '102',
          customer_id: '5002',
          customer_name: 'Second Customer',
          job_title: 'Cleanup',
          status: 'Scheduled',
          visit_total: '$145.00',
          crew_name: 'Jobs',
          employees: 'Rob',
          stop_order: 2,
          address: '2 Main St',
        },
      ],
    });

    expect(result).toEqual({
      serviceDate: '2026-04-18',
      total: 2,
      inserted: 1,
      updated: 1,
      marked_deleted: 4,
    });
    expect(query).toHaveBeenCalledTimes(3);
    expect(query.mock.calls[0][1][0]).toBe('copilot:2026-04-18:101');
    expect(query.mock.calls[1][1][0]).toBe('copilot:2026-04-18:102');
  });
});
