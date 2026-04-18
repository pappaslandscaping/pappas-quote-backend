jest.mock('../services/copilot/client', () => {
  const actual = jest.requireActual('../services/copilot/client');
  return {
    ...actual,
    getCopilotToken: jest.fn(),
    fetchCopilotScheduleGridJobsForDate: jest.fn(),
  };
});

const {
  getCopilotToken,
  fetchCopilotScheduleGridJobsForDate,
  parseCopilotScheduleGridDayHtml,
} = require('../services/copilot/client');

const {
  buildCopilotJobKey,
  fetchLiveCopilotScheduleDate,
  getCopilotLiveJobs,
  mapResolvedLiveJobToScheduleJob,
  normalizeResolvedRow,
  parseVisitTotal,
  upsertCopilotLiveJobs,
} = require('../services/copilot/live-jobs');

describe('copilot live jobs service', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

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
      source_surface: 'route_day',
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
      source_surface: 'route_day',
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
        surface: 'route_day',
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

  test('parseCopilotScheduleGridDayHtml parses the server-rendered Schedule grid rows', () => {
    const jobs = parseCopilotScheduleGridDayHtml(`
      <table class="table copilot-table table--with-hide-options">
        <tbody>
          <tr data-row-event-id="46122913">
            <td><input value="46122913"></td>
            <td>Apr 17, 2026</td>
            <td><strong><a class="getEventDetails" data-id="46122913" href="#">Spring Cleanup</a></strong></td>
            <td><span class="row-crew-label">Rob Mowing Crew<br><small>Robert Ellison, Wilkyn Camacho</small></span></td>
            <td><a href="/customers/details/1066227">Shibani Faehnle</a></td>
            <td><a href="/assets/details/edit/961507">1273 West 104th Street</a></td>
            <td>1273 West 104th Street Cleveland OH 44102, US</td>
            <td><span class="label">Visit</span></td>
            <td><span class="status-label green">Invoiced</span></td>
            <td>Single</td>
            <td>Apr 17, 2026</td>
            <td><span class="status-label green">Closed</span><br><a href="/finances/invoices/view/2736729">#10471</a></td>
            <td><div class="note_data_content"></div></td>
            <td><div class="time-tracking-hours">0.00</div></td>
            <td><div class="bh-total">0.00</div></td>
            <td>600.00</td>
            <td><a href="/scheduler/edit/46122913"></a></td>
          </tr>
        </tbody>
      </table>
    `);

    expect(jobs).toEqual([expect.objectContaining({
      event_id: '46122913',
      job_id: '46122913',
      customer_id: '1066227',
      customer_name: 'Shibani Faehnle',
      property_id: '961507',
      property_name: '1273 West 104th Street',
      crew_name: 'Rob Mowing Crew',
      employees: 'Robert Ellison, Wilkyn Camacho',
      address: '1273 West 104th Street Cleveland OH 44102, US',
      status: 'Closed',
      invoice_number: '#10471',
      invoiceable: 'Invoiced',
      visit_total: '600.00',
      job_title: 'Spring Cleanup',
      event_type: 'Visit',
      frequency: 'Single',
      tracked_time: '0.00',
      budgeted_hours: '0.00',
    })]);
  });

  test('parseCopilotScheduleGridDayHtml allows legitimate zero-event grid days', () => {
    expect(parseCopilotScheduleGridDayHtml(`
      <table class="table copilot-table">
        <tr><td style="text-align:center;">No Events Found</td></tr>
      </table>
    `)).toEqual([]);
  });

  test('mapResolvedLiveJobToScheduleJob flattens the live mirror into the Schedule contract', () => {
    const mapped = mapResolvedLiveJobToScheduleJob(normalizeResolvedRow({
      job_key: 'copilot:2026-04-18:visit-101',
      service_date: '2026-04-18',
      source_surface: 'route_day',
      source_event_id: 'visit-101',
      source_synced_at: '2026-04-18T13:30:00.000Z',
      source_deleted_at: null,
      source_customer_id: 'cust-9',
      customer_name: 'Jane Doe',
      job_title: 'Spring Cleanup',
      source_status: 'Started',
      visit_total: '120.50',
      source_crew_name: 'Crew A',
      source_employees_text: 'Tim, Rob',
      source_stop_order: 3,
      address_raw: '123 Main St, Lakewood OH',
      overlay_job_key: 'copilot:2026-04-18:visit-101',
      review_state: 'reviewed',
      office_note: 'Gate code on file',
      hold_from_dispatch: false,
      local_tags: ['vip'],
      address_override: null,
      customer_link_id: 55,
      property_link_id: 91,
      overlay_updated_at: '2026-04-18T13:31:00.000Z',
      overlay_updated_by_name: 'Theresa',
      dispatch_plan_job_key: 'copilot:2026-04-18:visit-101',
      crew_override_name: 'Crew A',
      route_order_override: 2,
      route_locked: false,
      map_lat: '41.4767000',
      map_lng: '-81.8123000',
      map_source: 'manual_override',
      map_quality: 'street',
      print_group_key: 'crew-a-am',
      print_note: null,
      dispatch_updated_at: '2026-04-18T13:32:00.000Z',
      dispatch_updated_by_name: 'Theresa',
    }));

    expect(mapped).toMatchObject({
      id: 'copilot:2026-04-18:visit-101',
      source_system: 'copilot',
      source_kind: 'live_schedule',
      source_surface: 'route_day',
      freshness_source: 'mirror',
      fetched_at: '2026-04-18T13:30:00.000Z',
      is_read_only: true,
      can_edit: false,
      can_complete: false,
      can_delete: false,
      job_date: '2026-04-18',
      service_date: '2026-04-18',
      visit_id: 'visit-101',
      copilot_visit_id: 'visit-101',
      copilot_customer_id: 'cust-9',
      customer_id: 55,
      local_customer_id: 55,
      customer_name: 'Jane Doe',
      address: '123 Main St, Lakewood OH',
      service_type: 'Spring Cleanup',
      service_title: 'Spring Cleanup',
      service_price: 120.5,
      crew_assigned: 'Crew A',
      crew_name: 'Crew A',
      crew_members_text: 'Tim, Rob',
      status: 'in_progress',
      status_raw: 'Started',
      route_order: 2,
      stop_order: 2,
      special_notes: 'Gate code on file',
      lat: 41.4767,
      lng: -81.8123,
      geocode_quality: 'street',
      has_street_address: true,
      geocode_address: '123 Main St, Lakewood OH',
    });
  });

  test('fetchLiveCopilotScheduleDate fetches and persists a selected date from Copilot', async () => {
    fetchCopilotScheduleGridJobsForDate.mockResolvedValue({
      raw: { html_length: 1000 },
      jobs: [{
        event_id: 'visit-101',
        customer_id: 'cust-9',
        customer_name: 'Jane Doe',
        job_title: 'Spring Cleanup',
        status: 'Started',
        visit_total: '$120.50',
        crew_name: 'Crew A',
        employees: 'Tim, Rob',
        stop_order: 3,
        address: '123 Main St, Lakewood OH',
      }],
    });
    const poolClient = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
        .mockResolvedValueOnce({ rowCount: 0 }),
    };

    const result = await fetchLiveCopilotScheduleDate({
      poolClient,
      syncDate: '2026-04-17',
      cookieHeader: 'copilot=abc',
      fetchImpl: jest.fn(),
    });

    expect(result).toMatchObject({
      date: '2026-04-17',
      source: 'live',
      error: null,
    });
    expect(result.fetched_at).toBeTruthy();
    expect(fetchCopilotScheduleGridJobsForDate).toHaveBeenCalledWith(expect.objectContaining({
      cookieHeader: 'copilot=abc',
      syncDate: '2026-04-17',
    }));
    expect(poolClient.query).toHaveBeenCalledTimes(2);
    expect(poolClient.query.mock.calls[0][1][0]).toBe('copilot:2026-04-17:visit-101');
    expect(poolClient.query.mock.calls[0][0]).toContain('INSERT INTO copilot_schedule_live_jobs');
  });

  test('getCopilotLiveJobs returns live schedule payload for selected dates when Copilot fetch succeeds', async () => {
    getCopilotToken.mockResolvedValue({ cookieHeader: 'copilot=abc' });
    fetchCopilotScheduleGridJobsForDate
      .mockResolvedValueOnce({
        raw: { html_length: 1000 },
        jobs: [{
          event_id: 'visit-101',
          customer_id: 'cust-9',
          customer_name: 'Jane Doe',
          job_title: 'Spring Cleanup',
          status: 'Started',
          visit_total: '$120.50',
          crew_name: 'Crew A',
          employees: 'Tim, Rob',
          stop_order: 3,
          address: '123 Main St, Lakewood OH',
        }],
      })
      .mockResolvedValueOnce({
        raw: { html_length: 500, no_events_found: true },
        jobs: [],
      });

    const poolClient = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
        .mockResolvedValueOnce({ rowCount: 0 })
        .mockResolvedValueOnce({ rowCount: 0 })
        .mockResolvedValueOnce({
          rows: [{
            job_key: 'copilot:2026-04-17:visit-101',
            service_date: '2026-04-17',
            source_surface: 'schedule_grid',
            source_event_id: 'visit-101',
            source_synced_at: '2026-04-17T13:30:00.000Z',
            source_deleted_at: null,
            source_customer_id: 'cust-9',
            customer_name: 'Jane Doe',
            job_title: 'Spring Cleanup',
            source_status: 'Started',
            visit_total: '120.50',
            source_crew_name: 'Crew A',
            source_employees_text: 'Tim, Rob',
            source_stop_order: 3,
            address_raw: '123 Main St, Lakewood OH',
            overlay_job_key: null,
            review_state: null,
            office_note: null,
            hold_from_dispatch: false,
            local_tags: [],
            address_override: null,
            customer_link_id: null,
            property_link_id: null,
            overlay_updated_at: null,
            overlay_updated_by_name: null,
            dispatch_plan_job_key: null,
            crew_override_name: null,
            route_order_override: null,
            route_locked: false,
            map_lat: null,
            map_lng: null,
            map_source: null,
            map_quality: null,
            print_group_key: null,
            print_note: null,
            dispatch_updated_at: null,
            dispatch_updated_by_name: null,
          }],
        }),
    };

    const result = await getCopilotLiveJobs({
      poolClient,
      startDate: '2026-04-17',
      endDate: '2026-04-18',
      fetchImpl: jest.fn(),
    });

    expect(result).toMatchObject({
      start_date: '2026-04-17',
      end_date: '2026-04-18',
      freshness: {
        source: 'live',
        stale: false,
      },
      stats: {
        total: 1,
        byStatus: {
          in_progress: 1,
        },
        byCrew: {
          'Crew A': 1,
        },
        totalRevenue: 120.5,
      },
    });
    expect(result.freshness.fetched_at).toBeTruthy();
    expect(result.freshness.per_date).toHaveLength(2);
    expect(result.freshness.per_date[0]).toMatchObject({
      date: '2026-04-17',
      source: 'live',
      error: null,
    });
    expect(result.freshness.per_date[1]).toMatchObject({
      date: '2026-04-18',
      source: 'live',
      error: null,
    });
    expect(result.jobs[0]).toMatchObject({
      id: 'copilot:2026-04-17:visit-101',
      source_surface: 'schedule_grid',
      freshness_source: 'live',
      customer_name: 'Jane Doe',
    });
    expect(fetchCopilotScheduleGridJobsForDate).toHaveBeenCalledTimes(2);
    expect(poolClient.query.mock.calls[3][0]).toContain('FROM copilot_schedule_live_jobs clj');
  });

  test('getCopilotLiveJobs falls back to mirrored rows only when live fetch fails', async () => {
    getCopilotToken.mockResolvedValue({ cookieHeader: 'copilot=abc' });
    fetchCopilotScheduleGridJobsForDate.mockRejectedValue(new Error('Copilot unavailable'));

    const poolClient = {
      query: jest.fn().mockResolvedValue({
        rows: [
          {
            job_key: 'copilot:2026-04-18:visit-101',
            service_date: '2026-04-18',
            source_surface: 'schedule_grid',
            source_event_id: 'visit-101',
            source_synced_at: '2026-04-18T13:30:00.000Z',
            source_deleted_at: null,
            source_customer_id: 'cust-9',
            customer_name: 'Jane Doe',
            job_title: 'Spring Cleanup',
            source_status: 'Started',
            visit_total: '120.50',
            source_crew_name: 'Crew A',
            source_employees_text: 'Tim, Rob',
            source_stop_order: 3,
            address_raw: '123 Main St, Lakewood OH',
            overlay_job_key: null,
            review_state: null,
            office_note: null,
            hold_from_dispatch: false,
            local_tags: [],
            address_override: null,
            customer_link_id: null,
            property_link_id: null,
            overlay_updated_at: null,
            overlay_updated_by_name: null,
            dispatch_plan_job_key: null,
            crew_override_name: null,
            route_order_override: null,
            route_locked: false,
            map_lat: null,
            map_lng: null,
            map_source: null,
            map_quality: null,
            print_group_key: null,
            print_note: null,
            dispatch_updated_at: null,
            dispatch_updated_by_name: null,
          },
          {
            job_key: 'copilot:2026-04-19:visit-202',
            service_date: '2026-04-19',
            source_surface: 'schedule_grid',
            source_event_id: 'visit-202',
            source_synced_at: '2026-04-19T08:05:00.000Z',
            source_deleted_at: null,
            source_customer_id: 'cust-22',
            customer_name: 'Second Customer',
            job_title: 'Mowing',
            source_status: 'Scheduled',
            visit_total: '55.00',
            source_crew_name: 'Crew B',
            source_employees_text: 'Chris',
            source_stop_order: 1,
            address_raw: '500 Example Ave',
            overlay_job_key: null,
            review_state: null,
            office_note: null,
            hold_from_dispatch: false,
            local_tags: [],
            address_override: null,
            customer_link_id: null,
            property_link_id: null,
            overlay_updated_at: null,
            overlay_updated_by_name: null,
            dispatch_plan_job_key: null,
            crew_override_name: null,
            route_order_override: null,
            route_locked: false,
            map_lat: null,
            map_lng: null,
            map_source: null,
            map_quality: null,
            print_group_key: null,
            print_note: null,
            dispatch_updated_at: null,
            dispatch_updated_by_name: null,
          },
        ],
      }),
    };

    const result = await getCopilotLiveJobs({
      poolClient,
      startDate: '2026-04-18',
      endDate: '2026-04-19',
      fetchImpl: jest.fn(),
    });

    expect(result).toMatchObject({
      start_date: '2026-04-18',
      end_date: '2026-04-19',
      freshness: {
        source: 'mirror',
        fetched_at: '2026-04-19T08:05:00.000Z',
        stale: true,
      },
      stats: {
        total: 2,
        byStatus: {
          in_progress: 1,
          pending: 1,
        },
        byCrew: {
          'Crew A': 1,
          'Crew B': 1,
        },
        totalRevenue: 175.5,
      },
    });
    expect(result.freshness.per_date).toEqual([
      { date: '2026-04-18', source: 'mirror', fetched_at: '2026-04-18T13:30:00.000Z', error: 'Copilot unavailable' },
      { date: '2026-04-19', source: 'mirror', fetched_at: '2026-04-19T08:05:00.000Z', error: 'Copilot unavailable' },
    ]);
    expect(result.days).toEqual([
      {
        day: '2026-04-18',
        total_jobs: 1,
        completed: 0,
        pending: 0,
        in_progress: 1,
        skipped: 0,
        cancelled: 0,
        revenue: 120.5,
        crews: { 'Crew A': 1 },
      },
      {
        day: '2026-04-19',
        total_jobs: 1,
        completed: 0,
        pending: 1,
        in_progress: 0,
        skipped: 0,
        cancelled: 0,
        revenue: 55,
        crews: { 'Crew B': 1 },
      },
    ]);
    expect(result.jobs).toHaveLength(2);
    expect(poolClient.query).toHaveBeenCalledTimes(1);
    expect(poolClient.query.mock.calls[0][0]).toContain('FROM copilot_schedule_live_jobs clj');
    expect(poolClient.query.mock.calls[0][1]).toEqual(['2026-04-18', '2026-04-19']);
  });

  test('getCopilotLiveJobs throws instead of silently returning empty mirror data when live fetch fails and no mirror rows exist', async () => {
    getCopilotToken.mockResolvedValue({ cookieHeader: 'copilot=abc' });
    fetchCopilotScheduleGridJobsForDate.mockRejectedValue(new Error('Copilot unavailable'));
    const poolClient = {
      query: jest.fn().mockResolvedValue({ rows: [] }),
    };

    await expect(getCopilotLiveJobs({
      poolClient,
      date: '2026-04-20',
      fetchImpl: jest.fn(),
    })).rejects.toThrow('Copilot unavailable');

    expect(poolClient.query).toHaveBeenCalledTimes(1);
  });
});
