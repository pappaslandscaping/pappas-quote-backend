const {
  applyDispatchRouteTemplate,
  buildDispatchRouteTemplateStop,
  buildDispatchBoardPayload,
  buildCopilotJobKey,
  fetchLiveCopilotScheduleDate,
  getCopilotLiveJobs,
  isDispatchRouteTemplateApplicable,
  mapScheduleJobToDispatchJob,
  mapResolvedLiveJobToScheduleJob,
  normalizeDispatchPlanPatch,
  normalizeResolvedRow,
  parseVisitTotal,
  upsertCopilotLiveJobs,
} = require('../services/copilot/live-jobs');
const {
  fetchCopilotScheduleGridJobsForDate,
  parseCopilotScheduleGridDayHtml,
} = require('../services/copilot/client');

describe('copilot live jobs service', () => {
  const scheduleGridHtml = `
    <table class="copilot-table table--with-hide-options">
      <thead>
        <tr>
          <th></th>
          <th>Date</th>
          <th>Title</th>
          <th>Crew / Employees</th>
          <th>Name</th>
          <th>Property</th>
          <th>Address</th>
          <th>Type</th>
          <th>Invoiceable</th>
          <th>Frequency</th>
          <th>Last Serviced</th>
          <th>Status</th>
          <th>Visit Notes</th>
          <th>Tracked Time</th>
          <th>BH</th>
          <th>Visit Total</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr data-row-event-id="501" data-row-job-id="job-501">
          <td><input type="checkbox" value="501"></td>
          <td>Apr 17, 2026</td>
          <td>Spring Cleanup</td>
          <td><span>Jobs Crew</span><small>Tim, Rob</small></td>
          <td><a href="/customers/view/9001">Jane Smith</a></td>
          <td>123 Main St</td>
          <td>123 Main St, Lakewood, OH</td>
          <td>Visit</td>
          <td>Invoiced</td>
          <td>Weekly</td>
          <td>Apr 10, 2026</td>
          <td><span class="status-label green">Closed</span><br><a href="/finances/invoices/view/2736729">#10471</a></td>
          <td>Gate code</td>
          <td>01:05</td>
          <td>1.0</td>
          <td>$600.00</td>
          <td></td>
        </tr>
        <tr data-row-event-id="502" data-row-job-id="job-502">
          <td><input type="checkbox" value="502"></td>
          <td>Apr 17, 2026</td>
          <td>Mulch Refresh</td>
          <td><span>Mulch Crew</span><small>Ash, Eli</small></td>
          <td><a href="/customers/view/9002">John Doe</a></td>
          <td>45 Elm St</td>
          <td>45 Elm St, Rocky River, OH</td>
          <td>Visit</td>
          <td>Ready</td>
          <td>One-time</td>
          <td></td>
          <td><span class="status-label red">Open</span></td>
          <td></td>
          <td>00:00</td>
          <td>2.5</td>
          <td>$250.00</td>
          <td></td>
        </tr>
      </tbody>
    </table>
  `;
  const singleRowScheduleGridHtml = `
    <table class="copilot-table table--with-hide-options">
      <thead>
        <tr>
          <th></th>
          <th>Date</th>
          <th>Title</th>
          <th>Crew / Employees</th>
          <th>Name</th>
          <th>Property</th>
          <th>Address</th>
          <th>Type</th>
          <th>Invoiceable</th>
          <th>Frequency</th>
          <th>Last Serviced</th>
          <th>Status</th>
          <th>Visit Notes</th>
          <th>Tracked Time</th>
          <th>BH</th>
          <th>Visit Total</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr data-row-event-id="501" data-row-job-id="job-501">
          <td><input type="checkbox" value="501"></td>
          <td>Apr 17, 2026</td>
          <td>Spring Cleanup</td>
          <td><span>Jobs Crew</span><small>Tim, Rob</small></td>
          <td><a href="/customers/view/9001">Jane Smith</a></td>
          <td>123 Main St</td>
          <td>123 Main St, Lakewood, OH</td>
          <td>Visit</td>
          <td>Invoiced</td>
          <td>Weekly</td>
          <td>Apr 10, 2026</td>
          <td><span class="status-label green">Closed</span><br><a href="/finances/invoices/view/2736729">#10471</a></td>
          <td>Gate code</td>
          <td>01:05</td>
          <td>1.0</td>
          <td>$600.00</td>
          <td></td>
        </tr>
      </tbody>
    </table>
  `;

  test('buildCopilotJobKey composes a stable live-job key', () => {
    expect(buildCopilotJobKey('2026-04-18', '12345')).toBe('copilot:2026-04-18:12345');
  });

  test('parseVisitTotal parses Copilot money strings', () => {
    expect(parseVisitTotal('$145.50')).toBe(145.5);
    expect(parseVisitTotal('')).toBeNull();
    expect(parseVisitTotal(null)).toBeNull();
  });

  test('normalizeResolvedRow returns the ResolvedLiveJob contract', () => {
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

    expect(resolved).toMatchObject({
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

  test('dispatch route templates honor weekly and biweekly cadence', () => {
    expect(isDispatchRouteTemplateApplicable({
      cadence: 'weekly',
      anchor_date: '2026-04-20',
      day_of_week: 1,
    }, '2026-04-27')).toBe(true);
    expect(isDispatchRouteTemplateApplicable({
      cadence: 'biweekly',
      anchor_date: '2026-04-20',
      day_of_week: 1,
    }, '2026-05-04')).toBe(true);
    expect(isDispatchRouteTemplateApplicable({
      cadence: 'biweekly',
      anchor_date: '2026-04-20',
      day_of_week: 1,
    }, '2026-04-27')).toBe(false);
  });

  test('buildDispatchRouteTemplateStop saves only route-matching hints', () => {
    expect(buildDispatchRouteTemplateStop({
      customer_name: 'Jane Smith',
      copilot_customer_id: '9001',
      local_customer_id: 21,
      property_id: 44,
      address: '123 Main St, Lakewood OH 44107, US',
      service_title: 'Spring Cleanup',
      service_frequency: 'Weekly',
      copilot_event_type: 'Visit',
    }, 3)).toEqual({
      position: 3,
      source_customer_id: '9001',
      customer_link_id: 21,
      property_link_id: 44,
      customer_name: 'Jane Smith',
      address_fingerprint: '123 main st lakewood',
      service_title: 'Spring Cleanup',
      service_frequency: 'Weekly',
      source_event_type: 'Visit',
    });
  });

  test('buildDispatchRouteTemplateStop keeps the house number in address fingerprints', () => {
    expect(buildDispatchRouteTemplateStop({
      customer_name: 'Ronald Menzing',
      address: '14103 Saint James Avenue Cleveland OH 44135, US',
      service_title: 'Mowing',
    }, 25).address_fingerprint).toBe('14103 saint james avenue cleveland');
  });

  test('applyDispatchRouteTemplate matches saved stops and appends remaining crew jobs', () => {
    const result = applyDispatchRouteTemplate({
      template: {
        crew_name: 'Crew A',
      },
      templateStops: [
        {
          position: 1,
          source_customer_id: '9002',
          customer_link_id: null,
          property_link_id: null,
          customer_name: 'John Doe',
          address_fingerprint: '45 elm st rocky river',
          service_title: 'Mulch Refresh',
          service_frequency: 'One-time',
          source_event_type: 'Visit',
        },
      ],
      liveJobs: [
        {
          id: 'copilot:2026-04-20:job-1',
          crew_assigned: 'Crew A',
          route_order: 1,
          customer_name: 'Jane Smith',
          address: '123 Main St, Lakewood OH',
          service_title: 'Spring Cleanup',
          service_frequency: 'Weekly',
          copilot_customer_id: '9001',
        },
        {
          id: 'copilot:2026-04-20:job-2',
          crew_assigned: null,
          route_order: null,
          customer_name: 'John Doe',
          address: '45 Elm St, Rocky River OH',
          service_title: 'Mulch Refresh',
          service_frequency: 'One-time',
          copilot_customer_id: '9002',
        },
      ],
    });

    expect(result.patches).toEqual([
      {
        jobKey: 'copilot:2026-04-20:job-2',
        patch: {
          crew_override_name: 'Crew A',
          route_order_override: 1,
        },
      },
      {
        jobKey: 'copilot:2026-04-20:job-1',
        patch: {
          crew_override_name: 'Crew A',
          route_order_override: 2,
        },
      },
    ]);
    expect(result.unmatched_template_stops).toEqual([]);
    expect(result.ambiguous).toEqual([]);
  });

  test('applyDispatchRouteTemplate uses exact address to break ties for duplicate customer stops', () => {
    const result = applyDispatchRouteTemplate({
      template: {
        crew_name: 'Rob Mowing Crew',
      },
      templateStops: [
        {
          position: 25,
          source_customer_id: '9010',
          customer_link_id: null,
          property_link_id: null,
          customer_name: 'Ronald Menzing',
          address_fingerprint: '14103 saint james avenue cleveland',
          service_title: 'Mowing',
          service_frequency: null,
          source_event_type: null,
        },
        {
          position: 26,
          source_customer_id: '9010',
          customer_link_id: null,
          property_link_id: null,
          customer_name: 'Ronald Menzing',
          address_fingerprint: '14023 saint james avenue cleveland',
          service_title: 'Mowing',
          service_frequency: null,
          source_event_type: null,
        },
      ],
      liveJobs: [
        {
          id: 'copilot:2026-04-20:45001834',
          crew_assigned: 'Rob Mowing Crew',
          route_order: 37,
          customer_name: 'Ronald Menzing',
          address: '14103 Saint James Avenue Cleveland OH 44135, US',
          service_title: 'Mowing',
          service_frequency: null,
          copilot_customer_id: '9010',
        },
        {
          id: 'copilot:2026-04-20:45001940',
          crew_assigned: 'Rob Mowing Crew',
          route_order: 38,
          customer_name: 'Ronald Menzing',
          address: '14023 Saint James Avenue Cleveland OH 44135, US',
          service_title: 'Mowing',
          service_frequency: null,
          copilot_customer_id: '9010',
        },
        {
          id: 'copilot:2026-04-20:45002046',
          crew_assigned: 'Rob Mowing Crew',
          route_order: 25,
          customer_name: 'Monta Demchak',
          address: '14015 Saint James Avenue Cleveland OH 44135, US',
          service_title: 'Mowing',
          service_frequency: null,
          copilot_customer_id: '9020',
        },
      ],
    });

    expect(result.ambiguous).toEqual([]);
    expect(result.unmatched_template_stops).toEqual([]);
    expect(result.patches).toEqual([
      {
        jobKey: 'copilot:2026-04-20:45001834',
        patch: {
          crew_override_name: 'Rob Mowing Crew',
          route_order_override: 1,
        },
      },
      {
        jobKey: 'copilot:2026-04-20:45001940',
        patch: {
          crew_override_name: 'Rob Mowing Crew',
          route_order_override: 2,
        },
      },
      {
        jobKey: 'copilot:2026-04-20:45002046',
        patch: {
          crew_override_name: 'Rob Mowing Crew',
          route_order_override: 3,
        },
      },
    ]);
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

  test('parseCopilotScheduleGridDayHtml returns diagnostics and jobs from browser grid/day HTML', () => {
    const result = parseCopilotScheduleGridDayHtml(scheduleGridHtml);

    expect(result.diagnostics.html_source_length).toBeGreaterThan(0);
    expect(result.diagnostics.contains_expected_grid_table_markers).toBe(true);
    expect(result.diagnostics.parsed_row_count_before_filtering).toBe(2);
    expect(result.diagnostics.parsed_row_count_after_filtering).toBe(2);
    expect(result.jobs).toEqual([
      expect.objectContaining({
        job_id: 'job-501',
        event_id: '501',
        customer_id: '9001',
        customer_name: 'Jane Smith',
        crew_name: 'Jobs Crew',
        employees: 'Tim, Rob',
        address: '123 Main St, Lakewood, OH',
        status: 'Closed',
        visit_total: '$600.00',
        job_title: 'Spring Cleanup',
        raw_data: expect.objectContaining({
          service_date_label: 'Apr 17, 2026',
          property_name: '123 Main St',
          event_type: 'Visit',
          invoiceable: 'Invoiced',
          frequency: 'Weekly',
          last_serviced: 'Apr 10, 2026',
          notes: 'Gate code',
          tracked_time: '01:05',
          budgeted_hours: '1.0',
        }),
      }),
      expect.objectContaining({
        job_id: 'job-502',
        event_id: '502',
        customer_id: '9002',
        customer_name: 'John Doe',
        crew_name: 'Mulch Crew',
        employees: 'Ash, Eli',
      }),
    ]);
  });

  test('fetchCopilotScheduleGridJobsForDate uses the browser-visible grid/day surface', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      text: async () => scheduleGridHtml,
    });

    const result = await fetchCopilotScheduleGridJobsForDate({
      cookieHeader: 'copilotApiAccessToken=test',
      syncDate: '2026-04-17',
      fetchImpl,
    });

    expect(fetchImpl).toHaveBeenCalledWith(
      'https://secure.copilotcrm.com/scheduler/grid/day/?d=2026-04-17',
      expect.objectContaining({
        headers: expect.objectContaining({
          Cookie: 'copilotApiAccessToken=test',
        }),
      })
    );
    expect(result.jobs).toHaveLength(2);
    expect(result.diagnostics.parsed_row_count_before_filtering).toBe(2);
    expect(result.diagnostics.parsed_row_count_after_filtering).toBe(2);
  });

  test('mapResolvedLiveJobToScheduleJob flattens the live mirror into the Schedule contract', () => {
    const mapped = mapResolvedLiveJobToScheduleJob(normalizeResolvedRow({
      job_key: 'copilot:2026-04-18:visit-101',
      service_date: '2026-04-18',
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
      raw_payload: {
        raw_data: {
          property_name: '123 Main St',
          event_type: 'Visit',
          invoiceable: 'Invoiced',
          frequency: 'Weekly',
          last_serviced: 'Apr 10, 2026',
          tracked_time: '01:05',
          budgeted_hours: '1.0',
        },
      },
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
      service_frequency: 'Weekly',
      property_name: '123 Main St',
      copilot_event_type: 'Visit',
      copilot_invoiceable_status: 'Invoiced',
      service_price: 120.5,
      crew_assigned: 'Crew A',
      crew_name: 'Crew A',
      crew_members_text: 'Tim, Rob',
      status: 'in_progress',
      status_raw: 'Started',
      last_serviced: 'Apr 10, 2026',
      route_order: 2,
      stop_order: 2,
      tracked_time: '01:05',
      budgeted_hours: '1.0',
      special_notes: 'Gate code on file',
      lat: 41.4767,
      lng: -81.8123,
      geocode_quality: 'street',
      has_street_address: true,
      geocode_address: '123 Main St, Lakewood OH',
    });
  });

  test('mapScheduleJobToDispatchJob preserves live dispatch fields without creating local truth', () => {
    const mapped = mapScheduleJobToDispatchJob({
      id: 'copilot:2026-04-18:visit-101',
      source_system: 'copilot',
      job_date: '2026-04-18',
      service_date: '2026-04-18',
      visit_id: 'visit-101',
      customer_name: 'Jane Doe',
      address: '123 Main St, Lakewood OH',
      service_type: 'Spring Cleanup',
      service_frequency: 'Weekly',
      service_price: 120.5,
      crew_assigned: 'Crew A',
      crew_members_text: 'Tim, Rob',
      status: 'in_progress',
      status_raw: 'Started',
      route_order: 2,
      estimated_duration: 45,
      special_notes: 'Gate code on file',
      lat: 41.4767,
      lng: -81.8123,
      geocode_quality: 'street',
      hold_from_dispatch: false,
    });

    expect(mapped).toMatchObject({
      id: 'copilot:2026-04-18:visit-101',
      source_kind: 'live_dispatch',
      is_read_only: false,
      can_assign: true,
      can_geocode: true,
      job_date: '2026-04-18',
      visit_id: 'visit-101',
      customer_name: 'Jane Doe',
      address: '123 Main St, Lakewood OH',
      service_type: 'Spring Cleanup',
      service_frequency: 'Weekly',
      service_price: 120.5,
      crew_assigned: 'Crew A',
      status: 'in_progress',
      route_order: 2,
      estimated_duration: 45,
      lat: 41.4767,
      lng: -81.8123,
      geocode_quality: 'street',
    });
  });

  test('normalizeDispatchPlanPatch clears overrides that match the live source', () => {
    const normalized = normalizeDispatchPlanPatch({
      source: { crew_name: 'Crew A', stop_order: 2 },
      dispatch_plan: { map_lat: null, map_lng: null },
    }, {
      crew_override_name: 'Crew A',
      route_order_override: 2,
      map_lat: 41.45,
      map_lng: -81.78,
      map_source: 'manual_override',
      map_quality: 'street',
    });

    expect(normalized).toEqual({
      crew_override_name: null,
      route_order_override: null,
      map_lat: 41.45,
      map_lng: -81.78,
      map_source: 'manual_override',
      map_quality: 'street',
    });
  });

  test('buildDispatchBoardPayload groups live jobs by crew and keeps held jobs out of Dispatch', () => {
    const payload = buildDispatchBoardPayload({
      targetDate: '2026-04-18',
      view: 'day',
      freshness: { source: 'live', stale: false },
      crews: [{ id: 1, name: 'Crew A', members: 'Tim, Rob', color: '#059669' }],
      jobs: [
        {
          id: 'copilot:2026-04-18:visit-101',
          customer_name: 'Jane Doe',
          address: '123 Main St',
          service_type: 'Spring Cleanup',
          service_price: 120.5,
          crew_assigned: 'Crew A',
          route_order: 2,
          estimated_duration: 45,
          status: 'completed',
          lat: 41.4767,
          lng: -81.8123,
          geocode_quality: 'street',
        },
        {
          id: 'copilot:2026-04-18:visit-102',
          customer_name: 'John Roe',
          address: '45 Elm St',
          service_type: 'Mulch',
          service_price: 250,
          crew_assigned: null,
          estimated_duration: 30,
          status: 'pending',
        },
        {
          id: 'copilot:2026-04-18:visit-103',
          customer_name: 'Held Job',
          address: '99 Oak St',
          service_type: 'Mowing',
          service_price: 75,
          crew_assigned: 'Crew A',
          hold_from_dispatch: true,
          estimated_duration: 30,
          status: 'pending',
        },
      ],
    });

    expect(payload).toMatchObject({
      success: true,
      source_kind: 'live_dispatch',
      read_only: false,
      write_capabilities: {
        assign: true,
        route_order: true,
        geocode: true,
        optimize: true,
        reverse_route: true,
        complete: false,
        add_job: false,
      },
      freshness: { source: 'live', stale: false },
    });
    expect(payload.crews).toHaveLength(1);
    expect(payload.crews[0]).toMatchObject({
      name: 'Crew A',
      jobCount: 1,
      totalHours: 0.75,
    });
    expect(payload.crews[0].jobs).toHaveLength(1);
    expect(payload.unassigned).toHaveLength(1);
    expect(payload.unassigned[0].id).toBe('copilot:2026-04-18:visit-102');
  });

  test('fetchLiveCopilotScheduleDate fetches and persists a selected date from schedule grid/day', async () => {
    const poolClient = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
        .mockResolvedValueOnce({ rowCount: 0 }),
    };
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      text: async () => singleRowScheduleGridHtml,
    });

    const result = await fetchLiveCopilotScheduleDate({
      poolClient,
      syncDate: '2026-04-17',
      cookieHeader: 'copilot=abc',
      fetchImpl,
    });

    expect(result).toMatchObject({
      date: '2026-04-17',
      source: 'live',
      source_surface: 'schedule_grid',
      error: null,
      diagnostics: expect.objectContaining({
        parsed_row_count_before_filtering: 1,
        parsed_row_count_after_filtering: 1,
      }),
    });
    expect(result.fetched_at).toBeTruthy();
    expect(poolClient.query).toHaveBeenCalledTimes(2);
    expect(poolClient.query.mock.calls[0][1][0]).toBe('copilot:2026-04-17:501');
  });

  test('getCopilotLiveJobs returns live schedule payload with diagnostics when grid/day fetch succeeds', async () => {
    const poolClient = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [{ key: 'copilot_cookies', value: 'copilot=abc' }] })
        .mockResolvedValueOnce({ rows: [{ is_insert: true }] })
        .mockResolvedValueOnce({ rowCount: 0 })
        .mockResolvedValueOnce({
          rows: [{
            job_key: 'copilot:2026-04-17:501',
            service_date: '2026-04-17',
            source_event_id: '501',
            source_synced_at: '2026-04-17T13:30:00.000Z',
            source_deleted_at: null,
            source_customer_id: '9001',
            customer_name: 'Jane Smith',
            job_title: 'Spring Cleanup',
            source_status: 'Closed',
            visit_total: '600.00',
            source_crew_name: 'Jobs Crew',
            source_employees_text: 'Tim, Rob',
            source_stop_order: null,
            address_raw: '123 Main St, Lakewood, OH',
            raw_payload: {
              raw_data: {
                property_name: '123 Main St',
                event_type: 'Visit',
                invoiceable: 'Invoiced',
                frequency: 'Weekly',
                last_serviced: 'Apr 10, 2026',
                tracked_time: '01:05',
                budgeted_hours: '1.0',
              },
            },
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
            map_source: 'none',
            map_quality: 'missing',
            print_group_key: null,
            print_note: null,
            dispatch_updated_at: null,
            dispatch_updated_by_name: null,
          }],
        }),
    };
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      text: async () => singleRowScheduleGridHtml,
    });

    const result = await getCopilotLiveJobs({
      poolClient,
      date: '2026-04-17',
      fetchImpl,
    });

    expect(result.jobs).toHaveLength(1);
    expect(result.jobs[0]).toMatchObject({
      customer_name: 'Jane Smith',
      service_type: 'Spring Cleanup',
      service_title: 'Spring Cleanup',
      crew_name: 'Jobs Crew',
      crew_members_text: 'Tim, Rob',
      status: 'completed',
      status_raw: 'Closed',
      service_price: 600,
      service_frequency: 'Weekly',
      property_name: '123 Main St',
      copilot_event_type: 'Visit',
      copilot_invoiceable_status: 'Invoiced',
    });
    expect(result.freshness.per_date).toEqual([
      expect.objectContaining({
        date: '2026-04-17',
        source: 'live',
        source_surface: 'schedule_grid',
        diagnostics: expect.objectContaining({
          parsed_row_count_before_filtering: 1,
          parsed_row_count_after_filtering: 1,
        }),
      }),
    ]);
  });
});
