const {
  DISPATCH_ROUTE_TEMPLATE_SEEDS,
  MONDAY_TEMPLATE_ANCHOR_DATE,
  buildDispatchRouteSeedTemplateStops,
  deriveServiceFrequency,
} = require('../lib/dispatch-route-template-seeds');

describe('dispatch route template seeds', () => {
  test('includes weekly Monday templates for Rob and Tim', () => {
    expect(DISPATCH_ROUTE_TEMPLATE_SEEDS.map((seed) => seed.seed_key)).toEqual([
      'monday-rob-mowing-crew',
      'monday-tim-mowing-crew',
    ]);
    expect(DISPATCH_ROUTE_TEMPLATE_SEEDS.every((seed) => seed.cadence === 'weekly')).toBe(true);
    expect(DISPATCH_ROUTE_TEMPLATE_SEEDS.every((seed) => seed.anchor_date === MONDAY_TEMPLATE_ANCHOR_DATE)).toBe(true);
    expect(DISPATCH_ROUTE_TEMPLATE_SEEDS.every((seed) => seed.day_of_week === 1)).toBe(true);
  });

  test('buildDispatchRouteSeedTemplateStops keeps PDF order and enriches durable ids from live jobs', () => {
    const seed = {
      crew_name: 'Tim Mowing Crew',
      entries: [
        {
          position: 1,
          customer_name: 'Dan Wild',
          service_title: 'Mowing',
          address: '5764 Defiance Avenue Brook Park OH 44142, US',
        },
        {
          position: 2,
          customer_name: 'Mary Shamray',
          service_title: 'Mowing',
          address: '14186 Parkman Boulevard Brook Park OH 44142, US',
        },
      ],
    };
    const liveJobs = [
      {
        id: 'copilot:2026-04-20:1',
        copilot_customer_id: 'c-1',
        customer_id: 101,
        local_customer_id: 101,
        property_id: 201,
        customer_name: 'Dan Wild',
        address: '5764 Defiance Avenue, Brook Park, OH 44142',
        service_title: 'Mowing',
        service_type: 'Mowing',
        service_frequency: 'Weekly',
        copilot_event_type: 'Visit',
        crew_assigned: 'Tim Mowing Crew',
        route_order: 1,
      },
      {
        id: 'copilot:2026-04-20:2',
        copilot_customer_id: 'c-2',
        customer_id: 102,
        local_customer_id: 102,
        property_id: 202,
        customer_name: 'Mary Shamray',
        address: '14186 Parkman Boulevard Brook Park OH 44142, US',
        service_title: 'Mowing',
        service_type: 'Mowing',
        service_frequency: 'Weekly',
        copilot_event_type: 'Visit',
        crew_assigned: 'Tim Mowing Crew',
        route_order: 2,
      },
    ];

    const result = buildDispatchRouteSeedTemplateStops(seed, liveJobs);

    expect(result.unmatched_template_stops).toEqual([]);
    expect(result.ambiguous).toEqual([]);
    expect(result.stops.map((stop) => stop.position)).toEqual([1, 2]);
    expect(result.stops[0]).toMatchObject({
      position: 1,
      source_customer_id: 'c-1',
      customer_link_id: 101,
      property_link_id: 201,
      customer_name: 'Dan Wild',
      service_title: 'Mowing',
      service_frequency: 'Weekly',
      source_event_type: 'Visit',
    });
    expect(result.stops[1]).toMatchObject({
      position: 2,
      source_customer_id: 'c-2',
      customer_link_id: 102,
      property_link_id: 202,
      customer_name: 'Mary Shamray',
      service_title: 'Mowing',
      service_frequency: 'Weekly',
      source_event_type: 'Visit',
    });
  });

  test('unmatched seeded stops fall back to PDF matching hints without guessing ids', () => {
    const seed = {
      crew_name: 'Rob Mowing Crew',
      entries: [
        {
          position: 1,
          customer_name: 'Kellie Moncheck',
          service_title: 'Mowing (Bi-Weekly)',
          address: '2064 West 98th Street Cleveland OH 44102, US',
        },
      ],
    };

    const result = buildDispatchRouteSeedTemplateStops(seed, []);

    expect(result.unmatched_template_stops).toEqual([
      {
        position: 1,
        customer_name: 'Kellie Moncheck',
        service_title: 'Mowing (Bi-Weekly)',
        reason: 'missing',
      },
    ]);
    expect(result.stops[0]).toMatchObject({
      position: 1,
      source_customer_id: null,
      customer_link_id: null,
      property_link_id: null,
      customer_name: 'Kellie Moncheck',
      service_title: 'Mowing (Bi-Weekly)',
      service_frequency: 'Bi-Weekly',
      source_event_type: null,
    });
  });

  test('deriveServiceFrequency infers weekly and bi-weekly cadence from PDF service labels', () => {
    expect(deriveServiceFrequency('Mowing')).toBe(null);
    expect(deriveServiceFrequency('Litter Pickup Service (Weekly)')).toBe('Weekly');
    expect(deriveServiceFrequency('Mowing (Bi-Weekly)')).toBe('Bi-Weekly');
    expect(deriveServiceFrequency('Weed Control (Monthly)')).toBe('Monthly');
  });
});
