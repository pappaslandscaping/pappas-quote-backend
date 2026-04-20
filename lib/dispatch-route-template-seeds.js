const {
  applyDispatchRouteTemplate,
  buildRouteTemplateAddressFingerprint,
  fetchResolvedLiveJobs,
  mapResolvedLiveJobToScheduleJob,
  mapScheduleJobToDispatchJob,
} = require('../services/copilot/live-jobs');

const MONDAY_TEMPLATE_ANCHOR_DATE = '2026-04-20';
const MONDAY_TEMPLATE_DAY_OF_WEEK = 1;

function buildSeedEntries(entries) {
  return entries.map(([customer_name, service_title, address], index) => ({
    position: index + 1,
    customer_name,
    service_title,
    address,
  }));
}

function deriveServiceFrequency(serviceTitle) {
  const value = String(serviceTitle || '').toLowerCase();
  if (value.includes('bi-weekly') || value.includes('biweekly')) return 'Bi-Weekly';
  if (value.includes('monthly')) return 'Monthly';
  if (value.includes('weekly')) return 'Weekly';
  return null;
}

const DISPATCH_ROUTE_TEMPLATE_SEEDS = [
  {
    seed_key: 'monday-rob-mowing-crew',
    name: 'Monday - Rob Mowing Crew',
    crew_name: 'Rob Mowing Crew',
    cadence: 'weekly',
    anchor_date: MONDAY_TEMPLATE_ANCHOR_DATE,
    day_of_week: MONDAY_TEMPLATE_DAY_OF_WEEK,
    notes: 'Seeded from Apr 20, 2026 Monday PDF route order.',
    entries: buildSeedEntries([
      ['Phyllis Wright', 'Mowing', '5344 West 150th Street Brook Park OH 44142, US'],
      ['Dennis Kosarek', 'Mowing', '12530 Milligan Avenue Cleveland OH 44135, US'],
      ['Longmead Village HOA', 'Mowing', '12608-12710 Longmead Ave Cleveland OH 44135, US'],
      ['Lareesa Rice', 'Mowing', '11917 Saint John Avenue Cleveland, OH 44111'],
      ['Lareesa Rice', 'Mowing', '10509 Jasper Avenue Cleveland, OH 44111'],
      ['June Scanlon', 'Mowing', '11207 Peony Avenue Cleveland OH 44111, US'],
      ['Mark McManamon', 'Mowing', '10907 Flower Avenue Cleveland, OH 44111'],
      ['Heidi Thuning', 'Mowing', '10208 Thrush Avenue Cleveland OH 44111, US'],
      ['Diane Seredy', 'Mowing', '10101 Loretta Avenue Cleveland OH 44111, US'],
      ['Judy Calvert', 'Mowing', '3334 West 95th Street Cleveland OH 44102, US'],
      ['Rose Gunther', 'Mowing', '3316 West 88th Street Cleveland OH 44102, US'],
      ['Kellie Moncheck', 'Mowing (Bi-Weekly)', '2064 West 98th Street Cleveland OH 44102, US'],
      ['James Trzop', 'Mowing', '10912 Parkhurst Drive Cleveland OH 44111, US'],
      ['Joan Sapara', 'Mowing', '11508 Linnet Avenue Cleveland OH 44111, US'],
      ['Herman Turrington', 'Mowing', '12318 Milan Avenue Cleveland OH 44111, US'],
      ['Mary Keane', 'Mowing', '3588 West 128th Street Cleveland OH 44111, US'],
      ['Omar Kain', 'Mowing (Bi-Weekly)', '3523 West 128th Street Cleveland OH 44111, US'],
      ['Angela Ziegler', 'Mowing', '3445 West 131st Street Cleveland OH 44111, US'],
      ['Brennan Investments LLC', 'Mowing', '13000 Triskett Road Cleveland OH 44111, US'],
      ['Matthew Ditlevson', 'Mowing (Bi-Weekly)', '3737 West 134th Street Cleveland OH 44111, US'],
      ['Carol Horner', 'Mowing', '3762 West 133rd Street Cleveland OH 44111, US'],
      ['Lillian Prijatel', 'Mowing', '14513 Fairlawn Avenue Cleveland OH 44111, US'],
      ['Robert Buttler', 'Mowing', '4013 West 144th Street Cleveland OH 44135, US'],
      ['Claudia Demchak', 'Mowing', '14117 Clifford Avenue Cleveland OH 44135, US'],
      ['Ronald Menzing', 'Mowing', '14103 Saint James Avenue Cleveland OH 44135, US'],
      ['Ronald Menzing', 'Mowing', '14023 Saint James Avenue Cleveland OH 44135, US'],
      ['Monta Demchak', 'Mowing', '14015 Saint James Avenue Cleveland OH 44135, US'],
      ['Jack Egger', 'Mowing', '13606 Harold Avenue Cleveland OH 44135, US'],
      ['Pat Kugli', 'Mowing', '13602 Harold Avenue Cleveland OH 44135, US'],
      ['Kathleen Foreman', 'Mowing', '13917 Belleshire Avenue Cleveland OH 44135, US'],
      ['Denise Clark', 'Mowing', '4485 West 138th Street Cleveland OH 44135, US'],
      ['Zena Violetti', 'Mowing', '4501 West 135th Street Cleveland OH 44135, US'],
      ['William Lado', 'Mowing', '4593 West 146th Street Cleveland OH 44135, US'],
      ['Kathryn Walker', 'Mowing', '4576 West 148th Street Cleveland, OH 44135'],
      ['William Messer', 'Mowing (Bi-Weekly)', '4484 West 148th Street Cleveland OH 44135, US'],
      ['Edward Bolte', 'Mowing', '4641 West 149th Street Cleveland OH 44135, US'],
      ['Kathleen Vasko', 'Mowing', '15402 Marlene Avenue Cleveland OH 44135, US'],
      ['Gail Burlee', 'Mowing', '4712 West 150th Street Cleveland OH 44135, US'],
    ]),
  },
  {
    seed_key: 'monday-tim-mowing-crew',
    name: 'Monday - Tim Mowing Crew',
    crew_name: 'Tim Mowing Crew',
    cadence: 'weekly',
    anchor_date: MONDAY_TEMPLATE_ANCHOR_DATE,
    day_of_week: MONDAY_TEMPLATE_DAY_OF_WEEK,
    notes: 'Seeded from Apr 20, 2026 Monday PDF route order.',
    entries: buildSeedEntries([
      ['Dan Wild', 'Mowing', '5764 Defiance Avenue Brook Park OH 44142, US'],
      ['Mary Shamray', 'Mowing', '14186 Parkman Boulevard Brook Park OH 44142, US'],
      ['Ruth Miller', 'Mowing', '14381 Fayette Boulevard Brook Park OH 44142, US'],
      ['Joanne Sibert', 'Mowing', '14319 Fayette Boulevard Brook Park OH 44142, US'],
      ['Judith Stocker', 'Mowing', '6180 Michael Drive Brook Park OH 44142, US'],
      ['Mark Carpenter', 'Mowing', '14050 Heatherwood Drive Brook Park OH 44142, US'],
      ['Carole Gondek', 'Mowing', '13409 Brookhaven Boulevard Brook Park OH 44142, US'],
      ['Paul Hauser', 'Mowing', '13376 Kathleen Drive Brook Park OH 44142, US'],
      ['Keith Kawecki', 'Mowing', '13395 Kathleen Drive Brook Park OH 44142, US'],
      ['Robert Schultz', 'Mowing', '6372 Terre Drive Brook Park OH 44142, US'],
      ['Jan Zubal', 'Weed Control (Monthly)', '6364 Terre Drive Brook Park OH 44142, US'],
      ['Jan Zubal', 'Mowing', '6364 Terre Drive Brook Park OH 44142, US'],
      ['Barbara Sekerak', 'Mowing', '6312 Elmdale Road Brook Park OH 44142, US'],
      ['Barbara Sekerak', 'Weed Control (Monthly)', '6312 Elmdale Road Brook Park OH 44142, US'],
      ['Loretta Kuhlman', 'Mowing', '6328 Elmdale Road Brook Park OH 44142, US'],
      ['John Blasee', 'Mowing', '14248 Sheldon Boulevard Brook Park OH 44142, US'],
      ['Michael Hill', 'Mowing', '6401 Michael Drive Brook Park OH 44142, US'],
      ['Dianne Daugherty', 'Mowing', '6391 Smith Road Brook Park OH 44142, US'],
      ['Linda Scamaldo', 'Mowing', '6346 Smith Road Brook Park OH 44142, US'],
      ['Kelly Robison', 'Mowing', '6421 Edgehurst Drive Brook Park OH 44142, US'],
      ['Kathleen Chriszt', 'Mowing', '6503 Sandfield Drive Brook Park OH 44142, US'],
      ['David Gannon', 'Mowing', '15806 Sheldon Road Brook Park OH 44142, US'],
      ['Melissa Gens', 'Weed Control (Monthly)', '6503 Ledgebrook Drive Brook Park OH 44142, US'],
      ['Leo Oblak', 'Mowing', '6476 Sanfield Drive Brook Park OH 44142, US'],
      ['Kathleen Clark', 'Mowing', '6399 Delores Boulevard Brook Park OH 44142, US'],
      ['Kathleen Clark', 'Weed Control (Monthly)', '6399 Delores Boulevard Brook Park OH 44142, US'],
      ['Helen Carroll', 'Mowing', '15720 Birchcroft Drive Brook Park OH 44142, US'],
      ['Vera Bartuccio', 'Mowing', '6172 Fry Road Brook Park OH 44142, US'],
      ['Roseann Reye', 'Mowing', '6122 Fry Road Brook Park OH 44142, US'],
      ['John Galehouse', 'Mowing', '6118 Fry Road Brook Park OH 44142, US'],
      ['Ramona Thomas', 'Mowing', '6063 Fry Road Brook Park OH 44142, US'],
      ['Linda Fowler', 'Mowing', '16204 Hocking Boulevard Brook Park OH 44142, US'],
      ['Paul Pasek', 'Mowing', '15667 Paulding Boulevard Brook Park OH 44142, US'],
      ['Paul Pasek', 'Weed Control (Monthly)', '15667 Paulding Boulevard Brook Park OH 44142, US'],
      ['Bob Maclean', 'Mowing', '15646 Hocking Boulevard Brook Park OH 44142, US'],
      ['Laura Stein', 'Mowing', '15842 Pike Boulevard Brook Park OH 44142, US'],
      ['Linda Butters', 'Mowing', '16092 Pike Boulevard Brook Park OH 44142, US'],
      ['Jessyca Yucas', 'Mowing', '6123 Eavenson Road Brook Park OH 44142, US'],
      ['Carol Uher', 'Mowing', '16800 Shelby Drive Brook Park OH 44142, US'],
      ['CC Pkwy Owner LLC', 'Litter Pickup Service (Weekly)', '19681 Commerce Parkway Middleburg Heights OH 44130, US'],
    ]),
  },
];

function createTemplateStopFromSeed(seedEntry, matchedJob = null) {
  return {
    position: seedEntry.position,
    source_customer_id: matchedJob?.copilot_customer_id || null,
    customer_link_id: matchedJob?.local_customer_id ?? matchedJob?.customer_id ?? null,
    property_link_id: matchedJob?.property_id ?? null,
    customer_name: seedEntry.customer_name,
    address_fingerprint: buildRouteTemplateAddressFingerprint(seedEntry.address),
    service_title: seedEntry.service_title,
    service_frequency: matchedJob?.service_frequency || deriveServiceFrequency(seedEntry.service_title),
    source_event_type: matchedJob?.copilot_event_type || null,
  };
}

function buildDispatchRouteSeedTemplateStops(seed, liveJobs = []) {
  const templateStops = seed.entries.map((entry) => ({
    position: entry.position,
    customer_name: entry.customer_name,
    address_fingerprint: buildRouteTemplateAddressFingerprint(entry.address),
    service_title: entry.service_title,
    service_frequency: deriveServiceFrequency(entry.service_title),
    source_event_type: null,
    property_link_id: null,
    customer_link_id: null,
    source_customer_id: null,
  }));

  const applyResult = applyDispatchRouteTemplate({
    template: { crew_name: seed.crew_name },
    templateStops,
    liveJobs,
  });
  const matchedByPosition = new Map(
    applyResult.matched.map((entry) => [Number(entry.stop.position), entry.job])
  );

  return {
    stops: seed.entries.map((entry) => createTemplateStopFromSeed(entry, matchedByPosition.get(entry.position) || null)),
    unmatched_template_stops: applyResult.unmatched_template_stops,
    ambiguous: applyResult.ambiguous,
  };
}

async function fetchAnchorLiveDispatchJobs(pool, targetDate) {
  const rows = await fetchResolvedLiveJobs(pool, { date: targetDate });
  return rows
    .map((row) => mapScheduleJobToDispatchJob(mapResolvedLiveJobToScheduleJob(row)))
    .filter((job) => !job?.hold_from_dispatch && !job?.source_deleted);
}

async function seedDispatchRouteTemplates(pool) {
  const liveJobsByDate = new Map();

  for (const seed of DISPATCH_ROUTE_TEMPLATE_SEEDS) {
    if (!liveJobsByDate.has(seed.anchor_date)) {
      liveJobsByDate.set(seed.anchor_date, await fetchAnchorLiveDispatchJobs(pool, seed.anchor_date));
    }
    const liveJobs = liveJobsByDate.get(seed.anchor_date) || [];
    const built = buildDispatchRouteSeedTemplateStops(seed, liveJobs);

    const templateResult = await pool.query(
      `INSERT INTO dispatch_route_templates (
         seed_key,
         name,
         crew_name,
         cadence,
         anchor_date,
         day_of_week,
         active,
         notes
       ) VALUES ($1, $2, $3, $4, $5::date, $6, true, $7)
       ON CONFLICT (seed_key) WHERE seed_key IS NOT NULL DO UPDATE
         SET name = EXCLUDED.name,
             crew_name = EXCLUDED.crew_name,
             cadence = EXCLUDED.cadence,
             anchor_date = EXCLUDED.anchor_date,
             day_of_week = EXCLUDED.day_of_week,
             active = EXCLUDED.active,
             notes = EXCLUDED.notes,
             updated_at = NOW()
       RETURNING id`,
      [
        seed.seed_key,
        seed.name,
        seed.crew_name,
        seed.cadence,
        seed.anchor_date,
        seed.day_of_week,
        seed.notes,
      ]
    );
    const templateId = templateResult.rows[0]?.id;
    if (!templateId) continue;

    await pool.query(`DELETE FROM dispatch_route_template_stops WHERE template_id = $1`, [templateId]);
    for (const stop of built.stops) {
      await pool.query(
        `INSERT INTO dispatch_route_template_stops (
           template_id,
           position,
           source_customer_id,
           customer_link_id,
           property_link_id,
           customer_name,
           address_fingerprint,
           service_title,
           service_frequency,
           source_event_type
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [
          templateId,
          stop.position,
          stop.source_customer_id,
          stop.customer_link_id,
          stop.property_link_id,
          stop.customer_name,
          stop.address_fingerprint,
          stop.service_title,
          stop.service_frequency,
          stop.source_event_type,
        ]
      );
    }

    if (built.unmatched_template_stops.length || built.ambiguous.length) {
      console.warn(
        `⚠️ Dispatch route template seed "${seed.name}" had ${built.unmatched_template_stops.length} unmatched and ${built.ambiguous.length} ambiguous stop(s) during anchor enrichment`
      );
    }
  }
}

module.exports = {
  DISPATCH_ROUTE_TEMPLATE_SEEDS,
  MONDAY_TEMPLATE_ANCHOR_DATE,
  buildDispatchRouteSeedTemplateStops,
  deriveServiceFrequency,
  seedDispatchRouteTemplates,
};
