'use strict';
const { call, id, SOURCE } = require('./records');

const eventIdFromKey = (key) => {
  const match = String(key || '').match(/^(?:copilot|homeworks):(\d{4}-\d{2}-\d{2}:)?(\d+)$/);
  if (!match) throw new Error('Select a live Homeworks visit.');
  return id(match[2]);
};
async function routeState(context, date, crewName) {
  const data = await call(
    context,
    'YardDeskRouteState',
    `query YardDeskRouteState($date: Date!) {
    crews { id name }
    events(where: {startDate: {equals: $date},isDeleted:false},take:5000) {id crewId status startDate type sortOrder}
    routes(where: {date: {equals: $date}},take:1000) {id crewId date pinnedFirstEventId routeStops {eventId stopOrder}}
  }`,
    { date },
  );
  if (!data.events || data.events.length >= 5000 || !data.routes || data.routes.length >= 1000)
    throw new Error('Homeworks route state is incomplete.');
  const crews = (data.crews || []).filter((crew) => crew.name === crewName);
  if (crews.length !== 1) throw new Error('Choose one verified Homeworks crew.');
  const crew = crews[0],
    routes = data.routes.filter((route) => route.crewId === crew.id);
  if (routes.length > 1)
    throw new Error(
      'This crew has more than one Homeworks route. Review the route in Homeworks before saving.',
    );
  return { crew, route: routes[0] || null, events: data.events };
}

async function saveRouteOrder(context, { date, crewName, orderedKeys, optimize = false }) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) throw new Error('A valid route date is required.');
  const ids = orderedKeys.map(eventIdFromKey);
  if (!ids.length || new Set(ids).size !== ids.length)
    throw new Error('Each route visit must appear exactly once.');
  const state = await routeState(context, date, crewName);
  const live = state.events.filter(
    (event) => event.crewId === state.crew.id && !['CANCELLED', 'WAITLISTED'].includes(event.status),
  );
  if (live.length !== ids.length || live.some((event) => !ids.includes(event.id)))
    throw new Error('The live crew visit list changed. Refresh the route before saving.');
  const input = { eventIds: ids, crewId: state.crew.id, pinnedFirstEventId: ids[0] };
  let result;
  if (state.route) {
    result = await call(
      context,
      'YardDeskSaveRoute',
      `mutation YardDeskSaveRoute($id: SafeInt!, $input: UpdateRouteInput!, $optimize: Boolean) {
      updateRoute(routeId: $id,input: $input,optimize: $optimize) {id}
    }`,
      { id: state.route.id, input, optimize },
    );
  } else {
    result = await call(
      context,
      'YardDeskCreateRoute',
      `mutation YardDeskCreateRoute($input: CreateRouteInput!, $optimize: Boolean) {
      createRoute(input: $input,optimize: $optimize) {id}
    }`,
      { input: { date, ...input }, optimize },
    );
  }
  const routeId = result.updateRoute?.id || result.createRoute?.id;
  if (!routeId) throw new Error('Homeworks did not confirm the route save. Refresh before retrying.');
  const check = await call(
    context,
    'YardDeskVerifyRoute',
    `query YardDeskVerifyRoute($id: SafeInt!) {
    route(routeId: $id) {id date crewId totalMiles totalDriveSeconds routeStops {eventId stopOrder}}
  }`,
    { id: routeId },
  );
  const actual = [...(check.route?.routeStops || [])]
    .sort((a, b) => a.stopOrder - b.stopOrder)
    .map((stop) => stop.eventId);
  const sameSet = actual.length === ids.length && actual.every((value) => ids.includes(value));
  if (
    check.route?.date !== date ||
    check.route?.crewId !== state.crew.id ||
    !sameSet ||
    (!optimize && actual.some((value, index) => value !== ids[index]))
  ) {
    throw new Error('The saved Homeworks route could not be verified. Refresh before retrying.');
  }
  return {
    success: true,
    source: SOURCE,
    routeId,
    eventIds: actual,
    totalMiles: check.route.totalMiles,
    totalDriveSeconds: check.route.totalDriveSeconds,
    verified: true,
  };
}

async function repeatRoute(context, { date, crewName, frequency }) {
  const state = await routeState(context, date, crewName);
  if (!state.route) throw new Error('Save a Homeworks route before applying it to future visits.');
  const ids = state.route.routeStops
    .slice()
    .sort((a, b) => a.stopOrder - b.stopOrder)
    .map((stop) => stop.eventId);
  const result = await call(
    context,
    'YardDeskRepeatRoute',
    `mutation YardDeskRepeatRoute($input:UpdateRouteInput!,$id:SafeInt!,$weeks:Int!){
    updateRoute(routeId:$id,input:$input,repeatEveryWeeks:$weeks){id seriesKey}
  }`,
    { id: state.route.id, input: { eventIds: ids }, weeks: frequency === 'biweekly' ? 2 : 1 },
  );
  if (!result.updateRoute?.seriesKey)
    throw new Error('Homeworks did not confirm the repeated route series. Refresh before retrying.');
  return {
    success: true,
    source: SOURCE,
    routeId: state.route.id,
    seriesKey: result.updateRoute.seriesKey,
    message: 'Homeworks saved the repeated route for matching recurring visits.',
  };
}

async function saveAssignments(context, assignments) {
  if (!Array.isArray(assignments) || !assignments.length) throw new Error('Select live visits to assign.');
  const ids = assignments.map((row) => eventIdFromKey(row.job_key || row.jobId || row.job_id || row.id));
  if (new Set(ids).size !== ids.length) throw new Error('A visit appears more than once.');
  if (
    assignments.some(
      (row) => row.status !== undefined || row.job_date !== undefined || row.route_order !== undefined,
    )
  ) {
    throw new Error('Change crews here. Use route ordering or the visit schedule for other changes.');
  }
  const data = await call(
    context,
    'YardDeskAssignmentPreflight',
    `query YardDeskAssignmentPreflight($ids:[SafeInt!]!) {
    crews {id name} events(where:{id:{in:$ids},isDeleted:false},take:1000){id startDate status crewId}
  }`,
    { ids },
  );
  const planned = assignments.map((row, index) => {
    const event = data.events?.find((event) => event.id === ids[index]);
    if (!event || ['CANCELLED', 'CLOSED', 'SKIPPED'].includes(event.status))
      throw new Error('A selected visit is unavailable for crew assignment. Refresh first.');
    const crews = (data.crews || []).filter((crew) => crew.name === row.crew_assigned);
    if (crews.length !== 1) throw new Error('Select a verified Homeworks crew.');
    return { eventId: event.id, crewId: crews[0].id };
  });
  const completed = [];
  try {
    for (const row of planned) {
      const result = await call(
        context,
        'YardDeskAssignCrew',
        `mutation YardDeskAssignCrew($id:SafeInt!,$crewId:SafeInt!){assignEventCrew(eventId:$id,crewId:$crewId){id crewId}}`,
        { id: row.eventId, crewId: row.crewId },
      );
      if (result.assignEventCrew?.crewId !== row.crewId)
        throw new Error('Homeworks did not confirm the crew assignment.');
      completed.push(row.eventId);
    }
    const check = await call(
      context,
      'YardDeskVerifyAssignments',
      `query YardDeskVerifyAssignments($ids:[SafeInt!]!){events(where:{id:{in:$ids},isDeleted:false},take:1000){id crewId}}`,
      { ids },
    );
    if (
      planned.some(
        (row) => !check.events?.some((event) => event.id === row.eventId && event.crewId === row.crewId),
      )
    )
      throw new Error('Crew assignments could not be verified.');
    return { success: true, updated: completed.length, source: SOURCE, verified: true, eventIds: completed };
  } catch (error) {
    error.completedEventIds = completed;
    error.message += completed.length
      ? ' Some visits were updated. Refresh before retrying.'
      : ' Refresh before retrying.';
    throw error;
  }
}
async function applyDispatchChanges(context, patches) {
  const changed = patches.filter(
    (row) => row.patch?.crew_override_name != null || row.patch?.route_order_override != null,
  );
  if (!changed.length) return { dates: [] };
  const ids = changed.map((row) => eventIdFromKey(row.jobKey));
  const data = await call(
    context,
    'YardDeskDispatchChanges',
    `query YardDeskDispatchChanges($ids:[SafeInt!]!){
    crews{id name} events(where:{id:{in:$ids},isDeleted:false},take:1000){id startDate status crewId}
  }`,
    { ids },
  );
  if (data.events?.length !== ids.length)
    throw new Error('The live visit selection changed. Refresh before saving.');
  const groups = new Map();
  for (const row of changed) {
    const event = data.events.find((event) => event.id === eventIdFromKey(row.jobKey));
    const oldCrew = data.crews?.find((crew) => crew.id === event.crewId);
    const nextCrew = row.patch.crew_override_name ?? oldCrew?.name;
    if (!data.crews?.some((crew) => crew.name === nextCrew))
      throw new Error('Choose a verified Homeworks crew.');
    for (const name of [oldCrew?.name, nextCrew].filter(Boolean))
      groups.set(`${event.startDate}|${name}`, { date: event.startDate, name });
  }
  const assignments = changed
    .filter((row) => row.patch.crew_override_name != null)
    .map((row) => ({ job_key: row.jobKey, crew_assigned: row.patch.crew_override_name }));
  if (assignments.length) await saveAssignments(context, assignments);
  for (const group of groups.values()) {
    const state = await routeState(context, group.date, group.name);
    const active = state.events.filter(
      (event) => event.crewId === state.crew.id && !['CANCELLED', 'WAITLISTED'].includes(event.status),
    );
    if (!active.length) continue;
    const previous = new Map((state.route?.routeStops || []).map((stop) => [stop.eventId, stop.stopOrder]));
    const requested = new Map(
      changed
        .filter((row) => row.patch.route_order_override != null)
        .map((row) => [eventIdFromKey(row.jobKey), Number(row.patch.route_order_override)]),
    );
    active.sort(
      (a, b) =>
        (requested.get(a.id) ?? previous.get(a.id) ?? a.sortOrder ?? 1e9) -
          (requested.get(b.id) ?? previous.get(b.id) ?? b.sortOrder ?? 1e9) || a.id - b.id,
    );
    await saveRouteOrder(context, {
      date: group.date,
      crewName: group.name,
      orderedKeys: active.map((event) => `copilot:${event.startDate}:${event.id}`),
    });
  }
  return { dates: [...new Set(data.events.map((event) => event.startDate))] };
}
module.exports = {
  eventIdFromKey,
  routeState,
  saveRouteOrder,
  saveAssignments,
  applyDispatchChanges,
  repeatRoute,
};
