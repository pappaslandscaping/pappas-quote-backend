const { monthBounds } = require('./monthly-performance');
const { loadCustomerTargets } = require('./monthly-performance-targets');

const PAGE_SIZE = 500;
const MAX_PAGES = 40;
const MIN_PEERS = 10;

function finitePositive(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function serviceKey(value) {
  return String(value || 'Unlabeled service').trim().toLocaleLowerCase('en-US');
}

function proposalBounds(month) {
  const { end } = monthBounds(month);
  const [year, number] = month.split('-').map(Number);
  const start = new Date(Date.UTC(year, number - 3, 1)).toISOString().slice(0, 10);
  return { start, end, currentStart: `${month}-01` };
}

function buildTargetProposals({ month, events, savedTargets = {}, asOf = new Date() }) {
  const { start, end, currentStart } = proposalBounds(month);
  const recurringAmounts = new Map();
  for (const event of events) {
    const price = finitePositive(event.subtotal);
    if (!event.recurringEventId || price == null || price < 1000) continue;
    const key = `${event.recurringEventId}:${price}`;
    recurringAmounts.set(key, (recurringAmounts.get(key) || 0) + 1);
  }
  const properties = new Map();
  const currentCustomers = new Set();
  const excluded = { noPrice: 0, noBudget: 0, repeatedSeasonAmount: 0 };
  for (const event of events) {
    const price = finitePositive(event.subtotal);
    const hours = finitePositive(event.budgetedHours);
    if (price == null) { excluded.noPrice++; continue; }
    if (hours == null) { excluded.noBudget++; continue; }
    if (event.recurringEventId && price >= 1000
      && (recurringAmounts.get(`${event.recurringEventId}:${price}`) || 0) > 1) {
      excluded.repeatedSeasonAmount++; continue;
    }
    const customerId = String(event.customer?.id || '');
    if (!/^\d+$/.test(customerId)) continue;
    const service = serviceKey(event.title);
    const propertyId = String(event.property?.id || event.id);
    const key = `${customerId}:${propertyId}:${service}`;
    const row = properties.get(key) || {
      customerId, customer: event.customer?.fullName || 'Unknown customer',
      propertyId, property: event.property?.address?.street1 || event.property?.name || 'Unknown property',
      service, visits: 0, value: 0, budgetedHours: 0,
    };
    row.visits++; row.value += price; row.budgetedHours += hours;
    properties.set(key, row);
    if (String(event.startDate || '') >= currentStart && String(event.startDate || '') <= end) currentCustomers.add(customerId);
  }
  const peersByService = new Map();
  for (const row of properties.values()) {
    const peers = peersByService.get(row.service) || [];
    peers.push({ propertyId: row.propertyId, customerId: row.customerId,
      rate: row.value / row.budgetedHours });
    peersByService.set(row.service, peers);
  }
  const byCustomer = new Map();
  for (const row of properties.values()) {
    if (!currentCustomers.has(row.customerId)) continue;
    const peers = (peersByService.get(row.service) || [])
      .filter((peer) => !(peer.propertyId === row.propertyId && peer.customerId === row.customerId));
    const peerMedian = peers.length >= MIN_PEERS ? median(peers.map((peer) => peer.rate)) : null;
    const customer = byCustomer.get(row.customerId) || {
      customerId: row.customerId, customer: row.customer,
      visits: 0, value: 0, budgetedHours: 0, peerValue: 0,
      comparableProperties: 0, missingPeerServices: new Set(), properties: [],
    };
    customer.visits += row.visits;
    customer.value += row.value;
    customer.budgetedHours += row.budgetedHours;
    if (peerMedian == null) customer.missingPeerServices.add(row.service);
    else { customer.peerValue += peerMedian * row.budgetedHours; customer.comparableProperties++; }
    customer.properties.push({ property: row.property, service: row.service,
      visits: row.visits, currentPerHour: row.value / row.budgetedHours,
      peerMedian, peerCount: peers.length });
    byCustomer.set(row.customerId, customer);
  }
  const proposals = [...byCustomer.values()].map((row) => {
    const currentPerHour = row.value / row.budgetedHours;
    const peerPerHour = row.missingPeerServices.size ? null : row.peerValue / row.budgetedHours;
    const rawTarget = peerPerHour == null ? null : Math.ceil(Math.max(currentPerHour, peerPerHour));
    const suggestedTarget = rawTarget != null && rawTarget <= 10000 ? rawTarget : null;
    const increasePercent = suggestedTarget == null ? null : Math.max(0, suggestedTarget / currentPerHour - 1) * 100;
    const savedTarget = finitePositive(savedTargets[row.customerId]);
    const reviewReason = row.missingPeerServices.size ? 'No reliable service peer group'
      : suggestedTarget == null ? 'Proposed rate exceeds the supported target range'
      : row.visits < 3 ? 'Fewer than 3 priced visits in the review window'
        : increasePercent > 25 ? 'Proposed increase exceeds 25%; verify scope and budgeted time' : null;
    return {
      customerId: row.customerId, customer: row.customer, visits: row.visits,
      budgetedHours: row.budgetedHours, currentPerHour, peerPerHour,
      suggestedTarget, increasePercent, savedTarget,
      reviewStatus: savedTarget != null ? 'already_set' : reviewReason ? 'review' : 'ready',
      reviewReason, missingPeerServices: [...row.missingPeerServices],
      comparableProperties: row.comparableProperties, properties: row.properties,
    };
  }).sort((a, b) => {
    const order = { review: 0, ready: 1, already_set: 2 };
    return order[a.reviewStatus] - order[b.reviewStatus]
      || (b.increasePercent || 0) - (a.increasePercent || 0)
      || a.customer.localeCompare(b.customer);
  });
  return {
    month, reviewWindow: { start, end }, source: 'HomeWorks', asOf: asOf.toISOString(),
    method: 'Three months of completed, priced HomeWorks visits. Each property/service rate is visit price divided by budgeted visit duration. The proposal uses the median of at least ten other comparable properties for each service, weighted by this customer’s budgeted hours. It never proposes less than the customer’s recent rate. This is a peer-based planning goal, not a proven profit floor.',
    excluded,
    summary: {
      customers: proposals.length,
      ready: proposals.filter((row) => row.reviewStatus === 'ready').length,
      review: proposals.filter((row) => row.reviewStatus === 'review').length,
      alreadySet: proposals.filter((row) => row.reviewStatus === 'already_set').length,
    },
    proposals,
  };
}

async function fetchTargetProposals({ pool, month, fetchImpl, accessToken, queryImpl }) {
  const { start, end } = proposalBounds(month);
  const queryHomeWorksGraphql = queryImpl || require('../services/homeworks/client').queryHomeWorksGraphql;
  const events = [];
  for (let page = 0; page < MAX_PAGES; page++) {
    const data = await queryHomeWorksGraphql({ pool, fetchImpl, accessToken,
      operationName: 'YardDeskTargetProposalVisits',
      query: `query YardDeskTargetProposalVisits($start: Date!, $end: Date!, $skip: SafeInt!) {
        events(take: 500, skip: $skip, orderBy: [{ startDate: asc }, { id: asc }],
          where: { isDeleted: false, type: { equals: VISIT }, status: { equals: CLOSED },
            startDate: { gte: $start, lte: $end } }) {
          id title startDate subtotal budgetedHours recurringEventId
          customer { id fullName } property { id name address { street1 } }
        }
      }`, variables: { start, end, skip: page * PAGE_SIZE } });
    const batch = data.events || [];
    events.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    if (page === MAX_PAGES - 1) throw new Error('Target review exceeds the supported page limit');
  }
  const savedTargets = await loadCustomerTargets(pool);
  return buildTargetProposals({ month, events, savedTargets });
}

module.exports = { proposalBounds, buildTargetProposals, fetchTargetProposals };
