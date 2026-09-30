const { monthBounds, summarizeVisits } = require('./monthly-performance');
const { loadCustomerTargets } = require('./monthly-performance-targets');

const amount = (value) => Number.isFinite(Number(value)) ? Number(value) : 0;
const PAGE_SIZE = 500;
const MAX_PAGES = 40;

function buildOperationalReport({ month, events, invoices = [], routes = [], recurring = [], contractProgress = [], targets = {}, invoiceCustomers = {}, warnings = [], asOf = new Date() }) {
  const summary = summarizeVisits(events);
  const repeated = new Map();
  for (const event of events) {
    if (event.recurringEventId == null || amount(event.subtotal) < 1000) continue;
    const key = `${event.recurringEventId}:${amount(event.subtotal)}`;
    repeated.set(key, (repeated.get(key) || 0) + 1);
  }
  const facts = events.map((event) => {
    const raw = amount(event.subtotal);
    const contract = event.recurringEventId != null && raw >= 1000
      && (repeated.get(`${event.recurringEventId}:${raw}`) || 0) > 1;
    return {
      id: String(event.id), customerId: event.customer?.id == null ? null : String(event.customer.id),
      customer: event.customer?.fullName || 'Unknown customer', service: event.title || 'Untitled visit',
      property: event.property?.address?.street1 || event.property?.name || 'Unknown property',
      crew: event.crew?.name || null, date: event.startDate, recurringEventId: event.recurringEventId,
      value: contract || raw <= 0 ? null : raw, contractFaceValue: contract ? raw : null,
      budgetedHours: amount(event.budgetedHours),
      trackedHours: amount(event.timeEntryTotals?.pastTimeEntrySeconds) / 3600,
      assignedWorkers: (event.users || []).length,
    };
  });
  const customerMap = new Map();
  const crewMap = new Map();
  for (const row of facts) {
    if (row.customerId && row.value != null && row.budgetedHours > 0) {
      const item = customerMap.get(row.customerId) || { id: row.customerId, name: row.customer, visits: 0, value: 0, hours: 0 };
      item.visits++; item.value += row.value; item.hours += row.budgetedHours;
      customerMap.set(row.customerId, item);
    }
    const crewKey = row.crew || 'Unassigned';
    const crew = crewMap.get(crewKey) || { name: crewKey, visits: 0, budgetedHours: 0, trackedVisits: 0, visitValue: 0 };
    crew.visits++; crew.budgetedHours += row.budgetedHours;
    crew.trackedVisits += row.trackedHours > 0 ? 1 : 0; crew.visitValue += row.value || 0;
    crewMap.set(crewKey, crew);
  }
  const pricing = [...customerMap.values()].map((row) => {
    const target = amount(targets[row.id]) || null;
    return { ...row, perHour: row.value / row.hours, target,
      gap: target ? Math.max(0, target * row.hours - row.value) : null };
  }).sort((a, b) => (b.gap || 0) - (a.gap || 0));
  const serviceRates = new Map();
  for (const row of summary.pricingReview) {
    const key = String(row.service || '').trim().toLocaleLowerCase('en-US');
    const rates = serviceRates.get(key) || [];
    rates.push(row.revenuePerBudgetedHour);
    serviceRates.set(key, rates);
  }
  const peerPricing = summary.pricingReview.map((row) => {
    const rates = (serviceRates.get(String(row.service || '').trim().toLocaleLowerCase('en-US')) || []).sort((a, b) => a - b);
    if (rates.length < 10) return null;
    const median = (rates[Math.floor((rates.length - 1) / 2)] + rates[Math.ceil((rates.length - 1) / 2)]) / 2;
    return { customer: row.customer, service: row.service, address: row.address,
      visits: row.visits, perHour: row.revenuePerBudgetedHour, peerMedian: median,
      peerCount: rates.length, difference: Math.max(0, median * row.budgetedHours - row.visitValue) };
  }).filter((row) => row && row.difference > 0).sort((a, b) => b.difference - a.difference);
  const asOfDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).format(asOf);
  const dayNumber = (key) => Math.floor(Date.parse(`${key}T00:00:00Z`) / 86400000);
  const receivableRows = invoices.filter((row) => row.isSent === true).map((row) => ({
    id: String(row.id), number: row.number,
    customer: invoiceCustomers[String(row.customerId)] || 'Unknown customer',
    dueDate: row.dueDate, status: row.status,
    balance: Math.max(0, amount(row.total) - amount(row.paidAmount)),
    daysPastDue: row.dueDate && !Number.isNaN(dayNumber(String(row.dueDate).slice(0, 10)))
      ? Math.max(0, dayNumber(asOfDate) - dayNumber(String(row.dueDate).slice(0, 10))) : null,
  })).filter((row) => row.balance > 0).sort((a, b) => (b.daysPastDue ?? -1) - (a.daysPastDue ?? -1));
  const aging = { current: 0, days1to30: 0, days31to60: 0, days61plus: 0, unknownDueDate: 0 };
  for (const row of receivableRows) {
    const bucket = row.daysPastDue == null ? 'unknownDueDate' : row.daysPastDue <= 0 ? 'current' : row.daysPastDue <= 30 ? 'days1to30'
      : row.daysPastDue <= 60 ? 'days31to60' : 'days61plus';
    aging[bucket] += row.balance;
  }
  const contractRows = recurring.map((row) => {
    const matching = facts.filter((fact) => String(fact.recurringEventId) === String(row.id));
    const seasonPrice = (row.lineItems || []).reduce((sum, item) => sum + amount(item.price) * (amount(item.quantity) || 1), 0);
    const count = amount(row.eventCount);
    const completed = contractProgress.filter((entry) => String(entry.recurringEventId) === String(row.id)
      && entry.status === 'CLOSED').reduce((sum, entry) => sum + amount(entry._count?.id), 0);
    const remaining = contractProgress.filter((entry) => String(entry.recurringEventId) === String(row.id)
      && entry.status === 'OPEN').reduce((sum, entry) => sum + amount(entry._count?.id), 0);
    return { id: String(row.id), name: row.title || matching[0]?.service || 'Recurring service',
      customer: row.customer?.fullName || matching[0]?.customer || 'Unknown customer',
      endDate: row.endDate, isInfinite: Boolean(row.isInfinite), isCancelled: Boolean(row.isCancelled),
      plannedVisits: count || null, monthCompleted: matching.length,
      completedVisits: contractProgress.length ? completed : null,
      remainingVisits: contractProgress.length ? remaining : null,
      seasonPrice: seasonPrice >= 1000 ? seasonPrice : null,
      equalAllocationPerVisit: seasonPrice >= 1000 && count ? seasonPrice / count : null };
  }).sort((a, b) => String(a.endDate || '9999').localeCompare(String(b.endDate || '9999')));
  return {
    month, source: 'HomeWorks', asOf: asOf.toISOString(), warnings,
    pricing, peerPricing, contracts: contractRows,
    receivables: { total: receivableRows.reduce((sum, row) => sum + row.balance, 0), aging, rows: receivableRows },
    crews: [...crewMap.values()].sort((a, b) => b.visits - a.visits),
    routes: routes.map((row) => ({ crew: row.crew?.name || 'Unassigned',
      routes: amount(row._count?.id), stops: amount(row._sum?.totalStops),
      miles: amount(row._sum?.totalMiles), driveHours: amount(row._sum?.totalDriveSeconds) / 3600 })),
    issues: {
      missingVisitPrice: facts.filter((row) => row.value == null && row.contractFaceValue == null).length,
      missingBudget: facts.filter((row) => row.budgetedHours <= 0).length,
      missingTrackedTime: facts.filter((row) => row.trackedHours <= 0).length,
      missingCrew: facts.filter((row) => !row.crew).length,
      repeatedContractAmount: facts.filter((row) => row.contractFaceValue != null).length,
    },
    issueExamples: facts.map((row) => ({ ...row, reasons: [
      !row.value && !row.contractFaceValue ? 'No visit price' : null,
      !row.budgetedHours ? 'No budgeted duration' : null,
      !row.crew ? 'No crew' : null,
      row.contractFaceValue ? 'Season amount repeated' : null,
      !row.trackedHours ? 'No tracked time' : null,
    ].filter(Boolean) })).filter((row) => row.reasons.length)
      .sort((a, b) => (b.reasons.some((reason) => reason !== 'No tracked time') ? 1 : 0)
        - (a.reasons.some((reason) => reason !== 'No tracked time') ? 1 : 0))
      .slice(0, 40),
    profitability: { completedVisits: facts.length,
      trackedVisits: facts.filter((row) => row.trackedHours > 0).length,
      pricedAndBudgetedVisits: facts.filter((row) => row.value != null && row.budgetedHours > 0).length,
      visitValue: summary.visitValue,
      actualProfitAvailable: false,
      explanation: 'Visit prices and budgeted duration cannot establish profit. Tracked worker time, owner labor cost, materials, payroll burden and overhead are needed.' },
  };
}

async function fetchOperationalReport({ pool, month, fetchImpl, accessToken, queryImpl }) {
  const { start, end } = monthBounds(month);
  const queryHomeWorksGraphql = queryImpl || require('../services/homeworks/client').queryHomeWorksGraphql;
  const request = ({ operationName, query, variables }) => queryHomeWorksGraphql({ pool, fetchImpl, accessToken, operationName, query, variables });
  const events = [];
  for (let page = 0; page < MAX_PAGES; page++) {
    const data = await request({ operationName: 'YardDeskOperationalVisits',
      query: `query YardDeskOperationalVisits($start: Date!, $end: Date!, $skip: SafeInt!) {
        events(take: 500, skip: $skip, orderBy: [{ startDate: asc }, { id: asc }],
          where: { type: { equals: VISIT }, status: { equals: CLOSED },
            startDate: { gte: $start, lte: $end } }) {
          id title startDate subtotal budgetedHours recurringEventId
          customer { id fullName } property { id name address { street1 } }
          crew { id name } timeEntryTotals { pastTimeEntrySeconds }
        }
      }`, variables: { start, end, skip: page * PAGE_SIZE } });
    const batch = data.events || [];
    events.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    if (page === MAX_PAGES - 1) throw new Error('Visit report exceeds the supported page limit');
  }
  const warnings = [];
  const optional = async (label, fn, fallback) => {
    try { return await fn(); } catch (error) { warnings.push(`${label}: ${error.message}`); return fallback; }
  };
  const [targets, routes, invoices, recurring] = await Promise.all([
    loadCustomerTargets(pool),
    optional('Routes', async () => (await request({ operationName: 'YardDeskOperationalRoutes',
      query: `query YardDeskOperationalRoutes($start: Date!, $end: Date!) {
        routeReport(where: { date: { gte: $start, lte: $end } }) {
          crewId crew { name } _count { id } _sum { totalMiles totalDriveSeconds totalStops }
        }
      }`, variables: { start, end } })).routeReport || [], []),
    optional('Receivables', async () => {
      const rows = [];
      for (let page = 0; page < MAX_PAGES; page++) {
        const data = await request({ operationName: 'YardDeskOpenInvoices',
          query: `query YardDeskOpenInvoices($skip: SafeInt!) {
            invoices(take: 500, skip: $skip, orderBy: [{ id: asc }],
              where: { isArchived: false, deletedState: { equals: ACTIVE },
                status: { in: [PENDING, PARTIALLY_PAID, PAST_DUE] } }) {
              id number isSent dueDate daysPastDue total paidAmount status customerId
            }
          }`, variables: { skip: page * PAGE_SIZE } });
        const batch = data.invoices || [];
        rows.push(...batch);
        if (batch.length < PAGE_SIZE) break;
        if (page === MAX_PAGES - 1) throw new Error('Open invoice report exceeds the supported page limit');
      }
      return rows;
    }, []),
    optional('Contracts', async () => {
      const ids = [...new Set(events.map((event) => String(event.recurringEventId || '')).filter((id) => /^\d+$/.test(id)))];
      const rows = [];
      for (let index = 0; index < ids.length; index += 150) {
        const safeIds = ids.slice(index, index + 150).join(',');
        const data = await request({ operationName: 'YardDeskOperationalContracts',
          query: `query YardDeskOperationalContracts {
            recurringEvents(take: 150, where: { id: { in: [${safeIds}] } }) {
              id title endDate isInfinite isCancelled eventCount
              customer { id fullName } lineItems { name price quantity }
            }
          }` });
        rows.push(...(data.recurringEvents || []));
      }
      return rows;
    }, []),
  ]);
  const contractIds = recurring.map((row) => String(row.id)).filter((id) => /^\d+$/.test(id));
  const contractProgress = await optional('Contract progress', async () => {
    const rows = [];
    for (let index = 0; index < contractIds.length; index += 150) {
      const safeIds = contractIds.slice(index, index + 150).join(',');
      const data = await request({ operationName: 'YardDeskContractProgress',
        query: `query YardDeskContractProgress {
          eventReport(where: { recurringEventId: { in: [${safeIds}] }, type: { equals: VISIT } }) {
            recurringEventId status _count { id }
          }
        }` });
      rows.push(...(data.eventReport || []));
    }
    return rows;
  }, []);
  const invoiceCustomers = {};
  const customerIds = [...new Set(invoices.map((row) => String(row.customerId || '')).filter((id) => /^\d+$/.test(id)))];
  await optional('Invoice customer names', async () => {
    for (let index = 0; index < customerIds.length; index += 150) {
      const safeIds = customerIds.slice(index, index + 150).join(',');
      const data = await request({ operationName: 'YardDeskInvoiceCustomers',
        query: `query YardDeskInvoiceCustomers {
          customers(take: 150, where: { id: { in: [${safeIds}] } }) { id fullName }
        }` });
      for (const customer of data.customers || []) invoiceCustomers[String(customer.id)] = customer.fullName;
    }
  }, null);
  return buildOperationalReport({ month, events, invoices, routes, recurring, contractProgress, targets, invoiceCustomers, warnings });
}

module.exports = { buildOperationalReport, fetchOperationalReport };
