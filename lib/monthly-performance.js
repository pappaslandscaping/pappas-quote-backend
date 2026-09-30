const PAGE_SIZE = 1000;
const MAX_PAGES = 20;

function money(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function monthBounds(month) {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(String(month || ''))) {
    throw new Error('month must be YYYY-MM');
  }
  const [year, monthNumber] = month.split('-').map(Number);
  const start = `${month}-01`;
  const end = new Date(Date.UTC(year, monthNumber, 0)).toISOString().slice(0, 10);
  const prior = new Date(Date.UTC(year, monthNumber - 2, 1)).toISOString().slice(0, 7);
  return { start, end, prior };
}

function summarizeVisits(events) {
  const services = new Map();
  const properties = new Map();
  // A recurring estimate can carry its entire season price on every generated
  // visit. Repeated large, identical amounts cannot be treated as visit prices.
  const recurringCharges = new Map();
  for (const event of events) {
    if (event.recurringEventId == null || money(event.subtotal) < 1000) continue;
    const key = `${event.recurringEventId}:${money(event.subtotal)}`;
    recurringCharges.set(key, (recurringCharges.get(key) || 0) + 1);
  }
  let budgetedHours = 0;
  let pricedVisits = 0;
  let budgetedVisits = 0;
  let trackedVisits = 0;
  let trackedHours = 0;
  let visitValue = 0;
  let unallocatedContractVisits = 0;
  let unallocatedContractFaceValue = 0;

  for (const event of events) {
    const rawPrice = money(event.subtotal);
    const recurringKey = `${event.recurringEventId}:${rawPrice}`;
    const unallocatedContract = event.recurringEventId != null && rawPrice >= 1000
      && (recurringCharges.get(recurringKey) || 0) >= 2;
    const price = unallocatedContract ? 0 : rawPrice;
    if (unallocatedContract) {
      unallocatedContractVisits++;
      unallocatedContractFaceValue += rawPrice;
    }
    const budget = money(event.budgetedHours);
    const seconds = money(event.timeEntryTotals?.pastTimeEntrySeconds);
    const service = String(event.title || 'Unlabeled service').trim() || 'Unlabeled service';
    const address = event.property?.address?.street1 || event.property?.name || 'Unknown property';
    const customer = event.customer?.fullName || 'Unknown customer';
    const customerId = event.customer?.id == null ? null : String(event.customer.id);
    const serviceKey = service.toLocaleLowerCase('en-US');
    const propertyKey = `${event.property?.id ?? event.id}:${serviceKey}`;
    visitValue += price;
    budgetedHours += budget;
    if (price > 0) pricedVisits++;
    if (budget > 0) budgetedVisits++;
    if (seconds > 0) { trackedVisits++; trackedHours += seconds / 3600; }

    const serviceRow = services.get(serviceKey) || { name: service, visits: 0, visitValue: 0, budgetedHours: 0, trackedVisits: 0, customerIds: new Set() };
    serviceRow.visits++;
    serviceRow.visitValue += price;
    serviceRow.budgetedHours += unallocatedContract ? 0 : budget;
    if (event.customer?.id != null) serviceRow.customerIds.add(String(event.customer.id));
    if (seconds > 0) serviceRow.trackedVisits++;
    services.set(serviceKey, serviceRow);

    if (price > 0 && budget > 0) {
      const propertyRow = properties.get(propertyKey) || {
        customer, customerId, address, service, propertyId: event.property?.id || null,
        visits: 0, visitValue: 0, budgetedHours: 0, trackedVisits: 0,
      };
      propertyRow.visits++;
      propertyRow.visitValue += price;
      propertyRow.budgetedHours += budget;
      if (seconds > 0) propertyRow.trackedVisits++;
      properties.set(propertyKey, propertyRow);
    }
  }

  const toRate = (row) => ({
    ...row,
    averagePrice: row.visits ? row.visitValue / row.visits : null,
    averageBudgetedHours: row.visits ? row.budgetedHours / row.visits : null,
    revenuePerBudgetedHour: row.budgetedHours > 0 ? row.visitValue / row.budgetedHours : null,
  });
  return {
    coverage: { completedVisits: events.length, pricedVisits, budgetedVisits, trackedVisits, trackedHours, budgetedHours,
      unallocatedContractVisits, unallocatedContractFaceValue },
    visitValue,
    services: [...services.values()].map((row) => {
      const { customerIds, ...values } = row;
      return toRate({ ...values, customers: customerIds.size });
    }).sort((a, b) => b.visitValue - a.visitValue),
    pricingReview: [...properties.values()].map(toRate)
      .filter((row) => row.averagePrice > 0 && row.averageBudgetedHours > 0)
      .sort((a, b) => a.revenuePerBudgetedHour - b.revenuePerBudgetedHour),
  };
}

function reportTotals(row) {
  return {
    count: Number(row?._count?.id || 0),
    billed: money(row?._sum?.total),
    paidOnInvoices: money(row?._sum?.paidAmount),
  };
}

async function fetchMonthlyPerformance({ pool, month, fetchImpl, accessToken, queryImpl }) {
  const { start, end, prior } = monthBounds(month);
  const priorBounds = monthBounds(prior);
  const queryHomeWorksGraphql = queryImpl || require('../services/homeworks/client').queryHomeWorksGraphql;
  const request = ({ operationName, query, variables }) => queryHomeWorksGraphql({
    pool, fetchImpl, accessToken, operationName, query, variables,
  });

  const [summary, priorSummary] = await Promise.all([
    request({
      operationName: 'YardDeskMonthlyPerformanceTotals',
      query: `query YardDeskMonthlyPerformanceTotals($start: Date!, $end: Date!) {
        invoiceReport(where: { date: { gte: $start, lte: $end },
          deletedState: { equals: ACTIVE }, status: { in: [PENDING, PARTIALLY_PAID, PAID, PAST_DUE] } }) {
          _count { id } _sum { total paidAmount }
        }
        paymentReport(where: { date: { gte: $start, lte: $end }, isDeleted: false,
          isRefund: false, isExpensePayment: false }) { _count { id } _sum { totalAmount } }
        refundReport: paymentReport(where: { date: { gte: $start, lte: $end }, isDeleted: false,
          isRefund: true, isExpensePayment: false }) { _sum { totalAmount } }
      }`,
      variables: { start, end },
    }),
    request({
      operationName: 'YardDeskMonthlyPerformancePrior',
      query: `query YardDeskMonthlyPerformancePrior($start: Date!, $end: Date!) {
        invoiceReport(where: { date: { gte: $start, lte: $end },
          deletedState: { equals: ACTIVE }, status: { in: [PENDING, PARTIALLY_PAID, PAID, PAST_DUE] } }) {
          _count { id } _sum { total paidAmount }
        }
        paymentReport(where: { date: { gte: $start, lte: $end }, isDeleted: false,
          isRefund: false, isExpensePayment: false }) { _sum { totalAmount } }
        refundReport: paymentReport(where: { date: { gte: $start, lte: $end }, isDeleted: false,
          isRefund: true, isExpensePayment: false }) { _sum { totalAmount } }
      }`,
      variables: { start: priorBounds.start, end: priorBounds.end },
    }),
  ]);

  const events = [];
  for (let page = 0; page < MAX_PAGES; page++) {
    const data = await request({
      operationName: 'YardDeskMonthlyPerformanceVisits',
      query: `query YardDeskMonthlyPerformanceVisits($start: Date!, $end: Date!, $skip: SafeInt!) {
        events(take: 1000, skip: $skip, orderBy: [{ startDate: asc }, { id: asc }],
          where: { type: { equals: VISIT }, status: { equals: CLOSED },
            startDate: { gte: $start, lte: $end } }) {
          id title startDate subtotal budgetedHours recurringEventId
          customer { id fullName }
          property { id name address { street1 } }
          timeEntryTotals { pastTimeEntrySeconds }
        }
      }`,
      variables: { start, end, skip: page * PAGE_SIZE },
    });
    const batch = data.events || [];
    events.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    if (page === MAX_PAGES - 1) throw new Error('Monthly visit report exceeds the supported page limit');
  }

  const visitSummary = summarizeVisits(events);
  const invoices = reportTotals(summary.invoiceReport?.[0]);
  const previousInvoices = reportTotals(priorSummary.invoiceReport?.[0]);
  const netPayments = (data) => money(data.paymentReport?.[0]?._sum?.totalAmount)
    - money(data.refundReport?.[0]?._sum?.totalAmount);
  return {
    month, source: 'HomeWorks', asOf: new Date().toISOString(),
    definitions: {
      billed: 'Active, non-draft invoices dated in the month, including tax and fees. Excludes write-offs.',
      collected: 'Payments dated in the month, less refunds. They may pay invoices from other months.',
      visitValue: 'Pre-tax prices on completed HomeWorks visits dated in the month, excluding repeated high recurring contract amounts that cannot be assigned to a single visit. This is not invoiced revenue.',
      pricing: 'Visit price divided by budgeted visit duration, not total worker-hours. This is a planning signal, not actual profit.',
    },
    financials: { ...invoices, collected: netPayments(summary) },
    previousMonth: { month: prior, ...previousInvoices, collected: netPayments(priorSummary) },
    ...visitSummary,
  };
}

module.exports = { monthBounds, summarizeVisits, fetchMonthlyPerformance };
