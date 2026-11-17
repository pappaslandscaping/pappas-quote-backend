'use strict';
const { call, listAll, uniqueCustomer, dateKey, SOURCE } = require('./records');
const { fetchHomeWorksEvents } = require('./client');
async function execute(context, action, params = {}) {
  if (action === 'dispatch_today' || action === 'dispatch_date_range') action = 'dispatch';
  if (['dispatch', 'today_dispatch', 'route', 'schedule'].includes(action)) {
    const start = params.start_date || params.date || dateKey(),
      end = params.end_date || start;
    const jobs = await fetchHomeWorksEvents({ ...context, startDate: start, endDate: end });
    return { source: SOURCE, jobs, jobCount: jobs.length, dateRange: { start, end } };
  }
  const customer = await uniqueCustomer(context, {
    customerId: params.customer_id,
    name: params.customer_name,
    phone: params.phone,
    email: params.email,
  });
  delete customer.portalKey;
  if (action === 'customer_lookup') return { source: SOURCE, customer };
  if (action === 'customer_estimates') {
    const estimates = await listAll(context, {
      entity: 'estimates',
      filterType: 'EstimateFilter',
      where: { isDeleted: false, isArchived: false, customerId: { equals: customer.id } },
      fields: 'id number date title status total isSent requestedChanges acceptedAt',
    });
    return { source: SOURCE, customer, estimates };
  }
  if (action === 'service_history') {
    const since = new Date();
    since.setFullYear(since.getFullYear() - 1);
    const jobs = await listAll(context, {
      entity: 'events',
      filterType: 'EventFilter',
      where: {
        isDeleted: false,
        type: { equals: 'VISIT' },
        customerId: { equals: customer.id },
        startDate: { gte: dateKey(since), lte: dateKey() },
      },
      fields: 'id title status startDate total isInvoiceable invoiceId crew{id name} property{id name}',
    });
    return { source: SOURCE, customer, jobs, jobCount: jobs.length };
  }
  throw new Error('This Homeworks lookup is not supported.');
}
module.exports = { execute };
