const {
  formatPropertyAddress,
  queryHomeWorksGraphql,
  stripHtml,
} = require('./client');

const normalizePhone = (value) => String(value || '').replace(/\D/g, '').slice(-10);
const money = (value) => {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
};

const isCancelledStatus = (status) => ['CANCELLED', 'CANCELED'].includes(String(status || '').toUpperCase());

function customerPhone(customer) {
  return customer?.cell || customer?.phone || '';
}

function mapCustomer(customer) {
  return {
    id: customer.id,
    name: customer.fullName || [customer.firstName, customer.lastName].filter(Boolean).join(' ') || 'Unknown Customer',
    phone: customerPhone(customer),
    cell: customer.cell || '',
    email: customer.email || '',
    status: customer.status,
    tags: customer.tags || [],
    customerNotes: stripHtml(customer.description),
    address: formatPropertyAddress({ address: customer.address }),
    outstanding: money(customer.outstanding),
    pastDue: money(customer.pastDue),
    source: 'official_homeworks_graphql',
  };
}

function customerWhere({ customerId, phone, search }) {
  if (customerId) return { id: { equals: Number(customerId) }, isDeleted: false };
  const normalized = normalizePhone(phone);
  if (normalized) {
    return {
      isDeleted: false,
      OR: [
        { cell: { contains: normalized } },
        { phone: { contains: normalized } },
      ],
    };
  }
  const term = String(search || '').trim();
  if (!term) return { isDeleted: false };
  return {
    isDeleted: false,
    OR: [
      { fullName: { contains: term } },
      { firstName: { contains: term } },
      { lastName: { contains: term } },
      { email: { contains: term } },
      { cell: { contains: normalizePhone(term) || term } },
      { phone: { contains: normalizePhone(term) || term } },
      { address: { street1: { contains: term } } },
      { address: { city: { contains: term } } },
      { address: { zip: { contains: term } } },
    ],
  };
}

async function fetchMobileCustomers({ pool, accessToken, fetchImpl = fetch, search = '', take = 1500 }) {
  const data = await queryHomeWorksGraphql({
    pool,
    accessToken,
    fetchImpl,
    operationName: 'TwilioConnectCustomers',
    query: `query TwilioConnectCustomers($where: CustomerFilter, $take: SafeInt!) {
      customers(where: $where, take: $take, orderBy: [{ fullName: asc }, { id: asc }]) {
        id fullName firstName lastName email phone cell status tags description outstanding pastDue
        address { street1 street2 city state zip country }
      }
    }`,
    variables: { where: customerWhere({ search }), take },
  });
  return (data.customers || []).map(mapCustomer).filter((customer) => customer.phone);
}

function mapEvent(event) {
  return {
    id: event.id,
    title: event.title,
    date: event.startDate,
    startTime: event.startTime,
    endTime: event.endTime,
    status: event.status,
    total: money(event.total),
    budgetedHours: money(event.budgetedHours),
    crew: event.crew?.name || null,
    address: formatPropertyAddress(event.property),
    propertyId: event.property?.id || null,
    propertyName: event.property?.name || null,
    notes: event.description || '',
    runningTimers: Number(event.timeEntryTotals?.runningTimers || 0),
  };
}

function mapInvoice(invoice, portalKey) {
  const total = money(invoice.total);
  const paidAmount = money(invoice.paidAmount);
  const balance = Math.max(0, total - paidAmount);
  const isPaid = String(invoice.status || '').toUpperCase() === 'PAID' || balance === 0;

  return {
    id: invoice.id,
    number: invoice.number,
    date: invoice.date,
    dueDate: invoice.dueDate,
    status: invoice.status,
    total,
    paidAmount,
    balance,
    daysPastDue: isPaid ? 0 : Number(invoice.daysPastDue || 0),
    isSent: Boolean(invoice.isSent),
    sentAt: invoice.sentAt,
    portalUrl: portalKey
      ? `https://secure.copilotcrm.com/client/invoices/view/${invoice.id}/${portalKey}`
      : null,
  };
}

async function fetchMobileCustomerSnapshot({ pool, accessToken, fetchImpl = fetch, customerId, phone }) {
  let resolvedCustomerId = customerId ? Number(customerId) : null;
  if (!resolvedCustomerId) {
    const normalized = normalizePhone(phone);
    const lookup = await queryHomeWorksGraphql({
      pool,
      accessToken,
      fetchImpl,
      operationName: 'TwilioConnectCustomerPhoneLookup',
      query: `query TwilioConnectCustomerPhoneLookup {
        customers(where: { isDeleted: false }, take: 5000, orderBy: [{ id: asc }]) {
          id phone cell
        }
      }`,
    });
    const match = (lookup.customers || []).find((row) =>
      [row.cell, row.phone].some((value) => normalizePhone(value) === normalized));
    resolvedCustomerId = match?.id || null;
  }
  if (!resolvedCustomerId) return null;

  const data = await queryHomeWorksGraphql({
    pool,
    accessToken,
    fetchImpl,
    operationName: 'TwilioConnectCustomerSnapshot',
    query: `query TwilioConnectCustomerSnapshot($where: CustomerFilter) {
      customers(where: $where, take: 10, orderBy: [{ updatedAt: desc }, { id: desc }]) {
        id fullName firstName lastName email phone cell status tags description outstanding pastDue portalKey
        address { street1 street2 city state zip country }
        properties(take: 25, where: { isActive: true }, orderBy: [{ name: asc }]) {
          id name notes size lastServiceDate
          address { street1 street2 city state zip country }
        }
        events(take: 50, orderBy: [{ startDate: desc }, { id: desc }]) {
          id title startDate startTime endTime status total budgetedHours description
          property { id name address { street1 street2 city state zip country } }
          crew { id name }
          timeEntryTotals { runningTimers }
        }
        estimates(take: 25, orderBy: [{ date: desc }, { id: desc }]) {
          id number date status title total totalOwed paidAmount depositOwed isSent sentEmail sentSms acceptedAt
        }
        invoices(take: 25, orderBy: [{ date: desc }, { id: desc }]) {
          id number date dueDate status total paidAmount daysPastDue isSent sentAt
        }
        payments(take: 25, orderBy: [{ date: desc }, { id: desc }]) {
          id date methodDisplayName method totalAmount paidAmount invoiceId isRefund
        }
      }
    }`,
    variables: { where: customerWhere({ customerId: resolvedCustomerId }) },
  });

  const customer = (data.customers || []).find((row) => String(row.id) === String(resolvedCustomerId))
    || data.customers?.[0];
  if (!customer) return null;

  const smsData = await queryHomeWorksGraphql({
    pool,
    accessToken,
    fetchImpl,
    operationName: 'TwilioConnectCustomerSms',
    query: `query TwilioConnectCustomerSms($customerId: SafeInt!) {
      smsMessages(where: { customerId: { equals: $customerId } }, take: 100, orderBy: [{ sentAt: desc }, { id: desc }]) {
        id sid to from text status direction isViewed isDone sentAt
      }
    }`,
    variables: { customerId: Number(customer.id) },
  }).catch(() => ({ smsMessages: [] }));

  return {
    customer: mapCustomer(customer),
    properties: (customer.properties || []).map((property) => ({
      id: property.id,
      name: property.name,
      address: formatPropertyAddress(property),
      notes: property.notes || '',
      size: money(property.size),
      lastServiceDate: property.lastServiceDate,
    })),
    jobs: (customer.events || []).map(mapEvent),
    estimates: customer.estimates || [],
    invoices: (customer.invoices || []).map((invoice) => mapInvoice(invoice, customer.portalKey)),
    payments: customer.payments || [],
    homeWorksMessages: smsData.smsMessages || [],
    source: 'official_homeworks_graphql',
    asOf: new Date().toISOString(),
  };
}

async function fetchMobileToday({ pool, accessToken, fetchImpl = fetch, date }) {
  const data = await queryHomeWorksGraphql({
    pool,
    accessToken,
    fetchImpl,
    operationName: 'TwilioConnectToday',
    query: `query TwilioConnectToday($date: Date!) {
      events(take: 5000, where: { isDeleted: false, startDate: { equals: $date } }, orderBy: [{ crew: { name: asc } }, { sortOrder: asc }, { id: asc }]) {
        id title startDate startTime endTime status total budgetedHours description sortOrder customerId
        customer { id fullName phone cell }
        property { id name address { street1 street2 city state zip country } }
        crew { id name }
        users { id firstName lastName }
        timeEntryTotals { runningTimers runningTimerSeconds pastTimeEntrySeconds }
      }
      routes(where: { date: { equals: $date } }, take: 100, orderBy: [{ id: asc }]) {
        id date totalMiles totalCost totalTime totalDriveSeconds budgetedHours routeStopCount
        crew { id name }
        routeStops { stopOrder estimatedArrivalAt estimatedDepartureAt eventId }
      }
      estimateLineItems(take: 100, where: { estimate: { status: { equals: ACCEPTED } }, eventStatus: { equals: NO_EVENT } }, orderBy: [{ date: asc }, { id: asc }]) {
        id name description price quantity propertyId
        property { id name address { street1 street2 city state zip country } }
        estimate { id number customerId customer { id fullName phone cell } }
      }
    }`,
    variables: { date },
  });

  const jobs = (data.events || [])
    .filter((event) => !isCancelledStatus(event.status))
    .map((event) => ({
      ...mapEvent(event),
      customerId: event.customerId,
      customerName: event.customer?.fullName || 'Unknown Customer',
      customerPhone: customerPhone(event.customer),
      stopOrder: event.sortOrder,
      employees: (event.users || []).map((user) => [user.firstName, user.lastName].filter(Boolean).join(' ')),
      runningTimerSeconds: Number(event.timeEntryTotals?.runningTimerSeconds || 0),
      trackedSeconds: Number(event.timeEntryTotals?.pastTimeEntrySeconds || 0),
    }));
  const count = (status) => jobs.filter((job) => job.status === status).length;

  return {
    date,
    source: 'official_homeworks_graphql',
    asOf: new Date().toISOString(),
    summary: {
      total: jobs.length,
      open: count('OPEN'),
      completed: count('CLOSED'),
      skipped: count('SKIPPED'),
      activeTimers: jobs.reduce((sum, job) => sum + job.runningTimers, 0),
      scheduledRevenue: Number(jobs.reduce((sum, job) => sum + job.total, 0).toFixed(2)),
    },
    jobs,
    routes: data.routes || [],
    unscheduledAcceptedServices: (data.estimateLineItems || []).map((line) => ({
      id: line.id,
      name: line.name,
      description: line.description,
      price: money(line.price),
      quantity: money(line.quantity),
      propertyId: line.propertyId,
      address: formatPropertyAddress(line.property),
      estimateId: line.estimate?.id,
      estimateNumber: line.estimate?.number,
      customerId: line.estimate?.customerId,
      customerName: line.estimate?.customer?.fullName || 'Unknown Customer',
      customerPhone: customerPhone(line.estimate?.customer),
    })),
  };
}

module.exports = {
  customerWhere,
  fetchMobileCustomerSnapshot,
  fetchMobileCustomers,
  fetchMobileToday,
  mapCustomer,
  normalizePhone,
};
