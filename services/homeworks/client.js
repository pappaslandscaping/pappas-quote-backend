const { getCopilotToken } = require('../copilot/client');

const HOMEWORKS_GRAPHQL_URL = 'https://api.copilotcrm.com/graphql';
const DEFAULT_TIMEOUT_MS = 15000;

function extractAccessToken(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const cookieMatch = raw.match(/(?:^|;\s*)copilotApiAccessToken=([^;]+)/i);
  return decodeURIComponent(cookieMatch ? cookieMatch[1] : raw);
}

async function getHomeWorksAccessToken(pool) {
  const configured = process.env.HOMEWORKS_API_TOKEN || process.env.HOMEWORKS_ACCESS_TOKEN;
  if (configured) return extractAccessToken(configured);

  const tokenInfo = await getCopilotToken(pool);
  return extractAccessToken(tokenInfo?.cookieHeader);
}

async function queryHomeWorksGraphql({
  pool,
  accessToken,
  operationName,
  query,
  variables = {},
  fetchImpl = fetch,
  timeoutMs = DEFAULT_TIMEOUT_MS,
}) {
  const token = accessToken || await getHomeWorksAccessToken(pool);
  if (!token) throw new Error('HomeWorks API token is not configured');

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetchImpl(HOMEWORKS_GRAPHQL_URL, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ operationName, query, variables }),
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload?.errors?.length) {
      const message = payload?.errors?.map((error) => error.message).filter(Boolean).join('; ')
        || `HomeWorks API returned ${response.status}`;
      throw new Error(message);
    }
    return payload.data || {};
  } catch (error) {
    if (error?.name === 'AbortError') throw new Error('HomeWorks API request timed out');
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function stripHtml(value) {
  return String(value || '')
    .replace(/<br\s*\/?\s*>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/gi, '"')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n\s+/g, '\n')
    .trim();
}

function formatPropertyAddress(property) {
  const address = property?.address || {};
  const street = [address.street1, address.street2].filter(Boolean).join(' ').trim();
  const locality = [address.city, address.state, address.zip].filter(Boolean).join(' ').trim();
  return [street, locality].filter(Boolean).join(', ');
}

function mapHomeWorksEventToLiveJob(event) {
  const users = Array.isArray(event?.users) ? event.users : [];
  return {
    event_id: event?.id == null ? null : String(event.id),
    customer_id: event?.customerId == null ? null : String(event.customerId),
    customer_name: event?.customer?.fullName || event?.title || 'Unknown',
    crew_name: event?.crew?.name || null,
    employees: users.map((user) => [user.firstName, user.lastName].filter(Boolean).join(' ')).filter(Boolean).join(', '),
    address: formatPropertyAddress(event?.property),
    status: event?.status || null,
    visit_total: event?.total ?? null,
    job_title: event?.title || 'Service',
    stop_order: event?.sortOrder ?? null,
    raw_data: {
      source_surface: 'homeworks_graphql',
      property_id: event?.property?.id ?? null,
      property_name: event?.property?.name || null,
      event_type: event?.type || null,
      invoiceable: event?.isInvoiceable ?? null,
      frequency: event?.recurringEventId ? 'Recurring' : 'One-time',
      notes: stripHtml(event?.description),
      budgeted_hours: event?.budgetedHours ?? null,
      start_time: event?.startTime || null,
      end_time: event?.endTime || null,
      customer_phone: event?.customer?.cell || event?.customer?.phone || null,
      customer_email: event?.customer?.email || null,
      updated_at: event?.updatedAt || null,
    },
  };
}

async function fetchHomeWorksEvents({ pool, startDate, endDate, fetchImpl = fetch, accessToken }) {
  const data = await queryHomeWorksGraphql({
    pool,
    accessToken,
    fetchImpl,
    operationName: 'YardDeskSchedule',
    query: `query YardDeskSchedule($start: Date!, $end: Date!) {
      events(
        take: 5000
        orderBy: [{ startDate: asc }, { sortOrder: asc }, { id: asc }]
        where: { isDeleted: false, startDate: { gte: $start, lte: $end } }
      ) {
        id startDate startTime endTime title description type status total
        budgetedHours sortOrder customerId recurringEventId isInvoiceable updatedAt
        customer { id fullName phone cell email }
        property { id name address { street1 street2 city state zip country } }
        crew { id name }
        users { id firstName lastName }
      }
    }`,
    variables: { start: startDate, end: endDate },
  });

  return (data.events || []).map((event) => ({
    service_date: event.startDate,
    ...mapHomeWorksEventToLiveJob(event),
  }));
}

async function fetchHomeWorksIntegrationStatus({ pool, fetchImpl = fetch, accessToken }) {
  const data = await queryHomeWorksGraphql({
    pool,
    accessToken,
    fetchImpl,
    operationName: 'YardDeskIntegrationStatus',
    query: `query YardDeskIntegrationStatus {
      company {
        id name
        quickbooks {
          isConnected
          invoicesLastDownloaded invoicesSyncFailedCount
          customersLastDownloaded customersSyncFailedCount
          paymentsLastDownloaded paymentsSyncFailedCount
          expiredAt
        }
      }
      items(take: 5000, where: { isDeleted: false }) { id }
      payments(take: 25, orderBy: [{ date: desc }, { id: desc }]) {
        id date method methodDisplayName totalAmount
      }
      invoices(take: 100, orderBy: [{ updatedAt: desc }, { id: desc }]) {
        id quickbooksSyncStatus quickbooksSyncFailedCount quickbooksSyncFailedMessage
      }
    }`,
  });

  const invoices = data.invoices || [];
  const failedInvoices = invoices.filter((invoice) => Number(invoice.quickbooksSyncFailedCount || 0) > 0);
  return {
    connected: true,
    source: 'official_homeworks_graphql',
    checkedAt: new Date().toISOString(),
    company: data.company || null,
    catalog: { itemCount: (data.items || []).length },
    payments: (data.payments || []).map((payment) => ({
      id: payment.id,
      date: payment.date,
      method: payment.methodDisplayName || payment.method || 'Unknown',
      totalAmount: payment.totalAmount,
    })),
    quickbooks: {
      ...(data.company?.quickbooks || {}),
      recentInvoiceFailures: failedInvoices.length,
      recentInvoiceFailureIds: failedInvoices.map((invoice) => invoice.id),
    },
  };
}

module.exports = {
  HOMEWORKS_GRAPHQL_URL,
  extractAccessToken,
  fetchHomeWorksEvents,
  fetchHomeWorksIntegrationStatus,
  formatPropertyAddress,
  getHomeWorksAccessToken,
  mapHomeWorksEventToLiveJob,
  queryHomeWorksGraphql,
  stripHtml,
};
