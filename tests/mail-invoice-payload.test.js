const assert = require('assert');
const createInvoiceRoutes = require('../routes/invoices');

createInvoiceRoutes({
  pool: {
    query: async () => ({
      rows: [{
        id: 2384,
        display_name: 'Superior Industrial',
        address_fingerprint: '3855west150thstreetclevelandoh44111',
      }],
    }),
  },
  sendEmail: async () => ({}),
  emailTemplate: () => '',
  escapeHtml: (value) => String(value || ''),
  serverError: (_res, error) => {
    throw error;
  },
  authenticateToken: (_req, _res, next) => next(),
  nextInvoiceNumber: async () => '10000',
  squareClient: null,
  SQUARE_APP_ID: '',
  SQUARE_LOCATION_ID: '',
  SquareApiError: Error,
});

const {
  attachMailCustomerMatches,
  buildInvoiceListQuery,
  buildMailInvoicePayload,
  formatMailDate,
  latestMailServiceDate,
  resolveMailInvoiceDate,
} = createInvoiceRoutes._helpers;

function superiorIndustrialInvoice(overrides = {}) {
  return {
    id: 15319,
    invoice_number: '10300',
    created_at: '2026-04-07T04:00:00.000Z',
    due_date: null,
    customer_name: 'Superior Industrial',
    customer_address: '3855 West 150th Street\nCleveland, OH 44111',
    subtotal: '180.00',
    tax_amount: '14.08',
    total: '194.08',
    external_metadata: {
      invoice_date: null,
      prior_balance: 328.67,
      total_due: 522.75,
    },
    line_items: [
      { name: 'Mowing (Weekly)', service_date: '2026-04-07', quantity: 1, rate: 44, total: 47.52 },
      { name: 'Mowing (Weekly)', service_date: '2026-04-14', quantity: 1, rate: 44, total: 47.52 },
      { name: 'Mowing (Weekly)', service_date: '2026-04-21', quantity: 1, rate: 44, total: 47.52 },
      { name: 'Mowing (Weekly)', service_date: '2026-04-28', quantity: 1, rate: 44, total: 47.52 },
      { name: 'Fuel Surcharge', service_date: '2026-04-28', quantity: 1, rate: 4, total: 4 },
    ],
    ...overrides,
  };
}

async function runAssertions() {
  const payload = buildMailInvoicePayload(superiorIndustrialInvoice());

  assert.strictEqual(payload.invoice_date_raw, 'Apr 28, 2026');
  assert.strictEqual(
    latestMailServiceDate(superiorIndustrialInvoice().line_items),
    '2026-04-28'
  );
  assert.strictEqual(formatMailDate('2026-04-28'), 'Apr 28, 2026');

  const explicitCopilotDate = resolveMailInvoiceDate({
    row: superiorIndustrialInvoice(),
    metadata: { invoice_date: '2026-04-30' },
    lineItems: superiorIndustrialInvoice().line_items,
  });
  assert.strictEqual(explicitCopilotDate, '2026-04-30');

  const createdAtFallback = buildMailInvoicePayload(superiorIndustrialInvoice({ line_items: [] }));
  assert.strictEqual(createdAtFallback.invoice_date_raw, 'Apr 7, 2026');

  const [matchedInvoice] = await attachMailCustomerMatches([superiorIndustrialInvoice({
    customer_id: null,
    customer_name: 'Return',
    customer_address: '3855 West 150th Street Cleveland OH 44111',
    external_metadata: {
      customer_name: 'Return',
      invoice_date: null,
      prior_balance: 328.67,
      total_due: 522.75,
    },
  })]);
  const matchedPayload = buildMailInvoicePayload(matchedInvoice);

  assert.strictEqual(matchedPayload.customer_id, 2384);
  assert.strictEqual(matchedPayload.customer_name, 'Superior Industrial');
  assert.strictEqual(matchedPayload.metadata.customer_name, 'Superior Industrial');

  const listQuery = buildInvoiceListQuery({
    search: '10300',
    limit: 25000,
    offset: 0,
  });
  assert(listQuery.query.includes('address_match.display_name AS address_matched_customer_name'));
  assert(listQuery.query.includes("COALESCE(TRIM(i.customer_name), '') ~* '^(return|remit|payment stub)?$'"));
  assert(listQuery.query.includes('address_match.display_name ILIKE $1'));
  assert.deepStrictEqual(listQuery.params, ['%10300%', 25000, 0]);
}

if (typeof test === 'function') {
  test('mail invoice payload uses latest service date when Copilot invoice date is missing', runAssertions);
} else {
  runAssertions()
    .then(() => {
      console.log('mail-invoice-payload.test.js passed');
    })
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}
