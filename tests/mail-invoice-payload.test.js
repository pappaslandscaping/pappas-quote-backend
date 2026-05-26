const assert = require('assert');
const createInvoiceRoutes = require('../routes/invoices');

createInvoiceRoutes({
  pool: {
    query: async () => ({
      rows: [{
        id: 2384,
        display_name: 'Superior Industrial',
        customer_number: '2384',
        address_fingerprint: '3855west150thstreetclevelandoh44111',
      }, {
        id: 2385,
        display_name: 'Monta Demchak',
        customer_number: '1054031',
        address_fingerprint: '14015saintjamesavenueclevelandoh44135',
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
  resolveMailInvoiceNumber,
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
    lineItems: [],
  });
  assert.strictEqual(explicitCopilotDate, '2026-04-30');

  const createdAtFallback = buildMailInvoicePayload(superiorIndustrialInvoice({ line_items: [] }));
  assert.strictEqual(createdAtFallback.invoice_date_raw, 'Apr 7, 2026');

  const dateLikeInvoiceNumberPayload = buildMailInvoicePayload(superiorIndustrialInvoice({
    id: 14858,
    invoice_number: 'Apr 04, 2026',
    external_invoice_id: '2661850',
    external_metadata: {
      invoice_number: '10246',
      invoice_date: '2026-04-04',
    },
  }));
  assert.strictEqual(
    resolveMailInvoiceNumber({
      row: { invoice_number: 'Apr 04, 2026', external_invoice_id: '2661850' },
      metadata: { invoice_number: '10246' },
    }),
    '10246'
  );
  assert.strictEqual(dateLikeInvoiceNumberPayload.invoice_number, '10246');
  assert.strictEqual(dateLikeInvoiceNumberPayload.invoice_date_raw, 'Apr 28, 2026');

  const afroditaPayload = buildMailInvoicePayload(superiorIndustrialInvoice({
    id: 16011,
    invoice_number: '10494',
    created_at: '2026-04-15T04:00:00.000Z',
    customer_name: 'Afrodita Constantinidis',
    customer_address: '2205 Richland Avenue\nLakewood, OH 44107',
    external_metadata: {
      invoice_date: '2026-04-15',
      invoice_number: '10494',
    },
    line_items: [
      { description: 'Mowing (Bi-Weekly)', service_date: '2026-04-15', quantity: 1, rate: 42, line_total: 45.36 },
      { description: 'Mowing (Bi-Weekly)', service_date: '2026-04-30', quantity: 1, rate: 42, line_total: 45.36 },
      { description: 'Fuel Surcharge', service_date: '2026-04-30', quantity: 1, rate: 8, line_total: 8 },
    ],
  }));
  assert.strictEqual(afroditaPayload.invoice_date_raw, 'Apr 30, 2026');

  const marilynPayload = buildMailInvoicePayload(superiorIndustrialInvoice({
    id: 2009,
    invoice_number: '10424',
    created_at: '2026-04-13T04:00:00.000Z',
    customer_name: 'Marilyn Conrad',
    customer_address: '13761 Dalebrook Avenue\nBrook Park, OH 44142',
    external_metadata: {
      invoice_date: '2026-04-13',
      invoice_number: '10424',
    },
    line_items: [
      { description: 'Mowing (Bi-Weekly)', service_date: '2026-04-13', quantity: 1, rate: 55, line_total: 59.4 },
      { description: 'Mowing (Bi-Weekly)', service_date: '2026-04-27', quantity: 1, rate: 55, line_total: 59.4 },
      { description: 'Fuel Surcharge', service_date: '2026-04-27', quantity: 1, rate: 8, line_total: 8 },
    ],
  }));
  assert.strictEqual(marilynPayload.invoice_date_raw, 'Apr 27, 2026');

  const joanPayload = buildMailInvoicePayload(superiorIndustrialInvoice({
    id: 14764,
    invoice_number: '10437',
    created_at: '2026-04-13T04:00:00.000Z',
    customer_name: 'Joan Sapara',
    customer_address: '11508 Linnet Avenue\nCleveland, OH 44111',
    subtotal: '156.00',
    tax_amount: '11.52',
    total: '167.52',
    amount_paid: '12.12',
    external_metadata: {
      invoice_date: '2026-04-13',
      invoice_number: '10437',
      prior_balance: 155.4,
      outstanding_balance: 155.4,
      customer_outstanding_balance: 155.4,
      total_due_on_account: 322.92,
    },
    line_items: [
      { description: 'Mowing (Weekly)', service_date: '2026-04-06', quantity: 1, rate: 36, line_total: 38.88, tax_percent: 8 },
      { description: 'Mowing (Weekly)', service_date: '2026-04-13', quantity: 1, rate: 36, line_total: 38.88, tax_percent: 8 },
      { description: 'Mowing (Weekly)', service_date: '2026-04-20', quantity: 1, rate: 36, line_total: 38.88, tax_percent: 8 },
      { description: 'Mowing (Weekly)', service_date: '2026-04-27', quantity: 1, rate: 36, line_total: 38.88, tax_percent: 8 },
      { description: 'Fuel Surcharge', service_date: '2026-04-28', quantity: 1, rate: 12, line_total: 12 },
    ],
  }));
  assert.strictEqual(joanPayload.invoice_date_raw, 'Apr 27, 2026');
  assert.strictEqual(joanPayload.metadata.payment_credit, 12.12);
  assert.strictEqual(joanPayload.metadata.outstanding_balance, 0);
  assert.strictEqual(joanPayload.metadata.this_invoice, 155.4);
  assert.strictEqual(joanPayload.metadata.total_due_on_account, 155.4);

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

  const [copilotCustomerMatchedInvoice] = await attachMailCustomerMatches([superiorIndustrialInvoice({
    customer_id: null,
    customer_name: 'Return',
    customer_address: '6237 Vandemark Rd\nMedina, OH 44256',
    external_metadata: {
      customer_name: 'Return',
      copilot_customer_id: '1054031',
      property_address: '14015 Saint James Avenue',
    },
  })]);
  const copilotCustomerMatchedPayload = buildMailInvoicePayload(copilotCustomerMatchedInvoice);

  assert.strictEqual(copilotCustomerMatchedPayload.customer_id, 2385);
  assert.strictEqual(copilotCustomerMatchedPayload.customer_name, 'Monta Demchak');
  assert.strictEqual(copilotCustomerMatchedPayload.metadata.customer_name, 'Monta Demchak');

  const listQuery = buildInvoiceListQuery({
    search: '10300',
    limit: 25000,
    offset: 0,
  });
  assert(listQuery.query.includes('address_match.display_name AS address_matched_customer_name'));
  assert(listQuery.query.includes("COALESCE(TRIM(i.customer_name), '') ~* '^(return|remit|payment stub)?$'"));
  assert(listQuery.query.includes('address_match.display_name ILIKE $1'));
  assert(listQuery.query.includes("i.external_metadata->>'invoice_number' ILIKE $1"));
  assert(listQuery.query.includes("TRIM(i.invoice_number) = TRIM(i.external_invoice_id)"));
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
