const assert = require('assert');
const {
  parseCopilotPaymentsHtml,
  extractInvoiceNumberFromDetails,
  buildExternalPaymentKey,
} = require('../lib/copilot-payments');
const {
  computeTaxPortionCollected,
  choosePreferredInvoiceMatch,
  buildCopilotPaymentRecord,
  hydratePaymentRecord,
} = require('../scripts/import-copilot-payments');

let failures = 0;
function it(name, fn) {
  try {
    fn();
    console.log(`  \u2713 ${name}`);
  } catch (error) {
    failures += 1;
    console.error(`  \u2717 ${name}\n    ${error.message}`);
  }
}

console.log('copilot-payments');

const fixtureHtml = `
<html>
  <body>
    <div class="dataTables_info">1-100 of 165</div>
    <table>
      <thead>
        <tr>
          <th>Date</th>
          <th>Payer / Payee</th>
          <th>Amount</th>
          <th>Tip</th>
          <th>Method</th>
          <th>Details</th>
          <th>Notes</th>
        </tr>
      </thead>
      <tbody>
        <tr id="payment_10470">
          <td>Apr 17, 2026</td>
          <td><a href="/customers/details/1">Carol Horner</a></td>
          <td>$1,036.80</td>
          <td>$0.00</td>
          <td>Card</td>
          <td><a href="/finances/payments/view/abc123">$1,036.80 for Invoice #10470</a></td>
          <td>Paid online</td>
        </tr>
        <tr>
          <td>Apr 16, 2026</td>
          <td>Henrietta Pattantyus</td>
          <td>$297.00</td>
          <td>$10.00</td>
          <td>ACH</td>
          <td>$297.00 for Invoice #10096</td>
          <td></td>
        </tr>
        <tr>
          <td></td>
          <td>Page Total:</td>
          <td>$1,333.80</td>
          <td>$10.00</td>
          <td></td>
          <td></td>
          <td></td>
        </tr>
      </tbody>
    </table>
    <ul class="pagination">
      <li><a href="/finances/payments/?p=2&iop=100">2</a></li>
    </ul>
  </body>
</html>
`;

it('parses Copilot payments rows, totals, and trailing-slash pagination', () => {
  const parsed = parseCopilotPaymentsHtml(fixtureHtml, 'https://secure.copilotcrm.com/finances/payments');
  assert.strictEqual(parsed.total, 165);
  assert.strictEqual(parsed.payments.length, 2);
  assert.deepStrictEqual(parsed.page_paths, ['/finances/payments?p=2&iop=100']);
});

it('flags missing payments tables as parser warnings instead of a valid empty snapshot', () => {
  const parsed = parseCopilotPaymentsHtml('<html><body><h1>Login</h1></body></html>', 'https://secure.copilotcrm.com/finances/payments');
  assert.strictEqual(parsed.payments.length, 0);
  assert.strictEqual(parsed.parser_warning, 'Payments table not found');
});

it('extracts invoice linkage from details and preserves fields', () => {
  const parsed = parseCopilotPaymentsHtml(fixtureHtml, 'https://secure.copilotcrm.com/finances/payments');
  const first = parsed.payments[0];
  assert.strictEqual(first.customer_name, 'Carol Horner');
  assert.strictEqual(first.amount, 1036.8);
  assert.strictEqual(first.tip_amount, 0);
  assert.strictEqual(first.method, 'Card');
  assert.strictEqual(first.details, '$1,036.80 for Invoice #10470');
  assert.strictEqual(first.notes, 'Paid online');
  assert.strictEqual(first.extracted_invoice_number, '10470');
  assert.strictEqual(first.external_payment_key, 'row:payment_10470');
  assert.strictEqual(first.external_metadata.payment_path, '/finances/payments/view/abc123');
});

it('builds a stable hashed external payment key when no explicit identifier exists', () => {
  const first = buildExternalPaymentKey({
    dateText: 'Apr 16, 2026',
    customerName: 'Henrietta Pattantyus',
    amount: 297,
    tipAmount: 10,
    method: 'ACH',
    extractedInvoiceNumber: '10096',
  });
  const second = buildExternalPaymentKey({
    dateText: 'Apr 16, 2026',
    customerName: 'Henrietta Pattantyus',
    amount: 297,
    tipAmount: 10,
    method: 'ACH',
    extractedInvoiceNumber: '10096',
  });
  assert.strictEqual(first, second);
  assert.ok(first.startsWith('hash:'));
});

it('computes tax portion collected with tips excluded and invoice total cap applied', () => {
  assert.strictEqual(
    computeTaxPortionCollected({
      amount: 297,
      tip_amount: 10,
      invoice_total: 297,
      invoice_tax_amount: 17.82,
    }),
    17.22
  );

  assert.strictEqual(
    computeTaxPortionCollected({
      amount: 1200,
      tip_amount: 0,
      invoice_total: 1036.8,
      invoice_tax_amount: 62.21,
    }),
    62.21
  );
});

it('prefers Copilot-linked invoices when duplicates exist', () => {
  const preferred = choosePreferredInvoiceMatch([
    { id: 1, invoice_number: '10470', external_source: null, imported_at: null, updated_at: '2026-04-15T12:00:00Z' },
    { id: 2, invoice_number: '10470', external_source: 'copilotcrm', imported_at: '2026-04-17T12:00:00Z', updated_at: '2026-04-17T12:00:00Z' },
  ]);
  assert.strictEqual(preferred.id, 2);
});

it('builds linked payment reconciliation rows and hydrates computed fields', () => {
  const prepared = buildCopilotPaymentRecord({
    external_source: 'copilotcrm',
    external_payment_key: 'row:payment_10470',
    customer_name: 'Carol Horner',
    amount: 1036.8,
    tip_amount: 0,
    method: 'Card',
    details: '$1,036.80 for Invoice #10470',
    notes: 'Paid online',
    paid_at: '2026-04-17T12:00:00.000Z',
    source_date_raw: 'Apr 17, 2026',
    extracted_invoice_number: '10470',
    external_metadata: {},
  }, {
    id: 42,
    invoice_number: '10470',
    customer_id: 7,
    customer_name: 'Carol Horner',
    total: 1036.8,
    tax_amount: 62.21,
    external_source: 'copilotcrm',
  });
  const hydrated = hydratePaymentRecord({
    ...prepared,
    id: 9,
  });
  assert.strictEqual(prepared.payment_id, null);
  assert.strictEqual(prepared.invoice_id, 42);
  assert.strictEqual(prepared.tax_portion_collected, 62.21);
  assert.strictEqual(hydrated.applied_amount, 1036.8);
  assert.strictEqual(hydrated.tax_portion_collected, 62.21);
});

it('extracts invoice numbers directly from details strings', () => {
  assert.strictEqual(extractInvoiceNumberFromDetails('$172.80 for Invoice #10239'), '10239');
  assert.strictEqual(extractInvoiceNumberFromDetails('Paid in full'), null);
});

if (failures > 0) {
  console.error(`\n${failures} failure(s)`);
  process.exit(1);
} else {
  console.log('\nAll tests passed');
}
