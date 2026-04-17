const assert = require('assert');
const {
  parseCopilotPaymentsHtml,
  extractInvoiceNumberFromDetails,
  extractInvoiceDateFromDetails,
  buildExternalPaymentKey,
} = require('../lib/copilot-payments');
const {
  deriveInvoiceTaxableGrossTotal,
  computeTaxPortionCollected,
  choosePreferredInvoiceMatch,
  chooseFallbackInvoiceMatch,
  buildCopilotPaymentRecord,
  getExtractedInvoiceNumberForPayment,
  describeCopilotPaymentLinkage,
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
  assert.strictEqual(first.extracted_invoice_date, null);
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

it('derives taxable gross totals from taxed line items when available', () => {
  assert.strictEqual(
    deriveInvoiceTaxableGrossTotal({
      line_items: [
        { line_total: 216, tax_percent: 8 },
        { line_total: 6.76, tax_percent: 0 },
      ],
    }),
    216
  );
  assert.strictEqual(
    deriveInvoiceTaxableGrossTotal({
      external_metadata: { invoice_taxable_gross_total: 297 },
    }),
    297
  );
});

it('uses taxable gross totals instead of fee-inflated invoice totals when reconstructing tax', () => {
  assert.strictEqual(
    computeTaxPortionCollected({
      amount: 216,
      tip_amount: 0,
      invoice_total: 222.76,
      invoice_tax_amount: 16,
      invoice_taxable_gross_total: 216,
    }),
    16
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
    line_items: [
      { line_total: 1036.8, tax_percent: 8 },
    ],
    external_source: 'copilotcrm',
  });
  const hydrated = hydratePaymentRecord({
    ...prepared,
    id: 9,
  });
  assert.strictEqual(prepared.payment_id, null);
  assert.strictEqual(prepared.invoice_id, 42);
  assert.strictEqual(prepared.tax_portion_collected, 62.21);
  assert.strictEqual(prepared.external_metadata.invoice_taxable_gross_total, 1036.8);
  assert.strictEqual(hydrated.applied_amount, 1036.8);
  assert.strictEqual(hydrated.tax_portion_collected, 62.21);
});

it('hydrates card payments against taxable gross totals instead of full invoice totals', () => {
  const hydrated = hydratePaymentRecord({
    amount: 216,
    tip_amount: 0,
    invoice_total: 222.76,
    invoice_tax_amount: 16,
    external_metadata: { invoice_taxable_gross_total: 216 },
  });
  assert.strictEqual(hydrated.applied_amount, 216);
  assert.strictEqual(hydrated.tax_portion_collected, 16);
});

it('extracts invoice numbers directly from details strings', () => {
  assert.strictEqual(extractInvoiceNumberFromDetails('$172.80 for Invoice #10239'), '10239');
  assert.strictEqual(extractInvoiceNumberFromDetails('Paid in full'), null);
});

it('extracts invoice dates directly from payment details strings', () => {
  assert.strictEqual(
    extractInvoiceDateFromDetails('$9.60 for Invoice #9835 Sep 19, 2025 Payment Added Apr 16, 2026 9:14 pm by Theresa Pappas (Linda Scamaldo)'),
    '2025-09-19'
  );
  assert.strictEqual(
    extractInvoiceDateFromDetails('$297.00 for Invoice #10096 Mar 20, 2026 Invoice#10096'),
    '2026-03-20'
  );
  assert.strictEqual(extractInvoiceDateFromDetails('Paid in full'), null);
});

it('prefers a date-like fallback invoice match for older bad imports', () => {
  const chosen = chooseFallbackInvoiceMatch([
    { id: 1, invoice_number: '9239', external_source: 'copilotcrm', imported_at: '2026-04-17T00:00:00Z', updated_at: '2026-04-17T00:00:00Z' },
    { id: 2, invoice_number: 'Sep 19, 2025', external_source: 'copilotcrm', imported_at: '2026-04-17T00:00:00Z', updated_at: '2026-04-17T00:00:00Z' },
  ], '2025-09-19');
  assert.strictEqual(chosen.id, 2);
});

it('prefers stored extracted invoice numbers over reparsing details', () => {
  assert.strictEqual(
    getExtractedInvoiceNumberForPayment({
      details: '$172.80 for Invoice #10239',
      external_metadata: { extracted_invoice_number: '10470' },
    }),
    '10470'
  );
});

it('describes unresolved payments missing invoice numbers', () => {
  const linkage = describeCopilotPaymentLinkage({
    invoice_id: null,
    details: 'Paid in full',
    external_metadata: {},
  });
  assert.deepStrictEqual(linkage, {
    link_status: 'unresolved',
    extracted_invoice_number: null,
    link_failure_reason: 'No invoice number found in Copilot payment details.',
  });
});

it('describes unresolved payments whose invoice does not exist locally', () => {
  const linkage = describeCopilotPaymentLinkage({
    invoice_id: null,
    details: '$172.80 for Invoice #10239',
    external_metadata: {},
  });
  assert.deepStrictEqual(linkage, {
    link_status: 'unresolved',
    extracted_invoice_number: '10239',
    link_failure_reason: 'Invoice #10239 was not found in YardDesk.',
  });
});

it('describes stale unresolved payments when the invoice exists locally', () => {
  const linkage = describeCopilotPaymentLinkage({
    invoice_id: null,
    details: '$172.80 for Invoice #10239',
    external_metadata: {},
  }, {
    id: 77,
    invoice_number: '10239',
  });
  assert.deepStrictEqual(linkage, {
    link_status: 'unresolved',
    extracted_invoice_number: '10239',
    link_failure_reason: 'Invoice #10239 exists in YardDesk, but this payment row is still unresolved.',
  });
});

if (failures > 0) {
  console.error(`\n${failures} failure(s)`);
  process.exit(1);
} else {
  console.log('\nAll tests passed');
}
