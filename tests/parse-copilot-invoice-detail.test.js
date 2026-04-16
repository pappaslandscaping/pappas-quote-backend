// Detail-page parser tests for the CopilotCRM invoice view page.
// Runs with `node tests/parse-copilot-invoice-detail.test.js` — no Jest.
//
// The fixture mirrors the documented selectors from the real page
// (#inv_id, #inv_cust_id, .table--description, .table--sub-total,
// .inv-notes-container, .inv-terms-table). Once you capture the actual
// HTML for invoice 10448, swap the inline string for `fs.readFileSync`
// and the assertions should still pass — the parser keys off the same
// containers.

const assert = require('assert');
const { parseInvoiceDetailHtml } = require('../scripts/parse-copilot-invoice-detail');
const { toDbValuesFromDetail } = require('../scripts/import-copilot-invoices');

let failures = 0;
function it(name, fn) {
  try { fn(); console.log(`  \u2713 ${name}`); }
  catch (e) { failures++; console.error(`  \u2717 ${name}\n    ${e.message}`); }
}

console.log('parse-copilot-invoice-detail');

// ── Fixture for invoice 10448 / Nance Gorman ────────────────────
// Subtotal: $42.00, Tax: $3.36 (8%), Total: $45.36, Total Due: $45.36.
// Two line items: Mowing (Bi-Weekly) $35 + Fuel Surcharge $7.
const fixture10448 = `
<!DOCTYPE html>
<html>
<head><title>Invoice #10448 — CopilotCRM</title></head>
<body>
  <input type="hidden" id="inv_id" value="10448">
  <input type="hidden" id="inv_cust_id" value="237">
  <input type="hidden" id="invoice_number" value="10448">
  <input type="hidden" id="invoice_date" value="2024-07-15">
  <input type="hidden" id="due_date" value="2024-08-14">
  <input type="hidden" id="sent_status" value="Not Sent">

  <div class="invoice-header">
    <h1>Invoice #10448</h1>
  </div>

  <div class="bill-to">
    <div class="customer-name"><a href="/customers/details/237">Nance Gorman</a></div>
    <div class="customer-address">456 Lake Rd, Bay Village OH 44140</div>
    <a href="mailto:nance@example.com">nance@example.com</a>
  </div>

  <table class="table--description">
    <thead>
      <tr>
        <th>Service Date</th>
        <th>Description</th>
        <th>Rate</th>
        <th>Qty</th>
        <th>Hours</th>
        <th>Tax %</th>
        <th>Total</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>Jul 12, 2024</td>
        <td>Mowing (Bi-Weekly)</td>
        <td>$35.00</td>
        <td>1</td>
        <td>0.50</td>
        <td>8%</td>
        <td>$35.00</td>
      </tr>
      <tr>
        <td>Jul 12, 2024</td>
        <td>Fuel Surcharge</td>
        <td>$7.00</td>
        <td>1</td>
        <td></td>
        <td>8%</td>
        <td>$7.00</td>
      </tr>
    </tbody>
  </table>

  <table class="table--sub-total">
    <tr><td>Subtotal</td><td>$42.00</td></tr>
    <tr><td>Tax</td><td>$3.36</td></tr>
    <tr><td>Total</td><td>$45.36</td></tr>
    <tr><td>Amount Paid</td><td>$0.00</td></tr>
    <tr><td>Total Due</td><td>$45.36</td></tr>
  </table>

  <div class="inv-notes-container">Thanks for your business!</div>

  <table class="inv-terms-table">
    <tr><td>Net 30. Late fees apply after 30 days.</td></tr>
  </table>
</body>
</html>`;

const detail = parseInvoiceDetailHtml(fixture10448);

it('reads identity from #inv_id and #inv_cust_id', () => {
  assert.strictEqual(detail.external_invoice_id, '10448');
  assert.strictEqual(detail.copilot_customer_id, '237');
});

it('invoice_number = 10448', () => {
  assert.strictEqual(detail.invoice_number, '10448');
});

it('customer_name = Nance Gorman', () => {
  assert.strictEqual(detail.customer_name, 'Nance Gorman');
});

it('totals: subtotal 42.00, tax 3.36, total 45.36, total_due 45.36', () => {
  assert.strictEqual(detail.subtotal, 42);
  assert.strictEqual(detail.tax_amount, 3.36);
  assert.strictEqual(detail.total, 45.36);
  assert.strictEqual(detail.total_due, 45.36);
  assert.strictEqual(detail.amount_paid, 0);
  assert.strictEqual(detail.due_date, '2024-08-14');
});

it('parses 2 line items including Mowing (Bi-Weekly) and Fuel Surcharge', () => {
  assert.strictEqual(detail.line_items.length, 2);
  const descriptions = detail.line_items.map(it => it.description);
  assert.deepStrictEqual(descriptions, ['Mowing (Bi-Weekly)', 'Fuel Surcharge']);
});

it('first line item: Mowing — rate $35, qty 1, hours 0.5, tax 8%, total $35', () => {
  const item = detail.line_items[0];
  assert.strictEqual(item.description, 'Mowing (Bi-Weekly)');
  assert.strictEqual(item.rate, 35);
  assert.strictEqual(item.quantity, 1);
  assert.strictEqual(item.budgeted_hours, 0.5);
  assert.strictEqual(item.tax_percent, 8);
  assert.strictEqual(item.line_total, 35);
  assert.strictEqual(item.service_date, '2024-07-12');
});

it('second line item: Fuel Surcharge — rate $7, total $7, no hours', () => {
  const item = detail.line_items[1];
  assert.strictEqual(item.description, 'Fuel Surcharge');
  assert.strictEqual(item.rate, 7);
  assert.strictEqual(item.line_total, 7);
  assert.strictEqual(item.budgeted_hours, null);
});

it('captures customer email from mailto:', () => {
  assert.strictEqual(detail.customer_email, 'nance@example.com');
});

it('captures customer address', () => {
  assert.strictEqual(detail.customer_address, '456 Lake Rd, Bay Village OH 44140');
});

it('captures notes and terms', () => {
  assert.strictEqual(detail.notes, 'Thanks for your business!');
  assert.strictEqual(detail.terms, 'Net 30. Late fees apply after 30 days.');
});

it('preserves sent status separately from invoice status', () => {
  assert.strictEqual(detail.sent_status, 'not sent');
  assert.strictEqual(detail.status, 'pending');
});

it('toDbValuesFromDetail builds importer-ready row', () => {
  const v = toDbValuesFromDetail(detail, null);
  assert.strictEqual(v.external_invoice_id, '10448');
  assert.strictEqual(v.invoice_number, '10448');
  assert.strictEqual(v.subtotal, 42);
  assert.strictEqual(v.tax_amount, 3.36);
  assert.strictEqual(v.total, 45.36);
  assert.strictEqual(v.amount_paid, 0);
  assert.strictEqual(v.due_date, '2024-08-14');
  assert.strictEqual(v.notes, 'Thanks for your business!');
  assert.strictEqual(v.terms, 'Net 30. Late fees apply after 30 days.');
  assert.strictEqual(v.sent_status, 'not sent');
  assert.strictEqual(v.line_items.length, 2);
  assert.strictEqual(v.metadata.copilot_customer_id, '237');
  assert.strictEqual(v.metadata.terms_raw, 'Net 30. Late fees apply after 30 days.');
});

it('handles a partially-paid detail page (status inferred as partial)', () => {
  const partialFixture = fixture10448.replace(
    '<tr><td>Amount Paid</td><td>$0.00</td></tr>',
    '<tr><td>Amount Paid</td><td>$20.00</td></tr>'
  );
  const d = parseInvoiceDetailHtml(partialFixture);
  assert.strictEqual(d.amount_paid, 20);
  assert.strictEqual(d.status, 'partial');
});

it('survives a stripped-down detail page that only carries totals + line items', () => {
  const lean = `
    <input type="hidden" id="inv_id" value="999">
    <table class="table--description">
      <tbody>
        <tr><td>Jan 1, 2024</td><td>Service</td><td>$10</td><td>1</td><td></td><td>0%</td><td>$10</td></tr>
      </tbody>
    </table>
    <table class="table--sub-total">
      <tr><td>Subtotal</td><td>$10.00</td></tr>
      <tr><td>Total</td><td>$10.00</td></tr>
    </table>`;
  const d = parseInvoiceDetailHtml(lean);
  assert.strictEqual(d.external_invoice_id, '999');
  assert.strictEqual(d.invoice_number, '999'); // falls back to external id
  assert.strictEqual(d.subtotal, 10);
  assert.strictEqual(d.total, 10);
  assert.strictEqual(d.line_items.length, 1);
  assert.strictEqual(d.line_items[0].description, 'Service');
});

if (failures > 0) {
  console.error(`\n${failures} test(s) failed.`);
  process.exit(1);
} else {
  console.log('\nAll detail-parser tests passed.');
}
