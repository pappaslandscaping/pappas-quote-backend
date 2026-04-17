// Parser tests for the CopilotCRM invoice list HTML.
// Runs with `node tests/parse-copilot-invoices.test.js` — no Jest needed.

const assert = require('assert');
const { parseInvoiceListHtml, _internal } = require('../scripts/parse-copilot-invoices');

let failures = 0;
function it(name, fn) {
  try {
    fn();
    console.log(`  \u2713 ${name}`);
  } catch (e) {
    failures++;
    console.error(`  \u2717 ${name}\n    ${e.message}`);
  }
}

console.log('parse-copilot-invoices');

// ─── Helper unit tests ────────────────────────────────────────
it('parseMoney handles "$1,234.56"', () => {
  assert.strictEqual(_internal.parseMoney('$1,234.56'), 1234.56);
});
it('parseMoney handles negative and blank', () => {
  assert.strictEqual(_internal.parseMoney('-$100.00'), -100);
  assert.strictEqual(_internal.parseMoney(''), null);
  assert.strictEqual(_internal.parseMoney('—'), null);
});
it('mapStatus maps Paid in Full → paid', () => {
  assert.strictEqual(_internal.mapStatus('Paid in Full', 100, 100), 'paid');
});
it('mapStatus maps Pending → pending', () => {
  assert.strictEqual(_internal.mapStatus('Pending', 0, 100, 'not sent'), 'pending');
});
it('mapStatus infers partial when amount_paid < total', () => {
  assert.strictEqual(_internal.mapStatus('Pending', 40, 100, 'sent'), 'partial');
});
it('mapStatus maps Void', () => {
  assert.strictEqual(_internal.mapStatus('Void', 0, 100), 'void');
});

// ─── Fixture mirroring CopilotCRM's invoice list rows ─────────
// Three rows: a paid one, a partial one, and one with mailto: + property
// stacked over an address.
const fixtureHtml = `
<table>
  <thead>
    <tr>
      <th>Invoice #</th>
      <th>Date</th>
      <th>Customer</th>
      <th>Property</th>
      <th>Crew</th>
      <th>Tax</th>
      <th>Total</th>
      <th>Due</th>
      <th>Paid</th>
      <th>Credit</th>
      <th>Status</th>
      <th>Sent</th>
    </tr>
  </thead>
  <tbody>
    <tr id="invoice_101">
      <td><a href="/finances/invoices/view/101">INV-1001</a></td>
      <td>Aug 12, 2025</td>
      <td>
        <a href="/customers/details/55">Jane Smith</a><br>
        <a href="mailto:jane@example.com">jane@example.com</a>
      </td>
      <td>Smith Residence<br>123 Main St, Bay Village OH 44140</td>
      <td>Crew A</td>
      <td>$8.00</td>
      <td>$108.00</td>
      <td>$0.00</td>
      <td>$108.00</td>
      <td>$0.00</td>
      <td>Paid in Full</td>
      <td>Yes</td>
    </tr>
    <tr id="invoice_102">
      <td><a href="/finances/invoices/view/102">INV-1002</a></td>
      <td>09/03/2025</td>
      <td>
        <a href="/customers/details/77">Bob Jones</a>
        bob.jones@example.com
      </td>
      <td>Jones Property<br>9 Oak Ave</td>
      <td>Crew B</td>
      <td>$0.00</td>
      <td>$200.00</td>
      <td>$120.00</td>
      <td>$80.00</td>
      <td>$0.00</td>
      <td>Pending</td>
      <td>No</td>
    </tr>
    <tr id="invoice_103">
      <td><a href="/finances/invoices/view/103">INV-1003</a></td>
      <td>Sep 15, 2025</td>
      <td><a href="/customers/details/91">Pat Doe</a></td>
      <td>Doe Home</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Void</td>
      <td>No</td>
    </tr>
  </tbody>
</table>
`;

const rows = parseInvoiceListHtml({ html: fixtureHtml, isEmpty: false });

it('parses three rows', () => {
  assert.strictEqual(rows.length, 3);
});

it('row 1: paid, with property name + address split, mailto email', () => {
  const r = rows[0];
  assert.strictEqual(r.external_invoice_id, '101');
  assert.strictEqual(r.invoice_number, 'INV-1001');
  assert.strictEqual(r.invoice_date, '2025-08-12');
  assert.strictEqual(r.customer_name, 'Jane Smith');
  assert.strictEqual(r.customer_email, 'jane@example.com');
  assert.strictEqual(r.copilot_customer_id, '55');
  assert.strictEqual(r.property_name, 'Smith Residence');
  assert.strictEqual(r.property_address, '123 Main St, Bay Village OH 44140');
  assert.strictEqual(r.tax_amount, 8);
  assert.strictEqual(r.total, 108);
  assert.strictEqual(r.amount_paid, 108);
  assert.strictEqual(r.status, 'paid');
  assert.strictEqual(r.view_path, '/finances/invoices/view/101');
});

it('row 2: pending with partial payment → status partial, plain-text email', () => {
  const r = rows[1];
  assert.strictEqual(r.external_invoice_id, '102');
  assert.strictEqual(r.invoice_number, 'INV-1002');
  assert.strictEqual(r.customer_name, 'Bob Jones');
  assert.strictEqual(r.customer_email, 'bob.jones@example.com');
  assert.strictEqual(r.copilot_customer_id, '77');
  assert.strictEqual(r.property_address, '9 Oak Ave');
  assert.strictEqual(r.total, 200);
  assert.strictEqual(r.amount_paid, 80);
  assert.strictEqual(r.status, 'partial');
  assert.strictEqual(r.sent_status, 'not sent');
});

it('row 3: void status, single-line property (no address split), no email, blank money cells → null', () => {
  const r = rows[2];
  assert.strictEqual(r.external_invoice_id, '103');
  assert.strictEqual(r.status, 'void');
  assert.strictEqual(r.customer_email, null);
  assert.strictEqual(r.property_name, 'Doe Home');
  assert.strictEqual(r.property_address, null);
  assert.strictEqual(r.tax_amount, null);
  assert.strictEqual(r.total, null);
  assert.strictEqual(r.amount_paid, null);
});

it('toDbValues from importer maps subtotal = total - tax', () => {
  const { toDbValues } = require('../scripts/import-copilot-invoices');
  const v = toDbValues(rows[0], null);
  assert.strictEqual(v.subtotal, 100); // 108 - 8
  assert.strictEqual(v.total, 108);
  assert.strictEqual(v.amount_paid, 108);
  assert.strictEqual(v.metadata.copilot_customer_id, '55');
  assert.strictEqual(v.metadata.view_path, '/finances/invoices/view/101');
});

it('accepts a raw HTML string (not just JSON payload)', () => {
  const r = parseInvoiceListHtml(fixtureHtml);
  assert.strictEqual(r.length, 3);
});

it('accepts a row-only fragment (no <table>/<thead>) using positional fallback', () => {
  const fragment = `
    <tr id="invoice_999">
      <td><a href="/finances/invoices/view/999">INV-9999</a></td>
      <td>Oct 1, 2025</td>
      <td><a href="/customers/details/12">A B</a></td>
      <td>P</td>
      <td>C</td>
      <td>$0</td>
      <td>$50</td>
      <td>$0</td>
      <td>$50</td>
      <td>$0</td>
      <td>Paid</td>
      <td>Yes</td>
    </tr>`;
  const r = parseInvoiceListHtml(fragment);
  assert.strictEqual(r.length, 1);
  assert.strictEqual(r[0].external_invoice_id, '999');
  assert.strictEqual(r[0].invoice_number, 'INV-9999');
  assert.strictEqual(r[0].total, 50);
});

it('does not store a date label in invoice_number when the invoice cell is misaligned', () => {
  const fragment = `
    <tr id="invoice_777">
      <td><input type="checkbox"></td>
      <td>Apr 08, 2026</td>
      <td><a href="/finances/invoices/view/777">777</a></td>
      <td><a href="/customers/details/12">Test Customer</a></td>
      <td>$50.00</td>
      <td>Pending</td>
      <td>Not Sent</td>
    </tr>`;
  const r = parseInvoiceListHtml(fragment);
  assert.strictEqual(r.length, 1);
  assert.strictEqual(r[0].invoice_number, '777');
  assert.strictEqual(r[0].view_path, '/finances/invoices/view/777');
});

it('splits name and email when Copilot puts both inside the same customer link', () => {
  const fragment = `
    <table>
      <thead>
        <tr>
          <th>Ignore</th>
          <th>Invoice #</th>
          <th>Date</th>
          <th>Customer</th>
          <th>Ignore</th>
          <th>Property</th>
          <th>Address</th>
          <th>Crew</th>
          <th>Tax</th>
          <th>Total</th>
          <th>Due</th>
          <th>Paid</th>
          <th>Credit</th>
          <th>Status</th>
          <th>Sent</th>
        </tr>
      </thead>
      <tbody>
        <tr id="2664261">
          <td><input type="checkbox" value="2664261"></td>
          <td><a href="/finances/invoices/view/2664261">10252</a></td>
          <td>Apr 06, 2026</td>
          <td>
            <a href="/customers/details/1053980">
              Henrietta Pattantyus
              <br>
              henrijeff12@gmail.com
            </a>
          </td>
          <td></td>
          <td><p>6154 Sylvia Drive</p></td>
          <td><p>6154 Sylvia Drive Brook Park, OH 44142 US</p></td>
          <td>Tim Mowing Crew, Tim Mowing Crew</td>
          <td>$7.76</td>
          <td>$104.76</td>
          <td>$104.76</td>
          <td>$0.00</td>
          <td>$0.00</td>
          <td>Pending</td>
          <td><span class="badge badge-danger">Not Sent</span></td>
        </tr>
      </tbody>
    </table>`;
  const r = parseInvoiceListHtml(fragment);
  assert.strictEqual(r.length, 1);
  assert.strictEqual(r[0].invoice_number, '10252');
  assert.strictEqual(r[0].customer_name, 'Henrietta Pattantyus');
  assert.strictEqual(r[0].customer_email, 'henrijeff12@gmail.com');
});
it('preserves a not-sent pending invoice as pending instead of sent', () => {
  const fragment = `
    <tr id="invoice_10448">
      <td><a href="/finances/invoices/view/10448">10448</a></td>
      <td>Jul 15, 2024</td>
      <td><a href="/customers/details/237">Nance Gorman</a></td>
      <td>456 Lake Rd</td>
      <td>Crew A</td>
      <td>$3.36</td>
      <td>$45.36</td>
      <td>$45.36</td>
      <td>$0.00</td>
      <td>$0.00</td>
      <td>Pending</td>
      <td>Not Sent</td>
    </tr>`;
  const r = parseInvoiceListHtml(fragment);
  assert.strictEqual(r.length, 1);
  assert.strictEqual(r[0].invoice_number, '10448');
  assert.strictEqual(r[0].status, 'pending');
  assert.strictEqual(r[0].sent_status, 'not sent');
});

if (failures > 0) {
  console.error(`\n${failures} test(s) failed.`);
  process.exit(1);
} else {
  console.log('\nAll parser tests passed.');
}
