const assert = require('assert');
const { parseInvoiceListHtml } = require('../scripts/import-copilot-invoices.js');

const sampleHtml = `
<table>
  <tr id="2726976">
    <td><input type="checkbox"></td>
    <td><a href="/finances/invoices/view/2726976">10448</a></td>
    <td>Apr 16, 2026</td>
    <td><a href="/customers/details/1053890">Nance Gorman<br>ravennamiceli@yahoo.com</a></td>
    <td></td>
    <td><p>285 Glen Park Drive</p></td>
    <td><p>285 Glen Park Drive Bay Village, OH 44140 US</p></td>
    <td>Mowing</td>
    <td>$3.36</td>
    <td>$45.36</td>
    <td>$45.36</td>
    <td>$0.00</td>
    <td>$0.00</td>
    <td>Pending</td>
    <td><span class="badge badge-danger">Not Sent</span></td>
    <td></td>
  </tr>
  <tr id="2716506">
    <td><input type="checkbox"></td>
    <td><a href="/finances/invoices/view/2716506">10447</a></td>
    <td>Apr 14, 2026</td>
    <td><a href="/customers/details/1053576">John Estep<br>jle58@icloud.com</a></td>
    <td></td>
    <td><p>14859 Alger Road</p></td>
    <td><p>14859 Alger Road Cleveland, OH 44111 US</p></td>
    <td>Tim Mowing Crew</td>
    <td>$2.88</td>
    <td>$38.88</td>
    <td>$27.56</td>
    <td>$11.32</td>
    <td>$0.00</td>
    <td>Partial</td>
    <td><span class="badge badge-danger">Not Sent</span></td>
    <td></td>
  </tr>
</table>
`;

const invoices = parseInvoiceListHtml(sampleHtml);

assert.strictEqual(invoices.length, 2);
assert.strictEqual(invoices[0].external_invoice_id, '2726976');
assert.strictEqual(invoices[0].invoice_number, '10448');
assert.strictEqual(invoices[0].customer_name, 'Nance Gorman');
assert.strictEqual(invoices[0].customer_email, 'ravennamiceli@yahoo.com');
assert.strictEqual(invoices[0].external_customer_id, '1053890');
assert.strictEqual(invoices[0].tax_amount, 3.36);
assert.strictEqual(invoices[0].total, 45.36);
assert.strictEqual(invoices[0].subtotal, 42);
assert.strictEqual(invoices[0].sent_status, 'Not Sent');

assert.strictEqual(invoices[1].invoice_number, '10447');
assert.strictEqual(invoices[1].status, 'Partial');
assert.strictEqual(invoices[1].amount_paid, 11.32);
assert.strictEqual(invoices[1].total_due, 27.56);

console.log('parse-copilot-invoices.test.js passed');
