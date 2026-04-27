const assert = require('assert');
const { renderMailInvoiceHtml } = require('../lib/invoice-mail-html');

function runAssertions() {
  const html = renderMailInvoiceHtml({
    id: 42,
    invoice_number: '10191',
    invoice_date_raw: 'Apr 03, 2026',
    customer_name: 'Bob Maclean',
    customer_address: '15646 Hocking Boulevard\nBrook Park OH 44142',
    property_address: '15646 Hocking Boulevard Brook Park OH, 44142',
    subtotal: 209,
    tax_amount: 16.4,
    total: 225.4,
    notes: 'Thank you for your business. Please return the slip with your payment.',
    line_items: [
      {
        service_date_raw: '2026-04-02',
        name: 'Fertilizing',
        rich_description: 'Early spring fertilization and pre-emergent crabgrass control.',
        quantity: 1,
        rate: 49,
        amount: 52.92,
      },
      {
        service_date_raw: '2026-04-27',
        name: 'Fuel Surcharge',
        description: '',
        quantity: 1,
        rate: 4,
        amount: 4,
      },
    ],
    metadata: {
      outstanding_balance: 18.23,
      this_invoice: 225.4,
      total_due_on_account: 243.63,
    },
  });

  assert(html.includes('Services Performed'));
  assert(!html.includes("What's Included"));
  assert(html.includes('Completed services billed for the current route cycle.'));
  assert(html.includes('Detach and return with payment'));
  assert(html.indexOf('Fertilizing') < html.indexOf('Fuel Surcharge'));

  const fuelSnippetStart = html.indexOf('Fuel Surcharge');
  const fuelSnippet = html.slice(Math.max(0, fuelSnippetStart - 180), fuelSnippetStart + 180);
  assert(!fuelSnippet.includes('Apr 27, 2026'));
  assert(html.includes('Apr 2, 2026') || html.includes('Apr 02, 2026'));
}

if (typeof test === 'function') {
  test('renderMailInvoiceHtml creates a compact invoice and stub layout', runAssertions);
} else {
  runAssertions();
  console.log('invoice-mail-html.test.js passed');
}
