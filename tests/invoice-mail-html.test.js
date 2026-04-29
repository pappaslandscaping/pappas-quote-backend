const assert = require('assert');
const { renderMailInvoiceHtml } = require('../lib/invoice-mail-html');

function runAssertions() {
  const html = renderMailInvoiceHtml({
    id: 42,
    invoice_number: '10191',
    invoice_date_raw: 'Apr 03, 2026',
    customer_name: 'Bob Maclean',
    customer_address: '17894 Clifton Park Lane\nLakewood OH 44107',
    property_address: '15646 Hocking Boulevard Brook Park OH, 44142',
    subtotal: 209,
    tax_amount: 16.4,
    total: 225.4,
    notes: 'Other Services Available Need additional help around the property?',
    line_items: [
      {
        service_date_raw: '2026-04-02',
        name: 'Spring Cleanup',
        rich_description: 'Front and rear bed cleanup.',
        quantity: 1,
        rate: 250,
        total: 250,
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
  assert(!html.includes('Other Services Available'));
  assert(html.includes('Please return the remittance stub with your payment. Make checks payable to Pappas &amp; Co. Landscaping.'));
  assert(html.includes('class="window-recipient"'));
  assert(!html.includes('<div class="eyebrow">Mail To</div>'));
  assert(html.includes('left: 0.40in;'));
  assert(html.includes('top: 2.16in;'));
  assert(html.includes('height: 1.04in;'));
  assert(html.includes('class="stub-panel"'));
  assert(html.includes('height: 3.875in;'));
  assert(html.includes('class="stub-address-window stub-remit-window"'));
  assert(html.includes('left: 0.50in;'));
  assert(html.includes('bottom: 2.13in;'));
  assert(html.includes('width: 3.24in;'));
  assert(html.includes('class="stub-address-window stub-customer-window"'));
  assert(html.includes('bottom: 0.50in;'));
  assert(html.includes('width: 3.72in;'));
  assert(html.indexOf('Spring Cleanup') < html.indexOf('Fuel Surcharge'));
  assert(html.includes('$250.00'));

  const fuelSnippetStart = html.indexOf('Fuel Surcharge');
  const fuelSnippet = html.slice(Math.max(0, fuelSnippetStart - 180), fuelSnippetStart + 180);
  assert(!fuelSnippet.includes('Apr 27, 2026'));
  assert(html.includes('Apr 2, 2026') || html.includes('Apr 02, 2026'));
  assert(html.includes('17894 Clifton Park Lane'));
  assert(html.includes('Lakewood, OH 44107'));

  const directOutstandingHtml = renderMailInvoiceHtml({
    id: 43,
    invoice_number: '10300',
    invoice_date_raw: 'Apr 07, 2026',
    customer_name: 'Superior Industrial',
    customer_address: '3855 West 150th Street\nCleveland, OH 44111',
    property_address: '3855 West 150th Street\nCleveland, OH 44111',
    subtotal: 180,
    tax_amount: 14.08,
    total: 194.08,
    notes: '',
    line_items: [
      {
        service_date_raw: '2026-04-07',
        name: 'Landscape Service',
        description: 'Weekly service.',
        quantity: 1,
        rate: 180,
        total: 194.08,
      },
    ],
    metadata: {
      outstanding_balance: 118.8,
      this_invoice: 194.08,
      total_due_on_account: 312.88,
    },
  });

  assert(directOutstandingHtml.includes('Outstanding Balance'));
  assert(directOutstandingHtml.includes('$118.80'));

  const totalDueStoredAsOutstandingHtml = renderMailInvoiceHtml({
    id: 44,
    invoice_number: '10301',
    invoice_date_raw: 'Apr 28, 2026',
    customer_name: 'Superior Industrial',
    customer_address: '3855 West 150th Street\nCleveland, OH 44111',
    property_address: '3855 West 150th Street\nCleveland, OH 44111',
    subtotal: 180,
    tax_amount: 14.08,
    total: 194.08,
    notes: '',
    line_items: [
      {
        service_date_raw: '2026-04-28',
        name: 'Landscape Service',
        description: 'Weekly service.',
        quantity: 1,
        rate: 180,
        total: 194.08,
      },
    ],
    metadata: {
      outstanding_balance: 458.83,
      this_invoice: 194.08,
    },
  });

  assert(totalDueStoredAsOutstandingHtml.includes('Prior Balance</span><span>$264.75</span>'));
  assert(totalDueStoredAsOutstandingHtml.includes('Total Due</span><span>$458.83</span>'));
}

if (typeof test === 'function') {
  test('renderMailInvoiceHtml creates a compact invoice and stub layout', runAssertions);
} else {
  runAssertions();
  console.log('invoice-mail-html.test.js passed');
}
