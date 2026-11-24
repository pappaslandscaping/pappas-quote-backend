const assert = require('assert');
const { PDFPage } = require('pdf-lib');
const { splitAddressLines } = require('../lib/mail-address');
const { renderMailInvoiceHtml, renderMailBatchHtml } = require('../lib/invoice-mail-html');
const { renderMailInvoicePdf } = require('../lib/invoice-mail-pdf');

async function runAssertions() {
  const addresses = [
    '4492 West 158th Street, Cleveland, OH, 44135',
    '4492 West 158th Street, Cleveland, OH 44135',
    '4492 West 158th Street Cleveland OH, 44135',
    '4492 West 158th Street\nCleveland, OH, 44135',
  ];
  for (const address of addresses) {
    assert.deepStrictEqual(splitAddressLines(address), ['4492 West 158th Street', 'Cleveland, OH 44135']);
  }
  assert.deepStrictEqual(splitAddressLines('PO Box 770057, Lakewood, OH, 44107'), ['PO Box 770057', 'Lakewood, OH 44107']);
  assert.deepStrictEqual(splitAddressLines('15646 Hocking Boulevard, Brook Park, OH, 44142-1234'), ['15646 Hocking Boulevard', 'Brook Park, OH 44142-1234']);
  assert.deepStrictEqual(splitAddressLines('12 Main Street, Apt 2, North Olmsted, OH, 44070'), ['12 Main Street, Apt 2', 'North Olmsted, OH 44070']);
  assert.deepStrictEqual(splitAddressLines(''), []);

  const invoice = { invoice_number: '13377', customer_name: 'Diane Gottschalk', customer_address: addresses[0], line_items: [], total: 0 };
  for (const html of [renderMailInvoiceHtml(invoice), renderMailBatchHtml([invoice])]) {
    for (const className of ['window-recipient', 'stub-address-window stub-customer-window']) {
      const block = html.slice(html.indexOf('class="' + className + '"'));
      assert(/<strong>Diane Gottschalk<\/strong>\s*<div>4492 West 158th Street<\/div>\s*<div>Cleveland, OH 44135<\/div>/.test(block));
    }
  }

  const calls = [];
  const originalDrawText = PDFPage.prototype.drawText;
  PDFPage.prototype.drawText = function(text, options) {
    calls.push({ page: this, text, ...options });
    return originalDrawText.call(this, text, options);
  };
  try {
    await renderMailInvoicePdf(invoice);
  } finally {
    PDFPage.prototype.drawText = originalDrawText;
  }
  const recipients = calls.filter(call => call.text === 'Diane Gottschalk');
  // Invoice address and remittance address both use distinct lines.
  for (const recipient of [recipients[0], recipients[1]]) {
    assert(recipient);
    const street = calls.find(call => call.page === recipient.page && call.x === recipient.x && call.text === '4492 West 158th Street');
    const city = calls.find(call => call.page === recipient.page && call.x === recipient.x && call.text === 'Cleveland, OH 44135');
    assert(street && city);
    assert(recipient.y > street.y && street.y > city.y);
    assert(recipient.font.name.includes('Bold'));
  }
}
if (typeof test === 'function') {
  test('invoice and remittance addresses split HomeWorks comma-delimited fields in HTML and PDF', runAssertions);
} else {
  runAssertions().then(() => console.log('mail-address.test.js passed')).catch(error => { console.error(error); process.exit(1); });
}
