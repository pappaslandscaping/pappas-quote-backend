const assert = require('assert');
const { discoverInvoicePdf } = require('../lib/copilot-live-invoices');

async function runAssertions() {
  const originalFetch = global.fetch;
  const calls = [];

  global.fetch = async (url) => {
    calls.push(String(url));
    return {
      ok: true,
      url: String(url),
      headers: {
        get(name) {
          if (String(name).toLowerCase() === 'content-type') return 'text/html; charset=utf-8';
          return null;
        },
      },
      async text() {
        return `
          <html>
            <body>
              <div>Invoice #10956</div>
              <div>Superior Industrial</div>
              <div>3855 West 150th Street</div>
              <div>Cleveland, OH 44111</div>
            </body>
          </html>
        `;
      },
    };
  };

  try {
    const result = await discoverInvoicePdf(
      { cookies: 'copilotApiAccessToken=fake', invoiceListPath: '/finances/invoices' },
      {
        external_invoice_id: '10956',
        invoice_number: '10956',
        customer_name: 'Superior Industrial',
        customer_address: '3855 West 150th Street\nCleveland, OH 44111',
        metadata: {},
      }
    );

    assert.strictEqual(calls.length, 1);
    assert(calls[0].includes('/finances/invoices/view/10956'));
    assert(!calls[0].includes('print=1'));
    assert.strictEqual(result.pdfUrlUsed, null);
    assert.strictEqual(result.parsedInvoice.metadata.detail_fetch_mode, 'html-only');
  } finally {
    global.fetch = originalFetch;
  }
}

if (typeof test === 'function') {
  test('discoverInvoicePdf uses a single read-only invoice detail fetch', runAssertions);
} else {
  runAssertions()
    .then(() => {
      console.log('copilot-invoice-refresh-readonly.test.js passed');
    })
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}
