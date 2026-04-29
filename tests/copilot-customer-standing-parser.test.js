const assert = require('assert');
const { parseCopilotCustomerStandingHtml } = require('../lib/copilot-live-invoices');

function runAssertions() {
  const html = `
    <section>
      <div>Superior Industrial</div>
      <div>All values in USD.</div>
      <div class="balance-card">
        <div>
          <span>Past Due</span>
          <span>Due for 30+ days</span>
          <strong>264.75</strong>
        </div>
        <div>
          <span>Outstanding</span>
          <span>Includes past due amount</span>
          <strong>458.83</strong>
        </div>
        <div>
          <span>Paid</span>
          <strong>4,250.87</strong>
        </div>
        <div>
          <span>Credit</span>
          <strong>0.00</strong>
        </div>
      </div>
    </section>
  `;

  const standing = parseCopilotCustomerStandingHtml(html);
  assert.strictEqual(standing.past_due_balance, 264.75);
  assert.strictEqual(standing.outstanding_balance, 458.83);
  assert.strictEqual(standing.paid_balance, 4250.87);
  assert.strictEqual(standing.credit_available, 0);
}

if (typeof test === 'function') {
  test('parseCopilotCustomerStandingHtml extracts balance card metrics', runAssertions);
} else {
  runAssertions();
  console.log('copilot-customer-standing-parser.test.js passed');
}
