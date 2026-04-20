const assert = require('assert');
const {
  parseCopilotTaxSummaryHtml,
  normalizeTaxSummarySnapshot,
  buildDailyTaxRecommendation,
} = require('../lib/copilot-tax-summary');

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

console.log('copilot-tax-summary');

const fixtureHtml = `
<html>
  <body>
    <h1>Tax Summary</h1>
    <table>
      <thead>
        <tr>
          <th>Tax Rate</th>
          <th>Total Sales</th>
          <th>Taxable Amount</th>
          <th>Discount</th>
          <th>Tax Amount</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>8.00%</td>
          <td>$1,250.00</td>
          <td>$1,100.00</td>
          <td>$0.00</td>
          <td>$88.00</td>
        </tr>
        <tr>
          <td>0.00%</td>
          <td>$150.00</td>
          <td>$0.00</td>
          <td>$0.00</td>
          <td>$0.00</td>
        </tr>
      </tbody>
    </table>
    <div>Processing Fees $3.40</div>
    <div>Tips $12.00</div>
  </body>
</html>
`;

const alternateHeaderFixtureHtml = `
<html>
  <body>
    <h1>Sales Tax Report</h1>
    <table>
      <tr>
        <th>Rate</th>
        <th>Sales</th>
        <th>Taxable Sales</th>
        <th>Discount</th>
        <th>Sales Tax</th>
      </tr>
      <tr>
        <td>8.00%</td>
        <td>$500.00</td>
        <td>$400.00</td>
        <td>$5.00</td>
        <td>$32.00</td>
      </tr>
      <tr>
        <td>Total</td>
        <td>$500.00</td>
        <td>$400.00</td>
        <td>$5.00</td>
        <td>$32.00</td>
      </tr>
    </table>
    <table>
      <tr><th></th><th>Amount</th></tr>
      <tr><td>Processing Fees</td><td>$1.25</td></tr>
      <tr><td>Tips</td><td>$3.50</td></tr>
    </table>
  </body>
</html>
`;

it('parses Tax Summary collected rows and totals', () => {
  const parsed = parseCopilotTaxSummaryHtml(fixtureHtml, {
    startDate: '2026-04-17',
    endDate: '2026-04-17',
    basis: 'collected',
    pageUrl: '/reports/tax_summary?type=collected&sdate=2026-04-17&edate=2026-04-17',
  });
  assert.strictEqual(parsed.start_date, '2026-04-17');
  assert.strictEqual(parsed.end_date, '2026-04-17');
  assert.strictEqual(parsed.basis, 'collected');
  assert.strictEqual(parsed.rows.length, 2);
  assert.strictEqual(parsed.total_sales, 1400);
  assert.strictEqual(parsed.taxable_amount, 1100);
  assert.strictEqual(parsed.discount, 0);
  assert.strictEqual(parsed.tax_amount, 88);
  assert.strictEqual(parsed.processing_fees, 3.4);
  assert.strictEqual(parsed.tips, 12);
});

it('parses alternate live-like Tax Summary headers and ignores total rows', () => {
  const parsed = parseCopilotTaxSummaryHtml(alternateHeaderFixtureHtml, {
    startDate: '2026-04-17',
    endDate: '2026-04-17',
    basis: 'collected',
    pageUrl: '/reports/tax/?type=collected&sdate=2026-04-17&edate=2026-04-17',
  });
  assert.strictEqual(parsed.rows.length, 1);
  assert.strictEqual(parsed.total_sales, 500);
  assert.strictEqual(parsed.taxable_amount, 400);
  assert.strictEqual(parsed.discount, 5);
  assert.strictEqual(parsed.tax_amount, 32);
  assert.strictEqual(parsed.processing_fees, 1.25);
  assert.strictEqual(parsed.tips, 3.5);
});

it('flags a missing Tax Summary table as a parser warning instead of a valid zero snapshot', () => {
  const parsed = parseCopilotTaxSummaryHtml('<html><body><h1>Sign In</h1></body></html>', {
    startDate: '2026-04-17',
    endDate: '2026-04-17',
    basis: 'collected',
  });
  assert.strictEqual(parsed.rows.length, 0);
  assert.strictEqual(parsed.parser_warning, 'Tax Summary table not found');
});

it('normalizes daily tax summary snapshots into persisted/report shape', () => {
  const parsed = parseCopilotTaxSummaryHtml(fixtureHtml, {
    startDate: '2026-04-17',
    endDate: '2026-04-17',
    basis: 'collected',
  });
  const normalized = normalizeTaxSummarySnapshot({
    ...parsed,
    as_of: '2026-04-17T15:00:00.000Z',
  }, 'persisted_copilot_snapshot');
  assert.strictEqual(normalized.success, true);
  assert.strictEqual(normalized.source, 'persisted_copilot_snapshot');
  assert.strictEqual(normalized.tax_amount, 88);
  assert.strictEqual(normalized.rows[0].tax_rate, 8);
});

it('builds daily reconciliation math with Copilot collected tax authoritative', () => {
  const recommendation = buildDailyTaxRecommendation({
    snapshot: {
      tax_amount: 88,
    },
    backendReconstructedTax: 82.37,
  });
  assert.deepStrictEqual(recommendation, {
    recommended_transfer_amount: 88,
    copilot_collected_tax: 88,
    backend_reconstructed_tax: 82.37,
    variance: 5.63,
  });
});

it('keeps Copilot collected tax as recommendation even when backend reconstructed tax is higher', () => {
  const recommendation = buildDailyTaxRecommendation({
    snapshot: {
      tax_amount: 50,
    },
    backendReconstructedTax: 62.12,
  });
  assert.strictEqual(recommendation.recommended_transfer_amount, 50);
  assert.strictEqual(recommendation.copilot_collected_tax, 50);
  assert.strictEqual(recommendation.backend_reconstructed_tax, 62.12);
  assert.strictEqual(recommendation.variance, -12.12);
});

if (failures > 0) {
  console.error(`\n${failures} failure(s)`);
  process.exit(1);
} else {
  console.log('\nAll tests passed');
}
