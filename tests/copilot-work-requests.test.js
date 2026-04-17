const assert = require('assert');
const {
  parseCopilotWorkRequestsHtml,
  buildCopilotWorkRequestStats,
  normalizeCopilotWorkRequestsSnapshot,
  getWorkRequestsSnapshotExpiry,
} = require('../lib/copilot-work-requests');

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

console.log('copilot-work-requests');

const fixtureHtml = `
<html>
  <body>
    <div class="dataTables_info">1-100 of 165</div>
    <table>
      <thead>
        <tr>
          <th>Customer Name</th>
          <th>Phone</th>
          <th>Email</th>
          <th>Address</th>
          <th>Preferred Work Date</th>
          <th>Work requested</th>
          <th>Source</th>
        </tr>
      </thead>
      <tbody>
        <tr id="wr_101">
          <td><a href="/customers/details/9001">Carol Horner</a></td>
          <td>(440) 555-1212</td>
          <td><a href="mailto:chorner1970@gmail.com">chorner1970@gmail.com</a></td>
          <td>123 Main St, Bay Village OH 44140</td>
          <td>Apr 20, 2026</td>
          <td>Mowing and mulch refresh</td>
          <td>Client</td>
        </tr>
        <tr id="wr_102">
          <td><a href="/customers/details/9002">Henrietta Pattantyus</a></td>
          <td>440-555-2222</td>
          <td>henrijeff12@gmail.com</td>
          <td>44 Oak Ave, Rocky River OH 44116</td>
          <td></td>
          <td>Spring cleanup</td>
          <td>Lead</td>
        </tr>
      </tbody>
    </table>
    <ul class="pagination">
      <li><a href="/customers/work_requests?page=2">2</a></li>
    </ul>
  </body>
</html>
`;

it('parses Copilot work request rows and total', () => {
  const parsed = parseCopilotWorkRequestsHtml(fixtureHtml, 'https://secure.copilotcrm.com/customers/work_requests');
  assert.strictEqual(parsed.total, 165);
  assert.strictEqual(parsed.requests.length, 2);
  assert.deepStrictEqual(parsed.page_paths, ['/customers/work_requests?page=2']);
});

it('maps canonical fields from Copilot rows', () => {
  const parsed = parseCopilotWorkRequestsHtml(fixtureHtml);
  const first = parsed.requests[0];
  assert.strictEqual(first.id, 'wr_101');
  assert.strictEqual(first.external_source, 'copilotcrm');
  assert.strictEqual(first.customer_name, 'Carol Horner');
  assert.strictEqual(first.customer_phone, '(440) 555-1212');
  assert.strictEqual(first.customer_email, 'chorner1970@gmail.com');
  assert.strictEqual(first.customer_address, '123 Main St, Bay Village OH 44140');
  assert.strictEqual(first.preferred_work_date, '2026-04-20');
  assert.strictEqual(first.preferred_work_date_raw, 'Apr 20, 2026');
  assert.strictEqual(first.work_requested, 'Mowing and mulch refresh');
  assert.strictEqual(first.source, 'Client');
  assert.strictEqual(first.customer_path, '/customers/details/9001');
});

it('builds Copilot stats from source and preferred-date fields', () => {
  const parsed = parseCopilotWorkRequestsHtml(fixtureHtml);
  const stats = buildCopilotWorkRequestStats(parsed.requests);
  assert.deepStrictEqual(stats, {
    total: 2,
    open_total: 2,
    client: 1,
    lead: 1,
    with_preferred_date: 1,
  });
});

it('normalizes a persisted snapshot into the API contract', () => {
  const parsed = parseCopilotWorkRequestsHtml(fixtureHtml);
  const snapshot = normalizeCopilotWorkRequestsSnapshot({
    as_of: '2026-04-17T15:00:00.000Z',
    total: 165,
    requests: parsed.requests,
  }, 'persisted_copilot_snapshot');
  assert.strictEqual(snapshot.success, true);
  assert.strictEqual(snapshot.mode, 'copilot');
  assert.strictEqual(snapshot.source, 'persisted_copilot_snapshot');
  assert.strictEqual(snapshot.total, 165);
  assert.strictEqual(snapshot.stats.client, 1);
});

it('computes snapshot expiry from as_of', () => {
  const expiry = getWorkRequestsSnapshotExpiry({ as_of: '2026-04-17T15:00:00.000Z' }, 300000);
  assert.strictEqual(expiry, new Date('2026-04-17T15:00:00.000Z').getTime() + 300000);
});

if (failures > 0) {
  console.error(`\n${failures} failure(s)`);
  process.exit(1);
} else {
  console.log('\nAll tests passed');
}
