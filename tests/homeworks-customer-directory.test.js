const records = require('../services/homeworks/records');
const customers = require('../services/homeworks/customers');
const fs = require('node:fs');
const vm = require('node:vm');
afterEach(() => jest.restoreAllMocks());
const owner = { id: 9, fullName: 'Jane Smith', email: 'jane@example.com', status: 'ACTIVE', tags: ['Weekly'] };
function context(local, remote) {
  jest.spyOn(records, 'listAll').mockResolvedValue(remote);
  const rows = local.map(row => ({ ...row }));
  const pool = { query: jest.fn(async (sql, args = []) => {
    if (sql.startsWith('ALTER') || sql.startsWith('CREATE')) return { rows: [] };
    if (sql === 'SELECT * FROM customers') return { rows: rows.map(row => ({ ...row })) };
    if (sql.startsWith('UPDATE customers')) {
      const row = rows.find(row => row.id === args[0] && !row.homeworks_customer_id);
      if (row) row.homeworks_customer_id = args[1];
      return { rows: row ? [{ ...row }] : [] };
    }
    if (sql.startsWith('INSERT INTO customers')) {
      let row = rows.find(row => row.homeworks_customer_id === args[11]);
      if (!row) { row = { id: 100 + rows.length, homeworks_customer_id: args[11], customer_number: args[0] }; rows.push(row); }
      return { rows: [{ ...row }] };
    }
    throw new Error(sql);
  }) };
  return { pool, rows };
}
test('ambiguous historical aliases stay intact while a canonical source contact remains available', async () => {
  const ctx = context([{ id: 1, name: owner.fullName, email: owner.email }, { id: 2, name: owner.fullName, email: owner.email }], [owner]);
  const result = await customers.directory(ctx);
  expect(result[0]).toMatchObject({ homeworks_customer_id: 9, name: owner.fullName, linkage_review_required: true });
  expect(result[0].id).not.toBe(1);
  expect(ctx.rows.slice(0, 2).every(row => !row.homeworks_customer_id)).toBe(true);
  expect(ctx.pool.query.mock.calls.some(([sql]) => sql.startsWith('UPDATE'))).toBe(false);
});
test('shared source identities are planned before claiming any historical contact', async () => {
  const ctx = context([{ id: 1, name: owner.fullName, email: owner.email }], [owner, { ...owner, id: 10 }]);
  const result = await customers.directory(ctx);
  expect(result.map(row => row.homeworks_customer_id)).toEqual([9, 10]);
  expect(new Set(result.map(row => row.id)).size).toBe(2);
  expect(result.every(row => row.linkage_review_required)).toBe(true);
  expect(ctx.rows[0].homeworks_customer_id).toBeUndefined();
});
test('unique historical contacts retain their office identity and canonical reads stay stable', async () => {
  const ctx = context([{ id: 1, name: owner.fullName, email: owner.email }], [owner]);
  expect((await customers.directory(ctx))[0]).toMatchObject({ id: 1, homeworks_customer_id: 9, linkage_review_required: false });
  expect((await customers.directory(ctx))[0].id).toBe(1);
});
test('customer list failures remain visibly unavailable instead of rendering an empty directory', async () => {
  const html = fs.readFileSync(require.resolve('../public/customers.html'), 'utf8');
  const start = html.indexOf('async function loadCustomers()'), end = html.indexOf('async function checkCustomerBalances()', start);
  const elements = new Map();
  const element = id => { if (!elements.has(id)) elements.set(id, { value: '', style: {} }); return elements.get(id); };
  const renderTable = jest.fn();
  const ctx = vm.createContext({ document: { getElementById: element }, console: { error() {} }, currentPipelineTab: '', PAGE_SIZE: 50, currentOffset: 0, allCustomers: [], URLSearchParams,
    fetch: async () => ({ ok: false, json: async () => ({ success: false, error: 'Source temporarily unavailable' }) }), renderTable, esc: String });
  vm.runInContext(html.slice(start, end), ctx);
  await ctx.loadCustomers();
  expect(renderTable).not.toHaveBeenCalled();
  expect(element('loadingState').innerHTML).toContain('Customers unavailable: Source temporarily unavailable');
});
test('source tag arrays render alongside historical linkage review', () => {
  const html = fs.readFileSync(require.resolve('../public/customers.html'), 'utf8');
  const start = html.indexOf('function renderTable()'), end = html.indexOf('function prevPage()', start);
  const body = { innerHTML: '' };
  const ctx = vm.createContext({ allCustomers: [{ ...owner, name: owner.fullName, tags: owner.tags, linkage_review_required: true }], document: { getElementById: id => id === 'tableBody' ? body : { style: {} } }, esc: String });
  vm.runInContext(html.slice(start, end), ctx); ctx.renderTable();
  expect(body.innerHTML).toContain('Weekly');
  expect(body.innerHTML).toContain('Historical contact linkage needs review');
});
