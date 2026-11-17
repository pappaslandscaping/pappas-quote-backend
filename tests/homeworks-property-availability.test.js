const fs = require('node:fs');
const vm = require('node:vm');
const records = require('../services/homeworks/records');
const customers = require('../services/homeworks/customers');
const properties = require('../services/homeworks/properties');
afterEach(() => jest.restoreAllMocks());
test('property reads omit the unused sizeType enum that rejects legacy zero values', async () => {
  const read = jest.spyOn(records, 'listAll').mockResolvedValue([{ id: 7, customerId: 9, name: 'Home', isActive: true }]);
  jest.spyOn(customers, 'directory').mockResolvedValue([{ id: 1, homeworks_customer_id: 9, name: 'Jane' }]);
  const pool = { query: jest.fn(async sql => ({ rows: sql.startsWith('INSERT') ? [{ id: 2 }] : [] })) };
  const result = await properties.list({ pool });
  expect(read.mock.calls[0][1].fields).not.toMatch(/\bsizeType\b/);
  expect(result.properties[0]).toMatchObject({ homeworks_property_id: 7, customer_display_name: 'Jane' });
});
test('a failed source property request visibly ends loading without showing an empty list', async () => {
  const html = fs.readFileSync(require.resolve('../public/properties.html'), 'utf8');
  const start = html.indexOf('async function loadProperties()'), end = html.indexOf('// ─── Load Customers', start);
  const elements = new Map();
  const element = id => { if (!elements.has(id)) elements.set(id, { value: '', style: {} }); return elements.get(id); };
  const renderTable = jest.fn();
  const ctx = vm.createContext({ API_BASE: '', document: { getElementById: element }, console: { error() {} }, URLSearchParams, properties: [], renderTable, esc: String,
    fetch: async () => ({ ok: false, json: async () => ({ success: false, error: 'Source unavailable' }) }) });
  vm.runInContext(html.slice(start, end), ctx); await ctx.loadProperties();
  expect(renderTable).not.toHaveBeenCalled();
  expect(element('tableCount').textContent).toBe('Unavailable');
  expect(element('tableContainer').innerHTML).toContain('Properties unavailable: Source unavailable');
});
