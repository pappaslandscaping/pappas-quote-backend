const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

test('editing a phone does not deactivate an active customer or overwrite unexposed tax settings', async () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/customer-detail.html'), 'utf8');
  const start = html.indexOf('async function saveCustomer()');
  const end = html.indexOf('function showDeleteModal()', start);
  const customer = {
    name: 'Jane Smith',
    first_name: 'Jane',
    last_name: 'Smith',
    email: 'jane@example.com',
    phone: '2165551000',
    mobile: '',
    street: '1 Main St',
    street2: '',
    city: 'Lakewood',
    state: 'OH',
    postal_code: '44107',
    type: 'Customer',
    status: 'active',
    tags: ['Weekly'],
    notes: '',
  };
  const fields = Object.fromEntries(
    Object.entries(customer).map(([key, value]) => [
      `f_${key}`,
      { value: Array.isArray(value) ? value.join(',') : value },
    ]),
  );
  fields.f_status.value = 'Active';
  fields.f_phone.value = '2165552000';
  let submitted;
  const noop = () => {};
  const context = vm.createContext({
    customer,
    customerId: 44,
    document: {
      getElementById: (id) => {
        if (!fields[id]) throw new Error('Unsupported field read: ' + id);
        return fields[id];
      },
    },
    fetch: async (_url, options) => {
      submitted = JSON.parse(options.body);
      return { json: async () => ({ success: true, customer }) };
    },
    render: noop,
    showToast: noop,
    loadProperties: noop,
    loadJobs: noop,
    loadQuotes: noop,
    loadInvoices: noop,
    loadNotes: noop,
    loadSavedCards: noop,
  });
  vm.runInContext(html.slice(start, end), context);
  await context.saveCustomer();
  expect(submitted).toEqual({ phone: '2165552000' });
});
