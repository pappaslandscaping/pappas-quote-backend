const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { INVOICE_FOLLOWUP_TEMPLATE, renderInvoiceFollowupTemplate } = require('../lib/invoice-followup-template');
const { createFollowupRoutes, loadInvoiceChoices, loadFollowup, openInvoiceRows, reminderBody,
  signPreview, verifyPreview, validateBody, PAGE_SIZE } = require('../routes/invoice-followup');

process.env.JWT_SECRET = 'invoice-followup-tests-only';
const row = (id, changes = {}) => ({ id, number: 12000 + id, customerId: 51,
  status: 'PENDING', total: '100.00', paidAmount: '0', isSent: true, isArchived: false,
  deletedState: 'ACTIVE', daysPastDue: 0, dueDate: '2026-11-20T00:00:00.000Z', ...changes });
function fixture(rows = [row(1), row(2, { paidAmount: '25', daysPastDue: 12 })]) {
  const state = { rows, customer: { id: 51, fullName: 'Test Customer', cell: '2165550100', portalKey: 'test-key' } };
  const pool = { query: jest.fn(async sql => {
    if (sql === 'SELECT * FROM invoices WHERE id = $1') return { rows: [{ id: 7, external_invoice_id: '1', customer_id: 999, customer_name: 'Stale Name' }] };
    if (sql.startsWith('INSERT INTO messages')) return { rows: [] };
    throw new Error('Unexpected SQL: ' + sql);
  }) };
  const queryImpl = jest.fn(async ({ operationName, variables }) => {
    if (operationName === 'YardDeskFollowupOwner') return { invoice: { id: 1, customerId: 51 } };
    if (operationName === 'YardDeskFollowupCustomer') return { customers: [state.customer] };
    if (operationName === 'YardDeskFollowupInvoices') return { invoices: state.rows.slice(variables.skip, variables.skip + variables.take) };
    throw new Error('Unexpected operation');
  });
  const sendSms = jest.fn(async () => ({ sid: 'SM-test', status: 'queued' }));
  const router = createFollowupRoutes({ pool, queryImpl, sendSms, isTwilioConfigured: () => true,
    authenticateToken: (_req, _res, next) => next(), twilioPhoneNumber: '+14405550100' });
  const call = async (suffix, body = {}) => {
    const layer = router.stack.find(item => item.route?.path === '/api/invoices/:id/' + suffix);
    const handler = layer.route.stack[layer.route.stack.length - 1].handle;
    const res = { statusCode: 200, status(code) { this.statusCode = code; return this; },
      json(body) { this.body = body; return this; } };
    await handler({ params: { id: '7' }, body }, res);
    return res;
  };
  const preview = async scope => (await call('followup-preview', { scope: scope || 'all' })).body.preview;
  const sendBody = p => ({ confirm_send: true, scope: p.scope, phone: p.to,
    verification_receipt: p.verification_receipt, body: p.body });
  return { state, pool, queryImpl, sendSms, call, preview, sendBody };
}

test('all invoices come from the authoritative customer ID and use remaining balances', async () => {
  const f = fixture();
  const p = await f.preview();
  expect(p).toMatchObject({ customer_name: 'Test Customer', to: '+12165550100', balance: 175, past_due: 75,
    total_outstanding: 175, scope: 'all', source: 'official_homeworks_graphql' });
  expect(p.invoices.map(i => i.number)).toEqual(['12001', '12002']);
  expect(p.body).toContain('Invoice #12002: $75.00');
  expect(f.sendSms).not.toHaveBeenCalled();
  expect(f.pool.query.mock.calls.every(([sql]) => sql.startsWith('SELECT'))).toBe(true);
});
test('single mode includes only the selected invoice with its own balance', async () => {
  const p = await fixture().preview('single');
  expect(p.invoices).toHaveLength(1);
  expect(p.balance).toBe(100);
  expect(p.past_due).toBe(0);
  expect(p.total_outstanding).toBe(175);
  expect(p.body).not.toMatch(/past due/i);
});
test('paid, draft, unsent, void/write-off, archived, and deleted charges are excluded', () => {
  const rows = [row(1, { total: '187.80', paidAmount: '157.70', status: 'PARTIALLY_PAID' }),
    row(2, { status: 'PAID' }), row(3, { status: 'DRAFT' }), row(4, { isSent: false }),
    row(5, { status: 'WRITE_OFF' }), row(6, { isArchived: true }),
    row(7, { deletedState: 'DELETED' }), row(8, { paidAmount: '100' })];
  expect(openInvoiceRows(rows, 51, 'key').map(i => i.balance)).toEqual([30.1]);
});
test('missing financials and mismatched customer IDs fail closed', () => {
  expect(() => openInvoiceRows([row(1, { paidAmount: null })], 51, 'key')).toThrow(/amount/);
  expect(() => openInvoiceRows([row(1, { customerId: 999 })], 51, 'key')).toThrow(/owner/);
  expect(() => openInvoiceRows([row(1), row(1)], 51, 'key')).toThrow(/list changed/);
});
test('pagination includes invoices beyond the first page', async () => {
  const f = fixture(Array.from({ length: PAGE_SIZE + 1 }, (_, i) => row(i + 1)));
  const p = await f.preview();
  expect(p.invoices).toHaveLength(PAGE_SIZE + 1);
  expect(p.balance).toBe((PAGE_SIZE + 1) * 100);
  expect(f.queryImpl.mock.calls.filter(([v]) => v.operationName === 'YardDeskFollowupInvoices')).toHaveLength(2);
});
test('missing phone, missing portal key, and empty open list prevent a draft', async () => {
  for (const changes of [{ cell: '', phone: '' }, { portalKey: '' }]) {
    const f = fixture(); Object.assign(f.state.customer, changes);
    const r = await f.call('followup-preview', { scope: 'all' });
    expect(r.statusCode).toBe(409); expect(f.sendSms).not.toHaveBeenCalled();
  }
  expect((await fixture([]).call('followup-preview', { scope: 'all' })).statusCode).toBe(409);
});
test('revalidation blocks a payment or newly opened invoice after preview', async () => {
  for (const change of [f => { f.state.rows[0].paidAmount = '100'; }, f => f.state.rows.push(row(3))]) {
    const f = fixture(); const p = await f.preview(); change(f);
    const r = await f.call('followup-sms', f.sendBody(p));
    expect(r.statusCode).toBe(409); expect(r.body.error).toMatch(/changed/);
    expect(f.sendSms).not.toHaveBeenCalled();
  }
});
test('phone edits, wrong scope, and tampered receipts cannot send', async () => {
  for (const change of [b => { b.phone = '4405550999'; }, b => { b.scope = 'single'; },
    b => { b.verification_receipt += 'x'; }]) {
    const f = fixture(); const p = await f.preview(); const body = f.sendBody(p); change(body);
    expect((await f.call('followup-sms', body)).statusCode).toBe(409);
    expect(f.sendSms).not.toHaveBeenCalled();
  }
});
test('manual confirmation and every exact verified link are required', async () => {
  const f = fixture(); const p = await f.preview();
  expect((await f.call('followup-sms', { ...f.sendBody(p), confirm_send: false })).statusCode).toBe(400);
  expect(() => validateBody(p.body.replace(p.invoices[1].url, ''), p)).toThrow(/each invoice/);
  expect(() => validateBody(p.body + '\nhttps://unverified.example/pay', p)).toThrow(/verified payment/);
  expect(() => validateBody('x'.repeat(1601), p)).toThrow(/1600/);
  expect(f.sendSms).not.toHaveBeenCalled();
});
test('a reviewed follow-up sends once without changing invoice billing state', async () => {
  const f = fixture(); const p = await f.preview(); const body = f.sendBody(p);
  expect((await f.call('followup-sms', body)).statusCode).toBe(200);
  expect((await f.call('followup-sms', body)).statusCode).toBe(409);
  expect(f.sendSms).toHaveBeenCalledTimes(1);
  expect(f.sendSms).toHaveBeenCalledWith(expect.objectContaining({ to: '+12165550100', manualReviewedSend: true }));
  expect(f.pool.query.mock.calls.some(([sql]) => /UPDATE|DELETE/.test(sql))).toBe(false);
});
test('an upstream error during send does not fall back to stale snapshots', async () => {
  const f = fixture(); const p = await f.preview();
  f.queryImpl.mockRejectedValue(new Error('HomeWorks unavailable'));
  expect((await f.call('followup-sms', f.sendBody(p))).statusCode).toBe(409);
  expect(f.sendSms).not.toHaveBeenCalled();
});
test('logging failure after provider success reports sent and prevents a duplicate retry', async () => {
  const f = fixture(); const p = await f.preview();
  const original = f.pool.query.getMockImplementation();
  f.pool.query.mockImplementation(sql => sql.startsWith('INSERT') ? Promise.reject(new Error('DB offline')) : original(sql));
  const first = await f.call('followup-sms', f.sendBody(p));
  expect(first.body.success).toBe(true); expect(first.body.warning).toMatch(/Do not resend/);
  expect((await f.call('followup-sms', f.sendBody(p))).statusCode).toBe(409);
  expect(f.sendSms).toHaveBeenCalledTimes(1);
});
test('expired receipts and wrong invoice scope are rejected', async () => {
  const f = fixture(); const p = await loadFollowup({ pool: f.pool, invoiceId: '7', queryImpl: f.queryImpl });
  const time = jest.spyOn(Date, 'now').mockReturnValue(1000000);
  try {
    const token = signPreview(p, 'secret');
    expect(verifyPreview(token, p, 'secret')).toBe(true);
    time.mockReturnValue(1000000 + 31 * 60 * 1000);
    expect(verifyPreview(token, p, 'secret')).toBe(false);
  } finally { time.mockRestore(); }
});
function composer(fetchImpl = jest.fn()) {
  const html = fs.readFileSync(path.join(__dirname, '../public/communications.html'), 'utf8');
  const start = html.indexOf('async function openInvoiceTextComposer()');
  const end = html.indexOf('function formatCurrency', start);
  const nodes = new Map();
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, { value: '', checked: false, disabled: false, textContent: '', innerHTML: '',
      classList: { add() {}, remove() {} }, setAttribute() {} });
    return nodes.get(id);
  };
  node('sms-invoice-select').value = '7'; node('sms-invoice-scope').value = 'all';
  const context = vm.createContext({ document: { getElementById: node }, fetch: fetchImpl,
    localStorage: { getItem: () => '' }, formatCurrency: v => '$' + Number(v).toFixed(2),
    fmtPhone: v => v, esc: v => String(v), findSmsTemplate: () => null });
  const renderStart = html.indexOf('function renderSmsTemplate(');
  const renderEnd = html.indexOf('function basicSmsTemplateVars', renderStart);
  vm.runInContext(html.slice(renderStart, renderEnd), context);
  vm.runInContext('let smsTemplates = [], invoiceTextPreview = null, invoiceTextInvoices = [], invoiceTextRequestId = 0, invoiceTextAiRequestId = 0, invoiceTextLoading = false, invoiceTextSending = false, invoiceTextAiPending = false;\n' + html.slice(start, end), context);
  return { context, node };
}
test('AI formatting preserves every invoice link even when its response drops or invents links', async () => {
  const p = await fixture().preview();
  const { context } = composer();
  const result = context.formatInvoiceTextDraft('Please let us know when to expect payment. https://wrong.example', p.invoice_url, p.customer_name, p);
  for (const i of p.invoices) { expect(result).toContain(i.url); expect(result).toContain('#' + i.number); }
  expect(result).not.toContain('wrong.example');
  expect(() => validateBody(result, p)).not.toThrow();
});
test('late preview responses cannot replace a more recent invoice selection', async () => {
  const f = fixture(); const p = await f.preview();
  let resolveFirst;
  const fetchMock = jest.fn().mockImplementationOnce(() => new Promise(resolve => { resolveFirst = resolve; }))
    .mockResolvedValueOnce({ ok: true, json: async () => ({ preview: { ...p, invoice_id: '8', customer_name: 'Second Customer' } }) });
  const { context, node } = composer(fetchMock);
  const first = context.prepareInvoiceTextPreview();
  node('sms-invoice-select').value = '8';
  await context.prepareInvoiceTextPreview();
  resolveFirst({ ok: true, json: async () => ({ preview: p }) });
  await first;
  expect(node('sms-recipient-preview').innerHTML).toContain('Second Customer');
  expect(node('sms-recipient-preview').innerHTML).not.toContain('Test Customer');
});
test('the composer defaults to all open invoices and uses the official preview flow', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/communications.html'), 'utf8');
  expect(html).toMatch(/id="sms-invoice-scope"[\s\S]*?<option value="all">All open invoices/);
  expect(html).not.toContain("fetch('/api/copilot/invoices/sync'");
  expect(html).not.toContain('verifiedInvoiceTextDetails');
  const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)];
  scripts.forEach(match => expect(() => new vm.Script(match[1])).not.toThrow());
});

test('invoice choices use live HomeWorks invoices and exclude unsent balances', async () => {
  const pool = { query: jest.fn() };
  const queryImpl = jest.fn(async ({ operationName }) => operationName === 'YardDeskFollowupChoices'
    ? { invoices: [row(1), row(2, { isSent: false }), row(3, { paidAmount: '100' })] }
    : { customers: [{ id: 51, fullName: 'Current Customer' }] });
  const choices = await loadInvoiceChoices({ pool, queryImpl });
  expect(choices).toEqual([expect.objectContaining({ id: 'homeworks:1', customer_name: 'Current Customer' })]);
  expect(pool.query).not.toHaveBeenCalled();
  const f = fixture();
  const preview = await loadFollowup({ pool, invoiceId: choices[0].id, queryImpl: f.queryImpl });
  expect(preview.invoice_id).toBe('homeworks:1');
  expect(pool.query).not.toHaveBeenCalled();
});
test('client search matches name, invoice number, and remaining balance without an overdue requirement', () => {
  const { context, node } = composer();
  vm.runInContext("invoiceTextInvoices = [{ id: 'homeworks:1', customer_name: 'Test Customer', invoice_number: '12345', total: 100, amount_paid: 25 }];", context);
  context.filterInvoiceTextOptions('t');
  expect(node('sms-invoice-select').innerHTML).not.toContain('homeworks:1');
  for (const term of ['customer', '12345', '75.00']) {
    context.filterInvoiceTextOptions(term);
    expect(node('sms-invoice-select').innerHTML).toContain('homeworks:1');
  }
  context.filterInvoiceTextOptions('100.00');
  expect(node('sms-invoice-select').innerHTML).not.toContain('homeworks:1');
});
test('manual review is required in the UI before either send or confirmation is reached', async () => {
  const fetchMock = jest.fn(); const { context, node } = composer(fetchMock);
  context.previewFixture = await fixture().preview();
  vm.runInContext('invoiceTextPreview = previewFixture;', context);
  node('sms-compose-body').value = context.previewFixture.body;
  node('sms-reviewed').checked = false;
  await context.sendReviewedInvoiceText();
  expect(node('sms-send-btn').disabled).toBe(true);
  expect(fetchMock).not.toHaveBeenCalled();
});
test('AI drafts only and appends verified invoice facts without approving or sending', async () => {
  const fetchMock = jest.fn(async () => ({ ok: true, json: async () => ({ draft: 'Please let us know when payment will arrive.' }) }));
  const { context, node } = composer(fetchMock);
  context.previewFixture = await fixture().preview();
  vm.runInContext('invoiceTextPreview = previewFixture;', context);
  node('sms-compose-body').value = context.previewFixture.body;
  await context.draftInvoiceTextWithAi();
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0][0]).toBe('/api/app/ai/draft');
  expect(node('sms-reviewed').checked).toBe(false);
  for (const invoice of context.previewFixture.invoices) expect(node('sms-compose-body').value).toContain(invoice.url);
});
test('templates receive all selected invoice numbers, exact links and separate account balance', async () => {
  const p = await fixture().preview();
  const { context } = composer();
  const vars = context.invoiceSmsTemplateVars(p);
  expect(vars.invoice_number).toBe('12001, 12002');
  expect(vars.balance_due).toBe('175.00');
  expect(vars.invoices_links).toBe(p.invoices.map(i => i.url).join('\n'));
  const single = await fixture().preview('single');
  expect(context.invoiceSmsTemplateVars(single)).toMatchObject({ balance_due: '100.00', customer_account_balance: '175.00' });
});

test('saved follow-up has one total, each invoice once and no empty payment labels', async () => {
  const p = await fixture().preview();
  const { context } = composer();
  const text = context.renderInvoiceSmsTemplate(INVOICE_FOLLOWUP_TEMPLATE, p);
  expect(text).toBe(renderInvoiceFollowupTemplate(p));
  expect(text.match(/\$175\.00/g)).toHaveLength(1);
  expect(text.match(/#12001/g)).toHaveLength(1);
  expect(text.match(/#12002/g)).toHaveLength(1);
  expect(text).toContain('open invoices');
  expect(text).toContain('links above');
  expect(text).not.toMatch(/ready for review|This invoice:|Open balance:|View and pay your invoice here:/);
  expect(text).not.toMatch(/\{[a-z_]+\}|\n{3}/);
  for (const invoice of p.invoices) expect(text.split(invoice.url)).toHaveLength(2);
});
test('saved follow-up uses singular wording and the selected amount for one invoice', async () => {
  const p = await fixture().preview('single');
  const { context } = composer();
  const text = context.renderInvoiceSmsTemplate(INVOICE_FOLLOWUP_TEMPLATE, p);
  expect(text).toContain('open invoice.');
  expect(text).toContain('the link above');
  expect(text).not.toContain('open invoices');
  expect(text.match(/\$100\.00/g)).toHaveLength(2);
  expect(text).not.toContain('$175.00');
});
test('multiple invoices replace the invoice-ready template with the saved follow-up', async () => {
  const { context, node } = composer();
  context.previewFixture = await fixture().preview();
  context.followupFixture = { id: 2, ...INVOICE_FOLLOWUP_TEMPLATE };
  context.findSmsTemplate = () => ({ id: 1, slug: 'invoice_ready_sms', sms_body: 'Your invoice is ready' });
  vm.runInContext('invoiceTextPreview = previewFixture; smsTemplates = [followupFixture];', context);
  node('invoice-sms-template').value = '1';
  context.applyInvoiceSmsTemplate();
  expect(node('invoice-sms-template').value).toBe(2);
  expect(node('sms-compose-body').value).toBe(renderInvoiceFollowupTemplate(context.previewFixture));
  expect(node('sms-reviewed').checked).toBe(false);
});
test('startup saves the named SMS template without overwriting user edits', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8');
  const start = source.indexOf("    const { INVOICE_FOLLOWUP_TEMPLATE: followup }");
  const end = source.indexOf('    const invoiceReadySms =', start);
  const pool = { query: jest.fn(async () => ({ rows: [] })) };
  await new Function('pool', 'require', 'return (async () => {' + source.slice(start, end) + '})();')
    (pool, () => ({ INVOICE_FOLLOWUP_TEMPLATE }));
  expect(pool.query).toHaveBeenCalledTimes(1);
  const [sql, values] = pool.query.mock.calls[0];
  expect(sql).toMatch(/ON CONFLICT \(slug\) DO NOTHING/);
  expect(values.slice(0, 4)).toEqual(['Invoice Follow-Up', 'invoice_followup_sms', 'invoices', 'sms']);
  expect(values[4]).toBe(INVOICE_FOLLOWUP_TEMPLATE.sms_body);
});

test('an in-flight AI rewrite cannot be reviewed or sent and its result needs fresh approval', async () => {
  let finish;
  const fetchMock = jest.fn(() => new Promise(resolve => { finish = resolve; }));
  const { context, node } = composer(fetchMock);
  context.previewFixture = await fixture().preview();
  vm.runInContext('invoiceTextPreview = previewFixture;', context);
  node('sms-compose-body').value = context.previewFixture.body;
  node('sms-reviewed').checked = true;
  const rewriting = context.draftInvoiceTextWithAi();
  expect(node('sms-reviewed').disabled).toBe(true);
  expect(node('sms-reviewed').checked).toBe(false);
  node('sms-reviewed').checked = true; // Simulate the reported approval race.
  await context.sendReviewedInvoiceText();
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(node('sms-send-btn').disabled).toBe(true);
  finish({ ok: true, json: async () => ({ draft: 'Please let us know when payment can be expected.' }) });
  await rewriting;
  expect(node('sms-compose-body').value).toContain('when payment can be expected');
  expect(node('sms-reviewed').checked).toBe(false);
  expect(node('sms-reviewed').disabled).toBe(false);
  expect(node('sms-send-btn').disabled).toBe(true);
  node('sms-reviewed').checked = true;
  context.updateInvoiceTextSendState();
  expect(node('sms-send-btn').disabled).toBe(false);
});
test('a stale AI response cannot unlock review while a newer rewrite is pending', async () => {
  const pending = [];
  const fetchMock = jest.fn(() => new Promise(resolve => pending.push(resolve)));
  const { context, node } = composer(fetchMock);
  context.previewFixture = await fixture().preview();
  vm.runInContext('invoiceTextPreview = previewFixture;', context);
  node('sms-compose-body').value = context.previewFixture.body;
  const first = context.draftInvoiceTextWithAi();
  context.invoiceTextChanged(); // User edits the draft, invalidating the first request.
  const second = context.draftInvoiceTextWithAi();
  pending[0]({ ok: true, json: async () => ({ draft: 'Old result' }) });
  await first;
  expect(node('sms-reviewed').disabled).toBe(true);
  expect(node('sms-compose-body').value).not.toContain('Old result');
  pending[1]({ ok: true, json: async () => ({ draft: 'New result' }) });
  await second;
  expect(node('sms-reviewed').checked).toBe(false);
  expect(node('sms-reviewed').disabled).toBe(false);
  expect(node('sms-compose-body').value).toContain('New result');
});
