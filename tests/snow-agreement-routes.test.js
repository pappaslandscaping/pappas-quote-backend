jest.mock('../services/homeworks/client', () => ({ formatPropertyAddress: jest.fn(), queryHomeWorksGraphql: jest.fn() }));
jest.mock('../services/copilot/website-leads', () => ({ classifyServiceArea: jest.fn(), syncWebsiteLead: jest.fn() }));
jest.mock('../lib/email-renderer', () => ({ buildServiceAgreementEmailV4: jest.fn() }));
const createRoutes = require('../routes/quotes');
const agreement = require('../public/js/snow-agreement');
const { queryHomeWorksGraphql, formatPropertyAddress } = require('../services/homeworks/client');

describe('explicitly selected unsigned snow agreement repairs', () => {
  const originalRepair = process.env.SNOW_AGREEMENT_REPAIR_NUMBERS;
  const originalSecret = process.env.CRON_SECRET;
  afterEach(() => {
    if (originalRepair === undefined) delete process.env.SNOW_AGREEMENT_REPAIR_NUMBERS;
    else process.env.SNOW_AGREEMENT_REPAIR_NUMBERS = originalRepair;
    if (originalSecret === undefined) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = originalSecret;
  });
  async function poll(overrides = {}, selected = true) {
    process.env.SNOW_AGREEMENT_REPAIR_NUMBERS = selected ? '1726' : '';
    process.env.CRON_SECRET = 'test-secret';
    const old = { id: 42, customer_email: 'test@example.com', contract_signed_at: null,
      contract_document_hash: 'old-hash', contract_document: { kind: 'snow', source: { estimate_id: 888 } }, ...overrides };
    formatPropertyAddress.mockReturnValue('Example property');
    queryHomeWorksGraphql.mockResolvedValue({ estimates: [{ id:888, number:1726, total:'864', acceptedAt:'2026-10-25T21:40:49Z',
      customer:{fullName:'Test Client',email:'test@example.com'}, lineItems:[{name:'Snow Removal Contract',
        description:'2026-2027: December 1, 2026 through March 31, 2027. Seasonal price.', subtotalWithTax:'864', quantity:'1', property:{id:77}}] }] });
    const pool = { query: jest.fn(async sql => ({rows: sql.startsWith('SELECT') ? [old] : sql.startsWith('UPDATE') ? [{id:42}] : []})) };
    const sendEmail = jest.fn();
    const router = createRoutes({pool, sendEmail, authenticateToken:(req,res,next)=>next(), ensureQuoteEventsTable:jest.fn()});
    const route = router.stack.find(l => l.route?.path === '/api/cron/homeworks-accepted-estimates').route;
    const res = { status:jest.fn().mockReturnThis(),json:jest.fn() };
    await route.stack[0].handle({get:()=> 'test-secret',query:{},body:{}},res);
    return {pool,sendEmail,res};
  }
  test('refreshes a selected unsigned snapshot with optimistic concurrency and no duplicate email', async () => {
    const {pool,sendEmail} = await poll();
    const update = pool.query.mock.calls.find(([sql]) => sql.startsWith('UPDATE'));
    expect(update[0]).toContain('contract_signed_at IS NULL');
    expect(update[0]).toContain('contract_document_hash IS NOT DISTINCT FROM $6');
    expect(update[1][5]).toBe('old-hash');
    expect(JSON.parse(update[1][0]).total).toBe(864);
    expect(sendEmail).not.toHaveBeenCalled();
  });
  test.each([
    [{contract_signed_at:'2026-10-25'},true],
    [{},false],
    [{customer_email:'someone-else@example.com'},true],
    [{contract_document:{kind:'snow',source:{estimate_id:999}}},true]
  ])('does not modify signed, unselected, or mismatched agreements (%j)', async (overrides,selected) => {
    const {pool,sendEmail} = await poll(overrides,selected);
    expect(pool.query.mock.calls.some(([sql]) => sql.startsWith('UPDATE'))).toBe(false);
    expect(sendEmail).not.toHaveBeenCalled();
  });
});

function setup(quote) {
  const pool = { query: jest.fn(async () => ({ rows: [quote] })) };
  const sendEmail = jest.fn();
  const router = createRoutes({ pool, sendEmail, authenticateToken: (req, res, next) => next(), ensureQuoteEventsTable: jest.fn() });
  const layer = router.stack.find(layer => layer.route?.path === '/api/sent-quotes/:id/sign-contract');
  const res = { status: jest.fn().mockReturnThis(), json: jest.fn() };
  return { pool, sendEmail, res, run: body => layer.route.stack[0].handle({ params: { id: '42' }, body, headers: {}, socket: {} }, res) };
}
const stored = { id: 42, sign_token: 'valid-token', contract_document_hash: 'expected-hash', contract_document: { kind: 'snow', version: agreement.VERSION } };
const payload = { signature_data: 'Client', printed_name: 'Client', consent_given: true };
test('an unsigned legacy snow estimate cannot be signed as a landscaping agreement', async () => {
  const test = setup({ ...stored, contract_document: null, services: [{ name: 'Snow Removal Contract' }] });
  await test.run(payload);
  expect(test.res.status).toHaveBeenCalledWith(409);
  expect(test.pool.query).toHaveBeenCalledTimes(1);
  expect(test.sendEmail).not.toHaveBeenCalled();
});
test('snow signing requires the emailed token', async () => {
  const test = setup(stored); await test.run(payload);
  expect(test.res.status).toHaveBeenCalledWith(403); expect(test.pool.query).toHaveBeenCalledTimes(1); expect(test.sendEmail).not.toHaveBeenCalled();
});
test('snow signing rejects a stale document hash', async () => {
  const test = setup(stored); await test.run({ ...payload, sign_token: 'valid-token', contract_document_hash: 'outdated' });
  expect(test.res.status).toHaveBeenCalledWith(409); expect(test.pool.query).toHaveBeenCalledTimes(1);
});
test('a second concurrent signature cannot overwrite the first', async () => {
  const test = setup(stored); test.pool.query.mockResolvedValueOnce({ rows: [stored] }).mockResolvedValueOnce({ rows: [] });
  await test.run({ ...payload, sign_token: 'valid-token', contract_document_hash: 'expected-hash' });
  expect(test.pool.query.mock.calls[1][0]).toContain('contract_signed_at IS NULL');
  expect(test.res.status).toHaveBeenCalledWith(409); expect(test.sendEmail).not.toHaveBeenCalled();
});

test('an official accepted snow estimate creates a versioned document when enabled', async () => {
  const oldFlag = process.env.SNOW_AGREEMENT_ENABLED;
  process.env.SNOW_AGREEMENT_ENABLED = 'true';
  try {
    let inserted;
    const pool = { query: jest.fn(async (sql, values) => {
      if (sql.includes('SELECT * FROM sent_quotes')) return { rows: [] };
      if (sql.includes('SELECT id FROM customers')) return { rows: [{ id: 3 }] };
      if (sql.includes('INSERT INTO sent_quotes')) { inserted = values; return { rows: [{ id: 50 }] }; }
      return { rows: [] };
    }) };
    const sendEmail = jest.fn();
    const serverError = jest.fn();
    const router = createRoutes({ pool, sendEmail, serverError, authenticateToken: (req, res, next) => next(), ensureQuoteEventsTable: jest.fn() });
    const route = router.stack.find(layer => layer.route?.path === '/api/copilotcrm/estimate-accepted').route;
    const res = { status: jest.fn().mockReturnThis(), json: jest.fn() };
    await route.stack[route.stack.length - 1].handle({ body: {
      integration_source: 'homeworks_official', homeworks_estimate_id: 888, estimate_number: '1500', estimate_amount: 675,
      customer_name: 'Preview Client', email: 'preview@example.com', address: 'Example property',
      services: [{ name: 'Snow Removal Contract', description: '2026-2027: December 1, 2026 through March 31, 2027. Seasonal total, due November 1.', price: 675, property_id: 77, property_address: 'Actual service property' }]
    } }, res);
    expect(serverError).not.toHaveBeenCalled();
    expect(inserted).toBeDefined();
    const doc = JSON.parse(inserted[10]);
    expect(doc.version).toBe(agreement.VERSION);
    expect(doc.source.estimate_id).toBe(888);
    expect(doc.services[0].property_address).toBe('Actual service property');
    expect(inserted[11]).toMatch(/^[a-f0-9]{64}$/);
    expect(sendEmail).toHaveBeenCalledTimes(1);
  } finally {
    if (oldFlag === undefined) delete process.env.SNOW_AGREEMENT_ENABLED;
    else process.env.SNOW_AGREEMENT_ENABLED = oldFlag;
  }
});
