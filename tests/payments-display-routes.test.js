const assert = require('assert');
const createInvoiceRoutes = require('../routes/invoices');

const tests = [];

function it(name, fn) {
  tests.push({ name, fn });
}

function makeResponse() {
  return {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
  };
}

function createPool(handler) {
  return {
    async query(sql, params = []) {
      return handler(String(sql), params);
    },
  };
}

function createRouter({ pool } = {}) {
  return createInvoiceRoutes({
    pool: pool || createPool(async () => ({ rows: [] })),
    sendEmail: async () => {},
    emailTemplate: () => '',
    escapeHtml: (value) => String(value ?? ''),
    serverError: (res, error) => res.status(500).json({ success: false, error: error.message }),
    authenticateToken: (_req, _res, next) => next(),
    nextInvoiceNumber: async () => 1,
    squareClient: {},
    SQUARE_APP_ID: '',
    SQUARE_LOCATION_ID: '',
    SquareApiError: Error,
    NOTIFICATION_EMAIL: '',
    LOGO_URL: '',
    FROM_EMAIL: '',
    COMPANY_NAME: 'Pappas & Co. Landscaping',
    getCopilotToken: async () => null,
  });
}

function findRoute(router, path, method) {
  return router.stack.find((layer) => layer.route?.path === path && layer.route.methods?.[method]);
}

async function invokeRoute(router, path, method, reqOverrides = {}) {
  const layer = findRoute(router, path, method);
  if (!layer) throw new Error(`Route not found: ${method.toUpperCase()} ${path}`);

  const req = {
    method: method.toUpperCase(),
    headers: {},
    query: {},
    body: {},
    get(name) {
      const match = Object.keys(this.headers).find((key) => key.toLowerCase() === String(name).toLowerCase());
      return match ? this.headers[match] : undefined;
    },
    ...reqOverrides,
  };
  req.headers = reqOverrides.headers || req.headers;
  req.query = reqOverrides.query || req.query;
  req.body = reqOverrides.body || req.body;

  const res = makeResponse();
  for (const stackItem of layer.route.stack) {
    let nextCalled = false;
    await new Promise((resolve, reject) => {
      const next = (error) => {
        nextCalled = true;
        if (error) reject(error);
        else resolve();
      };

      Promise.resolve(stackItem.handle(req, res, next))
        .then(() => {
          if (!nextCalled) resolve();
        })
        .catch(reject);
    });

    if (!nextCalled) break;
  }

  return res;
}

const records=require('../services/homeworks/records');
const {payments}=require('../services/homeworks/invoice-views');
afterEach(()=>jest.restoreAllMocks());
const payment=(overrides={})=>({id:41,paid_at:'2026-11-01',customer_name:'Jane',amount:108,is_refund:false,extracted_invoice_number:'12345',homeworks:{invoiceId:99999,invoiceAmount:'108',creditAmount:'0'},...overrides});

it('shows the customer-facing invoice number and canonical payment ID from Homeworks',async()=>{
 jest.spyOn(records,'loadPayments').mockResolvedValue([payment()]);
 const res=await invokeRoute(createRouter(),'/api/payments','get');
 assert.strictEqual(res.statusCode,200);assert.strictEqual(res.body.source,records.SOURCE);
 assert.strictEqual(res.body.payments[0].display_invoice_number,'12345');assert.strictEqual(res.body.payments[0].payment_record_id,41);assert.strictEqual(res.body.payments[0].read_only,true);
});
it('never substitutes an internal invoice ID for a missing public number',async()=>{
 jest.spyOn(records,'loadPayments').mockResolvedValue([payment({extracted_invoice_number:null})]);
 const result=await payments({});assert.strictEqual(result.payments[0].display_invoice_number,null);assert.strictEqual(result.payments[0].invoice_number,null);
});
it('keeps an invoice and account credit allocation on one payment instead of double counting',async()=>{
 jest.spyOn(records,'loadPayments').mockResolvedValue([payment({amount:200,homeworks:{invoiceId:99999,invoiceAmount:'108',creditAmount:'92'}})]);
 const result=await payments({});assert.strictEqual(result.payments.length,1);assert.strictEqual(result.totalReceived,200);
});
it('does not present unavailable payment data as an empty account',async()=>{
 jest.spyOn(records,'loadPayments').mockRejectedValue(new Error('Homeworks unavailable'));
 const res=await invokeRoute(createRouter(),'/api/payments','get');assert.strictEqual(res.statusCode,500);assert.strictEqual(res.body.success,false);
});

describe('official payment display',()=>{for(const {name,fn}of tests)test(name,fn);});
