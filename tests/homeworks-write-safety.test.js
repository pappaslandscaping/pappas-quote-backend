const records = require('../services/homeworks/records');
const jobs = require('../services/homeworks/jobs');
const invoices = require('../services/homeworks/invoice-actions');
const agreements = require('../services/homeworks/agreements');
const customers = require('../services/homeworks/customers');

afterEach(() => jest.restoreAllMocks());

describe('official visit write safety', () => {
  const visit = {
    id: 41,
    customerId: 9,
    startDate: '2026-11-17',
    status: 'OPEN',
    isInvoiceable: true,
    title: 'Mowing',
    total: '40',
  };

  test('invoiceable completion cannot occur without a separate billing confirmation', async () => {
    const queryImpl = jest.fn().mockResolvedValue({ event: visit });
    await expect(jobs.changeStatus({ queryImpl }, 'homeworks:41', 'completed')).rejects.toThrow(
      'Confirm the billing effect',
    );
    expect(queryImpl).toHaveBeenCalledTimes(1);
  });

  test('completion is read back and does not create a second invoice', async () => {
    const queryImpl = jest
      .fn()
      .mockResolvedValueOnce({ event: visit })
      .mockResolvedValueOnce({ closeEvent: { id: 41, status: 'CLOSED' } })
      .mockResolvedValueOnce({ event: { ...visit, status: 'CLOSED', invoiceId: 501 } });
    const result = await jobs.changeStatus({ queryImpl }, 'homeworks:41', 'completed', {
      confirm_billing: true,
    });
    expect(result.job.invoice_id).toBe(501);
    expect(queryImpl.mock.calls.map(([request]) => request.query).join('\n')).not.toContain('createInvoice');
  });

  test('an already closed visit is an idempotent read without another close mutation', async () => {
    const queryImpl = jest.fn().mockResolvedValue({ event: { ...visit, status: 'CLOSED' } });
    const result = await jobs.changeStatus({ queryImpl }, 'homeworks:41', 'completed', {
      confirm_billing: true,
    });
    expect(result.alreadyApplied).toBe(true);
    expect(queryImpl).toHaveBeenCalledTimes(1);
  });

  test('a mutation response alone cannot confirm completion', async () => {
    const queryImpl = jest
      .fn()
      .mockResolvedValueOnce({ event: visit })
      .mockResolvedValueOnce({ closeEvent: { id: 41, status: 'CLOSED' } })
      .mockResolvedValueOnce({ event: visit });
    await expect(
      jobs.changeStatus({ queryImpl }, 'homeworks:41', 'completed', { confirm_billing: true }),
    ).rejects.toThrow('did not confirm');
  });

  test('new visits require an explicit billing choice before any source lookup', async () => {
    const queryImpl = jest.fn();
    await expect(jobs.create({ queryImpl }, { customer_name: 'Jane' })).rejects.toThrow('Choose whether');
    expect(queryImpl).not.toHaveBeenCalled();
  });

  test('unsupported mixed edits do not partially modify the visit', async () => {
    const queryImpl = jest.fn().mockResolvedValue({ event: visit });
    await expect(
      jobs.update({ queryImpl }, 'homeworks:41', { service_type: 'Cleanup', service_price: 10 }),
    ).rejects.toThrow('reviewed Homeworks edit');
    expect(queryImpl).toHaveBeenCalledTimes(1);
  });

  test('invalid photos are rejected before any upload or attachment mutation', async () => {
    const queryImpl = jest.fn().mockResolvedValue({ event: visit });
    await expect(
      jobs.uploadPhotos({ queryImpl }, 'homeworks:41', [{ mimetype: 'text/html', buffer: Buffer.from('x') }]),
    ).rejects.toThrow('supported photos');
    expect(queryImpl).toHaveBeenCalledTimes(1);
  });
});

describe('invoice request receipts', () => {
  function poolWithReceipts() {
    const receipts = new Map();
    return {
      receipts,
      query: jest.fn(async (sql, params = []) => {
        if (sql.startsWith('CREATE TABLE')) return { rows: [] };
        if (sql.startsWith('INSERT INTO homeworks_action_receipts')) {
          if (receipts.has(params[0])) return { rows: [] };
          receipts.set(params[0], { fingerprint: params[1], status: 'pending' });
          return { rows: [{ request_id: params[0] }] };
        }
        if (sql.startsWith('SELECT * FROM homeworks_action_receipts'))
          return { rows: [receipts.get(params[0])] };
        if (sql.startsWith('UPDATE homeworks_action_receipts')) {
          receipts.get(params[0]).remote_id = params[1];
          return { rows: [] };
        }
        if (sql.startsWith('SELECT id,invoice_number')) return { rows: [] };
        if (sql.startsWith('INSERT INTO invoices'))
          return { rows: [{ id: 77, external_invoice_id: '501', invoice_number: '123' }] };
        throw new Error('Unexpected SQL: ' + sql);
      }),
    };
  }
  const input = {
    homeworks_customer_id: 9,
    invoice_date: '2026-11-17',
    tax_rate: 0.08,
    line_items: [{ name: 'Mowing', rate: 100, quantity: 1.25 }],
  };
  const sourceInvoice = {
    id: 501,
    number: 123,
    customerId: 9,
    date: '2026-11-17',
    status: 'DRAFT',
    total: '135',
    subtotal: '125',
    tax: '10',
    paidAmount: '0',
    isSent: false,
    lineItems: [],
  };

  function queryImpl() {
    return jest.fn(async (request) => {
      if (request.operationName === 'YardDeskCreateInvoice') return { createInvoice: { id: 501 } };
      if (request.operationName === 'YardDeskRecords' && request.query.includes('invoices('))
        return { invoices: [sourceInvoice] };
      return { customers: [{ id: 9, fullName: 'Jane' }] };
    });
  }

  test('repeating a successful invoice request reuses its source record', async () => {
    const pool = poolWithReceipts(),
      query = queryImpl(),
      context = { pool, queryImpl: query };
    const first = await invoices.create(context, input, 'request-1234567890');
    const second = await invoices.create(context, input, 'request-1234567890');
    expect(first.homeworks_invoice_id).toBe(501);
    expect(second.homeworks_invoice_id).toBe(501);
    const mutations = query.mock.calls.filter(
      ([request]) => request.operationName === 'YardDeskCreateInvoice',
    );
    expect(mutations).toHaveLength(1);
    expect(mutations[0][0].variables.input.lineItems[0]).toMatchObject({
      quantity: '1.25',
      taxPercent1: '8',
    });
  });

  test('a lost mutation response blocks a retry instead of creating a duplicate invoice', async () => {
    const pool = poolWithReceipts(),
      query = queryImpl();
    query.mockImplementation(async (request) => {
      if (request.operationName === 'YardDeskCreateInvoice') throw new Error('Response lost');
      return { customers: [{ id: 9, fullName: 'Jane' }] };
    });
    await expect(invoices.create({ pool, queryImpl: query }, input, 'request-1234567890')).rejects.toThrow(
      'Response lost',
    );
    await expect(invoices.create({ pool, queryImpl: query }, input, 'request-1234567890')).rejects.toThrow(
      'Do not submit it again',
    );
    expect(
      query.mock.calls.filter(([request]) => request.operationName === 'YardDeskCreateInvoice'),
    ).toHaveLength(1);
  });
});

describe('invoice selection and conversion safety', () => {
  test('the selected customer takes precedence over an edited email', async () => {
    const remote = { id: 9, fullName: 'Jane' };
    const resolve = jest.spyOn(customers, 'resolve').mockResolvedValue({ remote });
    const owner = jest.spyOn(records, 'uniqueCustomer');
    const pool = { query: jest.fn().mockResolvedValue({ rows: [] }) };
    // Stop at validation, before creating any invoice; identity must already be canonical.
    await expect(
      invoices.create(
        { pool },
        { customer_id: 44, customer_email: 'another@example.com', line_items: [] },
        'request-1234567890',
      ),
    ).rejects.toThrow('Review invoice services');
    expect(resolve).toHaveBeenCalledWith(expect.objectContaining({ pool }), 44);
    expect(owner).not.toHaveBeenCalled();
  });

  test('a concurrent estimate conversion cannot create a second invoice', async () => {
    const pool = {
      query: jest.fn(async (sql) => ({
        rows: sql.startsWith('SELECT quote_number') ? [{ quote_number: 123 }] : [],
      })),
    };
    const queryImpl = jest.fn(async (request) =>
      request.operationName === 'YardDeskEstimateInvoice'
        ? { estimate: { id: 8, status: 'ACCEPTED', invoiceId: null } }
        : { estimates: [{ id: 8, number: 123 }] },
    );
    await expect(invoices.fromQuote({ pool, queryImpl }, 1)).rejects.toThrow('needs verification');
    expect(
      queryImpl.mock.calls.some(([request]) => request.operationName === 'YardDeskInvoiceEstimate'),
    ).toBe(false);
  });
});

describe('agreement attachment recovery', () => {
  const quote = {
    id: 1,
    quote_number: '123',
    contract_signed_at: '2026-11-17',
    customer_email: 'jane@example.com',
  };
  const estimate = {
    id: 8,
    number: 123,
    customerId: 9,
    status: 'ACCEPTED',
    customer: { email: 'jane@example.com' },
    lineItems: [{ isIncluded: true, property: { id: 90 } }],
  };

  test('uncertain attachment attempts are never blindly retried', async () => {
    const pool = {
      query: jest.fn(async (sql) => ({
        rows: sql.startsWith('SELECT') ? [{ status: 'needs_verification' }] : [],
      })),
    };
    const queryImpl = jest.fn();
    await expect(
      agreements.attachAgreement({ pool, queryImpl }, { quote, estimate, pdfBytes: Buffer.from('%PDF') }),
    ).rejects.toThrow('needs verification');
    expect(queryImpl).not.toHaveBeenCalled();
  });

  test('confirmed attachments are checked on the property without uploading another copy', async () => {
    const pool = {
      query: jest.fn(async (sql) => ({
        rows: sql.startsWith('SELECT') ? [{ status: 'confirmed', media_id: 20 }] : [],
      })),
    };
    const queryImpl = jest
      .fn()
      .mockResolvedValue({ property: { id: 90, customerId: 9, media: [{ id: 20 }] } });
    const result = await agreements.attachAgreement(
      { pool, queryImpl },
      { quote, estimate, pdfBytes: Buffer.from('%PDF') },
    );
    expect(result.property_ids).toEqual([90]);
    expect(queryImpl).toHaveBeenCalledTimes(1);
  });

  test('an ownership mismatch blocks the upload before any mutation', async () => {
    const pool = { query: jest.fn().mockResolvedValue({ rows: [] }) };
    const queryImpl = jest.fn().mockResolvedValue({ property: { id: 90, customerId: 10, media: [] } });
    await expect(
      agreements.attachAgreement({ pool, queryImpl }, { quote, estimate, pdfBytes: Buffer.from('%PDF') }),
    ).rejects.toThrow('different customer');
    expect(queryImpl).toHaveBeenCalledTimes(1);
  });
});

test('an invalid customer status cannot partially update contact data', async () => {
  jest.spyOn(records, 'uniqueCustomer').mockResolvedValue({ id: 9, address: {} });
  const pool = { query: jest.fn().mockResolvedValue({ rows: [{ id: 1, homeworks_customer_id: 9 }] }) };
  const queryImpl = jest.fn();
  await expect(
    customers.update({ pool, queryImpl }, 1, { email: 'new@example.com', status: 'BROKEN' }),
  ).rejects.toThrow('active or inactive');
  expect(queryImpl).not.toHaveBeenCalled();
});
