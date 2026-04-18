const assert = require('assert');
const createInvoiceRoutes = require('../routes/invoices');

const tests = [];
let failures = 0;

function it(name, fn) {
  tests.push({ name, fn });
}

function makeResponse() {
  return {
    statusCode: 200,
    body: null,
    headers: {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    setHeader(name, value) {
      this.headers[name.toLowerCase()] = value;
      return this;
    },
    set(name, value) {
      this.headers[String(name).toLowerCase()] = value;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
    send(payload) {
      this.body = payload;
      return this;
    },
  };
}

function buildSnapshotRow(date, taxAmount) {
  return {
    external_source: 'copilotcrm',
    basis: 'collected',
    start_date: date,
    end_date: date,
    rows: [],
    total_sales: '100.00',
    taxable_amount: '100.00',
    discount: '0.00',
    tax_amount: String(taxAmount),
    processing_fees: '0.00',
    tips: '0.00',
    external_metadata: {},
    imported_at: `${date}T20:15:00.000Z`,
    updated_at: `${date}T20:15:00.000Z`,
  };
}

function createInstructionPool({ snapshots = {}, instructions = [] } = {}) {
  const state = {
    instructions: instructions.map((row) => ({ ...row })),
    nextId: instructions.reduce((max, row) => Math.max(max, Number(row.id) || 0), 0) + 1,
  };

  function sortRows(rows) {
    return rows.sort((a, b) => {
      const dateCmp = String(b.tax_date || '').localeCompare(String(a.tax_date || ''));
      if (dateCmp) return dateCmp;
      const createdCmp = String(b.created_at || '').localeCompare(String(a.created_at || ''));
      if (createdCmp) return createdCmp;
      return (Number(b.id) || 0) - (Number(a.id) || 0);
    });
  }

  async function query(sql, params = []) {
    const text = String(sql).trim();
    if (text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK') return { rows: [] };

    if (text.includes('FROM copilot_tax_summary_snapshots')) {
      if (text.includes('start_date >= $2::date')) {
        const [basis, startDate, endDate] = params;
        const rows = Object.entries(snapshots)
          .filter(([date, row]) => row.basis === basis && date >= startDate && date <= endDate)
          .sort(([left], [right]) => right.localeCompare(left))
          .map(([, row]) => ({
            start_date: row.start_date,
            end_date: row.end_date,
            tax_amount: row.tax_amount,
            imported_at: row.imported_at,
            updated_at: row.updated_at,
          }));
        return { rows };
      }
      const startDate = params[1] || params[0];
      const row = snapshots[startDate];
      return { rows: row ? [row] : [] };
    }

    if (text.includes('FROM payments p') && text.includes('p.invoice_id IS NOT NULL')) {
      return { rows: [] };
    }

    if (text.includes('FROM tax_transfer_instructions') && text.includes('WHERE tax_date = $1') && text.includes('status = ANY')) {
      const [taxDate, statuses] = params;
      const rows = sortRows(state.instructions.filter((row) => row.tax_date === taxDate && statuses.includes(row.status)));
      return { rows: rows.slice(0, 1) };
    }

    if (text.includes('FROM tax_transfer_instructions') && text.includes('WHERE tax_date >= $1::date')) {
      const [startDate, endDate] = params;
      const rows = sortRows(state.instructions.filter((row) => row.tax_date >= startDate && row.tax_date <= endDate));
      return { rows };
    }

    if (text.includes('FROM tax_transfer_instructions') && text.includes('WHERE id = $1')) {
      const row = state.instructions.find((instruction) => instruction.id === params[0]) || null;
      return { rows: row ? [row] : [] };
    }

    if (text.includes('INSERT INTO tax_transfer_instructions')) {
      const row = {
        id: state.nextId++,
        tax_date: params[0],
        instruction_date: params[1],
        source_account_code: params[2],
        destination_account_code: params[3],
        transfer_method: params[4],
        amount_cents: params[5],
        currency: 'USD',
        recommendation_source: 'copilot_collected_tax',
        recommendation_as_of: params[6],
        tax_summary_snapshot_source: params[7],
        backend_reconstructed_tax_cents: params[8],
        variance_cents: params[9],
        status: 'pending_approval',
        memo: params[10],
        generation_trigger: params[11],
        approved_at: null,
        approved_by_user_id: null,
        approved_by_name: null,
        approved_by_email: null,
        submitted_at: null,
        submitted_by_user_id: null,
        submitted_by_name: null,
        submitted_by_email: null,
        bank_confirmation_ref: null,
        submission_note: null,
        canceled_at: null,
        canceled_by_user_id: null,
        canceled_by_name: null,
        canceled_by_email: null,
        cancellation_reason: null,
        superseded_at: null,
        superseded_by_instruction_id: null,
        superseded_reason: null,
        created_at: `${params[1]}T20:25:00.000Z`,
        updated_at: `${params[1]}T20:25:00.000Z`,
      };
      state.instructions.push(row);
      return { rows: [row] };
    }

    if (text.includes('UPDATE tax_transfer_instructions') && text.includes('SET recommendation_as_of = $2')) {
      const row = state.instructions.find((instruction) => instruction.id === params[0]);
      row.recommendation_as_of = params[1];
      row.tax_summary_snapshot_source = params[2];
      row.backend_reconstructed_tax_cents = params[3];
      row.variance_cents = params[4];
      row.updated_at = '2026-04-18T00:05:00.000Z';
      return { rows: [row] };
    }

    if (text.includes("SET status = 'superseded'")) {
      const row = state.instructions.find((instruction) => instruction.id === params[0]);
      row.status = 'superseded';
      row.superseded_by_instruction_id = params[1];
      row.superseded_reason = 'recommendation_changed_before_submission';
      row.superseded_at = '2026-04-18T00:05:00.000Z';
      row.updated_at = '2026-04-18T00:05:00.000Z';
      return { rows: [row] };
    }

    if (text.includes("SET status = 'approved'")) {
      const row = state.instructions.find((instruction) => instruction.id === params[0]);
      row.status = 'approved';
      row.approved_at = '2026-04-18T00:06:00.000Z';
      row.approved_by_user_id = params[1];
      row.approved_by_name = params[2];
      row.approved_by_email = params[3];
      row.updated_at = '2026-04-18T00:06:00.000Z';
      return { rows: [row] };
    }

    if (text.includes("SET status = 'submitted'")) {
      const row = state.instructions.find((instruction) => instruction.id === params[0]);
      row.status = 'submitted';
      row.submitted_at = '2026-04-18T00:07:00.000Z';
      row.submitted_by_user_id = params[1];
      row.submitted_by_name = params[2];
      row.submitted_by_email = params[3];
      row.bank_confirmation_ref = params[4];
      row.submission_note = params[5];
      row.updated_at = '2026-04-18T00:07:00.000Z';
      return { rows: [row] };
    }

    if (text.includes("SET status = 'canceled'")) {
      const row = state.instructions.find((instruction) => instruction.id === params[0]);
      row.status = 'canceled';
      row.canceled_at = '2026-04-18T00:08:00.000Z';
      row.canceled_by_user_id = params[1];
      row.canceled_by_name = params[2];
      row.canceled_by_email = params[3];
      row.cancellation_reason = params[4];
      row.updated_at = '2026-04-18T00:08:00.000Z';
      return { rows: [row] };
    }

    throw new Error(`Unexpected SQL: ${text}`);
  }

  return {
    state,
    async query(sql, params = []) {
      return query(sql, params);
    },
    async connect() {
      return {
        query,
        release() {},
      };
    },
  };
}

function createRouter({ pool, getCopilotToken } = {}) {
  return createInvoiceRoutes({
    pool: pool || createInstructionPool(),
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
    getCopilotToken: getCopilotToken || (async () => null),
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
    params: {},
    user: null,
    get(name) {
      const match = Object.keys(this.headers).find((key) => key.toLowerCase() === String(name).toLowerCase());
      return match ? this.headers[match] : undefined;
    },
    ...reqOverrides,
  };
  req.headers = reqOverrides.headers || req.headers;
  req.query = reqOverrides.query || req.query;
  req.body = reqOverrides.body || req.body;
  req.params = reqOverrides.params || req.params;

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

function withMockNow(isoString, fn) {
  const RealDate = Date;
  class MockDate extends RealDate {
    constructor(...args) {
      if (args.length) super(...args);
      else super(isoString);
    }
    static now() {
      return new RealDate(isoString).getTime();
    }
  }
  global.Date = MockDate;
  return Promise.resolve()
    .then(fn)
    .finally(() => {
      global.Date = RealDate;
    });
}

const ADMIN_USER = {
  id: 7,
  email: 'admin@example.com',
  name: 'Admin User',
  role: 'owner',
  isAdmin: true,
};

function buildInstructionRow(overrides = {}) {
  return {
    id: 1,
    tax_date: '2026-04-17',
    instruction_date: '2026-04-18',
    source_account_code: 'chase_business_checking',
    destination_account_code: 'huntington_tax',
    transfer_method: 'chase_linked_external_transfer',
    amount_cents: 1842,
    currency: 'USD',
    recommendation_source: 'copilot_collected_tax',
    recommendation_as_of: '2026-04-17T20:15:00.000Z',
    tax_summary_snapshot_source: 'persisted_copilot_snapshot',
    backend_reconstructed_tax_cents: 0,
    variance_cents: 1842,
    status: 'pending_approval',
    memo: 'Daily sales tax transfer for 2026-04-17',
    generation_trigger: 'manual',
    approved_at: null,
    approved_by_user_id: null,
    approved_by_name: null,
    approved_by_email: null,
    submitted_at: null,
    submitted_by_user_id: null,
    submitted_by_name: null,
    submitted_by_email: null,
    bank_confirmation_ref: null,
    submission_note: null,
    canceled_at: null,
    canceled_by_user_id: null,
    canceled_by_name: null,
    canceled_by_email: null,
    cancellation_reason: null,
    superseded_at: null,
    superseded_by_instruction_id: null,
    superseded_reason: null,
    created_at: '2026-04-18T00:05:00.000Z',
    updated_at: '2026-04-18T00:05:00.000Z',
    ...overrides,
  };
}

it('returns no_instruction for yesterday when a recommendation exists but no instruction has been generated', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-16': buildSnapshotRow('2026-04-16', 18.42),
    },
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T01:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-status', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.tax_date, '2026-04-16');
  assert.strictEqual(res.body.ui_state, 'no_instruction');
  assert.strictEqual(res.body.recommendation.recommended_transfer_amount, 18.42);
});

it('returns a before-cutoff missing-instruction alert when yesterday has a recommendation but no instruction', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-17': buildSnapshotRow('2026-04-17', 18.42),
    },
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T15:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-alert', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.tax_date, '2026-04-17');
  assert.strictEqual(res.body.ui_state, 'no_instruction');
  assert.strictEqual(res.body.alert_state, 'missing_instruction');
  assert.strictEqual(res.body.tone, 'warn');
  assert.strictEqual(res.body.cutoff_passed, false);
});

it('treats exactly 12:00 PM America/New_York as after cutoff for pending instructions', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-17': buildSnapshotRow('2026-04-17', 18.42),
    },
    instructions: [
      buildInstructionRow(),
    ],
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T16:00:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-alert', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.alert_state, 'awaiting_approval');
  assert.strictEqual(res.body.tone, 'error');
  assert.strictEqual(res.body.cutoff_passed, true);
});

it('stays snapshot-only and returns an after-cutoff missing alert when yesterday has no persisted recommendation', async () => {
  const pool = createInstructionPool();
  const router = createRouter({
    pool,
    getCopilotToken: async () => {
      throw new Error('should not attempt live fetch');
    },
  });

  const res = await withMockNow('2026-04-18T16:05:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-alert', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.ui_state, 'blocked_missing_recommendation');
  assert.strictEqual(res.body.alert_state, 'missing_instruction');
  assert.strictEqual(res.body.tone, 'error');
  assert.strictEqual(res.body.cutoff_passed, true);
});

it('returns an awaiting-approval alert after cutoff for pending instructions', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-17': buildSnapshotRow('2026-04-17', 18.42),
    },
    instructions: [
      buildInstructionRow(),
    ],
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T16:05:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-alert', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.ui_state, 'pending_approval');
  assert.strictEqual(res.body.alert_state, 'awaiting_approval');
  assert.strictEqual(res.body.tone, 'error');
  assert.strictEqual(res.body.instruction_status, 'pending_approval');
});

it('returns an approved-but-not-submitted alert before cutoff for approved instructions', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-17': buildSnapshotRow('2026-04-17', 18.42),
    },
    instructions: [
      buildInstructionRow({
        status: 'approved',
        approved_at: '2026-04-18T00:06:00.000Z',
        approved_by_user_id: 7,
        approved_by_name: 'Admin User',
        approved_by_email: 'admin@example.com',
        updated_at: '2026-04-18T00:06:00.000Z',
      }),
    ],
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T15:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-alert', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.ui_state, 'approved');
  assert.strictEqual(res.body.alert_state, 'approved_not_submitted');
  assert.strictEqual(res.body.tone, 'warn');
  assert.strictEqual(res.body.cutoff_passed, false);
});

it('returns a submitted alert after cutoff for submitted instructions', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-17': buildSnapshotRow('2026-04-17', 18.42),
    },
    instructions: [
      buildInstructionRow({
        status: 'submitted',
        approved_at: '2026-04-18T00:06:00.000Z',
        approved_by_user_id: 7,
        approved_by_name: 'Admin User',
        approved_by_email: 'admin@example.com',
        submitted_at: '2026-04-18T00:07:00.000Z',
        submitted_by_user_id: 7,
        submitted_by_name: 'Admin User',
        submitted_by_email: 'admin@example.com',
        bank_confirmation_ref: 'CHASE-99',
        updated_at: '2026-04-18T00:07:00.000Z',
      }),
    ],
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T16:05:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-alert', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.ui_state, 'submitted');
  assert.strictEqual(res.body.alert_state, 'submitted');
  assert.strictEqual(res.body.tone, 'success');
  assert.strictEqual(res.body.instruction_status, 'submitted');
});

it('returns a success alert when yesterday requires no transfer', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-17': buildSnapshotRow('2026-04-17', 0),
    },
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T15:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-alert', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.ui_state, 'no_transfer_required');
  assert.strictEqual(res.body.alert_state, 'submitted');
  assert.strictEqual(res.body.tone, 'success');
});

it('exports missing instructions as csv without attempting a live Copilot fetch', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-17': buildSnapshotRow('2026-04-17', 18.42),
    },
  });
  const router = createRouter({
    pool,
    getCopilotToken: async () => {
      throw new Error('should not attempt live fetch');
    },
  });

  const res = await invokeRoute(router, '/api/tax-transfer-instructions/export', 'get', {
    query: {
      start_date: '2026-04-17',
      end_date: '2026-04-17',
      exception_filter: 'missing_instruction',
    },
  });

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.headers['content-type'], 'text/csv; charset=utf-8');
  assert.ok(String(res.headers['content-disposition']).includes('tax-transfer-instructions-2026-04-17-through-2026-04-17-missing_instruction.csv'));
  assert.ok(String(res.body).includes('tax_date,amount,status,recommendation_as_of'));
  assert.ok(String(res.body).includes('2026-04-17,18.42,missing'));
  assert.ok(String(res.body).includes('missing_instruction'));
});

it('exports exception-focused csv rows for late submitted, superseded, canceled, and recommendation-changed instructions', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-12': buildSnapshotRow('2026-04-12', 14.10),
      '2026-04-13': buildSnapshotRow('2026-04-13', 15.25),
      '2026-04-14': buildSnapshotRow('2026-04-14', 16.75),
      '2026-04-15': buildSnapshotRow('2026-04-15', 18.42),
    },
    instructions: [
      buildInstructionRow({
        id: 41,
        tax_date: '2026-04-15',
        instruction_date: '2026-04-16',
        status: 'submitted',
        approved_at: '2026-04-16T13:30:00.000Z',
        approved_by_name: 'Ops Admin',
        submitted_at: '2026-04-16T16:30:00.000Z',
        submitted_by_name: 'Ops Admin',
        bank_confirmation_ref: 'CHASE-LATE',
        updated_at: '2026-04-16T16:30:00.000Z',
      }),
      buildInstructionRow({
        id: 42,
        tax_date: '2026-04-14',
        instruction_date: '2026-04-15',
        amount_cents: 1200,
        status: 'submitted',
        approved_at: '2026-04-15T13:30:00.000Z',
        approved_by_name: 'Ops Admin',
        submitted_at: '2026-04-15T14:00:00.000Z',
        submitted_by_name: 'Ops Admin',
        bank_confirmation_ref: 'CHASE-CHANGE',
        updated_at: '2026-04-15T14:00:00.000Z',
      }),
      buildInstructionRow({
        id: 43,
        tax_date: '2026-04-13',
        instruction_date: '2026-04-14',
        status: 'superseded',
        superseded_reason: 'recommendation_changed_before_submission',
        superseded_by_instruction_id: 99,
        superseded_at: '2026-04-14T13:00:00.000Z',
        updated_at: '2026-04-14T13:00:00.000Z',
      }),
      buildInstructionRow({
        id: 44,
        tax_date: '2026-04-12',
        instruction_date: '2026-04-13',
        status: 'canceled',
        cancellation_reason: 'Duplicate instruction',
        canceled_at: '2026-04-13T14:00:00.000Z',
        canceled_by_name: 'Ops Admin',
        updated_at: '2026-04-13T14:00:00.000Z',
      }),
    ],
  });
  const router = createRouter({ pool });

  const res = await invokeRoute(router, '/api/tax-transfer-instructions/export', 'get', {
    query: {
      start_date: '2026-04-12',
      end_date: '2026-04-15',
      exception_filter: 'exceptions',
    },
  });

  assert.strictEqual(res.statusCode, 200);
  const csv = String(res.body);
  assert.ok(csv.includes('2026-04-15,18.42,submitted'));
  assert.ok(csv.includes('submitted_after_cutoff'));
  assert.ok(csv.includes('2026-04-14,12.00,submitted'));
  assert.ok(csv.includes('recommendation_changed_after_submission'));
  assert.ok(csv.includes('2026-04-13,18.42,superseded'));
  assert.ok(csv.includes('recommendation_changed_before_submission'));
  assert.ok(csv.includes('2026-04-12,18.42,canceled'));
  assert.ok(csv.includes('Duplicate instruction'));
});

it('generates one pending instruction and is idempotent when the amount has not changed', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-16': buildSnapshotRow('2026-04-16', 18.42),
    },
  });
  const router = createRouter({ pool });

  const first = await withMockNow('2026-04-18T01:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/generate', 'post', { user: ADMIN_USER }));
  assert.strictEqual(first.statusCode, 200);
  assert.strictEqual(first.body.action, 'created');
  assert.strictEqual(first.body.instruction.status, 'pending_approval');
  assert.strictEqual(pool.state.instructions.length, 1);

  const second = await withMockNow('2026-04-18T01:31:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/generate', 'post', { user: ADMIN_USER }));
  assert.strictEqual(second.statusCode, 200);
  assert.strictEqual(second.body.action, 'unchanged');
  assert.strictEqual(pool.state.instructions.length, 1);
  assert.strictEqual(pool.state.instructions[0].status, 'pending_approval');
});

it('supersedes an unsubmitted instruction when the recommended amount changes', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-16': buildSnapshotRow('2026-04-16', 22.50),
    },
    instructions: [{
      id: 1,
      tax_date: '2026-04-16',
      instruction_date: '2026-04-18',
      source_account_code: 'chase_business_checking',
      destination_account_code: 'huntington_tax',
      transfer_method: 'chase_linked_external_transfer',
      amount_cents: 1842,
      currency: 'USD',
      recommendation_source: 'copilot_collected_tax',
      recommendation_as_of: '2026-04-17T20:15:00.000Z',
      tax_summary_snapshot_source: 'persisted_copilot_snapshot',
      backend_reconstructed_tax_cents: 0,
      variance_cents: 1842,
      status: 'approved',
      memo: 'Daily sales tax transfer for 2026-04-16',
      generation_trigger: 'manual',
      approved_at: '2026-04-18T00:06:00.000Z',
      approved_by_user_id: 7,
      approved_by_name: 'Admin User',
      approved_by_email: 'admin@example.com',
      submitted_at: null,
      submitted_by_user_id: null,
      submitted_by_name: null,
      submitted_by_email: null,
      bank_confirmation_ref: null,
      submission_note: null,
      canceled_at: null,
      canceled_by_user_id: null,
      canceled_by_name: null,
      canceled_by_email: null,
      cancellation_reason: null,
      superseded_at: null,
      superseded_by_instruction_id: null,
      superseded_reason: null,
      created_at: '2026-04-18T00:05:00.000Z',
      updated_at: '2026-04-18T00:06:00.000Z',
    }],
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T01:35:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/generate', 'post', { user: ADMIN_USER }));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.action, 'superseded_and_created');
  assert.strictEqual(pool.state.instructions.length, 2);
  const oldRow = pool.state.instructions.find((row) => row.id === 1);
  const newRow = pool.state.instructions.find((row) => row.id !== 1);
  assert.strictEqual(oldRow.status, 'superseded');
  assert.strictEqual(oldRow.superseded_by_instruction_id, newRow.id);
  assert.strictEqual(newRow.status, 'pending_approval');
  assert.strictEqual(newRow.amount_cents, 2250);
});

it('skips generation when yesterday has no Copilot collected-tax snapshot', async () => {
  const pool = createInstructionPool();
  const router = createRouter({
    pool,
    getCopilotToken: async () => {
      throw new Error('should not attempt live fetch');
    },
  });

  const res = await withMockNow('2026-04-18T01:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/generate', 'post', { user: ADMIN_USER }));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.action, 'skipped_missing_recommendation');
  assert.strictEqual(pool.state.instructions.length, 0);
});

it('reports submitted_recommendation_changed without replacing a submitted row', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-16': buildSnapshotRow('2026-04-16', 22.50),
    },
    instructions: [{
      id: 15,
      tax_date: '2026-04-16',
      instruction_date: '2026-04-17',
      source_account_code: 'chase_business_checking',
      destination_account_code: 'huntington_tax',
      transfer_method: 'chase_linked_external_transfer',
      amount_cents: 1842,
      currency: 'USD',
      recommendation_source: 'copilot_collected_tax',
      recommendation_as_of: '2026-04-16T20:15:00.000Z',
      tax_summary_snapshot_source: 'persisted_copilot_snapshot',
      backend_reconstructed_tax_cents: 0,
      variance_cents: 1842,
      status: 'submitted',
      memo: 'Daily sales tax transfer for 2026-04-16',
      generation_trigger: 'manual',
      approved_at: '2026-04-17T00:06:00.000Z',
      approved_by_user_id: 7,
      approved_by_name: 'Admin User',
      approved_by_email: 'admin@example.com',
      submitted_at: '2026-04-17T00:07:00.000Z',
      submitted_by_user_id: 7,
      submitted_by_name: 'Admin User',
      submitted_by_email: 'admin@example.com',
      bank_confirmation_ref: 'CHASE-15',
      submission_note: null,
      canceled_at: null,
      canceled_by_user_id: null,
      canceled_by_name: null,
      canceled_by_email: null,
      cancellation_reason: null,
      superseded_at: null,
      superseded_by_instruction_id: null,
      superseded_reason: null,
      created_at: '2026-04-17T00:05:00.000Z',
      updated_at: '2026-04-17T00:07:00.000Z',
    }],
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T01:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-status', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.ui_state, 'submitted_recommendation_changed');
  assert.strictEqual(res.body.instruction.status, 'submitted');
  assert.strictEqual(pool.state.instructions.length, 1);
});

it('does not replace a submitted instruction when a new recommendation exists', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-16': buildSnapshotRow('2026-04-16', 22.50),
    },
    instructions: [{
      id: 9,
      tax_date: '2026-04-16',
      instruction_date: '2026-04-18',
      source_account_code: 'chase_business_checking',
      destination_account_code: 'huntington_tax',
      transfer_method: 'chase_linked_external_transfer',
      amount_cents: 1842,
      currency: 'USD',
      recommendation_source: 'copilot_collected_tax',
      recommendation_as_of: '2026-04-17T20:15:00.000Z',
      tax_summary_snapshot_source: 'persisted_copilot_snapshot',
      backend_reconstructed_tax_cents: 0,
      variance_cents: 1842,
      status: 'submitted',
      memo: 'Daily sales tax transfer for 2026-04-16',
      generation_trigger: 'manual',
      approved_at: '2026-04-18T00:06:00.000Z',
      approved_by_user_id: 7,
      approved_by_name: 'Admin User',
      approved_by_email: 'admin@example.com',
      submitted_at: '2026-04-18T00:07:00.000Z',
      submitted_by_user_id: 7,
      submitted_by_name: 'Admin User',
      submitted_by_email: 'admin@example.com',
      bank_confirmation_ref: 'CHASE-123',
      submission_note: null,
      canceled_at: null,
      canceled_by_user_id: null,
      canceled_by_name: null,
      canceled_by_email: null,
      cancellation_reason: null,
      superseded_at: null,
      superseded_by_instruction_id: null,
      superseded_reason: null,
      created_at: '2026-04-18T00:05:00.000Z',
      updated_at: '2026-04-18T00:07:00.000Z',
    }],
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-18T01:35:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/generate', 'post', { user: ADMIN_USER }));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.action, 'blocked_submitted_exists');
  assert.strictEqual(pool.state.instructions.length, 1);
  assert.strictEqual(pool.state.instructions[0].status, 'submitted');
});

it('requires a human admin for approval routes', async () => {
  const pool = createInstructionPool();
  const router = createRouter({ pool });

  const res = await invokeRoute(router, '/api/tax-transfer-instructions/:id/approve', 'post', {
    params: { id: '1' },
    user: { id: 5, isAdmin: true, isServiceToken: true, email: 'token@example.com' },
  });

  assert.strictEqual(res.statusCode, 403);
  assert.deepStrictEqual(res.body, {
    success: false,
    error: 'Human admin access required',
  });
});

it('records approve then submit audit fields and blocks invalid submit state', async () => {
  const pool = createInstructionPool({
    instructions: [{
      id: 11,
      tax_date: '2026-04-17',
      instruction_date: '2026-04-18',
      source_account_code: 'chase_business_checking',
      destination_account_code: 'huntington_tax',
      transfer_method: 'chase_linked_external_transfer',
      amount_cents: 1842,
      currency: 'USD',
      recommendation_source: 'copilot_collected_tax',
      recommendation_as_of: '2026-04-17T20:15:00.000Z',
      tax_summary_snapshot_source: 'persisted_copilot_snapshot',
      backend_reconstructed_tax_cents: 0,
      variance_cents: 1842,
      status: 'pending_approval',
      memo: 'Daily sales tax transfer for 2026-04-17',
      generation_trigger: 'manual',
      approved_at: null,
      approved_by_user_id: null,
      approved_by_name: null,
      approved_by_email: null,
      submitted_at: null,
      submitted_by_user_id: null,
      submitted_by_name: null,
      submitted_by_email: null,
      bank_confirmation_ref: null,
      submission_note: null,
      canceled_at: null,
      canceled_by_user_id: null,
      canceled_by_name: null,
      canceled_by_email: null,
      cancellation_reason: null,
      superseded_at: null,
      superseded_by_instruction_id: null,
      superseded_reason: null,
      created_at: '2026-04-18T00:05:00.000Z',
      updated_at: '2026-04-18T00:05:00.000Z',
    }],
  });
  const router = createRouter({ pool });

  const approve = await invokeRoute(router, '/api/tax-transfer-instructions/:id/approve', 'post', {
    params: { id: '11' },
    user: ADMIN_USER,
  });
  assert.strictEqual(approve.statusCode, 200);
  assert.strictEqual(approve.body.instruction.status, 'approved');
  assert.strictEqual(approve.body.instruction.approved_by_user_id, 7);

  const submit = await invokeRoute(router, '/api/tax-transfer-instructions/:id/submit', 'post', {
    params: { id: '11' },
    user: ADMIN_USER,
    body: { bank_confirmation_ref: 'CHASE-55', submission_note: 'Submitted in portal' },
  });
  assert.strictEqual(submit.statusCode, 200);
  assert.strictEqual(submit.body.instruction.status, 'submitted');
  assert.strictEqual(submit.body.instruction.bank_confirmation_ref, 'CHASE-55');

  const submitAgain = await invokeRoute(router, '/api/tax-transfer-instructions/:id/submit', 'post', {
    params: { id: '11' },
    user: ADMIN_USER,
    body: { bank_confirmation_ref: 'CHASE-56' },
  });
  assert.strictEqual(submitAgain.statusCode, 409);
  assert.strictEqual(submitAgain.body.error, 'Only approved instructions can be marked submitted');
});

it('cancels only pending or approved instructions', async () => {
  const pool = createInstructionPool({
    instructions: [{
      id: 12,
      tax_date: '2026-04-17',
      instruction_date: '2026-04-18',
      source_account_code: 'chase_business_checking',
      destination_account_code: 'huntington_tax',
      transfer_method: 'chase_linked_external_transfer',
      amount_cents: 1842,
      currency: 'USD',
      recommendation_source: 'copilot_collected_tax',
      recommendation_as_of: '2026-04-17T20:15:00.000Z',
      tax_summary_snapshot_source: 'persisted_copilot_snapshot',
      backend_reconstructed_tax_cents: 0,
      variance_cents: 1842,
      status: 'approved',
      memo: 'Daily sales tax transfer for 2026-04-17',
      generation_trigger: 'manual',
      approved_at: '2026-04-18T00:06:00.000Z',
      approved_by_user_id: 7,
      approved_by_name: 'Admin User',
      approved_by_email: 'admin@example.com',
      submitted_at: null,
      submitted_by_user_id: null,
      submitted_by_name: null,
      submitted_by_email: null,
      bank_confirmation_ref: null,
      submission_note: null,
      canceled_at: null,
      canceled_by_user_id: null,
      canceled_by_name: null,
      canceled_by_email: null,
      cancellation_reason: null,
      superseded_at: null,
      superseded_by_instruction_id: null,
      superseded_reason: null,
      created_at: '2026-04-18T00:05:00.000Z',
      updated_at: '2026-04-18T00:06:00.000Z',
    }],
  });
  const router = createRouter({ pool });

  const cancel = await invokeRoute(router, '/api/tax-transfer-instructions/:id/cancel', 'post', {
    params: { id: '12' },
    user: ADMIN_USER,
    body: { cancellation_reason: 'Duplicate instruction' },
  });
  assert.strictEqual(cancel.statusCode, 200);
  assert.strictEqual(cancel.body.instruction.status, 'canceled');

  const cancelAgain = await invokeRoute(router, '/api/tax-transfer-instructions/:id/cancel', 'post', {
    params: { id: '12' },
    user: ADMIN_USER,
    body: { cancellation_reason: 'Second try' },
  });
  assert.strictEqual(cancelAgain.statusCode, 409);
  assert.strictEqual(cancelAgain.body.error, 'Only pending or approved instructions can be canceled');
});

it('enforces CRON_SECRET and accepts the GET cron fallback for generation', async () => {
  const previousSecret = process.env.CRON_SECRET;
  process.env.CRON_SECRET = 'expected-secret';
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-16': buildSnapshotRow('2026-04-16', 18.42),
    },
  });
  const router = createRouter({ pool });

  try {
    const invalid = await invokeRoute(router, '/api/cron/tax-transfer-instructions/generate', 'post', {
      headers: { 'x-cron-secret': 'wrong' },
    });
    assert.strictEqual(invalid.statusCode, 401);

    const valid = await withMockNow('2026-04-18T01:30:00.000Z', () =>
      invokeRoute(router, '/api/cron/tax-transfer-instructions/generate', 'get', {
        query: { key: 'expected-secret' },
      }));
    assert.strictEqual(valid.statusCode, 200);
    assert.strictEqual(valid.body.action, 'created');
    assert.strictEqual(pool.state.instructions.length, 1);
    assert.strictEqual(pool.state.instructions[0].generation_trigger, 'cron');
  } finally {
    if (previousSecret == null) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = previousSecret;
  }
});

it('uses America/New_York business dates for yesterday-status around UTC midnight', async () => {
  const pool = createInstructionPool({
    snapshots: {
      '2026-04-16': buildSnapshotRow('2026-04-16', 14.10),
    },
  });
  const router = createRouter({ pool });

  const res = await withMockNow('2026-04-17T02:30:00.000Z', () =>
    invokeRoute(router, '/api/tax-transfer-instructions/yesterday-status', 'get'));

  assert.strictEqual(res.statusCode, 200);
  assert.strictEqual(res.body.today_business_date, '2026-04-16');
  assert.strictEqual(res.body.tax_date, '2026-04-15');
  assert.strictEqual(res.body.ui_state, 'blocked_missing_recommendation');
});

async function run() {
  for (const test of tests) {
    try {
      await test.fn();
      process.stdout.write('.');
    } catch (error) {
      failures += 1;
      process.stdout.write('F');
      process.stderr.write(`\n[${test.name}] ${error.stack}\n`);
    }
  }

  process.stdout.write('\n');
  if (failures > 0) process.exit(1);
  console.log('tax-transfer-instructions-routes');
}

run();
