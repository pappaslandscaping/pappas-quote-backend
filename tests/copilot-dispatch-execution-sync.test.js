const createJobRoutes = require('../routes/jobs');

describe('copilot dispatch execution sync helpers', () => {
  const {
    normalizeCopilotExecutionStatus,
    mapCopilotExecutionMirror,
    canonicalizeCopilotExecutionMirror,
    hashCopilotExecutionPayload,
    syncCopilotDispatchExecutionRecords,
  } = createJobRoutes.__testables;

  test('maps Copilot raw statuses into YardDesk execution statuses', () => {
    expect(normalizeCopilotExecutionStatus('scheduled')).toBe('pending');
    expect(normalizeCopilotExecutionStatus('assigned')).toBe('pending');
    expect(normalizeCopilotExecutionStatus('started')).toBe('in_progress');
    expect(normalizeCopilotExecutionStatus('in-progress')).toBe('in_progress');
    expect(normalizeCopilotExecutionStatus('closed')).toBe('completed');
    expect(normalizeCopilotExecutionStatus('no_access')).toBe('skipped');
    expect(normalizeCopilotExecutionStatus('canceled')).toBe('cancelled');
    expect(normalizeCopilotExecutionStatus('unknown')).toBeNull();
  });

  test('matches by visit id before falling back to copilot_job_id + job_date', async () => {
    const poolClient = {
      query: jest.fn()
        .mockResolvedValueOnce({
          rows: [{
            id: 11,
            copilot_visit_id: 'visit-11',
            copilot_event_updated_at: null,
            copilot_payload_hash: null,
          }],
        })
        .mockResolvedValueOnce({
          rows: [{ id: 11, copilot_visit_id: 'visit-11' }],
        }),
    };

    const result = await syncCopilotDispatchExecutionRecords({
      records: [{
        visit_id: 'visit-11',
        job_id: 'job-11',
        job_date: '2026-04-18',
        status: 'started',
      }],
      poolClient,
    });

    expect(result).toMatchObject({
      fetched: 1,
      matched: 1,
      updated: 1,
      skipped_unmatched: 0,
    });
    expect(poolClient.query.mock.calls[0][0]).toContain('WHERE copilot_visit_id = $1');
    expect(poolClient.query.mock.calls[0][1]).toEqual(['visit-11']);
    expect(poolClient.query.mock.calls.some(([sql]) => sql.includes('WHERE copilot_job_id = $1 AND job_date = $2'))).toBe(false);
  });

  test('falls back to copilot_job_id + job_date when visit id is not linked', async () => {
    const poolClient = {
      query: jest.fn()
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({
          rows: [{
            id: 12,
            copilot_job_id: 'job-12',
            job_date: '2026-04-18',
            copilot_event_updated_at: null,
            copilot_payload_hash: null,
          }],
        })
        .mockResolvedValueOnce({
          rows: [{ id: 12, copilot_job_id: 'job-12' }],
        }),
    };

    const result = await syncCopilotDispatchExecutionRecords({
      records: [{
        visit_id: 'visit-missing',
        job_id: 'job-12',
        job_date: '2026-04-18',
        status: 'completed',
      }],
      poolClient,
    });

    expect(result.updated).toBe(1);
    expect(poolClient.query.mock.calls[0][0]).toContain('WHERE copilot_visit_id = $1');
    expect(poolClient.query.mock.calls[1][0]).toContain('WHERE copilot_job_id = $1 AND job_date = $2');
    expect(poolClient.query.mock.calls[1][1]).toEqual(['job-12', '2026-04-18']);
  });

  test('dry_run reports updates without mutating scheduled_jobs', async () => {
    const poolClient = {
      query: jest.fn().mockResolvedValue({
        rows: [{
          id: 20,
          copilot_visit_id: 'visit-20',
          copilot_event_updated_at: null,
          copilot_payload_hash: null,
        }],
      }),
    };

    const result = await syncCopilotDispatchExecutionRecords({
      records: [{ visit_id: 'visit-20', status: 'completed' }],
      poolClient,
      dryRun: true,
    });

    expect(result.updated).toBe(1);
    expect(poolClient.query).toHaveBeenCalledTimes(1);
  });

  test('rejects stale Copilot updates when the incoming event timestamp is older', async () => {
    const poolClient = {
      query: jest.fn().mockResolvedValue({
        rows: [{
          id: 21,
          copilot_visit_id: 'visit-21',
          copilot_event_updated_at: '2026-04-18T15:00:00.000Z',
          copilot_payload_hash: null,
        }],
      }),
    };

    const result = await syncCopilotDispatchExecutionRecords({
      records: [{
        visit_id: 'visit-21',
        status: 'completed',
        event_updated_at: '2026-04-18T14:00:00.000Z',
      }],
      poolClient,
    });

    expect(result.skipped_stale).toBe(1);
    expect(result.updated).toBe(0);
    expect(poolClient.query).toHaveBeenCalledTimes(1);
  });

  test('skips unchanged Copilot payload hashes', async () => {
    const mirror = mapCopilotExecutionMirror({
      visit_id: 'visit-22',
      status: 'completed',
      event_updated_at: '2026-04-18T15:00:00.000Z',
    });
    const payloadHash = hashCopilotExecutionPayload(canonicalizeCopilotExecutionMirror(mirror));
    const poolClient = {
      query: jest.fn().mockResolvedValue({
        rows: [{
          id: 22,
          copilot_visit_id: 'visit-22',
          copilot_event_updated_at: '2026-04-18T15:00:00.000Z',
          copilot_payload_hash: payloadHash,
        }],
      }),
    };

    const result = await syncCopilotDispatchExecutionRecords({
      records: [{
        visit_id: 'visit-22',
        status: 'completed',
        event_updated_at: '2026-04-18T15:00:00.000Z',
      }],
      poolClient,
    });

    expect(result.skipped_unchanged).toBe(1);
    expect(result.updated).toBe(0);
    expect(poolClient.query).toHaveBeenCalledTimes(1);
  });

  test('updates only copilot mirror fields on scheduled_jobs', async () => {
    const poolClient = {
      query: jest.fn()
        .mockResolvedValueOnce({
          rows: [{
            id: 23,
            copilot_visit_id: 'visit-23',
            copilot_event_updated_at: null,
            copilot_payload_hash: null,
          }],
        })
        .mockResolvedValueOnce({
          rows: [{ id: 23 }],
        }),
    };

    const result = await syncCopilotDispatchExecutionRecords({
      records: [{
        visit_id: 'visit-23',
        status: 'completed',
        assigned_crew_name: 'Crew A',
        completion_notes: 'Synced from Copilot',
        event_updated_at: '2026-04-18T16:00:00.000Z',
      }],
      poolClient,
    });

    expect(result.updated).toBe(1);
    const [updateSql] = poolClient.query.mock.calls[1];
    expect(updateSql).toContain('UPDATE scheduled_jobs SET');
    expect(updateSql).toContain('copilot_visit_id = $');
    expect(updateSql).toContain('copilot_assigned_crew_name = $');
    expect(updateSql).toContain('copilot_execution_status = $');
    expect(updateSql).toContain('copilot_execution_status_raw = $');
    expect(updateSql).toContain('copilot_completion_notes = $');
    expect(updateSql).toContain('copilot_event_updated_at = $');
    expect(updateSql).toContain('copilot_payload_hash = $');
    expect(updateSql).toContain('copilot_last_synced_at = $');
    expect(updateSql).not.toMatch(/(?:^|,\s)status = \$/);
    expect(updateSql).not.toContain('completed_at =');
    expect(updateSql).not.toContain('invoice_id =');
    expect(updateSql).not.toContain('dispatch_issue =');
  });
});

describe('copilot dispatch execution sync route', () => {
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
      user: null,
      get(name) {
        const key = Object.keys(this.headers).find((candidate) => candidate.toLowerCase() === String(name).toLowerCase());
        return key ? this.headers[key] : undefined;
      },
      ...reqOverrides,
    };
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

  test('requires admin access for manual Copilot execution sync', async () => {
    const noop = (_req, _res, next) => next();
    const upload = { single: () => noop, array: () => noop, none: () => noop };
    const router = createJobRoutes({
      pool: { query: jest.fn() },
      serverError: jest.fn(),
      authenticateToken: noop,
      nextInvoiceNumber: jest.fn(),
      upload,
    });

    const res = await invokeRoute(router, '/api/copilot/dispatch-execution/sync', 'post', {
      user: { isEmployee: true, accountType: 'employee' },
      body: { date_from: '2026-04-18', date_to: '2026-04-18', dry_run: true },
    });

    expect(res.statusCode).toBe(403);
    expect(res.body).toEqual({ success: false, error: 'Admin access required' });
  });
});
