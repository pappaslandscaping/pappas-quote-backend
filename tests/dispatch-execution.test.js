const fs = require('fs');
const path = require('path');

const JOBS_PATH = path.join(__dirname, '..', 'routes', 'jobs.js');
const createJobRoutes = require('../routes/jobs');

describe('dispatch execution status transitions', () => {
  const {
    transitionScheduledJobStatus,
    normalizeJobStatus,
    JobStatusTransitionError,
  } = createJobRoutes.__testables;

  test('normalizes job statuses to canonical values', () => {
    expect(normalizeJobStatus('in-progress')).toBe('in_progress');
    expect(normalizeJobStatus('In Progress')).toBe('in_progress');
    expect(normalizeJobStatus('canceled')).toBe('cancelled');
    expect(normalizeJobStatus('completed')).toBe('completed');
    expect(normalizeJobStatus('bogus')).toBeNull();
  });

  test('transitions pending jobs to in_progress and stamps start metadata', async () => {
    const mockPool = {
      query: jest.fn()
        .mockResolvedValueOnce({
          rows: [{ id: 17, status: 'pending', started_at: null, started_by: null, last_status_by: null, last_status_source: null }],
        })
        .mockResolvedValueOnce({
          rows: [{ id: 17, status: 'in_progress', started_at: '2026-04-18T12:00:00.000Z', started_by: 'Crew A', last_status_by: 'Crew A', last_status_source: 'dispatch_board' }],
        }),
    };

    const job = await transitionScheduledJobStatus({
      jobId: 17,
      nextStatus: 'in_progress',
      actorName: 'Crew A',
      source: 'dispatch_board',
      poolClient: mockPool,
      invoiceSideEffectFn: jest.fn(),
    });

    expect(job.status).toBe('in_progress');
    const [updateSql, updateVals] = mockPool.query.mock.calls[1];
    expect(updateSql).toContain('status = $1');
    expect(updateSql).toContain('last_status_at = CURRENT_TIMESTAMP');
    expect(updateSql).toContain('started_at = COALESCE(started_at, CURRENT_TIMESTAMP)');
    expect(updateSql).toContain('started_by = COALESCE(started_by');
    expect(updateVals).toEqual(expect.arrayContaining(['in_progress', 'Crew A', 'dispatch_board']));
  });

  test('transitions jobs to completed, persists proof-of-work fields, and refreshes after invoice side effects', async () => {
    const invoiceSideEffectFn = jest.fn().mockResolvedValue(undefined);
    const mockPool = {
      query: jest.fn()
        .mockResolvedValueOnce({
          rows: [{ id: 23, status: 'pending', completed_at: null, last_status_by: null, last_status_source: null }],
        })
        .mockResolvedValueOnce({
          rows: [{
            id: 23,
            status: 'completed',
            completed_at: '2026-04-18T12:30:00.000Z',
            completed_by: 'Crew B',
            completion_notes: 'Gate locked behind us',
            completion_photos: ['https://example.com/photo.jpg'],
            completion_lat: 41.48,
            completion_lng: -81.68,
          }],
        })
        .mockResolvedValueOnce({
          rows: [{
            id: 23,
            status: 'completed',
            invoice_id: 991,
            completed_by: 'Crew B',
            completion_notes: 'Gate locked behind us',
            completion_photos: ['https://example.com/photo.jpg'],
            completion_lat: 41.48,
            completion_lng: -81.68,
          }],
        }),
    };

    const job = await transitionScheduledJobStatus({
      jobId: 23,
      nextStatus: 'completed',
      actorName: 'Crew B',
      source: 'crew_mobile',
      completionNotes: 'Gate locked behind us',
      completionPhotos: ['https://example.com/photo.jpg'],
      completionLat: 41.48,
      completionLng: -81.68,
      poolClient: mockPool,
      invoiceSideEffectFn,
    });

    expect(job.invoice_id).toBe(991);
    const [updateSql, updateVals] = mockPool.query.mock.calls[1];
    expect(updateSql).toContain('completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)');
    expect(updateSql).toContain('completed_by = $');
    expect(updateSql).toContain('completion_notes = COALESCE(');
    expect(updateSql).toContain('completion_photos = COALESCE(');
    expect(updateSql).toContain('completion_lat = COALESCE(');
    expect(updateSql).toContain('completion_lng = COALESCE(');
    expect(updateVals).toEqual(expect.arrayContaining([
      'completed',
      'Crew B',
      'crew_mobile',
      'Gate locked behind us',
      JSON.stringify(['https://example.com/photo.jpg']),
      41.48,
      -81.68,
    ]));
    expect(invoiceSideEffectFn).toHaveBeenCalledTimes(1);
    expect(invoiceSideEffectFn.mock.calls[0][1]).toEqual({ poolClient: mockPool });
  });

  test('allows dispatch_issue updates without changing status', async () => {
    const mockPool = {
      query: jest.fn()
        .mockResolvedValueOnce({
          rows: [{ id: 31, status: 'pending', dispatch_issue: false, last_status_by: null, last_status_source: null }],
        })
        .mockResolvedValueOnce({
          rows: [{ id: 31, status: 'pending', dispatch_issue: true, dispatch_issue_reason: 'Locked gate' }],
        }),
    };

    const job = await transitionScheduledJobStatus({
      jobId: 31,
      actorName: 'Dispatcher',
      dispatchIssue: true,
      dispatchIssueReason: 'Locked gate',
      poolClient: mockPool,
      invoiceSideEffectFn: jest.fn(),
    });

    expect(job.dispatch_issue).toBe(true);
    const [updateSql, updateVals] = mockPool.query.mock.calls[1];
    expect(updateSql).toContain('dispatch_issue = $');
    expect(updateSql).toContain('dispatch_issue_reason = COALESCE(');
    expect(updateSql).toContain('dispatch_issue_reported_at = CURRENT_TIMESTAMP');
    expect(updateVals).toEqual(expect.arrayContaining([true, 'Locked gate', 'Dispatcher']));
  });

  test('rejects invalid reopen transitions from completed back to pending', async () => {
    const mockPool = {
      query: jest.fn().mockResolvedValueOnce({
        rows: [{ id: 41, status: 'completed' }],
      }),
    };

    await expect(
      transitionScheduledJobStatus({
        jobId: 41,
        nextStatus: 'pending',
        poolClient: mockPool,
        invoiceSideEffectFn: jest.fn(),
      })
    ).rejects.toBeInstanceOf(JobStatusTransitionError);

    expect(mockPool.query).toHaveBeenCalledTimes(1);
  });
});

describe('dispatch execution routes', () => {
  test('registers the new job status endpoint on the router', () => {
    const noop = (_req, _res, next) => next && next();
    const upload = {
      single: () => noop,
      array: () => noop,
      none: () => noop,
    };
    const router = createJobRoutes({
      pool: { query: jest.fn() },
      serverError: jest.fn(),
      authenticateToken: noop,
      nextInvoiceNumber: jest.fn(),
      upload,
    });
    const paths = router.stack.filter(layer => layer.route).map(layer => layer.route.path);
    expect(paths).toContain('/api/jobs/:id/status');
    expect(paths).toContain('/api/jobs/:id/complete');
  });

  test('job routes source delegates completion to the shared status transition helper', () => {
    const jobsCode = fs.readFileSync(JOBS_PATH, 'utf8');
    const completionBlock = jobsCode.slice(
      jobsCode.indexOf("router.patch('/api/jobs/:id/complete'"),
      jobsCode.indexOf("router.patch('/api/jobs/reorder'")
    );
    expect(completionBlock).toContain('transitionScheduledJobStatus');
    expect(completionBlock).toContain('completion_photos');
    expect(completionBlock).toContain('completion_lat');
    expect(completionBlock).toContain('completion_lng');
    expect(completionBlock).toContain('completed_by');
  });
});
