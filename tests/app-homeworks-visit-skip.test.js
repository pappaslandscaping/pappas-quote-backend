const express = require('express');

jest.mock('../services/homeworks/client', () => ({
  ...jest.requireActual('../services/homeworks/client'),
  queryHomeWorksGraphql: jest.fn(),
}));

const { queryHomeWorksGraphql } = require('../services/homeworks/client');
const createRoutes = require('../routes/app-homeworks');

describe('TwilioConnect approval based visit skip', () => {
  let server;
  let baseUrl;
  const pool = {
    query: jest.fn(async (sql) => {
      if (sql.includes('FROM messages')) return { rows: [{ id: 12, body: 'Please skip the mowing tomorrow', from_number: '+14405551212', created_at: new Date().toISOString() }] };
      if (sql.includes('FROM homeworks_phone_matches')) return { rows: [{ homeworks_customer_id: 42, customer_name: 'Gail' }] };
      throw new Error(`Unexpected SQL: ${sql}`);
    }),
  };

  beforeEach(async () => {
    queryHomeWorksGraphql.mockReset();
    pool.query.mockClear();
    queryHomeWorksGraphql.mockImplementation(async ({ operationName }) => {
      if (operationName === 'TwilioConnectSkipCandidates') return { events: [{
        id: 77, title: 'Lawn mowing', startDate: '2026-09-23', status: 'OPEN', recurringEventId: 8,
        property: { name: 'Home', address: { street1: '1 Main St', city: 'Cleveland', state: 'OH', zip: '44111' } },
      }] };
      if (operationName === 'TwilioConnectSkipVisit') return { skipEvent: { id: 77, status: 'SKIPPED', startDate: '2026-09-23' } };
      if (operationName === 'TwilioConnectSkipDispatchNote') return { createEventDispatchNote: { id: 77 } };
      throw new Error(`Unexpected HomeWorks operation: ${operationName}`);
    });
    const app = express();
    app.use(express.json());
    app.use(createRoutes({ pool, authenticateToken: (_req, _res, next) => next(),
      serverError: (res, error) => res.status(500).json({ error: error.message }) }));
    server = app.listen(0, '127.0.0.1');
    await new Promise((resolve) => server.once('listening', resolve));
    baseUrl = `http://127.0.0.1:${server.address().port}`;
  });

  afterEach(async () => {
    await new Promise((resolve) => server.close(resolve));
  });

  test('preview reads the inbound text and visits without changing HomeWorks', async () => {
    const response = await fetch(`${baseUrl}/api/app/homeworks/visit-skip-preview?messageId=12&phone=4405551212`);
    const result = await response.json();
    expect(response.status).toBe(200);
    expect(result.visits).toHaveLength(1);
    expect(result.visits[0].recurring).toBe(true);
    expect(queryHomeWorksGraphql).toHaveBeenCalledTimes(1);
    expect(queryHomeWorksGraphql.mock.calls[0][0].operationName).toBe('TwilioConnectSkipCandidates');
  });

  test('approval skips only the selected visit and attaches the original text', async () => {
    const response = await fetch(`${baseUrl}/api/app/homeworks/visit-skip`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messageId: 12, phone: '4405551212', eventId: 77 }),
    });
    const result = await response.json();
    expect(response.status).toBe(200);
    expect(result.visit.status).toBe('SKIPPED');
    expect(result.noteSaved).toBe(true);
    expect(queryHomeWorksGraphql.mock.calls.map(([call]) => call.operationName)).toEqual([
      'TwilioConnectSkipCandidates', 'TwilioConnectSkipVisit', 'TwilioConnectSkipDispatchNote',
    ]);
    expect(queryHomeWorksGraphql.mock.calls[1][0].variables.eventId).toBe(77);
    expect(queryHomeWorksGraphql.mock.calls[1][0].variables.reason).toContain('Please skip the mowing tomorrow');
  });

  test('a visit outside the matched open visits is rejected', async () => {
    const response = await fetch(`${baseUrl}/api/app/homeworks/visit-skip`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messageId: 12, phone: '4405551212', eventId: 99 }),
    });
    expect(response.status).toBe(409);
    expect(queryHomeWorksGraphql).toHaveBeenCalledTimes(1);
  });
});
