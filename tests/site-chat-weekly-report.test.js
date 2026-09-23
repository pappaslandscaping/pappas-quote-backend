const { reportWeek, loadReport, renderReport, createWeeklyChatReporter } = require('../lib/site-chat-weekly-report');

test('reports the previous Monday through Sunday in Eastern time, including DST week', () => {
  expect(reportWeek(new Date('2026-09-28T13:00:00Z'))).toEqual({ start: '2026-09-21', end: '2026-09-28', due: true });
  expect(reportWeek(new Date('2026-09-28T12:59:00Z')).due).toBe(false);
  expect(reportWeek(new Date('2026-11-02T14:00:00Z'))).toEqual({ start: '2026-10-26', end: '2026-11-02', due: true });
  expect(reportWeek(new Date('2026-09-27T16:00:00Z')).due).toBe(false);
});

test('summary separates AI chats and human follow-ups without exposing message text', async () => {
  const week = { start: '2026-09-21', end: '2026-09-28' };
  const rows = [
    { id: 'a', visitor_name: 'Visitor', mode: 'assistant', status: 'open', created_at: '2026-09-22T14:00:00Z', assistant_messages: 2, staff_messages: 0 },
    { id: 'b', visitor_name: '<script>Test</script>', mode: 'human', status: 'open', created_at: '2026-09-23T02:00:00Z', assistant_messages: 1, staff_messages: 0, last_visitor_at: '2026-09-23T02:01:00Z' },
    { id: 'c', visitor_name: 'Guest', mode: 'human', status: 'open', created_at: '2026-09-23T15:00:00Z', assistant_messages: 0, staff_messages: 1, last_visitor_at: '2026-09-23T15:01:00Z', last_staff_at: '2026-09-23T15:02:00Z' },
  ];
  const pool = { query: jest.fn().mockResolvedValue({ rows }) };
  const report = await loadReport(pool, week);
  expect(pool.query.mock.calls[0][1]).toEqual([week.start, week.end]);
  expect(report).toMatchObject({ total: 3, aiOnly: 1, staffReplied: 1, handoffs: 1, afterHours: 1 });
  expect(report.needsReply.map((chat) => chat.id)).toEqual(['b']);
  const html = renderReport(report);
  expect(html).toContain('&lt;script&gt;Test&lt;/script&gt;');
  expect(html).toContain('live-chat.html?chat=b');
  expect(html).not.toContain('<script>Test</script>');
});

test('sends once, and skips subsequent attempts for the same week', async () => {
  const week = '2026-09-21';
  let sent = false;
  const pool = { query: jest.fn(async (sql) => {
    if (sql.includes('INSERT INTO site_chat_weekly_reports')) {
      if (sent) return { rows: [] };
      sent = true;
      return { rows: [{ week_start: week }] };
    }
    if (sql.includes('FROM site_chats c')) return { rows: [] };
    return { rows: [] };
  }) };
  const fetchImpl = jest.fn().mockResolvedValue({ ok: true });
  const reporter = createWeeklyChatReporter({ pool, fetchImpl, apiKey: 'test', from: 'test@example.com', to: 'owner@example.com', now: () => new Date('2026-09-28T13:00:00Z') });
  expect(await reporter.run()).toMatchObject({ sent: true, week, total: 0 });
  expect((await reporter.run()).skipped).toBe('already sent or in progress');
  expect(fetchImpl).toHaveBeenCalledTimes(1);
  expect(JSON.parse(fetchImpl.mock.calls[0][1].body).to).toEqual(['owner@example.com']);
});
