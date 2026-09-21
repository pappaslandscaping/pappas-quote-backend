const {
  buildCommunicationCallNote,
  parseHomeWorksCallNotesHtml,
  saveHomeWorksCallNote,
} = require('../services/homeworks/call-notes');

describe('HomeWorks Call Notes integration', () => {
  test('parses recent call notes from the HomeWorks customer timeline', () => {
    const notes = parseHomeWorksCallNotesHtml(`
      <ul id="custom_timeline_communicat_tab">
        <li>
          <div class="text-primary"><a href="/customers/details/42"><strong><u>Sep 21, 2026</u></strong></a> by <a href="/resources/employees/edit/9">Theresa Pappas</a></div>
          <div class="text-dark">Twilio Connect · Text received<br>Message:<br>Hello there</div>
        </li>
      </ul>
    `);
    expect(notes).toEqual([expect.objectContaining({
      date: 'Sep 21, 2026',
      author: 'Theresa Pappas',
      body: 'Twilio Connect · Text received\nMessage:\nHello there',
    })]);
  });

  test('formats an inbound text as a readable communication journal entry', () => {
    const note = buildCommunicationCallNote({
      kind: 'text',
      direction: 'inbound',
      occurredAt: '2026-09-21T13:33:00.000Z',
      from: '+12165012439',
      to: '+12165551212',
      body: 'Please wait until mid April.',
      status: 'received',
      sourceId: 'SM123',
    });

    expect(note).toContain('Twilio Connect · Text received');
    expect(note).toContain('Sep 21, 2026');
    expect(note).toContain('Please wait until mid April.');
    expect(note).toContain('Twilio ID: SM123');
  });

  test('saves to the legacy customer Call Notes endpoint with the matched customer id', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ status: true }),
    });
    const getCopilotToken = jest.fn().mockResolvedValue({ cookieHeader: 'session=test' });
    const pool = { query: jest.fn() };

    const result = await saveHomeWorksCallNote({
      pool,
      getCopilotToken,
      customerId: 1053548,
      note: 'Twilio Connect · Text received\nHello <there>',
      fetchImpl,
    });

    expect(result.success).toBe(true);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [url, request] = fetchImpl.mock.calls[0];
    expect(url).toBe('https://secure.copilotcrm.com/customers/details/communicationSaveCall');
    expect(request.headers.Cookie).toBe('session=test');
    const form = new URLSearchParams(request.body);
    expect(form.get('customer_id')).toBe('1053548');
    expect(form.get('call_notes')).toContain('Hello &lt;there&gt;');
  });

  test('does not report success when HomeWorks rejects the note', async () => {
    const fetchImpl = jest.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ status: false, msg: 'Not saved' }),
    });

    await expect(saveHomeWorksCallNote({
      pool: { query: jest.fn() },
      getCopilotToken: jest.fn().mockResolvedValue({ cookieHeader: 'session=test' }),
      customerId: 42,
      note: 'Test note',
      fetchImpl,
    })).rejects.toThrow('Not saved');
  });
});
