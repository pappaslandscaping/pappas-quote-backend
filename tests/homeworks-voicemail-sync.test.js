jest.mock('../services/homeworks/call-notes', () => ({
  ensureSyncTable: jest.fn().mockResolvedValue(undefined),
  syncCommunicationToHomeWorks: jest.fn(),
}));

const { syncCommunicationToHomeWorks } = require('../services/homeworks/call-notes');
const { syncVoicemailToHomeWorks, voicemailSourceId } = require('../services/homeworks/voicemail-sync');

const voicemail = {
  id: 77,
  twilio_sid: 'CA-voicemail-77',
  from_number: '+12165012439',
  to_number: '+12165551212',
  duration: 42,
  recording_url: 'https://twilio.test/recording',
  transcription: 'Please call me back.',
  read: false,
  created_at: '2026-09-21T14:00:00.000Z',
};

function createPool(startedAt) {
  return {
    query: jest.fn(async (sql) => {
      if (sql.includes('SELECT started_at')) return { rows: [{ started_at: startedAt }] };
      if (sql.includes('RETURNING *')) return { rows: [{ id: 1, twilio_sid: voicemail.twilio_sid }] };
      if (sql.includes("WHERE source_type = 'voicemail'")) return { rows: [] };
      return { rows: [] };
    }),
  };
}

describe('HomeWorks voicemail synchronization', () => {
  beforeEach(() => jest.clearAllMocks());

  test('uses the Twilio SID as the stable duplicate-prevention id', () => {
    expect(voicemailSourceId(voicemail)).toBe('CA-voicemail-77');
    expect(voicemailSourceId({ id: 9 })).toBe('voicemail-9');
  });

  test('does not automatically backfill a voicemail from before the feature was enabled', async () => {
    const pool = createPool('2026-09-21T15:00:00.000Z');
    const result = await syncVoicemailToHomeWorks({
      pool,
      getCopilotToken: jest.fn(),
      voicemail,
      automatic: true,
      recordingUrl: 'https://yarddesk.test/api/recordings/RE123',
    });

    expect(result.status).toBe('legacy_untracked');
    expect(syncCommunicationToHomeWorks).not.toHaveBeenCalled();
    expect(pool.query).toHaveBeenCalledWith(expect.stringContaining('INSERT INTO calls'), expect.any(Array));
  });

  test('manual save sends the voicemail to the normal duplicate-safe communication sync', async () => {
    const pool = createPool('2026-09-21T15:00:00.000Z');
    syncCommunicationToHomeWorks.mockResolvedValue({ success: true, status: 'synced', customerId: 1053548 });

    const result = await syncVoicemailToHomeWorks({
      pool,
      getCopilotToken: jest.fn(),
      voicemail,
      automatic: false,
      recordingUrl: 'https://yarddesk.test/api/recordings/RE123',
    });

    expect(result).toEqual({ success: true, status: 'synced', customerId: 1053548 });
    expect(syncCommunicationToHomeWorks).toHaveBeenCalledWith(expect.objectContaining({
      sourceType: 'voicemail',
      sourceId: 'CA-voicemail-77',
      phone: '+12165012439',
      communication: expect.objectContaining({
        kind: 'voicemail',
        transcription: 'Please call me back.',
        recordingUrl: 'https://yarddesk.test/api/recordings/RE123',
      }),
    }));
  });
});
