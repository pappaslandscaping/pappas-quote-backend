jest.mock('../services/homeworks/voicemail-sync', () => ({
  mirrorVoicemail: jest.fn(),
  voicemailSourceId: (voicemail) => String(voicemail.twilio_sid || `voicemail-${voicemail.id}`),
}));

const { mirrorVoicemail } = require('../services/homeworks/voicemail-sync');
const { captureNewVoicemails, getVoicemailIntakeStatus } = require('../services/homeworks/voicemail-intake');

const recent = { id: 4, twilio_sid: 'CA-4', created_at: '2026-09-21T21:00:00Z', from_number: '+12165550100' };

function makePool(claimed = [{ attempts: 1 }]) {
  return { query: jest.fn(async (sql) => {
    if (sql.includes('SELECT started_at')) return { rows: [{ started_at: '2026-09-21T20:00:00Z' }] };
    if (sql.includes('RETURNING attempts')) return { rows: claimed };
    if (sql.includes('SELECT status, error')) return { rows: [{ status: 'pending_homeworks', error: null }] };
    return { rows: [] };
  }) };
}

describe('background voicemail intake', () => {
  beforeEach(() => jest.clearAllMocks());

  test('captures new voicemails without writing HomeWorks Call Notes', async () => {
    const pool = makePool();
    const result = await captureNewVoicemails({ pool, voicemails: [recent] });
    expect(result).toEqual({ captured: 1, retried: 0, skipped: 0, failed: 0 });
    expect(mirrorVoicemail).toHaveBeenCalledWith(pool, recent);
    expect(pool.query).toHaveBeenCalledWith(expect.stringContaining("status = 'pending_homeworks'"), ['CA-4']);
    expect(await getVoicemailIntakeStatus(pool, 'CA-4')).toEqual({ status: 'pending_homeworks', error: null });
  });

  test('skips older, undated, and already captured feed entries', async () => {
    const pool = makePool([]);
    const result = await captureNewVoicemails({ pool, voicemails: [
      { ...recent, id: 1, created_at: '2026-09-21T19:00:00Z' },
      { ...recent, id: 2, created_at: null },
      recent,
    ] });
    expect(result.skipped).toBe(3);
    expect(mirrorVoicemail).not.toHaveBeenCalled();
  });

  test('records an intake failure so the next scan can retry it', async () => {
    const pool = makePool([{ attempts: 2 }]);
    mirrorVoicemail.mockRejectedValueOnce(new Error('Temporary database issue'));
    const first = await captureNewVoicemails({ pool, voicemails: [recent] });
    expect(first.failed).toBe(1);
    expect(pool.query).toHaveBeenCalledWith(expect.stringContaining("status = 'intake_failed'"), ['CA-4', 'Temporary database issue']);
    const second = await captureNewVoicemails({ pool, voicemails: [recent] });
    expect(second.retried).toBe(1);
  });
});
