const { normalizeTwilioCallDirection, isMissedInboundCall } = require('../lib/twilio-call-classification');

describe('Twilio call classification', () => {
  test.each([
    ['inbound', 'inbound'],
    ['outbound-api', 'outbound'],
    ['outbound-dial', 'outbound'],
    ['outbound', 'outbound'],
    ['trunking-originating', 'inbound'],
    ['trunking-terminating', 'outbound'],
    ['unexpected', 'unknown'],
    [undefined, 'unknown'],
  ])('%s normalizes to %s', (raw, expected) => {
    expect(normalizeTwilioCallDirection(raw)).toBe(expected);
  });

  test('only unanswered inbound calls count as missed', () => {
    expect(isMissedInboundCall({ direction: 'inbound', status: 'no-answer' })).toBe(true);
    expect(isMissedInboundCall({ direction: 'inbound', status: 'busy' })).toBe(true);
    expect(isMissedInboundCall({ direction: 'inbound', status: 'completed', duration: 0 })).toBe(false);
    expect(isMissedInboundCall({ direction: 'outbound-api', status: 'no-answer' })).toBe(false);
    expect(isMissedInboundCall({ direction: 'outbound-dial', status: 'failed' })).toBe(false);
  });
});
