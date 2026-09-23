const MISSED_CALL_STATUSES = new Set(['no-answer', 'busy', 'failed', 'canceled', 'cancelled']);

function normalizeTwilioCallDirection(value) {
  const direction = String(value || '').trim().toLowerCase();
  if (direction === 'inbound' || direction === 'trunking-originating') return 'inbound';
  if (direction === 'outbound' || direction === 'outbound-api' || direction === 'outbound-dial' || direction === 'trunking-terminating') return 'outbound';
  return 'unknown';
}

function isMissedInboundCall(call) {
  return normalizeTwilioCallDirection(call?.direction) === 'inbound'
    && MISSED_CALL_STATUSES.has(String(call?.status || '').trim().toLowerCase());
}

module.exports = { normalizeTwilioCallDirection, isMissedInboundCall };
