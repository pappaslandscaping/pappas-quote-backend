function sanitizeDetails(details = {}) {
  const sanitized = { ...details };
  delete sanitized.password;
  delete sanitized.current_password;
  delete sanitized.new_password;
  delete sanitized.token;
  return sanitized;
}

function logAuditEvent(event, details = {}) {
  const payload = {
    timestamp: new Date().toISOString(),
    event,
    ...sanitizeDetails(details),
  };
  console.log(`[AUDIT] ${JSON.stringify(payload)}`);
}

module.exports = {
  logAuditEvent,
};
