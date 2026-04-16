// ═══════════════════════════════════════════════════════════
// Structured API Errors
// Consistent error classes mapped to HTTP status codes.
// Use with the central error middleware in server.js.
// ═══════════════════════════════════════════════════════════

class ApiError extends Error {
  constructor(status, code, message, details = null) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.details = details;
  }

  toJSON() {
    const obj = { success: false, error: this.message, code: this.code };
    if (this.details) obj.details = this.details;
    return obj;
  }
}

class ValidationError extends ApiError {
  constructor(message, details = []) {
    super(400, 'VALIDATION_ERROR', message, details);
    this.name = 'ValidationError';
  }
}

class NotFoundError extends ApiError {
  constructor(entity = 'Resource', id = null) {
    super(404, 'NOT_FOUND', id ? `${entity} ${id} not found` : `${entity} not found`);
    this.name = 'NotFoundError';
  }
}

class UnauthorizedError extends ApiError {
  constructor(message = 'Authentication required') {
    super(401, 'UNAUTHORIZED', message);
    this.name = 'UnauthorizedError';
  }
}

class ForbiddenError extends ApiError {
  constructor(message = 'Access denied') {
    super(403, 'FORBIDDEN', message);
    this.name = 'ForbiddenError';
  }
}

class ConflictError extends ApiError {
  constructor(message = 'Resource conflict') {
    super(409, 'CONFLICT', message);
    this.name = 'ConflictError';
  }
}

class IntegrationError extends ApiError {
  constructor(service, message = 'Integration failed') {
    super(502, 'INTEGRATION_ERROR', `${service}: ${message}`);
    this.name = 'IntegrationError';
    this.service = service;
  }
}

module.exports = {
  ApiError,
  ValidationError,
  NotFoundError,
  UnauthorizedError,
  ForbiddenError,
  ConflictError,
  IntegrationError,
};
