// ═══════════════════════════════════════════════════════════
// Request Validation Helpers
// Declarative schema validation for req.body, req.query, req.params.
// Returns field-level errors via ValidationError.
//
// Usage:
//   const { validate, schemas } = require('./lib/validate');
//   app.post('/api/customers', validate(schemas.createCustomer), handler);
// ═══════════════════════════════════════════════════════════

const { ValidationError } = require('./api-error');

// ─── Field validators ────────────────────────────────────

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONE_RE = /^[\d\s()+-]{7,20}$/;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_RE = /^\d{2}:\d{2}/;

const validators = {
  required: (v, field) => (v === undefined || v === null || v === '') ? `${field} is required` : null,
  string: (v, field) => (v !== undefined && v !== null && typeof v !== 'string') ? `${field} must be a string` : null,
  number: (v, field) => (v !== undefined && v !== null && (typeof v !== 'number' || isNaN(v))) ? `${field} must be a number` : null,
  positiveNumber: (v, field) => (v !== undefined && v !== null && (typeof v !== 'number' || isNaN(v) || v < 0)) ? `${field} must be a non-negative number` : null,
  integer: (v, field) => (v !== undefined && v !== null && !Number.isInteger(v)) ? `${field} must be an integer` : null,
  boolean: (v, field) => (v !== undefined && v !== null && typeof v !== 'boolean') ? `${field} must be a boolean` : null,
  email: (v, field) => (v && !EMAIL_RE.test(v)) ? `${field} must be a valid email` : null,
  phone: (v, field) => (v && !PHONE_RE.test(v.replace(/\s/g, ''))) ? `${field} must be a valid phone number` : null,
  date: (v, field) => (v && !DATE_RE.test(v)) ? `${field} must be a valid date (YYYY-MM-DD)` : null,
  time: (v, field) => (v && !TIME_RE.test(v)) ? `${field} must be a valid time (HH:MM)` : null,
  array: (v, field) => (v !== undefined && v !== null && !Array.isArray(v)) ? `${field} must be an array` : null,
  object: (v, field) => (v !== undefined && v !== null && (typeof v !== 'object' || Array.isArray(v))) ? `${field} must be an object` : null,
  enum: (allowed) => (v, field) => (v !== undefined && v !== null && !allowed.includes(v)) ? `${field} must be one of: ${allowed.join(', ')}` : null,
  minLength: (min) => (v, field) => (v && v.length < min) ? `${field} must be at least ${min} characters` : null,
  maxLength: (max) => (v, field) => (v && v.length > max) ? `${field} must be at most ${max} characters` : null,
};

// ─── Schema runner ───────────────────────────────────────

function validateBody(body, schema) {
  const errors = [];

  for (const [field, rules] of Object.entries(schema)) {
    const value = body[field];
    const ruleList = Array.isArray(rules) ? rules : [rules];

    for (const rule of ruleList) {
      const fn = typeof rule === 'string' ? validators[rule] : rule;
      if (!fn) continue;
      const err = fn(value, field);
      if (err) {
        errors.push({ field, message: err });
        break; // one error per field
      }
    }
  }

  return errors;
}

// ─── Express middleware factory ──────────────────────────

function validate(schema, source = 'body') {
  return (req, res, next) => {
    const data = req[source];
    const errors = validateBody(data || {}, schema);
    if (errors.length > 0) {
      throw new ValidationError('Validation failed', errors);
    }
    next();
  };
}

// ─── Schemas ─────────────────────────────────────────────

const schemas = {
  // Auth
  login: {
    email: ['required', 'string', 'email'],
    password: ['required', 'string'],
  },
  changePassword: {
    current_password: ['required', 'string'],
    new_password: ['required', 'string', validators.minLength(8)],
  },
  resetPassword: {
    token: ['required', 'string'],
    new_password: ['required', 'string', validators.minLength(8)],
  },

  // Customers
  createCustomer: {
    name: ['string'],
    email: ['email'],
    phone: ['phone'],
  },
  updateCustomer: {
    email: ['email'],
    phone: ['phone'],
  },

  // Quotes (public form)
  createQuoteRequest: {
    email: ['email'],
  },

  // Sent Quotes (admin)
  createSentQuote: {
    customer_name: ['required', 'string'],
    services: ['required', 'array'],
    total: ['required', 'number', 'positiveNumber'],
  },
  updateSentQuote: {
    status: [validators.enum(['draft', 'pending', 'sent', 'viewed', 'signed', 'contracted', 'accepted', 'declined', 'expired', 'cancelled'])],
  },

  // Contract signing
  signContract: {
    signer_name: ['required', 'string'],
    signature_data: ['required', 'string'],
  },

  // Invoices
  createInvoice: {
    customer_name: ['required', 'string'],
    line_items: ['required', 'array'],
    total: ['required', 'number', 'positiveNumber'],
  },
  updateInvoice: {
    status: [validators.enum(['draft', 'sent', 'paid', 'overdue', 'cancelled', 'partial'])],
  },

  // Payments
  recordPayment: {
    amount: ['required', 'number', 'positiveNumber'],
  },

  // Jobs
  createJob: {
    customer_name: ['required', 'string'],
    service_type: ['required', 'string'],
  },
  updateJob: {
    status: [validators.enum(['scheduled', 'in-progress', 'completed', 'cancelled', 'pending'])],
    pipeline_stage: [validators.enum(['pending', 'scheduled', 'in-progress', 'completed', 'cancelled'])],
  },
  completeJob: {},

  // Crews
  createCrew: {
    name: ['required', 'string'],
  },

  // Employees
  createEmployee: {
    first_name: ['required', 'string'],
    last_name: ['required', 'string'],
  },

  // Broadcasts
  sendBroadcast: {
    subject: ['required', 'string'],
    body: ['required', 'string'],
    recipients: ['required', 'array'],
  },

  // Messages
  sendMessage: {
    to: ['required', 'string'],
    body: ['required', 'string'],
  },

  // Templates
  createTemplate: {
    name: ['required', 'string'],
    slug: ['required', 'string'],
  },

  // Campaigns
  createCampaign: {
    name: ['required', 'string'],
  },

  // Settings
  updateSetting: {
    value: ['required'],
  },

  // Expenses
  createExpense: {
    amount: ['required', 'number', 'positiveNumber'],
  },

  // Notes
  createNote: {
    content: ['required', 'string'],
  },

  // Copilot sync
  copilotSettings: {
    key: ['required', 'string'],
    value: ['required', 'string'],
  },
};

module.exports = { validate, validateBody, validators, schemas };
