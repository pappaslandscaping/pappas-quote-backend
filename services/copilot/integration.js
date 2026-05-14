function customerDisplayName(customer = {}) {
  return (
    customer.name ||
    [customer.first_name, customer.last_name].filter(Boolean).join(' ') ||
    ''
  ).trim();
}

function asNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function statusFor(rows, error) {
  if (error) return 'error';
  return rows.length ? 'live' : 'empty';
}

function sourceState(rows, error = null) {
  return {
    status: statusFor(rows, error),
    source: 'local_database',
    error: error ? error.message || 'Failed to load source' : null,
  };
}

async function safeQuery(pool, query, params = []) {
  try {
    const result = await pool.query(query, params);
    return { rows: result.rows || [], error: null };
  } catch (error) {
    return { rows: [], error };
  }
}

function eventDate(row, fields) {
  for (const field of fields) {
    if (row[field]) return row[field];
  }
  return null;
}

function readableText(value, fallback = '') {
  if (value == null) return fallback;
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) {
    return value
      .map((item) => readableText(item))
      .filter(Boolean)
      .join(', ');
  }
  if (typeof value === 'object') {
    return Object.entries(value)
      .map(([key, entry]) => `${key}: ${readableText(entry)}`)
      .filter((entry) => !entry.endsWith(': '))
      .join(', ');
  }
  return String(value);
}

function timelineEvent({ type, id, title, detail, status, date, amount, href, source = 'local_database' }) {
  return {
    id: `${type}-${id}`,
    record_id: id,
    type,
    title,
    detail,
    status: status || null,
    date: date || null,
    amount: amount == null ? null : asNumber(amount),
    href: href || null,
    source,
  };
}

async function getCustomerRecord(pool, customerId) {
  const result = await pool.query(
    'SELECT id, customer_number, name, first_name, last_name, email, phone, mobile, status, customer_type, notes, created_at, updated_at FROM customers WHERE id = $1',
    [customerId]
  );
  return result.rows[0] || null;
}

async function getCustomerQuotes(pool, customer) {
  const name = customerDisplayName(customer);
  const { rows, error } = await safeQuery(
    pool,
    `SELECT id, quote_number, customer_name, customer_email, services, subtotal, tax_amount, total, monthly_payment, status, created_at, sent_at, viewed_at, contract_signed_at
     FROM sent_quotes
     WHERE (LOWER(COALESCE(customer_email, '')) = LOWER($1) AND $1 <> '')
        OR LOWER(COALESCE(customer_name, '')) = LOWER($2)
     ORDER BY COALESCE(created_at, sent_at) DESC
     LIMIT 50`,
    [customer.email || '', name]
  );

  return {
    rows,
    source: sourceState(rows, error),
    events: rows.map((quote) =>
      timelineEvent({
        type: 'quote',
        id: quote.id,
        title: `Quote #${quote.quote_number || quote.id}`,
        detail: readableText(quote.services, 'Quote created'),
        status: quote.status,
        date: eventDate(quote, ['contract_signed_at', 'viewed_at', 'sent_at', 'created_at']),
        amount: quote.total,
        href: `/quotes/${quote.id}`,
      })
    ),
  };
}

async function getCustomerJobs(pool, customer) {
  const name = customerDisplayName(customer);
  const { rows, error } = await safeQuery(
    pool,
    `SELECT id, job_date, job_date AS scheduled_date, customer_name, service_type, service_price, address, status, completed_at, crew_assigned, created_at, completion_notes
     FROM scheduled_jobs
     WHERE customer_id = $1
        OR LOWER(COALESCE(customer_name, '')) = LOWER($2)
        OR LOWER(COALESCE(customer_name, '')) LIKE LOWER($2) || ' %'
     ORDER BY COALESCE(job_date, completed_at, created_at) DESC
     LIMIT 50`,
    [customer.id, name]
  );

  return {
    rows,
    source: sourceState(rows, error),
    events: rows.map((job) =>
      timelineEvent({
        type: 'job',
        id: job.id,
        title: job.service_type || `Job #${job.id}`,
        detail: [job.address, job.crew_assigned ? `Crew: ${job.crew_assigned}` : null]
          .filter(Boolean)
          .join(' · '),
        status: job.status,
        date: eventDate(job, ['completed_at', 'job_date', 'scheduled_date', 'created_at']),
        amount: job.service_price,
        href: `/jobs/${job.id}`,
      })
    ),
  };
}

async function getCustomerInvoices(pool, customer) {
  const name = customerDisplayName(customer);
  const { rows, error } = await safeQuery(
    pool,
    `SELECT id, invoice_number, customer_name, customer_email, total, amount_paid, status, due_date, paid_at, created_at
     FROM invoices
     WHERE customer_id = $1
        OR LOWER(COALESCE(customer_name, '')) = LOWER($2)
        OR (LOWER(COALESCE(customer_email, '')) = LOWER($3) AND $3 <> '')
     ORDER BY COALESCE(created_at, due_date) DESC
     LIMIT 50`,
    [customer.id, name, customer.email || '']
  );

  return {
    rows,
    source: sourceState(rows, error),
    events: rows.map((invoice) =>
      timelineEvent({
        type: 'invoice',
        id: invoice.id,
        title: `Invoice #${invoice.invoice_number || invoice.id}`,
        detail: invoice.due_date ? `Due ${new Date(invoice.due_date).toISOString().slice(0, 10)}` : 'Invoice created',
        status: invoice.status,
        date: eventDate(invoice, ['paid_at', 'created_at', 'due_date']),
        amount: invoice.total,
        href: `/invoices/${invoice.id}`,
      })
    ),
  };
}

async function getCustomerPayments(pool, customer) {
  const { rows, error } = await safeQuery(
    pool,
    `SELECT p.id, p.invoice_id, p.amount, p.method, p.status, p.paid_at, p.created_at, i.invoice_number
     FROM payments p
     LEFT JOIN invoices i ON i.id = p.invoice_id
     WHERE p.customer_id = $1 OR i.customer_id = $1
     ORDER BY COALESCE(p.paid_at, p.created_at) DESC
     LIMIT 50`,
    [customer.id]
  );

  return {
    rows,
    source: sourceState(rows, error),
    events: rows.map((payment) =>
      timelineEvent({
        type: 'payment',
        id: payment.id,
        title: `Payment${payment.invoice_number ? ` for invoice #${payment.invoice_number}` : ''}`,
        detail: payment.method || 'Payment recorded',
        status: payment.status,
        date: eventDate(payment, ['paid_at', 'created_at']),
        amount: payment.amount,
        href: payment.invoice_id ? `/invoices/${payment.invoice_id}` : null,
      })
    ),
  };
}

async function getCustomerCommunications(pool, customer) {
  const phones = [customer.phone, customer.mobile]
    .map((value) => String(value || '').replace(/\D/g, '').slice(-10))
    .filter(Boolean);
  const phoneKeys = Array.from(new Set(phones));
  const params = phoneKeys.length ? [customer.id, phoneKeys] : [customer.id];

  const messagesQuery = phoneKeys.length
    ? `SELECT id, direction, from_number, to_number, body, status, read, created_at
       FROM messages
       WHERE customer_id = $1
          OR RIGHT(REGEXP_REPLACE(COALESCE(from_number, ''), '[^0-9]', '', 'g'), 10) = ANY($2::text[])
          OR RIGHT(REGEXP_REPLACE(COALESCE(to_number, ''), '[^0-9]', '', 'g'), 10) = ANY($2::text[])
       ORDER BY created_at DESC
       LIMIT 50`
    : `SELECT id, direction, from_number, to_number, body, status, read, created_at
       FROM messages
       WHERE customer_id = $1
       ORDER BY created_at DESC
       LIMIT 50`;

  const callsQuery = phoneKeys.length
    ? `SELECT id, direction, from_number, to_number, status, duration, transcription, created_at
       FROM calls
       WHERE customer_id = $1
          OR RIGHT(REGEXP_REPLACE(COALESCE(from_number, ''), '[^0-9]', '', 'g'), 10) = ANY($2::text[])
          OR RIGHT(REGEXP_REPLACE(COALESCE(to_number, ''), '[^0-9]', '', 'g'), 10) = ANY($2::text[])
       ORDER BY created_at DESC
       LIMIT 50`
    : `SELECT id, direction, from_number, to_number, status, duration, transcription, created_at
       FROM calls
       WHERE customer_id = $1
       ORDER BY created_at DESC
       LIMIT 50`;

  const [messagesResult, callsResult] = await Promise.all([
    safeQuery(pool, messagesQuery, params),
    safeQuery(pool, callsQuery, params),
  ]);

  const rows = [
    ...messagesResult.rows.map((message) => ({ ...message, record_type: 'message' })),
    ...callsResult.rows.map((call) => ({ ...call, record_type: 'call' })),
  ].sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));
  const error = messagesResult.error || callsResult.error;

  return {
    rows,
    source: sourceState(rows, error),
    events: rows.map((row) =>
      timelineEvent({
        type: 'communication',
        id: `${row.record_type}-${row.id}`,
        title:
          row.record_type === 'call'
            ? `${row.direction || 'Inbound'} call`
            : `${row.direction || 'Inbound'} message`,
        detail:
          row.record_type === 'call'
            ? row.transcription || (row.duration ? `${row.duration}s call` : 'Call record')
            : readableText(row.body, 'Message record'),
        status: row.status,
        date: row.created_at,
      })
    ),
  };
}

async function getCustomerNotes(pool, customer) {
  const { rows, error } = await safeQuery(
    pool,
    `SELECT id, author_name, content, pinned, created_at
     FROM internal_notes
     WHERE entity_type = 'customer' AND entity_id = $1
     ORDER BY pinned DESC, created_at DESC
     LIMIT 50`,
    [customer.id]
  );

  const noteRows = customer.notes
    ? [
        {
          id: `profile-${customer.id}`,
          author_name: 'Customer profile',
          content: customer.notes,
          pinned: false,
          created_at: customer.updated_at || customer.created_at,
        },
        ...rows,
      ]
    : rows;

  return {
    rows: noteRows,
    source: sourceState(noteRows, error),
    events: noteRows.map((note) =>
      timelineEvent({
        type: 'note',
        id: note.id,
        title: note.author_name ? `Note from ${note.author_name}` : 'Customer note',
        detail: readableText(note.content),
        status: note.pinned ? 'pinned' : null,
        date: note.created_at,
      })
    ),
  };
}

function buildSummary({ quotes, jobs, invoices, payments, communications, notes }) {
  const openInvoiceBalance = invoices.rows
    .filter((invoice) => !['paid', 'void', 'cancelled'].includes(String(invoice.status || '').toLowerCase()))
    .reduce((sum, invoice) => sum + Math.max(0, asNumber(invoice.total) - asNumber(invoice.amount_paid)), 0);
  const signedQuotes = quotes.rows.filter((quote) =>
    ['signed', 'accepted', 'approved'].includes(String(quote.status || '').toLowerCase())
  ).length;
  const completedJobs = jobs.rows.filter((job) =>
    ['completed', 'done'].includes(String(job.status || '').toLowerCase())
  ).length;

  return {
    quote_count: quotes.rows.length,
    signed_quote_count: signedQuotes,
    job_count: jobs.rows.length,
    completed_job_count: completedJobs,
    invoice_count: invoices.rows.length,
    open_invoice_balance: openInvoiceBalance,
    payment_count: payments.rows.length,
    communication_count: communications.rows.length,
    note_count: notes.rows.length,
  };
}

async function getCustomer360({ pool, customerId }) {
  const customer = await getCustomerRecord(pool, customerId);
  if (!customer) return null;

  const [quotes, jobs, invoices, payments, communications, notes] = await Promise.all([
    getCustomerQuotes(pool, customer),
    getCustomerJobs(pool, customer),
    getCustomerInvoices(pool, customer),
    getCustomerPayments(pool, customer),
    getCustomerCommunications(pool, customer),
    getCustomerNotes(pool, customer),
  ]);

  const timeline = [
    ...quotes.events,
    ...jobs.events,
    ...invoices.events,
    ...payments.events,
    ...communications.events,
    ...notes.events,
  ]
    .filter((event) => event.date)
    .sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0))
    .slice(0, 100);

  return {
    customer,
    summary: buildSummary({ quotes, jobs, invoices, payments, communications, notes }),
    sources: {
      quotes: quotes.source,
      jobs: jobs.source,
      invoices: invoices.source,
      payments: payments.source,
      communications: communications.source,
      notes: notes.source,
    },
    records: {
      quotes: quotes.rows,
      jobs: jobs.rows,
      invoices: invoices.rows,
      payments: payments.rows,
      communications: communications.rows,
      notes: notes.rows,
    },
    timeline,
    ai: {
      mode: 'draft_only',
      allowed_actions: ['prepare_followup_draft'],
      blocked_actions: ['send_email', 'send_sms', 'collect_payment', 'update_job', 'delete_record'],
    },
  };
}

module.exports = {
  customerDisplayName,
  getCustomer360,
  getCustomerQuotes,
  getCustomerJobs,
  getCustomerInvoices,
  getCustomerPayments,
  getCustomerCommunications,
  getCustomerNotes,
};
