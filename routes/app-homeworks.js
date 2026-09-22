const express = require('express');
const { queryHomeWorksGraphql } = require('../services/homeworks/client');
const {
  fetchMobileCustomerSnapshot,
  fetchMobileCustomers,
  fetchMobileToday,
  normalizePhone,
} = require('../services/homeworks/mobile-app');
const { ensureSyncTable, fetchHomeWorksCallNotes, saveHomeWorksCallNote } = require('../services/homeworks/call-notes');
const {
  createLeadForPhone,
  ensureMatchTable,
  listUnmatchedCommunications,
  matchCommunication,
} = require('../services/homeworks/unmatched-communications');

function isDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

const homeWorksDate = (value) => new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
}).format(value);

async function previewMessageVisitSkip({ pool, messageId, phone }) {
  const id = Number(messageId);
  const normalized = normalizePhone(phone);
  if (!Number.isSafeInteger(id) || id <= 0 || normalized.length !== 10) {
    return { status: 400, error: 'Valid message and phone are required' };
  }
  const messageResult = await pool.query(`
    SELECT id, twilio_sid, body, from_number, created_at FROM messages
    WHERE id = $1 AND direction = 'inbound'
      AND RIGHT(REGEXP_REPLACE(COALESCE(from_number, ''), '[^0-9]', '', 'g'), 10) = $2
  `, [id, normalized]);
  const message = messageResult.rows[0];
  if (!message) return { status: 404, error: 'Incoming text not found in this conversation' };

  const matched = await pool.query(`
    SELECT homeworks_customer_id, customer_name FROM homeworks_phone_matches
    WHERE normalized_phone = $1 LIMIT 1
  `, [normalized]).catch(() => ({ rows: [] }));
  let customer = matched.rows[0] && {
    id: Number(matched.rows[0].homeworks_customer_id), name: matched.rows[0].customer_name,
  };
  if (!customer) {
    const lookup = await queryHomeWorksGraphql({
      pool, operationName: 'TwilioConnectSkipCustomerLookup',
      query: `query TwilioConnectSkipCustomerLookup {
        customers(where: { isDeleted: false }, take: 5000) { id fullName phone cell }
      }`,
    });
    const matches = (lookup.customers || []).filter((row) =>
      [row.cell, row.phone].some((value) => normalizePhone(value) === normalized));
    if (matches.length > 1) return { status: 409, error: 'This number matches multiple HomeWorks customers. Match it before skipping a visit.' };
    if (matches.length === 1) customer = { id: matches[0].id, name: matches[0].fullName };
  }
  if (!customer?.id) return { status: 404, error: 'No matching HomeWorks customer found' };
  const today = homeWorksDate(new Date());
  const through = homeWorksDate(new Date(Date.now() + 48 * 60 * 60 * 1000));
  const data = await queryHomeWorksGraphql({
    pool,
    operationName: 'TwilioConnectSkipCandidates',
    query: `query TwilioConnectSkipCandidates($customerId: SafeInt!, $from: Date!, $through: Date!) {
      events(where: { customerId: { equals: $customerId }, type: { equals: VISIT }, status: { equals: OPEN },
        isDeleted: false, startDate: { gte: $from, lte: $through } },
        take: 200, orderBy: [{ startDate: asc }, { id: asc }]) {
        id title startDate startTime status recurringEventId customerId
        property { id name address { street1 city state zip } }
      }
    }`,
    variables: { customerId: Number(customer.id), from: today, through },
  });
  const visits = (data.events || []).map((event) => ({
    id: event.id, title: event.title, date: event.startDate, time: event.startTime,
    recurring: Boolean(event.recurringEventId),
    property: event.property?.name || '',
    address: [event.property?.address?.street1, event.property?.address?.city,
      event.property?.address?.state, event.property?.address?.zip].filter(Boolean).join(', '),
  }));
  return {
    status: 200, customer,
    message: { id: message.id, body: message.body, receivedAt: message.created_at },
    visits, window: { from: today, through },
  };
}

async function loadLocalCommunications(pool, phone, customerId) {
  const normalized = normalizePhone(phone);
  if (!normalized) return { messages: [], calls: [] };
  await Promise.all([ensureSyncTable(pool), ensureMatchTable(pool)]);
  const aliases = customerId ? await pool.query(`SELECT normalized_phone FROM homeworks_phone_matches WHERE homeworks_customer_id = $1`, [Number(customerId)]).catch(() => ({ rows: [] })) : { rows: [] };
  const phoneKeys = [...new Set([normalized, ...aliases.rows.map((row) => normalizePhone(row.normalized_phone))].filter(Boolean))];
  const [messages, calls] = await Promise.all([
    pool.query(`
      SELECT m.id, m.twilio_sid, m.direction, m.from_number, m.to_number, m.body,
        m.status, m.read, m.created_at,
        sync.status AS homeworks_sync_status, sync.error AS homeworks_sync_error
      FROM messages m
      LEFT JOIN homeworks_communication_sync sync
        ON sync.source_type = 'sms' AND sync.source_id = m.twilio_sid
      WHERE RIGHT(REGEXP_REPLACE(COALESCE(from_number, ''), '[^0-9]', '', 'g'), 10) = ANY($1::text[])
         OR RIGHT(REGEXP_REPLACE(COALESCE(to_number, ''), '[^0-9]', '', 'g'), 10) = ANY($1::text[])
      ORDER BY m.created_at DESC LIMIT 100
    `, [phoneKeys]).catch(() => ({ rows: [] })),
    pool.query(`
      SELECT c.id, c.twilio_sid, c.direction, c.from_number, c.to_number,
        c.status, c.duration, c.transcription, c.created_at,
        sync.status AS homeworks_sync_status, sync.error AS homeworks_sync_error
      FROM calls c
      LEFT JOIN homeworks_communication_sync sync
        ON sync.source_type = 'call' AND sync.source_id = c.twilio_sid
      WHERE RIGHT(REGEXP_REPLACE(COALESCE(from_number, ''), '[^0-9]', '', 'g'), 10) = ANY($1::text[])
         OR RIGHT(REGEXP_REPLACE(COALESCE(to_number, ''), '[^0-9]', '', 'g'), 10) = ANY($1::text[])
      ORDER BY c.created_at DESC LIMIT 100
    `, [phoneKeys]).catch(() => ({ rows: [] })),
  ]);
  return { messages: messages.rows, calls: calls.rows };
}

function createAppHomeWorksRoutes({ pool, authenticateToken, serverError, getCopilotToken }) {
  const router = express.Router();

  router.get('/api/app/homeworks/customers', authenticateToken, async (req, res) => {
    try {
      const customers = await fetchMobileCustomers({
        pool,
        search: String(req.query.search || ''),
        summary: req.query.summary === '1',
      });
      res.json({ success: true, customers, summary: req.query.summary === '1', source: 'official_homeworks_graphql', asOf: new Date().toISOString() });
    } catch (error) {
      serverError(res, error, 'Failed to load HomeWorks customers');
    }
  });

  router.get('/api/app/homeworks/customer-snapshot', authenticateToken, async (req, res) => {
    try {
      const phone = String(req.query.phone || '');
      const customerId = req.query.customerId ? Number(req.query.customerId) : null;
      if (!phone && !customerId) return res.status(400).json({ success: false, error: 'phone or customerId is required' });
      const snapshot = await fetchMobileCustomerSnapshot({ pool, phone, customerId });
      if (!snapshot) return res.status(404).json({ success: false, error: 'HomeWorks customer not found' });
      const [localCommunications, callNotes] = await Promise.all([
        loadLocalCommunications(pool, snapshot.customer.phone || phone, snapshot.customer.id),
        fetchHomeWorksCallNotes({ pool, getCopilotToken, customerId: snapshot.customer.id }).catch(() => []),
      ]);
      res.json({
        success: true,
        ...snapshot,
        twilioMessages: localCommunications.messages,
        calls: localCommunications.calls,
        callNotes,
        communicationWriteSupport: {
          generalSms: true,
          generalCalls: true,
          destination: 'customer_call_notes',
          eventDispatchNotes: true,
          explanation: 'Twilio Connect calls and texts are journaled in the customer Call Notes. Customer Notes remain reserved for durable account and service instructions.',
        },
      });
    } catch (error) {
      serverError(res, error, 'Failed to load HomeWorks customer snapshot');
    }
  });

  router.post('/api/app/homeworks/customers/:customerId/call-notes', authenticateToken, async (req, res) => {
    try {
      const customerId = Number(req.params.customerId);
      const note = String(req.body.note || '').trim();
      if (!Number.isInteger(customerId) || customerId <= 0) return res.status(400).json({ success: false, error: 'Valid customerId is required' });
      if (!note) return res.status(400).json({ success: false, error: 'note is required' });
      if (note.length > 10000) return res.status(400).json({ success: false, error: 'note must be 10000 characters or fewer' });
      const result = await saveHomeWorksCallNote({
        pool,
        getCopilotToken,
        customerId,
        note,
        notifyUser: req.body.notifyUser === true,
        createFollowUp: req.body.createFollowUp === true,
        followUpDate: req.body.followUpDate,
      });
      res.json({ ...result, destination: 'customer_call_notes' });
    } catch (error) {
      serverError(res, error, 'Failed to save HomeWorks Call Note');
    }
  });

  router.get('/api/app/homeworks/unmatched-communications', authenticateToken, async (req, res) => {
    try {
      const communications = await listUnmatchedCommunications(pool, req.query.limit);
      res.json({ success: true, communications });
    } catch (error) {
      serverError(res, error, 'Failed to load unmatched communications');
    }
  });

  router.post('/api/app/homeworks/unmatched-communications/:sourceType/:sourceId/match', authenticateToken, async (req, res) => {
    try {
      const result = await matchCommunication({ pool, getCopilotToken, sourceType: req.params.sourceType, sourceId: req.params.sourceId, customerId: req.body.customerId, customerName: req.body.customerName });
      res.json(result);
    } catch (error) {
      serverError(res, error, 'Failed to match communication');
    }
  });

  router.post('/api/app/homeworks/unmatched-communications/:sourceType/:sourceId/create-lead', authenticateToken, async (req, res) => {
    try {
      const customer = await createLeadForPhone({ pool, fullName: req.body.fullName, phone: req.body.phone });
      try {
        const result = await matchCommunication({ pool, getCopilotToken, sourceType: req.params.sourceType, sourceId: req.params.sourceId, customerId: customer.id, customerName: customer.fullName });
        res.status(201).json({ ...result, customer, created: true });
      } catch (matchError) {
        res.status(207).json({ success: false, created: true, customer, error: `Lead created, but the communication still needs matching: ${matchError.message}` });
      }
    } catch (error) {
      serverError(res, error, 'Failed to create and match HomeWorks lead');
    }
  });

  router.get('/api/app/homeworks/today', authenticateToken, async (req, res) => {
    try {
      const date = String(req.query.date || new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' }));
      if (!isDate(date)) return res.status(400).json({ success: false, error: 'date must be YYYY-MM-DD' });
      res.json({ success: true, ...(await fetchMobileToday({ pool, date })) });
    } catch (error) {
      serverError(res, error, 'Failed to load today from HomeWorks');
    }
  });

  router.get('/api/app/homeworks/visit-skip-preview', authenticateToken, async (req, res) => {
    try {
      const result = await previewMessageVisitSkip({ pool, messageId: req.query.messageId, phone: req.query.phone });
      res.status(result.status).json(result);
    } catch (error) {
      serverError(res, error, 'Failed to find matching HomeWorks visits');
    }
  });

  router.post('/api/app/homeworks/visit-skip', authenticateToken, async (req, res) => {
    try {
      const eventId = Number(req.body.eventId);
      if (!Number.isSafeInteger(eventId) || eventId <= 0) {
        return res.status(400).json({ success: false, error: 'Valid eventId is required' });
      }
      const preview = await previewMessageVisitSkip({ pool, messageId: req.body.messageId, phone: req.body.phone });
      if (preview.status !== 200) return res.status(preview.status).json({ success: false, error: preview.error });
      const visit = preview.visits.find((candidate) => candidate.id === eventId);
      if (!visit) return res.status(409).json({ success: false, error: 'This visit is no longer open in the review window. Refresh and review again.' });
      const reason = `Customer requested skip by text: ${String(preview.message.body || '').slice(0, 1000)}`;
      const result = await queryHomeWorksGraphql({
        pool, operationName: 'TwilioConnectSkipVisit',
        query: `mutation TwilioConnectSkipVisit($eventId: SafeInt!, $reason: String!) {
          skipEvent(eventId: $eventId, skippedReason: $reason) { id status skippedReason startDate }
        }`,
        variables: { eventId, reason },
      });
      if (result.skipEvent?.status !== 'SKIPPED') throw new Error('HomeWorks did not confirm the visit was skipped');
      let noteSaved = false;
      try {
        const note = await queryHomeWorksGraphql({
          pool, operationName: 'TwilioConnectSkipDispatchNote',
          query: `mutation TwilioConnectSkipDispatchNote($eventId: SafeInt!, $input: DispatchNoteInput!) {
            createEventDispatchNote(eventId: $eventId, input: $input) { id }
          }`,
          variables: { eventId, input: { message: `Customer text ${preview.message.id}: ${String(preview.message.body || '').slice(0, 1700)}`, date: new Date().toISOString() } },
        });
        noteSaved = Boolean(note.createEventDispatchNote?.id);
      } catch (noteError) {
        console.warn('Visit skipped, but dispatch note failed:', noteError.message);
      }
      res.json({ success: true, visit: result.skipEvent, customer: preview.customer, noteSaved, source: 'official_homeworks_graphql' });
    } catch (error) {
      serverError(res, error, 'Failed to skip HomeWorks visit');
    }
  });

  router.post('/api/app/homeworks/events/:eventId/communication-note', authenticateToken, async (req, res) => {
    try {
      const eventId = Number(req.params.eventId);
      const message = String(req.body.message || '').trim();
      if (!Number.isInteger(eventId) || eventId <= 0) return res.status(400).json({ success: false, error: 'Valid eventId is required' });
      if (!message) return res.status(400).json({ success: false, error: 'message is required' });
      if (message.length > 2000) return res.status(400).json({ success: false, error: 'message must be 2000 characters or fewer' });
      const data = await queryHomeWorksGraphql({
        pool,
        operationName: 'TwilioConnectCommunicationNote',
        query: `mutation TwilioConnectCommunicationNote($eventId: SafeInt!, $input: DispatchNoteInput!) {
          createEventDispatchNote(eventId: $eventId, input: $input) {
            id dispatchNotes { message author date }
          }
        }`,
        variables: {
          eventId,
          input: { message, date: new Date().toISOString() },
        },
      });
      res.json({ success: true, event: data.createEventDispatchNote, source: 'official_homeworks_graphql' });
    } catch (error) {
      serverError(res, error, 'Failed to attach communication note in HomeWorks');
    }
  });

  return router;
}

module.exports = createAppHomeWorksRoutes;
module.exports.loadLocalCommunications = loadLocalCommunications;
module.exports.previewMessageVisitSkip = previewMessageVisitSkip;
