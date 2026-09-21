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
      });
      res.json({ success: true, customers, source: 'official_homeworks_graphql', asOf: new Date().toISOString() });
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
