const express = require('express');
const { getCopilotLiveJobs } = require('../services/copilot/live-jobs');
const { getCopilotToken } = require('../services/copilot/client');

const TIME_ZONE = 'America/New_York';
const DEFAULT_START_DATE = '2026-08-20';
function formatServiceDate(serviceDate) {
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  }).format(new Date(`${serviceDate}T12:00:00.000Z`));
}

function buildReminderBody(serviceDate) {
  return `Automated reminder from Pappas & Co. Landscaping:\n\nYour property is on our schedule for tomorrow, ${formatServiceDate(serviceDate)}.\n\nPlease note that weather or other unexpected delays may affect the schedule.\n\nQuestions? Reply to this text or email hello@pappaslandscaping.com.`;
}

function easternDateParts(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: TIME_ZONE,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(now);
  return Object.fromEntries(parts.map(({ type, value }) => [type, value]));
}

function addDays(date, days) {
  const value = new Date(`${date}T12:00:00.000Z`);
  value.setUTCDate(value.getUTCDate() + days);
  return value.toISOString().slice(0, 10);
}

function tomorrowInEastern(now = new Date()) {
  const parts = easternDateParts(now);
  return addDays(`${parts.year}-${parts.month}-${parts.day}`, 1);
}

function normalizePhone(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return '';
}

function extractAccessToken(cookieHeader) {
  const match = String(cookieHeader || '').match(/(?:^|;\s*)copilotApiAccessToken=([^;]+)/i);
  return match ? decodeURIComponent(match[1]) : '';
}

async function fetchCopilotCustomer(fetchImpl, accessToken, customerId) {
  const response = await fetchImpl('https://api.copilotcrm.com/graphql', {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      operationName: 'ResolveServiceReminderCustomer',
      variables: { customerId: Number(customerId) },
      query: `query ResolveServiceReminderCustomer($customerId: SafeInt!) {
        customers(where: { id: { equals: $customerId } }, take: 1) {
          id
          fullName
          cell
          phone
        }
      }`,
    }),
  });
  if (!response.ok) throw new Error(`Homeworks customer lookup returned ${response.status}`);
  const payload = await response.json();
  if (payload?.errors?.length) throw new Error(payload.errors.map((error) => error.message).join('; '));
  return payload?.data?.customers?.[0] || null;
}

function groupEligibleJobs(jobs) {
  const groups = new Map();
  for (const job of jobs || []) {
    const status = String(job?.status || '').toLowerCase();
    const eventType = String(job?.copilot_event_type || '').toUpperCase();
    const customerId = String(job?.copilot_customer_id || '').trim();
    if (status !== 'pending' || !customerId || (eventType && eventType !== 'VISIT')) continue;
    const group = groups.get(customerId) || { customerId, customerName: job.customer_name || '', jobs: [] };
    group.jobs.push(job);
    groups.set(customerId, group);
  }
  return [...groups.values()];
}

function createServiceReminderRoutes({
  pool,
  authenticateToken,
  twilioClient,
  twilioPhoneNumber,
  fetchImpl = fetch,
  liveJobsProvider = getCopilotLiveJobs,
  tokenProvider = getCopilotToken,
  now = () => new Date(),
}) {
  const router = express.Router();

  async function ensureTable() {
    await pool.query(`CREATE TABLE IF NOT EXISTS service_reminder_sends (
      id BIGSERIAL PRIMARY KEY,
      service_date DATE NOT NULL,
      copilot_customer_id VARCHAR(100) NOT NULL,
      copilot_event_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
      customer_name VARCHAR(255),
      phone VARCHAR(30),
      body TEXT NOT NULL,
      status VARCHAR(30) NOT NULL DEFAULT 'pending',
      twilio_sid VARCHAR(100),
      error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      sent_at TIMESTAMPTZ,
      UNIQUE(service_date, copilot_customer_id)
    )`);
  }

  async function buildPreview(serviceDate) {
    const startDate = process.env.SERVICE_REMINDERS_START_DATE || DEFAULT_START_DATE;
    if (serviceDate < startDate) return { serviceDate, candidates: [], skippedBeforeStart: true };
    const schedule = await liveJobsProvider({ poolClient: pool, date: serviceDate, fetchImpl });
    const groups = groupEligibleJobs(schedule.jobs);
    const tokenInfo = await tokenProvider(pool);
    const accessToken = extractAccessToken(tokenInfo?.cookieHeader);
    if (!accessToken) throw new Error('Homeworks connection is unavailable');

    const candidates = [];
    for (const group of groups) {
      const customer = await fetchCopilotCustomer(fetchImpl, accessToken, group.customerId);
      candidates.push({
        ...group,
        customerName: customer?.fullName || group.customerName,
        phone: normalizePhone(customer?.cell || customer?.phone),
        body: buildReminderBody(serviceDate),
        eventIds: group.jobs.map((job) => String(job.copilot_visit_id || job.visit_id)).filter(Boolean),
      });
    }
    return { serviceDate, candidates, skippedBeforeStart: false, freshness: schedule.freshness };
  }

  async function run(serviceDate, { dryRun = false } = {}) {
    await ensureTable();
    const preview = await buildPreview(serviceDate);
    if (dryRun || preview.skippedBeforeStart) return { ...preview, sent: 0, skipped: preview.candidates.length, failed: 0 };
    if (!twilioClient?.messages?.create) throw new Error('Twilio messaging is unavailable');

    let sent = 0;
    let skipped = 0;
    let failed = 0;
    const results = [];
    for (const candidate of preview.candidates) {
      if (!candidate.phone) {
        skipped += 1;
        results.push({ customerId: candidate.customerId, status: 'skipped', reason: 'missing_phone' });
        continue;
      }
      const reservation = await pool.query(
        `INSERT INTO service_reminder_sends
          (service_date, copilot_customer_id, copilot_event_ids, customer_name, phone, body)
         VALUES ($1::date, $2, $3::jsonb, $4, $5, $6)
         ON CONFLICT (service_date, copilot_customer_id) DO NOTHING
         RETURNING id`,
        [serviceDate, candidate.customerId, JSON.stringify(candidate.eventIds), candidate.customerName, candidate.phone, candidate.body]
      );
      if (!reservation.rows[0]) {
        skipped += 1;
        results.push({ customerId: candidate.customerId, status: 'skipped', reason: 'already_processed' });
        continue;
      }
      const sendId = reservation.rows[0].id;
      try {
        const message = await twilioClient.messages.create({
          from: twilioPhoneNumber,
          to: candidate.phone,
          body: candidate.body,
        });
        await pool.query(
          `UPDATE service_reminder_sends SET status = 'sent', twilio_sid = $1, sent_at = NOW() WHERE id = $2`,
          [message.sid, sendId]
        );
        await pool.query(
          `INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, created_at)
           VALUES ($1, 'outbound', $2, $3, $4, $5, NOW()) ON CONFLICT (twilio_sid) DO NOTHING`,
          [message.sid, twilioPhoneNumber, candidate.phone, candidate.body, message.status || 'queued']
        );
        sent += 1;
        results.push({ customerId: candidate.customerId, status: 'sent', sid: message.sid });
      } catch (error) {
        failed += 1;
        await pool.query(
          `UPDATE service_reminder_sends SET status = 'failed', error = $1 WHERE id = $2`,
          [String(error.message || error), sendId]
        );
        results.push({ customerId: candidate.customerId, status: 'failed', reason: error.message });
      }
    }
    return { serviceDate, sent, skipped, failed, results };
  }

  router.get('/api/service-reminders/preview', authenticateToken, async (req, res) => {
    try {
      const serviceDate = String(req.query.date || tomorrowInEastern(now()));
      res.json({ success: true, ...(await run(serviceDate, { dryRun: true })) });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  });

  async function handleCron(req, res) {
    const configuredSecret = process.env.CRON_SECRET || process.env.CRON_API_KEY || '';
    const suppliedSecret = req.get('x-cron-secret') || req.query.key || req.body?.key || '';
    if (!configuredSecret || suppliedSecret !== configuredSecret) {
      return res.status(401).json({ success: false, error: 'Invalid cron secret' });
    }
    if (process.env.SERVICE_REMINDERS_ENABLED !== 'true') {
      return res.status(503).json({ success: false, error: 'Service reminders are not enabled' });
    }
    try {
      const serviceDate = String(req.query.date || req.body?.date || tomorrowInEastern(now()));
      res.json({ success: true, ...(await run(serviceDate)) });
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  }

  router.post('/api/cron/service-reminders', handleCron);
  router.get('/api/cron/service-reminders', handleCron);
  router.runServiceReminders = run;
  router.startScheduler = () => {
    let lastRunMinute = '';
    const check = async () => {
      if (process.env.SERVICE_REMINDERS_ENABLED !== 'true') return;
      const current = now();
      const parts = easternDateParts(current);
      const minuteKey = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}`;
      if (parts.hour !== '18' || parts.minute !== '00' || minuteKey === lastRunMinute) return;
      lastRunMinute = minuteKey;
      try {
        const result = await run(tomorrowInEastern(current));
        console.log('Service reminder automation complete:', JSON.stringify(result));
      } catch (error) {
        console.error('Service reminder automation failed:', error);
      }
    };
    const timer = setInterval(check, 30 * 1000);
    timer.unref?.();
    check();
    return timer;
  };
  return router;
}

module.exports = createServiceReminderRoutes;
module.exports._helpers = { addDays, buildReminderBody, easternDateParts, formatServiceDate, groupEligibleJobs, normalizePhone, tomorrowInEastern };
