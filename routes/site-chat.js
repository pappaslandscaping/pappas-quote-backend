const express = require('express');
const crypto = require('crypto');

const MAX_MESSAGE = 1200;
const tokenHash = (token) => crypto.createHash('sha256').update(token).digest('hex');
const clean = (value, limit) => String(value || '').trim().slice(0, limit);
const validId = (value) => /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value);

function isAlertHours(now = new Date(), schedule = process.env.SITE_CHAT_ALERT_HOURS || '1,2,3,4,5:09:00-18:00;6:09:00-16:00') {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York', weekday: 'short', hour: '2-digit', minute: '2-digit', hourCycle: 'h23'
  }).formatToParts(now);
  const read = (type) => parts.find((part) => part.type === type)?.value;
  const day = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].indexOf(read('weekday'));
  const minute = Number(read('hour')) * 60 + Number(read('minute'));
  return schedule.split(';').some((window) => {
    const match = /^(\d(?:,\d)*):(\d\d):(\d\d)-(\d\d):(\d\d)$/.exec(window);
    if (!match) return false;
    const start = Number(match[2]) * 60 + Number(match[3]);
    const end = Number(match[4]) * 60 + Number(match[5]);
    return match[1].split(',').map(Number).includes(day) && minute >= start && minute < end;
  });
}

function createSiteChatRoutes({ pool, twilioClient, fromNumber, ownerNumber, verifyRecaptcha, recaptchaRequired = false, now = () => new Date() }) {
  const router = express.Router();

  async function findVisitorChat(req, res) {
    const token = clean(req.get('x-chat-token'), 128);
    if (!validId(req.params.id) || !/^[a-f0-9]{64}$/.test(token)) {
      res.status(404).json({ success: false, error: 'Chat not found' });
      return null;
    }
    const result = await pool.query(
      `SELECT id, status FROM site_chats WHERE id = $1 AND visitor_token_hash = $2
       AND created_at > NOW() - INTERVAL '7 days'`,
      [req.params.id, tokenHash(token)]
    );
    if (!result.rows[0]) res.status(404).json({ success: false, error: 'Chat not found' });
    return result.rows[0] || null;
  }

  router.post('/api/site-chat', async (req, res) => {
    const input = req.body || {};
    const name = clean(input.name, 80);
    const contact = clean(input.contact, 160);
    const message = clean(input.message, MAX_MESSAGE);
    if (!name || !message || String(input.message || '').length > MAX_MESSAGE) {
      return res.status(400).json({ success: false, error: 'Please enter your name and message.' });
    }
    if (contact && !/^[+()\-\s\d]{7,30}$/.test(contact) && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contact)) {
      return res.status(400).json({ success: false, error: 'Enter a valid phone or email, or leave contact blank.' });
    }
    if (recaptchaRequired) {
      if (!input.recaptchaToken) return res.status(400).json({ success: false, error: 'Security verification required.' });
      const check = await verifyRecaptcha(input.recaptchaToken);
      if (!check.success || check.score < 0.5 || check.action !== 'site_chat') {
        return res.status(403).json({ success: false, error: 'Security verification failed. Please try again.' });
      }
    }
    const token = crypto.randomBytes(32).toString('hex');
    const id = crypto.randomUUID();
    try {
      const created = await pool.query(
        `INSERT INTO site_chats (id, visitor_token_hash, visitor_name, visitor_contact)
         VALUES ($1, $2, $3, $4) RETURNING id`,
        [id, tokenHash(token), name, contact || null]
      );
      const savedId = created.rows[0].id;
      await pool.query(
        `INSERT INTO site_chat_messages (chat_id, sender, body) VALUES ($1, 'visitor', $2)`,
        [savedId, message]
      );
      let alerted = false;
      const afterHours = !isAlertHours(now());
      if (!afterHours && twilioClient && fromNumber && ownerNumber) {
        try {
          await twilioClient.messages.create({
            from: fromNumber,
            to: ownerNumber,
            body: `New website chat from ${name}. Reply in YardDesk: https://app.pappaslandscaping.com/live-chat.html?chat=${savedId}`,
          });
          alerted = true;
        } catch (error) {
          console.error('Website chat alert failed:', error);
        }
      }
      return res.status(201).json({ success: true, id: savedId, token, alerted, afterHours, pollSeconds: 5 });
    } catch (error) {
      console.error('Website chat creation failed:', error);
      return res.status(500).json({ success: false, error: 'Chat could not be started.' });
    }
  });

  router.get('/api/site-chat/:id', async (req, res) => {
    try {
      const chat = await findVisitorChat(req, res);
      if (!chat) return;
      const result = await pool.query(
        `SELECT id, sender, body, created_at FROM site_chat_messages
         WHERE chat_id = $1 ORDER BY id ASC LIMIT 200`, [chat.id]
      );
      res.set('Cache-Control', 'no-store');
      return res.json({ success: true, status: chat.status, messages: result.rows });
    } catch (error) {
      console.error('Website chat read failed:', error);
      return res.status(500).json({ success: false, error: 'Chat unavailable.' });
    }
  });

  router.post('/api/site-chat/:id/messages', async (req, res) => {
    const body = clean(req.body?.message, MAX_MESSAGE);
    if (!body || String(req.body?.message || '').length > MAX_MESSAGE) {
      return res.status(400).json({ success: false, error: 'Enter a message under 1200 characters.' });
    }
    try {
      const chat = await findVisitorChat(req, res);
      if (!chat) return;
      if (chat.status !== 'open') return res.status(409).json({ success: false, error: 'This chat is closed.' });
      const result = await pool.query(
        `INSERT INTO site_chat_messages (chat_id, sender, body) VALUES ($1, 'visitor', $2)
         RETURNING id, sender, body, created_at`, [chat.id, body]
      );
      await pool.query(`UPDATE site_chats SET updated_at = NOW() WHERE id = $1`, [chat.id]);
      return res.status(201).json({ success: true, message: result.rows[0] });
    } catch (error) {
      console.error('Website chat send failed:', error);
      return res.status(500).json({ success: false, error: 'Message could not be sent.' });
    }
  });

  function staffOnly(req, res, next) {
    if (!req.user || !req.user.isAdmin || req.user.isEmployee) {
      return res.status(403).json({ success: false, error: 'Admin access required' });
    }
    next();
  }
  router.use('/api/site-chat-staff', staffOnly);

  router.get('/api/site-chat-staff', async (_req, res) => {
    try {
      const result = await pool.query(
        `SELECT c.id, c.visitor_name, c.visitor_contact, c.status, c.created_at, c.updated_at,
          (SELECT body FROM site_chat_messages WHERE chat_id = c.id ORDER BY id DESC LIMIT 1) AS last_message,
          (SELECT sender FROM site_chat_messages WHERE chat_id = c.id ORDER BY id DESC LIMIT 1) AS last_sender
         FROM site_chats c WHERE c.created_at > NOW() - INTERVAL '7 days'
         ORDER BY CASE WHEN c.status = 'open' THEN 0 ELSE 1 END, c.updated_at DESC LIMIT 100`
      );
      res.set('Cache-Control', 'no-store');
      return res.json({ success: true, chats: result.rows });
    } catch (error) {
      console.error('Staff chat list failed:', error);
      return res.status(500).json({ success: false, error: 'Chats unavailable.' });
    }
  });

  router.get('/api/site-chat-staff/:id', async (req, res) => {
    if (!validId(req.params.id)) return res.status(404).json({ success: false, error: 'Chat not found' });
    try {
      const chat = await pool.query(`SELECT id, visitor_name, visitor_contact, status FROM site_chats WHERE id = $1`, [req.params.id]);
      if (!chat.rows[0]) return res.status(404).json({ success: false, error: 'Chat not found' });
      const messages = await pool.query(
        `SELECT id, sender, body, created_at FROM site_chat_messages WHERE chat_id = $1 ORDER BY id ASC LIMIT 200`, [req.params.id]
      );
      res.set('Cache-Control', 'no-store');
      return res.json({ success: true, chat: chat.rows[0], messages: messages.rows });
    } catch (error) {
      console.error('Staff chat read failed:', error);
      return res.status(500).json({ success: false, error: 'Chat unavailable.' });
    }
  });

  router.post('/api/site-chat-staff/:id/messages', async (req, res) => {
    const body = clean(req.body?.message, MAX_MESSAGE);
    if (!validId(req.params.id)) return res.status(404).json({ success: false, error: 'Chat not found' });
    if (!body || String(req.body?.message || '').length > MAX_MESSAGE) {
      return res.status(400).json({ success: false, error: 'Enter a message under 1200 characters.' });
    }
    try {
      const chat = await pool.query(`SELECT status FROM site_chats WHERE id = $1`, [req.params.id]);
      if (!chat.rows[0]) return res.status(404).json({ success: false, error: 'Chat not found' });
      if (chat.rows[0].status !== 'open') return res.status(409).json({ success: false, error: 'This chat is closed.' });
      const result = await pool.query(
        `INSERT INTO site_chat_messages (chat_id, sender, body) VALUES ($1, 'staff', $2)
         RETURNING id, sender, body, created_at`, [req.params.id, body]
      );
      await pool.query(`UPDATE site_chats SET updated_at = NOW() WHERE id = $1`, [req.params.id]);
      return res.status(201).json({ success: true, message: result.rows[0] });
    } catch (error) {
      console.error('Staff chat send failed:', error);
      return res.status(500).json({ success: false, error: 'Message could not be sent.' });
    }
  });

  router.post('/api/site-chat-staff/:id/close', async (req, res) => {
    if (!validId(req.params.id)) return res.status(404).json({ success: false, error: 'Chat not found' });
    try {
      const result = await pool.query(
        `UPDATE site_chats SET status = 'closed', updated_at = NOW() WHERE id = $1 RETURNING id`, [req.params.id]
      );
      if (!result.rows[0]) return res.status(404).json({ success: false, error: 'Chat not found' });
      return res.json({ success: true });
    } catch (error) {
      console.error('Staff chat close failed:', error);
      return res.status(500).json({ success: false, error: 'Chat could not be closed.' });
    }
  });

  return router;
}

module.exports = createSiteChatRoutes;
module.exports.isAlertHours = isAlertHours;
