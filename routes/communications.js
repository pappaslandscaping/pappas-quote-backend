// ═══════════════════════════════════════════════════════════
// Communications Routes — extracted from server.js
// Handles: admin message threads/conversations, SMS webhook,
//          broadcasts (preview/send/filter-options), email log
// ═══════════════════════════════════════════════════════════

const express = require('express');
const crypto = require('crypto');
const { validate, schemas } = require('../lib/validate');

module.exports = function createCommunicationRoutes({ pool, sendEmail, emailTemplate, escapeHtml, serverError, twilioClient, TWILIO_PHONE_NUMBER, NOTIFICATION_EMAIL }) {
  const router = express.Router();

  async function findCustomerContext({ customerId, phoneNumber }) {
    if (customerId) {
      const result = await pool.query(
        'SELECT id, name, email, phone, mobile FROM customers WHERE id = $1 LIMIT 1',
        [customerId]
      );
      return result.rows[0] || null;
    }

    if (phoneNumber) {
      const cleanedPhone = String(phoneNumber).replace(/\D/g, '').slice(-10);
      if (!cleanedPhone) return null;
      const result = await pool.query(`
        SELECT id, name, email, phone, mobile
        FROM customers
        WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1
           OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1
        ORDER BY id ASC
        LIMIT 1
      `, [`%${cleanedPhone}`]);
      return result.rows[0] || null;
    }

    return null;
  }

  function plainTextToHtml(text) {
    return String(text || '')
      .split(/\r?\n/)
      .map((line) => escapeHtml(line))
      .join('<br>');
  }

router.get('/api/messages/conversations', async (req, res) => {
  try {
    const result = await pool.query(`
      WITH latest_messages AS (
        SELECT DISTINCT ON (
          CASE 
            WHEN direction = 'inbound' THEN from_number 
            ELSE to_number 
          END
        )
        id, twilio_sid, direction, from_number, to_number, body, media_urls, status, customer_id, read, created_at,
        CASE 
          WHEN direction = 'inbound' THEN from_number 
          ELSE to_number 
        END as contact_number
        FROM messages
        ORDER BY contact_number, created_at DESC
      )
      SELECT 
        lm.*,
        c.name as customer_name,
        (SELECT COUNT(*) FROM messages m2 
         WHERE m2.read = false 
         AND m2.direction = 'inbound'
         AND (m2.from_number = lm.contact_number OR m2.to_number = lm.contact_number)
        ) as unread_count,
        (SELECT COUNT(*) FROM messages m3 
         WHERE m3.from_number = lm.contact_number OR m3.to_number = lm.contact_number
        ) as message_count
      FROM latest_messages lm
      LEFT JOIN customers c ON lm.customer_id = c.id
      ORDER BY lm.created_at DESC
      LIMIT 100
    `);

    // Enrich with customer names where missing
    const conversations = await Promise.all(result.rows.map(async (conv) => {
      if (!conv.customer_name) {
        const cleanedPhone = conv.contact_number.replace(/\D/g, '').slice(-10);
        const customerResult = await pool.query(`
          SELECT name FROM customers 
          WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
             OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
          LIMIT 1
        `, [`%${cleanedPhone}`]);
        conv.customer_name = customerResult.rows[0]?.name || null;
      }
      return {
        id: conv.id,
        phoneNumber: conv.contact_number,
        customerName: conv.customer_name,
        lastMessage: conv.body,
        lastMessageTime: conv.created_at,
        direction: conv.direction,
        unreadCount: parseInt(conv.unread_count) || 0,
        messageCount: parseInt(conv.message_count) || 0,
        read: conv.read
      };
    }));

    res.json({ success: true, conversations });
  } catch (error) {
    console.error('Get conversations error:', error);
    res.status(500).json({ success: false, conversations: [] });
  }
});

// Get all messages for a specific conversation thread
router.get('/api/messages/thread/:phoneNumber', async (req, res) => {
  const { phoneNumber } = req.params;
  try {
    const result = await pool.query(`
      SELECT m.*, c.name as customer_name
      FROM messages m
      LEFT JOIN customers c ON m.customer_id = c.id
      WHERE m.from_number = $1 OR m.to_number = $1
      ORDER BY m.created_at ASC
    `, [phoneNumber]);

    // Mark inbound messages as read
    await pool.query(`
      UPDATE messages SET read = true 
      WHERE (from_number = $1 OR to_number = $1) AND direction = 'inbound' AND read = false
    `, [phoneNumber]);

    const messages = result.rows.map(msg => ({
      id: msg.id,
      sid: msg.twilio_sid,
      direction: msg.direction,
      from: msg.from_number,
      to: msg.to_number,
      body: msg.body,
      mediaUrls: msg.media_urls,
      status: msg.status,
      customerName: msg.customer_name,
      timestamp: msg.created_at,
      read: msg.read
    }));

    res.json({ success: true, messages });
  } catch (error) {
    console.error('Get thread error:', error);
    res.status(500).json({ success: false, messages: [] });
  }
});

// Get all messages for web dashboard (legacy - flat list)
router.get('/api/messages', async (req, res) => {
  const limit = parseInt(req.query.limit) || 200;
  try {
    const result = await pool.query(`
      SELECT 
        m.*,
        c.name as customer_name
      FROM messages m
      LEFT JOIN customers c ON m.customer_id = c.id
      ORDER BY m.created_at DESC
      LIMIT $1
    `, [limit]);

    // Enrich with customer names where missing
    const messages = await Promise.all(result.rows.map(async (msg) => {
      if (!msg.customer_name) {
        const phoneToSearch = msg.direction === 'inbound' ? msg.from_number : msg.to_number;
        const cleanedPhone = phoneToSearch.replace(/\D/g, '').slice(-10);
        const customerResult = await pool.query(`
          SELECT name FROM customers 
          WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
             OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
          LIMIT 1
        `, [`%${cleanedPhone}`]);
        msg.customer_name = customerResult.rows[0]?.name || null;
      }
      return {
        id: msg.id,
        sid: msg.twilio_sid,
        direction: msg.direction,
        from: msg.from_number,
        to: msg.to_number,
        body: msg.body,
        status: msg.status,
        customerName: msg.customer_name,
        timestamp: msg.created_at,
        read: msg.read
      };
    }));

    res.json({ success: true, messages });
  } catch (error) {
    console.error('Get messages error:', error);
    res.status(500).json({ success: false, messages: [] });
  }
});

// Send SMS from web dashboard
router.post('/api/messages/send', validate(schemas.sendMessage), async (req, res) => {
  const { to, body } = req.body;

  try {
    let formattedTo = to.replace(/\D/g, '');
    if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
    else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

    const twilioMessage = await twilioClient.messages.create({
      body,
      from: TWILIO_PHONE_NUMBER,
      to: formattedTo
    });

    // Find customer
    const cleanedPhone = formattedTo.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`
      SELECT id FROM customers 
      WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
         OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
      LIMIT 1
    `, [`%${cleanedPhone}`]);

    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
      VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
    `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, formattedTo, body, twilioMessage.status, customerResult.rows[0]?.id || null]);

    res.json({ success: true, sid: twilioMessage.sid });
  } catch (error) {
    console.error('Send SMS error:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

router.post('/api/communications/email/send', validate(schemas.sendOperationalEmail), async (req, res) => {
  try {
    const {
      to,
      customer_id: customerId,
      customer_name: customerName,
      phone_number: phoneNumber,
      subject,
      body,
      html_body: htmlBody,
    } = req.body;

    if (!subject || !String(subject).trim()) {
      return res.status(400).json({ success: false, error: 'subject is required' });
    }

    if ((!body || !String(body).trim()) && (!htmlBody || !String(htmlBody).trim())) {
      return res.status(400).json({ success: false, error: 'body or html_body is required' });
    }

    const customer = await findCustomerContext({ customerId, phoneNumber });
    const recipient = String(to || customer?.email || '').trim();

    if (!recipient) {
      return res.status(400).json({
        success: false,
        error: 'No recipient email available for this contact',
      });
    }

    const innerHtml = htmlBody && String(htmlBody).trim()
      ? String(htmlBody)
      : plainTextToHtml(body);

    await sendEmail(
      recipient,
      String(subject).trim(),
      emailTemplate(innerHtml),
      null,
      {
        type: 'communication',
        customer_id: customer?.id || customerId || null,
        customer_name: customer?.name || customerName || null,
      }
    );

    res.json({
      success: true,
      recipient_email: recipient,
      customer: customer ? {
        id: customer.id,
        name: customer.name,
        email: customer.email,
      } : null,
    });
  } catch (error) {
    console.error('Inbox email send error:', error);
    serverError(res, error);
  }
});


router.post('/api/sms/webhook', async (req, res) => {
  const { MessageSid, From, To, Body, NumMedia } = req.body;
  
  try {
    // Get media URLs if any
    const mediaUrls = [];
    const numMedia = parseInt(NumMedia) || 0;
    for (let i = 0; i < numMedia; i++) {
      if (req.body[`MediaUrl${i}`]) {
        mediaUrls.push(req.body[`MediaUrl${i}`]);
      }
    }

    // Find customer by phone number
    const cleanedPhone = From.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`
      SELECT id, name FROM customers 
      WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
         OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
      LIMIT 1
    `, [`%${cleanedPhone}`]);
    
    const customerId = customerResult.rows[0]?.id || null;
    const customerName = customerResult.rows[0]?.name || 'Unknown';

    // Store message
    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, media_urls, status, customer_id, read)
      VALUES ($1, 'inbound', $2, $3, $4, $5, 'received', $6, false)
      ON CONFLICT (twilio_sid) DO NOTHING
    `, [MessageSid, From, To, Body, mediaUrls, customerId]);

    console.log(`📨 Incoming SMS from ${customerName} (${From}): ${Body?.substring(0, 50)}...`);

    // Send push notification
    await sendPushToAllDevices(`💬 ${customerName}`, Body?.substring(0, 100) || 'New message', { type: 'sms', phoneNumber: cleanedPhone, contactName: customerName });

    // Send email notification (fire-and-forget)
    const smsDisplayName = customerName !== 'Unknown' ? escapeHtml(customerName) : From;
    const smsTimestamp = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', dateStyle: 'medium', timeStyle: 'short' });
    sendEmail(NOTIFICATION_EMAIL, `💬 New text from ${customerName !== 'Unknown' ? customerName : From}`, emailTemplate(`
      <h2 style="color:#1e293b;margin:0 0 16px;">New Text Message</h2>
      <table style="width:100%;border-collapse:collapse;">
        <tr><td style="padding:8px 0;color:#64748b;width:80px;">From</td><td style="padding:8px 0;color:#1e293b;font-weight:500;">${smsDisplayName}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b;">Phone</td><td style="padding:8px 0;color:#1e293b;">${escapeHtml(From)}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b;">Time</td><td style="padding:8px 0;color:#1e293b;">${smsTimestamp}</td></tr>
      </table>
      <div style="margin-top:20px;padding:16px;background:#f8fafc;border-radius:8px;border-left:4px solid #2e403d;">
        <p style="margin:0;color:#1e293b;line-height:1.6;">${escapeHtml(Body || 'No message content')}</p>
      </div>
    `, { showSignature: false })).catch(err => console.error('SMS notification email error:', err));

    // Send TwiML response (empty - don't auto-reply)
    res.type('text/xml').send('<Response></Response>');
  } catch (error) {
    console.error('SMS webhook error:', error);
    res.type('text/xml').send('<Response></Response>');
  }
});

// Get message conversations (grouped by NORMALIZED phone number) - for app
// FIX: Now properly groups by last 10 digits to prevent duplicate conversations

router.get('/api/broadcasts/filter-options', async (req, res) => {
  // Auth already verified by global middleware
  try {
    // Get all unique tags (comma-separated field, need to split and deduplicate)
    const [tagsResult, cities, postalCodes, statuses, customerTypes] = await Promise.all([
      pool.query(`SELECT DISTINCT tags FROM customers WHERE tags IS NOT NULL AND tags != ''`),
      pool.query(`SELECT DISTINCT city FROM customers WHERE city IS NOT NULL AND city != '' ORDER BY city`),
      pool.query(`SELECT DISTINCT postal_code FROM customers WHERE postal_code IS NOT NULL AND postal_code != '' ORDER BY postal_code`),
      pool.query(`SELECT DISTINCT status FROM customers WHERE status IS NOT NULL AND status != '' ORDER BY status`),
      pool.query(`SELECT DISTINCT customer_type FROM customers WHERE customer_type IS NOT NULL AND customer_type != '' ORDER BY customer_type`)
    ]);
    const tagSet = new Set();
    for (const row of tagsResult.rows) {
      (row.tags || '').split(',').forEach(t => { const trimmed = t.trim(); if (trimmed) tagSet.add(trimmed); });
    }

    res.json({
      success: true,
      tags: Array.from(tagSet).sort(),
      cities: cities.rows.map(r => r.city),
      postal_codes: postalCodes.rows.map(r => r.postal_code),
      statuses: statuses.rows.map(r => r.status),
      customer_types: customerTypes.rows.map(r => r.customer_type)
    });
  } catch (error) {
    console.error('Broadcast filter-options error:', error);
    serverError(res, error);
  }
});

// POST /api/broadcasts/preview - Preview audience with filters
router.post('/api/broadcasts/preview', async (req, res) => {
  // Auth already verified by global middleware
  try {
    const filters = req.body.filters || {};
    const conditions = [];
    const params = [];
    let paramIdx = 1;

    // Tags filter (comma-separated text field, match ANY of the provided tags)
    if (filters.tags && filters.tags.length > 0) {
      const tagConditions = filters.tags.map(tag => {
        params.push(`%${tag}%`);
        return `c.tags ILIKE $${paramIdx++}`;
      });
      conditions.push(`(${tagConditions.join(' OR ')})`);
    }

    // Postal codes
    if (filters.postal_codes && filters.postal_codes.length > 0) {
      params.push(filters.postal_codes);
      conditions.push(`c.postal_code = ANY($${paramIdx++})`);
    }

    // Cities (case-insensitive)
    if (filters.cities && filters.cities.length > 0) {
      params.push(filters.cities.map(c => c.toLowerCase()));
      conditions.push(`LOWER(c.city) = ANY($${paramIdx++})`);
    }

    // Status
    if (filters.status) {
      params.push(filters.status);
      conditions.push(`c.status = $${paramIdx++}`);
    }

    // Customer type
    if (filters.customer_type) {
      params.push(filters.customer_type);
      conditions.push(`c.customer_type = $${paramIdx++}`);
    }

    // Has email
    if (filters.has_email) {
      conditions.push(`c.email IS NOT NULL AND c.email != ''`);
    }

    // Has mobile
    if (filters.has_mobile) {
      conditions.push(`c.mobile IS NOT NULL AND c.mobile != ''`);
    }

    // Monthly plan
    if (filters.monthly_plan) {
      conditions.push(`c.monthly_plan_amount > 0`);
    }

    // Active since N months (had jobs in last N months)
    if (filters.active_since_months) {
      params.push(filters.active_since_months);
      conditions.push(`c.id IN (SELECT DISTINCT customer_id FROM scheduled_jobs WHERE created_at >= NOW() - ($${paramIdx++} || ' months')::INTERVAL)`);
    }

    // Scheduled on specific date (for daily reminders)
    if (filters.job_date) {
      params.push(filters.job_date);
      conditions.push(`c.id IN (SELECT DISTINCT customer_id FROM scheduled_jobs WHERE job_date::date = $${paramIdx++}::date AND customer_id IS NOT NULL)`);
    }

    const whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
    const query = `SELECT c.id, c.name, c.first_name, c.last_name, c.email, c.mobile, c.city, c.postal_code, c.tags FROM customers c ${whereClause} ORDER BY c.name`;
    const result = await pool.query(query, params);

    // Normalize names
    const customers = result.rows.map(c => ({
      id: c.id,
      name: c.name || ((c.first_name || '') + (c.last_name ? ' ' + c.last_name : '')).trim() || 'Unknown',
      email: c.email,
      mobile: c.mobile,
      city: c.city,
      postal_code: c.postal_code,
      tags: c.tags
    }));

    // Build summary stats
    const summary = {
      total: customers.length,
      with_email: customers.filter(c => c.email && c.email.trim()).length,
      with_mobile: customers.filter(c => c.mobile && c.mobile.trim()).length,
      email_opted_in: customers.length, // default: assume opted in
      sms_opted_in: 0
    };

    // Check communication prefs for opted-in counts
    if (customers.length > 0) {
      const custIds = customers.map(c => c.id);
      const prefs = await pool.query('SELECT customer_id, email_marketing, sms_marketing FROM customer_communication_prefs WHERE customer_id = ANY($1)', [custIds]);
      const prefsMap = {};
      prefs.rows.forEach(p => { prefsMap[p.customer_id] = p; });
      let emailOptIn = 0, smsOptIn = 0;
      customers.forEach(c => {
        const p = prefsMap[c.id];
        if (!p || p.email_marketing !== false) emailOptIn++;
        if (p && p.sms_marketing === true) smsOptIn++;
      });
      summary.email_opted_in = emailOptIn;
      summary.sms_opted_in = smsOptIn;
    }

    res.json({ success: true, count: customers.length, customers, summary });
  } catch (error) {
    console.error('Broadcast preview error:', error);
    serverError(res, error);
  }
});

// POST /api/broadcasts/send - Send broadcast email and/or SMS
// requireAdmin middleware blocks employees at the /api/broadcasts/send mount
router.post('/api/broadcasts/send', async (req, res) => {
  // Auth already verified by global middleware
  try {
    const { channel, template_id, sms_body, customer_ids, campaign_id, job_date } = req.body;
    if (!channel || !['email', 'sms', 'both'].includes(channel)) {
      return res.status(400).json({ success: false, error: 'channel must be email, sms, or both' });
    }
    if (!customer_ids || customer_ids.length === 0) {
      return res.status(400).json({ success: false, error: 'customer_ids required' });
    }
    if ((channel === 'email' || channel === 'both') && !template_id) {
      return res.status(400).json({ success: false, error: 'template_id required for email' });
    }
    if ((channel === 'sms' || channel === 'both') && !sms_body) {
      return res.status(400).json({ success: false, error: 'sms_body required for SMS' });
    }

    // Load template for email
    let tmpl = null;
    if (template_id) {
      const templateResult = await pool.query('SELECT * FROM email_templates WHERE id = $1', [template_id]);
      if (templateResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
      tmpl = templateResult.rows[0];
    }

    // Load customers
    const custResult = await pool.query('SELECT * FROM customers WHERE id = ANY($1)', [customer_ids]);

    // Load communication preferences for all target customers
    const prefsResult = await pool.query('SELECT * FROM customer_communication_prefs WHERE customer_id = ANY($1)', [customer_ids]);
    const prefsMap = {};
    for (const p of prefsResult.rows) { prefsMap[p.customer_id] = p; }

    const results = { email_sent: 0, email_skipped: 0, email_errors: 0, sms_sent: 0, sms_skipped: 0, sms_errors: 0 };
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';

    for (const cust of custResult.rows) {
      const custName = cust.name || ((cust.first_name || '') + (cust.last_name ? ' ' + cust.last_name : '')).trim() || 'Unknown';
      const vars = {
        customer_name: custName,
        customer_first_name: cust.first_name || custName,
        customer_email: cust.email,
        customer_phone: cust.phone || cust.mobile,
        customer_address: [cust.street, cust.city, cust.state, cust.postal_code].filter(Boolean).join(', '),
        company_name: 'Pappas & Co. Landscaping',
        company_phone: '(440) 886-7318',
        company_email: 'hello@pappaslandscaping.com',
        company_website: 'pappaslandscaping.com',
        portal_link: `${baseUrl}/customer-portal.html`,
        unsubscribe_email: encodeURIComponent(cust.email || '')
      };

      // If job_date provided, look up ALL job details for this customer on that date
      if (job_date) {
        try {
          const jobResult = await pool.query(
            `SELECT service_type, address, service_price, job_date FROM scheduled_jobs
             WHERE customer_id = $1 AND job_date::date = $2::date
             ORDER BY id ASC`,
            [cust.id, job_date]
          );
          if (jobResult.rows.length > 0) {
            if (jobResult.rows.length === 1) {
              // Single job — keep simple format
              const job = jobResult.rows[0];
              vars.service_type = job.service_type || '';
              const fullAddr = job.address || vars.customer_address || '';
              vars.address = fullAddr.split(',')[0].trim();
              vars.service_list = `${vars.service_type} at ${vars.address}`;
              vars.services_list = vars.service_list;
              vars.service_price = job.service_price ? '$' + Number(job.service_price).toFixed(2) : '';
            } else {
              // Multiple jobs — build "Mowing at 123 Main St and Spring Cleanup at 456 Oak Ave"
              const jobParts = jobResult.rows.map(j => {
                const svc = j.service_type || '';
                const fa = j.address || vars.customer_address || '';
                const street = fa.split(',')[0].trim();
                return `${svc} at ${street}`;
              });
              vars.service_list = jobParts.join(' and ');
              vars.services_list = vars.service_list;
              vars.service_type = jobResult.rows.map(j => j.service_type || '').join(' & ');
              vars.address = jobResult.rows.map(j => {
                const fa = j.address || vars.customer_address || '';
                return fa.split(',')[0].trim();
              }).join(' & ');
              const total = jobResult.rows.reduce((sum, j) => sum + (j.service_price ? Number(j.service_price) : 0), 0);
              vars.service_price = total > 0 ? '$' + total.toFixed(2) : '';
            }
            const firstJob = jobResult.rows[0];
            vars.job_date = firstJob.job_date ? new Date(firstJob.job_date).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' }) : '';
          }
        } catch (e) { console.error('Job lookup error:', e.message); }
      }

      const prefs = prefsMap[cust.id];

      // Send email
      if (channel === 'email' || channel === 'both') {
        // Check prefs: default allow email if no prefs row
        const emailAllowed = prefs ? prefs.email_marketing !== false : true;
        if (!emailAllowed || !cust.email) {
          results.email_skipped++;
        } else {
          try {
            const trackingId = crypto.randomUUID().replace(/-/g, '').slice(0, 24);
            const subject = replaceTemplateVars(tmpl.subject, vars);
            let body = replaceTemplateVars(tmpl.body, vars);
            body += `<img src="${baseUrl}/api/t/${trackingId}/open.png" width="1" height="1" style="display:none;" />`;
            const finalHtml = replaceTemplateVars(emailTemplate(body), vars);
            await sendEmail(cust.email, subject, finalHtml, null, { type: 'broadcast', customer_id: cust.id, customer_name: custName });
            // Track in campaign_sends if campaign_id provided
            if (campaign_id) {
              await pool.query(
                'INSERT INTO campaign_sends (campaign_id, template_id, customer_id, customer_email, status, tracking_id) VALUES ($1, $2, $3, $4, $5, $6)',
                [campaign_id, template_id, cust.id, cust.email, 'sent', trackingId]
              );
            }
            results.email_sent++;
          } catch (e) {
            console.error(`Broadcast email error for customer ${cust.id}:`, e.message);
            results.email_errors++;
          }
        }
      }

      // Send SMS
      if (channel === 'sms' || channel === 'both') {
        // Check prefs: allow SMS by default if no prefs row exists
        const smsAllowed = prefs ? prefs.sms_marketing !== false : true;
        if (!smsAllowed || !cust.mobile) {
          results.sms_skipped++;
        } else {
          try {
            const smsText = replaceTemplateVars(sms_body, vars);
            let formattedTo = cust.mobile.replace(/\D/g, '');
            if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
            else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

            const twilioMessage = await twilioClient.messages.create({
              body: smsText,
              from: TWILIO_PHONE_NUMBER,
              to: formattedTo
            });

            // Log to messages table
            await pool.query(`
              INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
              VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
            `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, formattedTo, smsText, twilioMessage.status, cust.id]);

            results.sms_sent++;
          } catch (e) {
            console.error(`Broadcast SMS error for customer ${cust.id}:`, e.message);
            results.sms_errors++;
          }
        }
      }
    }

    // Update campaign stats if linked
    if (campaign_id && results.email_sent > 0) {
      await pool.query('UPDATE campaigns SET template_id = COALESCE(template_id, $1), send_count = COALESCE(send_count, 0) + $2 WHERE id = $3', [template_id, results.email_sent, campaign_id]);
    }

    res.json({ success: true, ...results });
  } catch (error) {
    console.error('Broadcast send error:', error);
    serverError(res, error);
  }
});


router.get('/api/email-log', async (req, res) => {
  try {
    const { type, search, days, limit = 100, offset = 0 } = req.query;
    let where = [];
    let params = [];
    let idx = 1;

    if (type && type !== 'all') {
      where.push(`email_type = $${idx++}`);
      params.push(type);
    }
    if (search) {
      where.push(`(recipient_email ILIKE $${idx} OR subject ILIKE $${idx} OR customer_name ILIKE $${idx})`);
      params.push(`%${search}%`);
      idx++;
    }
    if (days) {
      where.push(`sent_at >= NOW() - $${idx}::int * INTERVAL '1 day'`);
      params.push(parseInt(days));
      idx++;
    }

    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    const [countResult, result] = await Promise.all([
      pool.query(`SELECT COUNT(*) FROM email_log ${whereClause}`, params),
      pool.query(`SELECT * FROM email_log ${whereClause} ORDER BY sent_at DESC LIMIT $${idx} OFFSET $${idx + 1}`, [...params, parseInt(limit), parseInt(offset)])
    ]);

    res.json({ success: true, emails: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) {
    console.error('Email log error:', error);
    serverError(res, error);
  }
});

// Email log stats
router.get('/api/email-log/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours') AS last_24h,
        COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days') AS last_7d,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed,
        COUNT(DISTINCT recipient_email) AS unique_recipients
      FROM email_log
    `);
    const byType = await pool.query(`
      SELECT email_type, COUNT(*) AS count
      FROM email_log
      GROUP BY email_type
      ORDER BY count DESC
    `);
    res.json({ success: true, stats: stats.rows[0], by_type: byType.rows });
  } catch (error) {
    serverError(res, error);
  }
});


  return router;
};
