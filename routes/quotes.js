// ═══════════════════════════════════════════════════════════
// Quote & Contract Routes — extracted from server.js
// Handles: quote requests, sent quotes, signing, contracts,
//          quote events, PDF downloads, next-number
// ═══════════════════════════════════════════════════════════

const express = require('express');
const crypto = require('crypto');
const { validate, schemas } = require('../lib/validate');
const { clientCommunicationsDisabledResponse } = require('../lib/client-communications');

module.exports = function createQuoteRoutes({ pool, sendEmail, escapeHtml, serverError, authenticateToken, verifyRecaptcha, RECAPTCHA_SECRET_KEY, NOTIFICATION_EMAIL, LOGO_URL, FROM_EMAIL, COMPANY_NAME, SERVICE_DESCRIPTIONS, getServiceDescription, nextCustomerNumber, anthropicClient, ensureQuoteEventsTable: _ensureQuoteEventsTable, generateQuotePDF, emailTemplate }) {
  const router = express.Router();

  // ─── Quote Event Helpers ───────────────────────────────
  async function logQuoteEvent(quoteId, eventType, description, details = null) {
    try {
      await _ensureQuoteEventsTable();
      await pool.query(
        'INSERT INTO quote_events (sent_quote_id, event_type, description, details) VALUES ($1, $2, $3, $4)',
        [quoteId, eventType, description, details ? JSON.stringify(details) : null]
      );
    } catch (e) {
      console.error('Error logging quote event:', e);
    }
  }

  function normalizeQuoteQuestions(body, fullName, servicesArray) {
    const existing = body.questions && typeof body.questions === 'object' && !Array.isArray(body.questions)
      ? { ...body.questions }
      : {};
    const smsConsent = existing.smsConsent && typeof existing.smsConsent === 'object' && !Array.isArray(existing.smsConsent)
      ? { ...existing.smsConsent }
      : {};

    const copyIfPresent = (target, source) => {
      if (body[source] !== undefined && body[source] !== null && body[source] !== '') {
        existing[target] = body[source];
      }
    };

    copyIfPresent('city', 'city');
    copyIfPresent('propertyType', 'propertyType');
    copyIfPresent('gate', 'gate');
    copyIfPresent('dogs', 'dogs');
    copyIfPresent('lawnHeight', 'lawnHeight');
    copyIfPresent('contactMethod', 'contactMethod');
    copyIfPresent('startTime', 'startTime');
    copyIfPresent('backyardAccess', 'backyardAccess');

    if (body.package !== undefined && body.package !== null && body.package !== '') {
      existing.selectedPackage = body.package;
    }
    if (servicesArray) {
      existing.selectedServices = servicesArray;
    }
    if (body.notes !== undefined && body.notes !== null && body.notes !== '') {
      existing.notes = body.notes;
    }

    if (body.consentTransactional !== undefined) smsConsent.transactional = body.consentTransactional === true || body.consentTransactional === 'yes';
    if (body.consentMarketing !== undefined) smsConsent.marketing = body.consentMarketing === true || body.consentMarketing === 'yes';
    if (body.consentTerms !== undefined) smsConsent.termsAccepted = body.consentTerms === true || body.consentTerms === 'yes';
    if (Object.keys(smsConsent).length > 0) existing.smsConsent = smsConsent;

    if (fullName) existing.customerName = existing.customerName || fullName;

    return existing;
  }

  function formatBooleanAnswer(value) {
    if (value === true || value === 'true' || value === 'yes') return 'Yes';
    if (value === false || value === 'false' || value === 'no') return 'No';
    return '';
  }

  function buildQuoteQuestionEmailRows(questions) {
    const smsConsent = questions.smsConsent || questions.sms_consent || {};
    const rows = [
      ['City', questions.city],
      ['Property Type', questions.propertyType || questions.property_type],
      ['Gate', questions.gate],
      ['Dogs', questions.dogs],
      ['Lawn over 6 inches', questions.lawnHeight || questions.overgrown],
      ['Preferred Contact Method', questions.contactMethod || questions.contact_method],
      ['Start Timing', questions.startTime || questions.start_timing],
      ['Backyard Access', questions.backyardAccess || questions.backyard_access],
      ['Transactional SMS Consent', formatBooleanAnswer(smsConsent.transactional)],
      ['Marketing SMS Consent', formatBooleanAnswer(smsConsent.marketing)],
      ['Terms Accepted', formatBooleanAnswer(smsConsent.termsAccepted)],
    ];

    return rows
      .filter(([, value]) => value !== undefined && value !== null && value !== '')
      .map(([label, value]) => `<p><strong>${escapeHtml(label)}:</strong> ${escapeHtml(value)}</p>`)
      .join('');
  }

// ═══════════════════════════════════════════════════════════
// SENT QUOTES ENDPOINTS - For tracking quotes sent to customers
// ═══════════════════════════════════════════════════════════

// Helper to generate unique token for signing links
function generateToken() {
  return require('crypto').randomBytes(32).toString('hex');
}

// GET /api/sent-quotes - List all sent quotes
router.get('/api/sent-quotes', async (req, res) => {
  try {
    const { status, quote_type, search, limit = 50, offset = 0 } = req.query;
    let query = `
      SELECT sq.*, c.name as customer_name_lookup, c.email as customer_email_lookup
      FROM sent_quotes sq
      LEFT JOIN customers c ON sq.customer_id = c.id
      WHERE 1=1
    `;
    const values = [];
    let p = 1;

    if (status) {
      if (status === 'pending_signature') {
        query += ` AND sq.contract_signed_at IS NULL
                   AND sq.status IN ('sent', 'viewed')
                   AND COALESCE(sq.notes, '') ILIKE 'Auto-created from CopilotCRM estimate #%'
        `;
      } else {
        query += ` AND sq.status = $${p++}`;
        values.push(status);
      }
    }
    if (quote_type) {
      query += ` AND sq.quote_type = $${p++}`;
      values.push(quote_type);
    }
    if (search) {
      query += ` AND (sq.customer_name ILIKE $${p} OR sq.customer_email ILIKE $${p})`;
      values.push(`%${search}%`);
      p++;
    }

    query += ` ORDER BY sq.created_at DESC LIMIT $${p++} OFFSET $${p++}`;
    values.push(parseInt(limit), parseInt(offset));

    const [result, countsResult] = await Promise.all([
      pool.query(query, values),
      pool.query(`
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (WHERE status = 'draft')::int AS draft,
          COUNT(*) FILTER (WHERE status = 'sent')::int AS sent,
          COUNT(*) FILTER (WHERE status = 'viewed')::int AS viewed,
          COUNT(*) FILTER (WHERE status = 'signed')::int AS signed,
          COUNT(*) FILTER (WHERE status = 'contracted')::int AS contracted,
          COUNT(*) FILTER (WHERE status = 'declined')::int AS declined,
          COUNT(*) FILTER (WHERE status = 'changes_requested')::int AS changes_requested,
          COUNT(*) FILTER (
            WHERE contract_signed_at IS NULL
              AND status IN ('sent', 'viewed')
              AND COALESCE(notes, '') ILIKE 'Auto-created from CopilotCRM estimate #%'
          )::int AS pending_signatures
        FROM sent_quotes
      `)
    ]);
    const counts = countsResult.rows[0] || {
      total: 0,
      draft: 0,
      sent: 0,
      viewed: 0,
      signed: 0,
      contracted: 0,
      declined: 0,
      changes_requested: 0,
      pending_signatures: 0,
    };

    res.json({ success: true, quotes: result.rows, counts });
  } catch (error) {
    console.error('Error fetching sent quotes:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/view-counts - Bulk view counts for all sent quotes
// IMPORTANT: Must be registered BEFORE :id route
router.get('/api/sent-quotes/view-counts', async (req, res) => {
  try {
    // quote_views table created at startup
    const counts = await pool.query(
      'SELECT sent_quote_id, COUNT(*) as view_count, MAX(viewed_at) as last_viewed FROM quote_views GROUP BY sent_quote_id'
    );
    const map = {};
    counts.rows.forEach(r => { map[r.sent_quote_id] = { count: parseInt(r.view_count), lastViewed: r.last_viewed }; });
    res.json({ success: true, viewCounts: map });
  } catch (error) {
    console.error('Error fetching view counts:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/event-counts - Bulk event counts (resend/edit tracking)
// IMPORTANT: Must be registered BEFORE :id route
router.get('/api/sent-quotes/event-counts', async (req, res) => {
  try {
    await _ensureQuoteEventsTable();
    const result = await pool.query(
      `SELECT sent_quote_id,
        COUNT(*) FILTER (WHERE event_type = 'resent') as resend_count,
        COUNT(*) FILTER (WHERE event_type = 'edited') as edit_count,
        COUNT(*) FILTER (WHERE event_type IN ('sent', 'resent')) as total_sends
       FROM quote_events
       GROUP BY sent_quote_id`
    );
    const map = {};
    result.rows.forEach(r => {
      map[r.sent_quote_id] = {
        resend_count: parseInt(r.resend_count),
        edit_count: parseInt(r.edit_count),
        total_sends: parseInt(r.total_sends)
      };
    });
    res.json({ success: true, eventCounts: map });
  } catch (error) {
    console.error('Error fetching event counts:', error);
    res.json({ success: true, eventCounts: {} });
  }
});

// GET /api/sent-quotes/:id - Get single quote
router.get('/api/sent-quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error fetching quote:', error);
    serverError(res, error);
  }
});
// ═══════════════════════════════════════════════════════════
// QUOTES ENDPOINTS
// ═══════════════════════════════════════════════════════════

router.post('/api/quotes', async (req, res) => {
  try {
    const { name, firstName, lastName, email, phone, address, package: pkg, services, notes, source, recaptchaToken } = req.body;
    const quoteTestMode = process.env.QUOTE_TEST_MODE === 'true';
    const fullName = name || ((firstName || '') + ' ' + (lastName || '')).trim();
    let servicesArray = null;
    if (services) {
      if (Array.isArray(services)) servicesArray = services;
      else if (typeof services === 'string' && services.length > 0) servicesArray = services.split(',').map(s => s.trim());
    }
    const normalizedQuestions = normalizeQuoteQuestions(req.body, fullName, servicesArray);

    if (quoteTestMode) {
      if (!fullName || !email || !phone || !address) {
        return res.status(400).json({ success: false, error: 'Missing required fields' });
      }

      const testQuote = {
        id: `test_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`,
        name: fullName,
        email,
        phone,
        address,
        package: pkg || null,
        services: servicesArray,
        questions: normalizedQuestions,
        notes: notes || null,
        source: source || null,
        status: 'test',
        created_at: new Date().toISOString(),
      };

      console.log('[QUOTE_TEST_MODE] Test quote submission received. No database insert, email, or external integrations will run.', {
        id: testQuote.id,
        name: fullName,
        email,
        phone,
        address,
        package: pkg || null,
        services: servicesArray,
        source: source || null,
      });

      return res.json({ success: true, testMode: true, quote: testQuote });
    }
    
    // Verify reCAPTCHA if token provided
    if (recaptchaToken) {
      const recaptchaResult = await verifyRecaptcha(recaptchaToken);
      if (!recaptchaResult.success || recaptchaResult.score < 0.5) {
        console.log('reCAPTCHA failed - likely bot. Score:', recaptchaResult.score);
        return res.status(403).json({ success: false, error: 'Spam detection triggered. Please try again.' });
      }
      console.log('reCAPTCHA passed. Score:', recaptchaResult.score);
    } else if (RECAPTCHA_SECRET_KEY) {
      // If reCAPTCHA is configured but no token provided, reject
      console.log('No reCAPTCHA token provided');
      return res.status(400).json({ success: false, error: 'Security verification required' });
    }
    
    if (!fullName || !email || !phone || !address) {
      return res.status(400).json({ success: false, error: 'Missing required fields' });
    }
    const result = await pool.query(
      `INSERT INTO quotes (name, email, phone, address, package, services, questions, notes, source) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [fullName, email, phone, address, pkg || null, servicesArray, JSON.stringify(normalizedQuestions), notes || null, source || null]
    );
    res.json({ success: true, quote: result.rows[0] });
    
    // Send detailed notification email
    const servicesText = servicesArray ? servicesArray.join(', ') : 'None specified';
    const dashboardUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com') + '/quote-requests.html';
    const questionRows = buildQuoteQuestionEmailRows(normalizedQuestions);
    
    const emailHtml = `
      <h2>New Quote Request</h2>
      <p><strong>Name:</strong> ${escapeHtml(fullName)}</p>
      <p><strong>Email:</strong> <a href="mailto:${escapeHtml(email)}">${escapeHtml(email)}</a></p>
      <p><strong>Phone:</strong> ${escapeHtml(phone)}</p>
      <p><strong>Address:</strong> ${escapeHtml(address)}</p>
      <p><strong>Package:</strong> ${escapeHtml(pkg || 'None')}</p>
      <p><strong>Services:</strong> ${escapeHtml(servicesText)}</p>
      <p><strong>Notes:</strong> ${escapeHtml(notes || 'No notes provided')}</p>
      ${questionRows ? `<h3>Quote Form Answers</h3>${questionRows}` : ''}
      <br>
      <p><a href="${dashboardUrl}">View Dashboard</a></p>
    `;

    sendEmail(NOTIFICATION_EMAIL, `New Quote Request from ${escapeHtml(fullName)}`, emailHtml);
  } catch (error) {
    serverError(res, error);
  }
});

// Admin-created quote request (authenticated, no reCAPTCHA)
router.post('/api/quotes/admin', authenticateToken, async (req, res) => {
  try {
    const { name, firstName, lastName, email, phone, address, package: pkg, services, questions, notes, source } = req.body;
    const fullName = name || ((firstName || '') + ' ' + (lastName || '')).trim();
    if (!fullName || !phone) {
      return res.status(400).json({ success: false, error: 'Name and phone are required' });
    }
    let servicesArray = null;
    if (services) {
      if (Array.isArray(services)) servicesArray = services;
      else if (typeof services === 'string' && services.length > 0) servicesArray = services.split(',').map(s => s.trim());
    }
    const result = await pool.query(
      `INSERT INTO quotes (name, email, phone, address, package, services, questions, notes, source) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [fullName, email || null, phone, address || null, pkg || null, servicesArray, JSON.stringify(questions || {}), notes || null, source || 'phone_call']
    );
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    serverError(res, error);
  }
});

router.get('/api/quotes', async (req, res) => {
  try {
    const { status } = req.query;
    let query = 'SELECT * FROM quotes';
    const params = [];
    if (status) { query += ' WHERE status = $1'; params.push(status); }
    query += ' ORDER BY created_at DESC';
    const result = await pool.query(query, params);
    res.json({ success: true, quotes: result.rows });
  } catch (error) {
    serverError(res, error);
  }
});

router.get('/api/quotes/:id', async (req, res) => {
  try {
    if (!/^\d+$/.test(req.params.id)) return res.status(400).json({ success: false, error: 'Invalid quote ID' });
    const result = await pool.query('SELECT * FROM quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.patch('/api/quotes/:id', async (req, res) => {
  try {
    const { status } = req.body;
    const result = await pool.query('UPDATE quotes SET status = $1 WHERE id = $2 RETURNING *', [status, req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.delete('/api/quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM quotes WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// POST /api/sent-quotes - Create new quote
router.post('/api/sent-quotes', validate(schemas.createSentQuote), async (req, res) => {
  try {
    const {
      customer_name, customer_email, customer_phone, customer_address,
      quote_type, services, subtotal, tax_rate, tax_amount, total, monthly_payment, notes, quote_number
    } = req.body;

    // Look up or create customer
    let customer_id = null;
    if (customer_email) {
      const existingCustomer = await pool.query(
        'SELECT id FROM customers WHERE email = $1',
        [customer_email]
      );
      if (existingCustomer.rows.length > 0) {
        customer_id = existingCustomer.rows[0].id;
      } else {
        // Create new customer
        const newCustNum = await nextCustomerNumber();
        const newCustomer = await pool.query(
          `INSERT INTO customers (customer_number, name, email, phone, street, created_at)
           VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) RETURNING id`,
          [newCustNum, customer_name, customer_email, customer_phone, customer_address]
        );
        customer_id = newCustomer.rows[0].id;
        console.log('Created new customer:', customer_id);
        
        // Trigger Zapier webhook for CopilotCRM sync if configured
        if (process.env.ZAPIER_CUSTOMER_WEBHOOK) {
          try {
            await fetch(process.env.ZAPIER_CUSTOMER_WEBHOOK, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                customer_id,
                name: customer_name,
                email: customer_email,
                phone: customer_phone,
                address: customer_address,
                source: 'quote_generator'
              })
            });
          } catch (e) { console.error('Zapier webhook failed:', e); }
        }
      }
    }

    const sign_token = generateToken();

    const result = await pool.query(
      `INSERT INTO sent_quotes (
        customer_id, customer_name, customer_email, customer_phone, customer_address,
        quote_type, services, subtotal, tax_rate, tax_amount, total, monthly_payment,
        status, sign_token, notes, quote_number, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'draft', $13, $14, $15, CURRENT_TIMESTAMP)
      RETURNING *`,
      [
        customer_id, customer_name, customer_email, customer_phone, customer_address,
        quote_type || 'regular', JSON.stringify(services), subtotal, tax_rate || 8, tax_amount, total, monthly_payment,
        sign_token, notes, quote_number || null
      ]
    );

    // Log creation event
    await logQuoteEvent(result.rows[0].id, 'created', 'Quote created', {
      total: total,
      services_count: services ? services.length : 0
    });

    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error creating quote:', error);
    serverError(res, error);
  }
});

// PUT /api/sent-quotes/:id - Update quote
router.put('/api/sent-quotes/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = [];
    const values = [];
    let p = 1;

    const allowedFields = [
      'customer_name', 'customer_email', 'customer_phone', 'customer_address',
      'quote_type', 'services', 'subtotal', 'tax_rate', 'tax_amount', 'total',
      'monthly_payment', 'status', 'notes', 'quote_number'
    ];

    for (const field of allowedFields) {
      if (req.body[field] !== undefined) {
        if (field === 'services') {
          updates.push(`${field} = $${p++}`);
          values.push(JSON.stringify(req.body[field]));
        } else {
          updates.push(`${field} = $${p++}`);
          values.push(req.body[field]);
        }
      }
    }

    if (updates.length === 0) {
      return res.status(400).json({ success: false, error: 'No fields to update' });
    }

    values.push(id);
    const result = await pool.query(
      `UPDATE sent_quotes SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    // Log edit event with what changed
    const changedFields = allowedFields.filter(f => req.body[f] !== undefined);
    await logQuoteEvent(id, 'edited', 'Quote edited', {
      fields_changed: changedFields,
      new_total: result.rows[0].total
    });

    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error updating quote:', error);
    serverError(res, error);
  }
});

// POST /api/sent-quotes/:id/send - Send quote via email
router.post('/api/sent-quotes/:id/send', async (req, res) => {
  return clientCommunicationsDisabledResponse(res);
  try {
    const { id } = req.params;
    
    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [id]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = quoteResult.rows[0];
    
    if (!quote.customer_email) {
      return res.status(400).json({ success: false, error: 'No customer email address' });
    }

    const signUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-quote.html?token=${quote.sign_token}`;
    const quoteNumber = quote.quote_number || `Q-${quote.id}`;

    // Clean email - detailed but warm tone
    const firstName = (quote.customer_name || '').split(' ')[0] || 'there';
    
    const emailContent = `
      <div style="text-align:center;margin:0 0 28px;">
        <img src="${process.env.EMAIL_ASSETS_URL || process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/email-assets/heading-quote.png" alt="Your Quote is Ready" style="max-width:400px;width:auto;height:34px;" />
      </div>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Hi ${firstName},</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Thanks for reaching out to Pappas & Co. Landscaping! We've put together a custom quote for your property that includes the scope of work and pricing for your requested services.</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Click the button below to view your full quote:</p>

      <div style="text-align:center;margin:28px 0 20px;">
        <a href="${signUrl}" style="background:#c9dd80;color:#2e403d;padding:16px 52px;text-decoration:none;border-radius:50px;font-weight:700;font-size:15px;display:inline-block;letter-spacing:0.3px;">View Your Quote \u{2192}</a>
      </div>
      <p style="font-size:14px;color:#94a3b8;text-align:center;margin:0 0 24px;">Or just reply to this email with any questions</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 8px;font-weight:600;">From the quote page, you can:</p>

      <ul style="color:#4a5568;font-size:15px;line-height:1.8;padding-left:20px;margin:0 0 18px;">
        <li><strong>Accept the quote</strong> to secure your spot on our schedule and sign the service agreement</li>
        <li><strong>Request changes</strong> if you'd like us to adjust the scope of work</li>
      </ul>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">If you have any questions, feel free to call or text us at <strong>440-886-7318</strong>. We're always happy to help!</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0;">We look forward to working with you!</p>
    `;

    // Generate branded PDF attachment
    console.log('📄 Generating quote PDF for quote #' + quoteNumber + '...');
    const pdfResult = await generateQuotePDF(quote);
    let attachments = null;
    let pdfAttached = false;
    let pdfType = 'none';
    let pdfError = null;

    if (pdfResult && pdfResult.bytes) {
      const pdfSize = pdfResult.bytes.length;
      pdfType = pdfResult.type || 'unknown';
      pdfError = pdfResult.error || null;
      console.log('✅ Quote PDF generated (' + pdfType + '): ' + pdfSize + ' bytes (' + Math.round(pdfSize / 1024) + ' KB)');
      if (pdfError) console.log('⚠️ Main PDF error (fallback used): ' + pdfError);
      attachments = [{
        filename: 'Quote-' + quoteNumber + '-' + quote.customer_name.replace(/[^a-zA-Z0-9]/g, '-') + '.pdf',
        content: Buffer.from(pdfResult.bytes).toString('base64'),
        type: 'application/pdf'
      }];
      pdfAttached = true;
    } else {
      pdfError = pdfResult ? pdfResult.error : 'generateQuotePDF returned null';
      console.error('❌ Quote PDF generation failed:', pdfError);
    }

    await sendEmail(
      quote.customer_email,
      'Your ' + (quote.quote_type === 'monthly_plan' ? 'Annual Care Plan' : 'Quote') + ' from ' + COMPANY_NAME,
      emailTemplate(emailContent),
      attachments,
      { type: 'quote', customer_id: quote.customer_id, customer_name: quote.customer_name, quote_id: quote.id }
    );

    // Determine if this is first send or resend
    const isResend = quote.sent_at !== null;

    // Update status to sent
    await pool.query(
      'UPDATE sent_quotes SET status = $1, sent_at = CURRENT_TIMESTAMP WHERE id = $2',
      ['sent', id]
    );

    // Log send/resend event
    await logQuoteEvent(id, isResend ? 'resent' : 'sent',
      isResend ? 'Quote resent to ' + quote.customer_email : 'Quote sent to ' + quote.customer_email, {
      email: quote.customer_email,
      total: quote.total,
      pdf_attached: pdfAttached
    });

    res.json({ success: true, message: 'Quote sent successfully', pdfAttached, pdfType, pdfError, pdfSize: pdfResult && pdfResult.bytes ? pdfResult.bytes.length : 0 });
  } catch (error) {
    console.error('Error sending quote:', error);
    serverError(res, error);
  }
});

// POST /api/sent-quotes/:id/send-sms - Send quote via text message
router.post('/api/sent-quotes/:id/send-sms', authenticateToken, async (req, res) => {
  return clientCommunicationsDisabledResponse(res);
  try {
    const { id } = req.params;

    if (!twilioClient) {
      return res.status(400).json({ success: false, error: 'SMS is not configured' });
    }

    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [id]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = quoteResult.rows[0];

    // Get customer phone — from quote or customer record
    let phone = quote.customer_phone;
    if (!phone && quote.customer_id) {
      const custResult = await pool.query('SELECT phone, mobile FROM customers WHERE id = $1', [quote.customer_id]);
      if (custResult.rows.length > 0) {
        phone = custResult.rows[0].mobile || custResult.rows[0].phone;
      }
    }

    if (!phone) {
      return res.status(400).json({ success: false, error: 'No phone number on file for this customer' });
    }

    // Format phone number
    const cleaned = phone.replace(/\D/g, '');
    const toNumber = cleaned.length === 10 ? '+1' + cleaned : '+' + cleaned;

    const signUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-quote.html?token=${quote.sign_token}`;
    const firstName = (quote.customer_name || '').split(' ')[0] || '';
    const quoteNumber = quote.quote_number || `Q-${quote.id}`;

    const smsBody = `Hi ${firstName}! This is Tim from Pappas & Co. Landscaping. Thanks for giving us the opportunity to quote your service!\n\nYou can view and accept your pricing here: ${signUrl}\n\nTo secure your spot on our route, please click "Accept" on the quote. If you have any questions while reviewing it, feel free to text me back here.\n\nWe look forward to servicing your property!`;

    const twilioMessage = await twilioClient.messages.create({
      body: smsBody,
      from: TWILIO_PHONE_NUMBER,
      to: toNumber
    });

    // Log to messages table
    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
      VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
    `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, toNumber, smsBody, twilioMessage.status, quote.customer_id]);

    // Update quote status to sent if still draft
    if (quote.status === 'draft') {
      await pool.query('UPDATE sent_quotes SET status = $1, sent_at = CURRENT_TIMESTAMP WHERE id = $2', ['sent', id]);
    }

    // Log event
    await logQuoteEvent(id, 'sent_sms', 'Quote sent via text to ' + phone, { phone, quote_number: quoteNumber });

    console.log(`📱 Quote ${quoteNumber} sent via SMS to ${phone}`);
    res.json({ success: true, message: 'Quote sent via text!' });
  } catch (error) {
    console.error('Error sending quote SMS:', error);
    serverError(res, error);
  }
});

// DELETE /api/sent-quotes/:id - Delete quote
router.delete('/api/sent-quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM sent_quotes WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting quote:', error);
    serverError(res, error);
  }
});

// GET /api/sign/:token - Get quote for signing (public)
router.get('/api/sign/:token', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE sign_token = $1', [req.params.token]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = result.rows[0];

    // quote_views table created at startup

    // Log every view
    const ip = req.headers['x-forwarded-for'] || req.connection?.remoteAddress || '';
    const ua = req.headers['user-agent'] || '';
    await pool.query(
      'INSERT INTO quote_views (sent_quote_id, ip_address, user_agent) VALUES ($1, $2, $3)',
      [quote.id, ip.split(',')[0].trim(), ua]
    );

    // Mark as viewed if first time
    if (quote.status === 'sent' && !quote.viewed_at) {
      await pool.query(
        'UPDATE sent_quotes SET status = $1, viewed_at = CURRENT_TIMESTAMP WHERE id = $2',
        ['viewed', quote.id]
      );
      quote.status = 'viewed';
      await logQuoteEvent(quote.id, 'viewed', 'Quote viewed by customer');
    }

    // Enrich services with descriptions
    let services = typeof quote.services === 'string' ? JSON.parse(quote.services) : quote.services;
    if (Array.isArray(services)) {
      services = services.map(s => ({
        ...s,
        description: s.description || getServiceDescription(s.name)
      }));
      quote.services = services;
    }

    // Don't expose internal fields
    delete quote.sign_token;
    
    res.json({ success: true, quote });
  } catch (error) {
    console.error('Error fetching quote for signing:', error);
    serverError(res, error);
  }
});

// POST /api/sign/:token - Accept quote (public) - no signature needed, just name confirmation
router.post('/api/sign/:token', async (req, res) => {
  try {
    const { signed_by_name } = req.body;
    
    if (!signed_by_name) {
      return res.status(400).json({ success: false, error: 'Name confirmation required' });
    }

    const result = await pool.query(
      `UPDATE sent_quotes 
       SET status = 'signed', signed_by_name = $1, signed_at = CURRENT_TIMESTAMP
       WHERE sign_token = $2 AND status IN ('sent', 'viewed')
       RETURNING *`,
      [signed_by_name, req.params.token]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found or already signed' });
    }

    const quote = result.rows[0];
    const quoteNumber = quote.quote_number || `Q-${quote.id}`;

    // Notify Pappas team - CopilotCRM style
    const adminContent = `
      <h2 style="font-family:Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:28px;font-weight:400;text-align:center;">✅ Quote Accepted</h2>
      <p style="color:#64748b;margin:0 0 20px;text-align:center;">Customer will now sign the service agreement.</p>
      <div style="background:#f8fafc;border-radius:8px;padding:24px;">
        <p style="margin:0 0 12px;"><strong>Quote #:</strong> ${escapeHtml(quoteNumber)}</p>
        <p style="margin:0 0 12px;"><strong>Customer:</strong> ${escapeHtml(quote.customer_name)}</p>
        <p style="margin:0 0 12px;"><strong>Email:</strong> <a href="mailto:${escapeHtml(quote.customer_email)}" style="color:#2e403d;">${escapeHtml(quote.customer_email)}</a></p>
        <p style="margin:0 0 12px;"><strong>Phone:</strong> ${escapeHtml(quote.customer_phone)}</p>
        <p style="margin:0 0 12px;"><strong>Address:</strong> ${escapeHtml(quote.customer_address)}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0 0 12px;"><strong>Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
        ${quote.monthly_payment ? `<p style="margin:0 0 12px;"><strong>Monthly:</strong> $${parseFloat(quote.monthly_payment).toFixed(2)}/mo</p>` : ''}
        <p style="margin:0;"><strong>Accepted:</strong> ${new Date().toLocaleString()}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `✅ Quote #${escapeHtml(quoteNumber)} Accepted: ${escapeHtml(quote.customer_name)}`, emailTemplate(adminContent, { showSignature: false }));

    // Return success with contract URL for redirect
    const contractUrl = `/sign-contract.html?token=${req.params.token}`;
    res.json({ success: true, message: 'Quote accepted successfully', contractUrl });
  } catch (error) {
    console.error('Error signing quote:', error);
    serverError(res, error);
  }
});

// POST /api/sign/:token/decline - Decline quote (public)
router.post('/api/sign/:token/decline', async (req, res) => {
  try {
    const { decline_reason, decline_comments } = req.body;
    
    if (!decline_reason) {
      return res.status(400).json({ success: false, error: 'Reason required' });
    }

    const result = await pool.query(
      `UPDATE sent_quotes 
       SET status = 'declined', decline_reason = $1, decline_comments = $2, declined_at = CURRENT_TIMESTAMP
       WHERE sign_token = $3 AND status NOT IN ('signed', 'contracted', 'declined')
       RETURNING *`,
      [decline_reason, decline_comments || '', req.params.token]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found or already actioned' });
    }

    const quote = result.rows[0];
    const reasonLabels = {
      'price_too_high': 'Price is too high',
      'found_another': 'Found another provider',
      'not_needed': 'No longer need the service',
      'timing': 'Timing doesn\'t work',
      'selling_home': 'Selling the home',
      'diy': 'Going to do it myself',
      'budget': 'Budget constraints',
      'other': 'Other'
    };

    const adminContent = `
      <h2 style="color:#dc2626;margin:0 0 16px;">❌ Quote Declined</h2>
      <div style="background:#fef2f2;border-radius:8px;padding:20px;margin-bottom:20px;">
        <p style="margin:0 0 8px;"><strong>Reason:</strong> ${escapeHtml(reasonLabels[decline_reason] || decline_reason)}</p>
        ${decline_comments ? `<p style="margin:0;"><strong>Comments:</strong> ${escapeHtml(decline_comments)}</p>` : ''}
      </div>
      <div style="background:#f8fafc;border-radius:8px;padding:20px;">
        <p style="margin:0 0 8px;"><strong>Customer:</strong> ${escapeHtml(quote.customer_name)}</p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> ${escapeHtml(quote.customer_email)}</p>
        <p style="margin:0 0 8px;"><strong>Phone:</strong> ${escapeHtml(quote.customer_phone)}</p>
        <p style="margin:0 0 8px;"><strong>Address:</strong> ${escapeHtml(quote.customer_address)}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0;"><strong>Quote Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `❌ Quote Declined: ${escapeHtml(quote.customer_name)}`, emailTemplate(adminContent, { showSignature: false }));

    // Log decline event
    await logQuoteEvent(quote.id, 'declined', 'Quote declined by customer', {
      reason: decline_reason,
      comments: decline_comments || null
    });

    res.json({ success: true, message: 'Quote declined' });
  } catch (error) {
    console.error('Error declining quote:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/:id/views - Full view history for a quote
router.get('/api/sent-quotes/:id/views', async (req, res) => {
  try {
    // quote_views table created at startup
    const views = await pool.query(
      'SELECT id, viewed_at, ip_address, user_agent FROM quote_views WHERE sent_quote_id = $1 ORDER BY viewed_at DESC',
      [req.params.id]
    );
    res.json({ success: true, views: views.rows, total: views.rows.length });
  } catch (error) {
    console.error('Error fetching quote views:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/:id/events - Full event history for a quote
router.get('/api/sent-quotes/:id/events', async (req, res) => {
  try {
    await _ensureQuoteEventsTable();
    const result = await pool.query(
      'SELECT * FROM quote_events WHERE sent_quote_id = $1 ORDER BY created_at ASC',
      [req.params.id]
    );
    res.json({ success: true, events: result.rows });
  } catch (error) {
    console.error('Error fetching quote events:', error);
    res.json({ success: true, events: [] });
  }
});

// POST /api/sign/:token/request-changes - Request changes to quote (public)
router.post('/api/sign/:token/request-changes', async (req, res) => {
  try {
    const { change_type, change_details, change_request } = req.body;

    // Support both formats: {change_type, change_details} and {change_request}
    const type = change_type || 'general';
    const details = change_details || change_request;

    if (!details) {
      return res.status(400).json({ success: false, error: 'Please describe the changes you would like' });
    }

    const result = await pool.query(
      `UPDATE sent_quotes
       SET status = 'changes_requested', change_type = $1, change_details = $2, changes_requested_at = CURRENT_TIMESTAMP
       WHERE sign_token = $3 AND status NOT IN ('signed', 'contracted', 'declined')
       RETURNING *`,
      [type, details, req.params.token]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found or already actioned' });
    }

    const quote = result.rows[0];
    const typeLabels = {
      'add_services': 'Add more services',
      'remove_services': 'Remove some services',
      'pricing': 'Question about pricing',
      'schedule': 'Change schedule/frequency',
      'scope': 'Adjust scope of work',
      'other': 'Other'
    };

    const adminContent = `
      <h2 style="color:#f59e0b;margin:0 0 16px;">📝 Change Request</h2>
      <div style="background:#fffbeb;border-radius:8px;padding:20px;margin-bottom:20px;">
        <p style="margin:0 0 8px;"><strong>Type:</strong> ${escapeHtml(typeLabels[type] || type)}</p>
        <p style="margin:0;"><strong>Details:</strong></p>
        <p style="margin:8px 0 0;padding:12px;background:white;border-radius:6px;">${escapeHtml(details).replace(/\n/g, '<br>')}</p>
      </div>
      <div style="background:#f8fafc;border-radius:8px;padding:20px;">
        <p style="margin:0 0 8px;"><strong>Customer:</strong> ${escapeHtml(quote.customer_name)}</p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> <a href="mailto:${escapeHtml(quote.customer_email)}" style="color:#2e403d;">${escapeHtml(quote.customer_email)}</a></p>
        <p style="margin:0 0 8px;"><strong>Phone:</strong> <a href="tel:${escapeHtml(quote.customer_phone)}" style="color:#2e403d;">${escapeHtml(quote.customer_phone)}</a></p>
        <p style="margin:0;"><strong>Original Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
      </div>
      <p style="margin-top:20px;"><a href="${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sent-quotes.html" style="background:#f59e0b;color:white;padding:12px 24px;text-decoration:none;border-radius:6px;display:inline-block;font-weight:600;">Review Quote</a></p>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `📝 Change Request: ${escapeHtml(quote.customer_name)}`, emailTemplate(adminContent, { showSignature: false }));

    // Log changes requested event
    await logQuoteEvent(quote.id, 'changes_requested', 'Customer requested changes', {
      change_type: type,
      change_details: details
    });

    res.json({ success: true, message: 'Changes requested' });
  } catch (error) {
    console.error('Error requesting changes:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// CONTRACT SIGNING ENDPOINTS
// ═══════════════════════════════════════════════════════════

// POST /api/sent-quotes/:id/sign-contract - Sign the service agreement
router.post('/api/sent-quotes/:id/sign-contract', async (req, res) => {
  try {
    const { id } = req.params;
    const { signature_data, signature_type, printed_name, consent_given } = req.body;

    if (!signature_data || !printed_name || !consent_given) {
      return res.status(400).json({ success: false, error: 'Missing required fields' });
    }

    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [id]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = quoteResult.rows[0];
    if (quote.contract_signed_at) {
      return res.status(400).json({ success: false, error: 'Contract already signed' });
    }

    const signerIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';

    const updateResult = await pool.query(`
      UPDATE sent_quotes SET
        contract_signed_at = CURRENT_TIMESTAMP,
        contract_signature_data = $1,
        contract_signature_type = $2,
        contract_signer_ip = $3,
        contract_signer_name = $4,
        status = 'contracted'
      WHERE id = $5
      RETURNING *
    `, [signature_data, signature_type, signerIp, printed_name, id]);

    const updatedQuote = updateResult.rows[0];

    // Log contract signed event
    await logQuoteEvent(id, 'contracted', 'Contract signed by ' + printed_name, {
      signer_name: printed_name,
      signer_ip: signerIp,
      signature_type: signature_type
    });

    const signedDate = new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'America/New_York' });
    const signedTime = new Date().toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });

    let servicesText = 'See agreement for details';
    let servicesHtml = '';
    let services = [];
    try {
      services = typeof updatedQuote.services === 'string' ? JSON.parse(updatedQuote.services) : updatedQuote.services;
      if (Array.isArray(services)) {
        servicesText = services.map(s => s.name || s).join(', ');
        servicesHtml = services.map(s => `<li style="margin:6px 0;">${s.name} - $${parseFloat(s.amount).toFixed(2)}</li>`).join('');
      } else {
        services = [];
      }
    } catch (e) { services = []; }

    // Generate the contract HTML attachment (matches Canva template style)
    const quoteNumber = updatedQuote.quote_number || 'Q-' + updatedQuote.id;
    const isDrawnSignature = signature_data && signature_data.startsWith('data:image');
    const signatureHtml = isDrawnSignature 
      ? `<img src="${signature_data}" style="max-height:60px;margin:8px 0;" alt="Signature">`
      : `<p style="font-family:'Brush Script MT',cursive;font-size:28px;margin:8px 0;color:#2e403d;">${signature_data || printed_name}</p>`;

    const contractHtml = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Service Agreement - ${updatedQuote.customer_name}</title>
<style>
body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 40px; color: #333; font-size: 11px; line-height: 1.5; }
.header { display: flex; justify-content: space-between; align-items: center; padding-bottom: 16px; border-bottom: 4px solid #c9dd80; margin-bottom: 28px; }
.logo img { max-height: 56px; max-width: 180px; display: block; }
.contact-info { text-align: right; font-size: 10.5px; color: #666; line-height: 1.7; }
h1 { text-align: center; color: #2e403d; font-size: 22px; margin: 0 0 8px; font-weight: 700; letter-spacing: 0.5px; }
.intro { text-align: center; color: #666; font-size: 11px; margin-bottom: 20px; }
.parties { display: flex; gap: 40px; margin: 20px 0 30px; }
.party { flex: 1; }
.party-label { font-weight: bold; color: #333; margin-bottom: 8px; }
h2 { color: #2e403d; font-size: 13px; margin: 22px 0 10px; padding-bottom: 4px; border-bottom: 2px solid #c9dd80; }
.accent-bar { height: 4px; background: linear-gradient(90deg, #c9dd80, #bef264); margin: 0 0 24px; }
.section { margin-bottom: 16px; }
.section p { margin: 6px 0; text-align: justify; }
.section ul { margin: 8px 0 8px 20px; padding: 0; }
.section li { margin: 4px 0; }
.highlight { background: #f8fafc; border-left: 3px solid #84cc16; padding: 10px 12px; margin: 10px 0; font-size: 10px; }
.signature-section { margin-top: 40px; border: 2px solid #2e403d; border-radius: 8px; padding: 24px; background: #fafafa; }
.sig-row { display: flex; gap: 40px; margin-top: 20px; }
.sig-block { flex: 1; }
.sig-label { font-weight: bold; margin-bottom: 8px; }
.sig-line { border-bottom: 1px solid #333; height: 40px; margin-bottom: 4px; }
.footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; text-align: center; font-size: 10px; color: #666; }
@media print { body { padding: 20px; } }
</style>
</head>
<body>
<div class="header">
  <div class="logo"><img src="${LOGO_URL}" alt="Pappas &amp; Co. Landscaping"></div>
  <div class="contact-info">pappaslandscaping.com<br>hello@pappaslandscaping.com<br>(440) 886-7318</div>
</div>

<h1>Service Agreement</h1>
<p class="intro">This Agreement is made effective on the date the Client accepts a<br>quote from Pappas & Co. Landscaping (the "Effective Date") between:</p>

<div class="parties">
  <div class="party">
    <p class="party-label">Contractor:</p>
    <p>Pappas & Co. Landscaping<br>PO Box 770057<br>Lakewood, OH 44107</p>
  </div>
  <div class="party">
    <p class="party-label">Client:</p>
    <p><strong>${updatedQuote.customer_name}</strong><br>${updatedQuote.customer_address || ''}<br>${updatedQuote.customer_email || ''}<br>${updatedQuote.customer_phone || ''}</p>
  </div>
</div>

<h2>Services & Pricing (Quote #${quoteNumber})</h2>
<div class="section">
  <ul>${servicesHtml}</ul>
  <p><strong>Total: $${parseFloat(updatedQuote.total).toFixed(2)}</strong>${updatedQuote.monthly_payment ? ` (Monthly: $${parseFloat(updatedQuote.monthly_payment).toFixed(2)}/mo)` : ''}</p>
</div>

<h2>I. Scope of Agreement</h2>
<div class="section">
  <p><strong>A. Associated Quote:</strong> This Agreement is directly tied to Quote/Proposal Number: <strong>${quoteNumber}</strong>.</p>
  <p><strong>B. Scope of Services:</strong> The Contractor agrees to provide services at the Client Service Address as detailed in the Proposal, which outlines the specific services, schedule, and pricing. This Proposal is hereby incorporated into and made a part of this Agreement.</p>
  <p><strong>C. Additional Work:</strong> Additional work requested by the Client outside of the scope defined in the Proposal will be performed at an additional cost, requiring a separate, pre-approved quote.</p>
</div>

<h2>II. Terms and Renewal</h2>
<div class="section">
  <p><strong>A. Term:</strong> This Agreement begins on the Effective Date and remains in effect until canceled as outlined in Section IX.</p>
  <p><strong>B. Automatic Renewal:</strong> The Agreement automatically renews each year at the start of the new season, which begins in <strong>March</strong>, unless canceled in writing by either party at least <strong>30 days before the new season begins</strong>.</p>
</div>

<h2>III. Payment Terms</h2>
<div class="section">
  <p>A. Mowing Services Invoicing:</p>
  <ul>
    <li><strong>Per-Service Mowing:</strong> Invoices will be sent on the <strong>final day of each month</strong>.</li>
    <li><strong>Monthly Mowing Contracts:</strong> Invoices will be sent on the <strong>first day of each month</strong>.</li>
  </ul>
  <p><strong>B. All Other Services Invoicing:</strong> Invoices will be sent upon job completion.</p>
  <p><strong>C. Due Date:</strong> Payments are due upon receipt of the invoice.</p>
  <p><strong>D. Accepted Payment Methods:</strong> Major credit cards, Zelle, cash, checks, money orders, and bank transfers.</p>
  <p><strong>E. Fuel Surcharge:</strong> A small flat-rate fuel surcharge will be added to each invoice to help offset transportation-related costs.</p>
  <p><strong>F. Returned Checks:</strong> A $25 fee will be applied for any returned checks.</p>
</div>

<h2>IV. Card on File Authorization and Fees</h2>
<div class="section">
  <p>By placing a credit or debit card on file, the Client authorizes Pappas & Co. Landscaping to charge that card for any services rendered under this Agreement, including applicable fees and surcharges.</p>
  <p><strong>Processing Fee:</strong> A processing fee of <strong>2.9% + $0.30</strong> applies to each successful domestic card transaction.</p>
  <div class="highlight">
    <strong>For Monthly Service Contracts with card-on-file billing:</strong> If a scheduled payment fails, the Client will be notified and given 5 business days to update payment information. If payment is not resolved, the account will revert to per-service invoicing and standard late fee terms (Section V) will apply.
  </div>
</div>

<h2>V. Late Fees and Suspension of Service</h2>
<div class="section">
  <p>Pappas & Co. Landscaping incurs upfront costs for labor, materials, and equipment. Late payments disrupt business operations, and the following fees and policies apply:</p>
  <ul>
    <li><strong>30-Day Late Fee:</strong> A <strong>10% late fee</strong> will be applied if payment is not received within 30 days of the invoice date.</li>
    <li><strong>Recurring Late Fee:</strong> An additional <strong>5% late fee</strong> will be applied for each additional 30-day period past due.</li>
    <li><strong>Service Suspension and Collections:</strong> If payment is <strong>not received within 60 days</strong>, services will be <strong>suspended</strong>, and Pappas & Co. Landscaping reserves the right to initiate collection proceedings.</li>
  </ul>
</div>

<h2>VI. Client Responsibilities</h2>
<div class="section">
  <p>The Client agrees to the following:</p>
  <ul>
    <li><strong>Accessibility:</strong> All gates must be unlocked, and service areas must be accessible on the scheduled service day.</li>
    <li><strong>Return Trip Fee:</strong> A <strong>$25 return trip fee</strong> may be charged if rescheduling is needed due to Client-related access issues.</li>
    <li><strong>Property Clearance:</strong> The property must be free of hazards, obstacles, and pre-existing damage.</li>
    <li><strong>Personal Items:</strong> Our crew may move personal items if necessary to perform work, but <strong>we are not responsible for any damage caused by moving such items</strong>.</li>
    <li><strong>Pet Waste:</strong> All dog feces must be picked up prior to service. A <strong>$15 cleanup fee</strong> may be added if pet waste is present.</li>
    <li><strong>Underground Infrastructure:</strong> Pappas & Co. Landscaping is not liable for damage to underground utilities, irrigation lines, or invisible fences <strong>unless they are clearly marked and disclosed in advance</strong> by the Client.</li>
  </ul>
</div>

<h2>VII. Lawn/Plant Installs (If Applicable)</h2>
<div class="section">
  <p>The Client is responsible for watering newly installed lawns and plants <strong>twice daily or as recommended</strong> to ensure proper growth. Pappas & Co. Landscaping is <strong>not responsible</strong> for plant or lawn failure due to lack of watering or improper care after installation.</p>
</div>

<h2>VIII. Weather and Materials</h2>
<div class="section">
  <p><strong>A. Materials and Equipment:</strong> Pappas & Co. Landscaping will supply all materials, tools, and equipment necessary to perform the agreed-upon services unless specified otherwise.</p>
  <p><strong>B. Weather Disruptions:</strong> If inclement weather prevents services, Pappas & Co. Landscaping will make <strong>reasonable efforts</strong> to complete the service the following business day. Service on the next day is <strong>not guaranteed</strong> and will be rescheduled based on availability. Refunds or credits will not be issued for weather-related delays unless the service is permanently canceled.</p>
</div>

<h2>IX. Cancellation and Termination</h2>
<div class="section">
  <p><strong>A. Non-Renewal:</strong> To stop the automatic renewal of this Agreement, the Client must provide <strong>written notice at least 30 days before your renewal date</strong> (which occurs in March).</p>
  <p><strong>B. Mid-Season Cancellation by Client:</strong> To cancel service mid-season, the Client must provide <strong>15 days' written notice</strong> at any time. Services will continue through the notice period, and the final invoice will include any completed work. No refunds are given for prepaid services or unused portions of seasonal contracts.</p>
  <p><strong>C. Termination by Contractor:</strong> Pappas & Co. Landscaping may cancel service at any time with <strong>15 days' notice</strong>.</p>
</div>

<h2>X. Liability, Insurance, and Quality</h2>
<div class="section">
  <p><strong>A. Quality of Workmanship:</strong> Pappas & Co. Landscaping will perform all services with due care and in accordance with industry standards.</p>
  <ul>
    <li>If defects or deficiencies in workmanship occur, the Client must notify Pappas & Co. Landscaping <strong>within 7 days</strong> of service completion. If the issue is due to improper workmanship, it will be corrected at no additional cost.</li>
    <li>Issues resulting from <strong>natural wear, environmental conditions, or improper client maintenance</strong> are not covered under this clause.</li>
  </ul>
  <p><strong>B. Independent Contractor:</strong> Pappas & Co. Landscaping is an independent contractor and is not an employee, partner, or agent of the Client.</p>
  <p><strong>C. Indemnification:</strong> Pappas & Co. Landscaping agrees to indemnify and hold harmless the Client from claims arising directly from its performance of work.</p>
  <p><strong>D. Limitation of Liability:</strong> The total liability of Pappas & Co. Landscaping for any claim shall <strong>not exceed the total amount paid by the Client</strong> under this agreement. Pappas & Co. Landscaping is <strong>not liable</strong> for indirect, incidental, consequential, or special damages.</p>
  <p><strong>E. Insurance:</strong> Pappas & Co. Landscaping carries general liability insurance, automobile liability insurance, and workers' compensation insurance as required by law.</p>
  <p><strong>F. Force Majeure:</strong> Neither party shall be held liable for delays or failure in performance caused by events beyond their reasonable control.</p>
</div>

<h2>XI. Governing Law and Dispute Resolution</h2>
<div class="section">
  <p><strong>A. Jurisdiction:</strong> This agreement shall be governed by the laws of the <strong>State of Ohio</strong>. Any disputes shall be resolved in the county courts of <strong>Cuyahoga County, Ohio</strong>.</p>
  <p><strong>B. Dispute Resolution:</strong> Any disputes will first be subject to <strong>good-faith negotiations</strong> between the parties. If a resolution cannot be reached, the dispute may be subject to <strong>mediation or arbitration</strong> before legal action is pursued.</p>
</div>

<h2>XII. Acceptance of Agreement</h2>
<div class="section">
  <p>By signing below, the parties acknowledge that they have read, understand, and agree to the terms and conditions of this Landscaping Services Agreement and the incorporated Proposal/Quote.</p>
</div>

<div class="signature-section">
  <div class="sig-row">
    <div class="sig-block">
      <p class="sig-label">Pappas & Co. Landscaping:</p>
      <p style="font-family:'Brush Script MT',cursive;font-size:24px;margin:8px 0;">Timothy Pappas</p>
      <div class="sig-line"></div>
      <p>Name: <strong>Timothy Pappas</strong></p>
    </div>
    <div class="sig-block">
      <p class="sig-label">Client:</p>
      ${signatureHtml}
      <div class="sig-line"></div>
      <p>Name: <strong>${printed_name}</strong></p>
      <p>Date: <strong>${signedDate}</strong></p>
    </div>
  </div>
</div>

<div class="footer">
  <div class="accent-bar" style="margin:15px 0;"></div>
</div>
</body>
</html>`;

    // Generate PDF from template
    console.log('📄 Attempting to generate contract PDF for quote', id);
    let pdfBytes = null;
    try {
      pdfBytes = await generateContractPDF(updatedQuote, signature_data, printed_name, signedDate);
      if (pdfBytes) {
        console.log('✅ Contract PDF generated successfully, size:', pdfBytes.length, 'bytes');
      } else {
        console.log('⚠️ generateContractPDF returned null');
      }
    } catch (pdfError) {
      console.error('❌ PDF generation threw an error:', pdfError.message);
      console.error('Stack:', pdfError.stack);
    }
    
    // Create attachment - use PDF if available, otherwise HTML
    let contractAttachment;
    if (pdfBytes && pdfBytes.length > 0) {
      contractAttachment = {
        filename: `Service-Agreement-${quoteNumber}.pdf`,
        content: Buffer.from(pdfBytes).toString('base64'),
        type: 'application/pdf'
      };
      console.log('📎 Using PDF attachment');
    } else {
      // Fallback to HTML if PDF generation fails
      console.log('⚠️ Falling back to HTML attachment');
      contractAttachment = {
        filename: `Service-Agreement-${quoteNumber}.html`,
        content: Buffer.from(contractHtml).toString('base64'),
        type: 'text/html'
      };
    }

    // Backend client communications are disabled; CopilotCRM remains the customer-facing system.
    if (false && updatedQuote.customer_email) {
      const firstName = (updatedQuote.customer_name || '').split(' ')[0] || 'there';
      const customerContent = `
        <div style="text-align:center;margin:0 0 28px;">
          <img src="${process.env.EMAIL_ASSETS_URL || process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/email-assets/heading-welcome.png" alt="Welcome to the Pappas Family" style="max-width:400px;width:auto;height:34px;" />
        </div>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Hi ${firstName},</p>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Thank you for signing your service agreement! We're excited to have you as a customer.</p>

        <p style="background:#f0f2eb;padding:16px;border-radius:8px;color:#2e403d;font-size:14px;margin:0 0 24px;">Your signed service agreement is attached to this email.</p>

        <div style="background:#f8faf6;border-radius:12px;padding:24px;margin:0 0 24px;">
          <p style="margin:0 0 16px;color:#2e403d;font-size:16px;font-weight:700;border-bottom:2px solid #c9dd80;padding-bottom:12px;">Agreement Details</p>
          <p style="margin:0 0 6px;"><span style="color:#64748b;font-size:13px;">Quote Number</span><br><span style="color:#1e293b;font-size:15px;font-weight:600;">#${quoteNumber}</span></p>
          <p style="margin:12px 0 6px;"><span style="color:#64748b;font-size:13px;">Service Address</span><br><span style="color:#1e293b;font-size:15px;">${(() => { const al = formatAddressLines(updatedQuote.customer_address); return al.line2 ? al.line1 + '<br>' + al.line2 : al.line1; })()}</span></p>
          <p style="margin:12px 0 6px;border-top:1px solid #e2e8f0;padding-top:12px;"><span style="color:#64748b;font-size:13px;">Services</span><br><span style="color:#1e293b;font-size:15px;">${servicesText}</span></p>
          <table style="width:100%;margin-top:16px;border-top:2px solid #c9dd80;border-collapse:collapse;">
            <tr><td style="padding:12px 0;color:#64748b;font-size:14px;">Total</td><td style="padding:12px 0;color:#2e403d;font-size:22px;text-align:right;font-weight:700;">$${parseFloat(updatedQuote.total).toFixed(2)}</td></tr>
            ${updatedQuote.monthly_payment ? `<tr><td style="padding:4px 0;color:#64748b;font-size:14px;">Monthly Payment</td><td style="padding:4px 0;color:#2e403d;font-size:16px;text-align:right;font-weight:600;">$${parseFloat(updatedQuote.monthly_payment).toFixed(2)}/mo</td></tr>` : ''}
          </table>
        </div>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;"><strong>What's next?</strong> If you haven't already, please add a payment method in your customer portal to complete your setup. Once that's done, we'll add you to the schedule!</p>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0;">We can't wait to get started!</p>
      `;
      await sendEmail(updatedQuote.customer_email, `You're All Set! Welcome to Pappas & Co. Landscaping`, emailTemplate(customerContent), [contractAttachment], { type: 'welcome', customer_id: updatedQuote.customer_id, customer_name: updatedQuote.customer_name, quote_id: updatedQuote.id });
    }
    console.log(`Client welcome email skipped for signed contract ${quoteNumber}; backend client communications are disabled.`);

    // Email to admin - matches Quote Accepted style
    const adminContent = `
      <h2 style="font-family:Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:28px;font-weight:400;text-align:center;">🎉 Contract Signed</h2>
      <p style="color:#64748b;margin:0 0 20px;text-align:center;">Ready to schedule services.</p>
      <div style="background:#f8fafc;border-radius:8px;padding:24px;">
        <p style="margin:0 0 12px;"><strong>Quote #:</strong> ${quoteNumber}</p>
        <p style="margin:0 0 12px;"><strong>Customer:</strong> ${updatedQuote.customer_name}</p>
        <p style="margin:0 0 12px;"><strong>Email:</strong> <a href="mailto:${updatedQuote.customer_email}" style="color:#2e403d;">${updatedQuote.customer_email}</a></p>
        <p style="margin:0 0 12px;"><strong>Phone:</strong> ${updatedQuote.customer_phone}</p>
        <p style="margin:0 0 12px;"><strong>Address:</strong> ${updatedQuote.customer_address}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0 0 12px;"><strong>Services:</strong> ${servicesText}</p>
        <p style="margin:0 0 12px;"><strong>Total:</strong> $${parseFloat(updatedQuote.total).toFixed(2)}</p>
        ${updatedQuote.monthly_payment ? `<p style="margin:0 0 12px;"><strong>Monthly:</strong> $${parseFloat(updatedQuote.monthly_payment).toFixed(2)}/mo</p>` : ''}
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0 0 12px;"><strong>Signed by:</strong> ${printed_name}</p>
        <p style="margin:0;"><strong>Signed:</strong> ${signedDate} at ${signedTime}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `🎉 Contract Signed: ${updatedQuote.customer_name}`, emailTemplate(adminContent, { showSignature: false }));

    // Sync to CopilotCRM — update estimate status to accepted + upload signed contract
    if (process.env.COPILOTCRM_USERNAME && process.env.COPILOTCRM_PASSWORD) {
      try {
        console.log(`🔄 CopilotCRM sync starting for "${updatedQuote.customer_name}" (quote ${updatedQuote.quote_number || id})`);
        // Step 1: Login to CopilotCRM
        const copilotLogin = await fetch('https://api.copilotcrm.com/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
          body: JSON.stringify({ username: process.env.COPILOTCRM_USERNAME, password: process.env.COPILOTCRM_PASSWORD })
        });
        const copilotLoginText = await copilotLogin.text();
        let copilotAuth;
        try { copilotAuth = JSON.parse(copilotLoginText); } catch (e) { throw new Error(`CopilotCRM login returned non-JSON: ${copilotLoginText.substring(0, 200)}`); }
        console.log(`🔑 CopilotCRM login status: ${copilotLogin.status}, hasToken: ${!!copilotAuth.accessToken}`);
        if (!copilotAuth.accessToken) throw new Error(`CopilotCRM login failed: ${copilotLoginText.substring(0, 200)}`);
        const copilotCookie = `copilotApiAccessToken=${copilotAuth.accessToken}`;
        const copilotHeaders = {
          'Cookie': copilotCookie,
          'Origin': 'https://secure.copilotcrm.com',
          'Referer': 'https://secure.copilotcrm.com/',
          'X-Requested-With': 'XMLHttpRequest'
        };

        // Step 2: Search for customer by name
        const customerName = updatedQuote.customer_name || '';
        const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
          method: 'POST',
          headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
          body: `query=${encodeURIComponent(customerName)}`
        });
        const searchText = await searchRes.text();
        let customers;
        try { customers = JSON.parse(searchText); } catch (e) { throw new Error(`CopilotCRM customer search returned non-JSON (status ${searchRes.status}): ${searchText.substring(0, 300)}`); }
        console.log(`🔍 CopilotCRM: Customer search for "${customerName}" returned ${Array.isArray(customers) ? customers.length : 'non-array'} results`);

        // Find matching customer
        const match = customers.find(c => c.id && String(c.id) !== '0');
        if (!match) {
          console.log(`⚠️ CopilotCRM: No customer found for "${customerName}". Search results:`, JSON.stringify(customers).substring(0, 500));
        } else {
          const copilotCustomerId = match.id;
          console.log(`🔍 CopilotCRM: Found customer ${copilotCustomerId} for "${customerName}"`);

          // Step 3: Get customer's estimates to find matching estimate number
          const estRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
            method: 'POST',
            headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `customer_id=${copilotCustomerId}`
          });
          const estText = await estRes.text();
          let estData;
          try { estData = JSON.parse(estText); } catch (e) { throw new Error(`CopilotCRM estimates returned non-JSON (status ${estRes.status}): ${estText.substring(0, 300)}`); }
          const estHtml = estData.html || '';
          console.log(`📋 CopilotCRM: Estimates response for customer ${copilotCustomerId}: status=${estRes.status}, htmlLength=${estHtml.length}`);

          // Parse estimate IDs and numbers from HTML
          const quoteNum = updatedQuote.quote_number || '';
          const paddedNum = quoteNum.replace(/^0+/, '').padStart(7, '0');

          const estimateRegex = /<tr\s+id="(\d+)"[\s\S]*?<a\s+href="\/finances\/estimates\/view\/\d+">\s*(\d+)\s*<\/a>/g;
          let estMatch;
          let copilotEstimateId = null;
          const allEstNums = [];
          while ((estMatch = estimateRegex.exec(estHtml)) !== null) {
            allEstNums.push({ id: estMatch[1], num: estMatch[2] });
            if (estMatch[2] === paddedNum || estMatch[2] === quoteNum) {
              copilotEstimateId = estMatch[1];
              break;
            }
          }
          console.log(`📋 CopilotCRM: Found estimates: ${JSON.stringify(allEstNums)}. Looking for quoteNum="${quoteNum}" / padded="${paddedNum}"`);

          if (!copilotEstimateId) {
            console.log(`⚠️ CopilotCRM: No estimate matching "${quoteNum}" (padded: ${paddedNum}) found for customer ${copilotCustomerId}`);
          } else {
            console.log(`🔍 CopilotCRM: Found estimate ${copilotEstimateId} for quote ${quoteNum}`);

            // Step 4: Accept the estimate
            const acceptRes = await fetch('https://secure.copilotcrm.com/finances/estimates/accept', {
              method: 'POST',
              headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
              body: `id=${copilotEstimateId}&key=`
            });
            if (acceptRes.ok) {
              console.log(`✅ CopilotCRM: Estimate ${copilotEstimateId} marked as accepted`);
            } else {
              console.error(`CopilotCRM: Accept failed with status ${acceptRes.status}`);
            }

            // Step 5: Upload signed contract PDF if available
            if (pdfBytes && pdfBytes.length > 0) {
              try {
                // Get signed upload URL from CopilotCRM (S3)
                const signUrlRes = await fetch('https://secure.copilotcrm.com/getSignedUploadUrl', {
                  method: 'POST',
                  headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
                  body: JSON.stringify({ contentType: 'application/pdf', size: pdfBytes.length })
                });
                const signUrlData = await signUrlRes.json();

                if (signUrlData.data && signUrlData.data.uploadUrl) {
                  // Upload PDF to S3
                  await fetch(signUrlData.data.uploadUrl, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/pdf' },
                    body: Buffer.from(pdfBytes)
                  });

                  // Link uploaded file to the estimate
                  const uploadRes = await fetch('https://secure.copilotcrm.com/finances/estimates/uploadImage', {
                    method: 'POST',
                    headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      estimateId: String(copilotEstimateId),
                      tempFileName: signUrlData.data.key,
                      contentType: 'application/pdf'
                    })
                  });
                  if (uploadRes.ok) {
                    console.log(`✅ CopilotCRM: Signed contract uploaded to estimate ${copilotEstimateId}`);
                  }
                }
              } catch (uploadErr) {
                console.error('CopilotCRM: Contract upload failed:', uploadErr.message);
              }
            }

            // Step 6: Customer portal invites must be sent from CopilotCRM, not YardDesk.
            console.log('CopilotCRM portal invite skipped; backend client communications are disabled.');
            if (false) try {
              const portalUrl = 'https://secure.copilotcrm.com/client/forget?co=5261';
              const customerFirstName = (updatedQuote.customer_name || '').split(' ')[0] || 'there';
              const portalEmailContent = `
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Hi ${customerFirstName},</p>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Welcome to Pappas & Co. Landscaping! Your service agreement has been signed and we're excited to get started.</p>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">To keep things running smoothly, <strong>please add a card on file</strong> to your client portal. Your card will only be charged when invoices are due — no surprise charges.</p>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Your portal also gives you access to:</p>
                <table width="100%" cellpadding="0" cellspacing="0" style="margin:16px 0;">
                  <tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">💳</td><td><strong style="color:#1e293b;">Card on File</strong><br><span style="color:#64748b;font-size:13px;">Add a payment method for seamless billing</span></td></tr></table></td></tr>
                  <tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">📄</td><td><strong style="color:#1e293b;">Quotes & Invoices</strong><br><span style="color:#64748b;font-size:13px;">View and pay invoices online anytime</span></td></tr></table></td></tr>
                  <tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">📅</td><td><strong style="color:#1e293b;">Service Schedule</strong><br><span style="color:#64748b;font-size:13px;">View upcoming visits and service history</span></td></tr></table></td></tr>
                  <tr><td style="padding:12px 0;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">💬</td><td><strong style="color:#1e293b;">Direct Messaging</strong><br><span style="color:#64748b;font-size:13px;">Send questions or requests to our team</span></td></tr></table></td></tr>
                </table>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Click below to create your password and add your card.</p>
                <div style="text-align:center;margin:32px 0;">
                  <a href="${portalUrl}" style="display:inline-block;padding:14px 40px;background:#2e403d;color:#ffffff;text-decoration:none;border-radius:8px;font-size:16px;font-weight:600;">Set Up My Portal</a>
                </div>
              `;
              const portalEmailHtml = emailTemplate(portalEmailContent);
              const sendMailBody = new URLSearchParams({
                co_id: '5261',
                'to_customer[]': String(copilotCustomerId),
                type: 'email',
                subject: 'Get Started: Complete Your Client Portal Registration',
                content: portalEmailHtml
              });
              const sendMailRes = await fetch('https://secure.copilotcrm.com/emails/sendMail', {
                method: 'POST',
                headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
                body: sendMailBody.toString()
              });
              if (sendMailRes.ok) {
                console.log(`✅ CopilotCRM: Portal invite sent to customer ${copilotCustomerId}`);
              } else {
                console.error(`CopilotCRM: Portal invite failed with status ${sendMailRes.status}`);
              }
            } catch (portalErr) {
              console.error('CopilotCRM: Portal invite failed:', portalErr.message);
            }
          }
        }
      } catch (copilotErr) {
        console.error('❌ CopilotCRM sync failed:', copilotErr.message);
        console.error('CopilotCRM stack:', copilotErr.stack);
      }
    } else {
      console.log('⚠️ CopilotCRM sync skipped — COPILOTCRM_USERNAME or COPILOTCRM_PASSWORD not set');
    }

    // Stop quote follow-up sequence since quote was accepted
    try {
      const quoteNum = updatedQuote.quote_number || 'Q-' + updatedQuote.id;
      await pool.query(`
        UPDATE quote_followups 
        SET status = 'accepted', stopped_at = NOW(), stopped_reason = 'accepted', stopped_by = 'contract_signed', updated_at = NOW()
        WHERE (quote_number = $1 OR customer_email = $2) AND status = 'pending'
      `, [quoteNum, updatedQuote.customer_email]);
      console.log(`✅ Follow-up sequence stopped for accepted quote ${quoteNum}`);
    } catch (followupErr) {
      console.log('Follow-up stop skipped:', followupErr.message);
    }

    console.log(`📝 Contract signed for quote ${id} by ${printed_name}`);
    res.json({ success: true, quote: updatedQuote });

  } catch (error) {
    console.error('Error signing contract:', error);
    serverError(res, error);
  }
});

// POST /api/copilotcrm/backfill-contract - Manually trigger CopilotCRM sync for a signed quote
router.post('/api/copilotcrm/backfill-contract', authenticateToken, async (req, res) => {
  try {
    const { customer_name } = req.body;
    if (!customer_name) return res.status(400).json({ success: false, error: 'customer_name required' });
    if (!process.env.COPILOTCRM_USERNAME || !process.env.COPILOTCRM_PASSWORD) {
      return res.status(400).json({ success: false, error: 'CopilotCRM credentials not configured' });
    }

    // Find the signed quote
    const quoteResult = await pool.query(
      `SELECT * FROM sent_quotes WHERE LOWER(customer_name) = LOWER($1) AND contract_signed_at IS NOT NULL ORDER BY contract_signed_at DESC LIMIT 1`,
      [customer_name.trim()]
    );
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: `No signed contract found for "${customer_name}"` });
    }
    const quote = quoteResult.rows[0];
    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    console.log(`🔄 Backfilling CopilotCRM sync for ${customer_name}, quote ${quoteNumber}`);

    // Generate contract PDF
    let pdfBytes = null;
    try {
      const signedDate = new Date(quote.contract_signed_at).toLocaleDateString();
      pdfBytes = await generateContractPDF(quote, quote.contract_signature_data, quote.contract_signer_name, signedDate);
    } catch (pdfErr) {
      console.error('PDF generation failed:', pdfErr.message);
    }

    // Login to CopilotCRM
    const copilotLogin = await fetch('https://api.copilotcrm.com/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
      body: JSON.stringify({ username: process.env.COPILOTCRM_USERNAME, password: process.env.COPILOTCRM_PASSWORD })
    });
    const copilotAuth = await copilotLogin.json();
    if (!copilotAuth.accessToken) return res.status(500).json({ success: false, error: 'CopilotCRM login failed' });
    const copilotHeaders = {
      'Cookie': `copilotApiAccessToken=${copilotAuth.accessToken}`,
      'Origin': 'https://secure.copilotcrm.com',
      'Referer': 'https://secure.copilotcrm.com/',
      'X-Requested-With': 'XMLHttpRequest'
    };

    // Search for customer
    const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
      method: 'POST',
      headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `query=${encodeURIComponent(customer_name)}`
    });
    const customers = await searchRes.json();
    const match = customers.find(c => c.id && String(c.id) !== '0');
    if (!match) return res.status(404).json({ success: false, error: `No customer found in CopilotCRM for "${customer_name}"` });

    const copilotCustomerId = match.id;
    const log = [`Found customer ${copilotCustomerId}`];

    // Get estimates
    const estRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
      method: 'POST',
      headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `customer_id=${copilotCustomerId}`
    });
    const estData = await estRes.json();
    const estHtml = estData.html || '';

    const quoteNum = quote.quote_number || '';
    const paddedNum = quoteNum.replace(/^0+/, '').padStart(7, '0');
    const estimateRegex = /<tr\s+id="(\d+)"[\s\S]*?<a\s+href="\/finances\/estimates\/view\/\d+">\s*(\d+)\s*<\/a>/g;
    let estMatch;
    let copilotEstimateId = null;
    while ((estMatch = estimateRegex.exec(estHtml)) !== null) {
      if (estMatch[2] === paddedNum || estMatch[2] === quoteNum) {
        copilotEstimateId = estMatch[1];
        break;
      }
    }

    if (!copilotEstimateId) {
      return res.status(404).json({ success: false, error: `No estimate matching "${quoteNum}" found in CopilotCRM for customer ${copilotCustomerId}` });
    }
    log.push(`Found estimate ${copilotEstimateId} for quote ${quoteNum}`);

    // Accept estimate
    const acceptRes = await fetch('https://secure.copilotcrm.com/finances/estimates/accept', {
      method: 'POST',
      headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `id=${copilotEstimateId}&key=`
    });
    if (acceptRes.ok) {
      log.push(`Estimate ${copilotEstimateId} marked as accepted`);
    } else {
      log.push(`Accept failed: ${acceptRes.status}`);
    }

    // Upload PDF
    if (pdfBytes && pdfBytes.length > 0) {
      try {
        const signUrlRes = await fetch('https://secure.copilotcrm.com/getSignedUploadUrl', {
          method: 'POST',
          headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
          body: JSON.stringify({ contentType: 'application/pdf', size: pdfBytes.length })
        });
        const signUrlData = await signUrlRes.json();
        if (signUrlData.data && signUrlData.data.uploadUrl) {
          await fetch(signUrlData.data.uploadUrl, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/pdf' },
            body: Buffer.from(pdfBytes)
          });
          const uploadRes = await fetch('https://secure.copilotcrm.com/finances/estimates/uploadImage', {
            method: 'POST',
            headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
            body: JSON.stringify({ estimateId: String(copilotEstimateId), tempFileName: signUrlData.data.key, contentType: 'application/pdf' })
          });
          if (uploadRes.ok) log.push('Signed contract PDF uploaded');
        }
      } catch (uploadErr) {
        log.push(`PDF upload failed: ${uploadErr.message}`);
      }
    } else {
      log.push('No PDF available to upload');
    }

    console.log(`✅ CopilotCRM backfill complete for ${customer_name}:`, log.join(' → '));
    res.json({ success: true, log });
  } catch (error) {
    serverError(res, error, 'CopilotCRM backfill error');
  }
});

function getCopilotEstimateAcceptedWebhookSecret() {
  return process.env.COPILOTCRM_WEBHOOK_SECRET || process.env.COPILOT_WEBHOOK_SECRET || '';
}

function parseCopilotEstimateAcceptedTextPayload(text) {
  const parsed = {};
  if (!text) return parsed;

  for (const line of String(text).split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Z_][A-Z0-9_]*)\s*:\s*(.*?)\s*$/);
    if (!match) continue;
    const key = match[1].toLowerCase();
    parsed[key] = match[2].trim();
  }

  return parsed;
}

function getCopilotEstimateAcceptedPayload(req) {
  const body = req.body && typeof req.body === 'object' ? req.body : {};
  const rawText = [
    body.text,
    body.body,
    body.html,
    body.plain,
    body.raw,
    body.email_body,
    body.message
  ].filter(Boolean).join('\n');
  return { ...parseCopilotEstimateAcceptedTextPayload(rawText), ...body };
}

function htmlToCopilotEstimateText(html) {
  return String(html || '')
    .replace(/<\s*br\s*\/?>/gi, '\n')
    .replace(/<\s*\/(?:p|div|td|th|tr|li|h[1-6])\s*>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/gi, '"')
    .replace(/\r/g, '\n')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n[ \t]+/g, '\n')
    .trim();
}

function parseCopilotEstimateServicesFromText(html) {
  const lines = htmlToCopilotEstimateText(html)
    .split(/\n+/)
    .map(line => line.trim())
    .filter(Boolean);
  const parsedServices = [];

  for (let i = 0; i < lines.length; i += 1) {
    if (!/^WHAT'?S INCLUDED$/i.test(lines[i])) continue;

    for (let j = i - 1; j >= 0; j -= 1) {
      const candidate = lines[j];
      if (
        !candidate ||
        /^(Description|Calc|Cost\/Rate|Qty\/Hr|Visits|M\.Rate|BH|T\.BH|T\.B\.Cost|Margin %|Taxes %|Total|Visit)$/i.test(candidate) ||
        /^\$?[\d,]+(?:\.\d{2})?$/.test(candidate)
      ) {
        continue;
      }

      const numericAfterHeading = lines
        .slice(i + 1, i + 16)
        .map(line => line.match(/^\$?\s*([\d,]+(?:\.\d{2})?)$/))
        .filter(Boolean)
        .map(match => parseFloat(match[1].replace(/,/g, '')));
      const price = numericAfterHeading.find(value => value > 0) || 0;
      parsedServices.push({ name: candidate, price });
      break;
    }
  }

  return parsedServices;
}

function isLikelyCopilotCustomerName(value) {
  const name = String(value || '').trim();
  return Boolean(name) &&
    !/[=;{}<>]/.test(name) &&
    !/^\/\*/.test(name) &&
    !/\*\/$/.test(name) &&
    !/^window\./i.test(name) &&
    !/\b(function|const|let|var|return|false|true|null|undefined)\b/i.test(name) &&
    !/pappaslandscaping\.com|@/.test(name);
}

function parseCopilotAcceptedEstimateDetail(html, fallback = {}) {
  const lines = htmlToCopilotEstimateText(html)
    .split(/\n+/)
    .map(line => line.trim())
    .filter(Boolean);
  const text = lines.join('\n');
  const estimateNumber = fallback.estimate_number || text.match(/Estimate #\s*\n?\s*(\d+)/i)?.[1];
  const total = Number(
    (text.match(/Total Estimated Cost\s*\n?\s*([\d,]+\.\d{2})/i)?.[1] ||
      text.match(/Total\s*\n?\s*([\d,]+\.\d{2})/i)?.[1] ||
      fallback.estimate_amount ||
      '0').replace(/,/g, '')
  );
  const email = fallback.email || text.match(/Sent to Email:\s*([^\s]+)/i)?.[1];
  let customerName = fallback.customer_name || text.match(/Accepted by Customer:\s*([^\n]+)/i)?.[1];
  let address = fallback.address || '';

  const websiteIndex = lines.findIndex(line => /pappaslandscaping\.com/i.test(line));
  if (websiteIndex >= 0) {
    const addressParts = [];
    for (let i = websiteIndex + 2; i < lines.length; i += 1) {
      if (/^Estimate #$/i.test(lines[i]) || /^\d+$/.test(lines[i])) break;
      addressParts.push(lines[i]);
      if (addressParts.length >= 2) break;
    }
    if (!address && addressParts.length > 0) address = addressParts.join(', ');
  }

  if (!isLikelyCopilotCustomerName(customerName)) {
    customerName = email ? email.split('@')[0].replace(/[._-]+/g, ' ').trim() : '';
  }

  return {
    customer_name: customerName,
    email,
    address,
    estimate_number: estimateNumber,
    estimate_amount: total || fallback.estimate_amount,
    services: parseCopilotEstimateServicesFromText(html)
  };
}

function assertQuoteCronSecret(req, res) {
  const configuredSecret = process.env.CRON_SECRET || process.env.CRON_API_KEY || '';
  if (!configuredSecret) return true;

  const provided = req.get('x-cron-secret') || req.query.key || req.query.token || req.body?.key || req.body?.token || '';
  if (provided === configuredSecret) return true;

  res.status(401).json({ success: false, error: 'Invalid cron secret' });
  return false;
}

async function loginToCopilotCrmForEstimates() {
  if (!process.env.COPILOTCRM_USERNAME || !process.env.COPILOTCRM_PASSWORD) {
    throw new Error('CopilotCRM credentials not configured');
  }
  const copilotLogin = await fetch('https://api.copilotcrm.com/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
    body: JSON.stringify({ username: process.env.COPILOTCRM_USERNAME, password: process.env.COPILOTCRM_PASSWORD })
  });
  const copilotAuth = await copilotLogin.json();
  if (!copilotAuth.accessToken) throw new Error('CopilotCRM login failed');
  return {
    'Cookie': `copilotApiAccessToken=${copilotAuth.accessToken}`,
    'Origin': 'https://secure.copilotcrm.com',
    'Referer': 'https://secure.copilotcrm.com/',
    'X-Requested-With': 'XMLHttpRequest'
  };
}

async function findAcceptedCopilotEstimateByNumber(copilotHeaders, estimateNumber, { maxAgeDays = 30 } = {}) {
  const listRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
    method: 'POST',
    headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: encodeCopilotForm({
      postData: { estimate_status: [2] },
      pagination: '',
      sort: 'datedesc'
    })
  });
  const listData = await listRes.json().catch(() => null);
  const listHtml = listData?.html || '';
  const acceptedRows = parseRecentAcceptedCopilotEstimateRows(listHtml, maxAgeDays);
  const normalizedEstimateNumber = String(estimateNumber || '').replace(/^0+/, '');

  return acceptedRows.find(row => String(row.estimate_number || '').replace(/^0+/, '') === normalizedEstimateNumber) || null;
}

function encodeCopilotFormValue(params, key, value) {
  if (Array.isArray(value)) {
    for (const item of value) {
      encodeCopilotFormValue(params, `${key}[]`, item);
    }
    return;
  }
  if (value && typeof value === 'object') {
    for (const [childKey, childValue] of Object.entries(value)) {
      encodeCopilotFormValue(params, `${key}[${childKey}]`, childValue);
    }
    return;
  }
  params.append(key, value == null ? '' : String(value));
}

function encodeCopilotForm(data) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(data)) {
    encodeCopilotFormValue(params, key, value);
  }
  return params.toString();
}

function parseRecentAcceptedCopilotEstimateRows(html, maxAgeDays) {
  const cutoff = Date.now() - maxAgeDays * 24 * 60 * 60 * 1000;
  const rows = String(html || '').match(/<tr[\s\S]*?<\/tr>/gi) || [];
  const accepted = [];

  for (const row of rows) {
    if (!/\bAccepted\b/i.test(row)) continue;
    const linkMatch = row.match(/\/finances\/estimates\/view\/(\d+)[^>]*>\s*(\d+)\s*</i);
    if (!linkMatch) continue;
    const rowText = htmlToCopilotEstimateText(row);
    const dateMatch = rowText.match(/\b([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\b/);
    if (dateMatch) {
      const timestamp = Date.parse(dateMatch[1]);
      if (Number.isFinite(timestamp) && timestamp < cutoff) continue;
    }
    accepted.push({ copilot_id: linkMatch[1], estimate_number: linkMatch[2], date: dateMatch?.[1] || null });
  }

  return accepted;
}

function verifyCopilotEstimateAcceptedWebhook(req, res) {
  const expectedSecret = getCopilotEstimateAcceptedWebhookSecret();
  if (!expectedSecret) {
    res.status(503).json({ success: false, error: 'CopilotCRM estimate-accepted webhook secret is not configured' });
    return false;
  }

  const payload = getCopilotEstimateAcceptedPayload(req);
  const providedSecret =
    req.get('x-copilot-webhook-secret') ||
    req.get('x-webhook-secret') ||
    payload.webhook_secret ||
    payload.WEBHOOK_SECRET ||
    payload.webhookSecret ||
    req.query?.secret;

  if (!providedSecret) {
    res.status(401).json({ success: false, error: 'Webhook secret required' });
    return false;
  }

  const expected = Buffer.from(String(expectedSecret));
  const provided = Buffer.from(String(providedSecret));
  if (expected.length !== provided.length || !crypto.timingSafeEqual(expected, provided)) {
    res.status(403).json({ success: false, error: 'Invalid webhook secret' });
    return false;
  }

  return true;
}

function hasBearerToken(req) {
  return /^Bearer\s+\S+/i.test(req.get('authorization') || '');
}

function verifyEstimateAcceptedAdmin(req, res, next) {
  authenticateToken(req, res, () => {
    if (!req.user || !req.user.isAdmin || req.user.isEmployee) {
      return res.status(403).json({ success: false, error: 'Admin access required' });
    }
    return next();
  });
}

function authorizeEstimateAcceptedRequest(req, res, next) {
  if (hasBearerToken(req)) {
    return verifyEstimateAcceptedAdmin(req, res, next);
  }

  if (!verifyCopilotEstimateAcceptedWebhook(req, res)) return;
  return next();
}

// POST /api/webhooks/copilotcrm/estimate-accepted - Public webhook: CopilotCRM estimate accepted → send YardDesk contract
router.post('/api/webhooks/copilotcrm/estimate-accepted', async (req, res) => {
  if (!verifyCopilotEstimateAcceptedWebhook(req, res)) return;
  return handleCopilotEstimateAccepted(req, res);
});

// POST /api/copilotcrm/estimate-accepted - Admin/manual trigger: CopilotCRM estimate accepted → send YardDesk contract
// Minimal required: customer_name + estimate_number. Fetches email, services, and total from CopilotCRM.
// Optional overrides: email, phone, address, services, estimate_amount.
async function handleCopilotEstimateAccepted(req, res) {
  try {
    const payload = getCopilotEstimateAcceptedPayload(req);
    let {
      customer_name,
      CUSTOMER_NAME,
      phone,
      PHONE,
      address,
      PROPERTY_ADDRESS,
      email,
      CUSTOMER_EMAIL,
      estimate_number,
      ESTIMATE_NUMBER,
      estimate_amount,
      ESTIMATE_AMOUNT,
      services
    } = payload;
    customer_name = customer_name || CUSTOMER_NAME;
    phone = phone || PHONE;
    address = address || PROPERTY_ADDRESS;
    email = email || CUSTOMER_EMAIL;
    estimate_number = estimate_number || ESTIMATE_NUMBER;
    estimate_amount = estimate_amount || ESTIMATE_AMOUNT;

    if (!estimate_number) {
      return res.status(400).json({ success: false, error: 'Missing required field: estimate_number' });
    }

    if ((!customer_name || !email || !services?.length || !estimate_amount) && process.env.COPILOTCRM_USERNAME && process.env.COPILOTCRM_PASSWORD) {
      try {
        const copilotHeaders = await loginToCopilotCrmForEstimates();
        const acceptedEstimate = await findAcceptedCopilotEstimateByNumber(copilotHeaders, estimate_number);
        if (acceptedEstimate?.copilot_id) {
          const detailRes = await fetch(`https://secure.copilotcrm.com/finances/estimates/view/${acceptedEstimate.copilot_id}`, {
            method: 'GET',
            headers: copilotHeaders
          });
          const detailHtml = await detailRes.text();
          const detailPayload = parseCopilotAcceptedEstimateDetail(detailHtml, {
            customer_name,
            email,
            address,
            estimate_number,
            estimate_amount
          });
          customer_name = customer_name || detailPayload.customer_name;
          email = email || detailPayload.email;
          address = address || detailPayload.address;
          estimate_amount = estimate_amount || detailPayload.estimate_amount;
          services = Array.isArray(services) && services.length > 0 ? services : detailPayload.services;
        }
      } catch (lookupError) {
        console.error('CopilotCRM accepted estimate direct lookup failed:', lookupError.message);
      }
    }

    if (!customer_name) {
      return res.status(400).json({ success: false, error: 'Missing required field: customer_name' });
    }

    // Dedupe check — don't create duplicate contracts for the same estimate
    const existing = await pool.query(
      `SELECT * FROM sent_quotes WHERE quote_number = $1 AND status NOT IN ('declined') LIMIT 1`,
      [estimate_number]
    );
    if (existing.rows.length > 0) {
      const ex = existing.rows[0];
      const contractUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-contract.html?token=${ex.sign_token}`;
      if (payload.force_resend_contract === true || payload.force_resend_contract === 'true') {
        const resendEmail = email || ex.customer_email;
        const resendName = customer_name || ex.customer_name;
        const resendCustomerId = ex.customer_id || null;
        const serviceItems = Array.isArray(services) && services.length > 0
          ? services.map(s => ({ name: s.name, amount: s.price || s.amount, price: s.price || s.amount }))
          : ex.services;
        const resendTotal = estimate_amount ? Number(estimate_amount) : Number(ex.total || 0);

        if (!resendEmail) {
          return res.status(400).json({ success: false, error: 'No customer email available for existing contract resend.' });
        }

        await pool.query(
          `UPDATE sent_quotes
           SET customer_name = COALESCE($1, customer_name),
               customer_email = COALESCE($2, customer_email),
               customer_phone = COALESCE($3, customer_phone),
               customer_address = COALESCE($4, customer_address),
               services = COALESCE($5, services),
               subtotal = COALESCE($6, subtotal),
               total = COALESCE($6, total),
               sent_at = CURRENT_TIMESTAMP,
               updated_at = CURRENT_TIMESTAMP
           WHERE id = $7`,
          [
            resendName || null,
            resendEmail || null,
            phone || null,
            address || null,
            Array.isArray(serviceItems) ? JSON.stringify(serviceItems) : null,
            Number.isFinite(resendTotal) && resendTotal > 0 ? resendTotal : null,
            ex.id
          ]
        );

        const firstName = escapeHtml((resendName || '').split(' ')[0] || 'there');
        const emailContent = `
          <div style="text-align:center;margin:0 0 28px;">
            <h2 style="font-family:'Open Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#2e403d;font-size:24px;font-weight:600;margin:0;">Your Service Agreement is Ready</h2>
          </div>
          <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Hi ${firstName},</p>
          <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Thank you for accepting your estimate with Pappas & Co. Landscaping! Before we get started, please take a moment to review and sign your service agreement.</p>
          <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">This agreement covers the scope of work, terms, and pricing for your accepted estimate.</p>
          <div style="text-align:center;margin:28px 0 20px;">
            <a href="${contractUrl}" style="background:#c9dd80;color:#2e403d;padding:16px 52px;text-decoration:none;border-radius:50px;font-weight:700;font-size:15px;display:inline-block;letter-spacing:0.3px;">Review & Sign Agreement &#8594;</a>
          </div>
          <p style="font-size:14px;color:#94a3b8;text-align:center;margin:0 0 24px;">Or just reply to this email with any questions</p>
          <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">If you have any questions, feel free to call or text us at <strong>440-886-7318</strong>. We're always happy to help!</p>
          <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0;">We look forward to working with you!</p>
        `;

        await sendEmail(
          resendEmail,
          'Your Service Agreement from ' + COMPANY_NAME,
          emailTemplate(emailContent),
          null,
          { type: 'contract', customer_id: resendCustomerId, customer_name: resendName, quote_id: ex.id }
        );
        await logQuoteEvent(ex.id, 'resent', 'Existing CopilotCRM contract resent to ' + resendEmail, { email: resendEmail, source: 'copilotcrm', estimate_number });
        return res.json({ success: true, message: 'Existing contract resent to ' + resendEmail, quote_id: ex.id, contract_url: contractUrl });
      }
      return res.json({ success: true, message: 'Contract already exists for this estimate', quote_id: ex.id, contract_url: contractUrl });
    }

    // Fetch missing data from CopilotCRM. Copilot automations historically only sent
    // customer_name + estimate_number, so this lookup is required for compatibility.
    const needsCopilotLookup = !email || !services || services.length === 0 || !estimate_amount;
    if (needsCopilotLookup) {
      if (!process.env.COPILOTCRM_USERNAME || !process.env.COPILOTCRM_PASSWORD) {
        return res.status(400).json({ success: false, error: 'CopilotCRM credentials not configured. Provide email, services, and estimate_amount manually.' });
      }
      const copilotLogin = await fetch('https://api.copilotcrm.com/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
        body: JSON.stringify({ username: process.env.COPILOTCRM_USERNAME, password: process.env.COPILOTCRM_PASSWORD })
      });
      const copilotAuth = await copilotLogin.json();
      if (!copilotAuth.accessToken) {
        return res.status(500).json({ success: false, error: 'CopilotCRM login failed' });
      }
      const copilotHeaders = {
        'Cookie': `copilotApiAccessToken=${copilotAuth.accessToken}`,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest'
      };
      const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
        method: 'POST',
        headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `query=${encodeURIComponent(customer_name)}`
      });
      const crmCustomers = await searchRes.json().catch(() => null);
      const crmMatch = Array.isArray(crmCustomers) ? crmCustomers.find(c => c.id && String(c.id) !== '0') : null;
      if (!crmMatch) {
        return res.status(404).json({ success: false, error: `Customer "${customer_name}" not found in CopilotCRM` });
      }

      if (!email) email = crmMatch.email;
      if (!phone && crmMatch.phone) phone = crmMatch.phone;
      if (!address && crmMatch.address) address = crmMatch.address;
      console.log(`📧 CopilotCRM lookup: customer=${crmMatch.id}, email=${email || 'none'}, phone=${phone || 'n/a'}`);

      if (!email) {
        return res.status(404).json({ success: false, error: `No email found for "${customer_name}" in CopilotCRM` });
      }

      if (!services || services.length === 0 || !estimate_amount) {
        const estListRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
          method: 'POST',
          headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
          body: `customer_id=${crmMatch.id}`
        });
        const estListData = await estListRes.json().catch(() => null);
        const estHtml = (estListData && estListData.html) || '';

        const paddedNum = String(estimate_number).replace(/^0+/, '').padStart(7, '0');
        const estimateRegex = /<tr\s+id="(\d+)"[\s\S]*?<a\s+href="\/finances\/estimates\/view\/\d+">\s*(\d+)\s*<\/a>/g;
        let estMatch;
        let copilotEstimateId = null;
        while ((estMatch = estimateRegex.exec(estHtml)) !== null) {
          if (estMatch[2] === paddedNum || estMatch[2] === String(estimate_number)) {
            copilotEstimateId = estMatch[1];
            break;
          }
        }

        if (!copilotEstimateId) {
          return res.status(404).json({ success: false, error: `Estimate #${estimate_number} not found in CopilotCRM for "${customer_name}". Provide services and estimate_amount manually.` });
        }
        console.log(`📋 CopilotCRM: Found estimate ID ${copilotEstimateId} for #${estimate_number}`);

        const estDetailRes = await fetch(`https://secure.copilotcrm.com/finances/estimates/view/${copilotEstimateId}`, {
          method: 'GET',
          headers: copilotHeaders
        });
        const estDetailHtml = await estDetailRes.text();

        const parsedServices = [];
        let parsedTotal = 0;
        const lineItemRegex = /<tr[^>]*class="[^"]*item-row[^"]*"[^>]*>[\s\S]*?<\/tr>/gi;
        const lineItems = estDetailHtml.match(lineItemRegex) || [];

        for (const row of lineItems) {
          const nameMatch = row.match(/<td[^>]*class="[^"]*item-name[^"]*"[^>]*>([\s\S]*?)<\/td>/i)
            || row.match(/<td[^>]*>\s*<div[^>]*class="[^"]*name[^"]*"[^>]*>([\s\S]*?)<\/div>/i)
            || row.match(/<td[^>]*>\s*<span[^>]*class="[^"]*name[^"]*"[^>]*>([\s\S]*?)<\/span>/i);
          const amountMatches = row.match(/\$\s*([\d,]+\.?\d*)/g);
          const amount = amountMatches && amountMatches.length > 0
            ? parseFloat(amountMatches[amountMatches.length - 1].replace(/[$,]/g, ''))
            : 0;

          if (nameMatch) {
            const name = nameMatch[1].replace(/<[^>]+>/g, '').trim();
            if (name) parsedServices.push({ name, price: amount });
          }
        }

        if (parsedServices.length === 0) {
          const simpleTdRegex = /<td[^>]*>([\s\S]*?)<\/td>/gi;
          const allTds = [];
          let tdMatch;
          while ((tdMatch = simpleTdRegex.exec(estDetailHtml)) !== null) {
            allTds.push(tdMatch[1].replace(/<[^>]+>/g, '').trim());
          }
          console.log(`⚠️ CopilotCRM: item-row pattern found 0 items. Total tds in page: ${allTds.length}`);
          parsedServices.push(...parseCopilotEstimateServicesFromText(estDetailHtml));
        }

        const totalMatch = estDetailHtml.match(/(?:Total|Grand\s*Total)[^$]*\$\s*([\d,]+\.?\d*)/i)
          || estDetailHtml.match(/class="[^"]*total[^"]*"[^>]*>[^$]*\$\s*([\d,]+\.?\d*)/i);
        if (totalMatch) {
          parsedTotal = parseFloat(totalMatch[1].replace(/,/g, ''));
        }

        if ((!services || services.length === 0) && parsedServices.length > 0) {
          services = parsedServices;
          console.log(`📋 CopilotCRM: Parsed ${services.length} line items from estimate #${estimate_number}`);
        }
        if (!estimate_amount && parsedTotal > 0) {
          estimate_amount = parsedTotal;
          console.log(`💰 CopilotCRM: Parsed total $${estimate_amount} from estimate #${estimate_number}`);
        }
      }
    }

    if (!services || services.length === 0) {
      return res.status(422).json({
        success: false,
        error: `Could not parse line items from CopilotCRM estimate #${estimate_number}. Provide services manually.`,
        hint: 'The CopilotCRM estimate page HTML structure may have changed. Check server logs for details.'
      });
    }
    if (!estimate_amount) {
      estimate_amount = services.reduce((sum, s) => sum + (parseFloat(s.price || s.amount) || 0), 0);
    }

    // Final validation
    if (!email) return res.status(400).json({ success: false, error: 'No customer email available. Provide email manually.' });
    if (!estimate_amount) return res.status(400).json({ success: false, error: 'No estimate amount available. Provide estimate_amount manually.' });

    // Find or create customer
    let customer_id = null;
    const existingCustomer = await pool.query('SELECT id FROM customers WHERE email = $1', [email]);
    if (existingCustomer.rows.length > 0) {
      customer_id = existingCustomer.rows[0].id;
    } else {
      const newCustNum = await nextCustomerNumber();
      const newCustomer = await pool.query(
        `INSERT INTO customers (customer_number, name, email, phone, street, created_at)
         VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) RETURNING id`,
        [newCustNum, customer_name, email, phone || null, address || null]
      );
      customer_id = newCustomer.rows[0].id;
      console.log('Created new customer from CopilotCRM estimate:', customer_id);

      if (process.env.ZAPIER_CUSTOMER_WEBHOOK) {
        try {
          await fetch(process.env.ZAPIER_CUSTOMER_WEBHOOK, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ customer_id, name: customer_name, email, phone, address, source: 'copilotcrm_estimate' })
          });
        } catch (e) { console.error('Zapier webhook failed:', e); }
      }
    }

    // Generate token and map services
    const sign_token = generateToken();
    const serviceItems = services.map(s => ({ name: s.name, amount: s.price || s.amount, price: s.price || s.amount }));

    // Create sent_quotes record — status='sent' since estimate is already accepted
    const result = await pool.query(
      `INSERT INTO sent_quotes (
        customer_id, customer_name, customer_email, customer_phone, customer_address,
        quote_type, services, subtotal, tax_rate, tax_amount, total,
        status, sign_token, notes, quote_number, created_at, sent_at
      ) VALUES ($1, $2, $3, $4, $5, 'regular', $6, $7, 0, 0, $7, 'sent', $8, $9, $10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *`,
      [
        customer_id, customer_name, email, phone || null, address || null,
        JSON.stringify(serviceItems), estimate_amount,
        sign_token, 'Auto-created from CopilotCRM estimate #' + estimate_number, estimate_number
      ]
    );
    const newQuote = result.rows[0];

    await logQuoteEvent(newQuote.id, 'created', 'Contract created from CopilotCRM estimate #' + estimate_number, {
      source: 'copilotcrm', estimate_number, total: estimate_amount, services_count: services.length
    });

    // Build and send contract email
    const contractUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-contract.html?token=${sign_token}`;
    const firstName = escapeHtml((customer_name || '').split(' ')[0] || 'there');
    const assetsUrl = process.env.EMAIL_ASSETS_URL || process.env.BASE_URL || 'https://app.pappaslandscaping.com';

    const emailContent = `
      <div style="text-align:center;margin:0 0 28px;">
        <h2 style="font-family:'Open Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#2e403d;font-size:24px;font-weight:600;margin:0;">Your Service Agreement is Ready</h2>
      </div>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Hi ${firstName},</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Thank you for accepting your estimate with Pappas & Co. Landscaping! Before we get started, please take a moment to review and sign your service agreement.</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">This agreement covers the scope of work, terms, and pricing for your accepted estimate.</p>
      <div style="text-align:center;margin:28px 0 20px;">
        <a href="${contractUrl}" style="background:#c9dd80;color:#2e403d;padding:16px 52px;text-decoration:none;border-radius:50px;font-weight:700;font-size:15px;display:inline-block;letter-spacing:0.3px;">Review & Sign Agreement \u{2192}</a>
      </div>
      <p style="font-size:14px;color:#94a3b8;text-align:center;margin:0 0 24px;">Or just reply to this email with any questions</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">If you have any questions, feel free to call or text us at <strong>440-886-7318</strong>. We're always happy to help!</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0;">We look forward to working with you!</p>
    `;

    await sendEmail(
      email,
      'Your Service Agreement from ' + COMPANY_NAME,
      emailTemplate(emailContent),
      null,
      { type: 'contract', customer_id, customer_name, quote_id: newQuote.id }
    );

    await logQuoteEvent(newQuote.id, 'sent', 'Contract sent to ' + email, { email, source: 'copilotcrm' });

    console.log(`✅ CopilotCRM estimate-accepted: Contract sent to ${email} for estimate #${estimate_number}`);
    res.json({ success: true, message: 'Contract sent to ' + email, quote_id: newQuote.id, contract_url: contractUrl });
  } catch (error) {
    serverError(res, error, 'Error creating contract from CopilotCRM estimate');
  }
}

router.post('/api/copilotcrm/estimate-accepted', authorizeEstimateAcceptedRequest, handleCopilotEstimateAccepted);

async function createContractFromAcceptedEstimatePayload(payload) {
  return new Promise((resolve) => {
    const req = { body: payload };
    const res = {
      statusCode: 200,
      status(code) {
        this.statusCode = code;
        return this;
      },
      json(data) {
        resolve({ statusCode: this.statusCode, data });
      }
    };

    handleCopilotEstimateAccepted(req, res).catch(error => {
      resolve({
        statusCode: 500,
        data: { success: false, error: error.message || 'Error creating contract from CopilotCRM estimate' }
      });
    });
  });
}

async function processRecentAcceptedCopilotEstimates({ maxAgeDays = 3, limit = 10 } = {}) {
  const copilotHeaders = await loginToCopilotCrmForEstimates();
  const listRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
    method: 'POST',
    headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
    body: encodeCopilotForm({
      postData: { estimate_status: [2] },
      pagination: '',
      sort: 'datedesc'
    })
  });
  const listData = await listRes.json().catch(() => null);
  const listHtml = listData?.html || '';
  const acceptedRows = parseRecentAcceptedCopilotEstimateRows(listHtml, maxAgeDays).slice(0, limit);
  const results = [];

  for (const row of acceptedRows) {
    const existing = await pool.query(
      `SELECT id, sign_token FROM sent_quotes WHERE quote_number = $1 AND status NOT IN ('declined') LIMIT 1`,
      [row.estimate_number]
    );
    if (existing.rows.length > 0) {
      results.push({ estimate_number: row.estimate_number, skipped: true, reason: 'contract_exists', quote_id: existing.rows[0].id });
      continue;
    }

    const detailRes = await fetch(`https://secure.copilotcrm.com/finances/estimates/view/${row.copilot_id}`, {
      method: 'GET',
      headers: copilotHeaders
    });
    const detailHtml = await detailRes.text();
    const payload = parseCopilotAcceptedEstimateDetail(detailHtml, {
      estimate_number: row.estimate_number
    });

    if (!payload.customer_name || !payload.email || !payload.estimate_number || !payload.estimate_amount || !payload.services?.length) {
      results.push({
        estimate_number: row.estimate_number,
        success: false,
        error: 'Could not parse required accepted estimate detail',
        parsed: {
          customer_name: payload.customer_name || null,
          email: payload.email || null,
          estimate_amount: payload.estimate_amount || null,
          services_count: payload.services?.length || 0
        }
      });
      continue;
    }

    const result = await createContractFromAcceptedEstimatePayload(payload);
    results.push({ estimate_number: row.estimate_number, ...result.data, statusCode: result.statusCode });
  }

  return {
    checked: acceptedRows.length,
    sent: results.filter(result => result.success && !result.skipped && /Contract sent/i.test(result.message || '')).length,
    skipped: results.filter(result => result.skipped || /already exists/i.test(result.message || '')).length,
    failed: results.filter(result => result.success === false).length,
    results
  };
}

let copilotAcceptedEstimatePollInProgress = false;

async function runCopilotAcceptedEstimatePoll(trigger = 'interval') {
  if (copilotAcceptedEstimatePollInProgress) {
    return { success: true, skipped: true, reason: 'poll_already_running' };
  }
  copilotAcceptedEstimatePollInProgress = true;
  try {
    const result = await processRecentAcceptedCopilotEstimates({
      maxAgeDays: Number(process.env.COPILOT_ACCEPTED_ESTIMATE_POLL_DAYS || 3),
      limit: Number(process.env.COPILOT_ACCEPTED_ESTIMATE_POLL_LIMIT || 10)
    });
    console.log(`✅ CopilotCRM accepted-estimate poll (${trigger}): checked=${result.checked}, sent=${result.sent}, skipped=${result.skipped}, failed=${result.failed}`);
    return { success: true, trigger, ...result };
  } catch (error) {
    console.error(`CopilotCRM accepted-estimate poll failed (${trigger}):`, error);
    return { success: false, trigger, error: error.message || 'CopilotCRM accepted-estimate poll failed' };
  } finally {
    copilotAcceptedEstimatePollInProgress = false;
  }
}

async function handleCopilotAcceptedEstimateCron(req, res) {
  if (!assertQuoteCronSecret(req, res)) return;
  const result = await runCopilotAcceptedEstimatePoll('cron');
  res.status(result.success ? 200 : 500).json(result);
}

router.post('/api/cron/copilot-accepted-estimates', handleCopilotAcceptedEstimateCron);
router.get('/api/cron/copilot-accepted-estimates', handleCopilotAcceptedEstimateCron);

if (process.env.NODE_ENV !== 'test' && process.env.COPILOT_ACCEPTED_ESTIMATE_POLLING !== 'false') {
  const intervalMs = Math.max(60_000, Number(process.env.COPILOT_ACCEPTED_ESTIMATE_POLL_MS || 300_000));
  setTimeout(() => runCopilotAcceptedEstimatePoll('startup'), 30_000).unref?.();
  setInterval(() => runCopilotAcceptedEstimatePoll('interval'), intervalMs).unref?.();
}

// GET /api/sent-quotes/:id/contract-status - Check contract status
router.get('/api/sent-quotes/:id/contract-status', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, quote_number, status, contract_signed_at, contract_signer_name, contract_signature_type
      FROM sent_quotes WHERE id = $1
    `, [req.params.id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = result.rows[0];
    res.json({
      success: true,
      contract_signed: !!quote.contract_signed_at,
      signed_at: quote.contract_signed_at,
      signer_name: quote.contract_signer_name,
      signature_type: quote.contract_signature_type,
      status: quote.status
    });

  } catch (error) {
    console.error('Error getting contract status:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/:id/download-pdf - Download signed contract PDF
router.get('/api/sent-quotes/:id/download-pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    
    const quote = result.rows[0];
    
    if (!quote.contract_signed_at) {
      return res.status(400).json({ success: false, error: 'Contract not yet signed' });
    }
    
    const signedDate = new Date(quote.contract_signed_at).toLocaleDateString();
    const pdfBytes = await generateContractPDF(
      quote, 
      quote.contract_signature_data, 
      quote.contract_signer_name, 
      signedDate
    );
    
    if (!pdfBytes) {
      return res.status(500).json({ success: false, error: 'PDF generation failed - template may be missing' });
    }
    
    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="Service-Agreement-${quoteNumber}.pdf"`);
    res.send(Buffer.from(pdfBytes));
    
  } catch (error) {
    console.error('Error downloading PDF:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/:id/download-quote - Download quote PDF
router.get('/api/sent-quotes/:id/download-quote', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    const quote = result.rows[0];

    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    const pdfResult = await generateQuotePDF(quote);

    if (!pdfResult || !pdfResult.bytes) {
      return res.status(500).json({ success: false, error: 'PDF generation failed' });
    }

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="Quote-${quoteNumber}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Error downloading quote:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/:id/download-contract - Download signed contract PDF
router.get('/api/sent-quotes/:id/download-contract', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    const quote = result.rows[0];
    
    if (!quote.contract_signed_at) {
      return res.status(400).json({ success: false, error: 'Contract not yet signed' });
    }
    
    // Return contract data for client-side PDF generation
    res.json({ success: true, quote, type: 'contract' });
  } catch (error) {
    console.error('Error downloading contract:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════

router.get('/api/quotes/next-number', async (req, res) => {
  try {
    // sent_quotes table created at startup by runStartupTableInit (lib/startup-schema.js)
    const result = await pool.query(
      `SELECT MAX(CAST(quote_number AS INTEGER)) as max_num
       FROM sent_quotes
       WHERE quote_number ~ '^[0-9]+$'`
    );
    const maxNum = result.rows[0]?.max_num || 1500;
    res.json({ success: true, next_number: maxNum + 1 });
  } catch (error) {
    serverError(res, error);
  }
});

  return router;
};
