// ═══════════════════════════════════════════════════════════
// Campaign Routes — extracted from server.js
// Handles: campaigns CRUD, public submissions, send history,
//          tracking pixels (open/click), unsubscribe
// ═══════════════════════════════════════════════════════════

const express = require('express');
const crypto = require('crypto');
const { validate, schemas } = require('../lib/validate');
const {
  isCompiledCopilotTemplateSlug,
  renderCompiledCopilotTemplate
} = require('../lib/compiled-copilot-templates');

function buildCampaignCustomerActivityCondition({ liveDateClause, scheduledDateClause }) {
  return `(
    EXISTS (
      SELECT 1
      FROM copilot_live_jobs clj
      LEFT JOIN yarddesk_job_overlays yjo ON yjo.job_key = clj.job_key
      LEFT JOIN customers live_customer
        ON live_customer.customer_number IS NOT NULL
       AND clj.source_customer_id IS NOT NULL
       AND live_customer.customer_number = clj.source_customer_id
      WHERE clj.source_deleted_at IS NULL
        AND COALESCE(yjo.customer_link_id, live_customer.id) = c.id
        AND ${liveDateClause}
    )
    OR EXISTS (
      SELECT 1
      FROM scheduled_jobs sj
      WHERE sj.customer_id = c.id
        AND ${scheduledDateClause}
    )
  )`;
}

function buildActiveCampaignCustomerQuery({ liveMonthsPlaceholder = '$1', scheduledMonthsPlaceholder = '$2' } = {}) {
  return `SELECT DISTINCT c.*
    FROM customers c
    WHERE c.email IS NOT NULL
      AND c.email != ''
      AND ${buildCampaignCustomerActivityCondition({
        liveDateClause: `clj.service_date >= CURRENT_DATE - (${liveMonthsPlaceholder}::text || ' months')::INTERVAL`,
        scheduledDateClause: `COALESCE(sj.job_date::date, sj.created_at::date) >= CURRENT_DATE - (${scheduledMonthsPlaceholder}::text || ' months')::INTERVAL`
      })}`;
}

function createCampaignRoutes({ pool, sendEmail, emailTemplate, renderManagedEmail, serverError, NOTIFICATION_EMAIL, replaceTemplateVars }) {
  const router = express.Router();

function appendTrackingPixel(html, pixelUrl) {
  const pixel = `<img src="${pixelUrl}" width="1" height="1" style="display:none;" />`;
  if (!html) return pixel;
  if (html.includes('</body>')) {
    return html.replace('</body>', `${pixel}</body>`);
  }
  return `${html}${pixel}`;
}

router.get('/api/campaigns', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        c.*,
        COUNT(s.id) as submission_count,
        COUNT(CASE WHEN s.status = 'new' THEN 1 END) as new_count,
        COUNT(CASE WHEN s.status = 'enrolled' THEN 1 END) as enrolled_count
      FROM campaigns c
      LEFT JOIN campaign_submissions s ON c.name = s.campaign_id::text OR c.id::text = s.campaign_id::text
      GROUP BY c.id
      ORDER BY c.created_at DESC
    `);

    const weekResult = await pool.query(`
      SELECT COUNT(*) as count 
      FROM campaign_submissions 
      WHERE created_at >= NOW() - INTERVAL '7 days'
    `);

    res.json({
      success: true,
      campaigns: result.rows,
      new_this_week: parseInt(weekResult.rows[0]?.count || 0)
    });
  } catch (error) {
    console.error('Error fetching campaigns:', error);
    serverError(res, error);
  }
});

// POST /api/campaigns - Create a new campaign
router.post('/api/campaigns', validate(schemas.createCampaign), async (req, res) => {
  try {
    const { name, description, form_url, status = 'active' } = req.body;
    if (!name) {
      return res.status(400).json({ success: false, error: 'Campaign name is required' });
    }
    const result = await pool.query(`
      INSERT INTO campaigns (name, description, form_url, status)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `, [name, description, form_url, status]);
    res.json({ success: true, campaign: result.rows[0] });
  } catch (error) {
    console.error('Error creating campaign:', error);
    serverError(res, error);
  }
});

// GET /api/campaigns/:id - Get single campaign
router.get('/api/campaigns/:id', async (req, res) => {
  try {
    const { id } = req.params;
    // If id is numeric, search by id; otherwise search by name
    const isNumeric = /^\d+$/.test(id);
    const result = isNumeric
      ? await pool.query('SELECT * FROM campaigns WHERE id = $1', [id])
      : await pool.query('SELECT * FROM campaigns WHERE name = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    res.json({ success: true, campaign: result.rows[0] });
  } catch (error) {
    console.error('Error fetching campaign:', error);
    serverError(res, error);
  }
});

// PATCH /api/campaigns/:id - Update campaign
router.patch('/api/campaigns/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { name, description, form_url, status } = req.body;
    const updates = [];
    const values = [];
    let p = 1;
    if (name !== undefined) { updates.push(`name = $${p++}`); values.push(name); }
    if (description !== undefined) { updates.push(`description = $${p++}`); values.push(description); }
    if (form_url !== undefined) { updates.push(`form_url = $${p++}`); values.push(form_url); }
    if (status !== undefined) { updates.push(`status = $${p++}`); values.push(status); }
    if (updates.length === 0) {
      return res.status(400).json({ success: false, error: 'No fields to update' });
    }
    updates.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);
    const result = await pool.query(
      `UPDATE campaigns SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`,
      values
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    res.json({ success: true, campaign: result.rows[0] });
  } catch (error) {
    console.error('Error updating campaign:', error);
    serverError(res, error);
  }
});

// DELETE /api/campaigns/:id - Delete campaign
router.delete('/api/campaigns/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM campaigns WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting campaign:', error);
    serverError(res, error);
  }
});

// GET /api/campaigns/:id/submissions - Get submissions for a campaign
router.get('/api/campaigns/:id/submissions', async (req, res) => {
  try {
    const { id } = req.params;
    const { status, limit = 100, offset = 0 } = req.query;
    let query = 'SELECT * FROM campaign_submissions WHERE campaign_id = $1';
    const params = [id];
    if (status) {
      query += ' AND status = $2';
      params.push(status);
    }
    query += ` ORDER BY created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    params.push(limit, offset);
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query('SELECT COUNT(*) as total FROM campaign_submissions WHERE campaign_id = $1', [id])
    ]);
    res.json({
      success: true,
      submissions: result.rows,
      total: parseInt(countResult.rows[0]?.total || 0)
    });
  } catch (error) {
    console.error('Error fetching submissions:', error);
    serverError(res, error);
  }
});

// POST /api/campaigns/submissions - Create a new submission (from customer form)
router.post('/api/campaigns/submissions', async (req, res) => {
  try {
    const { campaign_id, name, firstName, lastName, email, phone, address, services = [], notes } = req.body;
    if (!campaign_id) {
      return res.status(400).json({ success: false, error: 'Campaign ID is required' });
    }
    if (!email && !phone) {
      return res.status(400).json({ success: false, error: 'Email or phone is required' });
    }
    let servicesArray = null;
    if (services) {
      if (Array.isArray(services)) servicesArray = services;
      else if (typeof services === 'string' && services.length > 0) servicesArray = services.split(',').map(s => s.trim());
    }
    const fullName = name || ((firstName || '') + ' ' + (lastName || '')).trim();
    const result = await pool.query(`
      INSERT INTO campaign_submissions 
      (campaign_id, name, first_name, last_name, email, phone, address, services, notes, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'new')
      RETURNING *
    `, [campaign_id, fullName, firstName || null, lastName || null, email || null, phone || null, address || null, servicesArray, notes || null]);
    res.json({ success: true, submission: result.rows[0] });

    // Send notification email
    const servicesText = servicesArray ? servicesArray.join(', ') : 'None specified';
    const dashboardUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com') + '/campaigns.html';
    const emailHtml = `
      <h2>New Campaign Submission</h2>
      <p><strong>Campaign:</strong> ${campaign_id}</p>
      <p><strong>Name:</strong> ${fullName}</p>
      <p><strong>Email:</strong> <a href="mailto:${email}">${email}</a></p>
      <p><strong>Phone:</strong> ${phone || 'Not provided'}</p>
      <p><strong>Address:</strong> ${address || 'Not provided'}</p>
      <p><strong>Services:</strong> ${servicesText}</p>
      <p><strong>Notes:</strong> ${notes || 'None'}</p>
      <br>
      <p><a href="${dashboardUrl}">View in Dashboard</a></p>
    `;
    sendEmail(NOTIFICATION_EMAIL, `New ${campaign_id} Request from ${fullName}`, emailHtml);
  } catch (error) {
    console.error('Error creating submission:', error);
    serverError(res, error);
  }
});

// PATCH /api/campaigns/submissions/:id - Update submission status
router.patch('/api/campaigns/submissions/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { status, notes } = req.body;
    const updates = [];
    const values = [];
    let p = 1;
    if (status !== undefined) { updates.push(`status = $${p++}`); values.push(status); }
    if (notes !== undefined) { updates.push(`notes = $${p++}`); values.push(notes); }
    if (updates.length === 0) {
      return res.status(400).json({ success: false, error: 'No fields to update' });
    }
    updates.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);
    const result = await pool.query(
      `UPDATE campaign_submissions SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`,
      values
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Submission not found' });
    }
    res.json({ success: true, submission: result.rows[0] });
  } catch (error) {
    console.error('Error updating submission:', error);
    serverError(res, error);
  }
});

// DELETE /api/campaigns/submissions/:id - Delete a submission
router.delete('/api/campaigns/submissions/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'DELETE FROM campaign_submissions WHERE id = $1 RETURNING *',
      [req.params.id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Submission not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting submission:', error);
    serverError(res, error);
  }
});

router.post('/api/campaigns/:id/send', async (req, res) => {
  try {
    const { template_id, customer_ids, segment } = req.body;
    if (!template_id) return res.status(400).json({ success: false, error: 'template_id required' });
    const template = await pool.query('SELECT * FROM email_templates WHERE id = $1', [template_id]);
    if (template.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
    const tmpl = template.rows[0];

    // Get target customers
    let customers;
    if (customer_ids && customer_ids.length > 0) {
      customers = await pool.query('SELECT * FROM customers WHERE id = ANY($1)', [customer_ids]);
    } else if (segment === 'all') {
      customers = await pool.query('SELECT * FROM customers WHERE email IS NOT NULL AND email != \'\'');
    } else if (segment === 'monthly_plan') {
      customers = await pool.query('SELECT * FROM customers WHERE monthly_plan_amount > 0 AND email IS NOT NULL');
    } else if (segment === 'active') {
      customers = await pool.query(buildActiveCampaignCustomerQuery(), [6, 6]);
    } else {
      return res.status(400).json({ success: false, error: 'customer_ids or segment required' });
    }

    const results = { sent: 0, errors: 0 };
    for (const cust of customers.rows) {
      try {
        const trackingId = crypto.randomUUID().replace(/-/g, '').slice(0, 24);
        const vars = {
          customer_name: cust.name, customer_first_name: cust.first_name || cust.name,
          customer_email: cust.email, company_name: 'Pappas & Co. Landscaping',
          company_phone: '(440) 886-7318', company_website: 'pappaslandscaping.com',
          unsubscribe_email: encodeURIComponent(cust.email || '')
        };
        const subject = replaceTemplateVars(tmpl.subject, vars);
        // Add tracking pixel
        const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
        let finalHtml;
        if (tmpl.slug && isCompiledCopilotTemplateSlug(tmpl.slug)) {
          finalHtml = renderCompiledCopilotTemplate(tmpl.slug, vars);
        } else {
          const body = replaceTemplateVars(tmpl.body, vars);
          finalHtml = await renderManagedEmail(body, {
            wrapper: tmpl.options?.wrapper || 'full',
            showFeatures: tmpl.options?.showFeatures || false,
            showSignature: tmpl.options?.showSignature !== false,
            baseUrl,
            unsubscribeEmail: encodeURIComponent(cust.email || '')
          });
        }
        finalHtml = appendTrackingPixel(finalHtml, `${baseUrl}/api/t/${trackingId}/open.png`);
        await sendEmail(cust.email, subject, finalHtml, null, { type: 'campaign', customer_id: cust.id, customer_name: cust.name });
        await pool.query(
          'INSERT INTO campaign_sends (campaign_id, template_id, customer_id, customer_email, status, tracking_id) VALUES ($1, $2, $3, $4, $5, $6)',
          [req.params.id, template_id, cust.id, cust.email, 'sent', trackingId]
        );
        results.sent++;
      } catch(e) { results.errors++; }
    }
    // Update campaign stats
    await pool.query('UPDATE campaigns SET template_id = $1, send_count = COALESCE(send_count, 0) + $2 WHERE id = $3', [template_id, results.sent, req.params.id]);
    res.json({ success: true, ...results });
  } catch (error) {
    console.error('Campaign send error:', error);
    serverError(res, error);
  }
});

// GET /api/campaigns/:id/send-history - Send stats
router.get('/api/campaigns/:id/send-history', async (req, res) => {
  try {
    const [sends, stats] = await Promise.all([
      pool.query(`SELECT cs.*, c.name as customer_name FROM campaign_sends cs LEFT JOIN customers c ON cs.customer_id = c.id WHERE cs.campaign_id = $1 ORDER BY cs.sent_at DESC`, [req.params.id]),
      pool.query(`SELECT COUNT(*) as total, COUNT(opened_at) as opens, COUNT(clicked_at) as clicks FROM campaign_sends WHERE campaign_id = $1`, [req.params.id])
    ]);
    res.json({ success: true, sends: sends.rows, stats: stats.rows[0] });
  } catch (error) { serverError(res, error); }
});


router.post('/api/unsubscribe', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ success: false, error: 'Email required' });

    const cleanEmail = email.toLowerCase().trim();
    // Find customer by email
    const cust = await pool.query('SELECT id FROM customers WHERE LOWER(email) = $1', [cleanEmail]);
    if (cust.rows.length === 0) {
      return res.json({ success: true, message: 'Unsubscribed' }); // Don't reveal if email exists
    }

    const customerId = cust.rows[0].id;
    // Update or insert communication prefs
    await pool.query(`
      INSERT INTO customer_communication_prefs (customer_id, email_marketing, sms_marketing, updated_at)
      VALUES ($1, false, false, NOW())
      ON CONFLICT (customer_id) DO UPDATE SET email_marketing = false, sms_marketing = false, updated_at = NOW()
    `, [customerId]);

    // Also add 'Unsubscribed' tag if not already present
    await pool.query(`
      UPDATE customers SET tags = CASE
        WHEN tags IS NULL OR tags = '' THEN 'Unsubscribed'
        WHEN tags ILIKE '%Unsubscribed%' THEN tags
        ELSE tags || ', Unsubscribed'
      END, updated_at = CURRENT_TIMESTAMP
      WHERE id = $1
    `, [customerId]);

    res.json({ success: true, message: 'Unsubscribed' });
  } catch (error) {
    console.error('Unsubscribe error:', error);
    res.status(500).json({ success: false, error: 'Something went wrong' });
  }
});

// Tracking pixel — records open
router.get('/api/t/:trackingId/open.png', async (req, res) => {
  try {
    await pool.query('UPDATE campaign_sends SET opened_at = COALESCE(opened_at, NOW()) WHERE tracking_id = $1', [req.params.trackingId]);
    await pool.query(`UPDATE campaigns SET open_count = (SELECT COUNT(opened_at) FROM campaign_sends WHERE campaign_id = campaigns.id) WHERE id = (SELECT campaign_id FROM campaign_sends WHERE tracking_id = $1)`, [req.params.trackingId]);
  } catch(e) { /* silently fail */ }
  // Return 1x1 transparent PNG
  const pixel = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64');
  res.set({ 'Content-Type': 'image/png', 'Cache-Control': 'no-store, no-cache, must-revalidate', 'Content-Length': pixel.length });
  res.send(pixel);
});

// Click tracking redirect
router.get('/api/t/:trackingId/click', async (req, res) => {
  const { url } = req.query;
  try {
    await pool.query('UPDATE campaign_sends SET clicked_at = COALESCE(clicked_at, NOW()) WHERE tracking_id = $1', [req.params.trackingId]);
    await pool.query(`UPDATE campaigns SET click_count = (SELECT COUNT(clicked_at) FROM campaign_sends WHERE campaign_id = campaigns.id) WHERE id = (SELECT campaign_id FROM campaign_sends WHERE tracking_id = $1)`, [req.params.trackingId]);
  } catch(e) { /* silently fail */ }
  res.redirect(url || '/');
});

// ═══════════════════════════════════════════════════════════
// ═══ BROADCAST ENDPOINTS ══════════════════════════════════
// ═══════════════════════════════════════════════════════════


  return router;
}

createCampaignRoutes._helpers = {
  buildCampaignCustomerActivityCondition,
  buildActiveCampaignCustomerQuery
};

module.exports = createCampaignRoutes;
