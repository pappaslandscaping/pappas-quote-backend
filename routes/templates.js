// ═══════════════════════════════════════════════════════════
// Template Routes — extracted from server.js
// Handles: email/SMS templates CRUD, preview, library, variables.
// Targets the email_templates table. The send-preview endpoint also
// reads message_templates as a fallback for any rows that exist there
// (legacy data — write path goes through email_templates).
// ═══════════════════════════════════════════════════════════

const express = require('express');

module.exports = function createTemplateRoutes({ pool, sendEmail, emailTemplate, renderWithBaseLayout, serverError, getTemplate, replaceTemplateVars }) {
  const router = express.Router();

router.get('/api/templates', async (req, res) => {
  try {
    const { category } = req.query;
    let query = 'SELECT * FROM email_templates';
    const params = [];
    if (category) { query += ' WHERE category = $1'; params.push(category); }
    query += ' ORDER BY category, name';
    const result = await pool.query(query, params);
    res.json({ success: true, templates: result.rows });
  } catch (error) { serverError(res, error); }
});

router.post('/api/templates', async (req, res) => {
  try {
    const { name, slug, category, channel, subject, body, sms_body, variables, is_active, options } = req.body;
    if (!name || !slug) return res.status(400).json({ success: false, error: 'name and slug required' });
    const result = await pool.query(
      `INSERT INTO email_templates (name, slug, category, channel, subject, body, sms_body, variables, is_active, options)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING *`,
      [name, slug, category || 'system', channel || 'email', subject, body, sms_body, JSON.stringify(variables || []), is_active !== false, JSON.stringify(options || {})]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.patch('/api/templates/:id', async (req, res) => {
  try {
    const fields = ['name', 'slug', 'category', 'channel', 'subject', 'body', 'sms_body', 'variables', 'is_active', 'is_default', 'options'];
    const updates = [];
    const params = [];
    let p = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) {
        const val = (f === 'variables' || f === 'options') ? JSON.stringify(req.body[f]) : req.body[f];
        updates.push(`${f} = $${p++}`);
        params.push(val);
      }
    }
    if (updates.length === 0) return res.status(400).json({ success: false, error: 'No fields to update' });
    updates.push('updated_at = NOW()');
    params.push(req.params.id);
    const result = await pool.query(`UPDATE email_templates SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`, params);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.delete('/api/templates/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM email_templates WHERE id = $1 AND is_default = false RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(400).json({ success: false, error: 'Cannot delete default template or not found' });
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

router.post('/api/templates/:id/duplicate', async (req, res) => {
  try {
    const orig = await pool.query('SELECT * FROM email_templates WHERE id = $1', [req.params.id]);
    if (orig.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
    const t = orig.rows[0];
    const newSlug = t.slug + '_copy_' + Date.now();
    const result = await pool.query(
      `INSERT INTO email_templates (name, slug, category, subject, body, sms_body, variables, is_active, options)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [t.name + ' (Copy)', newSlug, t.category, t.subject, t.body, t.sms_body, JSON.stringify(t.variables), true, JSON.stringify(t.options)]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

router.post('/api/templates/preview', async (req, res) => {
  try {
    const { slug, vars = {}, subject: directSubject, body: directBody, wrapper, options } = req.body;
    let template = null;
    let sourceSubject = directSubject;
    let sourceBody = directBody;
    let templateOptions = options || {};

    if (slug) {
      template = await getTemplate(slug);
      if (!template) return res.status(404).json({ success: false, error: 'Template not found' });
      sourceSubject = template.subject;
      sourceBody = template.body;
      templateOptions = { ...template.options, ...templateOptions };
    }
    if (!sourceBody && !sourceSubject) {
      return res.status(400).json({ success: false, error: 'Template content required' });
    }
    const resolvedWrapper = wrapper || templateOptions.wrapper || 'full';
    const subject = replaceTemplateVars(sourceSubject || '', vars);
    const body = replaceTemplateVars(sourceBody || '', vars);

    // Check if MJML
    const useMJML = templateOptions.use_mjml === true || body.includes('<mj-') || body.includes('<mjml>');
    
    let html;
    if (useMJML) {
      html = await renderWithBaseLayout(body, {
        wrapper: resolvedWrapper,
        showFeatures: templateOptions.showFeatures || false,
        showSignature: templateOptions.showSignature !== false,
        baseUrl: process.env.BASE_URL,
        unsubscribeEmail: vars.unsubscribe_email || (vars.customer_email ? encodeURIComponent(vars.customer_email) : '{unsubscribe_email}')
      });
    } else {
      html = emailTemplate(body, { wrapper: resolvedWrapper });
      // Replace unsubscribe_email in wrapper footer
      if (vars.unsubscribe_email) {
        html = html.replace(/\{unsubscribe_email\}/g, vars.unsubscribe_email);
      } else if (vars.customer_email) {
        html = html.replace(/\{unsubscribe_email\}/g, encodeURIComponent(vars.customer_email));
      }
    }

    res.json({ success: true, subject, html });
  } catch (error) { serverError(res, error); }
});

router.post('/api/templates/send-preview', async (req, res) => {
  try {
    const { template_id, slug, subject: directSubject, html_content: directHtml, to, wrapper, options } = req.body;
    const sampleVars = { customer_name: 'Jane Smith', customer_first_name: 'Jane', customer_email: 'jane@example.com', customer_phone: '(440) 555-0123', customer_address: '123 Main St, Lakewood OH 44107', invoice_number: 'INV-1234', invoice_total: '285.00', invoice_due_date: 'March 15, 2026', amount_paid: '285.00', balance_due: '285.00', payment_link: '#preview', quote_number: 'Q-5678', quote_total: '1,250.00', quote_link: '#preview', services_list: 'Weekly Mowing, Spring Cleanup', job_date: 'March 10, 2026', service_type: 'Weekly Mowing', crew_name: 'Crew A', address: '123 Main St, Lakewood OH', company_name: 'Pappas & Co. Landscaping', company_phone: '(440) 886-7318', company_email: 'hello@pappaslandscaping.com', company_website: 'pappaslandscaping.com', portal_link: '#preview', yard_sign_yes_link: 'https://app.pappaslandscaping.com/yard-sign-response?token=preview&answer=yes', yard_sign_no_link: 'https://app.pappaslandscaping.com/yard-sign-response?token=preview&answer=no' };

    let subject, body, templateOptions = options || {};
    let wrapperMode = wrapper || templateOptions.wrapper || 'full';

    if (directSubject && directHtml) {
      // Direct content from the new templates editor
      subject = directSubject;
      body = directHtml;
    } else {
      // Look up from database
      let template;
      if (template_id) {
        // Try message_templates first (new table), then email_templates (legacy)
        let r = await pool.query('SELECT * FROM message_templates WHERE id = $1', [template_id]).catch(() => ({ rows: [] }));
        if (r.rows.length > 0) {
          template = { subject: r.rows[0].subject, body: r.rows[0].html_content, options: r.rows[0].options };
        } else {
          r = await pool.query('SELECT * FROM email_templates WHERE id = $1', [template_id]).catch(() => ({ rows: [] }));
          template = r.rows[0];
        }
      } else if (slug) {
        template = await getTemplate(slug);
      }
      if (!template) return res.status(404).json({ success: false, error: 'Template not found' });
      subject = template.subject;
      body = template.body || template.html_content;
      templateOptions = { ...template.options, ...templateOptions };
      wrapperMode = templateOptions.wrapper || wrapperMode;
    }

    const finalSubject = replaceTemplateVars(subject, sampleVars);
    const finalBody = replaceTemplateVars(body, sampleVars);
    
    // Check if MJML
    const useMJML = templateOptions.use_mjml === true || finalBody.includes('<mj-') || finalBody.includes('<mjml>');
    
    let html;
    if (useMJML) {
      html = await renderWithBaseLayout(finalBody, {
        wrapper: wrapperMode,
        showFeatures: templateOptions.showFeatures || false,
        showSignature: templateOptions.showSignature !== false,
        baseUrl: process.env.BASE_URL,
        unsubscribeEmail: sampleVars.customer_email
      });
    } else {
      html = emailTemplate(finalBody, { wrapper: wrapperMode });
      html = html.replace(/\{unsubscribe_email\}/g, encodeURIComponent(sampleVars.customer_email));
    }

    const recipient = to || 'hello@pappaslandscaping.com';
    await sendEmail(recipient, `[TEST] ${finalSubject}`, html);
    res.json({ success: true, message: 'Test email sent to ' + recipient });
  } catch (error) { serverError(res, error); }
});

router.get('/api/templates/variables', (req, res) => {
  res.json({
    success: true,
    variables: {
      customer: ['customer_name', 'customer_first_name', 'customer_email', 'customer_phone', 'customer_address'],
      invoice: ['invoice_number', 'invoice_total', 'invoice_due_date', 'amount_paid', 'balance_due', 'payment_link'],
      quote: ['quote_number', 'quote_total', 'quote_link', 'services_list'],
      job: ['job_date', 'service_type', 'crew_name', 'address'],
      company: ['company_name', 'company_phone', 'company_email', 'company_website', 'portal_link', 'yard_sign_yes_link', 'yard_sign_no_link']
    }
  });
});

// GET /api/templates/library - Pre-built professional template library
router.get('/api/templates/library', (req, res) => {
  const library = [
    {
      id: 'spring-cleanup',
      name: 'Spring Cleanup Promotion',
      category: 'marketing',
      description: 'Seasonal promo for spring cleanup services with CTA (MJML)',
      subject: 'Spring is here — time to refresh your yard!',
      sms_body: 'Hi {customer_first_name}! Spring is here and your yard is calling. Book your spring cleanup today: {portal_link} — Tim, Pappas & Co.',
      options: { use_mjml: true, showSignature: true },
      body: `<mj-text font-size="24px" font-weight="700" color="#2e403d" padding-bottom="16px">Spring Is Here, {customer_first_name}!</mj-text>
<mj-text padding-bottom="16px">The snow has melted, and your property is ready for some fresh attention. Our spring cleanup crew is booking fast — let&rsquo;s get your yard looking its best before the growing season kicks off.</mj-text>

<mj-table padding="0">
  <tr>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:15%;vertical-align:top;font-size:20px;">🌿</td>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:85%;vertical-align:top;">
      <strong style="color:#2e403d;font-family:'DM Sans',Arial,sans-serif;">Debris &amp; Leaf Removal</strong><br/>
      <span style="font-size:13px;color:#64748b;font-family:'DM Sans',Arial,sans-serif;">Clear winter buildup from beds, lawn, and hardscapes</span>
    </td>
  </tr>
  <tr>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:15%;vertical-align:top;font-size:20px;">✂️</td>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:85%;vertical-align:top;">
      <strong style="color:#2e403d;font-family:'DM Sans',Arial,sans-serif;">Bed Edging &amp; Mulch Prep</strong><br/>
      <span style="font-size:13px;color:#64748b;font-family:'DM Sans',Arial,sans-serif;">Crisp edges and fresh beds ready for mulch</span>
    </td>
  </tr>
  <tr>
    <td style="padding:12px 0;width:15%;vertical-align:top;font-size:20px;">🏡</td>
    <td style="padding:12px 0;width:85%;vertical-align:top;">
      <strong style="color:#2e403d;font-family:'DM Sans',Arial,sans-serif;">First Mow of the Season</strong><br/>
      <span style="font-size:13px;color:#64748b;font-family:'DM Sans',Arial,sans-serif;">Get your lawn off to the right start</span>
    </td>
  </tr>
</mj-table>

<mj-button background-color="#2e403d" color="#c9dd80" font-size="15px" font-weight="700" border-radius="8px" href="{portal_link}" padding-top="28px">
  Book Spring Cleanup
</mj-button>`
    },
    {
      id: 'fall-leaf-removal',
      name: 'Fall Leaf Removal Campaign',
      category: 'marketing',
      description: 'Seasonal promo for fall leaf removal and winterization',
      subject: 'Leaves are falling — let us handle the cleanup',
      sms_body: 'Hi {customer_first_name}! Leaves piling up? We\'ve got you. Book fall cleanup before spots fill up: {portal_link} — Tim',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Fall Cleanup Time, {customer_first_name}</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">The leaves are coming down, and your lawn needs protection before winter. Our fall cleanup includes everything to get your property ready for the cold months ahead.</p>
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px 24px;margin:24px 0;text-align:center;"><p style="color:#2e403d;font-size:18px;font-weight:700;margin:0 0 6px;">Fall Cleanup Includes</p><p style="color:#374151;font-size:14px;margin:0;">Leaf removal &bull; Gutter clearing &bull; Bed cleanup &bull; Final mow &bull; Winterization prep</p></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Spots fill up fast this time of year. Let us know if you'd like to get on the schedule.</p>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Schedule Fall Cleanup</a></div>`
    },
    {
      id: 'service-recap',
      name: 'Monthly Service Recap',
      category: 'system',
      description: 'Summary of completed work this month',
      subject: 'Your monthly service recap from Pappas & Co.',
      sms_body: 'Hi {customer_first_name}, your monthly service recap is ready! Check your email for details. — Tim, Pappas & Co.',
      body: `<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">Your Monthly Recap</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name}, here&rsquo;s a summary of the work we completed at your property this month.</p>
<hr style="border:none;border-top:2px solid #e5e7eb;margin:24px 0;">
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;"><strong style="color:#2e403d;">Services Completed:</strong> {services_list}</p>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;"><strong style="color:#2e403d;">Address:</strong> {customer_address}</p>
<hr style="border:none;border-top:2px solid #e5e7eb;margin:24px 0;">
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Have questions or want to adjust your services? Just reply to this email or give us a call.</p>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">View Your Portal</a></div>`
    },
    {
      id: 'new-customer-welcome',
      name: 'New Customer Welcome',
      category: 'portal',
      description: 'Welcome email for new customers with portal intro (MJML)',
      subject: 'Welcome to Pappas & Co. Landscaping!',
      sms_body: 'Welcome to Pappas & Co., {customer_first_name}! We\'re excited to work with you. Check your email for your portal access. — Tim',
      options: { use_mjml: true, showFeatures: true, showSignature: true },
      body: `<mj-text font-size="24px" font-weight="700" color="#2e403d" padding-bottom="16px">Welcome to the Family, {customer_first_name}!</mj-text>
<mj-text padding-bottom="16px">We&rsquo;re thrilled to have you as part of the Pappas &amp; Co. Landscaping family. Tim and the team are looking forward to taking care of your property.</mj-text>
<mj-text padding-bottom="16px">Your customer portal is ready. Here&rsquo;s what you can do:</mj-text>

<mj-table padding="0">
  <tr>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:15%;vertical-align:top;font-size:20px;">📅</td>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:85%;vertical-align:top;">
      <strong style="color:#2e403d;font-family:'DM Sans',Arial,sans-serif;">View Your Schedule</strong><br/>
      <span style="font-size:13px;color:#64748b;font-family:'DM Sans',Arial,sans-serif;">See upcoming services and past visits</span>
    </td>
  </tr>
  <tr>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:15%;vertical-align:top;font-size:20px;">💳</td>
    <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;width:85%;vertical-align:top;">
      <strong style="color:#2e403d;font-family:'DM Sans',Arial,sans-serif;">Pay Invoices Online</strong><br/>
      <span style="font-size:13px;color:#64748b;font-family:'DM Sans',Arial,sans-serif;">Quick, secure payments anytime</span>
    </td>
  </tr>
  <tr>
    <td style="padding:12px 0;width:15%;vertical-align:top;font-size:20px;">💬</td>
    <td style="padding:12px 0;width:85%;vertical-align:top;">
      <strong style="color:#2e403d;font-family:'DM Sans',Arial,sans-serif;">Message Us Directly</strong><br/>
      <span style="font-size:13px;color:#64748b;font-family:'DM Sans',Arial,sans-serif;">Questions, requests, or feedback — we&rsquo;re here</span>
    </td>
  </tr>
</mj-table>

<mj-button background-color="#2e403d" color="#c9dd80" font-size="15px" font-weight="700" border-radius="8px" href="{portal_link}" padding-top="28px">
  Access Your Portal
</mj-button>`
    },
    {
      id: 'service-reminder',
      name: 'Service Day Reminder',
      category: 'system',
      description: 'Remind customer about tomorrow\'s scheduled service',
      subject: 'Reminder: {service_type} tomorrow at your property',
      sms_body: 'Hi {customer_first_name}! Friendly reminder — we\'ll be at {address} tomorrow for {service_type}. See you then! — Tim',
      body: `<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">Service Reminder</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name}, just a quick heads-up that we&rsquo;ll be at your property tomorrow!</p>
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px 24px;margin:24px 0;"><table width="100%" cellpadding="0" cellspacing="0"><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Service:</strong> <span style="color:#374151;">{service_type}</span></td></tr><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Date:</strong> <span style="color:#374151;">{job_date}</span></td></tr><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Address:</strong> <span style="color:#374151;">{address}</span></td></tr><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Crew:</strong> <span style="color:#374151;">{crew_name}</span></td></tr></table></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">No need to be home — we&rsquo;ll take care of everything. If you have any special instructions, just reply to this email.</p>`
    },
    {
      id: 'rate-adjustment',
      name: 'Annual Rate Adjustment Notice',
      category: 'invoices',
      description: 'Professional notification of pricing changes',
      subject: 'A note about your 2026 service rates',
      sms_body: 'Hi {customer_first_name}, we sent you an important update about your service rates for the coming year. Please check your email when you get a chance. — Tim',
      body: `<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">A Note About Your Service Rates</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name},</p>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">I wanted to reach out personally about a small adjustment to our service rates for the coming season. As costs for fuel, equipment, and materials continue to rise, we&rsquo;re making a modest increase to keep delivering the same quality you&rsquo;ve come to expect.</p>
<blockquote style="border-left:4px solid #c9dd80;padding:16px 20px;margin:24px 0;background:#f8fafc;border-radius:0 8px 8px 0;"><p style="color:#374151;font-size:15px;line-height:1.8;margin:0;font-style:italic;">We value your business and work hard to keep our pricing fair while maintaining the high standards you deserve.</p></blockquote>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Your updated rates will take effect at the start of the new season. If you have any questions, please don&rsquo;t hesitate to reach out — I&rsquo;m always happy to chat.</p>`
    },
    {
      id: 'winter-dormancy',
      name: 'Winter Season End',
      category: 'marketing',
      description: '"See you in spring" end-of-season message',
      subject: 'Wrapping up for the season — see you in spring!',
      sms_body: 'Hi {customer_first_name}! Another great season in the books. We\'ll see you in spring. Have a wonderful winter! — Tim, Pappas & Co.',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Another Great Season in the Books!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name},</p>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">As the season wraps up, I just wanted to say thank you for trusting Pappas &amp; Co. with your property this year. It&rsquo;s been a pleasure taking care of your lawn and landscaping.</p>
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px 24px;margin:24px 0;text-align:center;"><p style="color:#2e403d;font-size:18px;font-weight:700;margin:0 0 6px;">Want to Lock In Your Spring Spot?</p><p style="color:#374151;font-size:14px;margin:0 0 16px;">Early-bird customers get priority scheduling when the season starts back up.</p><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px;display:inline-block;">Reserve My Spot</a></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Wishing you and your family a wonderful winter. We&rsquo;ll see you in spring!</p>`
    },
    {
      id: 'winback',
      name: 'Win-Back / Re-engagement',
      category: 'marketing',
      description: 'Re-engage inactive customers with a personal touch',
      subject: 'We miss taking care of your lawn, {customer_first_name}',
      sms_body: 'Hi {customer_first_name}, it\'s Tim from Pappas & Co. It\'s been a while! If your yard needs some love, we\'d be happy to help: {portal_link}',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Hey {customer_first_name}, It&rsquo;s Been a While!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">We noticed it&rsquo;s been some time since we last worked together, and I wanted to check in. Whether your needs changed or life just got busy, we&rsquo;d love to help with your property again.</p>
<table width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0;" role="presentation"><tr><td style="width:48%;vertical-align:top;padding-right:12px;"><h3 style="color:#2e403d;font-size:16px;margin:0 0 8px;">One-Time Service</h3><p style="color:#374151;font-size:14px;line-height:1.7;margin:0;">Need a cleanup, mulch job, or one-time mow? We&rsquo;re happy to help with just a single visit.</p></td><td style="width:4%;"></td><td style="width:48%;vertical-align:top;padding-left:12px;"><h3 style="color:#2e403d;font-size:16px;margin:0 0 8px;">Regular Service</h3><p style="color:#374151;font-size:14px;line-height:1.7;margin:0;">Ready to get back on a regular schedule? We&rsquo;ll pick up right where we left off.</p></td></tr></table>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Let&rsquo;s Reconnect</a></div>`
    },
    {
      id: 'holiday-thank-you',
      name: 'Holiday Thank You',
      category: 'marketing',
      description: 'End-of-year gratitude message to all customers',
      subject: 'Happy Holidays from Pappas & Co. Landscaping',
      sms_body: 'Happy Holidays from Tim and the whole Pappas & Co. team! Thank you for a wonderful year. Wishing you and your family all the best.',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;text-align:center;">Happy Holidays, {customer_first_name}!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;text-align:center;">From our family to yours, we want to say <strong>thank you</strong> for trusting Pappas &amp; Co. Landscaping with your property this year.</p>
<hr style="border:none;border-top:2px solid #e5e7eb;margin:28px 0;">
<blockquote style="border-left:4px solid #c9dd80;padding:16px 20px;margin:24px 0;background:#f8fafc;border-radius:0 8px 8px 0;"><p style="color:#374151;font-size:15px;line-height:1.8;margin:0;font-style:italic;">&ldquo;Every client is part of our extended family. We don&rsquo;t just care for your lawn — we care about your experience from the first call to the last leaf.&rdquo;</p><p style="color:#64748b;font-size:13px;margin:8px 0 0;font-weight:600;">&mdash; Tim Pappas</p></blockquote>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;text-align:center;">Wishing you a joyful holiday season and a wonderful new year. We&rsquo;ll see you in the spring!</p>`
    },
    {
      id: 'emergency-service',
      name: 'Emergency / Storm Service',
      category: 'marketing',
      description: 'Urgent service availability after storms',
      subject: 'Storm cleanup help available — Pappas & Co.',
      sms_body: 'Hi {customer_first_name}, storm damage? Our crew is available for emergency cleanup. Call us at (440) 886-7318 or reply here. — Tim, Pappas & Co.',
      body: `<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:12px;padding:20px 24px;margin:0 0 24px;text-align:center;"><p style="color:#991b1b;font-size:18px;font-weight:700;margin:0 0 6px;">Emergency Cleanup Available</p><p style="color:#374151;font-size:14px;margin:0;">Our crews are ready to help with storm damage and debris removal</p></div>
<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">We&rsquo;re Here to Help, {customer_first_name}</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">If the recent storms left damage on your property, our team is ready to help with emergency cleanup. We&rsquo;re prioritizing our existing customers first.</p>
<table width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0;" role="presentation"><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F333;</td><td><strong style="color:#2e403d;">Fallen Tree &amp; Branch Removal</strong></td></tr></table></td></tr><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F9F9;</td><td><strong style="color:#2e403d;">Debris &amp; Yard Cleanup</strong></td></tr></table></td></tr><tr><td style="padding:12px 0;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F6A8;</td><td><strong style="color:#2e403d;">Priority Scheduling for Current Customers</strong></td></tr></table></td></tr></table>
<div style="text-align:center;margin:28px 0;"><a href="tel:4408867318" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Call (440) 886-7318</a></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;text-align:center;">Or reply to this email and we&rsquo;ll get back to you right away.</p>`
    }
  ];
  res.json({ success: true, templates: library });
});


  return router;
};
