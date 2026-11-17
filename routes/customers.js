// ═══════════════════════════════════════════════════════════
// Customer & Property Routes — extracted from server.js
// Handles: customers CRUD, properties CRUD, imports,
//          search, dedup, pipeline, statement PDF
// ═══════════════════════════════════════════════════════════

const express = require('express');
const { validate, schemas } = require('../lib/validate');
const { getCustomer360 } = require('../services/copilot/integration');
const { loadCustomerEstimates, renderMailEstimate } = require('../lib/homeworks-customer-estimates');
const { loadHomeworksCustomerBilling, auditHomeworksCustomerBalances, resolveHomeworksCustomer } = require('../lib/homeworks-customer-billing');
const { scoreCustomerRelationship } = require('../lib/homeworks-crm-scoring');

module.exports = function createCustomerRoutes({ pool, serverError, authenticateToken, nextCustomerNumber, upload, generateStatementPDF, getCopilotToken }) {
  const router = express.Router();


// id, property_name, country, state, street, street2, city, zip, tags, status, lot_size, notes, customer_id
// ═══════════════════════════════════════════════════════════

// GET /api/properties
router.get('/api/properties', async(req,res)=>{try{res.json(await require('../services/homeworks/properties').list({pool},req.query));}catch(error){serverError(res,error,'Homeworks property operation failed');}});

// GET /api/properties/stats
router.get('/api/properties/stats', async(req,res)=>{try{const data=await require('../services/homeworks/properties').list({pool},{limit:5000});if(data.total>data.properties.length)throw new Error('Narrow the property report.');const rows=data.properties,cities=new Map();for(const row of rows)if(row.city)cities.set(row.city,(cities.get(row.city)||0)+1);const active=rows.filter(row=>row.status==='Active').length;res.json({success:true,source:data.source,stats:{total:rows.length,active,inactive:rows.length-active,topCities:[...cities].map(([city,count])=>({city,count})).sort((a,b)=>b.count-a.count).slice(0,20),citiesServed:cities.size,withPricing:null,revenue:{pricedProperties:null},pricingAvailable:false}});}catch(error){serverError(res,error);}});

// GET /api/properties/:id
router.get('/api/properties/:id', async(req,res)=>{try{res.json(await require('../services/homeworks/properties').detail({pool},req.params.id));}catch(error){serverError(res,error,'Homeworks property operation failed');}});

// POST /api/properties
router.post('/api/properties', async(req,res)=>{try{res.json(await require('../services/homeworks/properties').create({pool},req.body));}catch(error){serverError(res,error,'Homeworks property operation failed');}});

// PUT /api/properties/:id
router.put('/api/properties/:id', async(req,res)=>{try{res.json(await require('../services/homeworks/properties').update({pool},req.params.id,req.body));}catch(error){serverError(res,error,'Homeworks property operation failed');}});

// PATCH /api/properties/:id
router.patch('/api/properties/:id', async(req,res)=>{try{res.json(await require('../services/homeworks/properties').update({pool},req.params.id,req.body));}catch(error){serverError(res,error,'Homeworks property operation failed');}});

// DELETE /api/properties/:id
router.delete('/api/properties/:id', async(req,res)=>res.status(409).json({success:false,error:'Manage this customer or property operation in Homeworks. Legacy imports, local deletion, and automatic contact merges have been retired.',destination:'homeworks'}));

// POST /api/properties/:id/photos - Upload photos (base64 in JSONB)
router.post('/api/properties/:id/photos', upload.array('photos', 10), async (req, res) => {
  try {
    const { id } = req.params;
    const prop = await pool.query('SELECT photos FROM properties WHERE id = $1', [id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });

    const existing = prop.rows[0].photos || [];
    const newPhotos = [];
    for (const file of (req.files || [])) {
      const b64 = file.buffer.toString('base64');
      const dataUrl = `data:${file.mimetype};base64,${b64}`;
      newPhotos.push({ url: dataUrl, name: file.originalname, uploaded: new Date().toISOString() });
    }
    const allPhotos = [...existing, ...newPhotos];
    await pool.query('UPDATE properties SET photos = $1 WHERE id = $2', [JSON.stringify(allPhotos), id]);
    res.json({ success: true, photos: allPhotos });
  } catch (error) {
    console.error('Error uploading property photos:', error);
    serverError(res, error);
  }
});

// DELETE /api/properties/:id/photos/:index - Remove a photo
router.delete('/api/properties/:id/photos/:index', async (req, res) => {
  try {
    const { id, index } = req.params;
    const prop = await pool.query('SELECT photos FROM properties WHERE id = $1', [id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });
    const photos = prop.rows[0].photos || [];
    photos.splice(parseInt(index), 1);
    await pool.query('UPDATE properties SET photos = $1 WHERE id = $2', [JSON.stringify(photos), id]);
    res.json({ success: true, photos });
  } catch (error) { serverError(res, error); }
});

// GET /api/properties/:id/service-history
router.get('/api/properties/:id/service-history', async(req,res)=>{try{const {remote}=await require('../services/homeworks/properties').resolve({pool},req.params.id);const service=require('../services/homeworks/jobs'),records=require('../services/homeworks/records');const rows=await records.listAll({pool},{entity:'events',filterType:'EventFilter',where:{propertyId:{equals:remote.id},isDeleted:false},fields:service.fields,pageSize:100});res.json({success:true,source:records.SOURCE,jobs:rows.map(service.map).sort((a,b)=>b.job_date.localeCompare(a.job_date))});}catch(error){serverError(res,error);}});

// POST /api/import-properties - Using YOUR column names
router.post('/api/import-properties', upload.single('csvfile'), async(req,res)=>res.status(409).json({success:false,error:'Manage this customer or property operation in Homeworks. Legacy imports, local deletion, and automatic contact merges have been retired.',destination:'homeworks'}));

router.get('/api/customers', async(req,res)=>{try{res.json(await require('../services/homeworks/customers').list({pool},req.query));}catch(error){serverError(res,error,'Homeworks customer operation failed');}});

router.get('/api/customers/stats', async(req,res)=>{try{const rows=await require('../services/homeworks/customers').directory({pool});const cities=new Map(),now=Date.now();for(const row of rows)if(row.city)cities.set(row.city,(cities.get(row.city)||0)+1);const active=rows.filter(row=>row.status==='active').length,recent=rows.filter(row=>now-Date.parse(row.created_at)<=30*86400000).length,previous=rows.filter(row=>now-Date.parse(row.created_at)>30*86400000&&now-Date.parse(row.created_at)<=60*86400000).length;res.json({success:true,source:'official_homeworks_graphql',stats:{total:rows.length,active,inactive:rows.length-active,topCities:[...cities].map(([city,count])=>({city,count})).sort((a,b)=>b.count-a.count).slice(0,10),trend:{recent,previous,pct:previous?Math.round((recent-previous)/previous*100):recent?100:0}}});}catch(error){serverError(res,error);}});

// GET /api/customers/pipeline-stats - Lead vs Customer pipeline metrics
router.get('/api/customers/pipeline-stats', async(req,res)=>{try{const rows=await require('../services/homeworks/customers').directory({pool});const totalLeads=rows.filter(row=>String(row.customer_type).toLowerCase()==='lead').length;res.json({success:true,source:'official_homeworks_graphql',stats:{totalLeads,totalCustomers:rows.length-totalLeads,newLeadsThisMonth:rows.filter(row=>String(row.customer_type).toLowerCase()==='lead'&&String(row.created_at).slice(0,7)===require('../services/homeworks/records').dateKey().slice(0,7)).length,convertedThisMonth:null,conversionRate:null},note:'Homeworks does not expose a lead conversion timestamp. Conversion metrics are unavailable.'});}catch(error){serverError(res,error);}});

// GET /api/customers/search - Search customers by name for auto-fill
// IMPORTANT: This must come BEFORE /api/customers/:id to avoid :id matching "search"
router.get('/api/customers/search', async(req,res)=>{try{res.json(await require('../services/homeworks/customers').list({pool},{...req.query,limit:10}));}catch(error){serverError(res,error,'Homeworks customer operation failed');}});

router.get('/api/customers/balance-audit', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM customers ORDER BY id');
    const audit = await auditHomeworksCustomerBalances({ customers: result.rows, getCopilotToken });
    res.json({ success: true, ...audit });
  } catch (error) {
    console.error('Customer balance audit failed:', error.message);
    res.status(503).json({ success: false, error: 'Could not verify all customer balances with HomeWorks. Please try again.' });
  }
});

router.get('/api/customers/:id', async(req,res)=>{try{res.json(await require('../services/homeworks/customers').detail({pool},req.params.id));}catch(error){serverError(res,error,'Homeworks customer operation failed');}});

// POST /api/customers/deduplicate - Merge duplicate customers (same name)
router.post('/api/customers/deduplicate', async(req,res)=>res.status(409).json({success:false,error:'Manage this customer or property operation in Homeworks. Legacy imports, local deletion, and automatic contact merges have been retired.',destination:'homeworks'}));

// POST /api/customers/clean-names - Strip QB ID junk and embedded emails from customer names
router.post('/api/customers/clean-names', async(req,res)=>res.status(409).json({success:false,error:'Manage this customer or property operation in Homeworks. Legacy imports, local deletion, and automatic contact merges have been retired.',destination:'homeworks'}));

// POST /api/customers - Create new customer (from Zapier/CopilotCRM sync)
router.post('/api/customers', validate(schemas.createCustomer), async(req,res)=>{try{res.json(await require('../services/homeworks/customers').create({pool},req.body));}catch(error){serverError(res,error,'Homeworks customer operation failed');}});

router.get('/api/customers/:id/properties', async(req,res)=>{try{res.json(await require('../services/homeworks/properties').list({pool},{customer_id:req.params.id}));}catch(error){serverError(res,error,'Homeworks property operation failed');}});

router.get('/api/customers/:id/360', async (req, res) => {
  try {
    const customer360 = await getCustomer360({ pool, customerId: req.params.id, getCopilotToken });
    if (!customer360) {
      return res.status(404).json({ success: false, error: 'Customer not found' });
    }
    return res.json({ success: true, customer360 });
  } catch (error) {
    console.error('Error fetching customer 360:', error);
    serverError(res, error);
  }
});

router.patch('/api/customers/:id', async(req,res)=>{try{res.json(await require('../services/homeworks/customers').update({pool},req.params.id,req.body));}catch(error){serverError(res,error,'Homeworks customer operation failed');}});

router.delete('/api/customers/:id', async(req,res)=>res.status(409).json({success:false,error:'Manage this customer or property operation in Homeworks. Legacy imports, local deletion, and automatic contact merges have been retired.',destination:'homeworks'}));

// Verified customer activity, with no lead or churn guess based on imported jobs.
router.get('/api/customers/:id/intelligence', authenticateToken, async (req, res) => {
  try {
    const local = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (!local.rows.length) return res.status(404).json({ success: false, error: 'Customer not found' });
    const { owner, query } = await resolveHomeworksCustomer({ customer: local.rows[0], getCopilotToken });
    const today = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(new Date());
    const yearAgo = new Date(`${today}T12:00:00Z`);
    yearAgo.setUTCFullYear(yearAgo.getUTCFullYear() - 1);
    const since = yearAgo.toISOString().slice(0, 10);
    const result = await query('YardDeskCrmSignals', `query YardDeskCrmSignals($customerId: SafeInt!, $today: Date!, $since: Date!) {
      customer(customerId: $customerId) { id fullName status email phone cell outstanding pastDue address { street1 } }
      completed: events(where: { customerId: { equals: $customerId }, type: { equals: VISIT }, status: { equals: CLOSED }, isDeleted: false }, take: 1, orderBy: [{ startDate: desc }, { id: desc }]) { id startDate title }
      recentCompleted: events(where: { customerId: { equals: $customerId }, type: { equals: VISIT }, status: { equals: CLOSED }, isDeleted: false, startDate: { gte: $since } }, take: 500, orderBy: [{ startDate: desc }, { id: desc }]) { id startDate title }
      recentVisits: events(where: { customerId: { equals: $customerId }, type: { equals: VISIT }, isDeleted: false, startDate: { gte: $since } }, take: 100, orderBy: [{ startDate: desc }, { id: desc }]) { id startDate title status }
      upcoming: events(where: { customerId: { equals: $customerId }, type: { equals: VISIT }, status: { equals: OPEN }, isDeleted: false, startDate: { gte: $today } }, take: 1, orderBy: [{ startDate: asc }, { id: asc }]) { id startDate title }
      estimates(where: { customerId: { equals: $customerId }, isDeleted: false }, take: 100, orderBy: [{ date: desc }, { id: desc }]) { id number date title status isSent lineItems { name } }
    }`, { customerId: owner.id, today, since });
    if (result.customer?.id !== owner.id || !Array.isArray(result.completed)
      || !Array.isArray(result.recentCompleted) || !Array.isArray(result.recentVisits)
      || !Array.isArray(result.upcoming)
      || !Array.isArray(result.estimates)) {
      throw new Error('HomeWorks returned incomplete customer activity');
    }
    const snowEstimate = result.estimates.find(estimate => /snow/i.test(estimate.title || '')
      || estimate.lineItems?.some(item => /snow/i.test(item.name || '')));
    const pendingEstimates = result.estimates.filter(estimate => estimate.status === 'PENDING');
    const draftEstimates = result.estimates.filter(estimate => estimate.status === 'DRAFT');
    const scores = scoreCustomerRelationship({ customer: result.customer,
      localType: local.rows[0].customer_type, lastCompletedVisit: result.completed[0],
      nextScheduledVisit: result.upcoming[0], completedVisitsLast12Months: result.recentCompleted.length,
      recentVisits: result.recentVisits, estimates: result.estimates, today });
    const waitlistedVisit = result.recentVisits.find(visit => visit.status === 'WAITLISTED'
      && visit.startDate < today);
    res.set('Cache-Control', 'no-store');
    res.json({ success: true, source: 'homeworks', asOf: new Date().toISOString(),
      leadScore: scores.lead, churnRisk: scores.churn,
      accountStatus: result.customer.status, outstanding: result.customer.outstanding,
      pastDue: result.customer.pastDue,
      lastCompletedVisit: result.completed[0] || null, nextScheduledVisit: result.upcoming[0] || null,
      waitlistedVisitToReview: waitlistedVisit || null,
      completedVisitsLast12Months: result.recentCompleted.length,
      completedVisitHistoryLimited: result.recentCompleted.length === 500,
      pendingEstimateCount: pendingEstimates.length,
      pendingEstimates: pendingEstimates.slice(0, 3).map(estimate => ({
        id: estimate.id, number: estimate.number, date: estimate.date, title: estimate.title,
      })),
      draftEstimateCount: draftEstimates.length,
      latestSnowEstimate: snowEstimate ? { id: snowEstimate.id, number: snowEstimate.number,
        date: snowEstimate.date, status: snowEstimate.status } : null,
      estimateHistoryLimited: result.estimates.length === 100 });
  } catch (error) {
    console.error('Customer intelligence unavailable:', error.message);
    res.status(503).json({ success: false, error: 'Unable to verify customer activity with HomeWorks. Please try again.' });
  }
});

// Live HomeWorks estimates also work for customers without email.
router.get('/api/customers/:id/quotes', authenticateToken, async (req, res) => {
  try {
    const local = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (!local.rows.length) return res.status(404).json({ success: false, error: 'Customer not found' });
    const result = await loadCustomerEstimates({ customer: local.rows[0], getCopilotToken });
    res.set('Cache-Control', 'no-store');
    res.json({ success: true, source: 'homeworks', quotes: result.estimates.map(estimate => ({
      id: estimate.id, homeworks_id: estimate.id, quote_number: estimate.number,
      title: estimate.title, status: estimate.status.toLowerCase(), total: estimate.total,
      created_at: estimate.date, source: 'homeworks',
    })) });
  } catch (error) {
    console.error('Customer estimates unavailable:', error.message);
    res.status(503).json({ success: false, error: 'Unable to verify estimates with Homeworks. Please try again.' });
  }
});

router.get('/api/customers/:id/estimates/:estimateId/print', authenticateToken, async (req, res) => {
  try {
    const estimateId = Number(req.params.estimateId);
    if (!Number.isSafeInteger(estimateId) || estimateId <= 0) return res.status(400).json({ success:false, error:'Invalid estimate' });
    const local = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (!local.rows.length) return res.status(404).json({ success:false, error:'Customer not found' });
    const result = await loadCustomerEstimates({ customer: local.rows[0], getCopilotToken });
    const estimate = result.estimates.find(item => item.id === estimateId);
    if (!estimate) return res.status(404).json({ success:false, error:'Estimate not found for this customer' });
    res.set('Cache-Control', 'no-store');
    res.type('html').send(renderMailEstimate({ customer: result.customer, estimate }));
  } catch (error) {
    if (error.code === 'SNOW_ESTIMATE_NEEDS_REVIEW') return res.status(422).json({ success:false, error:error.message });
    serverError(res, error);
  }
});

// GET /api/customers/:id/jobs - Get all scheduled jobs for a customer
router.get('/api/customers/:id/jobs', async (req, res) => {
  try {
    const customerResult = await pool.query('SELECT name, first_name, last_name FROM customers WHERE id = $1', [req.params.id]);
    if (customerResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const c = customerResult.rows[0];
    const customerName = c.name || ((c.first_name || '') + ' ' + (c.last_name || '')).trim();

    const jobsResult = await pool.query(
      `SELECT id, job_date, job_date AS scheduled_date, customer_name, service_type, service_price, address, status, completed_at, crew_assigned
       FROM scheduled_jobs
       WHERE customer_id = $1
         OR LOWER(customer_name) = LOWER($2)
         OR LOWER(customer_name) LIKE LOWER($2) || ' %'
       ORDER BY job_date DESC LIMIT 50`,
      [req.params.id, customerName]
    );
    res.json({ success: true, jobs: jobsResult.rows });
  } catch (error) {
    console.error('Error fetching customer jobs:', error);
    serverError(res, error);
  }
});

// GET /api/customers/:id/invoices - Get all invoices for a customer
router.get('/api/customers/:id/invoices', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (!result.rows.length) return res.status(404).json({ success: false, error: 'Customer not found' });
    const billing = await loadHomeworksCustomerBilling({ customer: result.rows[0], getCopilotToken });
    const local = await pool.query("SELECT id, external_invoice_id FROM invoices WHERE external_source = 'copilotcrm' AND external_invoice_id = ANY($1::text[])", [billing.invoices.map(invoice => invoice.external_invoice_id)]);
    const ids = new Map(local.rows.map(invoice => [String(invoice.external_invoice_id), invoice.id]));
    billing.invoices = billing.invoices.map(invoice => ({ ...invoice, id: ids.get(invoice.external_invoice_id) || null }));
    res.json({ success: true, ...billing });
  } catch (error) {
    console.error('HomeWorks customer billing failed:', error.message);
    res.status(503).json({ success: false, error: 'Could not verify this account with HomeWorks. Please try again.' });
  }
});


router.post('/api/import-customers', upload.single('csvfile'), async(req,res)=>res.status(409).json({success:false,error:'Manage this customer or property operation in Homeworks. Legacy imports, local deletion, and automatic contact merges have been retired.',destination:'homeworks'}));


router.get('/api/customers/:id/statement-pdf', async (req, res) => {
  try {
    if (typeof generateStatementPDF !== 'function') {
      return res.status(503).json({ success: false, error: 'Statement PDF generator is not configured' });
    }
    const custResult = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (custResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const customer = custResult.rows[0];
    const { from, to, status } = req.query;
    const billing = await loadHomeworksCustomerBilling({ customer, getCopilotToken });
    // Only sent HomeWorks invoices belong on a customer-facing statement.
    const statementInvoices = billing.invoices.filter(invoice => invoice.is_sent === true)
      .filter(invoice => !from || invoice.invoice_date >= from)
      .filter(invoice => !to || invoice.invoice_date <= to)
      .filter(invoice => !status || invoice.status === String(status).toLowerCase());
    // Do not silently hide older payments on an unfiltered account statement.
    const paymentFrom = from || '';
    const statementPayments = billing.payments
      .filter(payment => (!paymentFrom || payment.paid_at >= paymentFrom) && (!to || payment.paid_at <= to));
    const dateRange = from ? from + ' to ' + (to || 'Present') : to ? 'All payments through ' + to : 'All recorded payments';
    const pdfResult = await generateStatementPDF({
      customer,
      invoices: statementInvoices,
      payments: statementPayments,
      statementDate: new Date().toISOString(),
      dateRange,
      sourceAsOf: billing.sourceAsOf,
      activityFrom: paymentFrom,
      activityTo: to || new Date().toISOString().slice(0, 10),
    });
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'Statement PDF generation failed' });
    const custName = (customer.name || customer.first_name || 'customer').replace(/\s+/g, '-').toLowerCase();
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="statement-${custName}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Statement PDF error:', error);
    serverError(res, error);
  }
});

  return router;
};
