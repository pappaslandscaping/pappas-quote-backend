const express = require('express');
const {
  fetchHomeWorksBusinessSummary,
  fetchHomeWorksIntegrationStatus,
} = require('../services/homeworks/client');

function createHomeWorksRoutes({
  pool,
  authenticateToken,
  serverError,
  fetchImpl = fetch,
  summaryProvider = fetchHomeWorksBusinessSummary,
  statusProvider = fetchHomeWorksIntegrationStatus,
}) {
  const router = express.Router();
  const ownerOnly=(req,res,next)=>(!req.user?.isAdmin || req.user?.isEmployee) ? res.status(403).json({success:false,error:'Owner access is required.'}) : next();
  router.get('/api/homeworks/review',authenticateToken,ownerOnly,async(req,res)=>{
    try{res.json(await require('../services/homeworks/review').loadReview({pool,fetchImpl},{startDate:req.query.start_date}));}
    catch(error){serverError(res,error,'Homeworks review is unavailable');}
  });
  router.get('/api/homeworks/snow-readiness',authenticateToken,ownerOnly,async(req,res)=>{
    try{res.json(await require('../services/homeworks/review').loadSnowReadiness({pool,fetchImpl},req.query.season||'2026-2027'));}
    catch(error){serverError(res,error,'Snow readiness is unavailable');}
  });
  router.get('/api/homeworks/agreements',authenticateToken,ownerOnly,async(req,res)=>{
    try{
      await require('../services/homeworks/schema').ensureHomeworksSchema(pool);
      const rows=await pool.query(`SELECT q.id,q.quote_number,q.customer_name,q.contract_signed_at,
        (SELECT json_agg(json_build_object('type',event_type,'details',details,'date',created_at))
         FROM quote_events e WHERE e.sent_quote_id=q.id AND e.event_type IN ('snow_pdf_synced','contract_sync_failed')) AS attachments
        FROM sent_quotes q WHERE q.contract_signed_at IS NOT NULL ORDER BY q.contract_signed_at DESC LIMIT 200`);
      const ledger=await pool.query('SELECT quote_id,property_id,status,error,media_id FROM homeworks_document_attachments WHERE quote_id=ANY($1::bigint[])',[rows.rows.map(row=>row.id)]);
      const agreements=rows.rows.map(row=>({...row,uploadStatus:ledger.rows.filter(item=>Number(item.quote_id)===Number(row.id))}));
      res.json({success:true,source:'yarddesk',agreements,limited:rows.rows.length>=200,estimateAttachmentSupported:false});
    }catch(error){serverError(res,error,'Signed agreement status is unavailable');}
  });
  let summaryCache = null;
  const summaryCacheMs = 2 * 60 * 1000;

  router.get('/api/homeworks/summary', authenticateToken, async (req, res) => {
    try {
      const now = Date.now();
      if (!summaryCache || now - summaryCache.cachedAt >= summaryCacheMs) {
        summaryCache = {
          cachedAt: now,
          value: await summaryProvider({ pool, fetchImpl }),
        };
      }
      res.json({ success: true, ...summaryCache.value });
    } catch (error) {
      serverError(res, error);
    }
  });

  router.get('/api/homeworks/status', authenticateToken, async (req, res) => {
    try {
      const status = await statusProvider({ pool, fetchImpl });
      res.json({ success: true, ...status });
    } catch (error) {
      serverError(res, error);
    }
  });

  return router;
}

module.exports = createHomeWorksRoutes;
