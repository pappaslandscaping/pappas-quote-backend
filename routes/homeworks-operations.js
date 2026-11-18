'use strict';
const express=require('express');
const operations=require('../services/homeworks/operations');
const checks=require('../services/homeworks/operations-checks');
function createOperationsRoutes({pool,authenticateToken,serverError,generateJson,context:injectedContext,provider=operations,warmup=false}) {
  const router=express.Router(),context=injectedContext||{pool};
  const ownerOnly=(req,res,next)=>req.user?.isAdmin&&!req.user?.isEmployee?next():res.status(403).json({success:false,error:'Owner access is required.'});
  const protect=[authenticateToken,ownerOnly];
  const wrap=handler=>async(req,res)=>{try{res.set?.('Cache-Control','no-store');await handler(req,res);}catch(error){serverError(res,error,'Operations review is unavailable');}};
  const windowFor=req=>({startDate:req.query.start_date,endDate:req.query.end_date});
  router.get('/api/homeworks/operations',...protect,wrap(async(req,res)=>res.json(await provider.report(context,windowFor(req)))));
  router.post('/api/homeworks/operations/refresh',...protect,wrap(async(req,res)=>res.json(await provider.report(context,{startDate:req.body.startDate,endDate:req.body.endDate},true))));
  router.post('/api/homeworks/operations/review',...protect,wrap(async(req,res)=>res.json(await provider.reviewAction(context,req.body,req.user.id||req.user.email))));
  router.get('/api/homeworks/operations/pricing',...protect,wrap(async(req,res)=>res.json(provider.workflows.pricing(await provider.dataset(context),req.query))));
  router.get('/api/homeworks/operations/renewals',...protect,wrap(async(req,res)=>res.json(provider.workflows.renewalCandidates(await provider.dataset(context),req.query))));
  router.post('/api/homeworks/operations/renewal-draft',...protect,wrap(async(req,res)=>res.json(await provider.prepareRenewal(context,req.body,req.user.id||req.user.email))));
  router.post('/api/homeworks/operations/followup-draft',...protect,wrap(async(req,res)=>res.json(await provider.workflows.followupDraft(context,req.body,generateJson))));
  router.post('/api/homeworks/operations/save-followup',...protect,wrap(async(req,res)=>{
    if(!req.body.reviewed||!String(req.body.body||'').trim())return res.status(400).json({success:false,error:'Review the completed draft before saving it.'});
    const fresh=await provider.workflows.followupDraft(context,req.body);
    if(fresh.fingerprint!==req.body.fingerprint)throw new Error('The conversation or account changed. Prepare and review a new draft.');
    const body=String(req.body.body).slice(0,6000),id='followup:'+checks.hash([fresh.customerId,fresh.kind,fresh.fingerprint,body]).slice(0,32);
    res.json(await provider.storage.saveDraft(pool,{id,kind:'followup',customerId:fresh.customerId,fingerprint:fresh.fingerprint,payload:{...fresh,body,context:undefined,reviewed:true,saved:true}},req.user.id||req.user.email));
  }));
  router.get('/api/homeworks/operations/drafts',...protect,wrap(async(req,res)=>res.json({success:true,drafts:await provider.storage.listDrafts(pool),localOnly:true})));
  router.post('/api/homeworks/operations/route-preview',...protect,wrap(async(req,res)=>res.json(await provider.workflows.previewRoute(context,req.body))));
  if(warmup) {
    const timer=setTimeout(async()=>{
      try {
        const report=await provider.report(context);
        const unavailable=Object.entries(report.availability).filter(([,source])=>!source.available).map(([name])=>name);
        console.info('[OperationsAssistant] Read-only data check '+JSON.stringify({asOf:report.asOf,customers:report.choices.customers.length,properties:report.choices.properties.length,todayVisits:report.briefing.visits,findings:report.actions.length,currentChecks:report.openActionCount,categories:report.openActionCounts,matchedSeparateVisits:report.sections.scheduling.matchedOutsideSeries?.length||0,reviewStorageAvailable:report.reviewStorageAvailable,unavailable}));
      }catch(error){console.error('[OperationsAssistant] Data check unavailable: '+error.message);}
    },6000);
    timer.unref?.();
  }
  return router;
}
module.exports=createOperationsRoutes;
