'use strict';
const source=require('./operations-source'), checks=require('./operations-checks'), workflows=require('./operations-workflows'), storage=require('./operations-store');
const caches=new WeakMap();
const CACHE_MS=5*60*1000;
function combinedMessages(data) {
  if(!data.sms)return null;
  const seen=new Map();
  for(const row of [...data.sms,...(data.officeSms||[])]) {
    const key=row.sid||String(row.id);
    if(!seen.has(key))seen.set(key,row);
  }
  return [...seen.values()];
}
function assemble(raw) {
  const data={...raw,sms:combinedMessages(raw)};
  const scheduling=checks.scheduling(data),billing=checks.billing(data),followups=checks.followups(data),snow=workflows.snowBoard(data),crew=checks.crewReport(data);
  const customers=new Map((data.customers||[]).map(c=>[c.id,c])),properties=new Map((data.properties||[]).map(p=>[p.id,p]));
  const sections={scheduling,billing,followups,snow,crew};
  const allActions=Object.values(sections).flatMap(s=>s.items||[]).map(item=>({...item,customerName:customers.get(item.customerId)?.fullName||'Customer unavailable',propertyName:properties.get(item.propertyId)?.name||'',reviewState:'open'}));
  allActions.sort((a,b)=>(a.severity==='high'?0:1)-(b.severity==='high'?0:1)||String(a.date||'9999').localeCompare(String(b.date||'9999'))||a.id.localeCompare(b.id));
  const today=checks.briefing(data,sections);
  const services=new Map();
  for(const e of data.estimates||[])for(const line of e.lineItems||[])if(line.isIncluded)services.set(line.itemId?'item:'+line.itemId:'name:'+line.name,{itemId:line.itemId,name:line.name});
  return {success:true,source:data.source,asOf:data.asOf,range:data.range,availability:data.availability,reviewOnly:true,briefing:today,sections,actions:allActions,
    choices:{crews:data.crews||[],customers:(data.customers||[]).map(c=>({id:c.id,name:c.fullName})).sort((a,b)=>a.name.localeCompare(b.name)),properties:(data.properties||[]).map(p=>({id:p.id,name:p.name,city:p.address?.city,customerId:p.customerId})),services:[...services.values()].sort((a,b)=>a.name.localeCompare(b.name))}};
}
async function dataset(context,input={},force=false) {
  if(!caches.has(context.pool))caches.set(context.pool,new Map());
  const cache=caches.get(context.pool), range=source.period(input), key=JSON.stringify([range.today,range.startDate,range.endDate]);
  const existing=cache.get(key);
  if(existing?.promise)return existing.promise;
  if(!force&&existing?.data&&Date.now()-existing.at<CACHE_MS)return existing.data;
  const promise=source.load(context,input).then(data=>{cache.set(key,{data,at:Date.now()});while(cache.size>5)cache.delete(cache.keys().next().value);return data;}).catch(error=>{cache.delete(key);throw error;});
  cache.set(key,{promise});return promise;
}
async function report(context,input={},force=false) {
  const data=await dataset(context,input,force),result=assemble(data);
  try {
    result.actions=await storage.reviews(context.pool,result.actions,result.range.today);
    result.reviewStorageAvailable=true;
  }catch(error){result.reviewStorageAvailable=false;result.reviewStorageError='Office review state is unavailable; source findings are still shown.';}
  result.openActionCount=result.actions.filter(a=>a.reviewState==='open').length;
  result.briefing.topActions=result.actions.filter(a=>a.reviewState==='open'&&a.severity==='high').slice(0,8);
  return result;
}
async function reviewAction(context,input,user) {
  const current=await report(context,input.range||{},true),item=current.actions.find(a=>a.id===input.id);
  if(!item||item.fingerprint!==input.fingerprint)throw new Error('This finding changed. Refresh and review the current evidence.');
  return storage.saveReview(context.pool,item,input,user);
}
async function prepareRenewal(context,input,user) {
  const data=await dataset(context,{},true),plan=workflows.renewalCandidates(data,input),candidate=plan.candidates.find(c=>c.id===input.candidateId);
  if(!candidate||candidate.sourceFingerprint!==input.sourceFingerprint)throw new Error('The renewal source changed. Refresh the candidates.');
  if(candidate.blockers.length)throw new Error(candidate.blockers.join(' '));
  return {...await storage.saveDraft(context.pool,{id:'renewal:'+candidate.id,kind:'renewal',customerId:candidate.customerId,fingerprint:candidate.sourceFingerprint,payload:candidate.payload},user),draft:candidate.payload};
}
module.exports={assemble,dataset,report,reviewAction,prepareRenewal,storage,workflows};
