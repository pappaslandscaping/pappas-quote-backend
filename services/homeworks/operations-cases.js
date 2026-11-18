'use strict';
// Customer-level triage is an office view, never proof that a source record is wrong.
const routineKinds=new Set(['unsent_invoice','accepted_unscheduled']);
const messageKinds=new Set(['reply_needed','failed_message','skip_request']);
const billingKinds=new Set(['unsent_invoice','unbilled_visit','duplicate_invoice','estimate_change','accepted_unscheduled']);
function topic(item) {return messageKinds.has(item.kind)?'replies':billingKinds.has(item.kind)?'billing':'schedule';}
function priority(item,today) {
  if(item.kind==='reply_needed'||item.kind==='failed_message')return 100;
  if(item.kind==='skip_request')return 95;
  if(item.kind==='duplicate_invoice')return 90;
  if(item.kind==='unassigned'&&item.date===today)return 90;
  if(item.kind==='duplicate_visit')return 85;
  if(item.kind==='past_open')return 75;
  if(item.kind==='estimate_change')return 70;
  if(item.kind==='missing_visit')return 65;
  if(item.kind==='crew_mismatch')return 55;
  return routineKinds.has(item.kind)?10:45;
}
function buildCases(actions,today) {
  const groups=new Map();
  for(const item of actions) {
    if(item.reviewState!=='open'||(item.audience||'office')!=='office'||!item.customerId)continue;
    if(!groups.has(item.customerId))groups.set(item.customerId,{id:'customer:'+item.customerId,customerId:item.customerId,name:item.customerName,items:[]});
    groups.get(item.customerId).items.push(item);
  }
  const cases=[...groups.values()].map(group=>{
    group.items.sort((a,b)=>priority(b,today)-priority(a,today)||String(a.date||'').localeCompare(String(b.date||''))||a.id.localeCompare(b.id));
    const lead=group.items[0],score=priority(lead,today);
    return {...group,score,routine:group.items.every(a=>routineKinds.has(a.kind)),topics:[...new Set(group.items.map(topic))],properties:[...new Set(group.items.map(a=>a.propertyName).filter(Boolean))],summary:lead.title,nextStep:topic(lead)==='replies'?'Read the conversation, then prepare a reply.':topic(lead)==='billing'?'Compare the current account and service records.':'Compare the visits before deciding what to change.'};
  }).sort((a,b)=>b.score-a.score||String(a.name).localeCompare(String(b.name))||a.customerId-b.customerId);
  return {cases,priorityCustomers:cases.filter(c=>!c.routine).length,routineCustomers:cases.filter(c=>c.routine).length};
}
module.exports={buildCases,topic,priority,routineKinds};
