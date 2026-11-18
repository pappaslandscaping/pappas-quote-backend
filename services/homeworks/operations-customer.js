'use strict';
const records=require('./records'),jobs=require('./jobs'),checks=require('./operations-checks');
const {shift,validDate,ESTIMATE_FIELDS}=require('./operations-source');
const {loadUnifiedMessages}=require('./unified-messages');
const visitFingerprint=event=>checks.hash([event.id,event.customerId,event.propertyId,event.title,event.startDate,event.status,event.crewId,event.isInvoiceable,event.invoiceId,event.updatedAt]);
async function customerWorkspace(context,input) {
  const customerId=records.id(input.customerId),owner=await records.uniqueCustomer(context,{customerId}),today=records.dateKey();
  const range={startDate:shift(today,-35),endDate:shift(today,14)},availability={},data={};
  const queries={
    properties:{entity:'properties',filterType:'PropertyFilter',where:{customerId:{equals:customerId},deletedStatus:'ACTIVE'},fields:'id customerId name address{street1 city state zip}',pageSize:100,maxRows:500},
    visits:{entity:'events',filterType:'EventFilter',where:{customerId:{equals:customerId},isDeleted:false,type:{equals:'VISIT'},startDate:{gte:range.startDate,lte:range.endDate}},fields:jobs.fields,pageSize:100,maxRows:2000},
    invoices:{entity:'invoices',filterType:'InvoiceFilter',where:{customerId:{equals:customerId},isArchived:false,deletedState:{equals:'ACTIVE'}},fields:'id number customerId date status isSent total paidAmount updatedAt lineItems{id name date price quantity subtotal propertyId}',pageSize:100,maxRows:1000},
    estimates:{entity:'estimates',filterType:'EstimateFilter',where:{customerId:{equals:customerId},isDeleted:false,isArchived:false,date:{gte:shift(today,-550)}},fields:ESTIMATE_FIELDS,pageSize:100,maxRows:1000},
    messages:{entity:'smsMessages',filterType:'SmsFilter',where:{customerId:{equals:customerId},sentAt:{gte:shift(today,-30)+'T00:00:00Z'}},fields:'id sid text direction status sentAt',pageSize:100,maxRows:1000},
  };
  // Isolated reads keep a failed message source from hiding verified visits.
  const names=Object.keys(queries);let index=0;
  await Promise.all(Array.from({length:2},async()=>{while(index<names.length){const name=names[index++];try{data[name]=await records.listAll(context,queries[name]);if(data[name].some(row=>Array.isArray(row.lineItems)&&row.lineItems.length>=1000))throw new Error('Service lines exceed the verified limit.');availability[name]={available:true};}catch(error){data[name]=[];availability[name]={available:false,error:error.message};}}}));
  const unified=await loadUnifiedMessages({pool:context.pool,customer:owner,sms:data.messages,homeworksError:availability.messages.available?null:availability.messages.error});
  return {success:true,source:records.SOURCE,checkedAt:new Date().toISOString(),range,customer:{id:owner.id,name:owner.fullName,email:owner.email,phone:owner.cell||owner.phone,outstanding:owner.outstanding,pastDue:owner.pastDue,credit:owner.credit,url:checks.sourceLink('customer',owner.id)},availability,
    properties:data.properties,visits:data.visits.map(event=>({...event,fingerprint:visitFingerprint(event)})).sort((a,b)=>a.startDate.localeCompare(b.startDate)||a.id-b.id),
    invoices:data.invoices.filter(row=>row.date>=shift(today,-120)||['PENDING','PARTIALLY_PAID','PAST_DUE'].includes(row.status)).map(row=>({...row,balance:Math.max(0,records.cents(row.total)-records.cents(row.paidAmount))/100,url:checks.sourceLink('invoice',row.id)})).sort((a,b)=>b.date.localeCompare(a.date)),
    estimates:data.estimates.map(row=>({...row,url:checks.sourceLink('estimate',row.id)})).sort((a,b)=>b.date.localeCompare(a.date)),messages:unified.messages,messageSources:unified.sources,messageWarning:unified.error||null,messagesLimited:unified.limited};
}
async function applyVisitDecision(context,input) {
  if(input.confirmed!==true)throw new Error('Review and confirm the visit change first.');
  const customerId=records.id(input.customerId),eventId=records.id(input.eventId),key='homeworks:'+eventId;
  const event=await jobs.detail(context,key);
  if(event.customerId!==customerId||event.type!=='VISIT')throw new Error('This visit does not belong to the selected customer.');
  if(!input.fingerprint||visitFingerprint(event)!==input.fingerprint)throw new Error('This visit changed. Reload the customer and review it again.');
  if(event.status!=='OPEN'||event.invoiceId)throw new Error('Only open, uninvoiced visits can be changed here. Open the source for this visit.');
  const kind=input.kind;
  if(!['skip','reschedule','complete'].includes(kind))throw new Error('Choose skip, reschedule, or complete.');
  if(kind==='skip'&&!String(input.reason||'').trim())throw new Error('Enter the reason for this skip.');
  if(kind==='reschedule'&&(!validDate(input.date)||input.date===event.startDate))throw new Error('Choose a different valid visit date.');
  if(kind==='complete'&&event.startDate>records.dateKey())throw new Error('Future visits cannot be completed here.');
  if(kind==='complete'&&event.isInvoiceable&&input.confirmBilling!==true)throw new Error('Confirm that completing this visit can create a Homeworks invoice.');
  // The shared job service re-reads the event immediately before its official API write.
  // Guard that second read as well, so evidence changed during preflight cannot be applied.
  const guarded={...context,queryImpl:async request=>{const data=await records.call(context,request.operationName,request.query,request.variables);if(request.operationName==='YardDeskVisit'&&data.event&&visitFingerprint(data.event)!==input.fingerprint)throw new Error('This visit changed during review. Reload before making a decision.');return data;}};
  // Readback after mutation must allow the expected change; guard only the first job-service read.
  let firstRead=true;const query=guarded.queryImpl;
  guarded.queryImpl=async request=>{if(request.operationName==='YardDeskVisit'&&firstRead){firstRead=false;return query(request);}return records.call(context,request.operationName,request.query,request.variables);};
  const result=kind==='reschedule'?await jobs.update(guarded,key,{job_date:input.date}):await jobs.changeStatus(guarded,key,kind==='skip'?'skipped':'completed',{reason:String(input.reason||'').trim().slice(0,1500),confirm_billing:input.confirmBilling===true});
  return {...result,decision:kind};
}
module.exports={customerWorkspace,visitFingerprint,applyVisitDecision};
