const fs=require('node:fs'),vm=require('node:vm');
const cases=require('../services/homeworks/operations-cases');
const customer=require('../services/homeworks/operations-customer');
const records=require('../services/homeworks/records');
const store=require('../services/homeworks/operations-store');
const ops=require('../services/homeworks/operations');
const source=require('../services/homeworks/operations-source');
const today='2026-11-18';
const item=(id,customerId,kind,extra={})=>({id,customerId,kind,customerName:'Customer '+customerId,reviewState:'open',audience:'office',title:kind,fingerprint:'fp-'+id,evidence:{},...extra});
const event={id:31,type:'VISIT',customerId:9,propertyId:90,title:'Mowing',startDate:today,status:'OPEN',crewId:2,isInvoiceable:false,invoiceId:null,updatedAt:today+'T12:00:00Z'};
const decision=(extra={})=>({customerId:9,eventId:31,kind:'skip',reason:'Customer requested a skip',fingerprint:customer.visitFingerprint(event),confirmed:true,...extra});
function contextWithVisit(initial=event,{changeSecondRead=false}={}) {
  let current={...initial},reads=0;
  const queryImpl=jest.fn(async request=>{
    if(request.operationName==='YardDeskVisit'){reads++;return {event:changeSecondRead&&reads===2?{...current,status:'CLOSED'}:{...current}};}
    if(request.operationName==='YardDeskVisitStatus'){current.status=request.query.includes('skipEvent(')?'SKIPPED':'CLOSED';return {};}
    if(request.operationName==='YardDeskVisitUpdate'){current.startDate=request.variables.value;return {};}
    throw new Error('Unexpected operation '+request.operationName);
  });
  return {queryImpl};
}
const mutations=context=>context.queryImpl.mock.calls.filter(([r])=>r.query.includes('mutation '));
afterEach(()=>jest.restoreAllMocks());
beforeEach(()=>jest.spyOn(records,'dateKey').mockReturnValue(today));
test('customer triage groups related checks and keeps routine statuses out of priorities',()=>{
  const result=cases.buildCases([item('a',1,'unsent_invoice'),item('b',1,'accepted_unscheduled'),item('c',2,'duplicate_visit'),item('d',2,'past_open'),item('e',3,'reply_needed')],today);
  expect(result.cases.map(c=>c.customerId)).toEqual([3,2,1]);expect(result.cases[1].items).toHaveLength(2);
  expect(result).toMatchObject({priorityCustomers:2,routineCustomers:1});expect(result.cases[2].routine).toBe(true);
});
test('history, reviewed, snoozed, and unlinked records do not inflate today customer counts',()=>{
  const result=cases.buildCases([item('a',1,'past_open',{audience:'history'}),item('b',2,'past_open',{reviewState:'reviewed'}),item('c',3,'past_open',{reviewState:'snoozed'}),item('d',null,'past_open')],today);
  expect(result.cases).toEqual([]);
});
test('a routine customer becomes actionable when a real conversation is present',()=>{
  const result=cases.buildCases([item('a',1,'unsent_invoice'),item('b',1,'reply_needed')],today);
  expect(result.cases[0]).toMatchObject({routine:false,summary:'reply_needed',topics:['replies','billing']});
});
test.each([
  [{confirmed:false},'confirm'],[{fingerprint:'old'},'changed'],[{customerId:10},'belong'],[{kind:'delete'},'Choose'],[{reason:''},'reason'],[{kind:'reschedule',date:'2026-02-30'},'valid'],
])('visit decisions reject invalid input before a source mutation: %j',async(extra,message)=>{
  const context=contextWithVisit();await expect(customer.applyVisitDecision(context,decision(extra))).rejects.toThrow(message);expect(mutations(context)).toHaveLength(0);
});
test('a change during the second source read blocks the write',async()=>{
  const context=contextWithVisit(event,{changeSecondRead:true});await expect(customer.applyVisitDecision(context,decision())).rejects.toThrow('during review');expect(mutations(context)).toHaveLength(0);
});
test.each([{status:'CLOSED'},{invoiceId:44},{startDate:'2026-11-19'}])('closed, linked, and future completion protections use fresh data: %j',async(extra)=>{
  const fresh={...event,...extra},context=contextWithVisit(fresh);
  await expect(customer.applyVisitDecision(context,decision({kind:'complete',fingerprint:customer.visitFingerprint(fresh)}))).rejects.toThrow();expect(mutations(context)).toHaveLength(0);
});
test('completing an invoiceable visit requires separate billing consent',async()=>{
  const fresh={...event,isInvoiceable:true},context=contextWithVisit(fresh);
  await expect(customer.applyVisitDecision(context,decision({kind:'complete',fingerprint:customer.visitFingerprint(fresh)}))).rejects.toThrow('create a Homeworks invoice');expect(mutations(context)).toHaveLength(0);
  const result=await customer.applyVisitDecision(context,decision({kind:'complete',fingerprint:customer.visitFingerprint(fresh),confirmBilling:true}));
  expect(result.billingPending).toBe(true);expect(mutations(context)).toHaveLength(1);expect(mutations(context)[0][0].query).toContain('closeEvent(');
});
test('a confirmed skip uses the canonical Homeworks ID and verifies source readback',async()=>{
  const context=contextWithVisit(),result=await customer.applyVisitDecision(context,decision());
  expect(result.job.homeworks_status).toBe('SKIPPED');expect(mutations(context)).toHaveLength(1);expect(mutations(context)[0][0].variables).toEqual({id:31,reason:'Customer requested a skip'});
});
test('a date change edits one existing visit and verifies the new date',async()=>{
  const context=contextWithVisit(),result=await customer.applyVisitDecision(context,decision({kind:'reschedule',date:'2026-11-20'}));
  expect(result.job.job_date).toBe('2026-11-20');expect(mutations(context)).toHaveLength(1);expect(mutations(context)[0][0].query).toContain('scheduleEvent(');expect(context.queryImpl.mock.calls.some(([r])=>r.query.includes('createEvent'))).toBe(false);
});
test('customer context isolates source failures and never mistakes unavailable messages for verified history',async()=>{
  jest.spyOn(records,'uniqueCustomer').mockResolvedValue({id:9,fullName:'Jane',outstanding:'100',pastDue:'0',credit:'0'});
  const queryImpl=jest.fn(async request=>{const name=request.query.match(/\n\s+(\w+)\(where:/)[1];if(name==='smsMessages')throw new Error('Message service unavailable');return {[name]:name==='events'?[event]:[]};});
  const result=await customer.customerWorkspace({queryImpl},{customerId:9});
  expect(result.availability.visits.available).toBe(true);expect(result.availability.messages.available).toBe(false);expect(result.messageSources.homeworks).toBe('unavailable');expect(result.visits[0].fingerprint).toBe(customer.visitFingerprint(event));expect(mutations({queryImpl})).toHaveLength(0);
  expect(queryImpl.mock.calls.every(([r])=>r.variables.where.customerId.equals===9)).toBe(true);
});
test('review batches roll back together when any write fails',async()=>{
  const client={query:jest.fn(async sql=>{if(sql.startsWith('INSERT'))throw new Error('Storage unavailable');return {rows:[]};}),release:jest.fn()},pool={query:jest.fn(async()=>({rows:[]})),connect:jest.fn(async()=>client)};
  await expect(store.saveReviewBatch(pool,[item('a',9,'past_open')],{state:'reviewed',note:'Verified intentional'},'owner')).rejects.toThrow('Storage unavailable');
  expect(client.query.mock.calls.map(c=>c[0])).toContain('ROLLBACK');expect(client.query.mock.calls.map(c=>c[0])).not.toContain('COMMIT');expect(client.release).toHaveBeenCalled();
});
test('snoozes reopen on the chosen return date',async()=>{
  const a=item('a',9,'past_open'),pool={query:jest.fn(async sql=>({rows:sql.startsWith('SELECT')?[{action_id:a.id,fingerprint:a.fingerprint,state:'snoozed',snoozed_until:today}]:[]}))};
  expect((await store.reviews(pool,[a],today))[0].reviewState).toBe('open');
});
test('customer review validates every selected source fingerprint before any batch write',async()=>{
  jest.spyOn(source,'load').mockResolvedValue({customers:[{id:9,fullName:'Jane',status:'ACTIVE'}],properties:[],events:[{...event,startDate:'2026-11-17'}],recurring:[],invoices:[],estimates:[],sms:[],officeSms:[],crews:[],routes:[],templates:[],agreements:{quotes:[],ledger:[]},range:source.period({today}),availability:{}});
  const batch=jest.spyOn(store,'saveReviewBatch'),pool={query:jest.fn(async()=>({rows:[]}))},a=(await ops.report({pool})).actions.find(a=>a.kind==='past_open');
  await expect(ops.reviewCase({pool},{customerId:10,items:[{id:a.id,fingerprint:a.fingerprint}]},'owner')).rejects.toThrow('changed');
  await expect(ops.reviewCase({pool},{customerId:9,items:[{id:a.id,fingerprint:'old'}]},'owner')).rejects.toThrow('changed');expect(batch).not.toHaveBeenCalled();
});
function browserHarness(api) {
  const elements=new Map(),element=id=>{if(!elements.has(id))elements.set(id,{value:'',hidden:false,checked:false,disabled:false,open:false,dataset:{},handlers:{},addEventListener(type,fn){this.handlers[type]=fn;},querySelectorAll(){return [];},setAttribute(){},removeAttribute(){},showModal(){this.open=true;},scrollIntoView(){},insertAdjacentHTML(){}});return elements.get(id);};
  const document={handlers:{},querySelectorAll:()=>[],addEventListener(type,fn){this.handlers[type]=fn;}},capture={};
  const script=fs.readFileSync(require.resolve('../public/js/operations-workspace.js'),'utf8').replace('  if(report)renderCases();','  Object.assign(capture,{openCase,invalidateCaseDraft,setCustomer(data){caseData=data;caseId=data.customer.id;}});');
  vm.runInNewContext(script,{$:element,document,report:{actions:[],choices:{customers:[],properties:[]},workspace:{cases:[]},range:{today}},api,esc:String,money:String,table:()=>'',safeLink:()=>'',chooseTab:()=>{},nameFor:()=>'',refresh:async()=>{},capture,Date});
  return {element,document,capture};
}
test('closing or replacing a customer prevents stale context from overwriting the workspace',async()=>{
  let resolve;const h=browserHarness(()=>new Promise(done=>resolve=done)),task=h.capture.openCase(9);
  h.element('customer-workspace').open=false;h.element('customer-workspace').handlers.close();resolve({customer:{name:'Old customer'}});await task;
  expect(h.element('case-title').textContent).not.toBe('Old customer');
});
test('an in-flight draft cannot restore review approval after instructions change',async()=>{
  let resolve;const h=browserHarness(()=>new Promise(done=>resolve=done));h.capture.setCustomer({customer:{id:9}});h.element('customer-workspace').open=true;
  const task=h.element('case-draft-form').handlers.submit({preventDefault(){}});h.capture.invalidateCaseDraft();resolve({body:'Old draft',blockers:[],references:[]});await task;
  expect(h.element('case-draft-preview').hidden).toBe(true);expect(h.element('case-draft-reviewed').checked).toBe(false);expect(h.element('save-case-draft').disabled).toBe(true);
});
