const checks=require('../services/homeworks/operations-checks');
const workflows=require('../services/homeworks/operations-workflows');
const source=require('../services/homeworks/operations-source');
const ops=require('../services/homeworks/operations');
const records=require('../services/homeworks/records');
const store=require('../services/homeworks/operations-store');
const range=source.period({today:'2026-11-18',startDate:'2026-11-01',endDate:'2026-12-02'});
const series={id:1,title:'Biweekly mowing',customerId:9,propertyId:90,crewId:2,type:'VISIT',startDate:'2026-10-12',endDate:'2026-11-30',isInfinite:false,frequency:'WEEKLY',weeklyCount:2,weekday1:true};
const owner={id:9,fullName:'Jane Smith',firstName:'Jane',status:'ACTIVE',email:'jane@example.com',cell:'2165550100',portalKey:'portal-key'};
const line={id:101,name:'Snow Removal Contract',description:'Primary driveway; December 1, 2026 through March 31, 2027',itemId:5,propertyId:90,isIncluded:true,price:'625',quantity:'1',subtotal:'625',taxPercent1:'8',taxPercent2:'0',eventStatus:'NO_EVENT'};
const estimate={id:11,number:123,customerId:9,title:'2026-2027 Snow Removal',date:'2026-10-20',status:'ACCEPTED',total:'675',subtotal:'625',lineItems:[line]};
function data(overrides={}){return {events:[],recurring:[],invoices:[],sms:[],officeSms:[],customers:[owner],properties:[{id:90,customerId:9,name:'123 Main St',address:{city:'Lakewood'},pavedSize:'0'}],estimates:[estimate],crews:[{id:2,name:'Tim'}],routes:[],templates:[],agreements:{quotes:[],ledger:[]},range,asOf:'2026-11-18T14:00:00Z',availability:{},...overrides};}
afterEach(()=>jest.restoreAllMocks());
test('date windows reject impossible dates and excessive spans',()=>{
  expect(source.validDate('2026-99-99')).toBe(false);expect(source.validDate('2026-02-30')).toBe(false);
  expect(()=>source.period({today:'2026-11-18',startDate:'2026-01-01'})).toThrow('93 days');
});
test('biweekly parity follows the series anchor and ends with its season',()=>{
  expect(checks.expectedDates(series,'2026-11-01','2026-12-15').dates).toEqual(['2026-11-09','2026-11-23']);
});
test('off-seasons spanning the new year suppress expected visits',()=>{
  const s={...series,isInfinite:true,offSeason:{startMonth:11,startDay:15,endMonth:3,endDay:31}};
  expect(checks.expectedDates(s,'2026-11-01','2026-12-15').dates).toEqual(['2026-11-09']);
  expect(checks.inOffSeason('2027-01-02',s.offSeason)).toBe(true);
});
test('skipped and explicitly cancelled occurrences are not reported missing',()=>{
  const result=checks.scheduling(data({recurring:[series],events:[{id:1,recurringEventId:1,startDate:'2026-11-09',status:'SKIPPED'},{id:2,recurringEventId:1,startDate:'2026-11-23',status:'CANCELLED'}]}));
  expect(result.items.filter(a=>a.kind==='missing_visit')).toHaveLength(0);
});
test('a nearby moved occurrence is not reported as an extra missing visit',()=>{
  const result=checks.scheduling(data({recurring:[series],events:[{id:1,recurringEventId:1,startDate:'2026-11-10',status:'CLOSED'},{id:2,recurringEventId:1,startDate:'2026-11-23',status:'SKIPPED'}]}));
  expect(result.items.filter(a=>a.kind==='missing_visit')).toHaveLength(0);
});
test('unsupported monthly ordinal patterns remain explicit manual reviews',()=>{
  const result=checks.scheduling(data({recurring:[{...series,frequency:'MONTHLY',monthlyOption:'ON_WEEKDAY_OF_MONTH'}]}));
  expect(result.items.map(a=>a.kind)).toEqual(['schedule_pattern']);
});
test('a waitlisted recurring occurrence is flagged as unscheduled rather than silently counted as planned',()=>{
  const result=checks.scheduling(data({recurring:[series],events:[{id:1,recurringEventId:1,startDate:'2026-11-23',status:'WAITLISTED'}]}));
  expect(result.items.some(a=>a.kind==='waitlisted_recurring')).toBe(true);
});
test('ongoing multi-day visits count today and are not falsely overdue',()=>{
  const d=data({events:[{id:1,title:'Cleanup',crewId:2,status:'OPEN',startDate:'2026-11-17',endDate:'2026-11-20'}]});
  expect(checks.briefing(d,{}).visits).toBe(1);
  expect(checks.scheduling(d).items.some(a=>a.kind==='past_open')).toBe(false);
});
test('monthly field visits keep their chosen day and interval',()=>{
  expect(checks.expectedDates({...series,startDate:'2026-09-12',endDate:'2026-12-31',frequency:'MONTHLY',monthlyOption:'ON_DAY_OF_MONTH',monthCount1:1,monthDay:12},'2026-11-01','2026-12-15').dates).toEqual(['2026-11-12','2026-12-12']);
});
test('crew mismatches and duplicate visits are review candidates, without changes',()=>{
  const events=[1,2].map(id=>({id,recurringEventId:1,startDate:'2026-11-23',status:'OPEN',customerId:9,propertyId:90,title:'Mowing',crewId:3}));
  const result=checks.scheduling(data({recurring:[series],events}));
  expect(result.items.some(a=>a.kind==='crew_mismatch')).toBe(true);expect(result.items.some(a=>a.kind==='duplicate_visit')).toBe(true);
});
test('a message mentioning a skip is a review cue and does not mark visits skipped',()=>{
  const events=[{id:1,customerId:9,startDate:'2026-11-23',status:'OPEN',title:'Mowing'}];
  const result=checks.scheduling(data({events,sms:[{id:1,customerId:9,direction:'IN',text:'Skip next time',sentAt:'2026-11-18T12:00:00Z'}]}));
  expect(result.items.some(a=>a.kind==='skip_request')).toBe(true);expect(events[0].status).toBe('OPEN');
});
test('unbilled review excludes non-invoiceable seasonal work and already linked visits',()=>{
  const result=checks.billing(data({events:[{id:1,status:'CLOSED',isInvoiceable:false},{id:2,status:'CLOSED',isInvoiceable:true,invoiceId:22},{id:3,status:'CLOSED',isInvoiceable:true,subtotal:'40'}]}));
  expect(result.items.map(a=>a.evidence.eventId)).toEqual([3]);
});
test('invoice flags use the source total once and catch paid/unpaid duplicate candidates',()=>{
  const invoice={id:1,number:1,customerId:9,date:'2026-11-01',status:'PENDING',isSent:true,total:'108',paidAmount:'0',lineItems:[{name:'Mowing',date:'2026-10-20',price:'100',quantity:'1',propertyId:90}]};
  const result=checks.billing(data({invoices:[invoice,{...invoice,id:2,number:2,status:'PAID',paidAmount:'108'},{...invoice,id:3,number:3,isSent:false,paidAmount:'8'}]}));
  expect(result.items.find(a=>a.kind==='unsent_invoice').evidence.balance).toBe(100);
  expect(result.items.find(a=>a.kind==='duplicate_invoice').evidence.invoiceIds).toEqual([1,2]);
});
test('failed outbound texts do not hide the unanswered inbound message',()=>{
  const result=checks.followups(data({estimates:[],sms:[{id:1,customerId:9,direction:'IN',sentAt:'2026-11-18T11:00:00Z',text:'Question'},{id:2,customerId:9,direction:'OUT',status:'FAILED',sentAt:'2026-11-18T12:00:00Z',text:'Reply'}]}));
  expect(result.items.map(a=>a.kind).sort()).toEqual(['failed_message','reply_needed']);
});
test('accepted scheduling backlog respects inclusion and explicit event overrides',()=>{
  const result=checks.followups(data({estimates:[{...estimate,lineItems:[line,{...line,id:102,isIncluded:false},{...line,id:103,eventStatus:'EVENT_OVERRIDE'}]}]}));
  expect(result.items.filter(a=>a.kind==='accepted_unscheduled')).toHaveLength(1);
});
test('crew comparisons exclude running timers and unmatched budget coverage',()=>{
  const result=checks.crewReport(data({events:[{id:1,title:'Mowing',crewId:2,status:'CLOSED',startDate:'2026-11-17',subtotal:'40',budgetedHours:'1',timeEntryTotals:{pastTimeEntries:1,pastTimeEntrySeconds:7200,runningTimerSeconds:99999}},{id:2,crewId:2,status:'CLOSED',startDate:'2026-11-17',subtotal:'40',budgetedHours:'10',timeEntryTotals:{pastTimeEntries:0,pastTimeEntrySeconds:0}}]}));
  expect(result.rows[0]).toMatchObject({completed:2,timed:1,actualHours:2,varianceHours:1,serviceValue:80});
});
test('pricing uses service rates and won statuses, not whole estimate totals or excluded options',()=>{
  const estimates=[0,1,2].map((n)=>({...estimate,id:11+n,customerId:9+n,lineItems:[{...line,price:String(600+25*n)}]}));
  estimates.push({...estimate,id:20,customerId:20,status:'DECLINED',total:'9999',lineItems:[{...line,price:'900'}]},{...estimate,id:21,customerId:21,lineItems:[{...line,isIncluded:false,price:'1'}]});
  const result=workflows.pricing(data({estimates}),{itemId:5,city:'Lakewood',propertyId:90});
  expect(result.suggestion).toMatchObject({low:600,median:625,high:650,provisional:true});expect(result.declinedCount).toBe(1);expect(result.missingDetails.join(' ')).toContain('No positive paved-area');
});
test('pricing does not invent a reliable range from one matching quote',()=>{
  expect(workflows.pricing(data(),{service:line.name}).suggestion).toBeNull();
});
test('renewal proposals preserve included scope and show fractional quantities and explicit rate changes',()=>{
  const result=workflows.renewalCandidates(data({estimates:[{...estimate,lineItems:[{...line,quantity:'1.5'},{...line,id:102,isIncluded:false,price:'9999'}]}]}),{sourceYear:2026,targetYear:2027,increasePercent:8,service:'snow'});
  expect(result.candidates).toHaveLength(1);expect(result.candidates[0].payload).toMatchObject({subtotal:1012.5,lines:[{unitRate:675,quantity:1.5}]});
});
test('renewal proposals flag inactive owners and an existing target-year quote',()=>{
  const result=workflows.renewalCandidates(data({customers:[{...owner,status:'INACTIVE'}],estimates:[estimate,{...estimate,id:30,date:'2027-10-01',status:'PENDING'}]}),{sourceYear:2026,targetYear:2027,service:'snow'});
  expect(result.candidates[0].blockers).toHaveLength(2);
});
test('snow agreements require an exact estimate/customer match; ledger alone is not verified property media',()=>{
  const result=workflows.snowBoard(data({agreements:{quotes:[{id:1,quote_number:123,customer_email:owner.email,contract_signed_at:'2026-11-01'}],ledger:[{quote_id:1,property_id:90,status:'confirmed',media_id:55}]}}));
  expect(result.rows[0]).toMatchObject({agreementStatus:'Signed in YardDesk',attachmentStatus:'Upload recorded; open property to verify'});
  expect(result.issues.some(i=>i.type==='accepted_missing_dispatch')).toBe(true);
});
test('a missing customer does not match empty names in the signature ledger',()=>{
  const result=workflows.snowBoard(data({customers:[],agreements:{quotes:[{id:1,quote_number:123,contract_signed_at:'2026-11-01'}],ledger:[]}}));
  expect(result.rows[0].agreementStatus).toBe('No matched YardDesk agreement');
});
test('source failures remain unavailable, not successful zero findings',()=>{
  const result=ops.assemble(data({events:null,estimates:null}));expect(result.briefing.available).toBe(false);expect(result.sections.billing.available).toBe(false);expect(result.sections.followups.available).toBe(false);
});
test('messages with shared provider IDs are counted once in the office action list',()=>{
  const sms={id:1,sid:'SM1',customerId:9,direction:'IN',text:'Question',sentAt:'2026-11-18T11:00:00Z'};
  const result=ops.assemble(data({sms:[sms],officeSms:[{...sms,id:'yarddesk:1'}],estimates:[]}));expect(result.actions.filter(a=>a.kind==='reply_needed')).toHaveLength(1);
});
test('review approval is invalidated when source evidence changes, and expired snoozes reopen',async()=>{
  const one=checks.action('test','1','Test','Details',{value:1}),two=checks.action('test','2','Test','Details',{value:2});
  const pool={query:jest.fn(async sql=>({rows:sql.startsWith('SELECT')?[{action_id:one.id,fingerprint:'old',state:'reviewed'},{action_id:two.id,fingerprint:two.fingerprint,state:'snoozed',snoozed_until:new Date('2026-11-17T00:00:00Z')}]:[]}))};
  const result=await store.reviews(pool,[one,two],'2026-11-18');expect(result.map(a=>a.reviewState)).toEqual(['open','open']);
});
test('the source loader isolates a failing source and never issues mutations',async()=>{
  const queryImpl=jest.fn(async request=>{if(request.query.includes('events('))throw new Error('Source unavailable');if(request.operationName==='YardDeskOperationsRoutes')return {routeReport:[]};if(request.operationName==='YardDeskOperationsSnow')return {quickDispatchTemplates:[]};const name=request.query.match(/\n\s+(\w+)\(where:/)?.[1];return {[name]:[]};});
  const result=await source.load({queryImpl,pool:{query:jest.fn(async()=>({rows:[]}))}},{today:'2026-11-18'});
  expect(result.availability.events.available).toBe(false);expect(result.availability.customers.available).toBe(true);expect(queryImpl.mock.calls.every(([r])=>!r.query.includes('mutation '))).toBe(true);
});
test('recurring-series pagination uses the root API Int arguments while ordinary collections retain SafeInt',async()=>{
  const queryImpl=jest.fn(async()=>({recurringEvents:[]}));
  await records.listAll({queryImpl},{entity:'recurringEvents',filterType:'RecurringEventFilter',fields:'id',paginationType:'Int'});
  expect(queryImpl.mock.calls[0][0].query).toContain('$take: Int!, $skip: Int!');
  await expect(records.listAll({queryImpl},{entity:'recurringEvents',filterType:'RecurringEventFilter',fields:'id',paginationType:'String'})).rejects.toThrow('pagination type');
  const regular=jest.fn(async()=>({customers:[]}));await records.listAll({queryImpl:regular},{entity:'customers',filterType:'CustomerFilter',fields:'id'});
  expect(regular.mock.calls[0][0].query).toContain('$take: SafeInt!, $skip: SafeInt!');
});
