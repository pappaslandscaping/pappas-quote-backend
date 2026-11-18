const checks=require('../services/homeworks/operations-checks');
const ops=require('../services/homeworks/operations');
const fs=require('node:fs'),vm=require('node:vm');
const range={today:'2026-11-18',startDate:'2026-10-21',endDate:'2026-12-16',billingStart:'2026-08-20',estimateStart:'2025-05-17',season:2026};
const series={id:165510,title:'Mowing (Bi-Weekly)',customerId:1730909,propertyId:1605472,crewId:2221,startDate:'2025-06-04',endDate:'2029-10-10',isInfinite:true,frequency:'WEEKLY',weeklyCount:2,weekday3:true,lineItems:[{itemId:110508,name:null}]};
const completed={id:60140584,title:'Mowing (Bi-Weekly)',customerId:1730909,propertyId:1605472,crewId:2221,recurringEventId:null,startDate:'2026-10-21',status:'CLOSED',lineItems:[{itemId:110508,name:'Mowing (Bi-Weekly)'}]};
function fixture(overrides={}){return {range,customers:[{id:1730909,fullName:'Murello Properties',status:'ACTIVE'}],properties:[],recurring:[series],events:[completed,{...completed,id:2,recurringEventId:165510,startDate:'2026-10-28',status:'SKIPPED'},{...completed,id:3,recurringEventId:165510,startDate:'2026-11-05'},{...completed,id:4,recurringEventId:165510,startDate:'2026-11-18',status:'OPEN'}],invoices:[],estimates:[],sms:[],officeSms:[],crews:[],routes:[],templates:[],agreements:{quotes:[],ledger:[]},availability:{},...overrides};}
test('the real Murello October 21 separate completion satisfies its customer/property/service slot',()=>{
  const result=checks.scheduling(fixture());
  expect(result.matchedOutsideSeries).toContainEqual({seriesId:165510,date:'2026-10-21',eventId:60140584,status:'CLOSED'});
  expect(result.items.filter(i=>i.kind==='missing_visit').flatMap(i=>i.evidence.dates)).not.toContain('2026-10-21');
});
test('another property cannot satisfy the missing service slot',()=>{
  const d=fixture();d.events[0]={...completed,propertyId:1605470};const result=checks.scheduling(d);
  expect(result.items.filter(i=>i.kind==='missing_visit').flatMap(i=>i.evidence.dates)).toContain('2026-10-21');
});
test('another customer cannot satisfy the missing slot even with the same property ID',()=>{
  const d=fixture();d.events[0]={...completed,customerId:99};const result=checks.scheduling(d);
  expect(result.items.filter(i=>i.kind==='missing_visit').flatMap(i=>i.evidence.dates)).toContain('2026-10-21');
});
test('a different catalog service is not mistaken for the expected mowing',()=>{
  const d=fixture();d.events[0]={...completed,lineItems:[{itemId:999,name:'Cleanup'}]};const result=checks.scheduling(d);
  expect(result.items.filter(i=>i.kind==='missing_visit').flatMap(i=>i.evidence.dates)).toContain('2026-10-21');
});
test('a cancelled separate visit does not prove the recurring service was performed or intentionally skipped',()=>{
  const d=fixture();d.events[0]={...completed,status:'CANCELLED'};const result=checks.scheduling(d);
  expect(result.items.filter(i=>i.kind==='missing_visit').flatMap(i=>i.evidence.dates)).toContain('2026-10-21');
});
test('a separate waitlisted record is not treated as a scheduled or completed visit',()=>{
  const d=fixture();d.events[0]={...completed,status:'WAITLISTED'};const result=checks.scheduling(d);
  expect(result.items.filter(i=>i.kind==='missing_visit').flatMap(i=>i.evidence.dates)).toContain('2026-10-21');
});
test('the older weekly Murello rule becomes one setup review rather than many missed visits',()=>{
  const old={...series,id:165508,title:'Mowing',weeklyCount:1,lineItems:[{itemId:110482}]};
  const result=checks.scheduling(fixture({recurring:[old]}));
  expect(result.items.filter(i=>i.kind==='missing_visit')).toHaveLength(0);
  expect(result.items.filter(i=>i.kind==='recurring_setup')).toMatchObject([{audience:'setup',title:'Review overlapping recurring schedules'}]);
});
test('an older rule with no recent attached visits is one service-status question, not a list of missed cuts',()=>{
  const result=checks.scheduling(fixture({events:[]}));
  expect(result.items).toHaveLength(1);expect(result.items[0]).toMatchObject({kind:'recurring_setup',audience:'setup',severity:'normal'});
});
test('an inactive account is flagged for service confirmation without generating missed visits',()=>{
  const result=checks.scheduling(fixture({customers:[{id:1730909,status:'INACTIVE'}]}));
  expect(result.items.filter(i=>i.kind==='missing_visit')).toHaveLength(0);
  expect(result.items[0].evidence.customerStatus).toBe('INACTIVE');
});
test('remaining potential gaps are grouped by schedule/category and are not labeled priority mistakes',()=>{
  const result=checks.scheduling(fixture({events:[{...completed,recurringEventId:165510}]}));
  const gaps=result.items.filter(i=>i.kind==='missing_visit');expect(gaps.length).toBeLessThanOrEqual(3);
  expect(gaps.every(i=>i.severity==='normal')).toBe(true);expect(gaps.some(i=>i.evidence.dates.length>1)).toBe(true);
});
test('historical and setup checks do not inflate current briefing priorities',()=>{
  const result=ops.assemble(fixture({recurring:[{...series,frequency:'YEARLY'}],events:[]}));
  expect(result.actions).toHaveLength(1);expect(result.actions[0].audience).toBe('setup');
  expect(result.briefing.actionCount).toBe(0);expect(result.briefing.highPriority).toBe(0);
});
test('current office view hides historical/setup checks until the category is selected',()=>{
  const script=fs.readFileSync(require.resolve('../public/js/operations.js'),'utf8');
  const start=script.indexOf('function renderActions()'),end=script.indexOf('function render(){',start);
  const elements={};for(const id of ['action-search','action-state','action-kind','action-scope','actions','action-count','more-actions'])elements[id]={value:''};
  elements['action-state'].value='open';elements['action-kind'].value='all';elements['action-scope'].value='office';
  const context=vm.createContext({$:id=>elements[id],visibleActions:40,findings:items=>items.map(i=>i.title).join(','),report:{actions:[{title:'Current check',audience:'office',reviewState:'open'},{title:'Old date',audience:'history',reviewState:'open'},{title:'Old schedule',audience:'setup',reviewState:'open'}]}});
  vm.runInContext(script.slice(start,end),context);context.renderActions();
  expect(elements.actions.innerHTML).toBe('Current check');expect(elements['action-count'].textContent).toContain('1 review suggestions in this view');
  elements['action-scope'].value='setup';context.renderActions();expect(elements.actions.innerHTML).toBe('Old schedule');
});
