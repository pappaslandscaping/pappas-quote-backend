const fs=require('node:fs'),vm=require('node:vm');
const workflows=require('../services/homeworks/operations-workflows');
const records=require('../services/homeworks/records');
const createRoutes=require('../routes/homeworks-operations');
afterEach(()=>jest.restoreAllMocks());
const owner={id:9,firstName:'Jane',fullName:'Jane Smith',email:'jane@example.com',cell:'2165550100',portalKey:'portal-key',credit:'0',outstanding:'108'};
function followupContext({incoming=true,outbound=false,matched=true,invoices=[],changed=false}={}){
  jest.spyOn(records,'uniqueCustomer').mockResolvedValue(owner);
  const sms=incoming?[{id:1,sid:'SM1',direction:'IN',text:'Question',sentAt:'2026-11-18T12:00:00Z'}]:[];
  if(outbound)sms.unshift({id:2,sid:'SM2',direction:'OUT',text:'Already replied',sentAt:'2026-11-18T13:00:00Z'});
  const pool={query:jest.fn(async sql=>({rows:sql.includes('homeworks_phone_matches')?(matched?[{homeworks_customer_id:9}]:[]):[]}))};
  const queryImpl=jest.fn(async()=>({smsMessages:sms,estimates:[],invoices}));
  return {pool,queryImpl};
}
test('unverified phone matches prevent AI from drafting as if the whole conversation was checked',async()=>{
  const context=followupContext({matched:false}),generate=jest.fn();
  const result=await workflows.followupDraft(context,{customerId:9,kind:'reply'},generate);
  expect(generate).not.toHaveBeenCalled();expect(result.sources.yarddesk).toBe('unavailable');expect(result.blockers.length).toBeGreaterThan(0);expect(result.sent).toBe(false);
});
test('an existing later reply is detected before generating another',async()=>{
  const context=followupContext({outbound:true}),generate=jest.fn();
  const result=await workflows.followupDraft(context,{customerId:9},generate);
  expect(generate).not.toHaveBeenCalled();expect(result.blockers.join(' ')).toContain('already outbound');
});
test('invoice follow-up uses only sent source balances and verified client links',async()=>{
  const context=followupContext({invoices:[{id:7,number:100,total:'108',paidAmount:'8',isSent:true},{id:8,number:101,total:'999',paidAmount:'0',isSent:false}]});
  const result=await workflows.followupDraft(context,{customerId:9,kind:'invoice'});
  expect(result.body).toContain('Invoice #100: $100.00');expect(result.body).toContain('/client/invoices/view/7/portal-key');expect(result.body).not.toContain('999');expect(result.body).not.toContain('/finances/');
});
test('an AI provider failure keeps a factual template and explicitly identifies the fallback',async()=>{
  const result=await workflows.followupDraft(followupContext(),{customerId:9},async()=>{throw new Error('Unavailable');});
  expect(result.method).toBe('verified template');expect(result.aiError).toContain('unavailable');expect(result.body).toContain('thank you for your message');
});
test('generated unsupported amounts and action claims are rejected',()=>{
  const context={invoices:[{balance:100,total:108}],estimates:[]};
  expect(()=>workflows.validateAiDraft('Your balance is $999.',context)).toThrow('unsupported');
  expect(()=>workflows.validateAiDraft('We have skipped your visit.',context)).toThrow('unverified action');
  expect(()=>workflows.validateAiDraft('Your balance due is $100.00.',context)).not.toThrow();
});
test('AI text is returned as an unsent draft with untrusted content isolated from instructions',async()=>{
  const generate=jest.fn(async()=>({json:{body:'Hi Jane, could you confirm which date you mean?',missingDetails:['Which visit date?']}}));
  const result=await workflows.followupDraft(followupContext(),{customerId:9},generate);
  expect(result).toMatchObject({sent:false,saved:false,method:'AI draft from verified context'});expect(generate.mock.calls[0][0].systemPrompt).toContain('untrusted evidence');expect(result.blockers).toContain('Which visit date?');
});
test('all operations endpoints have authentication and owner-only protection',()=>{
  const auth=jest.fn(),router=createRoutes({pool:{},authenticateToken:auth,serverError:jest.fn()});
  expect(router.stack.length).toBeGreaterThanOrEqual(10);
  for(const layer of router.stack){expect(layer.route.stack[0].handle).toBe(auth);const res={status:jest.fn().mockReturnThis(),json:jest.fn()},next=jest.fn();layer.route.stack[1].handle({user:{isAdmin:true,isEmployee:true}},res,next);expect(res.status).toHaveBeenCalledWith(403);expect(next).not.toHaveBeenCalled();}
});
test('saving a follow-up rejects a stale account/conversation fingerprint',async()=>{
  const provider={workflows:{followupDraft:jest.fn(async()=>({fingerprint:'fresh'}))},storage:{saveDraft:jest.fn()}};
  const error=jest.fn(),router=createRoutes({pool:{},authenticateToken:jest.fn(),serverError:error,provider});
  const handler=router.stack.find(l=>l.route.path.endsWith('/save-followup')).route.stack[2].handle;
  await handler({body:{reviewed:true,body:'Hi',fingerprint:'old'},user:{id:1}}, {json:jest.fn()});
  expect(error.mock.calls[0][1].message).toContain('changed');expect(provider.storage.saveDraft).not.toHaveBeenCalled();
});
test('saving an unreviewed draft is blocked before fetching or writing',async()=>{
  const provider={workflows:{followupDraft:jest.fn()},storage:{saveDraft:jest.fn()}};
  const router=createRoutes({pool:{},authenticateToken:jest.fn(),serverError:jest.fn(),provider}),res={status:jest.fn().mockReturnThis(),json:jest.fn()};
  const handler=router.stack.find(l=>l.route.path.endsWith('/save-followup')).route.stack[2].handle;
  await handler({body:{body:'Hi',reviewed:false},user:{id:1}},res);
  expect(res.status).toHaveBeenCalledWith(400);expect(provider.workflows.followupDraft).not.toHaveBeenCalled();
});
test('route efficiency uses query-only previews and preserves the first source stop',async()=>{
  const queryImpl=jest.fn(async request=>request.operationName==='YardDeskRouteState'?{crews:[{id:2,name:'Tim'}],events:[{id:1,type:'VISIT',crewId:2,status:'OPEN',startDate:'2026-11-18',sortOrder:2},{id:2,type:'VISIT',crewId:2,status:'OPEN',startDate:'2026-11-18',sortOrder:1}],routes:[{id:5,crewId:2,pinnedFirstEventId:1,routeStops:[{eventId:1,stopOrder:1},{eventId:2,stopOrder:2}]}]}:{routePreview:{routeStops:[{eventId:1},{eventId:2}]}});
  const result=await workflows.previewRoute({queryImpl},{date:'2026-11-18',crewName:'Tim'});
  expect(result.saved).toBe(false);expect(result.pinnedFirstEventId).toBe(1);
  const calls=queryImpl.mock.calls.map(([r])=>r);expect(calls.every(r=>!r.query.includes('mutation '))).toBe(true);expect(calls[1].variables.input.eventIds).toEqual([1,2]);expect(calls[2].variables.optimize).toBe(true);
});
test('a stale asynchronous draft response cannot restore a preview after inputs change',async()=>{
  const script=fs.readFileSync(require.resolve('../public/js/operations.js'),'utf8'),start=script.indexOf('function invalidateDraft()'),end=script.indexOf("$('save-followup').addEventListener",start);
  const elements=new Map(),element=id=>{if(!elements.has(id))elements.set(id,{value:id==='followup-customer'?'9':'',hidden:false,checked:true,disabled:false,addEventListener(type,fn){this[type]=fn;}});return elements.get(id);};
  let resolve;const pending=new Promise(done=>{resolve=done;});
  const context=vm.createContext({$:element,api:()=>pending,esc:String,safeLink:()=>'',Date,draftGeneration:0,currentDraft:null,draftBusy:false});
  vm.runInContext(script.slice(start,end),context);
  const task=element('followup-form').submit({preventDefault(){}});context.invalidateDraft();
  resolve({body:'Old response',references:[],sources:{},blockers:[],context:{messages:[]}});await task;
  expect(element('draft-preview').hidden).toBe(true);expect(context.currentDraft).toBeNull();expect(element('draft-reviewed').checked).toBe(false);
});
