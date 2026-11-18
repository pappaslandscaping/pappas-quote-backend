'use strict';
(() => {
  let lane='priority',limit=5,caseId=null,caseData=null,caseTab='overview',loadGeneration=0,caseDraft=null,draftVersion=0,draftWorking=false,visitBusy=false;
  const dialog=$('customer-workspace');
  const labels={priority:'Customers with a conversation or operational detail to review first.',replies:'Read the complete conversation before replying or changing a visit.',schedule:'Review related dates together, then choose the right visit action.',billing:'Compare invoices and accepted services before billing or following up.',routine:'Unsent invoices and accepted services can be normal. Review these when you handle billing or seasonal setup.'};
  const routineKinds=new Set(['unsent_invoice','accepted_unscheduled']);
  const messageKinds=new Set(['reply_needed','failed_message','skip_request']);
  const topic=a=>messageKinds.has(a.kind)?'replies':['unsent_invoice','unbilled_visit','duplicate_invoice','estimate_change','accepted_unscheduled'].includes(a.kind)?'billing':'schedule';
  function availableCases(){return (report?.workspace?.cases||[]).map(c=>({...c,items:c.items.map(a=>report.actions.find(row=>row.id===a.id)).filter(a=>a?.reviewState==='open')})).filter(c=>c.items.length).map(c=>({...c,routine:c.items.every(a=>routineKinds.has(a.kind)),topics:[...new Set(c.items.map(topic))],summary:c.items[0].title}));}
  function currentCases(){return availableCases().filter(c=>lane==='priority'?!c.routine:lane==='routine'?c.routine:c.topics.includes(lane));}
  function renderCases(){
    const rows=currentCases();$('workspace-count').textContent=rows.length+' customer'+(rows.length===1?'':'s');$('workspace-description').textContent=labels[lane];
    document.querySelectorAll('[data-lane]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.lane===lane)));
    $('customer-cases').innerHTML=rows.slice(0,limit).map((c,index)=>'<article class="customer-case"><div class="case-number">'+(index+1)+'</div><div class="customer-case-copy"><h3>'+esc(c.name)+'</h3><p>'+esc(c.summary)+(c.items.length>1?' <span class="muted">+ '+(c.items.length-1)+' related check'+(c.items.length===2?'':'s')+'</span>':'')+'</p><small>'+esc(c.properties.join(' · ')||c.nextStep)+'</small></div><button data-open-case="'+esc(c.customerId)+'">Review customer <span aria-hidden="true">→</span></button></article>').join('')||'<div class="workspace-empty"><h3>No customers in this view</h3><p>Try another category or find a customer above.</p></div>';
    $('more-cases').hidden=rows.length<=limit;
  }
  function selectLane(value){lane=value;limit=5;chooseTab('morning');renderCases();$('customer-cases').scrollIntoView({block:'nearest',behavior:'smooth'});}
  function searchWorkspace(){
    const query=$('workspace-search').value.trim().toLowerCase(),out=$('workspace-search-results');out.hidden=false;
    if(!report){out.textContent='Records are still loading.';return;}
    if(!query){out.textContent='Enter a customer name, property, or task such as replies or scheduling.';return;}
    const commands=[[/^(who needs (a )?reply|repl(?:y|ies)|messages?)\??$/,'replies'],[/^(review )?schedul(?:e|ing)$/,'schedule'],[/^(review )?billing$/,'billing'],[/^(routine|routine checks)$/,'routine']];
    const command=commands.find(([pattern])=>pattern.test(query));if(command){selectLane(command[1]);out.textContent='Showing '+currentCases().length+' customers for '+command[1]+'.';return;}
    if(/^(check )?snow( readiness)?$/.test(query)){chooseTab('snow');out.hidden=true;return;}
    const term=query.replace(/^(show|find|open|review)\s+/,'');
    const matchingProperties=report.choices.properties.filter(p=>(p.name+' '+(p.city||'')).toLowerCase().includes(term));
    const ids=new Set(matchingProperties.map(p=>p.customerId));
    const customers=report.choices.customers.filter(c=>c.name.toLowerCase().includes(term)||ids.has(c.id));
    out.innerHTML=customers.length?'<p class="muted">'+customers.length+' matching customer'+(customers.length===1?'':'s')+(customers.length>8?' · showing 8; add more of the name or address to narrow it down.':'.')+'</p><div class="search-customer-list">'+customers.slice(0,8).map(c=>'<button class="secondary" data-open-case="'+esc(c.id)+'">'+esc(c.name)+' →</button>').join('')+'</div>':'<p>No verified customer matched. Try part of the name or street address, or use the task shortcuts.</p>';
  }
  function status(message,error=false){$('case-status').textContent=message;$('case-status').className=error?'error':'';}
  function selectCaseTab(name){caseTab=name;document.querySelectorAll('[data-case-tab]').forEach(b=>{if(b.dataset.caseTab===name)b.setAttribute('aria-current','page');else b.removeAttribute('aria-current');});document.querySelectorAll('[data-case-panel]').forEach(p=>p.hidden=p.dataset.casePanel!==name);}
  function checksForCustomer(){return (report?.actions||[]).filter(a=>a.customerId===caseId).sort((a,b)=>(a.reviewState==='open'?0:1)-(b.reviewState==='open'?0:1));}
  function renderCaseChecks(){
    const items=checksForCustomer();
    $('case-checks').innerHTML=items.length?items.slice(0,50).map(a=>'<article class="case-check"><label class="check"><input type="checkbox" data-case-check="'+esc(a.id)+'"><strong>'+esc(a.title)+'</strong><span class="chip">'+esc(a.reviewState)+'</span></label><p>'+esc(a.detail)+'</p><small>'+esc([a.propertyName,a.date,a.audience==='office'?'Current review':a.audience+' review'].filter(Boolean).join(' · '))+'</small>'+(a.reviewNote?'<p class="review-note">Office decision: '+esc(a.reviewNote)+'</p>':'')+'<details><summary>Why this showed up & supporting source</summary><p>'+esc(a.detail)+'</p>'+(a.url?safeLink(a.url,'Open source'):'')+'<pre>'+esc(JSON.stringify(a.evidence,null,2))+'</pre></details></article>').join('')+(items.length>50?'<p class="notice">Showing 50 checks. Save a decision, then continue with the remaining checks in All checks.</p>':''):'<p class="empty">There are no review checks for this customer in the loaded window. You can still inspect their records.</p>';
    $('case-review-form').hidden=!items.length;
  }
  function sourceError(name){const state=caseData?.availability[name];return state?.available?null:'<p class="notice">'+esc(state?.error||'This source could not be verified.')+' Refresh the customer or open Homeworks.</p>';}
  function renderCustomer(){
    const c=caseData.customer;
    $('case-title').textContent=c.name;$('case-subtitle').textContent='Checked '+new Date(caseData.checkedAt).toLocaleString()+' · '+[c.email,c.phone].filter(Boolean).join(' · ');
    $('case-account').innerHTML='<div><span class="muted">Account balance</span><strong>'+money(c.outstanding)+'</strong></div><div><span class="muted">Past due</span><strong>'+money(c.pastDue)+'</strong></div><div><span class="muted">Credit</span><strong>'+money(c.credit)+'</strong></div><div>'+safeLink(c.url,'Open Homeworks customer')+'</div>';
    $('case-visit-range').textContent='Visits '+caseData.range.startDate+' through '+caseData.range.endDate+'. Changes here apply to one existing visit.';
    $('case-visits').innerHTML=sourceError('visits')||caseData.visits.map(v=>'<article class="visit-card"><div><strong>'+esc(v.title)+'</strong><p>'+esc(v.startDate)+' · '+esc(v.property?.name||'Property unavailable')+'</p><small>'+esc(v.crew?.name||'No crew')+' · '+esc(v.status)+' · '+(v.isInvoiceable?'Invoiceable':'Not invoiceable')+(v.invoiceId?' · Invoice linked':'')+'</small></div><div class="visit-buttons">'+(v.status==='OPEN'&&!v.invoiceId?'<button class="secondary" data-visit="'+v.id+'" data-decision="skip">Skip</button><button class="secondary" data-visit="'+v.id+'" data-decision="reschedule">Reschedule</button><button class="secondary" data-visit="'+v.id+'" data-decision="complete" '+(v.startDate>report.range.today?'disabled title="Future visits cannot be completed here"':'')+'>Complete</button>':'')+safeLink('/scheduling.html?date='+v.startDate,'View schedule')+'</div></article>').join('')||'<p class="empty">No visits in this date window.</p>';
    $('case-invoices').innerHTML=sourceError('invoices')||caseData.invoices.map(i=>'<article class="record-card"><div class="panel-heading"><h3>'+safeLink(i.url,'Invoice #'+i.number)+'</h3><strong>'+money(i.balance)+' balance</strong></div><p>'+esc(i.date)+' · '+esc(i.status)+' · '+(i.isSent?'Marked sent':'Marked unsent')+'</p><details><summary>Service lines & totals</summary>'+table(['Service','Date','Rate','Quantity','Subtotal'],(i.lineItems||[]).map(l=>[esc(l.name),esc(l.date),money(l.price),esc(l.quantity),money(l.subtotal)]))+'<p>Total '+money(i.total)+' · Paid '+money(i.paidAmount)+'</p></details></article>').join('')||'<p class="empty">No recent invoices or older open balances found.</p>';
    $('case-estimates').innerHTML=sourceError('estimates')||caseData.estimates.map(e=>'<article class="record-card"><div class="panel-heading"><h3>'+safeLink(e.url,'Estimate #'+e.number)+'</h3><strong>'+money(e.total)+'</strong></div><p>'+esc(e.title)+' · '+esc(e.date)+' · '+esc(e.status)+'</p><details><summary>Included & optional services</summary>'+table(['Service','Included','Rate','Quantity','Scheduling status'],(e.lineItems||[]).map(l=>[esc(l.name),l.isIncluded?'Yes':'Optional / excluded',money(l.price),esc(l.quantity),esc(l.eventStatus)]))+'</details></article>').join('')||'<p class="empty">No estimates in this date window.</p>';
    $('case-message-coverage').textContent='Homeworks messages: '+caseData.messageSources.homeworks+' (30 days). YardDesk messages: '+caseData.messageSources.yarddesk+'. '+(caseData.messageWarning||'')+(caseData.messagesLimited?' History is limited; open the complete conversation.':'');
    $('case-messages').innerHTML=caseData.messages.slice(0,50).map(m=>'<article class="message-bubble '+(m.direction==='outbound'?'outbound':'')+'"><small>'+esc(m.direction==='inbound'?c.name:'Office')+' · '+esc(new Date(m.at).toLocaleString())+' · '+esc(m.source)+' · '+esc(m.status||'')+'</small><p>'+esc(m.text)+'</p></article>').join('')||'<p class="empty">No verified messages found in this context.</p>';
    if(caseData.messages.length>50)$('case-messages').insertAdjacentHTML('beforeend','<p class="muted">Showing the latest 50 messages. Open the full conversation for older history.</p>');
  }
  function invalidateCaseDraft(){draftVersion++;caseDraft=null;$('case-draft-reviewed').checked=false;$('case-draft-reviewed').disabled=true;$('save-case-draft').disabled=true;$('case-draft-preview').hidden=true;}
  async function openCase(id,tab='overview'){
    if(visitBusy)return;const generation=++loadGeneration;caseId=Number(id);caseData=null;invalidateCaseDraft();$('case-draft-instruction').value='';$('case-draft-status').textContent='';$('visit-decision').hidden=true;$('case-account').innerHTML='';['case-visits','case-invoices','case-estimates','case-messages'].forEach(id=>$(id).innerHTML='');
    $('case-title').textContent=nameFor(caseId);$('case-subtitle').textContent='';status('Loading current customer records…');renderCaseChecks();selectCaseTab(tab);if(!dialog.open)dialog.showModal();
    const rows=currentCases(),index=rows.findIndex(c=>c.customerId===caseId);$('previous-case').disabled=index<=0;$('next-case').disabled=index<0||index>=rows.length-1;
    $('case-review-decision').value='';$('case-review-note').value='';$('case-return-label').hidden=true;
    try{const data=await api('/api/homeworks/operations/customer/'+caseId);if(generation!==loadGeneration||!dialog.open)return;caseData=data;renderCustomer();const failures=Object.values(data.availability).filter(s=>!s.available).length;status(failures?'Some records are unavailable. Review the source warnings below.':'Current customer records loaded.',!!failures);}catch(error){if(generation===loadGeneration)status(error.message,true);}
  }
  function prepareVisit(id,kind){
    const v=caseData?.visits.find(v=>v.id===Number(id));if(!v||visitBusy)return;
    const panel=$('visit-decision');panel.hidden=false;
    panel.innerHTML='<form id="visit-change-form" class="visit-change-form" data-event="'+v.id+'" data-kind="'+kind+'"><h3>'+esc({skip:'Skip this visit',reschedule:'Move this visit',complete:'Complete this visit'}[kind])+'</h3><p>'+esc(v.title)+' · '+esc(v.property?.name||'')+' · '+esc(v.startDate)+'</p>'+(kind==='skip'?'<label>Reason <textarea id="visit-change-reason" rows="2" maxlength="1500" required></textarea></label>':kind==='reschedule'?'<label>New visit date <input id="visit-change-date" type="date" required></label>':'<p>Confirm that this work was performed. '+(v.isInvoiceable?'Completing it can create a Homeworks invoice.':'Homeworks marks this visit as not invoiceable.')+'</p>')+'<button type="submit">Review change</button> <button type="button" id="cancel-visit-change" class="secondary">Cancel</button><div id="visit-change-preview" hidden></div><p id="visit-change-status" role="status"></p></form>';
    panel.scrollIntoView({block:'nearest',behavior:'smooth'});
  }
  document.addEventListener('operations:loaded',renderCases);
  $('workspace-search-form').addEventListener('submit',e=>{e.preventDefault();searchWorkspace();});
  $('workspace-search').addEventListener('input',()=>{if(!$('workspace-search').value)$('workspace-search-results').hidden=true;});
  $('more-cases').addEventListener('click',()=>{limit+=5;renderCases();});
  document.querySelectorAll('[data-lane]').forEach(b=>b.addEventListener('click',()=>selectLane(b.dataset.lane)));
  document.querySelectorAll('[data-workspace-lane]').forEach(b=>b.addEventListener('click',()=>selectLane(b.dataset.workspaceLane)));
  document.querySelectorAll('[data-workspace-tool]').forEach(b=>b.addEventListener('click',()=>chooseTab(b.dataset.workspaceTool)));
  document.querySelectorAll('[data-case-tab]').forEach(b=>b.addEventListener('click',()=>selectCaseTab(b.dataset.caseTab)));
  document.addEventListener('click',e=>{const b=e.target.closest('[data-open-case]');if(b)openCase(b.dataset.openCase);const v=e.target.closest('[data-visit]');if(v)prepareVisit(v.dataset.visit,v.dataset.decision);if(e.target.closest('#cancel-visit-change')&&!visitBusy)$('visit-decision').hidden=true;});
  $('reload-case').addEventListener('click',()=>{if(caseId&&!visitBusy)openCase(caseId,caseTab);});
  $('close-case').addEventListener('click',()=>{if(!visitBusy)dialog.close();});dialog.addEventListener('cancel',e=>{if(visitBusy)e.preventDefault();});dialog.addEventListener('close',()=>{loadGeneration++;invalidateCaseDraft();});
  for(const [id,offset] of [['previous-case',-1],['next-case',1]])$(id).addEventListener('click',()=>{const rows=currentCases(),index=rows.findIndex(c=>c.customerId===caseId);if(rows[index+offset])openCase(rows[index+offset].customerId);});
  $('case-review-decision').addEventListener('change',()=>{$('case-return-label').hidden=$('case-review-decision').value!=='snoozed';});
  $('case-review-form').addEventListener('submit',async e=>{
    e.preventDefault();const choice=$('case-review-decision').value,ids=[...dialog.querySelectorAll('[data-case-check]:checked')].map(c=>c.dataset.caseCheck),items=checksForCustomer().filter(a=>ids.includes(a.id));
    if(!items.length||!choice){status('Select the checks you reviewed and choose a decision.',true);return;}
    const note={accounted:'Already accounted for',intentional:'Intentional schedule or billing choice',resolved:'Verified and resolved',snoozed:'Review later',open:'Reopened for review'}[choice]+($('case-review-note').value.trim()?': '+$('case-review-note').value.trim():'');
    const button=$('save-case-review'),generation=loadGeneration;button.disabled=true;status('Saving your office decision…');
    try{await api('/api/homeworks/operations/review-case',{method:'POST',body:{customerId:caseId,items:items.map(a=>({id:a.id,fingerprint:a.fingerprint})),state:choice==='snoozed'?'snoozed':choice==='open'?'open':'reviewed',until:$('case-return-date').value,note,range:{startDate:report.range.startDate,endDate:report.range.endDate}}});await refresh(false);if(generation!==loadGeneration)return;renderCaseChecks();status(items.length+' check'+(items.length===1?'':'s')+' updated. '+(choice==='snoozed'?'They return on '+$('case-return-date').value+'.':'Your explanation is saved with the review.'));}catch(error){if(generation===loadGeneration)status(error.message,true);}finally{button.disabled=false;}
  });
  dialog.addEventListener('input',e=>{if(e.target.matches('#visit-change-reason,#visit-change-date')){const preview=$('visit-change-preview');if(preview)preview.hidden=true;}});
  dialog.addEventListener('submit',async e=>{
    const form=e.target.closest('#visit-change-form');if(!form)return;e.preventDefault();if(visitBusy)return;
    const v=caseData?.visits.find(v=>v.id===Number(form.dataset.event));if(!v)return;const kind=form.dataset.kind,date=$('visit-change-date')?.value,reason=$('visit-change-reason')?.value.trim();
    if(kind==='reschedule'&&(!date||date===v.startDate)){$('visit-change-status').textContent='Choose a different visit date.';return;}
    const preview=$('visit-change-preview');preview.hidden=false;preview.innerHTML='<div class="notice"><strong>Review before applying</strong><p>'+esc(caseData.customer.name)+' · '+esc(v.property?.name||'')+'<br>'+esc(v.title)+' · '+esc(v.startDate)+'</p><p>'+esc(kind==='skip'?'Mark this single visit skipped. Reason: '+reason:kind==='reschedule'?'Move this single visit to '+date+'.':'Mark this visit completed in Homeworks.')+'</p></div>'+(kind==='complete'&&v.isInvoiceable?'<label class="check"><input id="visit-billing-confirmed" type="checkbox">I understand this can create a Homeworks invoice.</label>':'')+'<button type="button" id="confirm-visit-change">'+esc(kind==='skip'?'Confirm skip':kind==='reschedule'?'Confirm new date':'Confirm completion')+'</button>';
    preview.scrollIntoView({block:'nearest',behavior:'smooth'});
  });
  dialog.addEventListener('click',async e=>{
    const confirm=e.target.closest('#confirm-visit-change');if(!confirm||visitBusy)return;const form=$('visit-change-form'),v=caseData?.visits.find(v=>v.id===Number(form?.dataset.event));if(!v)return;
    if(form.dataset.kind==='complete'&&v.isInvoiceable&&!$('visit-billing-confirmed')?.checked){$('visit-change-status').textContent='Review and confirm the billing effect first.';return;}
    const customerId=caseId,generation=loadGeneration;visitBusy=true;confirm.disabled=true;$('close-case').disabled=true;$('previous-case').disabled=true;$('next-case').disabled=true;form.querySelectorAll('input,textarea,button').forEach(el=>el.disabled=true);$('visit-change-status').textContent='Applying and verifying the Homeworks change…';
    try{const result=await api('/api/homeworks/operations/visit-decision',{method:'POST',body:{customerId,eventId:v.id,fingerprint:v.fingerprint,kind:form.dataset.kind,date:$('visit-change-date')?.value,reason:$('visit-change-reason')?.value,confirmBilling:$('visit-billing-confirmed')?.checked===true,confirmed:true}});visitBusy=false;await refresh(true);if(generation!==loadGeneration)return;await openCase(customerId,'visits');status('Homeworks confirmed the visit change.'+(result.billingPending?' Invoice processing may still be pending.':''));}catch(error){$('visit-change-status').textContent=error.message+' Reload this customer before retrying. The change may already have reached Homeworks.';form.querySelectorAll('input,textarea,button').forEach(el=>el.disabled=true);}finally{visitBusy=false;$('close-case').disabled=false;}
  });
  ['case-draft-kind','case-draft-instruction'].forEach(id=>$(id).addEventListener('input',invalidateCaseDraft));
  $('case-draft-body').addEventListener('input',()=>{$('case-draft-reviewed').checked=false;$('save-case-draft').disabled=true;});
  $('case-draft-reviewed').addEventListener('change',()=>{$('save-case-draft').disabled=!caseDraft||draftWorking||!$('case-draft-reviewed').checked;});
  $('case-draft-form').addEventListener('submit',async e=>{
    e.preventDefault();if(!caseData||draftWorking)return;invalidateCaseDraft();const version=draftVersion;draftWorking=true;$('prepare-case-draft').disabled=true;$('case-draft-status').textContent='Reading the conversation and preparing a draft…';
    try{const draft=await api('/api/homeworks/operations/followup-draft',{method:'POST',body:{customerId:caseId,kind:$('case-draft-kind').value,instruction:$('case-draft-instruction').value}});if(version!==draftVersion||!dialog.open)return;caseDraft=draft;$('case-draft-body').value=draft.body;$('case-draft-preview').hidden=false;$('case-draft-reviewed').disabled=false;$('case-draft-warnings').textContent=[...draft.blockers,draft.aiError,draft.limited?'Conversation is limited. Review the complete history.':null].filter(Boolean).join(' ');$('case-draft-references').innerHTML=draft.references.map(r=>'<p>'+safeLink(r.url,r.label)+(r.customerFacing?'':' <strong>Office link only</strong>')+'</p>').join('');$('case-draft-status').textContent='Draft ready for review. Nothing has been sent.';}catch(error){if(version===draftVersion)$('case-draft-status').textContent=error.message;}finally{draftWorking=false;$('prepare-case-draft').disabled=false;}
  });
  $('save-case-draft').addEventListener('click',async()=>{
    if(!caseDraft||draftWorking||!$('case-draft-reviewed').checked)return;const version=draftVersion;$('save-case-draft').disabled=true;draftWorking=true;
    try{await api('/api/homeworks/operations/save-followup',{method:'POST',body:{customerId:caseDraft.customerId,kind:caseDraft.kind,instruction:$('case-draft-instruction').value,fingerprint:caseDraft.fingerprint,body:$('case-draft-body').value,reviewed:true}});if(version===draftVersion)$('case-draft-status').textContent='Internal draft saved. Nothing was sent.';}catch(error){if(version===draftVersion){$('case-draft-status').textContent=error.message;$('case-draft-reviewed').checked=false;}}finally{draftWorking=false;if(version===draftVersion)$('save-case-draft').disabled=!$('case-draft-reviewed').checked;}
  });
  if(report)renderCases();
})();
