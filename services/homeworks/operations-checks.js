'use strict';
const crypto = require('node:crypto');
const {shift, DAY} = require('./operations-source');
const {cents} = require('./records');
const normalized = value => String(value || '').trim().toLowerCase().replace(/\s+/g,' ');
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const sourceLink = (kind,id) => 'https://secure.copilotcrm.com/' + ({estimate:'finances/estimates/view/',invoice:'finances/invoices/view/',customer:'customers/view/'}[kind] || '') + Number(id);
const eventLink = event => '/scheduling.html?date=' + encodeURIComponent(event.startDate);
function action(kind, key, title, detail, evidence, extra = {}) {
  const item = {id:kind + ':' + key,kind,title,detail,evidence,severity:'normal',...extra};
  item.fingerprint = hash({title,detail,evidence});
  return item;
}
function inOffSeason(date, season) {
  if (!season) return false;
  const day = Number(date.slice(5,7)) * 100 + Number(date.slice(8));
  const start = season.startMonth * 100 + season.startDay, end = season.endMonth * 100 + season.endDay;
  return start <= end ? day >= start && day <= end : day >= start || day <= end;
}
function expectedDates(series, start, end) {
  const first = series.startDate, last = series.isInfinite ? end : [end,series.endDate].sort()[0];
  if (!first || !last || first > end || last < start) return {dates:[],supported:true};
  const supported = series.frequency === 'WEEKLY' || series.frequency === 'DAILY' ||
    (series.frequency === 'MONTHLY' && series.monthlyOption === 'ON_DAY_OF_MONTH');
  if (!supported) return {dates:[],supported:false};
  const dates = [], anchor = new Date(first + 'T12:00:00Z');
  const weekStart = shift(first,-((anchor.getUTCDay()+6)%7));
  for (let date = first > start ? first : start; date <= last; date = shift(date,1)) {
    if (inOffSeason(date,series.offSeason)) continue;
    const current = new Date(date + 'T12:00:00Z'), weekday = (current.getUTCDay()+6)%7+1;
    const elapsed = Math.round((Date.parse(date)-Date.parse(first))/DAY);
    let expected = false;
    if (series.frequency === 'WEEKLY') {
      const interval = Number(series.weeklyCount);
      if (!Number.isInteger(interval) || interval < 1) return {dates:[],supported:false};
      const weeks = Math.floor((Date.parse(date)-Date.parse(weekStart))/(DAY*7));
      const hasWeekdays = [1,2,3,4,5,6,7].some(day=>series['weekday'+day]);
      expected = weeks % interval === 0 && (hasWeekdays ? series['weekday'+weekday] : current.getUTCDay() === anchor.getUTCDay());
    } else if (series.frequency === 'DAILY') {
      if (series.dailyOption === 'EVERY_WEEKDAY') expected = weekday <= 5;
      else if (series.dailyOption === 'EVERY_N_DAYS' && Number(series.dailyDaysCount) > 0) expected = elapsed % Number(series.dailyDaysCount) === 0;
      else return {dates:[],supported:false};
    } else {
      const months = (current.getUTCFullYear()-anchor.getUTCFullYear())*12+current.getUTCMonth()-anchor.getUTCMonth();
      const interval = Number(series.monthCount1);
      if (!Number.isInteger(interval) || interval < 1) return {dates:[],supported:false};
      expected = months % interval === 0 && current.getUTCDate() === Number(series.monthDay);
    }
    if (expected) dates.push(date);
  }
  return {dates,supported:true};
}
function scheduling(data) {
  if (!data.events || !data.recurring) return {available:false,items:[],note:'Visits and recurring schedules are required.'};
  const items = [], {startDate,endDate,today} = data.range;
  const active = data.events.filter(event=>event.status !== 'CANCELLED');
  for (const series of data.recurring) {
    const planned = expectedDates(series,startDate,endDate);
    const visits = data.events.filter(event=>event.recurringEventId === series.id);
    if (!planned.supported) {
      items.push(action('schedule_pattern',series.id,'Review recurring pattern',series.title + ': this recurrence requires a manual calendar check.',{seriesId:series.id,frequency:series.frequency},{customerId:series.customerId}));
      continue;
    }
    for (const date of planned.dates) {
      const occurrences = visits.filter(event=>event.startDate === date);
      // Explicit cancellations and skips are intentional occurrences, not missing work.
      if (occurrences.length) continue;
      const moved = visits.find(event=>event.status !== 'CANCELLED' && !planned.dates.includes(event.startDate) && Math.abs(Date.parse(event.startDate)-Date.parse(date)) <= 3*DAY);
      if (moved) continue;
      items.push(action('missing_visit',series.id + ':' + date,'Expected visit not found',series.title + ' was expected on ' + date + '. Check postponements before adding work.',{seriesId:series.id,date,frequency:series.frequency,interval:series.weeklyCount},{customerId:series.customerId,propertyId:series.propertyId,date,url:'/scheduling.html?date='+date,severity:date <= today?'high':'normal'}));
    }
    for (const event of visits.filter(event=>['OPEN','WAITLISTED'].includes(event.status))) {
      if (series.crewId && event.crewId !== series.crewId) items.push(action('crew_mismatch',event.id,'Crew differs from recurring schedule',event.title + ' has a different crew assignment. Confirm whether this was intentional.',{eventId:event.id,expectedCrewId:series.crewId,actualCrewId:event.crewId},{customerId:event.customerId,propertyId:event.propertyId,date:event.startDate,url:eventLink(event)}));
    }
  }
  const groups = new Map();
  for (const event of active) {
    if (!event.customerId || !event.propertyId) continue;
    const key = [event.customerId,event.propertyId,event.startDate,normalized(event.title)].join(':');
    if (!groups.has(key)) groups.set(key,[]);
    groups.get(key).push(event);
  }
  for (const rows of groups.values()) if (rows.length > 1) {
    const row = rows[0];
    items.push(action('duplicate_visit',rows.map(e=>e.id).sort().join('-'),'Possible duplicate visits',row.title + ': multiple visits appear for the same property and date. Check the service lines before removing anything.',{eventIds:rows.map(e=>e.id),date:row.startDate},{customerId:row.customerId,propertyId:row.propertyId,date:row.startDate,url:eventLink(row),severity:'high'}));
  }
  for (const event of active) {
    if (event.status === 'OPEN' && event.startDate < today) items.push(action('past_open',event.id,'Past visit still open',event.title + ' on ' + event.startDate + ': confirm whether it was performed, moved, or skipped.',{eventId:event.id,status:event.status,isInvoiceable:event.isInvoiceable},{customerId:event.customerId,propertyId:event.propertyId,date:event.startDate,url:eventLink(event)}));
    if (event.status === 'OPEN' && event.startDate >= today && !event.crewId) items.push(action('unassigned',event.id,'Visit has no crew',event.title + ' needs an assignment.',{eventId:event.id,crewId:null},{customerId:event.customerId,date:event.startDate,url:eventLink(event)}));
  }
  // Message intent is a review cue only. It does not prove a skip, cancellation, or its target date.
  for (const sms of data.sms || []) {
    if (sms.direction !== 'IN' || !sms.customerId || !/\b(skip|cancel|stop|hold off|do not mow|don.t (?:cut|mow|come))\b/i.test(sms.text)) continue;
    const future = active.filter(event=>event.customerId===sms.customerId && event.status==='OPEN' && event.startDate>=today);
    if (future.length) items.push(action('skip_request',sms.id,'Check service request against future visits','A recent message may request a skip or cancellation. Read the conversation and confirm which dates it applies to.',{messageId:sms.id,text:sms.text,sentAt:sms.sentAt,eventIds:future.map(e=>e.id)},{customerId:sms.customerId,severity:'high'}));
  }
  return {available:true,items,scope:{startDate,endDate},note:'Findings are review candidates. Skipped/cancelled occurrences and assigned off-seasons are respected; unsupported patterns are listed separately.'};
}
function billing(data) {
  if (!data.invoices || !data.events) return {available:false,items:[]};
  const items=[],groups=new Map();
  for (const event of data.events) if (event.status==='CLOSED' && event.isInvoiceable===true && !event.invoiceId) {
    items.push(action('unbilled_visit',event.id,'Completed billable visit has no invoice link','Check Homeworks monthly/seasonal billing and pending invoice processing before creating an invoice.',{eventId:event.id,date:event.startDate,subtotal:event.subtotal},{customerId:event.customerId,propertyId:event.propertyId,url:eventLink(event),severity:'high'}));
  }
  for (const invoice of data.invoices) {
    if (invoice.status==='WRITE_OFF') continue;
    const balance = Math.max(0,cents(invoice.total)-cents(invoice.paidAmount))/100;
    if (balance>0 && !invoice.isSent) items.push(action('unsent_invoice',invoice.id,'Invoice is marked unsent','Invoice #' + invoice.number + ' has a balance of $' + balance.toFixed(2) + '. Confirm prior delivery and service before sending.',{invoiceId:invoice.id,number:invoice.number,balance,isSent:false,date:invoice.date},{customerId:invoice.customerId,url:sourceLink('invoice',invoice.id)}));
    if (!invoice.isSent || !invoice.customerId || !invoice.lineItems?.length) continue;
    const signature=hash([invoice.customerId,invoice.propertyId,invoice.date,cents(invoice.total),invoice.lineItems.map(l=>[normalized(l.name),l.date,l.propertyId,String(l.quantity),cents(l.price)]).sort()]);
    if (!groups.has(signature)) groups.set(signature,[]);
    groups.get(signature).push(invoice);
  }
  for (const rows of groups.values()) if (rows.length>1) items.push(action('duplicate_invoice',rows.map(r=>r.id).sort().join('-'),'Possible duplicate invoices','The same customer, invoice date, total, and service-line details appear on multiple sent invoices. Compare the source records.',{invoiceIds:rows.map(r=>r.id),numbers:rows.map(r=>r.number)},{customerId:rows[0].customerId,url:sourceLink('invoice',rows[0].id),severity:'high'}));
  return {available:true,items,scope:{invoiceStart:data.range.billingStart,invoiceEnd:data.range.today},note:'Invoice review covers the last 90 days. A missing link is not proof that a customer has not been billed.'};
}
function followups(data) {
  if (!data.sms || !data.estimates) return {available:false,items:[]};
  const items=[],latest=new Map();
  for (const sms of data.sms) if (sms.customerId) {
    if(sms.direction==='OUT'&&['FAILED','UNDELIVERED'].includes(String(sms.status).toUpperCase())) {
      items.push(action('failed_message',sms.id,'Customer text failed delivery','Check the number and complete conversation before retrying.',{messageId:sms.id,text:sms.text,status:sms.status,sentAt:sms.sentAt},{customerId:sms.customerId,severity:'high'}));
      continue;
    }
    const old=latest.get(sms.customerId);
    if (!old || Date.parse(sms.sentAt)>Date.parse(old.sentAt) || (sms.sentAt===old.sentAt&&sms.id>old.id)) latest.set(sms.customerId,sms);
  }
  for (const sms of latest.values()) if (sms.direction==='IN') items.push(action('reply_needed',sms.id,'Latest customer text has no later Homeworks reply','Read the complete Homeworks and YardDesk conversation before sending a response.',{messageId:sms.id,text:sms.text,sentAt:sms.sentAt},{customerId:sms.customerId}));
  for (const estimate of data.estimates) {
    if (['CHANGE_REQUESTED','CHANGE_UPDATED'].includes(estimate.status)) items.push(action('estimate_change',estimate.id,'Estimate changes need review','Review estimate #' + estimate.number + ' and the requested scope before following up.',{estimateId:estimate.id,number:estimate.number,status:estimate.status},{customerId:estimate.customerId,url:sourceLink('estimate',estimate.id)}));
    if (['ACCEPTED','INVOICED'].includes(estimate.status)) for (const line of estimate.lineItems || []) {
      if (line.isIncluded && line.eventStatus==='NO_EVENT') items.push(action('accepted_unscheduled',estimate.id + ':' + line.id,'Accepted service is not scheduled',line.name + ' on estimate #' + estimate.number + ' has no scheduled event. Confirm whether this is seasonal billing or a service needing a visit.',{estimateId:estimate.id,number:estimate.number,lineId:line.id,itemId:line.itemId,propertyId:line.propertyId,eventStatus:line.eventStatus},{customerId:estimate.customerId,propertyId:line.propertyId,url:sourceLink('estimate',estimate.id)}));
    }
  }
  return {available:true,items,note:'The Homeworks message scan covers 14 days. Draft preparation also checks explicitly matched YardDesk conversations.'};
}
function crewReport(data) {
  if (!data.events) return {available:false,rows:[]};
  const crews=new Map((data.crews||[]).map(c=>[c.id,c.name])),groups=new Map(),overruns=[];
  for (const event of data.events.filter(e=>e.status==='CLOSED' && e.startDate<=data.range.today)) {
    const key=event.crewId || 0;
    if (!groups.has(key)) groups.set(key,{crewId:key,name:crews.get(key)||'Unassigned / unavailable crew',completed:0,timed:0,budgeted:0,actual:0,comparableBudget:0,comparableActual:0,serviceValueCents:0});
    const row=groups.get(key), totals=event.timeEntryTotals, hours=Number(totals?.pastTimeEntrySeconds||0)/3600, budget=Number(event.budgetedHours||0);
    row.completed++;row.budgeted+=budget;row.serviceValueCents+=cents(event.subtotal);
    if (Number(totals?.pastTimeEntries)>0) {row.timed++;row.actual+=hours;if(budget>0){row.comparableBudget+=budget;row.comparableActual+=hours;}}
    if (budget>0 && Number(totals?.pastTimeEntries)>0 && hours>budget*1.25) overruns.push({eventId:event.id,title:event.title,date:event.startDate,customerId:event.customerId,propertyId:event.propertyId,budgetedHours:budget,actualHours:hours,url:eventLink(event)});
  }
  const properties=new Map();
  for(const row of overruns){const key=row.propertyId||'event:'+row.eventId;if(!properties.has(key))properties.set(key,[]);properties.get(key).push(row);}
  return {available:true,rows:[...groups.values()].map(row=>({...row,coverage:row.completed?row.timed/row.completed:0,varianceHours:row.comparableBudget?row.comparableActual-row.comparableBudget:null,actualHours:row.timed?row.actual:null,serviceValue:row.serviceValueCents/100,route:data.routes?.find(route=>route.crewId===row.crewId)?._sum || null})),overruns,repeatedOverruns:[...properties.values()].filter(rows=>rows.length>1),routeAvailable:Array.isArray(data.routes),note:'Recorded completed labor hours only; running timers are excluded. Service value is not collected revenue or profit. Comparisons use only visits with both a budget and recorded time.'};
}
function briefing(data, sections) {
  const visits=(data.events||[]).filter(e=>e.startDate===data.range.today&&['OPEN','CLOSED','SKIPPED'].includes(e.status));
  const crews=new Map((data.crews||[]).map(c=>[c.id,c.name]));
  const todayCrews=[...new Set(visits.map(e=>e.crewId))].map(id=>({crewId:id,name:crews.get(id)||'Unassigned',visits:visits.filter(e=>e.crewId===id).length}));
  const actions=Object.values(sections).flatMap(section=>section.items||[]);
  return {available:!!data.events,date:data.range.today,visits:data.events?visits.length:null,crews:todayCrews,completed:visits.filter(e=>e.status==='CLOSED').length,skipped:visits.filter(e=>e.status==='SKIPPED').length,highPriority:actions.filter(a=>a.severity==='high').length,actionCount:actions.length,topActions:actions.filter(a=>a.severity==='high').slice(0,8),note:'Review-only briefing. Opening it never changes visits, invoices, or customer messages.'};
}
module.exports={normalized,hash,action,sourceLink,inOffSeason,expectedDates,scheduling,billing,followups,crewReport,briefing};
