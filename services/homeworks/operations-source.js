'use strict';
const records = require('./records');
const DAY = 86400000;
function validDate(value) {
  const parsed = new Date(value + 'T12:00:00Z');
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value)) && Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}
function shift(date, days) { return new Date(Date.parse(date + 'T12:00:00Z') + days * DAY).toISOString().slice(0, 10); }
function period(input = {}) {
  const today = input.today || records.dateKey();
  const startDate = input.startDate || shift(today, -28), endDate = input.endDate || shift(today, 28);
  if (![today, startDate, endDate].every(validDate) || startDate > today || endDate < today ||
      Date.parse(endDate) - Date.parse(startDate) > 93 * DAY) throw new Error('Choose a review window containing today, up to 93 days.');
  return { today, startDate, endDate, billingStart: shift(today, -90), messageStart: shift(today, -14) + 'T00:00:00Z',
    estimateStart: shift(today, -550), season: (Number(today.slice(5, 7)) < 7 ? Number(today.slice(0, 4)) - 1 : Number(today.slice(0, 4))) };
}
const EVENT_FIELDS = 'id type title status startDate endDate customerId propertyId crewId recurringEventId estimateId isInvoiceable invoiceId budgetedHours subtotal skippedReason timeEntryTotals{pastTimeEntries pastTimeEntrySeconds runningTimers} lineItems{id name itemId price quantity}';
const ESTIMATE_FIELDS = 'id number customerId title date status subtotal total acceptedAt updatedAt lineItems{id name description price quantity subtotal propertyId itemId isIncluded eventStatus taxPercent1 taxPercent2}';
const RECURRING_FIELDS = 'id title type customerId propertyId crewId startDate endDate isInfinite frequency weeklyCount weekday1 weekday2 weekday3 weekday4 weekday5 weekday6 weekday7 dailyOption dailyDaysCount monthlyOption monthDay monthCount1 offSeason{id startMonth startDay endMonth endDay}';
async function load(context, input) {
  const range = period(input);
  const jobs = {
    customers: () => records.listAll(context, {entity:'customers',filterType:'CustomerFilter',where:{isDeleted:false},fields:'id fullName firstName status email cell phone outstanding pastDue credit emailSendability smsSendability',pageSize:1000}),
    properties: () => records.listAll(context, {entity:'properties',filterType:'PropertyFilter',where:{deletedStatus:'ACTIVE'},fields:'id customerId name address{street1 city state zip} size pavedSize drivewayWalkwaySize',pageSize:500}),
    events: () => records.listAll(context, {entity:'events',filterType:'EventFilter',where:{isDeleted:false,type:{equals:'VISIT'},startDate:{gte:range.startDate,lte:range.endDate}},fields:EVENT_FIELDS,pageSize:200,maxRows:20000}),
    recurring: () => records.listAll(context, {entity:'recurringEvents',filterType:'RecurringEventFilter',where:{isCancelled:false,type:{equals:'VISIT'},startDate:{lte:range.endDate},OR:[{isInfinite:true},{endDate:{gte:range.startDate}}]},fields:RECURRING_FIELDS,pageSize:500}),
    invoices: () => records.listAll(context, {entity:'invoices',filterType:'InvoiceFilter',where:{isArchived:false,deletedState:{equals:'ACTIVE'},date:{gte:range.billingStart,lte:range.today}},fields:'id number customerId propertyId date status isSent total paidAmount updatedAt lineItems{id name date itemId price quantity subtotal propertyId}',pageSize:100,maxRows:10000}),
    estimates: () => records.listAll(context, {entity:'estimates',filterType:'EstimateFilter',where:{isDeleted:false,isArchived:false,date:{gte:range.estimateStart,lte:range.today}},fields:ESTIMATE_FIELDS,pageSize:100,maxRows:10000}),
    sms: () => records.listAll(context, {entity:'smsMessages',filterType:'SmsFilter',where:{sentAt:{gte:range.messageStart}},fields:'id sid customerId direction status sentAt text isViewed',orderBy:'[{id:asc}]',pageSize:500}),
    officeSms: async () => {
      const result=await context.pool.query("SELECT m.id,m.twilio_sid,m.body,m.direction,m.status,m.created_at,pm.customer_id FROM messages m JOIN (SELECT normalized_phone,MIN(homeworks_customer_id) AS customer_id FROM homeworks_phone_matches GROUP BY normalized_phone HAVING COUNT(DISTINCT homeworks_customer_id)=1) pm ON pm.normalized_phone=RIGHT(REGEXP_REPLACE(CASE WHEN m.direction='inbound' THEN m.from_number ELSE m.to_number END,'[^0-9]','','g'),10) WHERE m.created_at >= $1 ORDER BY m.created_at DESC LIMIT 5001",[range.messageStart]);
      if(result.rows.length>5000)throw new Error('Narrow the office message review.');
      return result.rows.map(row=>({id:'yarddesk:'+row.id,sid:row.twilio_sid,customerId:Number(row.customer_id),text:row.body,direction:row.direction==='inbound'?'IN':'OUT',status:row.status,sentAt:new Date(row.created_at).toISOString()}));
    },
    crews: () => records.listAll(context, {entity:'crews',filterType:'CrewFilter',where:{isDeleted:false},fields:'id name',pageSize:500}),
    routes: async () => {
      const data = await records.call(context,'YardDeskOperationsRoutes','query YardDeskOperationsRoutes($since:Date!,$until:Date!){routeReport(where:{date:{gte:$since,lte:$until}}){crewId crew{name} _sum{totalMiles totalDriveSeconds}}}',{since:range.startDate,until:range.today});
      if (!Array.isArray(data.routeReport)) throw new Error('Route report unavailable.');
      return data.routeReport;
    },
    templates: async () => {
      const data = await records.call(context,'YardDeskOperationsSnow','query YardDeskOperationsSnow{quickDispatchTemplates(take:1000){id name propertyCount properties{id propertyId crewId isInvoiceable lineItems{itemId price quantity tax}}}}');
      if (!Array.isArray(data.quickDispatchTemplates) || data.quickDispatchTemplates.length >= 1000) throw new Error('Snow lists are incomplete.');
      return data.quickDispatchTemplates;
    },
    agreements: async () => {
      const {ensureHomeworksSchema} = require('./schema');
      await ensureHomeworksSchema(context.pool);
      const quotes = await context.pool.query('SELECT id,quote_number,customer_name,customer_email,contract_signed_at FROM sent_quotes WHERE quote_number IS NOT NULL');
      const ledger = await context.pool.query('SELECT quote_id,property_id,status,media_id,error FROM homeworks_document_attachments');
      return { quotes:quotes.rows, ledger:ledger.rows };
    },
  };
  // A small worker pool avoids fanning a full-account review into dozens of API requests at once.
  const names = Object.keys(jobs), data = {}, availability = {};
  let position = 0;
  await Promise.all(Array.from({length:3},async () => {
    while (position < names.length) {
      const name = names[position++];
      try {
        data[name] = await jobs[name]();
        if(Array.isArray(data[name])&&data[name].some(row=>Array.isArray(row.lineItems)&&row.lineItems.length>=1000))throw new Error('Nested service lines exceed the verified limit.');
        if(name==='templates'&&data[name].some(row=>Number(row.propertyCount)>(row.properties||[]).length))throw new Error('A snow list has incomplete property entries.');
        availability[name] = {available:true};
      }
      catch(error) { data[name] = null; availability[name] = {available:false,error:error.message}; }
    }
  }));
  return { ...data, range, availability, asOf:new Date().toISOString(), source:records.SOURCE };
}
module.exports = { load, period, shift, validDate, DAY, EVENT_FIELDS, ESTIMATE_FIELDS, RECURRING_FIELDS };
