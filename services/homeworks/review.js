'use strict';
const { call, listAll, dateKey, cents, SOURCE } = require('./records');

function isSeason(name, season) {
  const [start, end] = season.split('-');
  return new RegExp(`${start}\\s*[-–—]\\s*(?:${end}|${end.slice(-2)})(?!\\d)`).test(String(name));
}

function snowReadiness(estimates, templates, season) {
  const accepted = new Map(),
    roster = new Map(),
    issues = [];
  for (const estimate of estimates)
    for (const line of estimate.lineItems || []) {
      if (!line.isIncluded) continue;
      if (!line.propertyId) {
        issues.push({ type: 'missing_property', estimateId: estimate.id, number: estimate.number });
        continue;
      }
      const list = accepted.get(line.propertyId) || [];
      list.push({
        estimateId: estimate.id,
        number: estimate.number,
        customerId: estimate.customerId,
        lineId: line.id,
        name: line.name,
        itemId: line.itemId,
        price: line.price,
        quantity: line.quantity,
      });
      accepted.set(line.propertyId, list);
    }
  const current = templates.filter((template) => isSeason(template.name, season));
  for (const template of current)
    for (const row of template.properties || []) {
      const list = roster.get(row.propertyId) || [];
      list.push({
        templateId: template.id,
        templatePropertyId: row.id,
        crewId: row.crewId,
        isInvoiceable: row.isInvoiceable,
        lineItems: row.lineItems,
      });
      roster.set(row.propertyId, list);
    }
  for (const [propertyId, services] of accepted) {
    if (!roster.has(propertyId)) issues.push({ type: 'accepted_missing_dispatch', propertyId, services });
    const entries = roster.get(propertyId) || [];
    if (entries.length > 1) issues.push({ type: 'multiple_dispatch_entries', propertyId, entries });
    for (const entry of entries) {
      if (!entry.crewId) issues.push({ type: 'missing_crew', propertyId, templateId: entry.templateId });
      for (const service of services)
        if (service.itemId && !entry.lineItems?.some((item) => item.itemId === service.itemId))
          issues.push({ type: 'service_missing_dispatch', propertyId, service });
      if (entry.isInvoiceable)
        issues.push({
          type: 'billing_review',
          propertyId,
          templateId: entry.templateId,
          message: 'Confirm per-visit billing versus seasonal billing before dispatch.',
        });
    }
  }
  for (const [propertyId, entries] of roster)
    if (!accepted.has(propertyId))
      issues.push({ type: 'dispatch_without_accepted_snow_estimate', propertyId, entries });
  return {
    season,
    acceptedPropertyCount: accepted.size,
    currentTemplateCount: current.length,
    legacyTemplates: templates
      .filter((template) => !isSeason(template.name, season))
      .map((template) => ({ id: template.id, name: template.name, propertyCount: template.propertyCount })),
    issues,
    reviewOnly: true,
  };
}

async function loadReview(context, { startDate, season } = {}) {
  const today = dateKey(),
    year = Number(today.slice(0, 4));
  season = season || `${year}-${year + 1}`;
  startDate = startDate || `${year}-10-01`;
  const base = { isDeleted: false };
  const data = await call(
    context,
    'YardDeskOwnerReview',
    `query YardDeskOwnerReview($since:Date!,$today:Date!){
    invoices(where:{date:{gte:$since,lte:$today},isArchived:false,deletedState:{equals:ACTIVE},status:{in:[PENDING,PARTIALLY_PAID,PAST_DUE]}},take:2000){id number customerId date total paidAmount isSent sentEmail sentSms sentSnailMail}
    events(where:{isDeleted:false,type:{equals:VISIT},status:{equals:OPEN},startDate:{gte:$since,lt:$today}},take:5000){id title customerId startDate crewId isInvoiceable}
    estimates(where:{isDeleted:false,isArchived:false,status:{in:[CHANGE_REQUESTED,CHANGE_UPDATED]}},take:1000){id number customerId title status requestedChanges}
    smsMessages(where:{direction:OUT,status:{in:[FAILED,UNDELIVERED]}},take:500){id customerId status sentAt text}
  }`,
    { since: startDate, today },
  );
  for (const [key, limit] of [
    ['invoices', 2000],
    ['events', 5000],
    ['estimates', 1000],
    ['smsMessages', 500],
  ])
    if (!Array.isArray(data[key]) || data[key].length >= limit)
      throw new Error('The owner review exceeds its verified limits. Narrow the date range.');
  const customers = await listAll(context, {
    entity: 'customers',
    filterType: 'CustomerFilter',
    where: base,
    fields: 'id fullName phone cell email emailSendability smsSendability',
  });
  const owners = new Map(customers.map((row) => [row.id, row]));
  const attach = (row) => ({ ...row, customer: owners.get(row.customerId) || null });
  const unsent = data.invoices.filter((row) => !row.isSent).map(attach);
  return {
    success: true,
    source: SOURCE,
    asOf: new Date().toISOString(),
    startDate,
    today,
    reviewOnly: true,
    unsentInvoices: unsent,
    unsentBalance:
      unsent.reduce((n, row) => n + Math.max(0, cents(row.total) - cents(row.paidAmount)), 0) / 100,
    oldOpenVisits: data.events.map(attach),
    estimateChanges: data.estimates.map(attach),
    failedMessages: data.smsMessages.map(attach),
  };
}
async function loadSnowReadiness(context, season) {
  const year = Number(season.split('-')[0]);
  if (!/^\d{4}-\d{4}$/.test(season) || !Number.isFinite(year)) throw new Error('Choose a snow season.');
  const estimates = await listAll(context, {
    entity: 'estimates',
    filterType: 'EstimateFilter',
    where: {
      isDeleted: false,
      isArchived: false,
      status: { in: ['ACCEPTED', 'INVOICED'] },
      date: { gte: `${year}-07-01`, lte: `${year + 1}-06-30` },
      title: { contains: 'snow' },
    },
    pageSize: 100,
    fields:
      'id number customerId title status lineItems {id isIncluded propertyId name itemId price quantity}',
  });
  const data = await call(
    context,
    'YardDeskSnowTemplates',
    'query YardDeskSnowTemplates{quickDispatchTemplates(take:1000){id name propertyCount properties{id propertyId crewId isInvoiceable lineItems{itemId price quantity tax}}}}',
  );
  if (!data.quickDispatchTemplates || data.quickDispatchTemplates.length >= 1000)
    throw new Error('The snow dispatch list is incomplete.');
  return {
    success: true,
    source: SOURCE,
    asOf: new Date().toISOString(),
    ...snowReadiness(estimates, data.quickDispatchTemplates, season),
    scope:
      'Snow-titled estimates only; review other accepted estimates before declaring the roster complete.',
  };
}
module.exports = { isSeason, snowReadiness, loadReview, loadSnowReadiness };
