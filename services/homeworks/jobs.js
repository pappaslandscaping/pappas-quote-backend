'use strict';
const records = require('./records');
const { eventIdFromKey } = require('./dispatch');
const { mapHomeWorksEventToLiveJob } = require('./client');
const fields =
  'id title description type status startDate startTime endTime total budgetedHours sortOrder customerId propertyId crewId recurringEventId isInvoiceable invoiceId updatedAt customer{id fullName email phone cell} property{id name address{street1 street2 city state zip country} location{name latitude longitude}} location{name latitude longitude} locationName crew{id name} users{id firstName lastName} recurringEvent{frequency weeklyCount} routeStops{routeId routeDate stopOrder}';
function map(event) {
  const row = mapHomeWorksEventToLiveJob(event);
  return {
    ...row,
    id: `copilot:${event.startDate}:${event.id}`,
    job_key: `copilot:${event.startDate}:${event.id}`,
    homeworks_event_id: event.id,
    job_date: event.startDate,
    service_type: event.title,
    service_price: Number(event.total),
    crew_assigned: event.crew?.name || null,
    customer_name: event.customer?.fullName,
    customer_id: event.customerId,
    property_id: event.propertyId,
    status: {
      OPEN: 'pending',
      CLOSED: 'completed',
      SKIPPED: 'skipped',
      CANCELLED: 'cancelled',
      WAITLISTED: 'waitlisted',
    }[event.status],
    homeworks_status: event.status,
    is_invoiceable: event.isInvoiceable,
    invoice_id: event.invoiceId,
    source: records.SOURCE,
    route_order: row.stop_order,
    special_notes: event.description,
    is_recurring: !!event.recurringEventId,
    service_frequency: row.raw_data.frequency,
  };
}
async function resolveId(context, value) {
  if (/^(copilot|homeworks):/.test(String(value))) return eventIdFromKey(value);
  const local = await context.pool.query('SELECT copilot_visit_id FROM scheduled_jobs WHERE id=$1', [value]);
  if (!local.rows[0]?.copilot_visit_id)
    throw new Error('This historical job is not linked to Homeworks. Select the live visit.');
  return records.id(local.rows[0].copilot_visit_id);
}
async function detail(context, value) {
  const eventId = await resolveId(context, value);
  const data = await records.call(
    context,
    'YardDeskVisit',
    'query YardDeskVisit($id:SafeInt!){event(eventId:$id){' + fields + '}}',
    { id: eventId },
  );
  if (!data.event) throw new Error('The Homeworks visit is unavailable.');
  return data.event;
}
async function list(context, params = {}) {
  const start = params.date || params.start_date || records.dateKey(),
    end = params.date || params.end_date || start;
  const rows = await records.listAll(context, {
    entity: 'events',
    filterType: 'EventFilter',
    where: { isDeleted: false, startDate: { gte: start, lte: end } },
    fields,
    pageSize: 100,
  });
  let jobs = rows.filter((row) => row.status !== 'CANCELLED').map(map);
  if (params.status) jobs = jobs.filter((row) => row.status === params.status);
  if (params.crew) jobs = jobs.filter((row) => row.crew_assigned === params.crew);
  if (params.search) {
    const term = String(params.search).toLowerCase();
    jobs = jobs.filter((row) =>
      [row.customer_name, row.service_type, row.address].some((value) =>
        String(value || '')
          .toLowerCase()
          .includes(term),
      ),
    );
  }
  return { success: true, source: records.SOURCE, jobs, total: jobs.length };
}
async function changeStatus(context, value, status, body = {}) {
  const event = await detail(context, value);
  const target = { completed: 'CLOSED', closed: 'CLOSED', pending: 'OPEN', open: 'OPEN', skipped: 'SKIPPED' }[
    String(status).toLowerCase()
  ];
  if (!target) throw new Error('Choose completed, skipped, or open.');
  if (event.status === target) return { success: true, job: map(event), alreadyApplied: true };
  if (target === 'CLOSED' && event.isInvoiceable && body.confirm_billing !== true)
    throw new Error(
      'Completing this visit can create a Homeworks invoice. Confirm the billing effect first.',
    );
  if (target === 'SKIPPED' && !String(body.skipped_reason || body.reason || '').trim())
    throw new Error('Enter the reason for skipping this visit.');
  if (body.completion_notes) {
    const note = String(body.completion_notes).trim();
    if (note.length > 5000) throw new Error('Keep completion notes within 5,000 characters.');
    await records.call(
      context,
      'YardDeskCompletionNote',
      'mutation YardDeskCompletionNote($id:SafeInt!,$input:DispatchNoteInput!){createEventDispatchNote(eventId:$id,input:$input){id}}',
      {
        id: event.id,
        input: { message: (body.completed_by ? 'Completed by ' + body.completed_by + '\n' : '') + note },
      },
    );
  }
  const operation = target === 'CLOSED' ? 'closeEvent' : target === 'SKIPPED' ? 'skipEvent' : 'reopenEvent';
  const extra = target === 'SKIPPED' ? ',skippedReason:$reason' : '';
  await records.call(
    context,
    'YardDeskVisitStatus',
    `mutation YardDeskVisitStatus($id:SafeInt!${target === 'SKIPPED' ? ', $reason:String!' : ''}){${operation}(eventId:$id${extra}){id status}}`,
    { id: event.id, ...(target === 'SKIPPED' ? { reason: body.skipped_reason || body.reason } : {}) },
  );
  const check = await detail(context, `copilot:${event.startDate}:${event.id}`);
  if (check.status !== target)
    throw new Error('Homeworks did not confirm the visit status. Refresh before retrying.');
  return {
    success: true,
    source: records.SOURCE,
    job: map(check),
    billingPending: target === 'CLOSED' && event.isInvoiceable && !check.invoiceId,
  };
}
async function update(context, value, input) {
  const event = await detail(context, value),
    allowed = new Set([
      'job_date',
      'service_type',
      'special_notes',
      'crew_assigned',
      'is_invoiceable',
      'status',
      'reason',
      'skipped_reason',
      'confirm_billing',
    ]);
  const unknown = Object.keys(input).filter((key) => !allowed.has(key));
  if (unknown.length) throw new Error('These fields need a reviewed Homeworks edit: ' + unknown.join(', '));
  if (
    input.status &&
    Object.keys(input).some((key) => !['status', 'reason', 'skipped_reason', 'confirm_billing'].includes(key))
  )
    throw new Error('Save visit details before changing completion status.');
  if (input.status) return changeStatus(context, value, input.status, input);
  const mutations = [];
  if (input.job_date) mutations.push(['scheduleEvent', 'startDate', 'LocalDate!', input.job_date]);
  if (input.service_type !== undefined)
    mutations.push(['updateEventTitle', 'title', 'String!', input.service_type]);
  if (input.special_notes !== undefined)
    mutations.push(['updateEventDescription', 'description', 'String!', input.special_notes]);
  if (input.is_invoiceable !== undefined)
    mutations.push(['updateEventInvoiceable', 'isInvoiceable', 'Boolean!', input.is_invoiceable === true]);
  let expectedCrew;
  if (input.crew_assigned !== undefined) {
    const data = await records.call(context, 'YardDeskCrews', 'query YardDeskCrews{crews{id name}}');
    const crews = data.crews.filter((row) => row.name === input.crew_assigned);
    if (input.crew_assigned && crews.length !== 1) throw new Error('Choose one Homeworks crew.');
    expectedCrew = crews[0]?.id ?? null;
    mutations.push(['assignEventCrew', 'crewId', 'SafeInt', expectedCrew]);
  }
  if (!mutations.length) throw new Error('No supported visit changes were supplied.');
  for (const [operation, field, type, value] of mutations)
    await records.call(
      context,
      'YardDeskVisitUpdate',
      `mutation YardDeskVisitUpdate($id:SafeInt!,$value:${type}){${operation}(eventId:$id,${field}:$value){id}}`,
      { id: event.id, value },
    );
  const check = await detail(context, `copilot:${event.startDate}:${event.id}`);
  if (
    (input.job_date && check.startDate !== input.job_date) ||
    (input.service_type !== undefined && check.title !== input.service_type) ||
    (input.special_notes !== undefined && check.description !== input.special_notes) ||
    (input.is_invoiceable !== undefined && check.isInvoiceable !== input.is_invoiceable)
  )
    throw new Error('The Homeworks edit needs verification. Refresh before retrying.');
  if (input.crew_assigned !== undefined && (check.crewId ?? null) !== expectedCrew)
    throw new Error('The crew assignment needs verification. Refresh before retrying.');
  return { success: true, source: records.SOURCE, job: map(check) };
}
async function create(context, input) {
  if (typeof input.is_invoiceable !== 'boolean')
    throw new Error('Choose whether completing this visit should create a Homeworks invoice.');
  const owner = await records.uniqueCustomer(context, {
    customerId: input.homeworks_customer_id,
    name: input.customer_name,
    email: input.customer_email,
  });
  if (
    !input.homeworks_customer_id &&
    !input.customer_email &&
    String(owner.fullName).trim().toLowerCase() !==
      String(input.customer_name || '')
        .trim()
        .toLowerCase()
  )
    throw new Error('Select the full Homeworks customer name.');
  const date = input.job_date || input.date;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date || '')) throw new Error('Choose a visit date.');
  if (
    input.service_frequency &&
    /biweekly|weekly|monthly|recurring/i.test(input.service_frequency) &&
    input.create_recurring === true
  )
    throw new Error('Set up the recurring series in Homeworks. Its API currently creates individual visits.');
  const data = await records.call(
    context,
    'YardDeskNewVisitOptions',
    'query YardDeskNewVisitOptions($customerId:SafeInt!){properties(where:{customerId:{equals:$customerId},isActive:true},take:100){id customerId taxTotal address{street1 city state zip}} items(where:{isDeleted:false},take:5000){id name} crews{id name}}',
    { customerId: owner.id },
  );
  if (data.properties?.length >= 100 || data.items?.length >= 5000)
    throw new Error('The service or property list is incomplete.');
  const normalize = (value) =>
    String(value || '')
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '');
  const properties = (data.properties || []).filter((property) =>
    input.homeworks_property_id
      ? property.id === Number(input.homeworks_property_id)
      : normalize(input.address).includes(normalize(property.address?.street1)) &&
        normalize(property.address?.street1).length > 3,
  );
  if (properties.length !== 1) throw new Error('Choose one verified Homeworks property for the visit.');
  const items = (data.items || []).filter((item) =>
    input.item_id
      ? item.id === Number(input.item_id)
      : normalize(item.name) === normalize(input.service_type),
  );
  if (items.length !== 1) throw new Error('Choose a service from the Homeworks catalog.');
  const crews = (data.crews || []).filter((crew) => crew.name === input.crew_assigned);
  if (input.crew_assigned && crews.length !== 1) throw new Error('Choose one Homeworks crew.');
  const price = String(input.service_price ?? '');
  if (price === '' || !Number.isFinite(Number(price)) || Number(price) < 0)
    throw new Error('Review the visit price.');
  const existing = await records.listAll(context, {
    entity: 'events',
    filterType: 'EventFilter',
    where: {
      isDeleted: false,
      customerId: { equals: owner.id },
      propertyId: { equals: properties[0].id },
      startDate: { equals: date },
      title: { equals: input.service_type },
    },
    fields: 'id status',
    pageSize: 100,
  });
  if (existing.length)
    throw new Error('A matching visit already exists on this date. Review it before creating another.');
  const remote = {
    type: 'VISIT',
    title: input.service_type,
    startDate: date,
    customerId: owner.id,
    propertyId: properties[0].id,
    isInvoiceable: input.is_invoiceable === true,
    description: [input.special_notes, input.property_notes].filter(Boolean).join('\n'),
    lineItems: [{ itemId: items[0].id, price, quantity: '1', tax: String(properties[0].taxTotal || 0) }],
    ...(crews[0] ? { crewId: crews[0].id } : {}),
  };
  const result = await records.call(
    context,
    'YardDeskCreateVisit',
    'mutation YardDeskCreateVisit($input:CreateEventInput!){createEvent(input:$input){id startDate}}',
    { input: remote },
  );
  const eventId = records.id(result.createEvent?.id),
    check = await detail(context, `copilot:${date}:${eventId}`);
  if (check.customerId !== owner.id || check.propertyId !== properties[0].id || check.startDate !== date)
    throw new Error('The new Homeworks visit needs verification. Refresh before retrying.');
  return {
    success: true,
    source: records.SOURCE,
    job: map(check),
    billingReviewRequired: !check.isInvoiceable,
  };
}
async function remove(context, values) {
  const events = [];
  for (const value of values) events.push(await detail(context, value));
  if (events.some((event) => event.invoiceId || !['OPEN', 'WAITLISTED'].includes(event.status)))
    throw new Error(
      'Only unbilled open or waitlisted visits can be deleted here. Review completed work in Homeworks.',
    );
  await records.call(
    context,
    'YardDeskDeleteVisits',
    'mutation YardDeskDeleteVisits($ids:[SafeInt!]!){deleteEvents(eventIds:$ids)}',
    { ids: events.map((event) => event.id) },
  );
  const check = await records.call(
    context,
    'YardDeskVerifyDeletedVisits',
    'query YardDeskVerifyDeletedVisits($ids:[SafeInt!]!){events(where:{id:{in:$ids},isDeleted:false},take:1000){id}}',
    { ids: events.map((event) => event.id) },
  );
  if (check.events?.length) throw new Error('The visit deletion could not be verified.');
  return { success: true, source: records.SOURCE, deleted: events.length };
}
async function uploadPhotos(context, value, files) {
  const event = await detail(context, value);
  if (!files?.length || files.length > 10) throw new Error('Select one to ten visit photos.');
  for (const file of files)
    if (
      !/^image\/(jpeg|png|webp|heic|heif)$/.test(file.mimetype || '') ||
      !file.buffer?.length ||
      file.buffer.length > 20 * 1024 * 1024
    )
      throw new Error('Choose supported photos up to 20 MB each.');
  const group = await records.call(
    context,
    'YardDeskVisitPhotoGroup',
    'mutation YardDeskVisitPhotoGroup($id:SafeInt!){addBeforeAndAfter(eventId:$id){id}}',
    { id: event.id },
  );
  const groupId = records.id(group.addBeforeAndAfter?.id),
    photos = [];
  for (const file of files) {
    const data = await records.call(
      context,
      'YardDeskVisitPhotoUpload',
      'mutation YardDeskVisitPhotoUpload($type:String!,$size:SafeInt!){createUpload(contentType:$type,size:$size){key uploadUrl}}',
      { type: file.mimetype, size: file.buffer.length },
    );
    const target = new URL(data.createUpload?.uploadUrl);
    if (target.protocol !== 'https:') throw new Error('The photo upload destination is invalid.');
    const response = await (context.fetchImpl || fetch)(target.toString(), {
      method: 'PUT',
      headers: { 'Content-Type': file.mimetype },
      body: file.buffer,
    });
    if (!response.ok) throw new Error('A photo could not be uploaded. Check the visit before retrying.');
    const attached = await records.call(
      context,
      'YardDeskAttachVisitPhoto',
      'mutation YardDeskAttachVisitPhoto($id:SafeInt!,$key:String!){addAfterMedia(beforeAndAfterId:$id,tempFileName:$key){id eventId afterMedia{id url}}}',
      { id: groupId, key: data.createUpload.key },
    );
    if (attached.addAfterMedia?.eventId !== event.id)
      throw new Error('The photo attachment could not be verified.');
    photos.splice(0, photos.length, ...(attached.addAfterMedia.afterMedia || []).map((row) => row.url));
  }
  if (photos.length !== files.length)
    throw new Error('Some photo attachments need review on the Homeworks visit.');
  return { success: true, source: records.SOURCE, photos };
}
module.exports = { fields, map, resolveId, detail, list, changeStatus, update, create, remove, uploadPhotos };
