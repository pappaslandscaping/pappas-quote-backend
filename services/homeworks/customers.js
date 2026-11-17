'use strict';
const records = require('./records');
const normalize = (value) =>
  String(value || '')
    .trim()
    .toLowerCase();
const phone = (value) =>
  String(value || '')
    .replace(/\D/g, '')
    .slice(-10);
const schemaReady = new WeakMap();
async function ensure(pool) {
  if (!schemaReady.has(pool))
    schemaReady.set(
      pool,
      pool
        .query('ALTER TABLE customers ADD COLUMN IF NOT EXISTS homeworks_customer_id BIGINT')
        .then(() =>
          pool.query(
            'CREATE UNIQUE INDEX IF NOT EXISTS customers_homeworks_id_uidx ON customers(homeworks_customer_id) WHERE homeworks_customer_id IS NOT NULL',
          ),
        )
        .catch((error) => {
          schemaReady.delete(pool);
          throw error;
        }),
    );
  await schemaReady.get(pool);
}
function view(remote, local) {
  return {
    ...local,
    homeworks_customer_id: remote.id,
    copilot_customer_id: remote.id,
    name: remote.fullName,
    type: remote.customerType?.name || '',
    tax_exempt: null,
    tax_exempt_available: false,
    first_name: remote.firstName,
    last_name: remote.lastName,
    email: remote.email,
    phone: remote.phone,
    mobile: remote.cell,
    street: remote.address?.street1,
    street2: remote.address?.street2,
    city: remote.address?.city,
    state: remote.address?.state,
    postal_code: remote.address?.zip,
    country: remote.address?.country,
    status: remote.status.toLowerCase(),
    customer_type: normalize(remote.customerType?.name) || local.customer_type,
    created_at: remote.createdAt || local.created_at,
    notes: remote.description,
    tags: remote.tags,
    source: records.SOURCE,
  };
}
async function directory(context) {
  await ensure(context.pool);
  const remote = await records.listAll(context, {
    entity: 'customers',
    filterType: 'CustomerFilter',
    where: { isDeleted: false },
    fields:
      'id fullName firstName lastName email phone cell status description tags address{street1 street2 city state zip country} customerType{name} createdAt',
  });
  const local = (await context.pool.query('SELECT * FROM customers')).rows;
  const matches = (row, owner) =>
    normalize(row.name) === normalize(owner.fullName) &&
    ((normalize(owner.email) && normalize(row.email) === normalize(owner.email)) ||
      [row.phone, row.mobile].some((value) => phone(value).length === 10 &&
        [owner.cell, owner.phone].some((candidate) => phone(candidate) === phone(value))));
  // Plan every alias before linking any of them. Shared household details cannot
  // establish which historical account belongs to which source customer.
  const claims = new Map();
  for (const row of local) {
    claims.set(row.id, remote.filter((owner) => matches(row, owner)).map((owner) => owner.id));
  }
  const result = [];
  for (const owner of remote) {
    const linked = local.filter((row) => Number(row.homeworks_customer_id) === owner.id);
    if (linked.length > 1) throw new Error('Duplicate canonical Homeworks customer linkage.');
    const aliases = local.filter((row) => !row.homeworks_customer_id && matches(row, owner));
    const ambiguous = aliases.length > 1 || aliases.some((row) => claims.get(row.id).length > 1) ||
      linked.some((row) => claims.get(row.id).length > 1);
    let row = linked[0];
    if (!row && aliases.length === 1 && !ambiguous) {
      const claimed = await context.pool.query(
        'UPDATE customers SET homeworks_customer_id=$2 WHERE id=$1 AND homeworks_customer_id IS NULL RETURNING *',
        [aliases[0].id, owner.id],
      );
      row = claimed.rows[0];
    }
    if (!row) {
      // A separate canonical mirror preserves ambiguous office history intact.
      const inserted = await context.pool.query(
        'INSERT INTO customers(customer_number,name,email,phone,mobile,street,city,state,postal_code,status,customer_type,homeworks_customer_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(homeworks_customer_id) WHERE homeworks_customer_id IS NOT NULL DO UPDATE SET homeworks_customer_id=EXCLUDED.homeworks_customer_id RETURNING *',
        [
          'HW-' + owner.id, owner.fullName, owner.email, owner.phone, owner.cell,
          owner.address?.street1, owner.address?.city, owner.address?.state, owner.address?.zip,
          owner.status.toLowerCase(), normalize(owner.customerType?.name) || 'customer', owner.id,
        ],
      );
      row = inserted.rows[0];
    }
    result.push({ ...view(owner, row), linkage_review_required: ambiguous,
      ...(ambiguous ? { linkage_note: 'Historical YardDesk contacts need review. Office history has not been merged.' } : {}) });
  }
  return result;
}
async function list(context, params = {}) {
  let rows = await directory(context),
    term = normalize(params.search || params.name || params.q);
  if (term)
    rows = rows.filter((row) =>
      [row.name, row.email, row.street, row.phone, row.mobile].some((value) =>
        normalize(value).includes(term),
      ),
    );
  if (params.status) rows = rows.filter((row) => row.status === normalize(params.status));
  if (params.type) rows = rows.filter((row) => normalize(row.customer_type) === normalize(params.type));
  if (params.city) rows = rows.filter((row) => normalize(row.city).includes(normalize(params.city)));
  rows.sort((a, b) => a.name.localeCompare(b.name) * (params.sort === 'name_desc' ? -1 : 1));
  return {
    success: true,
    source: records.SOURCE,
    total: rows.length,
    customers: rows.slice(
      Math.max(0, Number(params.offset) || 0),
      Math.max(0, Number(params.offset) || 0) + Math.min(5000, Number(params.limit) || 1000),
    ),
  };
}
async function resolve(context, localId) {
  await ensure(context.pool);
  const rows = await context.pool.query('SELECT * FROM customers WHERE id=$1', [localId]);
  if (!rows.rows[0]) throw new Error('Customer not found.');
  const local = rows.rows[0];
  const remote = await records.uniqueCustomer(context, {
    customerId: local.homeworks_customer_id || local.copilot_customer_id,
    email: local.email,
    name: local.email ? undefined : local.name,
  });
  return { local, remote };
}
async function detail(context, localId) {
  const { local, remote } = await resolve(context, localId);
  return { success: true, source: records.SOURCE, customer: view(remote, local) };
}
async function update(context, localId, input) {
  const { local, remote } = await resolve(context, localId),
    mapped = {};
  const names = {
    name: 'fullName',
    first_name: 'firstName',
    last_name: 'lastName',
    email: 'email',
    phone: 'phone',
    mobile: 'cell',
    notes: 'description',
    tags: 'tags',
  };
  const allowed = new Set([
    ...Object.keys(names),
    'street',
    'street2',
    'city',
    'state',
    'postal_code',
    'country',
    'status',
    'type',
    'customer_type',
  ]);
  const unsupported = Object.keys(input).filter((key) => !allowed.has(key));
  if (unsupported.length)
    throw new Error('Review these customer settings in Homeworks: ' + unsupported.join(', '));
  for (const [from, to] of Object.entries(names)) if (input[from] !== undefined) mapped[to] = input[from];
  if (mapped.tags !== undefined)
    mapped.tags = Array.isArray(mapped.tags)
      ? mapped.tags
      : String(mapped.tags)
          .split(',')
          .map((tag) => tag.trim())
          .filter(Boolean);
  const requestedType = input.type ?? input.customer_type;
  if (requestedType !== undefined && normalize(requestedType) !== normalize(remote.customerType?.name)) {
    const options = await records.call(
      context,
      'YardDeskCustomerTypes',
      'query YardDeskCustomerTypes{customerTypes{id name}}',
    );
    const matches = options.customerTypes.filter((type) => normalize(type.name) === normalize(requestedType));
    if (matches.length !== 1) throw new Error('Choose a supported Homeworks customer type.');
    mapped.customerTypeId = matches[0].id;
  }
  if (
    ['street', 'street2', 'city', 'state', 'postal_code', 'country'].some((key) => input[key] !== undefined)
  )
    mapped.address = {
      ...remote.address,
      street1: input.street ?? remote.address?.street1,
      street2: input.street2 ?? remote.address?.street2,
      city: input.city ?? remote.address?.city,
      state: input.state ?? remote.address?.state,
      zip: input.postal_code ?? remote.address?.zip,
      country: input.country ?? remote.address?.country,
    };
  if (input.status && !['ACTIVE', 'INACTIVE'].includes(String(input.status).toUpperCase()))
    throw new Error('Choose active or inactive in Homeworks.');
  if (Object.keys(mapped).length)
    await records.call(
      context,
      'YardDeskCustomerUpdate',
      'mutation YardDeskCustomerUpdate($id:SafeInt!,$input:CustomerInput!){updateCustomer(customerId:$id,input:$input){id}}',
      { id: remote.id, input: mapped },
    );
  if (input.status) {
    const status = String(input.status).toUpperCase();
    if (!['ACTIVE', 'INACTIVE'].includes(status)) throw new Error('Choose active or inactive in Homeworks.');
    await records.call(
      context,
      'YardDeskCustomerStatus',
      'mutation YardDeskCustomerStatus($id:SafeInt!,$status:CustomerStatus!){setCustomerStatus(customerId:$id,status:$status){id status}}',
      { id: remote.id, status },
    );
  }
  const check = await records.uniqueCustomer(context, { customerId: remote.id });
  for (const [key, value] of Object.entries(mapped))
    if (
      key !== 'address' &&
      (key === 'tags'
        ? JSON.stringify([...(check.tags || [])].sort()) !== JSON.stringify([...value].sort())
        : JSON.stringify(check[key]) !== JSON.stringify(value))
    )
      throw new Error('The customer update needs verification. Refresh before retrying.');
  if (input.status && check.status !== String(input.status).toUpperCase())
    throw new Error('The customer status needs verification. Refresh before retrying.');
  if (
    mapped.address &&
    Object.entries(mapped.address).some(([key, value]) => (check.address?.[key] ?? '') !== (value ?? ''))
  )
    throw new Error('The customer address needs verification. Refresh before retrying.');
  return { success: true, source: records.SOURCE, customer: view(check, local) };
}
async function create(context, input) {
  const name = String(input.name || [input.first_name, input.last_name].filter(Boolean).join(' ')).trim();
  if (!name) throw new Error('Enter the customer’s full name.');
  const existing = await records.listAll(context, {
    entity: 'customers',
    filterType: 'CustomerFilter',
    where: { isDeleted: false, fullName: { equals: name } },
    fields: 'id fullName email phone cell',
  });
  if (
    existing.some(
      (row) =>
        (normalize(row.email) && normalize(row.email) === normalize(input.email)) ||
        (phone(row.cell || row.phone).length === 10 &&
          phone(row.cell || row.phone) === phone(input.mobile || input.phone)),
    )
  )
    throw new Error(
      'A matching Homeworks customer already exists. Review that account before creating another.',
    );
  const types = await records.call(
    context,
    'YardDeskCustomerTypes',
    'query YardDeskCustomerTypes{customerTypes{id name}}',
  );
  const requested = normalize(input.type || input.customer_type || 'customer'),
    matches = (types.customerTypes || []).filter((row) => normalize(row.name) === requested);
  if (matches.length !== 1) throw new Error('Choose a supported Homeworks customer type.');
  const remote = {
    fullName: name,
    email: input.email || '',
    phone: input.phone || '',
    cell: input.mobile || '',
    customerTypeId: matches[0].id,
    description: input.notes || '',
    address: {
      street1: input.street || '',
      street2: input.street2 || '',
      city: input.city || '',
      state: input.state || 'OH',
      zip: input.postal_code || input.zip || '',
      country: input.country || 'US',
    },
  };
  const result = await records.call(
    context,
    'YardDeskCreateCustomer',
    'mutation YardDeskCreateCustomer($input:CustomerInput!){createCustomer(input:$input){id}}',
    { input: remote },
  );
  const rows = await directory(context),
    customer = rows.find((row) => row.homeworks_customer_id === result.createCustomer?.id);
  if (!customer) throw new Error('The new Homeworks customer needs verification. Refresh before retrying.');
  return { success: true, source: records.SOURCE, customer, customer_id: customer.id };
}
module.exports = { directory, list, resolve, detail, update, view, create };
