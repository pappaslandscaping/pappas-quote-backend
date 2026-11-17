'use strict';
const records = require('./records'),
  customers = require('./customers');
const fields =
  'id customerId name notes tags size sizeType taxTotal isActive deletedStatus address{street1 street2 city state zip country} location{name latitude longitude}';
const normalize = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
function view(remote, local) {
  return {
    ...local,
    homeworks_property_id: remote.id,
    property_name: remote.name,
    street: remote.address?.street1,
    street2: remote.address?.street2,
    city: remote.address?.city,
    state: remote.address?.state,
    zip: remote.address?.zip,
    country: remote.address?.country,
    notes: remote.notes,
    tags: remote.tags,
    lot_size: remote.size,
    status: remote.isActive ? 'Active' : 'Inactive',
    tax_total: remote.taxTotal,
    source: records.SOURCE,
  };
}
async function list(context, params = {}) {
  await context.pool.query('ALTER TABLE properties ADD COLUMN IF NOT EXISTS homeworks_property_id BIGINT');
  await context.pool.query(
    'CREATE UNIQUE INDEX IF NOT EXISTS properties_homeworks_id_uidx ON properties(homeworks_property_id) WHERE homeworks_property_id IS NOT NULL',
  );
  const where = { deletedStatus: 'ACTIVE' };
  if (params.customer_id)
    where.customerId = { equals: (await customers.resolve(context, params.customer_id)).remote.id };
  const remote = await records.listAll(context, {
    entity: 'properties',
    filterType: 'PropertyFilter',
    where,
    fields,
    pageSize: 250,
  });
  const local = (await context.pool.query('SELECT * FROM properties')).rows;
  const owners = await customers.directory(context),
    ownerIds = new Map(owners.map((row) => [row.homeworks_customer_id, row.id])),
    mapped = [];
  for (const property of remote) {
    const matches = local.filter(
      (row) =>
        Number(row.homeworks_property_id) === property.id ||
        (!row.homeworks_property_id &&
          row.customer_id === ownerIds.get(property.customerId) &&
          normalize(row.street) === normalize(property.address?.street1) &&
          normalize(row.zip) === normalize(property.address?.zip)),
    );
    if (matches.length > 1)
      throw new Error(
        'More than one YardDesk property matches ' + property.name + '. Review the property linkage.',
      );
    let row = matches[0];
    if (!row) {
      const inserted = await context.pool.query(
        'INSERT INTO properties(property_name,street,street2,city,state,country,zip,notes,status,customer_id,homeworks_property_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(homeworks_property_id) WHERE homeworks_property_id IS NOT NULL DO UPDATE SET homeworks_property_id=EXCLUDED.homeworks_property_id RETURNING *',
        [
          property.name,
          property.address?.street1 || '',
          property.address?.street2 || '',
          property.address?.city || '',
          property.address?.state || '',
          property.address?.country || 'US',
          property.address?.zip || '',
          property.notes,
          property.isActive ? 'Active' : 'Inactive',
          ownerIds.get(property.customerId),
          property.id,
        ],
      );
      row = inserted.rows[0];
    } else if (!row.homeworks_property_id)
      await context.pool.query(
        'UPDATE properties SET homeworks_property_id=$2 WHERE id=$1 AND homeworks_property_id IS NULL',
        [row.id, property.id],
      );
    mapped.push({
      ...view(property, row),
      customer_display_name: owners.find((owner) => owner.homeworks_customer_id === property.customerId)
        ?.name,
    });
  }
  let rows = mapped;
  if (params.status) rows = rows.filter((row) => normalize(row.status) === normalize(params.status));
  if (params.search) {
    const term = String(params.search).toLowerCase();
    rows = rows.filter((row) =>
      [row.property_name, row.street, row.city, row.customer_display_name].some((value) =>
        String(value || '')
          .toLowerCase()
          .includes(term),
      ),
    );
  }
  if (params.city) rows = rows.filter((row) => normalize(row.city).includes(normalize(params.city)));
  return {
    success: true,
    source: records.SOURCE,
    total: rows.length,
    properties: rows.slice(
      Number(params.offset) || 0,
      (Number(params.offset) || 0) + Math.min(5000, Number(params.limit) || 1000),
    ),
  };
}
async function resolve(context, localId) {
  const result = await context.pool.query('SELECT * FROM properties WHERE id=$1', [localId]),
    local = result.rows[0];
  if (!local?.homeworks_property_id)
    throw new Error('Open the live property list to verify this property’s Homeworks link first.');
  const data = await records.call(
    context,
    'YardDeskProperty',
    'query YardDeskProperty($id:SafeInt!){property(id:$id){' + fields + '}}',
    { id: records.id(local.homeworks_property_id) },
  );
  if (!data.property || data.property.deletedStatus !== 'ACTIVE')
    throw new Error('The Homeworks property is unavailable.');
  return { remote: data.property, local };
}
async function detail(context, localId) {
  const { remote, local } = await resolve(context, localId);
  return { success: true, source: records.SOURCE, property: view(remote, local) };
}
async function update(context, localId, input) {
  const { remote } = await resolve(context, localId),
    mapped = {};
  const supported = new Set([
    'property_name',
    'notes',
    'property_notes',
    'street',
    'street2',
    'city',
    'state',
    'country',
    'zip',
    'postal_code',
    'address',
    'lot_size',
    'lawn_sqft',
  ]);
  const unsupported = Object.keys(input).filter(
    (key) => !supported.has(key) && input[key] != null && input[key] !== '',
  );
  if (unsupported.length)
    throw new Error('Review these property settings in Homeworks: ' + unsupported.join(', '));
  if (input.property_name !== undefined) mapped.name = input.property_name;
  if (input.notes !== undefined || input.property_notes !== undefined)
    mapped.notes = input.notes ?? input.property_notes;
  if (input.lot_size !== undefined || input.lawn_sqft !== undefined)
    mapped.size = String(input.lot_size ?? input.lawn_sqft);
  const address = { ...remote.address },
    names = {
      street: 'street1',
      address: 'street1',
      street2: 'street2',
      city: 'city',
      state: 'state',
      country: 'country',
      zip: 'zip',
      postal_code: 'zip',
    };
  if (Object.keys(names).some((key) => input[key] !== undefined)) {
    for (const [from, to] of Object.entries(names)) if (input[from] !== undefined) address[to] = input[from];
    mapped.address = address;
  }
  await records.call(
    context,
    'YardDeskPropertyUpdate',
    'mutation YardDeskPropertyUpdate($id:SafeInt!,$input:UpdatePropertyInput!){updateProperty(propertyId:$id,input:$input){id}}',
    { id: remote.id, input: mapped },
  );
  const result = await detail(context, localId);
  if (
    (mapped.name !== undefined && result.property.property_name !== mapped.name) ||
    (mapped.notes !== undefined && result.property.notes !== mapped.notes)
  )
    throw new Error('The property update needs verification. Refresh before retrying.');
  return result;
}
async function create(context, input) {
  if (!input.customer_id) throw new Error('Choose the Homeworks customer first.');
  const { remote: owner } = await customers.resolve(context, input.customer_id),
    address = {
      street1: input.street || input.address,
      street2: input.street2 || '',
      city: input.city || '',
      state: input.state || 'OH',
      zip: input.zip || input.postal_code || '',
      country: input.country || 'US',
    };
  if (!address.street1) throw new Error('Enter the service street address.');
  const existing = await records.listAll(context, {
    entity: 'properties',
    filterType: 'PropertyFilter',
    where: { customerId: { equals: owner.id }, deletedStatus: 'ACTIVE' },
    fields: 'id address{street1 zip}',
    pageSize: 100,
  });
  if (
    existing.some(
      (row) =>
        normalize(row.address?.street1) === normalize(address.street1) &&
        normalize(row.address?.zip) === normalize(address.zip),
    )
  )
    throw new Error('This customer already has a matching Homeworks property.');
  const result = await records.call(
    context,
    'YardDeskPropertyCreate',
    'mutation YardDeskPropertyCreate($input:PropertyInput!){createProperty(input:$input){id}}',
    {
      input: {
        customerId: owner.id,
        name: input.property_name || address.street1,
        address,
        notes: input.notes || '',
      },
    },
  );
  const properties = await list(context, { customer_id: input.customer_id });
  const row = properties.properties.find((row) => row.homeworks_property_id === result.createProperty?.id);
  if (!row) throw new Error('The new property needs verification.');
  return { success: true, source: records.SOURCE, property: row };
}
module.exports = { list, resolve, detail, update, create };
