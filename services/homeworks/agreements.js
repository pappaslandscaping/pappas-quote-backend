'use strict';
const crypto = require('node:crypto');
const { call, listAll, id, SOURCE } = require('./records');
const ESTIMATE_FIELDS =
  'id number customerId status total terms notes acceptedAt customer{id fullName email phone cell} lineItems{id isIncluded itemId name description price quantity subtotal subtotalWithTax property{id name address{street1 street2 city state zip country}}}';
async function findEstimate(context, number) {
  const rows = await listAll(context, {
    entity: 'estimates',
    filterType: 'EstimateFilter',
    where: { isDeleted: false, isArchived: false, number: { equals: id(number) } },
    fields: ESTIMATE_FIELDS,
    pageSize: 50,
  });
  if (rows.length !== 1) throw new Error('The estimate number does not identify one Homeworks estimate.');
  return rows[0];
}
async function attachAgreement(context, { quote, estimate, pdfBytes }) {
  if (!quote.contract_signed_at || !pdfBytes?.length) throw new Error('A signed agreement PDF is required.');
  if (String(estimate.number) !== String(Number(quote.quote_number)))
    throw new Error('The signed agreement estimate number does not match Homeworks.');
  const expectedEmail = String(quote.customer_email || '')
    .trim()
    .toLowerCase();
  const emailMatches =
    expectedEmail &&
    expectedEmail ===
      String(estimate.customer?.email || '')
        .trim()
        .toLowerCase();
  const nameMatches =
    String(quote.customer_name || '')
      .trim()
      .toLowerCase() ===
    String(estimate.customer?.fullName || '')
      .trim()
      .toLowerCase();
  if (!emailMatches && !nameMatches) throw new Error('The signed agreement customer could not be verified.');
  if (!['ACCEPTED', 'INVOICED'].includes(estimate.status))
    throw new Error('The Homeworks estimate must be accepted before saving its agreement.');
  const properties = [
    ...new Set(
      estimate.lineItems
        .filter((line) => line.isIncluded)
        .map((line) => line.property?.id)
        .filter(Boolean),
    ),
  ];
  if (!properties.length) throw new Error('The accepted services need verified properties.');
  const hash = crypto
      .createHash('sha256')
      .update(
        JSON.stringify({
          quoteId: Number(quote.id),
          signedAt: new Date(quote.contract_signed_at).toISOString(),
          signature: quote.contract_signature_data || '',
        }),
      )
      .digest('hex'),
    pool = context.pool;
  await pool.query(
    "CREATE TABLE IF NOT EXISTS homeworks_document_attachments(quote_id BIGINT NOT NULL,property_id BIGINT NOT NULL,document_hash TEXT NOT NULL,upload_key TEXT,media_id BIGINT,status TEXT NOT NULL DEFAULT 'pending',error TEXT,updated_at TIMESTAMPTZ DEFAULT NOW(),PRIMARY KEY(quote_id,property_id,document_hash))",
  );
  const completed = [];
  for (const propertyId of properties) {
    const previous = await pool.query(
      'SELECT * FROM homeworks_document_attachments WHERE quote_id=$1 AND property_id=$2 AND document_hash=$3',
      [quote.id, propertyId, hash],
    );
    if (['attaching', 'uploading', 'needs_verification'].includes(previous.rows[0]?.status))
      throw new Error(
        'An earlier attachment attempt needs verification. Refresh the property before retrying.',
      );
    const property = await call(
      context,
      'YardDeskAgreementProperty',
      'query YardDeskAgreementProperty($id:SafeInt!){property(id:$id){id customerId media{id fileName}}}',
      { id: propertyId },
    );
    if (property.property?.customerId !== estimate.customerId)
      throw new Error('An agreement property belongs to a different customer.');
    if (previous.rows[0]?.status === 'confirmed') {
      if (
        !(property.property.media || []).some(
          (media) => Number(media.id) === Number(previous.rows[0].media_id),
        )
      )
        throw new Error(
          'A previously confirmed attachment is missing. Review the property before uploading again.',
        );
      completed.push(propertyId);
      continue;
    }
    const claim = await pool.query(
      "INSERT INTO homeworks_document_attachments(quote_id,property_id,document_hash,status) VALUES($1,$2,$3,'uploading') ON CONFLICT(quote_id,property_id,document_hash) DO UPDATE SET status='uploading',error=NULL,updated_at=NOW() WHERE homeworks_document_attachments.status='failed_upload' RETURNING quote_id",
      [quote.id, propertyId, hash],
    );
    if (!claim.rows.length)
      throw new Error(
        'An agreement upload is already in progress or needs verification. Refresh before retrying.',
      );
    let stage = 'uploading';
    try {
      const upload = await call(
        context,
        'YardDeskAgreementUpload',
        'mutation YardDeskAgreementUpload($size:SafeInt!){createUpload(contentType:"application/pdf",size:$size){key uploadUrl}}',
        { size: pdfBytes.length },
      );
      const target = new URL(upload.createUpload?.uploadUrl);
      if (target.protocol !== 'https:') throw new Error('The document upload destination is invalid.');
      const response = await (context.fetchImpl || fetch)(target.toString(), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/pdf' },
        body: Buffer.from(pdfBytes),
      });
      if (!response.ok) throw new Error('The agreement PDF upload failed.');
      await pool.query(
        "INSERT INTO homeworks_document_attachments(quote_id,property_id,document_hash,upload_key,status) VALUES($1,$2,$3,$4,'attaching') ON CONFLICT(quote_id,property_id,document_hash) DO UPDATE SET upload_key=EXCLUDED.upload_key,status='attaching',updated_at=NOW()",
        [quote.id, propertyId, hash, upload.createUpload.key],
      );
      stage = 'attaching';
      const attached = await call(
        context,
        'YardDeskAgreementAttach',
        'mutation YardDeskAgreementAttach($id:SafeInt!,$key:String!){addPropertyMedia(propertyId:$id,tempFileName:$key){id media{id fileName contentType}}}',
        { id: propertyId, key: upload.createUpload.key },
      );
      const old = new Set((property.property.media || []).map((row) => row.id));
      const added =
        attached.addPropertyMedia?.media?.filter(
          (row) => !old.has(row.id) && row.contentType === 'application/pdf',
        ) || [];
      if (attached.addPropertyMedia?.id !== propertyId || added.length !== 1)
        throw new Error('The agreement attachment needs verification on the property.');
      await pool.query(
        "UPDATE homeworks_document_attachments SET status='confirmed',media_id=$4,error=NULL,updated_at=NOW() WHERE quote_id=$1 AND property_id=$2 AND document_hash=$3",
        [quote.id, propertyId, hash, added[0].id],
      );
      completed.push(propertyId);
    } catch (error) {
      await pool.query(
        'UPDATE homeworks_document_attachments SET status=$4,error=$5,updated_at=NOW() WHERE quote_id=$1 AND property_id=$2 AND document_hash=$3',
        [
          quote.id,
          propertyId,
          hash,
          stage === 'uploading' ? 'failed_upload' : 'needs_verification',
          String(error.message).slice(0, 1000),
        ],
      );
      throw error;
    }
  }
  return {
    success: true,
    uploaded: true,
    source: SOURCE,
    copilot_estimate_id: estimate.id,
    property_ids: completed,
    estimate_attachment_supported: false,
  };
}
module.exports = { findEstimate, attachAgreement, ESTIMATE_FIELDS };
