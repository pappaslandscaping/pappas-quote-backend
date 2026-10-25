const { read } = require('../public/js/snow-agreement');

// The official API can attach media to properties, but currently has no
// addEstimateMedia mutation. Keep the signed PDF on its service properties.
async function syncSnowAgreementToHomeWorks({ quote, pdfBytes, pool, query, fetchImpl = global.fetch }) {
  try {
    const doc = read(quote);
    if (!doc || !pdfBytes?.length || !quote.contract_signed_at) throw new Error('A signed snow agreement PDF is required.');
    const estimateId = Number(doc.source.estimate_id);
    if (!Number.isSafeInteger(estimateId) || estimateId <= 0) throw new Error('Missing official HomeWorks estimate ID.');
    const properties = [...new Set(doc.services.map(s => Number(s.property_id)))];
    if (!properties.length || properties.some(id => !Number.isSafeInteger(id) || id <= 0)) throw new Error('Missing service property ID on the accepted estimate.');
    const call = (operationName, graphql, variables) => query({ pool, operationName, query: graphql, variables });
    const data = await call('YardDeskSnowContractEstimate', 'query YardDeskSnowContractEstimate($id: SafeInt!) { estimate(estimateId: $id) { id number lineItems { removed property { id } } } }', { id: estimateId });
    const estimate = data.estimate;
    const liveProperties = new Set((estimate?.lineItems || []).filter(s => s.removed !== 'REMOVED').map(s => s.property?.id));
    if (String(estimate?.number) !== doc.estimate_number || properties.some(id => !liveProperties.has(id))) throw new Error('The HomeWorks estimate and signed agreement property references do not match.');
    const previous = await pool.query("SELECT details FROM quote_events WHERE sent_quote_id = $1 AND event_type = 'snow_pdf_synced'", [quote.id]);
    const done = new Set(previous.rows.map(row => typeof row.details === 'string' ? JSON.parse(row.details) : row.details).filter(d => d?.document_hash === quote.contract_document_hash).map(d => d.property_id));
    for (const propertyId of properties) {
      if (done.has(propertyId)) continue;
      const uploaded = await call('YardDeskSnowContractUpload', 'mutation YardDeskSnowContractUpload($size: SafeInt!) { createUpload(contentType: "application/pdf", size: $size) { key uploadUrl } }', { size: pdfBytes.length });
      if (!uploaded.createUpload?.key || !uploaded.createUpload?.uploadUrl) throw new Error('HomeWorks did not provide a PDF upload destination.');
      const url = new URL(uploaded.createUpload.uploadUrl);
      if (url.protocol !== 'https:') throw new Error('HomeWorks returned an insecure upload destination.');
      const response = await fetchImpl(url.toString(), { method: 'PUT', headers: { 'Content-Type': 'application/pdf' }, body: Buffer.from(pdfBytes) });
      if (!response.ok) throw new Error(`Signed PDF upload failed (${response.status}).`);
      const attached = await call('YardDeskSnowContractAttach', 'mutation YardDeskSnowContractAttach($id: SafeInt!, $key: String!) { addPropertyMedia(propertyId: $id, tempFileName: $key) { id } }', { id: propertyId, key: uploaded.createUpload.key });
      if (Number(attached.addPropertyMedia?.id) !== propertyId) throw new Error('HomeWorks did not confirm the property attachment.');
      await pool.query('INSERT INTO quote_events (sent_quote_id, event_type, description, details) VALUES ($1, $2, $3, $4)', [quote.id, 'snow_pdf_synced', `Signed snow agreement saved to HomeWorks property ${propertyId}`, JSON.stringify({ property_id: propertyId, estimate_id: estimateId, document_hash: quote.contract_document_hash })]);
    }
    return { success: true, uploaded: true, copilot_estimate_id: estimateId, property_ids: properties };
  } catch (error) {
    return { success: false, error: error.message };
  }
}
module.exports = { syncSnowAgreementToHomeWorks };
