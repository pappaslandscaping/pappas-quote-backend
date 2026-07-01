const SERVICE_AREA_ZIPS = new Set(['44107', '44140', '44142', '44111', '44135', '44102']);

function extractExplicitZip(text) {
  const source = String(text || '');
  const matches = source.match(/\b\d{5}(?:-\d{4})?\b/g) || [];
  if (!matches[0]) return null;
  return matches[0].slice(0, 5);
}

function getOutOfAreaZip(text) {
  const zip = extractExplicitZip(text);
  if (!zip || SERVICE_AREA_ZIPS.has(zip)) return null;
  return zip;
}

function buildOutOfAreaAutoReply(source) {
  if (source === 'voicemail') {
    return "Hi, we received your voicemail. Unfortunately, we don't service your property area at this time. Thank you for reaching out to Pappas & Co. Landscaping.";
  }

  return "Hi, thanks for reaching out to Pappas & Co. Landscaping. Unfortunately, we don't service your property area at this time.";
}

module.exports = {
  SERVICE_AREA_ZIPS,
  extractExplicitZip,
  getOutOfAreaZip,
  buildOutOfAreaAutoReply,
};
