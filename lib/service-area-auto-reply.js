const SERVICE_AREA_ZIPS = new Set(['44107', '44140', '44142', '44111', '44135', '44102']);
const STREET_ADDRESS_RE = /\b\d{1,6}\s+[a-z0-9.'-]+(?:\s+[a-z0-9.'-]+){0,5}\s+(?:st|street|ave|avenue|rd|road|dr|drive|ln|lane|blvd|boulevard|ct|court|cir|circle|pl|place|way|terrace|ter|pkwy|parkway|trl|trail)\b/i;
const ADDRESS_LEAD_RE = /\b(\d{1,6})\s+([a-z0-9.'-]{3,})\b/i;

function extractExplicitZip(text) {
  const source = String(text || '');
  const zipRe = /\b\d{5}(?:-\d{4})?\b/g;
  let match;
  while ((match = zipRe.exec(source))) {
    const candidate = match[0].slice(0, 5);
    const before = source.slice(Math.max(0, match.index - 16), match.index).toLowerCase();
    const after = source.slice(match.index + match[0].length, match.index + match[0].length + 32).toLowerCase();
    const hasZipLabel = /\b(zip|zipcode|zip code|postal)\s*[:#-]?\s*$/.test(before);
    const looksLikeStreetAddress = /^\s+[a-z0-9.'-]{3,}\b/.test(after) && !hasZipLabel;
    if (looksLikeStreetAddress) continue;
    return candidate;
  }
  return null;
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

function hasLikelyStreetAddress(text) {
  return STREET_ADDRESS_RE.test(String(text || ''));
}

function extractAddressLead(text) {
  const match = String(text || '').match(ADDRESS_LEAD_RE);
  if (!match) return null;
  return `${match[1]} ${match[2]}`.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function getPostalCodeFromGoogleResult(result) {
  const components = result?.address_components || [];
  const postal = components.find((component) => Array.isArray(component.types) && component.types.includes('postal_code'));
  return postal?.long_name ? String(postal.long_name).slice(0, 5) : null;
}

async function reviewAddressServiceArea(text, options = {}) {
  const source = String(text || '').trim();
  if (!source || extractExplicitZip(source) || !hasLikelyStreetAddress(source)) return null;

  const googleApiKey = options.googleApiKey || process.env.GOOGLE_MAPS_API_KEY || '';
  if (!googleApiKey) {
    return { status: 'skipped', reason: 'missing_google_maps_api_key' };
  }

  const fetchImpl = options.fetchImpl || fetch;
  const query = `${source}, Northeast Ohio`;
  const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(query)}&components=country:US&key=${googleApiKey}`;
  const response = await fetchImpl(url);
  const data = await response.json().catch(() => ({}));
  const first = data?.results?.[0];

  if (!response.ok || data?.status !== 'OK' || !first) {
    return {
      status: 'skipped',
      reason: data?.status || `http_${response.status}`,
    };
  }

  const zip = getPostalCodeFromGoogleResult(first);
  if (!zip) {
    return {
      status: 'skipped',
      reason: 'no_postal_code',
      formattedAddress: first.formatted_address || null,
    };
  }

  return {
    status: 'review',
    zip,
    inServiceArea: SERVICE_AREA_ZIPS.has(zip),
    formattedAddress: first.formatted_address || null,
  };
}

module.exports = {
  SERVICE_AREA_ZIPS,
  extractExplicitZip,
  getOutOfAreaZip,
  buildOutOfAreaAutoReply,
  hasLikelyStreetAddress,
  extractAddressLead,
  reviewAddressServiceArea,
};
