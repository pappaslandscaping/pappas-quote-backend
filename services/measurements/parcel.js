function toNumber(value) {
  const parsed = parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildAddress(property = {}) {
  return [
    property.street,
    property.city,
    property.state,
    property.zip,
  ].filter(Boolean).join(', ');
}

async function geocodeAddress(address) {
  const googleApiKey = process.env.GOOGLE_MAPS_API_KEY;
  if (!googleApiKey || !address) return null;

  const geocodeUrl = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${googleApiKey}`;
  const geocodeResponse = await fetch(geocodeUrl);
  const geocodeData = await geocodeResponse.json();
  const first = geocodeData?.results?.[0];
  if (!first?.geometry?.location) return null;

  return {
    latitude: first.geometry.location.lat,
    longitude: first.geometry.location.lng,
    formattedAddress: first.formatted_address || address,
    raw: first,
  };
}

function extractParcelBoundary(parcelFeature) {
  const geometry = parcelFeature?.geometry;
  if (!geometry || !geometry.type || !geometry.coordinates) return null;
  return {
    type: 'Feature',
    geometry: {
      type: geometry.type,
      coordinates: geometry.coordinates,
    },
    properties: {},
  };
}

async function getParcelData(lat, lng) {
  const regridToken = process.env.REGRID_API_TOKEN;
  if (!regridToken || !lat || !lng) return null;

  const url = `https://app.regrid.com/api/v2/us/parcels/point?lat=${lat}&lon=${lng}&token=${regridToken}&limit=1&radius=50`;
  const response = await fetch(url);
  if (!response.ok) return null;

  const data = await response.json();
  const features = data.parcels?.features || data.features || data.results || [];
  if (!features.length) return null;

  const parcel = features[0];
  const props = parcel.properties || {};
  const lotSizeSqFt = props.ll_gissqft || props.sqft || props.lotsqft || props.lotsizearea ||
    (props.ll_gisacre ? parseFloat(props.ll_gisacre) * 43560 : null);

  return {
    lotSizeSqFt: lotSizeSqFt ? Math.round(parseFloat(lotSizeSqFt)) : null,
    owner: props.owner || null,
    address: props.address || null,
    parcelId: props.parcelnumb || props.parcel_id || null,
    yearBuilt: props.yearbuilt || null,
    buildingSqFt: props.ll_bldg_footprint_sqft || props.bldg_sqft || null,
    acres: props.ll_gisacre || null,
    boundaryGeojson: extractParcelBoundary(parcel),
    rawData: props,
  };
}

async function resolvePropertyParcel(property = {}) {
  const address = buildAddress(property);
  const existingLat = toNumber(property.latitude);
  const existingLng = toNumber(property.longitude);

  let geocode = null;
  if (existingLat && existingLng) {
    geocode = { latitude: existingLat, longitude: existingLng, formattedAddress: address || null, raw: null };
  } else if (address) {
    geocode = await geocodeAddress(address);
  }

  const parcel = geocode ? await getParcelData(geocode.latitude, geocode.longitude) : null;

  return {
    address,
    latitude: geocode?.latitude || null,
    longitude: geocode?.longitude || null,
    formattedAddress: geocode?.formattedAddress || address || null,
    geocodeSource: existingLat && existingLng ? 'property' : (geocode ? 'google' : null),
    parcel,
  };
}

module.exports = {
  buildAddress,
  geocodeAddress,
  getParcelData,
  resolvePropertyParcel,
};
