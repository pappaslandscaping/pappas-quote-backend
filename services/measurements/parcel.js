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
  if (!googleApiKey) {
    return { error: { reason: 'missing_google_maps_api_key', message: 'GOOGLE_MAPS_API_KEY is not configured.' } };
  }
  if (!address) {
    return { error: { reason: 'missing_property_address', message: 'Property address is incomplete.' } };
  }

  try {
    const geocodeUrl = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${googleApiKey}`;
    const geocodeResponse = await fetch(geocodeUrl);
    const geocodeData = await geocodeResponse.json();
    const first = geocodeData?.results?.[0];
    if (!geocodeResponse.ok) {
      return {
        error: {
          reason: 'google_geocode_http_error',
          message: `Google geocode request failed with HTTP ${geocodeResponse.status}.`,
          status: geocodeResponse.status,
        },
      };
    }
    if (!first?.geometry?.location) {
      return {
        error: {
          reason: 'google_geocode_failed',
          message: `Google geocode returned ${geocodeData?.status || 'no results'}.`,
          status: geocodeData?.status || null,
        },
      };
    }

    return {
      latitude: first.geometry.location.lat,
      longitude: first.geometry.location.lng,
      formattedAddress: first.formatted_address || address,
      raw: first,
    };
  } catch (error) {
    return {
      error: {
        reason: 'google_geocode_exception',
        message: error.message || 'Google geocode threw an exception.',
      },
    };
  }
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
  if (!regridToken) {
    return { parcel: null, error: { reason: 'missing_regrid_api_token', message: 'REGRID_API_TOKEN is not configured.' } };
  }
  if (!lat || !lng) {
    return { parcel: null, error: { reason: 'missing_coordinates_for_parcel_lookup', message: 'Parcel lookup skipped because coordinates are missing.' } };
  }

  try {
    const url = `https://app.regrid.com/api/v2/us/parcels/point?lat=${lat}&lon=${lng}&token=${regridToken}&limit=1&radius=50`;
    const response = await fetch(url);
    if (!response.ok) {
      return {
        parcel: null,
        error: {
          reason: 'regrid_http_error',
          message: `Regrid parcel lookup failed with HTTP ${response.status}.`,
          status: response.status,
        },
      };
    }

    const data = await response.json();
    const features = data.parcels?.features || data.features || data.results || [];
    if (!features.length) {
      return {
        parcel: null,
        error: {
          reason: 'regrid_no_parcel_found',
          message: 'Regrid did not return a parcel for this location.',
        },
      };
    }

    const parcel = features[0];
    const props = parcel.properties || {};
    const lotSizeSqFt = props.ll_gissqft || props.sqft || props.lotsqft || props.lotsizearea ||
      (props.ll_gisacre ? parseFloat(props.ll_gisacre) * 43560 : null);

    return {
      parcel: {
        lotSizeSqFt: lotSizeSqFt ? Math.round(parseFloat(lotSizeSqFt)) : null,
        owner: props.owner || null,
        address: props.address || null,
        parcelId: props.parcelnumb || props.parcel_id || null,
        yearBuilt: props.yearbuilt || null,
        buildingSqFt: props.ll_bldg_footprint_sqft || props.bldg_sqft || null,
        acres: props.ll_gisacre || null,
        boundaryGeojson: extractParcelBoundary(parcel),
        rawData: props,
      },
      error: null,
    };
  } catch (error) {
    return {
      parcel: null,
      error: {
        reason: 'regrid_exception',
        message: error.message || 'Regrid parcel lookup threw an exception.',
      },
    };
  }
}

async function resolvePropertyParcel(property = {}) {
  const address = buildAddress(property);
  const existingLat = toNumber(property.latitude);
  const existingLng = toNumber(property.longitude);
  const diagnostics = {
    reasons: [],
    messages: [],
  };

  let geocode = null;
  if (existingLat && existingLng) {
    geocode = { latitude: existingLat, longitude: existingLng, formattedAddress: address || null, raw: null };
  } else if (address) {
    geocode = await geocodeAddress(address);
    if (geocode?.error) {
      diagnostics.reasons.push(geocode.error.reason);
      diagnostics.messages.push(geocode.error.message);
      geocode = null;
    }
  } else {
    diagnostics.reasons.push('missing_property_address');
    diagnostics.messages.push('Property address is incomplete.');
  }

  let parcel = null;
  if (geocode) {
    const parcelLookup = await getParcelData(geocode.latitude, geocode.longitude);
    parcel = parcelLookup?.parcel || null;
    if (parcelLookup?.error) {
      diagnostics.reasons.push(parcelLookup.error.reason);
      diagnostics.messages.push(parcelLookup.error.message);
    }
  } else {
    diagnostics.reasons.push('missing_coordinates_for_parcel_lookup');
    diagnostics.messages.push('Parcel lookup skipped because coordinates are missing.');
  }

  return {
    address,
    latitude: geocode?.latitude || null,
    longitude: geocode?.longitude || null,
    formattedAddress: geocode?.formattedAddress || address || null,
    geocodeSource: existingLat && existingLng ? 'property' : (geocode ? 'google' : null),
    parcel,
    diagnostics,
  };
}

module.exports = {
  buildAddress,
  geocodeAddress,
  getParcelData,
  resolvePropertyParcel,
};
