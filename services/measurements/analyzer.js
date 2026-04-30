const { resolvePropertyParcel } = require('./parcel');

const DEFAULT_RATIOS = {
  lawn: 0.6,
  mulch_bed: 0.1,
  hardscape: 0.2,
};

function unique(list = []) {
  return [...new Set(list.filter(Boolean))];
}

function buildFallbackMessages(reasons = []) {
  const messageByReason = {
    missing_google_maps_api_key: 'GOOGLE_MAPS_API_KEY is not configured.',
    missing_property_address: 'Property address is incomplete.',
    google_geocode_http_error: 'Google geocode request failed.',
    google_geocode_failed: 'Google could not geocode this address.',
    google_geocode_exception: 'Google geocode threw an exception.',
    missing_coordinates_for_parcel_lookup: 'Parcel lookup was skipped because coordinates are missing.',
    missing_regrid_api_token: 'REGRID_API_TOKEN is not configured.',
    regrid_http_error: 'Regrid parcel lookup failed.',
    regrid_no_parcel_found: 'No parcel was found for this property.',
    regrid_exception: 'Regrid parcel lookup threw an exception.',
    missing_fal_api_key: 'FAL_API_KEY is not configured.',
    missing_static_map_url: 'Static imagery URL could not be built.',
    sam3_request_failed: 'SAM3 segmentation request failed.',
    sam3_no_usable_masks: 'SAM3 did not return usable masks for lawn, beds, or hardscape.',
  };
  return unique(reasons).map((reason) => messageByReason[reason] || reason);
}

async function estimatePixelRatio(maskUrl) {
  try {
    const response = await fetch(maskUrl);
    const buffer = await response.arrayBuffer();
    const fileSize = new Uint8Array(buffer).length;

    if (fileSize < 5000) return 0.02;
    if (fileSize < 10000) return 0.08;
    if (fileSize < 20000) return 0.15;
    if (fileSize < 40000) return 0.25;
    if (fileSize < 60000) return 0.35;
    if (fileSize < 80000) return 0.45;
    if (fileSize < 100000) return 0.55;
    return 0.65;
  } catch (_error) {
    return 0;
  }
}

async function segmentWithSAM3(apiKey, imageUrl, prompt) {
  const response = await fetch('https://fal.run/fal-ai/sam-3/image', {
    method: 'POST',
    headers: {
      Authorization: `Key ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      image_url: imageUrl,
      prompt,
      apply_mask: true,
      output_format: 'png',
      return_multiple_masks: false,
      include_scores: true,
    }),
  });

  if (!response.ok) {
    throw new Error(`SAM 3 API error: ${response.status}`);
  }

  const data = await response.json();
  const maskUrl = data.masks?.[0]?.url || data.image?.url || null;
  const score = data.scores?.[0] || data.metadata?.[0]?.score || 0.5;
  const pixelRatio = maskUrl ? await estimatePixelRatio(maskUrl) : 0;
  return { maskUrl, score, pixelRatio };
}

function buildStaticMapUrl(latitude, longitude, zoom = 19) {
  const googleApiKey = process.env.GOOGLE_MAPS_API_KEY;
  if (!googleApiKey || !latitude || !longitude) return null;
  return `https://maps.googleapis.com/maps/api/staticmap?center=${latitude},${longitude}&zoom=${zoom}&size=640x640&maptype=satellite&key=${googleApiKey}`;
}

function buildFeatureSet(analysis = {}) {
  const lotArea = analysis.totalLot || 0;
  const lawnArea = analysis.lawnArea || 0;
  const bedArea = analysis.bedArea || 0;
  const hardscapeArea = analysis.hardscapeArea || 0;
  const treeCount = analysis.shrubCount || 0;

  const hardEdge = Math.round(Math.sqrt(Math.max(hardscapeArea, 0)) * 4);
  const softEdge = Math.round(Math.sqrt(Math.max(bedArea + lawnArea, 0)) * 1.15);

  return [
    {
      feature_key: 'lot_area',
      feature_label: 'Lot Area',
      feature_group: 'surface',
      geometry_type: 'derived',
      unit: 'sqft',
      quantity: lotArea,
      confidence: analysis.confidence?.lotSize || null,
      metadata: { derived: true },
    },
    {
      feature_key: 'lawn',
      feature_label: 'Lawn',
      feature_group: 'surface',
      geometry_type: 'derived',
      unit: 'sqft',
      quantity: lawnArea,
      confidence: analysis.confidence?.ratios || null,
      metadata: { derived: true },
    },
    {
      feature_key: 'mulch_bed',
      feature_label: 'Mulch Bed',
      feature_group: 'surface',
      geometry_type: 'derived',
      unit: 'sqft',
      quantity: bedArea,
      confidence: analysis.confidence?.ratios || null,
      metadata: { derived: true },
    },
    {
      feature_key: 'hardscape',
      feature_label: 'Hardscape',
      feature_group: 'surface',
      geometry_type: 'derived',
      unit: 'sqft',
      quantity: hardscapeArea,
      confidence: analysis.confidence?.ratios || null,
      metadata: { derived: true },
    },
    {
      feature_key: 'hard_edge',
      feature_label: 'Hard Edge',
      feature_group: 'edge',
      geometry_type: 'derived',
      unit: 'ft',
      quantity: hardEdge,
      confidence: analysis.confidence?.ratios || null,
      metadata: { derived: true, heuristic: 'sqrt(hardscape)*4' },
    },
    {
      feature_key: 'soft_edge',
      feature_label: 'Soft Edge',
      feature_group: 'edge',
      geometry_type: 'derived',
      unit: 'ft',
      quantity: softEdge,
      confidence: analysis.confidence?.ratios || null,
      metadata: { derived: true, heuristic: 'sqrt(lawn+beds)*1.15' },
    },
    {
      feature_key: 'tree',
      feature_label: 'Tree',
      feature_group: 'point',
      geometry_type: 'derived',
      unit: 'count',
      quantity: treeCount,
      confidence: analysis.confidence?.ratios || null,
      metadata: { derived: true },
    },
  ].filter((feature) => feature.quantity > 0);
}

async function analyzePropertyMeasurement(property = {}, options = {}) {
  const parcelResolution = await resolvePropertyParcel(property);
  const imageryUrl = options.imageUrl || buildStaticMapUrl(parcelResolution.latitude, parcelResolution.longitude, options.zoom || 19);
  const fallbackReasons = [...(parcelResolution.diagnostics?.reasons || [])];
  const fallbackMessages = [...(parcelResolution.diagnostics?.messages || [])];

  let totalLot = options.lotSize ? parseInt(options.lotSize, 10) : null;
  let lotSizeSource = options.lotSize ? 'user' : null;

  if (!totalLot && parcelResolution.parcel?.lotSizeSqFt) {
    totalLot = parcelResolution.parcel.lotSizeSqFt;
    lotSizeSource = 'regrid';
  }

  if (!totalLot) {
    totalLot = 8500;
    lotSizeSource = 'estimate';
  }

  let ratios = { ...DEFAULT_RATIOS };
  let ratioSource = 'estimate';
  let samDebug = {};
  const samErrors = [];

  if (!process.env.FAL_API_KEY) {
    fallbackReasons.push('missing_fal_api_key');
    fallbackMessages.push('FAL_API_KEY is not configured.');
  }
  if (!imageryUrl) {
    fallbackReasons.push('missing_static_map_url');
    fallbackMessages.push('Static imagery URL could not be built.');
  }

  if (process.env.FAL_API_KEY && imageryUrl) {
    try {
      const lawnResult = await segmentWithSAM3(process.env.FAL_API_KEY, imageryUrl, 'green grass lawn yard turf').catch((error) => {
        samErrors.push(`lawn: ${error.message}`);
        return { pixelRatio: 0 };
      });
      const bedsResult = await segmentWithSAM3(process.env.FAL_API_KEY, imageryUrl, 'brown mulch garden bed landscaping bark').catch((error) => {
        samErrors.push(`beds: ${error.message}`);
        return { pixelRatio: 0 };
      });
      const hardscapeResult = await segmentWithSAM3(process.env.FAL_API_KEY, imageryUrl, 'gray concrete driveway sidewalk pavement asphalt').catch((error) => {
        samErrors.push(`hardscape: ${error.message}`);
        return { pixelRatio: 0 };
      });

      const rawLawn = lawnResult.pixelRatio || 0;
      const rawBed = bedsResult.pixelRatio || 0;
      const rawHardscape = hardscapeResult.pixelRatio || 0;
      const rawTotal = rawLawn + rawBed + rawHardscape;
      samDebug = { rawLawn, rawBed, rawHardscape, rawTotal };

      if (rawTotal > 0.1) {
        const normalizer = 0.9 / rawTotal;
        ratios = {
          lawn: rawLawn * normalizer,
          mulch_bed: rawBed * normalizer,
          hardscape: rawHardscape * normalizer,
        };
        ratioSource = 'sam3';
      } else {
        fallbackReasons.push(samErrors.length ? 'sam3_request_failed' : 'sam3_no_usable_masks');
        fallbackMessages.push(
          samErrors.length
            ? `SAM3 segmentation failed: ${samErrors.join('; ')}`
            : 'SAM3 did not return usable masks for lawn, beds, or hardscape.'
        );
      }
    } catch (error) {
      fallbackReasons.push('sam3_request_failed');
      fallbackMessages.push(`SAM3 segmentation failed: ${error.message || 'unknown error'}`);
    }
  }

  if (lotSizeSource === 'estimate' && !options.lotSize && !parcelResolution.parcel?.lotSizeSqFt) {
    fallbackReasons.push('missing_parcel_lot_size');
    fallbackMessages.push('Lot size fell back to the default estimate because no verified parcel lot size was available.');
  }

  const analysis = {
    totalLot,
    lawnArea: Math.round(totalLot * ratios.lawn),
    bedArea: Math.round(totalLot * ratios.mulch_bed),
    hardscapeArea: Math.round(totalLot * ratios.hardscape),
    shrubCount: Math.max(3, Math.round((totalLot * ratios.mulch_bed) / 55)),
    ratios,
    confidence: {
      lotSize: lotSizeSource === 'regrid' ? 0.95 : (lotSizeSource === 'user' ? 0.9 : 0.5),
      ratios: ratioSource === 'sam3' ? 0.8 : 0.5,
    },
  };

  const hasParcelBoundary = Boolean(parcelResolution.parcel?.boundaryGeojson);
  const hasParcelLotSize = lotSizeSource === 'regrid';
  const hasImagerySegmentation = ratioSource === 'sam3';

  const accuracy = {
    grade: hasParcelBoundary && hasParcelLotSize && hasImagerySegmentation
      ? 'parcel_precise'
      : hasImagerySegmentation
        ? 'approximate_imagery'
        : 'approximate_heuristic',
    isApproximate: !(hasParcelBoundary && hasParcelLotSize && hasImagerySegmentation),
    reasons: [
      hasParcelBoundary ? null : 'missing_parcel_boundary',
      hasParcelLotSize ? null : 'missing_parcel_lot_size',
      hasImagerySegmentation ? null : 'missing_imagery_segmentation',
    ].filter(Boolean),
  };

  const fallback = {
    reasons: unique([...accuracy.reasons, ...fallbackReasons]),
    messages: unique([...buildFallbackMessages([...accuracy.reasons, ...fallbackReasons]), ...fallbackMessages]),
  };

  return {
    method: `${lotSizeSource}+${ratioSource}`,
    analysis,
    accuracy,
    fallback,
    features: buildFeatureSet(analysis),
    parcel: parcelResolution.parcel,
    resolution: {
      address: parcelResolution.formattedAddress,
      latitude: parcelResolution.latitude,
      longitude: parcelResolution.longitude,
      geocodeSource: parcelResolution.geocodeSource,
      imageryUrl,
    },
    debug: {
      lotSizeSource,
      ratioSource,
      accuracy,
      fallback,
      ...samDebug,
      samErrors,
    },
  };
}

module.exports = {
  analyzePropertyMeasurement,
  buildFeatureSet,
};
