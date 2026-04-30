const { createCanvas, loadImage } = require('@napi-rs/canvas');
const { resolvePropertyParcel } = require('./parcel');

const DEFAULT_RATIOS = {
  lawn: 0.6,
  mulch_bed: 0.1,
  hardscape: 0.2,
};

function unique(list = []) {
  return [...new Set(list.filter(Boolean))];
}

function shouldShowFallbackReason(reason) {
  return ![
    'missing_parcel_boundary',
    'missing_parcel_lot_size',
    'missing_regrid_api_token',
    'regrid_http_error',
    'regrid_no_parcel_found',
    'regrid_exception',
    'missing_coordinates_for_parcel_lookup',
  ].includes(reason);
}

function buildFallbackMessages(reasons = []) {
  const messageByReason = {
    missing_google_maps_api_key: 'GOOGLE_MAPS_API_KEY is not configured.',
    missing_property_address: 'Property address is incomplete.',
    google_geocode_http_error: 'Google geocode request failed.',
    google_geocode_failed: 'Google could not geocode this address.',
    google_geocode_exception: 'Google geocode threw an exception.',
    missing_fal_api_key: 'FAL_API_KEY is not configured.',
    missing_static_map_url: 'Static imagery URL could not be built.',
    static_map_fetch_failed: 'Google static map download failed.',
    sam3_request_failed: 'SAM3 segmentation request failed.',
    sam3_no_usable_masks: 'SAM3 did not return usable masks for lawn, beds, or hardscape.',
  };
  return unique(reasons).filter(shouldShowFallbackReason).map((reason) => messageByReason[reason] || reason);
}

async function buildFalImageInput(imageUrl) {
  if (!imageUrl) return { imageInput: null, error: { reason: 'missing_static_map_url', message: 'Static imagery URL could not be built.' } };

  try {
    const response = await fetch(imageUrl);
    if (!response.ok) {
      return {
        imageInput: null,
        error: {
          reason: 'static_map_fetch_failed',
          message: `Google static map download failed with HTTP ${response.status}.`,
        },
      };
    }

    const contentType = response.headers.get('content-type') || 'image/png';
    const buffer = Buffer.from(await response.arrayBuffer());
    return {
      imageInput: `data:${contentType};base64,${buffer.toString('base64')}`,
      error: null,
    };
  } catch (error) {
    return {
      imageInput: null,
      error: {
        reason: 'static_map_fetch_failed',
        message: error.message || 'Google static map download failed.',
      },
    };
  }
}

async function estimatePixelRatio(maskUrl) {
  try {
    const response = await fetch(maskUrl);
    if (!response.ok) return 0;

    const buffer = Buffer.from(await response.arrayBuffer());
    const image = await loadImage(buffer);
    const width = image.width || 0;
    const height = image.height || 0;
    if (!width || !height) return 0;

    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext('2d');
    ctx.drawImage(image, 0, 0, width, height);

    const { data } = ctx.getImageData(0, 0, width, height);
    let filledPixels = 0;
    const totalPixels = width * height;

    for (let index = 0; index < data.length; index += 4) {
      const alpha = data[index + 3];
      const red = data[index];
      const green = data[index + 1];
      const blue = data[index + 2];
      const luminance = red + green + blue;

      if (alpha > 20 && luminance > 30) {
        filledPixels += 1;
      }
    }

    if (!totalPixels) return 0;
    return filledPixels / totalPixels;
  } catch (_error) {
    return 0;
  }
}

async function segmentWithSAM3(apiKey, imageInput, prompt) {
  const response = await fetch('https://fal.run/fal-ai/sam-3/image', {
    method: 'POST',
    headers: {
      Authorization: `Key ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      image_url: imageInput,
      prompt,
      point_prompts: [],
      box_prompts: [],
      apply_mask: true,
      output_format: 'png',
      max_masks: 3,
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
  const falImage = await buildFalImageInput(imageryUrl);
  const fallbackReasons = [...(parcelResolution.diagnostics?.reasons || [])];
  const fallbackMessages = [...(parcelResolution.diagnostics?.messages || [])];
  const visibleFallbackMessages = [];

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
    visibleFallbackMessages.push('FAL_API_KEY is not configured.');
  }
  if (falImage.error) {
    fallbackReasons.push(falImage.error.reason);
    visibleFallbackMessages.push(falImage.error.message);
  }

  if (process.env.FAL_API_KEY && falImage.imageInput) {
    try {
      const lawnResult = await segmentWithSAM3(process.env.FAL_API_KEY, falImage.imageInput, 'green grass lawn yard turf').catch((error) => {
        samErrors.push(`lawn: ${error.message}`);
        return { pixelRatio: 0 };
      });
      const bedsResult = await segmentWithSAM3(process.env.FAL_API_KEY, falImage.imageInput, 'brown mulch garden bed landscaping bark').catch((error) => {
        samErrors.push(`beds: ${error.message}`);
        return { pixelRatio: 0 };
      });
      const hardscapeResult = await segmentWithSAM3(process.env.FAL_API_KEY, falImage.imageInput, 'gray concrete driveway sidewalk pavement asphalt').catch((error) => {
        samErrors.push(`hardscape: ${error.message}`);
        return { pixelRatio: 0 };
      });

      const rawLawn = lawnResult.pixelRatio || 0;
      const rawBed = bedsResult.pixelRatio || 0;
      const rawHardscape = hardscapeResult.pixelRatio || 0;
      const rawTotal = rawLawn + rawBed + rawHardscape;
      samDebug = { rawLawn, rawBed, rawHardscape, rawTotal };

      if (rawTotal > 0.02 && rawLawn > 0.005) {
        const normalizer = 0.9 / rawTotal;
        ratios = {
          lawn: rawLawn * normalizer,
          mulch_bed: rawBed * normalizer,
          hardscape: rawHardscape * normalizer,
        };
        ratioSource = 'sam3';
      } else {
        fallbackReasons.push(samErrors.length ? 'sam3_request_failed' : 'sam3_no_usable_masks');
        visibleFallbackMessages.push(
          samErrors.length
            ? `SAM3 segmentation failed: ${samErrors.join('; ')}`
            : 'SAM3 did not return usable masks for lawn, beds, or hardscape.'
        );
      }
    } catch (error) {
      fallbackReasons.push('sam3_request_failed');
      visibleFallbackMessages.push(`SAM3 segmentation failed: ${error.message || 'unknown error'}`);
    }
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
    reasons: unique([...accuracy.reasons, ...fallbackReasons]).filter(shouldShowFallbackReason),
    messages: unique([
      ...buildFallbackMessages([...accuracy.reasons, ...fallbackReasons]),
      ...visibleFallbackMessages,
    ]),
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
      diagnostics: fallbackMessages,
    },
  };
}

module.exports = {
  analyzePropertyMeasurement,
  buildFeatureSet,
};
