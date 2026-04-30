const { resolvePropertyParcel } = require('./parcel');
const { runLegacySamPromptEngine } = require('./engines/legacy-sam-prompt');
const { runRoboflowSemanticEngine } = require('./engines/roboflow-semantic');

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
    missing_roboflow_api_key: 'ROBOFLOW_API_KEY is not configured.',
    missing_roboflow_model_config: 'ROBOFLOW_MODEL_SLUG and ROBOFLOW_MODEL_VERSION are not configured.',
    missing_static_map_url: 'Static imagery URL could not be built.',
    static_map_fetch_failed: 'Google static map download failed.',
    sam3_request_failed: 'SAM3 segmentation request failed.',
    sam3_no_usable_masks: 'SAM3 did not return usable masks for lawn, beds, or hardscape.',
    roboflow_http_error: 'Roboflow semantic segmentation request failed.',
    roboflow_no_segmentation_mask: 'Roboflow did not return a semantic segmentation mask.',
    roboflow_empty_segmentation_mask: 'Roboflow returned an empty semantic segmentation mask.',
    roboflow_no_mapped_surface_classes: 'Roboflow returned classes that did not map to lawn, beds, or hardscape.',
    roboflow_exception: 'Roboflow semantic segmentation threw an exception.',
  };
  return unique(reasons).filter(shouldShowFallbackReason).map((reason) => messageByReason[reason] || reason);
}

async function selectMeasurementEngine({ imageryUrl, fallbackReasons, visibleFallbackMessages }) {
  const preferred = process.env.MEASUREMENT_ENGINE || 'legacy_sam_prompt';

  if (preferred === 'roboflow_semantic') {
    const roboflowResult = await runRoboflowSemanticEngine({
      imageryUrl,
      fallbackReasons,
      visibleFallbackMessages,
    });
    if (roboflowResult) return roboflowResult;
  }

  return runLegacySamPromptEngine({
    imageryUrl,
    fallbackReasons,
    visibleFallbackMessages,
  });
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

  const engineResult = await selectMeasurementEngine({
    imageryUrl,
    fallbackReasons,
    visibleFallbackMessages,
  });

  const analysis = {
    totalLot,
    lawnArea: Math.round(totalLot * engineResult.ratios.lawn),
    bedArea: Math.round(totalLot * engineResult.ratios.mulch_bed),
    hardscapeArea: Math.round(totalLot * engineResult.ratios.hardscape),
    shrubCount: Math.max(3, Math.round((totalLot * engineResult.ratios.mulch_bed) / 55)),
    ratios: engineResult.ratios,
    confidence: {
      lotSize: lotSizeSource === 'regrid' ? 0.95 : (lotSizeSource === 'user' ? 0.9 : 0.5),
      ratios: engineResult.ratioSource === 'sam3' ? 0.8 : 0.5,
    },
  };

  const hasParcelBoundary = Boolean(parcelResolution.parcel?.boundaryGeojson);
  const hasParcelLotSize = lotSizeSource === 'regrid';
  const hasImagerySegmentation = engineResult.ratioSource === 'sam3';

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
    method: `${lotSizeSource}+${engineResult.ratioSource}`,
    engine: {
      id: engineResult.engineId,
      label: engineResult.engineLabel,
    },
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
      ratioSource: engineResult.ratioSource,
      engine: {
        id: engineResult.engineId,
        label: engineResult.engineLabel,
      },
      accuracy,
      fallback,
      ...engineResult.debug,
      diagnostics: fallbackMessages,
    },
  };
}

module.exports = {
  analyzePropertyMeasurement,
  buildFeatureSet,
};
