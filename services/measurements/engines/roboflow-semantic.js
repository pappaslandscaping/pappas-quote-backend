const { createCanvas, loadImage } = require('@napi-rs/canvas');

function normalizeClassName(value = '') {
  return String(value).trim().toLowerCase().replace(/[_-]+/g, ' ');
}

function classifySurface(className = '') {
  const normalized = normalizeClassName(className);
  if (/(lawn|grass|turf)/.test(normalized)) return 'lawn';
  if (/(mulch|bed|garden)/.test(normalized)) return 'mulch_bed';
  if (/(hardscape|driveway|sidewalk|walkway|concrete|asphalt|pavement|patio)/.test(normalized)) return 'hardscape';
  return null;
}

async function fetchImageAsBase64(imageUrl) {
  if (!imageUrl) {
    return { imageBase64: null, error: { reason: 'missing_static_map_url', message: 'Static imagery URL could not be built.' } };
  }

  try {
    const response = await fetch(imageUrl);
    if (!response.ok) {
      return {
        imageBase64: null,
        error: {
          reason: 'static_map_fetch_failed',
          message: `Google static map download failed with HTTP ${response.status}.`,
        },
      };
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    return { imageBase64: buffer.toString('base64'), error: null };
  } catch (error) {
    return {
      imageBase64: null,
      error: {
        reason: 'static_map_fetch_failed',
        message: error.message || 'Google static map download failed.',
      },
    };
  }
}

async function decodeClassPixelCounts(segmentationMaskBase64) {
  const normalized = String(segmentationMaskBase64 || '').replace(/^data:image\/[a-zA-Z0-9.+-]+;base64,/, '');
  const buffer = Buffer.from(normalized, 'base64');
  const image = await loadImage(buffer);
  const width = image.width || 0;
  const height = image.height || 0;
  if (!width || !height) return { totalPixels: 0, counts: {} };

  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');
  ctx.drawImage(image, 0, 0, width, height);

  const { data } = ctx.getImageData(0, 0, width, height);
  const counts = {};
  const totalPixels = width * height;

  for (let index = 0; index < data.length; index += 4) {
    const alpha = data[index + 3];
    if (alpha === 0) continue;
    const classId = data[index];
    counts[classId] = (counts[classId] || 0) + 1;
  }

  return { totalPixels, counts };
}

function toMaskDataUrl(segmentationMaskBase64) {
  const normalized = String(segmentationMaskBase64 || '').trim();
  if (!normalized) return null;
  if (normalized.startsWith('data:image/')) return normalized;
  return `data:image/png;base64,${normalized}`;
}

function firstPrediction(payload = {}) {
  if (Array.isArray(payload?.predictions)) return payload.predictions[0] || {};
  if (payload?.predictions && typeof payload.predictions === 'object') return payload.predictions;
  return {};
}

function extractSegmentationPayload(payload = {}) {
  const prediction = firstPrediction(payload);
  const maskRoot = payload.segmentation_mask || prediction.segmentation_mask || {};
  const segmentationMask =
    typeof maskRoot === 'string'
      ? maskRoot
      : maskRoot.base64 || maskRoot.value || maskRoot.mask || maskRoot.image || maskRoot.data || null;

  const classMap =
    payload.class_map ||
    prediction.class_map ||
    maskRoot.class_map ||
    {};

  const imageMeta =
    payload.image ||
    prediction.image ||
    maskRoot.image_meta ||
    {};

  return {
    prediction,
    maskRoot,
    segmentationMask,
    classMap,
    imageMeta,
    payloadShape: {
      payloadKeys: Object.keys(payload || {}),
      predictionKeys: Object.keys(prediction || {}),
      maskKeys: maskRoot && typeof maskRoot === 'object' ? Object.keys(maskRoot) : [],
    },
  };
}

async function runRoboflowSemanticEngine({ imageryUrl, fallbackReasons, visibleFallbackMessages }) {
  const apiKey = process.env.ROBOFLOW_API_KEY;
  const modelSlug = process.env.ROBOFLOW_MODEL_SLUG;
  const modelVersion = process.env.ROBOFLOW_MODEL_VERSION;

  if (!apiKey) {
    fallbackReasons.push('missing_roboflow_api_key');
    visibleFallbackMessages.push('ROBOFLOW_API_KEY is not configured.');
    return null;
  }
  if (!modelSlug || !modelVersion) {
    fallbackReasons.push('missing_roboflow_model_config');
    visibleFallbackMessages.push('ROBOFLOW_MODEL_SLUG and ROBOFLOW_MODEL_VERSION are required for Roboflow semantic segmentation.');
    return null;
  }

  const imagePayload = await fetchImageAsBase64(imageryUrl);
  if (imagePayload.error) {
    fallbackReasons.push(imagePayload.error.reason);
    visibleFallbackMessages.push(imagePayload.error.message);
    return null;
  }

  const endpoint = `https://segment.roboflow.com/${encodeURIComponent(modelSlug)}/${encodeURIComponent(modelVersion)}?api_key=${encodeURIComponent(apiKey)}&confidence=30`;

  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: imagePayload.imageBase64,
    });

    const rawText = await response.text();
    if (!response.ok) {
      fallbackReasons.push('roboflow_http_error');
      visibleFallbackMessages.push(`Roboflow semantic segmentation failed with HTTP ${response.status}.`);
      return null;
    }

    const payload = JSON.parse(rawText);
    const {
      prediction,
      maskRoot,
      segmentationMask,
      classMap,
      imageMeta,
      payloadShape,
    } = extractSegmentationPayload(payload);

    if (!segmentationMask || !Object.keys(classMap).length) {
      fallbackReasons.push('roboflow_no_segmentation_mask');
      const shapeSummary = [
        payloadShape.payloadKeys.length ? `payload keys: ${payloadShape.payloadKeys.join(', ')}` : null,
        payloadShape.predictionKeys.length ? `prediction keys: ${payloadShape.predictionKeys.join(', ')}` : null,
        payloadShape.maskKeys.length ? `mask keys: ${payloadShape.maskKeys.join(', ')}` : null,
      ].filter(Boolean).join(' | ');
      visibleFallbackMessages.push(
        shapeSummary
          ? `Roboflow did not return a usable semantic segmentation mask. ${shapeSummary}.`
          : 'Roboflow did not return a semantic segmentation mask.'
      );
      return null;
    }

    const { totalPixels, counts } = await decodeClassPixelCounts(segmentationMask);
    if (!totalPixels) {
      fallbackReasons.push('roboflow_empty_segmentation_mask');
      visibleFallbackMessages.push('Roboflow returned an empty semantic segmentation mask.');
      return null;
    }

    const bucketCounts = {
      lawn: 0,
      mulch_bed: 0,
      hardscape: 0,
    };

    Object.entries(classMap).forEach(([classId, className]) => {
      const bucket = classifySurface(className);
      if (!bucket) return;
      bucketCounts[bucket] += counts[Number(classId)] || 0;
    });

    const rawLawn = bucketCounts.lawn / totalPixels;
    const rawBed = bucketCounts.mulch_bed / totalPixels;
    const rawHardscape = bucketCounts.hardscape / totalPixels;
    const rawTotal = rawLawn + rawBed + rawHardscape;

    if (rawTotal <= 0.01) {
      fallbackReasons.push('roboflow_no_mapped_surface_classes');
      visibleFallbackMessages.push(
        `Roboflow returned a mask, but none of its classes mapped to lawn, beds, or hardscape. Classes: ${Object.values(classMap).join(', ')}.`
      );
      return null;
    }

    const normalizer = Math.min(0.95 / rawTotal, 1.2);
    return {
      engineId: 'roboflow_semantic',
      engineLabel: 'Roboflow Semantic Segmentation',
      ratioSource: 'roboflow',
      hasImagerySegmentation: true,
      confidence: 0.85,
      ratios: {
        lawn: Math.min(rawLawn * normalizer, 0.9),
        mulch_bed: Math.min(rawBed * normalizer, 0.6),
        hardscape: Math.min(rawHardscape * normalizer, 0.8),
      },
      debug: {
        imageMeta,
        prediction,
        maskRoot,
        segmentationMaskDataUrl: toMaskDataUrl(segmentationMask),
        classMap,
        bucketCounts,
        rawLawn,
        rawBed,
        rawHardscape,
        rawTotal,
        payloadShape,
      },
    };
  } catch (error) {
    fallbackReasons.push('roboflow_exception');
    visibleFallbackMessages.push(`Roboflow semantic segmentation failed: ${error.message || 'unknown error'}`);
    return null;
  }
}

module.exports = {
  runRoboflowSemanticEngine,
};
