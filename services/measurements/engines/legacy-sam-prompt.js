const { createCanvas, loadImage } = require('@napi-rs/canvas');

const DEFAULT_RATIOS = {
  lawn: 0.6,
  mulch_bed: 0.1,
  hardscape: 0.2,
};

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

async function runLegacySamPromptEngine({ imageryUrl, fallbackReasons, visibleFallbackMessages }) {
  const falImage = await buildFalImageInput(imageryUrl);
  const samErrors = [];
  let ratios = { ...DEFAULT_RATIOS };
  let ratioSource = 'estimate';
  let samDebug = {};

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

  return {
    engineId: 'legacy_sam_prompt',
    engineLabel: 'Legacy SAM Prompt',
    ratioSource,
    ratios,
    debug: {
      ...samDebug,
      samErrors,
    },
  };
}

module.exports = {
  runLegacySamPromptEngine,
};
