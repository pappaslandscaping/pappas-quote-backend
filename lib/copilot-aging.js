const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
} = require('./copilot-metric-sources');

const AGING_BUCKET_KEYS = ['within_30', '31_60', '61_90', '90_plus'];

function hasValidAgingBuckets(buckets) {
  if (!buckets || typeof buckets !== 'object') return false;
  return AGING_BUCKET_KEYS.every((key) => {
    const bucket = buckets[key];
    return bucket
      && Number.isFinite(Number(bucket.count))
      && Number.isFinite(Number(bucket.total))
      && Array.isArray(bucket.invoices);
  });
}

function normalizeAgingSnapshot(snapshot, sourceOverride = LIVE_COPILOT_SOURCE) {
  if (!snapshot || typeof snapshot !== 'object' || !hasValidAgingBuckets(snapshot.buckets)) return null;
  const buckets = {};
  for (const key of AGING_BUCKET_KEYS) {
    const bucket = snapshot.buckets[key];
    buckets[key] = {
      count: Number(bucket.count) || 0,
      total: Number(bucket.total) || 0,
      invoices: Array.isArray(bucket.invoices) ? bucket.invoices : [],
    };
  }
  return {
    success: true,
    source: sourceOverride || snapshot.source || LIVE_COPILOT_SOURCE,
    as_of: snapshot.as_of || new Date().toISOString(),
    buckets,
  };
}

function getAgingSnapshotExpiry(snapshot, ttlMs) {
  const asOfMs = snapshot?.as_of ? new Date(snapshot.as_of).getTime() : NaN;
  if (!Number.isFinite(asOfMs)) return 0;
  return asOfMs + ttlMs;
}

module.exports = {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  AGING_BUCKET_KEYS,
  hasValidAgingBuckets,
  normalizeAgingSnapshot,
  getAgingSnapshotExpiry,
};
