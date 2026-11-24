function normalizeCityStateZip(line) {
  const raw = String(line || '').trim();
  if (!raw) return '';
  const compact = raw.replace(/\s+/g, ' ');
  const match = compact.match(/^(.*?)(?:,\s*|\s+)([A-Z]{2})(?:,\s*|\s+)(\d{5}(?:-\d{4})?)$/i);
  if (!match) return compact;
  const city = match[1].replace(/,\s*$/, '').trim();
  const state = match[2].toUpperCase();
  const zip = match[3];
  return `${city}, ${state} ${zip}`;
}

function splitSingleLineAddress(line) {
  const compact = String(line || '').trim().replace(/\s+/g, ' ');
  if (!compact) return [];

  // HomeWorks may flatten structured addresses with commas between every field.
  // Split the postal tail first so multi-word cities and PO boxes stay intact.
  const delimited = compact.match(/^(.+),\s*([^,]+),\s*([A-Z]{2})(?:,\s*|\s+)(\d{5}(?:-\d{4})?)$/i);
  if (delimited) {
    return [delimited[1].trim(), `${delimited[2].trim()}, ${delimited[3].toUpperCase()} ${delimited[4]}`];
  }

  const streetSuffixes = [
    'alley', 'aly', 'ave', 'avenue', 'blvd', 'boulevard', 'cir', 'circle',
    'court', 'ct', 'drive', 'dr', 'highway', 'hwy', 'lane', 'ln', 'parkway',
    'pkwy', 'place', 'pl', 'plaza', 'plz', 'road', 'rd', 'square', 'sq',
    'street', 'st', 'terrace', 'ter', 'trail', 'trl', 'way',
  ].join('|');

  const pattern = new RegExp(
    `^(.+?\\b(?:${streetSuffixes})\\.?` +
      `(?:\\s+(?:apt|apartment|unit|suite|ste|#)\\s*[^,]+)?)` +
      `\\s+(.+?)(?:,\\s*|\\s+)([A-Z]{2})(?:,\\s*|\\s+)(\\d{5}(?:-\\d{4})?)$`,
    'i'
  );
  const match = compact.match(pattern);
  if (!match) return [normalizeCityStateZip(compact)];

  const street = match[1].trim();
  const city = match[2].replace(/,\s*$/, '').trim();
  const state = match[3].toUpperCase();
  const zip = match[4];
  return [street, `${city}, ${state} ${zip}`];
}

function splitAddressLines(value) {
  const lines = String(value || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(0, 4);

  if (!lines.length) return [];
  if (lines.length === 1) return splitSingleLineAddress(lines[0]);
  if (lines.length === 2) return [lines[0], normalizeCityStateZip(lines[1])];

  const first = lines[0];
  const rest = normalizeCityStateZip(lines.slice(1).join(' '));
  return rest ? [first, rest] : [first];
}

module.exports = { splitAddressLines };
