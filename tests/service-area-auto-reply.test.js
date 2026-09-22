const { extractExplicitZip, getOutOfAreaZip } = require('../lib/service-area-auto-reply');

describe('service-area ZIP detection', () => {
  test('does not treat house numbers in the reported Clifton conversation as ZIP codes', () => {
    const message = 'And when do you expect to give me estimates on cutting both lawns 9718 Clifton and 16713?';
    expect(extractExplicitZip(message)).toBeNull();
    expect(getOutOfAreaZip(message)).toBeNull();
    expect(getOutOfAreaZip('Please quote 16713 Clifton Road and 9718 Clifton.')).toBeNull();
  });

  test('requires an explicit ZIP label in free-form customer messages', () => {
    expect(getOutOfAreaZip('My ZIP code is 44070.')).toBe('44070');
    expect(getOutOfAreaZip('The property zip: 44070')).toBe('44070');
    expect(getOutOfAreaZip('The property is at 16713 Clifton, 44070')).toBeNull();
    expect(getOutOfAreaZip('My ZIP is 44107')).toBeNull();
  });

  test('accepts bare ZIPs only from structured ZIP fields', () => {
    expect(extractExplicitZip('44070')).toBeNull();
    expect(extractExplicitZip('44070', { allowBare: true })).toBe('44070');
    expect(extractExplicitZip('44070-1234', { allowBare: true })).toBe('44070');
  });
});
