const fs = require('fs');
const path = require('path');

const page = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'sign-contract.html'),
  'utf8'
);

describe('customer service agreement page', () => {
  test('uses the Estimate v4 brand system and email logo', () => {
    expect(page).toContain('--primary-dark: #1f2933');
    expect(page).toContain('--accent: #c9dd80');
    expect(page).toContain('/images/email-logo.png');
    expect(page).toContain('Your service agreement is ready');
    expect(page).toContain("font-family: 'Allura', 'Snell Roundhand'");
  });

  test('uses consistent estimate wording throughout the customer flow', () => {
    expect(page).toContain('Review Estimate');
    expect(page).toContain('Accept Estimate');
    expect(page).toContain('Estimate Reference');
    expect(page).toContain('Estimate Total');
    expect(page).toContain('Associated Estimate');
    expect(page).not.toContain('Review Quote');
    expect(page).not.toContain('Accept Quote');
    expect(page).not.toContain('Quote Reference');
    expect(page).not.toContain('Quote Total');
  });

  test('preserves the signing controls and API flow', () => {
    expect(page).toContain('id="signatureCanvas"');
    expect(page).toContain('id="typedSignature"');
    expect(page).toContain('id="consentCheckbox"');
    expect(page).toContain("fetch(`${API_BASE}/api/sign/${token}`)");
    expect(page).toContain("fetch(`${API_BASE}/api/sent-quotes/${quoteData.id}/sign-contract`,");
  });
});
