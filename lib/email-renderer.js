let mjml2html;

const LOGO_URL = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/Your%20paragraph%20text%20%284.75%20x%202%20in%29%20%28800%20x%20400%20px%29%20%282%29.png';
const SIGNATURE_IMAGE = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/White%20Modern%20Minimalist%20Signature%20Brand%20Logo%20%281200%20x%20300%20px%29%20%281%29.png';
const SOCIAL_FACEBOOK = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/logo_facebook_chatting_brand_social_media_application_icon_210431.png';
const SOCIAL_INSTAGRAM = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/instagram_social_media_brand_logo_application_icon_210428.png';
const SOCIAL_NEXTDOOR = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/social_media_brand_logo_application_nextdoor_icon_210365.png';

/**
 * Renders an MJML string to HTML.
 * @param {string} mjmlContent The MJML content.
 * @returns {Promise<string>} The rendered HTML.
 */
async function renderMJML(mjmlContent) {
  if (!mjml2html) {
    try {
      // Lazy load to prevent startup crashes if mjml is heavy or missing
      mjml2html = require('mjml');
    } catch (err) {
      console.error('❌ Failed to load MJML module:', err);
      // Fallback: return the content stripped of mjml tags
      return mjmlContent.replace(/<mj-[^>]+>/g, '').replace(/<\/mj-[^>]+>/g, '');
    }
  }
  
  try {
    const result = await mjml2html(mjmlContent, {
      validationLevel: 'soft'
    });
    if (result.errors && result.errors.length > 0) {
      console.warn('MJML rendering errors:', result.errors);
    }
    return result.html;
  } catch (err) {
    console.error('❌ MJML rendering exception:', err);
    return mjmlContent.replace(/<mj-[^>]+>/g, '').replace(/<\/mj-[^>]+>/g, '');
  }
}

/**
 * Wraps content in a branded MJML layout.
 * @param {string} content HTML or MJML content for the body.
 * @param {Object} options Rendering options.
 * @returns {Promise<string>} The final HTML.
 */
async function renderWithBaseLayout(content, options = {}) {
  const {
    wrapper = 'full',
    showFeatures = false,
    showSignature = false,
    baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com',
    unsubscribeEmail = '{unsubscribe_email}'
  } = options;

  const assetsUrl = process.env.EMAIL_ASSETS_URL || baseUrl;
  const SOCIAL_FB_WHITE = `${assetsUrl}/email-assets/fb-white.png`;
  const SOCIAL_IG_WHITE = `${assetsUrl}/email-assets/ig-white.png`;
  const SOCIAL_ND_WHITE = `${assetsUrl}/email-assets/nd-white.png`;

  // Define MJML components for the layout
  const header = `
    <mj-section padding="20px 20px 0">
      <mj-group background-color="#ffffff">
        <mj-column vertical-align="middle" padding="18px 0 14px 28px" border="1px solid #dfe5de" border-bottom="none" border-right="none" border-radius="22px 0 0 0">
          <mj-image src="${LOGO_URL}" alt="Pappas & Co. Landscaping" width="168px" align="left" padding="0" />
        </mj-column>
        <mj-column vertical-align="middle" padding="18px 28px 14px 0" border="1px solid #dfe5de" border-bottom="none" border-left="none" border-radius="0 22px 0 0">
          <mj-text align="right" font-size="11px" letter-spacing="0.14em" color="#7b877f" font-weight="700" text-transform="uppercase" padding="0">
            Pappas &amp; Co. Landscaping
          </mj-text>
        </mj-column>
      </mj-group>
    </mj-section>
  `;

  const footer = `
    <mj-section padding="0 20px 24px">
      <mj-column width="100%" background-color="#ffffff" border-radius="0 0 22px 22px" border="1px solid #dfe5de" border-top="none" padding="0 30px 24px">
        <mj-divider border-width="1px" border-color="#e4e9e4" padding="0" />
        <mj-text padding="18px 0 0" font-size="12px" line-height="1.65" color="#69756f">
          Questions? Reply to this email or call <a href="tel:4408867318" style="color:#223330;font-weight:700;text-decoration:none;">(440) 886-7318</a>.
        </mj-text>
        <mj-social font-size="12px" icon-size="30px" mode="horizontal" align="left" padding="10px 0">
          <mj-social-element name="facebook" href="https://www.facebook.com/pappaslandscaping" src="${SOCIAL_FB_WHITE}" background-color="#2d3934" border-radius="999px" padding="0 8px 0 0" />
          <mj-social-element name="instagram" href="https://www.instagram.com/pappaslandscaping" src="${SOCIAL_IG_WHITE}" background-color="#2d3934" border-radius="999px" padding="0 8px 0 0" />
          <mj-social-element name="nextdoor" href="https://nextdoor.com/profile/01ZjZkwxhPWdnML2k" src="${SOCIAL_ND_WHITE}" background-color="#2d3934" border-radius="999px" padding="0" />
        </mj-social>
        <mj-text padding="0" font-size="11px" color="#77827c">
          <a href="https://pappaslandscaping.com" style="color:#556760;text-decoration:none;">pappaslandscaping.com</a>
        </mj-text>
        <mj-text padding="16px 0 0" font-size="10px" color="#94a3b8" align="center">
          <a href="${baseUrl}/unsubscribe.html?email=${unsubscribeEmail}" style="color:#94a3b8;text-decoration:underline;">Unsubscribe</a> from marketing emails
        </mj-text>
      </mj-column>
    </mj-section>
  `;

  const features = showFeatures ? `
    <mj-section padding="4px 30px 0" background-color="#ffffff">
      <mj-column background-color="#f7f8f4" border="1px solid #e3e8e2" border-radius="16px" padding="20px 22px">
        <mj-text font-size="10px" letter-spacing="0.18em" text-transform="uppercase" color="#728076" font-weight="700" padding="0 0 8px">Inside your account</mj-text>
        <mj-text font-size="20px" line-height="1.25" color="#223330" font-weight="700" padding="0 0 18px">Everything stays organized in one place</mj-text>
        <mj-table padding="0">
          <tr>
            <td style="padding:12px 0;border-bottom:1px solid #e3e8e2;">
              <span style="font-size:18px;color:#223330;margin-right:10px;">•</span>
              <strong style="color:#223330;">Service Schedule</strong><br/><span style="color:#68796f;font-size:13px;">View upcoming visits and service history</span>
            </td>
          </tr>
          <tr>
            <td style="padding:12px 0;border-bottom:1px solid #e3e8e2;">
              <span style="font-size:18px;color:#223330;margin-right:10px;">•</span>
              <strong style="color:#223330;">Easy Payments</strong><br/><span style="color:#68796f;font-size:13px;">Pay invoices securely online anytime</span>
            </td>
          </tr>
          <tr>
            <td style="padding:12px 0;">
              <span style="font-size:18px;color:#223330;margin-right:10px;">•</span>
              <strong style="color:#223330;">Quotes & Invoices</strong><br/><span style="color:#68796f;font-size:13px;">Access all your documents in one place</span>
            </td>
          </tr>
        </mj-table>
      </mj-column>
    </mj-section>
  ` : '';

  const signature = showSignature ? `
    <mj-divider border-width="1px" border-color="#e4e9e4" padding="28px 0 18px" />
    <mj-image src="${SIGNATURE_IMAGE}" alt="Timothy Pappas" width="220px" align="left" padding="0" />
    <mj-text padding="8px 0 0" font-size="12px" line-height="1.6" color="#6d7a72">Timothy Pappas &bull; Pappas &amp; Co. Landscaping</mj-text>
  ` : '';

  const mjml = `
    <mjml>
      <mj-head>
        <mj-font name="DM Sans" href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap" />
        <mj-attributes>
          <mj-all font-family="DM Sans, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Arial, sans-serif" />
          <mj-text font-size="15px" color="#4a5751" line-height="1.78" />
        </mj-attributes>
        <mj-style inline="inline">
          .body-text a { color: #2e403d; text-decoration: underline; }
          .btn-primary a { color: #c9dd80 !important; }
        </mj-style>
      </mj-head>
      <mj-body background-color="#f3f4ef">
        ${wrapper === 'full' ? header : ''}
        <mj-section padding="0 20px" background-color="#ffffff">
          <mj-column border-left="1px solid #dfe5de" border-right="1px solid #dfe5de" padding="26px 30px 24px">
            ${content.includes('<mj-') ? content : `<mj-text padding="0">\n              ${content}\n            </mj-text>`}
            ${signature}
          </mj-column>
        </mj-section>
        ${features}
        ${wrapper === 'full' ? footer : ''}
      </mj-body>
    </mjml>
  `;

  return renderMJML(mjml);
}

function emailTemplate(content, options = {}) {
  const wrapperMode = ['full', 'minimal', 'none'].includes(options.wrapper) ? options.wrapper : 'full';
  const showFooterFeatures = options.showFeatures || false;
  const showSignature = options.showSignature === true;

  const signatureHtml = showSignature ? `
    <div style="margin-top:28px;padding-top:18px;border-top:1px solid #e4e9e4;">
      <img src="${SIGNATURE_IMAGE}" alt="Timothy Pappas" style="display:block;max-width:220px;width:100%;height:auto;">
      <p style="margin:8px 0 0;font-size:12px;line-height:1.6;color:#6d7a72;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Timothy Pappas &bull; Pappas &amp; Co. Landscaping</p>
    </div>
  ` : '';

  const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
  const assetsUrl = process.env.EMAIL_ASSETS_URL || baseUrl;
  const SOCIAL_FB_WHITE = `${assetsUrl}/email-assets/fb-white.png`;
  const SOCIAL_IG_WHITE = `${assetsUrl}/email-assets/ig-white.png`;
  const SOCIAL_ND_WHITE = `${assetsUrl}/email-assets/nd-white.png`;
  const contentPadding = wrapperMode === 'minimal' ? '28px 30px 24px' : '26px 30px 24px';
  const contentBlock = `
  <tr><td style="padding:${contentPadding};font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#4a5751;font-size:15px;line-height:1.78;">
    ${content}
    ${showSignature && wrapperMode !== 'none' ? signatureHtml : ''}
  </td></tr>
  `;

  if (wrapperMode === 'none') {
    return `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:24px;background:#ffffff;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#4a5751;font-size:15px;line-height:1.78;">
${content}
</body>
</html>`;
  }

  const featuresSection = showFooterFeatures ? `
      <tr><td style="padding:4px 30px 0;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f8f4;border:1px solid #e3e8e2;border-radius:16px;">
          <tr><td style="padding:20px 22px;">
            <p style="text-align:left;font-size:10px;letter-spacing:0.18em;text-transform:uppercase;color:#728076;font-weight:700;margin:0 0 8px;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Inside your account</p>
            <p style="text-align:left;font-family:'DM Sans',-apple-system,Arial,sans-serif;font-size:20px;line-height:1.25;color:#223330;font-weight:700;margin:0 0 18px;">Everything stays organized in one place</p>
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td style="padding:12px 0;border-bottom:1px solid #e3e8e2;">
                  <table cellpadding="0" cellspacing="0"><tr>
                    <td style="width:32px;vertical-align:top;"><span style="font-size:18px;color:#223330;">•</span></td>
                    <td><strong style="color:#223330;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Service Schedule</strong><br><span style="color:#68796f;font-size:13px;font-family:'DM Sans',-apple-system,Arial,sans-serif;">View upcoming visits and service history</span></td>
                  </tr></table>
                </td>
              </tr>
              <tr>
                <td style="padding:12px 0;border-bottom:1px solid #e3e8e2;">
                  <table cellpadding="0" cellspacing="0"><tr>
                    <td style="width:32px;vertical-align:top;"><span style="font-size:18px;color:#223330;">•</span></td>
                    <td><strong style="color:#223330;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Easy Payments</strong><br><span style="color:#68796f;font-size:13px;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Pay invoices securely online anytime</span></td>
                  </tr></table>
                </td>
              </tr>
              <tr>
                <td style="padding:12px 0;border-bottom:1px solid #e3e8e2;">
                  <table cellpadding="0" cellspacing="0"><tr>
                    <td style="width:32px;vertical-align:top;"><span style="font-size:18px;color:#223330;">•</span></td>
                    <td><strong style="color:#223330;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Direct Messaging</strong><br><span style="color:#68796f;font-size:13px;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Send questions or requests to our team</span></td>
                  </tr></table>
                </td>
              </tr>
              <tr>
                <td style="padding:12px 0;">
                  <table cellpadding="0" cellspacing="0"><tr>
                    <td style="width:32px;vertical-align:top;"><span style="font-size:18px;color:#223330;">•</span></td>
                    <td><strong style="color:#223330;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Quotes & Invoices</strong><br><span style="color:#68796f;font-size:13px;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Access all your documents in one place</span></td>
                  </tr></table>
                </td>
              </tr>
            </table>
          </td></tr>
        </table>
      </td></tr>
  ` : '';

  const headerHtml = wrapperMode === 'minimal' ? `
  <tr><td style="padding:18px 28px 0;">
    <table width="100%" cellpadding="0" cellspacing="0" style="border-bottom:1px solid #e4e9e4;">
      <tr><td style="padding:0 0 14px;">
        <img src="${LOGO_URL}" alt="Pappas & Co. Landscaping" style="display:block;max-height:34px;max-width:156px;width:auto;">
      </td></tr>
    </table>
  </td></tr>
  ` : `
  <tr><td style="padding:0 18px;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;border:1px solid #dfe5de;border-bottom:none;border-radius:22px 22px 0 0;">
      <tr><td style="padding:18px 28px 14px;">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td style="vertical-align:middle;"><img src="${LOGO_URL}" alt="Pappas & Co. Landscaping" style="display:block;max-height:38px;max-width:168px;width:auto;"></td>
            <td align="right" style="vertical-align:middle;">
              <p style="margin:0;font-size:11px;letter-spacing:0.14em;text-transform:uppercase;color:#7b877f;font-weight:700;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Pappas &amp; Co. Landscaping</p>
            </td>
          </tr>
        </table>
      </td></tr>
    </table>
  </td></tr>
  `;

  const minimalShell = `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');</style>
</head>
<body style="margin:0;padding:0;background:#f4f5f1;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f5f1;padding:22px 12px;">
<tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0" style="width:620px;max-width:620px;background:#ffffff;border:1px solid #dfe5de;border-radius:20px;overflow:hidden;">
  ${headerHtml}
  ${contentBlock}
  <tr><td style="padding:0 30px 24px;">
    <table width="100%" cellpadding="0" cellspacing="0" style="border-top:1px solid #e4e9e4;">
      <tr><td style="padding:18px 0 0;text-align:left;">
        <p style="margin:0 0 8px;font-size:12px;line-height:1.65;color:#69756f;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Questions? Reply to this email or call <a href="tel:4408867318" style="color:#223330;font-weight:700;text-decoration:none;">(440) 886-7318</a>.</p>
        <table cellpadding="0" cellspacing="0" style="margin:10px 0 10px;">
          <tr>
            <td style="padding:0 8px 0 0;"><a href="https://www.facebook.com/pappaslandscaping" style="display:block;width:30px;height:30px;background:#2d3934;border-radius:999px;text-decoration:none;"><img src="${SOCIAL_FB_WHITE}" alt="Facebook" style="display:block;width:14px;height:14px;margin:8px auto;"></a></td>
            <td style="padding:0 8px 0 0;"><a href="https://www.instagram.com/pappaslandscaping" style="display:block;width:30px;height:30px;background:#2d3934;border-radius:999px;text-decoration:none;"><img src="${SOCIAL_IG_WHITE}" alt="Instagram" style="display:block;width:14px;height:14px;margin:8px auto;"></a></td>
            <td style="padding:0;"><a href="https://nextdoor.com/profile/01ZjZkwxhPWdnML2k" style="display:block;width:30px;height:30px;background:#2d3934;border-radius:999px;text-decoration:none;"><img src="${SOCIAL_ND_WHITE}" alt="Nextdoor" style="display:block;width:14px;height:14px;margin:8px auto;"></a></td>
          </tr>
        </table>
        <p style="margin:0;font-size:11px;color:#77827c;font-family:'DM Sans',-apple-system,Arial,sans-serif;"><a href="https://pappaslandscaping.com" style="color:#556760;text-decoration:none;">pappaslandscaping.com</a></p>
      </td></tr>
    </table>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>`;

  const fullShell = `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');</style>
</head>
<body style="margin:0;padding:0;background:#f3f4ef;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4ef;padding:20px 0 24px;">
<tr><td align="center">
<table width="640" cellpadding="0" cellspacing="0" style="width:640px;max-width:640px;">
  ${headerHtml}
  <tr><td style="padding:0 18px 18px;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#ffffff;border:1px solid #dfe5de;border-radius:0 0 22px 22px;overflow:hidden;">
      <tr><td style="padding:0 30px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border-bottom:1px solid #e4e9e4;">
          <tr><td style="padding:14px 0 13px;text-align:left;">
            <p style="margin:0;font-size:12px;line-height:1.6;color:#69756f;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Reply directly to this email or call <a href="tel:4408867318" style="color:#223330;font-weight:700;text-decoration:none;">(440) 886-7318</a> if you need anything from our team.</p>
          </td></tr>
        </table>
      </td></tr>
      ${contentBlock}
      ${featuresSection}
      <tr><td style="padding:20px 30px 24px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="border-top:1px solid #e4e9e4;">
          <tr>
            <td style="padding:16px 0 0;text-align:left;">
              <p style="margin:0 0 6px;font-size:12px;color:#223330;font-weight:700;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Pappas &amp; Co. Landscaping</p>
              <p style="margin:0 0 6px;font-size:11px;color:#77827c;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Reply here or call <a href="tel:4408867318" style="color:#556760;text-decoration:none;">(440) 886-7318</a> &bull; <a href="https://pappaslandscaping.com" style="color:#556760;text-decoration:none;">pappaslandscaping.com</a></p>
              <p style="margin:0;font-size:10px;color:#8a948e;font-family:'DM Sans',-apple-system,Arial,sans-serif;"><a href="${baseUrl}/unsubscribe.html?email={unsubscribe_email}" style="color:#67756d;text-decoration:underline;">Unsubscribe</a> from marketing emails</p>
            </td>
            <td align="right" style="padding:16px 0 0;vertical-align:top;">
              <table cellpadding="0" cellspacing="0" style="margin-left:auto;">
                <tr>
                  <td style="padding:0 0 0 8px;"><a href="https://www.facebook.com/pappaslandscaping" style="display:block;width:32px;height:32px;background:#2d3934;border-radius:999px;text-decoration:none;"><img src="${SOCIAL_FB_WHITE}" alt="Facebook" style="display:block;width:14px;height:14px;margin:9px auto;"></a></td>
                  <td style="padding:0 0 0 8px;"><a href="https://www.instagram.com/pappaslandscaping" style="display:block;width:32px;height:32px;background:#2d3934;border-radius:999px;text-decoration:none;"><img src="${SOCIAL_IG_WHITE}" alt="Instagram" style="display:block;width:14px;height:14px;margin:9px auto;"></a></td>
                  <td style="padding:0 0 0 8px;"><a href="https://nextdoor.com/profile/01ZjZkwxhPWdnML2k" style="display:block;width:32px;height:32px;background:#2d3934;border-radius:999px;text-decoration:none;"><img src="${SOCIAL_ND_WHITE}" alt="Nextdoor" style="display:block;width:14px;height:14px;margin:9px auto;"></a></td>
                </tr>
              </table>
              <p style="margin:10px 0 0;font-size:10px;color:#8a948e;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Follow along</p>
            </td>
          </tr>
        </table>
      </td></tr>
    </table>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>`;

  return wrapperMode === 'minimal' ? minimalShell : fullShell;
}

module.exports = {
  LOGO_URL,
  SIGNATURE_IMAGE,
  renderMJML,
  renderWithBaseLayout,
  emailTemplate
};
