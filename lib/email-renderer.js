let mjml2html;

const LOGO_URL = 'https://app.pappaslandscaping.com/images/email-logo.png';
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

  // Define MJML components for the layout
  const header = `
    <mj-section background-color="#1f2933">
      <mj-column>
        <mj-image src="${LOGO_URL}" alt="Pappas & Co. Landscaping" width="210px" align="left" padding="18px 28px" />
      </mj-column>
    </mj-section>
  `;

  const footer = `
    <mj-section background-color="#ffffff" padding="28px 28px 0 28px">
      <mj-column>
        <mj-divider border-color="#d7dfd1" border-width="1px" padding="0" />
      </mj-column>
    </mj-section>
    <mj-section background-color="#f7f9f5" padding="20px 28px 24px 28px">
      <mj-column>
        <mj-text font-size="14px" line-height="1.6" color="#5b6773" align="center" padding="0">
          Questions? Call <a href="tel:4408867318" style="color:#5b6773;text-decoration:none;">(440) 886-7318</a> or email <a href="mailto:hello@pappaslandscaping.com" style="color:#5b6773;text-decoration:none;">hello@pappaslandscaping.com</a> and we’ll be happy to help.
        </mj-text>
        <mj-text font-size="13px" line-height="1.7" color="#5b6773" padding="16px 0 0 0" align="center">
          Pappas &amp; Co. Landscaping<br />
          (440) 886-7318<br />
          hello@pappaslandscaping.com
        </mj-text>
        <mj-text padding="14px 0 0 0" font-size="10px" color="#8b97a3" align="center">
          <a href="${baseUrl}/unsubscribe.html?email=${unsubscribeEmail}" style="color:#8b97a3;text-decoration:underline;">Unsubscribe</a> from marketing emails
        </mj-text>
      </mj-column>
    </mj-section>
  `;

  const features = showFeatures ? `
    <mj-section background-color="#ffffff" padding="8px 28px 0 28px">
      <mj-column background-color="#f7f9f5" border="1px solid #d7dfd1" border-radius="12px" padding="20px 22px">
        <mj-text font-size="11px" letter-spacing="0.8px" text-transform="uppercase" color="#6e7f6d" font-weight="700" padding="0 0 8px">Inside your account</mj-text>
        <mj-text font-size="22px" line-height="1.25" color="#1f2933" font-weight="800" padding="0 0 18px">Everything stays organized in one place</mj-text>
        <mj-table padding="0">
          <tr>
            <td style="padding:12px 0;border-bottom:1px solid #d7dfd1;">
              <strong style="color:#2e403d;">Service Schedule</strong><br/><span style="color:#6e7f6d;font-size:13px;">View upcoming visits and service history</span>
            </td>
          </tr>
          <tr>
            <td style="padding:12px 0;border-bottom:1px solid #d7dfd1;">
              <strong style="color:#2e403d;">Easy Payments</strong><br/><span style="color:#6e7f6d;font-size:13px;">Pay invoices securely online anytime</span>
            </td>
          </tr>
          <tr>
            <td style="padding:12px 0;">
              <strong style="color:#2e403d;">Quotes &amp; Invoices</strong><br/><span style="color:#6e7f6d;font-size:13px;">Access all your documents in one place</span>
            </td>
          </tr>
        </mj-table>
      </mj-column>
    </mj-section>
  ` : '';

  const mjml = `
    <mjml>
      <mj-head>
        <mj-attributes>
          <mj-all font-family="'Helvetica Neue', Helvetica, Arial, sans-serif" />
          <mj-body background-color="#eef2eb" />
          <mj-section padding="0" />
          <mj-column padding="0" />
          <mj-text font-size="16px" line-height="1.6" color="#425466" padding="0" />
          <mj-button background-color="#2e403d" color="#c9dd80" font-size="14px" font-weight="700" inner-padding="16px 28px" border-radius="8px" padding="0" />
        </mj-attributes>
        <mj-style inline="inline">
          .email-shell { box-shadow: 0 14px 34px rgba(31, 41, 51, 0.08); }
          .body-text a { color: #2e403d; text-decoration: underline; }
          .btn-primary a { color: #c9dd80 !important; }
        </mj-style>
      </mj-head>
      <mj-body>
        <mj-wrapper css-class="email-shell" background-color="#ffffff" padding="28px 16px" border-radius="16px">
        ${wrapper === 'full' ? header : ''}
        <mj-section background-color="#ffffff">
          <mj-column>
            ${content.includes('<mj-') ? content : `<mj-text align="left" padding="30px 28px 0 28px">\n              ${content}\n            </mj-text>`}
          </mj-column>
        </mj-section>
        ${features}
        ${wrapper === 'full' ? footer : ''}
        </mj-wrapper>
      </mj-body>
    </mjml>
  `;

  return renderMJML(mjml);
}

async function renderManagedEmail(content, options = {}) {
  const wrapper = ['full', 'minimal', 'none'].includes(options.wrapper) ? options.wrapper : 'full';
  const showFeatures = options.showFeatures || false;
  const baseUrl = options.baseUrl || process.env.BASE_URL || 'https://app.pappaslandscaping.com';
  const unsubscribeEmail = options.unsubscribeEmail || '{unsubscribe_email}';
  const isMJML = String(content || '').includes('<mj-') || String(content || '').includes('<mjml>');

  // Fully composed transactional emails already include their own branded shell.
  // With "No Wrapper" selected, keep the finished document intact.
  if (wrapper === 'none' && /<!doctype\s+html|<html[\s>]/i.test(String(content || ''))) {
    return String(content).replace(/\{unsubscribe_email\}/g, unsubscribeEmail);
  }

  if (isMJML || wrapper !== 'none') {
    return renderWithBaseLayout(content, {
      wrapper,
      showFeatures,
      showSignature: false,
      baseUrl,
      unsubscribeEmail
    });
  }

  let html = emailTemplate(content, { wrapper, showFeatures, showSignature: false });
  html = html.replace(/\{unsubscribe_email\}/g, unsubscribeEmail);
  return html;
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

function escapeEmailHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function buildServiceAgreementEmailV4({
  customerFirstName,
  estimateNumber,
  total,
  contractUrl,
  variant = 'ready',
  companyName = 'Pappas & Co. Landscaping',
  companyPhone = '(440) 886-7318',
  companyEmail = 'hello@pappaslandscaping.com',
  assetsUrl = process.env.EMAIL_ASSETS_URL || process.env.BASE_URL || 'https://app.pappaslandscaping.com',
} = {}) {
  const firstName = escapeEmailHtml(customerFirstName || 'there');
  const estimate = escapeEmailHtml(estimateNumber || '');
  const totalText = String(total ?? '0').trim();
  const agreementTotal = /^\{[^}]+\}$/.test(totalText)
    ? escapeEmailHtml(totalText)
    : `$${Number(totalText.replace(/[$,]/g, '') || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  const safeContractUrl = escapeEmailHtml(contractUrl || '#');
  const safeCompanyName = escapeEmailHtml(companyName);
  const safeCompanyPhone = escapeEmailHtml(companyPhone);
  const safeCompanyEmail = escapeEmailHtml(companyEmail);
  const safeAssetsUrl = String(assetsUrl || '').replace(/\/$/, '');
  const copyByVariant = {
    ready: {
      eyebrow: 'Agreement Ready',
      title: 'Your service agreement is ready',
      message: `Thank you for accepting estimate #${estimate}. Please review and sign your service agreement online using the link below.`,
      summary: 'A quick look before you review and sign.',
      firstStep: 'Review the agreement to make sure the scope of work, terms, and pricing match your accepted estimate.',
      secondStep: 'Sign the agreement online to complete this step.',
      thirdStep: 'Once it is signed, our team can move forward with scheduling your service.',
    },
    reminder: {
      eyebrow: 'Signature Reminder',
      title: 'One quick step left',
      message: `Just a quick reminder that your service agreement for estimate #${estimate} is still waiting for your signature. We have your estimate approval, but we need the signed agreement before we can schedule your service.`,
      summary: 'Your accepted estimate and service agreement are ready when you are.',
      firstStep: 'Review the agreement to confirm the scope of work, terms, and pricing.',
      secondStep: 'Add your signature online. It should only take a minute.',
      thirdStep: 'Once it is signed, our team can move forward with scheduling your service.',
    },
    final: {
      eyebrow: 'Final Reminder',
      title: 'Your agreement still needs a signature',
      message: `We still need your signature on the service agreement for estimate #${estimate}. If you would still like to move forward, please review and sign it using the link below. If you have questions or your plans have changed, just reply to this email and let us know.`,
      summary: 'The agreement remains available for you to review and sign.',
      firstStep: 'Review the agreement and make sure everything looks correct.',
      secondStep: 'Sign online if you are ready to move forward.',
      thirdStep: 'If you need help or no longer need service, reply to this email and we will update your request.',
    },
  };
  const copy = copyByVariant[variant] || copyByVariant.ready;

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Your Service Agreement is Ready</title>
  <style>
    @media only screen and (max-width:620px) {
      .email-shell { width:100% !important; }
      .mobile-pad { padding-left:22px !important; padding-right:22px !important; }
      .hero-title { font-size:32px !important; }
      .summary-column { display:table-cell !important; }
    }
  </style>
</head>
<body style="margin:0;padding:0;background-color:#ffffff;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;background-color:#ffffff;border-collapse:collapse;">
    <tr><td align="center" style="padding:24px 12px;">
      <table role="presentation" class="email-shell" width="600" cellpadding="0" cellspacing="0" border="0" style="width:600px;max-width:600px;border-collapse:separate;border-spacing:0;">
        <tr>
          <td style="background-color:#1f2933;padding:18px 28px;border-radius:22px 22px 0 0;text-align:left;">
            <img src="${safeAssetsUrl}/images/email-logo.png" alt="${safeCompanyName}" width="210" style="display:block;border:0;outline:none;text-decoration:none;height:auto;width:210px;max-width:100%;">
          </td>
        </tr>
        <tr><td style="background-color:#ffffff;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;">
            <tr><td class="mobile-pad" style="padding:34px 34px 0;">
              <div style="background:linear-gradient(135deg,#f7f9f5 0%,#edf3e6 100%);border-radius:22px;padding:30px 30px 28px;text-align:left;">
                <div style="font-family:Helvetica,Arial,sans-serif;font-size:11px;font-weight:700;letter-spacing:1px;line-height:1.6;text-transform:uppercase;color:#6e7f6d;">${copy.eyebrow}</div>
                <div class="hero-title" style="font-family:Helvetica,Arial,sans-serif;font-size:34px;font-weight:700;line-height:1.08;color:#2e403d;padding:12px 0 16px;max-width:430px;">${copy.title}</div>
                <div style="font-family:Helvetica,Arial,sans-serif;font-size:15px;line-height:1.8;color:#425466;max-width:470px;">Hi ${firstName},</div>
                <div style="font-family:Helvetica,Arial,sans-serif;font-size:15px;line-height:1.8;color:#425466;padding-top:12px;max-width:470px;">${copy.message}</div>
              </div>
            </td></tr>
            <tr><td class="mobile-pad" style="padding:22px 34px 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;">
                <tr>
                  <td class="summary-column" style="vertical-align:top;width:48%;padding-right:18px;text-align:left;">
                    <div style="font-family:Helvetica,Arial,sans-serif;font-size:11px;font-weight:700;letter-spacing:.8px;line-height:1.6;text-transform:uppercase;color:#6e7f6d;">Agreement Summary</div>
                    <div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;line-height:1.75;color:#425466;padding-top:10px;">${copy.summary}</div>
                  </td>
                  <td class="summary-column" style="vertical-align:top;width:52%;text-align:left;">
                    <div style="border-left:3px solid #c9dd80;padding-left:16px;">
                      <div style="font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.7;color:#2e403d;padding-bottom:10px;"><strong>Estimate:</strong> #${estimate}</div>
                      <div style="font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.7;color:#2e403d;"><strong>Total:</strong> ${agreementTotal}</div>
                    </div>
                  </td>
                </tr>
              </table>
            </td></tr>
            <tr><td align="center" style="padding:20px 22px 0;">
              <a href="${safeContractUrl}" style="display:inline-block;background-color:#c9dd80;border:6px solid #c9dd80;border-radius:6px;color:#2e403d;font-family:Helvetica,Arial,sans-serif;font-size:14px;font-weight:700;line-height:2;padding:5px 15px;text-align:center;text-decoration:none;">Review &amp; Sign Agreement</a>
            </td></tr>
            <tr><td class="mobile-pad" style="padding:28px 28px 4px;">
              <div style="border-top:1px solid #d7dfd1;padding-top:18px;">
                <div style="font-family:Helvetica,Arial,sans-serif;font-size:11px;font-weight:700;letter-spacing:.8px;line-height:1.6;text-transform:uppercase;color:#6e7f6d;text-align:center;padding-bottom:14px;">Next Steps</div>
                <div style="font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.7;color:#2e403d;padding-bottom:14px;">${copy.firstStep}</div>
                <div style="font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.7;color:#2e403d;padding-bottom:14px;">${copy.secondStep}</div>
                <div style="font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.7;color:#2e403d;">${copy.thirdStep}</div>
              </div>
            </td></tr>
            <tr><td class="mobile-pad" style="padding:26px 28px 0;">
              <div style="border-top:1px solid #d7dfd1;font-size:1px;line-height:1px;">&nbsp;</div>
            </td></tr>
            <tr><td style="background-color:#f7f9f5;padding:20px 28px 24px;border-radius:0 0 22px 22px;">
              <div style="font-family:Helvetica,Arial,sans-serif;font-size:14px;line-height:1.65;color:#5b6773;text-align:center;">Questions about your agreement? Call or text ${safeCompanyPhone} or email ${safeCompanyEmail} and we’ll be happy to help.</div>
              <div style="font-size:0;line-height:0;text-align:center;padding-top:16px;">
                <a href="https://www.facebook.com/pappaslandscaping" style="display:inline-block;background-color:#2e403d;border-radius:999px;padding:8px;margin-right:10px;"><img src="${safeAssetsUrl}/email-assets/fb-white.png" alt="Facebook" width="20" height="20" style="display:block;border:0;"></a>
                <a href="https://www.instagram.com/pappaslandscaping" style="display:inline-block;background-color:#2e403d;border-radius:999px;padding:8px;"><img src="${safeAssetsUrl}/email-assets/ig-white.png" alt="Instagram" width="20" height="20" style="display:block;border:0;"></a>
              </div>
              <div style="font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.7;color:#5b6773;padding-top:16px;text-align:center;">${safeCompanyName}<br>${safeCompanyPhone}<br>${safeCompanyEmail}</div>
            </td></tr>
          </table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`;
}

module.exports = {
  LOGO_URL,
  SIGNATURE_IMAGE,
  renderMJML,
  renderWithBaseLayout,
  renderManagedEmail,
  emailTemplate,
  buildServiceAgreementEmailV4
};
