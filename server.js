require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
const jwt = require('jsonwebtoken');
const twilio = require('twilio');
const OAuthClient = require('intuit-oauth');

const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype.startsWith('image/') || file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Only images and CSV files are allowed'));
    }
  }
});

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '15mb' }));
app.use(express.urlencoded({ limit: '15mb', extended: true }));
app.use(express.static('public', {
  setHeaders: (res, path) => {
    if (path.endsWith('.html') || path.endsWith('.js')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
    }
  }
}));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Email via Resend API
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const NOTIFICATION_EMAIL = process.env.NOTIFICATION_EMAIL || 'hello@pappaslandscaping.com';
const FROM_EMAIL = 'Pappas & Co. Landscaping <hello@pappaslandscaping.com>';
// CopilotCRM-hosted logo (green leaf with text)
const LOGO_URL = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/Your%20paragraph%20text%20%284.75%20x%202%20in%29%20%28800%20x%20400%20px%29%20%282%29.png';
const COMPANY_NAME = 'Pappas & Co. Landscaping';

// TwilioConnect App Config
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
// Multi-number support - both business lines
const TWILIO_NUMBERS = {
  '4408867318': '+14408867318',  // Primary (440) 886-7318
  '2169413737': '+12169413737'   // Secondary (216) 941-3737
};
const TWILIO_PHONE_NUMBER = '+14408867318'; // Default for backward compatibility
const JWT_SECRET = process.env.JWT_SECRET || 'pappas-twilioconnect-secret-2026';
const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

// ═══════════════════════════════════════════════════════════
// SERVICE DESCRIPTIONS - From CopilotCRM for quotes and customer-facing displays
// ═══════════════════════════════════════════════════════════
const SERVICE_DESCRIPTIONS = {
  'Mowing (Weekly)': '1. Mowing: We use commercial-grade walk-behind mowers to cut your lawn weekly at the proper height for healthy growth and a well-maintained appearance. Cutting height is adjusted based on seasonal conditions.<br>2. Trimming: We trim around trees, flower beds, and pathways to maintain a clean, uniform look in areas mowers can\'t reach.<br>3. Cleanup: All grass clippings and debris will be removed, and all concrete surfaces (sidewalks, driveways, patios) will be blown off.<br>Edging of concrete is available as an add-on for an additional cost.',
  'Mowing (Bi-Weekly)': '1. Mowing: We use commercial-grade walk-behind mowers to cut your lawn bi-weekly at the proper height for healthy growth and a well-maintained appearance. Cutting height is adjusted based on seasonal conditions.<br>2. Trimming: We trim around trees, flower beds, and pathways to maintain a clean, uniform look in areas mowers can\'t reach.<br>3. Cleanup: All grass clippings and debris will be removed, and all concrete surfaces (sidewalks, driveways, patios) will be blown off.<br>Edging of concrete is available as an add-on for an additional cost.',
  'Mowing (Monthly)': 'Mowing: We will use commercial-grade walk behind mowers to efficiently mow your lawn weekly to the appropriate height, promoting healthy growth and a lush appearance. We adjust the cutting height based on seasonal conditions and grass type to achieve optimal results.<br>Trimming: We will trim around landscape features, such as trees, flower beds, and pathways, using string trimmers to reach areas inaccessible to mowers. This attention to detail ensures a clean and polished look for your entire lawn.<br>Edging (Optional): We will carefully edge along sidewalks, driveways, and other hardscape surfaces using precision tools, creating crisp lines that enhance the overall appearance of your property. Edging also helps prevent grass from encroaching onto paved areas. Be aware, we do not edge asphalt driveways.<br>Cleanup: After completing the mowing, trimming, and edging tasks, we thoroughly clean up any grass clippings and debris from your property.',
  'Mowing (One Time Cut)': 'Mowing: We will use commercial-grade walk behind mowers to efficiently mow your lawn one time to the appropriate height, promoting healthy growth and a lush appearance. We adjust the cutting height based on seasonal conditions and grass type to achieve optimal results.<br>Trimming: We will trim around landscape features, such as trees, flower beds, and pathways, using string trimmers to reach areas inaccessible to mowers. This attention to detail ensures a clean and polished look for your entire lawn.<br>Edging: We will carefully edge along sidewalks, driveways, and other hardscape surfaces using precision tools, creating crisp lines that enhance the overall appearance of your property. Edging also helps prevent grass from encroaching onto paved areas. Be aware, we do not edge asphalt driveways.<br>Cleanup: After completing the mowing, trimming, and edging tasks, we thoroughly clean up any grass clippings and debris from your property.',
  'Spring Cleanup': '1. Debris Removal: We will clear fallen branches, leaves, and dead plant material from your yard, landscaping beds, and around trees and shrubs.<br>2. Lawn Blowing: We will blow through your lawn to remove thatch, dead grass, acorns, and small sticks, creating a clean surface for healthy growth.<br>3. Perennial Trimming: We will trim back only overgrown perennials to prepare them for the growing season while leaving others undisturbed.',
  'Fall Cleanup': 'Collection and removal of any leaves and debris littering your property as a result of the changing seasons. Multiple visits as needed throughout the fall season.',
  'Fall Cleanup (One Time Charge)': 'Leaf Removal:<br>1. Our fall cleanup service includes thorough weekly leaf removal from the end of October through the end of November, or early December, depending on the weather, to prevent the buildup of leaves on lawns, flower beds, and other landscape areas.<br>2. We use specialized equipment to efficiently collect leaves, ensuring a tidy and debris-free environment.<br><br>Debris Removal:<br>1. In addition to leaves, our fall cleanup service addresses the removal of other debris such as branches.<br>2. We dispose of the debris, leaving your property clean and clutter-free.',
  'Mulching': 'Premium Mulch: We use triple-shredded mulch to enhance your landscape and refresh your beds.<br>Professional Application: Mulch will be evenly spread to ensure proper coverage while avoiding buildup around plants and trees.<br>Weed Prevention: We will apply Snapshot Weed Preventer to help suppress weed growth and reduce maintenance.',
  'Mulch Installation': 'Install 2" of high-quality black mulch over landscaping beds to enhance moisture retention, suppress weeds, and provide a finished, polished appearance.',
  'Aeration': 'Soil Aeration: We will perforate your lawn with small cores to improve air, water, and nutrient flow, reducing compaction and supporting stronger root growth.<br>Full Coverage: We will aerate the entire lawn, focusing on compacted and high-traffic areas for maximum benefit.<br>Optimal Timing: Service will be scheduled at the best time to promote healthy turf growth.<br>Overseeding (Optional): For a thicker lawn, overseeding can be added to help fill in thin or bare spots after aeration.',
  'Core Aeration': 'Soil Aeration: We will perforate your lawn with small cores to improve air, water, and nutrient flow, reducing compaction and supporting stronger root growth.<br>Full Coverage: We will aerate the entire lawn, focusing on compacted and high-traffic areas for maximum benefit.<br>Optimal Timing: Service will be scheduled at the best time to promote healthy turf growth.',
  'Dethatching': 'Thorough Thatch Removal: We utilize professional-grade dethatching equipment to systematically remove excess thatch from your lawn, ensuring thorough coverage and optimal results.<br>Enhanced Grass Health: By eliminating thatch buildup, our service promotes better air circulation, water penetration, and nutrient absorption, fostering healthier grass growth and root development.<br>Improved Lawn Aesthetic: Dethatching not only enhances the health of your lawn but also improves its visual appeal, resulting in a lush, green lawn that enhances the beauty of your property.',
  'Overseeding': 'Seed Selection: We offer grass seed tailored to suit your specific climate, soil type, and sun exposure, ensuring the best possible outcomes for your lawn.<br>Thorough Site Preparation: Prior to overseeding, we prepare the lawn and address any existing weeds or debris, creating an ideal environment for seed germination and growth.<br>Professional Application Techniques: We employ overseeding techniques to ensure even seed distribution and proper seed-to-soil contact, maximizing seed germination rates and promoting robust turf establishment.<br>Seasonal Timing: We schedule overseeding services at optimal times of the year to coincide with favorable weather conditions and grass growth cycles, maximizing the success of seed establishment and minimizing potential competition from weeds.<br>Post-Overseeding Care: Following overseeding, we provide guidance on post-care maintenance practices, including watering schedules, mowing heights, and fertilization routines, to support healthy seedling growth and long-term lawn vitality.',
  'Fertilizing': 'Professional lawn fertilization application using premium fertilizer for healthy, green grass. Application rates and timing optimized for Northeast Ohio climate conditions.',
  'Fertilizing - Early Spring': 'Fertilization & Pre-Emergent Crabgrass Control<br><br>Pre-Emergent is formulated to reduce unsightly infestation of annual grasses from emerging such as crabgrass, goosegrass, foxtail, and barnyard. This product works by establishing a barrier at the soil surface that interrupts the development of these grasses. Fertilizer is crucial to help the lawn recover from winter stresses and helps promote spring greening without excessive top growth.',
  'Fertilizing - Late Spring': 'Weed Control & Fertilization<br><br>This low-volume liquid product is formulated to control existing broadleaf weeds such as dandelions, plantain, chickweed, thistle, spurge, and clover. Fertilizer will provide the nutrients to improve your lawn\'s color, heartiness, and density.',
  'Fertilizing - Early Summer': 'Fertilization, Insecticide, & Weed Control<br><br>The insect-control product helps to prevent infestation of lawn-damaging surface insects such as chinch bugs, billbugs, and sod webworms and subsurface insects such as white grubs. Fertilizer provides nutrients to improve tolerance to summer heat and drought, which will help to sustain your lawn through the stresses of summer. Broadleaf weed control is applied as required to help maintain a weed-free lawn.',
  'Fertilizing - Late Summer': 'Fertilization & Weed Control<br><br>Fertilizer will help your lawn recover from the stresses of summer and help build new roots, tillers, and grass plants. Cooler temperatures and better moisture accelerate plant growth and increase density through early fall. Broadleaf weed control is applied as required to help maintain a weed-free lawn.',
  'Fertilizing - Fall': 'Fertilization & Winterization<br><br>Fertilizer promotes healthy root growth and development, which takes place from late fall into early winter. It replenishes important nutrient reserves in the soil, which provides extra energy for winter survival and is stored to be used for an early spring green-up.',
  'Fertilizing (Per Application)': 'Fertilization Plan: We provide a five application fertilization plan throughout the landscaping season which includes an application in early spring, late spring, early summer, late summer and fall.<br>High-Quality Fertilizers: We use premium-quality fertilizers specially formulated to deliver the essential nutrients your lawn needs for optimal growth and vitality. Our fertilizers are carefully selected to provide balanced nutrition.<br>Timely Application: We\'ll schedule the five fertilization treatments throughout the growing season to ensure consistent nutrient availability for your lawn. We will apply fertilizers at the appropriate times to coincide with key growth stages and maximize effectiveness.<br>Weed and Pest Control: In addition to providing essential nutrients, our fertilization treatments may include weed and pest control measures to help keep your lawn healthy and free from unwanted invaders. We\'ll target common lawn pests and weeds while minimizing the impact on beneficial organisms and the environment.<br>Expert Application: We will apply fertilizers with precision and care, ensuring even coverage and minimizing waste. We\'ll take care to avoid fertilizer runoff and oversaturation, following best practices for responsible application.',
  'Shrub Trimming': 'Professional shrub and hedge trimming to maintain shape, promote healthy growth, and enhance curb appeal. All clippings removed from property.',
  'Shrub Trimming (Per Occurrence)': 'Precision Pruning: We will remove dead or overgrown branches to support healthy growth and maintain a neat appearance.<br>Shape Enhancement: Shrubs will be trimmed to enhance their natural form and complement your landscape.<br>Size Management: We will keep shrubs at a balanced size to prevent overgrowth and maintain curb appeal.<br>Cleanup: All trimmings and debris will be removed, leaving your yard clean and tidy.',
  'Bush/Shrub Trimming': 'Professional shrub and hedge trimming to maintain shape, promote healthy growth, and enhance curb appeal. All clippings removed from property.',
  'Bed Edging': 'Create Defined Edges: We cut a clean, defined edge between your lawn and landscape beds using a professional bed edger.<br>Professional Equipment: All edging is done with a commercial-grade bed edger for clean, consistent results.<br>Debris Cleanup: We remove any loose soil, grass, or debris created during the edging process.',
  'Add-On Service: Edging': 'Edging along sidewalks, driveways (excluding asphalt), and hardscapes. This creates clean lines and helps prevent grass overgrowth.',
  'Weed Control (Monthly)': 'Targeted Herbicide Application: We will apply a professional-grade herbicide to eliminate actively growing weeds in your landscape beds. This ensures effective weed control while minimizing disruption to surrounding plants.<br>Thorough Coverage: Our application process ensures that all areas with weed growth receive proper treatment. We focus on high-growth areas and problem spots to maximize effectiveness.<br>Drying and Absorption: To allow for maximum effectiveness, the herbicide needs time to absorb into the weeds. We recommend avoiding watering or disturbing the treated areas for at least 24 hours.<br>Weed Decomposition: Treated weeds will begin to wither and die within 7–14 days. No manual removal is performed; weeds will naturally break down over time.<br>Ongoing Monthly Service: This service will be performed once per month from April through September to maintain a weed-free landscape. Regular applications ensure continued control throughout the growing season.',
  'Stump Grinding': 'Initial Assessment: Our team will conduct a thorough inspection of the stump(s) and surrounding area to determine the best approach for grinding.<br>State-of-the-Art Equipment: We use professional stump grinding equipment to ensure precise and effective removal of stumps, minimizing disruption to your yard.<br>Safety First: Our trained professionals prioritize safety, ensuring that all stump grinding operations are carried out with the utmost care to protect your property and nearby structures.<br>Clean-Up: After grinding, we will clean up the debris, leaving your yard tidy and ready for your next landscaping project.<br>Site Restoration: If requested, we can also fill the resulting hole with soil and seed it to blend seamlessly with your existing lawn.',
  'Power Washing': 'Surface Cleaning: We will use professional-grade power washing equipment to remove dirt, grime, algae, and stains from your driveways, sidewalks, and patios. This deep cleaning process restores the original appearance of your surfaces and enhances your property\'s curb appeal.<br>Pressure Adjustment: We will adjust the water pressure and technique based on the surface material to ensure effective cleaning without causing damage. This approach helps maintain the longevity and integrity of your hardscapes.<br>Detail Work: We will focus on stubborn stains and high-traffic areas, using specialized methods to lift embedded dirt and restore a fresh, clean look.<br>Final Rinse & Cleanup: After completing the power washing process, we will rinse all surfaces thoroughly and ensure your property is left clean and free of debris.',
  'Storm Cleanup': 'Debris Removal: We\'ll remove fallen branches, leaves, and any other storm-related debris scattered across your yard, ensuring a clean and safe environment.<br>Tree and Shrub Cleanup: Our team will clear away broken branches and damaged plants, carefully disposing of all storm-damaged vegetation.<br>Lawn and Garden Bed Cleanup: We\'ll tidy up your lawn and garden beds, removing debris.<br>Final Cleanup: We\'ll conduct a thorough final cleanup, leaving your yard neat, tidy, and ready for use.',
  'Top Dressing': 'Soil Application: We\'ll spread a layer of soil to improve turf growth and support a healthy lawn.<br>Starter Fertilizer: A fertilizer mix will be added to strengthen roots and encourage growth.<br>Peat Moss Layer: Peat moss will help retain moisture and improve soil quality.<br>Even Coverage: Materials will be applied evenly for a smooth and well-prepared surface.<br>Cleanup: We\'ll remove any excess materials, leaving your yard neat and ready for growth.',
  'Yard Cleanup': 'Debris Removal: We remove leaves, sticks, branches, and other debris from the lawn, landscape beds, and open areas for a clean, maintained look.<br>Weeding & Overgrowth Control: We remove weeds from beds and hard surfaces and cut back any overgrown plants or perennials as needed.<br>Concrete Edging: We edge along sidewalks, driveways, and other concrete surfaces to create crisp, clean lines.<br>Blowing & Surface Cleanup: All hard surfaces are blown clean of grass, mulch, and debris.<br>Haul-Away: All collected yard waste and debris are removed from the property.',
  'Backyard Bed Cleanup': 'Bed Cleanup: We will remove leaves, weeds, and debris from your backyard garden beds, creating a clean and tidy space ready for planting. This process ensures a well-maintained appearance and a healthier growing environment.<br>Bed Edging (Optional): We will redefine the edges of your garden beds to create crisp, clean lines that enhance the overall aesthetic of your landscape. This helps prevent grass and weeds from creeping into the planting areas.<br>Cleanup: After completing the service, we will remove and haul away all debris, leaving your beds neat and ready for planting.',
  'Snow Removal': 'Driveway clearing after every snow event of 2 inches or more, ensuring your driveway remains accessible throughout the season.',
  'Snow Removal Contract': 'Plowing: Driveway clearing after every snow event of two (2) inches or more, ensuring your driveway remains accessible throughout the season.<br><br>Snow Removal Timing: Snow will be removed from the driveway upon completion of snowfall when it reaches two (2) inches or more. Plowing will occur at any time throughout the 24-hour period based on snow conditions.<br><br>Specific Conditions: Must be two (2) inches of new snowfall, not drifting snow. Actual service times depend on various factors, such as the time of snowfall, its duration, and total accumulation.<br><br>Fixed Seasonal Rate: No surprises—just a straightforward fee for peace of mind all season.<br><br>Season Duration: December 1st - March 31st<br><br>Important Terms:<br>No Refunds or Rebates: There shall be no refund, rebate, or offset to the total amount paid, regardless of the total snowfall accumulation for the season.<br><br>Disclaimer: While we make every effort to keep your driveway free of snow and ice, Pappas & Co. Landscaping is not responsible for any slips, falls, or accidents that may occur before, during, or after our service. Weather conditions can change rapidly, and it is the property owner\'s responsibility to take additional safety measures, such as applying salt or ice melt, to further reduce hazards. By choosing our services, you acknowledge and accept that snow and ice management ultimately remains your responsibility.',
  'Snow Removal Per Push': 'Plowing: Driveway clearing after each snow event of 2 inches or more. Service is billed per visit.<br><br>Snow Removal Timing: Snow will be plowed once snowfall has ended and accumulation reaches 2 inches or more. Plowing may occur at any time within a 24-hour period depending on snow conditions.<br><br>Specific Conditions: Service applies only to 2 inches of new snowfall (not drifting snow). Timing will vary based on start, duration, and total accumulation of each event.<br><br>Billing: Each plow is invoiced individually after service is completed. Rates are based on your property size and agreed per-push pricing.<br><br>Season Duration: December 1st – March 31st<br><br>Important Terms:<br>No minimum or maximum number of visits guaranteed.<br>Charges apply only when service is performed.<br><br>Disclaimer: While we make every effort to keep your driveway/parking lot clear, Pappas & Co. Landscaping is not responsible for slips, falls, or accidents that may occur before, during, or after service. Weather conditions can change quickly. Property owners are responsible for applying salt or ice melt to further reduce hazards.',
  'Salt Application (Per Visit)': 'Deicing Treatment: Application of salt (or ice melt as specified) to driveways, walkways, and/or parking lots after each qualifying snow or ice event. Service is billed per visit.<br><br>Application Timing: Salt will be applied after plowing. In the event of an ongoing storm, salt and/or salt substitute shall be applied as needed to keep areas under control and safe for pedestrians and vehicles. Service may occur at any time within a 24-hour period depending on conditions.<br><br>Specific Conditions: Timing depends on snowfall start, duration, and total accumulation. Additional visits may be required if ice refreezes or additional accumulation occurs.<br><br>Billing: Each salt application is invoiced individually after service is completed. Rates are based on your property size.<br><br>Season Duration: December 1st – March 31st<br><br>Important Terms:<br>No minimum or maximum number of applications guaranteed.<br><br>Disclaimer: While salt greatly reduces ice hazards, Pappas & Co. Landscaping is not responsible for slips, falls, or accidents that may occur before, during, or after service. Weather conditions can change quickly, and salt effectiveness may vary. Property owners should monitor surfaces and may need to request additional treatments. By choosing this service, you accept that snow and ice management ultimately remains your responsibility.',
  '2025-2026 Snow Removal Contract': 'Plowing: Driveway clearing after every snow event of 2 inches or more, ensuring your driveway remains accessible throughout the season.<br><br>Snow Removal Timing: Snow will be removed from the driveway upon completion of snowfall when it reaches 2 inches or more. Plowing will occur at any time throughout the 24-hour period based on snow conditions.<br><br>Specific Conditions: Must be 2 inches of new snowfall, not drifting snow. Actual service times depend on various factors, such as the time of snowfall, its duration, and total accumulation.<br><br>Fixed Seasonal Rate: No surprises—just a straightforward fee for peace of mind all season.<br><br>Season Duration: December 1, 2025 - March 31, 2026<br><br>Important Terms:<br>No Refunds or Rebates: There shall be no refund, rebate, or offset to the total amount paid, regardless of the total snowfall accumulation for the season.<br><br>Disclaimer: While we make every effort to keep your driveway free of snow and ice, Pappas & Co. Landscaping is not responsible for any slips, falls, or accidents that may occur before, during, or after our service. Weather conditions can change rapidly, and it is the property owner\'s responsibility to take additional safety measures, such as applying salt or ice melt, to further reduce hazards. By choosing our services, you acknowledge and accept that snow and ice management ultimately remains your responsibility.',
  'Soil Repair': 'Soil Aeration: To alleviate compaction and improve soil structure, we use equipment to aerate the soil. This process creates pathways for air, water, and nutrients to penetrate the soil, promoting healthy root growth and plant development.<br>Seed Installation: We seed the area with grass seeds selected for their compatibility with your soil type and climate.<br>Peat Moss Application: Peat moss is an excellent soil conditioner that helps improve soil structure and moisture retention. We apply peat moss to the surface of the soil to provide a protective layer that promotes seed germination and root establishment.'
};

// Helper to get service description (returns with <br> tags for HTML, or plain text for PDF)
function getServiceDescription(serviceName, forPdf = false) {
  if (!serviceName) return '';
  let desc = '';
  
  // Try exact match first
  if (SERVICE_DESCRIPTIONS[serviceName]) {
    desc = SERVICE_DESCRIPTIONS[serviceName];
  } else {
    // Try partial match
    const lowerName = serviceName.toLowerCase();
    for (const [key, d] of Object.entries(SERVICE_DESCRIPTIONS)) {
      if (key.toLowerCase() === lowerName) {
        desc = d;
        break;
      }
    }
    // Try matching just the base service name
    if (!desc) {
      for (const [key, d] of Object.entries(SERVICE_DESCRIPTIONS)) {
        const baseKey = key.split(' (')[0].toLowerCase();
        const baseName = serviceName.split(' (')[0].toLowerCase();
        if (baseKey === baseName) {
          desc = d;
          break;
        }
      }
    }
  }
  
  // For PDF, convert <br> to newlines
  if (forPdf && desc) {
    return desc.replace(/<br>/g, '\n');
  }
  return desc;
}

// JWT Auth Middleware for TwilioConnect App
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ message: 'Access token required' });
  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ message: 'Invalid or expired token' });
    req.user = user;
    next();
  });
};

async function sendEmail(to, subject, html, attachments = null) {
  if (!RESEND_API_KEY) return;
  try {
    const payload = { from: FROM_EMAIL, to: [to], subject, html };
    if (attachments) payload.attachments = attachments;
    await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
  } catch (err) { console.error('Email failed:', err); }
}

// Cohesive email template wrapper - matches CopilotCRM style
const SIGNATURE_IMAGE = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/White%20Modern%20Minimalist%20Signature%20Brand%20Logo%20%281200%20x%20300%20px%29%20%281%29.png';
const SOCIAL_FACEBOOK = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/logo_facebook_chatting_brand_social_media_application_icon_210431.png';
const SOCIAL_INSTAGRAM = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/instagram_social_media_brand_logo_application_icon_210428.png';
const SOCIAL_NEXTDOOR = 'https://prod-beefree-images.s3.amazonaws.com/images/copilot-template-builder-5261/social_media_brand_logo_application_nextdoor_icon_210365.png';

function emailTemplate(content, options = {}) {
  const showFooterFeatures = options.showFeatures || false;
  const showSignature = options.showSignature !== false; // Default to true
  
  const signatureHtml = showSignature ? `
    <div style="margin-top:35px;padding-top:20px;">
      <img src="${SIGNATURE_IMAGE}" alt="Timothy Pappas" style="max-width:450px;width:100%;height:auto;">
    </div>
  ` : '';
  
  const featuresSection = showFooterFeatures ? `
    <tr><td style="padding:32px 40px;border-top:1px solid #e5e5e5;">
      <p style="text-align:center;font-family:'Playfair Display',Georgia,serif;font-size:22px;color:#1e293b;font-weight:400;margin:0 0 24px;">What's Inside</p>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;">
            <table cellpadding="0" cellspacing="0"><tr>
              <td style="width:40px;vertical-align:top;"><span style="font-size:20px;">📅</span></td>
              <td><strong style="color:#1e293b;">Service Schedule</strong><br><span style="color:#64748b;font-size:13px;">View upcoming visits and service history</span></td>
            </tr></table>
          </td>
        </tr>
        <tr>
          <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;">
            <table cellpadding="0" cellspacing="0"><tr>
              <td style="width:40px;vertical-align:top;"><span style="font-size:20px;">💳</span></td>
              <td><strong style="color:#1e293b;">Easy Payments</strong><br><span style="color:#64748b;font-size:13px;">Pay invoices securely online anytime</span></td>
            </tr></table>
          </td>
        </tr>
        <tr>
          <td style="padding:12px 0;border-bottom:1px solid #f1f5f9;">
            <table cellpadding="0" cellspacing="0"><tr>
              <td style="width:40px;vertical-align:top;"><span style="font-size:20px;">💬</span></td>
              <td><strong style="color:#1e293b;">Direct Messaging</strong><br><span style="color:#64748b;font-size:13px;">Send questions or requests to our team</span></td>
            </tr></table>
          </td>
        </tr>
        <tr>
          <td style="padding:12px 0;">
            <table cellpadding="0" cellspacing="0"><tr>
              <td style="width:40px;vertical-align:top;"><span style="font-size:20px;">📄</span></td>
              <td><strong style="color:#1e293b;">Quotes & Invoices</strong><br><span style="color:#64748b;font-size:13px;">Access all your documents in one place</span></td>
            </tr></table>
          </td>
        </tr>
      </table>
    </td></tr>
  ` : '';

  return `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&display=swap" rel="stylesheet">
</head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,0.08);">
  <tr><td style="background:#2e403d;padding:40px;text-align:center;">
    <img src="${LOGO_URL}" alt="Pappas & Co. Landscaping" style="max-height:100px;max-width:400px;">
  </td></tr>
  <tr><td style="padding:40px;">
    ${content}
    ${signatureHtml}
  </td></tr>
  ${featuresSection}
  <tr><td style="background:#f8fafc;padding:24px 40px;text-align:center;border-top:3px solid #c9dd80;">
    <p style="margin:0 0 16px;font-size:14px;color:#475569;">Questions? Reply to this email or call <a href="tel:4408867318" style="color:#2e403d;font-weight:600;text-decoration:none;">(440) 886-7318</a></p>
    <table cellpadding="0" cellspacing="0" style="margin:0 auto 16px;">
      <tr>
        <td style="padding:0 8px;"><a href="https://www.facebook.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_FACEBOOK}" alt="Facebook" style="width:28px;height:28px;"></a></td>
        <td style="padding:0 8px;"><a href="https://www.instagram.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_INSTAGRAM}" alt="Instagram" style="width:28px;height:28px;"></a></td>
        <td style="padding:0 8px;"><a href="https://nextdoor.com/profile/01ZjZkwxhPWdnML2k" style="text-decoration:none;"><img src="${SOCIAL_NEXTDOOR}" alt="Nextdoor" style="width:28px;height:28px;"></a></td>
      </tr>
    </table>
    <p style="margin:0 0 4px;font-size:13px;color:#64748b;font-weight:600;">Pappas & Co. Landscaping</p>
    <p style="margin:0 0 4px;font-size:12px;color:#94a3b8;">PO Box 770057 • Lakewood, Ohio 44107</p>
    <p style="margin:0;font-size:12px;"><a href="https://pappaslandscaping.com" style="color:#2e403d;text-decoration:none;">pappaslandscaping.com</a></p>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>`;
}

// Generate filled PDF contract from scratch using pdf-lib
async function generateContractPDF(quote, signatureData, signedBy, signedDate) {
  try {
    console.log('Starting PDF generation for contract...');
    const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
    const fontkit = require('@pdf-lib/fontkit');
    const fs = require('fs');
    const path = require('path');
    console.log('pdf-lib loaded successfully');

    // Create a new PDF document
    const pdfDoc = await PDFDocument.create();
    pdfDoc.registerFontkit(fontkit);
    console.log('PDF document created');

    // Embed fonts
    const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);
    const helveticaBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

    // Embed Qualy font for headers
    let qualyFont = helveticaBold; // fallback
    try {
      const qualyPath = path.join(__dirname, 'public', 'Qualy.otf');
      if (fs.existsSync(qualyPath)) {
        const qualyBytes = fs.readFileSync(qualyPath);
        qualyFont = await pdfDoc.embedFont(qualyBytes);
        console.log('Qualy font embedded in contract PDF');
      }
    } catch (fontErr) {
      console.log('Could not embed Qualy font in contract:', fontErr.message);
    }
    console.log('Fonts embedded');

    // Try to embed logo
    let logoImage = null;
    try {
      const logoPath = path.join(__dirname, 'public', 'logo.png');
      if (fs.existsSync(logoPath)) {
        logoImage = await pdfDoc.embedPng(fs.readFileSync(logoPath));
      }
    } catch (logoErr) {
      console.log('Could not embed logo in contract PDF:', logoErr.message);
    }
    
    // Page dimensions (US Letter)
    const pageWidth = 612;
    const pageHeight = 792;
    const margin = 50;
    const contentWidth = pageWidth - (margin * 2);
    
    // Colors
    const darkGreen = rgb(0.18, 0.25, 0.24); // #2e403d
    const limeGreen = rgb(0.79, 0.87, 0.50); // #c9dd80
    const black = rgb(0, 0, 0);
    const gray = rgb(0.4, 0.4, 0.4);
    
    // Parse services
    let services = [];
    try {
      services = typeof quote.services === 'string' ? JSON.parse(quote.services) : quote.services;
    } catch (e) {
      services = [];
    }
    
    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    
    // Helper function to add a new page
    function addPage() {
      const page = pdfDoc.addPage([pageWidth, pageHeight]);
      return page;
    }
    
    // Helper function to draw wrapped text
    function drawWrappedText(page, text, x, y, maxWidth, font, size, color, lineHeight = 1.3) {
      const words = text.split(' ');
      let line = '';
      let currentY = y;
      
      for (const word of words) {
        const testLine = line + (line ? ' ' : '') + word;
        const testWidth = font.widthOfTextAtSize(testLine, size);
        
        if (testWidth > maxWidth && line) {
          page.drawText(line, { x, y: currentY, size, font, color });
          line = word;
          currentY -= size * lineHeight;
        } else {
          line = testLine;
        }
      }
      
      if (line) {
        page.drawText(line, { x, y: currentY, size, font, color });
        currentY -= size * lineHeight;
      }
      
      return currentY;
    }
    
    // ========== PAGE 1 ==========
    let page = addPage();
    let y = pageHeight - margin;

    // Header: logo (or text fallback) + contact info on right
    if (logoImage) {
      const logoDims = logoImage.scale(0.26);
      page.drawImage(logoImage, { x: margin, y: y - logoDims.height, width: logoDims.width, height: logoDims.height });
      y -= logoDims.height + 4;
    } else {
      page.drawText('Pappas & Co. Landscaping', { x: margin, y, size: 20, font: qualyFont, color: darkGreen });
      y -= 24;
    }
    page.drawText('pappaslandscaping.com', { x: pageWidth - margin - 130, y: pageHeight - margin, size: 9, font: helvetica, color: gray });
    page.drawText('hello@pappaslandscaping.com', { x: pageWidth - margin - 130, y: pageHeight - margin - 12, size: 9, font: helvetica, color: gray });
    page.drawText('(440) 886-7318', { x: pageWidth - margin - 130, y: pageHeight - margin - 24, size: 9, font: helvetica, color: gray });

    page.drawText('SERVICE AGREEMENT', { x: margin, y, size: 11, font: qualyFont, color: gray });
    page.drawText(`Quote #${quoteNumber}`, { x: margin + 155, y, size: 11, font: helvetica, color: gray });

    // Lime accent line
    y -= 14;
    page.drawRectangle({ x: margin, y, width: contentWidth, height: 3, color: limeGreen });
    y -= 25;
    
    // Two column layout for parties
    const colWidth = (contentWidth - 20) / 2;
    
    // Service Provider
    page.drawText('SERVICE PROVIDER', { x: margin, y, size: 8, font: helveticaBold, color: gray });
    y -= 14;
    page.drawText('Pappas & Co. Landscaping', { x: margin, y, size: 10, font: helveticaBold, color: black });
    y -= 12;
    page.drawText('T T Pappas Enterprises LLC', { x: margin, y, size: 9, font: helvetica, color: black });
    y -= 11;
    page.drawText('PO Box 770057', { x: margin, y, size: 9, font: helvetica, color: black });
    y -= 11;
    page.drawText('Lakewood, OH 44107', { x: margin, y, size: 9, font: helvetica, color: black });
    y -= 11;
    page.drawText('(440) 886-7318', { x: margin, y, size: 9, font: helvetica, color: black });
    y -= 11;
    page.drawText('hello@pappaslandscaping.com', { x: margin, y, size: 9, font: helvetica, color: black });
    
    // Client (right column)
    let clientY = pageHeight - margin - 58;
    page.drawText('CLIENT', { x: margin + colWidth + 20, y: clientY, size: 8, font: helveticaBold, color: gray });
    clientY -= 14;
    page.drawText(quote.customer_name || '', { x: margin + colWidth + 20, y: clientY, size: 10, font: helveticaBold, color: black });
    clientY -= 12;
    page.drawText(quote.customer_address || '', { x: margin + colWidth + 20, y: clientY, size: 9, font: helvetica, color: black });
    clientY -= 11;
    page.drawText(quote.customer_email || '', { x: margin + colWidth + 20, y: clientY, size: 9, font: helvetica, color: black });
    clientY -= 11;
    page.drawText(quote.customer_phone || '', { x: margin + colWidth + 20, y: clientY, size: 9, font: helvetica, color: black });
    
    y -= 40;
    
    // ===== SERVICES & PRICING - Two Column Table =====
    // Dark green header bar
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: darkGreen });
    page.drawText('Services & Pricing', { x: margin + 15, y: y + 2, size: 12, font: qualyFont, color: rgb(1, 1, 1) });
    y -= 35;

    // Conditional layout: two columns for 6+ services, single column for fewer
    const useSvcTwoColumns = services.length >= 6;
    const svcColWidth = (contentWidth - 2) / 2;
    const svcRowHeight = 22;

    if (useSvcTwoColumns) {
      // Two-column table header
      page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 22, color: rgb(0.95, 0.95, 0.95) });
      page.drawText('Service', { x: margin + 10, y: y, size: 9, font: helveticaBold, color: gray });
      page.drawText('Amount', { x: margin + svcColWidth - 50, y: y, size: 9, font: helveticaBold, color: gray });
      page.drawText('Service', { x: margin + svcColWidth + 10, y: y, size: 9, font: helveticaBold, color: gray });
      page.drawText('Amount', { x: margin + contentWidth - 50, y: y, size: 9, font: helveticaBold, color: gray });
      page.drawLine({ start: { x: margin + svcColWidth, y: y + 5 }, end: { x: margin + svcColWidth, y: y - 17 }, thickness: 1, color: rgb(0.85, 0.85, 0.85) });
      y -= 24;

      for (let i = 0; i < services.length; i += 2) {
        const bgColor = (i / 2) % 2 === 0 ? rgb(1, 1, 1) : rgb(0.98, 0.98, 0.98);
        page.drawRectangle({ x: margin, y: y - svcRowHeight + 15, width: contentWidth, height: svcRowHeight, color: bgColor });

        const svc1 = services[i];
        page.drawText(svc1.name, { x: margin + 10, y: y, size: 9, font: helvetica, color: black });
        page.drawText(`$${parseFloat(svc1.amount).toFixed(2)}`, { x: margin + svcColWidth - 50, y: y, size: 9, font: helveticaBold, color: black });

        if (i + 1 < services.length) {
          const svc2 = services[i + 1];
          page.drawText(svc2.name, { x: margin + svcColWidth + 10, y: y, size: 9, font: helvetica, color: black });
          page.drawText(`$${parseFloat(svc2.amount).toFixed(2)}`, { x: margin + contentWidth - 50, y: y, size: 9, font: helveticaBold, color: black });
        }

        page.drawLine({ start: { x: margin + svcColWidth, y: y + 7 }, end: { x: margin + svcColWidth, y: y - svcRowHeight + 15 }, thickness: 1, color: rgb(0.9, 0.9, 0.9) });
        y -= svcRowHeight;
      }
    } else {
      // Single-column table header
      page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 22, color: rgb(0.95, 0.95, 0.95) });
      page.drawText('Service', { x: margin + 10, y: y, size: 9, font: helveticaBold, color: gray });
      page.drawText('Amount', { x: pageWidth - margin - 60, y: y, size: 9, font: helveticaBold, color: gray });
      y -= 24;

      for (let i = 0; i < services.length; i++) {
        const bgColor = i % 2 === 0 ? rgb(1, 1, 1) : rgb(0.98, 0.98, 0.98);
        page.drawRectangle({ x: margin, y: y - svcRowHeight + 15, width: contentWidth, height: svcRowHeight, color: bgColor });

        const svc = services[i];
        page.drawText(svc.name, { x: margin + 10, y: y, size: 9, font: helvetica, color: black });
        page.drawText(`$${parseFloat(svc.amount).toFixed(2)}`, { x: pageWidth - margin - 60, y: y, size: 9, font: helveticaBold, color: black });
        y -= svcRowHeight;
      }
    }

    y -= 10;

    // Total bar
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: rgb(0.98, 0.98, 0.98), borderColor: limeGreen, borderWidth: 1 });
    page.drawText(`Total: $${parseFloat(quote.total).toFixed(2)}${quote.monthly_payment ? ` | Monthly: $${parseFloat(quote.monthly_payment).toFixed(2)}/mo` : ''}`, { x: margin + 15, y: y + 2, size: 11, font: helveticaBold, color: darkGreen });

    y -= 40;
    
    // Contract sections
    const sections = [
      { title: 'I. Scope of Agreement', content: `A. Associated Quote: This Agreement is directly tied to Quote/Proposal Number: ${quoteNumber}.\n\nB. Scope of Services: The Contractor agrees to provide services at the Client Service Address as detailed in the Proposal, which outlines the specific services, schedule, and pricing.\n\nC. Additional Work: Additional work requested by the Client outside of the scope defined in the Proposal will be performed at an additional cost, requiring a separate, pre-approved quote.` },
      { title: 'II. Terms and Renewal', content: 'A. Term: This Agreement begins on the Effective Date and remains in effect until canceled.\n\nB. Automatic Renewal: The Agreement automatically renews each year at the start of the new season (March), unless canceled in writing by either party at least 30 days before.' },
      { title: 'III. Payment Terms', content: 'A. Mowing Services: Per-Service invoices sent at month end; Monthly Contracts invoiced on the 1st.\nB. All Other Services: Invoiced upon completion.\nC. Due Date: Upon receipt.\nD. Accepted Methods: Credit cards, Zelle, cash, checks, money orders, bank transfers.\nE. Fuel Surcharge: A small flat-rate fee added to each invoice.\nF. Returned Checks: $25 fee.' },
      { title: 'IV. Card on File Authorization', content: 'By placing a card on file, the Client authorizes Pappas & Co. Landscaping to charge that card for services rendered. Processing Fee: 2.9% + $0.30 per transaction.' },
      { title: 'V. Late Fees', content: '30-Day Late Fee: 10% if not paid within 30 days.\nRecurring Late Fee: Additional 5% per 30-day period.\nService Suspension: After 60 days, services suspended and collections may begin.' },
      { title: 'VI. Client Responsibilities', content: 'Accessibility: Gates unlocked, areas accessible. Return Trip Fee: $25 if rescheduling needed due to access issues. Property Clearance: Free of hazards. Pet Waste: $15 cleanup fee if present.' },
      { title: 'VII. Lawn/Plant Installs', content: 'Client responsible for watering newly installed lawns and plants. Pappas & Co. not responsible for failure due to lack of watering.' },
      { title: 'VIII. Weather and Materials', content: 'A. Materials: Pappas & Co. supplies all necessary materials unless specified otherwise.\nB. Weather: Reasonable efforts to reschedule. No refunds for weather delays.' },
      { title: 'IX. Cancellation', content: 'A. Non-Renewal: 30 days written notice before March renewal.\nB. Mid-Season: 15 days written notice. No refunds for prepaid services.\nC. Termination by Contractor: 15 days notice.' },
      { title: 'X. Liability & Insurance', content: 'A. Quality: Services performed with due care. Notify within 7 days of defects.\nB. Independent Contractor: Not an employee or agent of Client.\nC. Limitation: Liability shall not exceed total amount paid.\nD. Insurance: General liability, auto, and workers comp insurance carried.' },
      { title: 'XI. Governing Law', content: 'A. Jurisdiction: State of Ohio, Cuyahoga County courts.\nB. Dispute Resolution: Good-faith negotiations first, then mediation/arbitration.' },
      { title: 'XII. Acceptance', content: 'By signing below, the parties acknowledge they have read, understand, and agree to all terms of this Agreement.' }
    ];
    
    for (const section of sections) {
      // Check if we need a new page
      if (y < 120) {
        page = addPage();
        y = pageHeight - margin;
      }
      
      // Section title with lime underline
      page.drawText(section.title, { x: margin, y, size: 11, font: helveticaBold, color: darkGreen });
      y -= 3;
      page.drawRectangle({ x: margin, y, width: contentWidth, height: 2, color: limeGreen });
      y -= 14;
      
      // Section content
      const lines = section.content.split('\n');
      for (const line of lines) {
        if (line.trim()) {
          y = drawWrappedText(page, line, margin, y, contentWidth, helvetica, 9, black);
          y -= 4;
        } else {
          y -= 6;
        }
        
        if (y < 80) {
          page = addPage();
          y = pageHeight - margin;
        }
      }
      
      y -= 10;
    }
    
    // Signature section
    if (y < 200) {
      page = addPage();
      y = pageHeight - margin;
    }
    
    // Signature box
    page.drawRectangle({ x: margin, y: y - 150, width: contentWidth, height: 155, color: rgb(0.98, 0.98, 0.98), borderColor: darkGreen, borderWidth: 2 });
    y -= 10;
    
    page.drawText('[X] Agreement Accepted & Digitally Signed', { x: margin + 15, y, size: 12, font: helveticaBold, color: darkGreen });
    y -= 25;
    
    // Client signature
    page.drawText('CLIENT SIGNATURE:', { x: margin + 15, y, size: 8, font: helveticaBold, color: gray });
    y -= 20;
    
    // If signature is an image, try to embed it
    if (signatureData && signatureData.startsWith('data:image')) {
      try {
        const base64Data = signatureData.split(',')[1];
        const signatureBytes = Buffer.from(base64Data, 'base64');
        let signatureImage;
        
        // Check if it's PNG or JPEG
        if (signatureData.includes('image/png')) {
          signatureImage = await pdfDoc.embedPng(signatureBytes);
        } else if (signatureData.includes('image/jpeg') || signatureData.includes('image/jpg')) {
          signatureImage = await pdfDoc.embedJpg(signatureBytes);
        } else {
          // Try PNG first, fall back to JPG
          try {
            signatureImage = await pdfDoc.embedPng(signatureBytes);
          } catch (e) {
            signatureImage = await pdfDoc.embedJpg(signatureBytes);
          }
        }
        
        page.drawImage(signatureImage, { x: margin + 15, y: y - 25, width: 120, height: 35 });
        y -= 40;
      } catch (e) {
        console.log('Error embedding signature image, falling back to text:', e.message);
        // Fall back to text
        page.drawText(signedBy || '', { x: margin + 15, y, size: 14, font: helvetica, color: black });
        y -= 20;
      }
    } else {
      // Typed signature
      page.drawText(signatureData || signedBy || '', { x: margin + 15, y, size: 14, font: helvetica, color: black });
      y -= 20;
    }
    
    page.drawRectangle({ x: margin + 15, y: y + 5, width: 200, height: 1, color: black });
    y -= 15;
    page.drawText(`Name: ${signedBy || ''}`, { x: margin + 15, y, size: 9, font: helvetica, color: black });
    y -= 12;
    page.drawText(`Date: ${signedDate || new Date().toLocaleDateString()}`, { x: margin + 15, y, size: 9, font: helvetica, color: black });
    y -= 20;
    
    // Signature verification
    page.drawRectangle({ x: margin + 15, y: y - 25, width: contentWidth - 30, height: 1, color: rgb(0.8, 0.8, 0.8) });
    y -= 35;
    const signerIp = quote.contract_signer_ip || 'Recorded';
    const signatureType = quote.contract_signature_type === 'draw' ? 'Hand-drawn' : 'Typed';
    const signedTimestamp = quote.contract_signed_at ? new Date(quote.contract_signed_at).toLocaleString() : signedDate;
    page.drawText(`${signatureType} signature | IP: ${signerIp} | ${signedTimestamp}`, { x: margin + 15, y, size: 7, font: helvetica, color: gray });
    
    // Footer
    y = 40;
    page.drawRectangle({ x: margin, y: y + 10, width: contentWidth, height: 3, color: limeGreen });
    page.drawText('Pappas & Co. Landscaping | T T Pappas Enterprises LLC | PO Box 770057, Lakewood, OH 44107', { x: margin, y: y - 5, size: 8, font: helvetica, color: gray });
    page.drawText('(440) 886-7318 | hello@pappaslandscaping.com | pappaslandscaping.com', { x: margin, y: y - 15, size: 8, font: helvetica, color: gray });
    
    // Save the PDF
    console.log('Saving PDF...');
    const pdfBytes = await pdfDoc.save();
    console.log('Contract PDF generated successfully, size:', pdfBytes.length, 'bytes');
    return pdfBytes;
    
  } catch (error) {
    console.error('Error generating PDF:', error.message);
    console.error('Stack trace:', error.stack);
    return null;
  }
}

// Generate Quote PDF - Branded style with dark green and lime accents
async function generateQuotePDF(quote) {
  try {
    console.log('Starting Quote PDF generation...');
    const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
    const fontkit = require('@pdf-lib/fontkit');
    const fs = require('fs');
    const path = require('path');
    console.log('pdf-lib loaded for quote PDF');

    const pdfDoc = await PDFDocument.create();
    pdfDoc.registerFontkit(fontkit);
    const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);
    const helveticaBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

    // Embed Qualy font for headers
    let qualyFont = helveticaBold; // fallback
    try {
      const qualyPath = path.join(__dirname, 'public', 'Qualy.otf');
      if (fs.existsSync(qualyPath)) {
        const qualyBytes = fs.readFileSync(qualyPath);
        qualyFont = await pdfDoc.embedFont(qualyBytes);
        console.log('Qualy font embedded successfully');
      }
    } catch (fontErr) {
      console.log('Could not embed Qualy font:', fontErr.message);
    }

    // Try to embed logo
    let logoImage = null;
    try {
      const logoPath = path.join(__dirname, 'public', 'logo.png');
      if (fs.existsSync(logoPath)) {
        const logoBytes = fs.readFileSync(logoPath);
        logoImage = await pdfDoc.embedPng(logoBytes);
      }
    } catch (logoErr) {
      console.log('Could not embed logo:', logoErr.message);
    }

    const pageWidth = 612;
    const pageHeight = 792;
    const margin = 50;
    const contentWidth = pageWidth - (margin * 2);

    // Brand colors
    const darkGreen = rgb(0.18, 0.25, 0.24); // #2e403d
    const limeGreen = rgb(0.79, 0.87, 0.50); // #c9dd80
    const black = rgb(0, 0, 0);
    const gray = rgb(0.4, 0.45, 0.45);
    const midGray = rgb(0.55, 0.58, 0.58);
    const lightGray = rgb(0.97, 0.98, 0.96);

    let services = [];
    try {
      services = typeof quote.services === 'string' ? JSON.parse(quote.services) : quote.services;
    } catch (e) {
      services = [];
    }

    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    const quoteDate = new Date(quote.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });

    // Helper: word-wrap text and return final Y position
    function wrapText(page, text, x, y, maxWidth, font, size, color, lineHeight = 1.4) {
      const words = text.split(' ');
      let line = '';
      let curY = y;
      for (const word of words) {
        const test = line + (line ? ' ' : '') + word;
        if (font.widthOfTextAtSize(test, size) > maxWidth && line) {
          page.drawText(line, { x, y: curY, size, font, color });
          line = word;
          curY -= size * lineHeight;
        } else {
          line = test;
        }
      }
      if (line) { page.drawText(line, { x, y: curY, size, font, color }); curY -= size * lineHeight; }
      return curY;
    }

    // Helper: estimate wrapped text height
    function wrapHeight(text, maxWidth, font, size, lineHeight = 1.4) {
      const words = text.split(' ');
      let line = '';
      let lines = 0;
      for (const word of words) {
        const test = line + (line ? ' ' : '') + word;
        if (font.widthOfTextAtSize(test, size) > maxWidth && line) {
          lines++;
          line = word;
        } else {
          line = test;
        }
      }
      if (line) lines++;
      return lines * size * lineHeight;
    }

    // Helper: add a new continuation page
    function addContinuationPage() {
      const newPage = pdfDoc.addPage([pageWidth, pageHeight]);
      let py = pageHeight - margin;
      if (logoImage) {
        const logoDims = logoImage.scale(0.18);
        newPage.drawImage(logoImage, { x: margin, y: py - logoDims.height, width: logoDims.width, height: logoDims.height });
        newPage.drawText(`Quote #${quoteNumber} (continued)`, { x: margin + logoDims.width + 12, y: py - 14, size: 10, font: qualyFont, color: darkGreen });
        // Right-aligned contact info
        newPage.drawText('pappaslandscaping.com', { x: pageWidth - margin - 130, y: py, size: 8, font: helvetica, color: gray });
        newPage.drawText('(440) 886-7318', { x: pageWidth - margin - 130, y: py - 11, size: 8, font: helvetica, color: gray });
        py -= logoDims.height + 8;
      } else {
        newPage.drawText(`Quote #${quoteNumber} (continued)`, { x: margin, y: py - 10, size: 10, font: qualyFont, color: darkGreen });
        py -= 30;
      }
      newPage.drawRectangle({ x: margin, y: py, width: contentWidth, height: 3, color: limeGreen });
      py -= 20;
      return { page: newPage, y: py };
    }

    const page = pdfDoc.addPage([pageWidth, pageHeight]);
    let y = pageHeight - margin;

    // ===== HEADER =====
    if (logoImage) {
      const logoDims = logoImage.scale(0.28);
      page.drawImage(logoImage, { x: margin, y: y - logoDims.height, width: logoDims.width, height: logoDims.height });
      // Contact info to the right of logo
      const cx = pageWidth - margin - 145;
      page.drawText('pappaslandscaping.com', { x: cx, y, size: 9, font: helvetica, color: gray });
      page.drawText('hello@pappaslandscaping.com', { x: cx, y: y - 13, size: 9, font: helvetica, color: gray });
      page.drawText('(440) 886-7318', { x: cx, y: y - 26, size: 9, font: helvetica, color: gray });
      y -= logoDims.height + 8;
    } else {
      page.drawText('Pappas & Co. Landscaping', { x: margin, y, size: 20, font: qualyFont, color: darkGreen });
      page.drawText('pappaslandscaping.com', { x: pageWidth - margin - 145, y, size: 9, font: helvetica, color: gray });
      page.drawText('hello@pappaslandscaping.com', { x: pageWidth - margin - 145, y: y - 13, size: 9, font: helvetica, color: gray });
      page.drawText('(440) 886-7318', { x: pageWidth - margin - 145, y: y - 26, size: 9, font: helvetica, color: gray });
      y -= 28;
    }

    // Lime green accent line
    page.drawRectangle({ x: margin, y, width: contentWidth, height: 4, color: limeGreen });
    y -= 30;

    // ===== QUOTE BADGE =====
    page.drawRectangle({ x: margin, y: y - 8, width: 140, height: 26, color: darkGreen });
    page.drawText(`QUOTE  #${quoteNumber}`, { x: margin + 12, y: y - 1, size: 11, font: helveticaBold, color: limeGreen });
    y -= 46;

    // ===== PREPARED FOR / QUOTE DETAILS =====
    const infoBoxH = 95;
    page.drawRectangle({ x: margin, y: y - infoBoxH, width: 250, height: infoBoxH, color: lightGray, borderColor: limeGreen, borderWidth: 2 });
    page.drawText('PREPARED FOR', { x: margin + 14, y: y - 10, size: 8, font: helveticaBold, color: midGray });
    page.drawText(quote.customer_name || '', { x: margin + 14, y: y - 26, size: 13, font: helveticaBold, color: darkGreen });
    let infoY = y - 42;
    if (quote.customer_address) {
      page.drawText(quote.customer_address, { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
      infoY -= 14;
    }
    if (quote.customer_email) {
      page.drawText(quote.customer_email, { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
      infoY -= 14;
    }
    if (quote.customer_phone) {
      page.drawText(quote.customer_phone, { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
    }

    // Right side - Quote Details
    const dx = margin + 275;
    page.drawText('QUOTE DETAILS', { x: dx, y: y - 10, size: 8, font: helveticaBold, color: midGray });
    page.drawText(`Date:`, { x: dx, y: y - 26, size: 9, font: helveticaBold, color: gray });
    page.drawText(quoteDate, { x: dx + 30, y: y - 26, size: 9, font: helvetica, color: black });
    page.drawText(`Valid For:`, { x: dx, y: y - 40, size: 9, font: helveticaBold, color: gray });
    page.drawText('30 Days', { x: dx + 48, y: y - 40, size: 9, font: helvetica, color: black });
    page.drawText(`Quote #:`, { x: dx, y: y - 54, size: 9, font: helveticaBold, color: gray });
    page.drawText(String(quoteNumber), { x: dx + 44, y: y - 54, size: 9, font: helvetica, color: black });
    page.drawText(`Type:`, { x: dx, y: y - 68, size: 9, font: helveticaBold, color: gray });
    page.drawText(quote.quote_type === 'monthly_plan' ? 'Annual Care Plan' : 'Standard Quote', { x: dx + 28, y: y - 68, size: 9, font: helvetica, color: black });

    y -= infoBoxH + 18;

    // ===== SERVICES SECTION HEADER =====
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: darkGreen });
    page.drawText('Services Included', { x: margin + 14, y: y + 2, size: 12, font: qualyFont, color: rgb(1, 1, 1) });
    y -= 33;

    // Table column header
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 20, color: rgb(0.93, 0.94, 0.93) });
    page.drawText('SERVICE / DESCRIPTION', { x: margin + 10, y: y - 1, size: 8, font: helveticaBold, color: gray });
    page.drawText('AMOUNT', { x: pageWidth - margin - 55, y: y - 1, size: 8, font: helveticaBold, color: gray });
    y -= 22;

    // ===== SERVICE ROWS (single column with descriptions) =====
    for (let i = 0; i < services.length; i++) {
      const svc = services[i];
      const desc = svc.description || '';
      const descLineHeight = 1.35;
      const descSize = 8;
      const nameSize = 10;
      const descMaxWidth = contentWidth - 75; // leave room for amount column

      // Calculate row height
      let rowH = nameSize * 1.6 + 6; // name + padding
      if (desc) {
        const descLines = desc.split('\n');
        for (const dLine of descLines) {
          if (!dLine.trim()) { rowH += 4; continue; }
          rowH += wrapHeight(dLine, descMaxWidth, helvetica, descSize, descLineHeight);
        }
        rowH += 8; // bottom padding
      }

      // New page if needed
      if (y - rowH < 100) {
        const cont = addContinuationPage();
        page = cont.page;
        y = cont.y;
        const cp = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];
        cp.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 20, color: rgb(0.93, 0.94, 0.93) });
        cp.drawText('SERVICE / DESCRIPTION', { x: margin + 10, y: y - 1, size: 8, font: helveticaBold, color: gray });
        cp.drawText('AMOUNT', { x: pageWidth - margin - 55, y: y - 1, size: 8, font: helveticaBold, color: gray });
        y -= 22;
      }

      const cp = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];
      const bg = i % 2 === 0 ? rgb(1, 1, 1) : rgb(0.97, 0.98, 0.97);
      cp.drawRectangle({ x: margin, y: y - rowH + nameSize * 0.4, width: contentWidth, height: rowH, color: bg });

      // Service name (bold dark green)
      cp.drawText(svc.name, { x: margin + 10, y, size: nameSize, font: helveticaBold, color: darkGreen });
      // Amount (right-aligned bold)
      const amtStr = `$${parseFloat(svc.amount).toFixed(2)}`;
      cp.drawText(amtStr, { x: pageWidth - margin - 55, y, size: nameSize, font: helveticaBold, color: black });

      // Description lines
      if (desc) {
        let dy = y - nameSize * 1.5;
        const descLines = desc.split('\n');
        for (const dLine of descLines) {
          if (!dLine.trim()) { dy -= 4; continue; }
          dy = wrapText(cp, dLine, margin + 10, dy, descMaxWidth, helvetica, descSize, midGray, descLineHeight);
        }
      }

      y -= rowH;
    }

    y -= 10;

    // ===== TOTALS BOX =====
    let cp = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];
    if (y < 180) {
      const cont = addContinuationPage();
      cp = cont.page;
      y = cont.y;
    }

    cp.drawRectangle({ x: margin, y: y - 100, width: contentWidth, height: 105, color: lightGray, borderColor: limeGreen, borderWidth: 2 });
    cp.drawText('Subtotal', { x: margin + 15, y: y - 16, size: 10, font: helvetica, color: gray });
    cp.drawText(`$${parseFloat(quote.subtotal).toFixed(2)}`, { x: pageWidth - margin - 80, y: y - 16, size: 10, font: helvetica, color: black });
    cp.drawText(`Tax (${quote.tax_rate || 8}%)`, { x: margin + 15, y: y - 33, size: 10, font: helvetica, color: gray });
    cp.drawText(`$${parseFloat(quote.tax_amount).toFixed(2)}`, { x: pageWidth - margin - 80, y: y - 33, size: 10, font: helvetica, color: black });
    cp.drawRectangle({ x: margin + 15, y: y - 48, width: contentWidth - 30, height: 2, color: limeGreen });
    cp.drawText('TOTAL', { x: margin + 15, y: y - 70, size: 14, font: helveticaBold, color: darkGreen });
    cp.drawText(`$${parseFloat(quote.total).toFixed(2)}`, { x: pageWidth - margin - 95, y: y - 70, size: 18, font: helveticaBold, color: darkGreen });
    y -= 115;

    // Monthly payment banner
    if (quote.monthly_payment) {
      cp.drawRectangle({ x: margin, y: y - 6, width: contentWidth, height: 32, color: darkGreen });
      cp.drawText('Monthly Payment Plan', { x: margin + 14, y: y + 3, size: 11, font: helveticaBold, color: rgb(1, 1, 1) });
      cp.drawText(`$${parseFloat(quote.monthly_payment).toFixed(2)}/mo`, { x: pageWidth - margin - 100, y: y + 3, size: 14, font: helveticaBold, color: limeGreen });
      y -= 46;
    }

    y -= 10;

    // ===== NEXT STEPS =====
    cp.drawRectangle({ x: margin, y: y - 48, width: contentWidth, height: 52, color: rgb(0.97, 0.99, 0.97), borderColor: limeGreen, borderWidth: 1 });
    cp.drawText('How to Accept This Quote', { x: margin + 14, y: y - 10, size: 10, font: helveticaBold, color: darkGreen });
    cp.drawText('Review your quote email and click "View Your Quote" to accept online and sign your service agreement.', { x: margin + 14, y: y - 25, size: 8, font: helvetica, color: gray });
    cp.drawText('Questions? Call or text (440) 886-7318 — we\'re happy to help!', { x: margin + 14, y: y - 38, size: 8, font: helvetica, color: gray });
    y -= 65;

    // ===== FOOTER =====
    cp.drawRectangle({ x: margin, y: y + 5, width: contentWidth, height: 3, color: limeGreen });
    y -= 14;
    cp.drawText('Pappas & Co. Landscaping  |  PO Box 770057, Lakewood, OH 44107  |  (440) 886-7318  |  hello@pappaslandscaping.com', { x: margin, y, size: 8, font: helvetica, color: gray });
    cp.drawText(`This quote is valid for 30 days from ${quoteDate}. Prices subject to change after expiration.`, { x: margin, y: y - 12, size: 7.5, font: helvetica, color: rgb(0.65, 0.67, 0.67) });

    console.log('Saving Quote PDF...');
    const pdfBytes = await pdfDoc.save();
    console.log('Quote PDF generated successfully, size:', pdfBytes.length, 'bytes');
    return pdfBytes;

  } catch (error) {
    console.error('Error generating quote PDF:', error.message);
    console.error('Stack trace:', error.stack);
    return null;
  }
}

// reCAPTCHA verification
const RECAPTCHA_SECRET_KEY = process.env.RECAPTCHA_SECRET_KEY;

async function verifyRecaptcha(token) {
  if (!RECAPTCHA_SECRET_KEY) {
    console.log('reCAPTCHA secret key not configured, skipping verification');
    return { success: true, score: 1.0 };
  }
  try {
    const response = await fetch('https://www.google.com/recaptcha/api/siteverify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `secret=${RECAPTCHA_SECRET_KEY}&response=${token}`
    });
    const data = await response.json();
    console.log('reCAPTCHA verification result:', data);
    return data;
  } catch (err) {
    console.error('reCAPTCHA verification failed:', err);
    return { success: false, score: 0 };
  }
}

// Helper to parse CSV
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];
    if (char === '"' && !inQuotes) { inQuotes = true; }
    else if (char === '"' && inQuotes) {
      if (nextChar === '"') { current += '"'; i++; }
      else { inQuotes = false; }
    } else if (char === ',' && !inQuotes) { result.push(current); current = ''; }
    else { current += char; }
  }
  result.push(current);
  return result;
}

// ═══════════════════════════════════════════════════════════
// PROPERTIES ENDPOINTS - Using YOUR existing schema:
// id, property_name, country, state, street, street2, city, zip, tags, status, lot_size, notes, customer_id
// ═══════════════════════════════════════════════════════════

// GET /api/properties
app.get('/api/properties', async (req, res) => {
  try {
    const { status, city, search, sort, limit = 1000, offset = 0 } = req.query;
    
    let query = `
      SELECT p.*, c.name as customer_display_name, c.email as customer_email, c.phone as customer_phone
      FROM properties p
      LEFT JOIN customers c ON p.customer_id = c.id
      WHERE 1=1
    `;
    let countQuery = 'SELECT COUNT(*) FROM properties WHERE 1=1';
    const params = [];
    const countParams = [];
    let paramCount = 1;
    let countParamCount = 1;
    
    if (status) {
      query += ` AND LOWER(p.status) = LOWER($${paramCount})`;
      countQuery += ` AND LOWER(status) = LOWER($${countParamCount})`;
      params.push(status);
      countParams.push(status);
      paramCount++;
      countParamCount++;
    }
    
    if (city) {
      query += ` AND p.city ILIKE $${paramCount}`;
      countQuery += ` AND city ILIKE $${countParamCount}`;
      params.push(`%${city}%`);
      countParams.push(`%${city}%`);
      paramCount++;
      countParamCount++;
    }
    
    if (search) {
      query += ` AND (p.street ILIKE $${paramCount} OR p.property_name ILIKE $${paramCount} OR p.city ILIKE $${paramCount} OR c.name ILIKE $${paramCount})`;
      countQuery += ` AND (street ILIKE $${countParamCount} OR property_name ILIKE $${countParamCount} OR city ILIKE $${countParamCount})`;
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);
      paramCount++;
      countParamCount++;
    }
    
    let orderBy = 'p.street ASC';
    switch (sort) {
      case 'address_asc': orderBy = 'p.street ASC'; break;
      case 'address_desc': orderBy = 'p.street DESC'; break;
      case 'customer_asc': orderBy = 'c.name ASC NULLS LAST'; break;
      case 'customer_desc': orderBy = 'c.name DESC NULLS LAST'; break;
      case 'city_asc': orderBy = 'p.city ASC'; break;
      case 'city_desc': orderBy = 'p.city DESC'; break;
      case 'newest': orderBy = 'p.id DESC'; break;
      case 'oldest': orderBy = 'p.id ASC'; break;
    }
    
    query += ` ORDER BY ${orderBy} LIMIT $${paramCount} OFFSET $${paramCount + 1}`;
    params.push(limit, offset);
    
    const result = await pool.query(query, params);
    const countResult = await pool.query(countQuery, countParams);
    
    res.json({
      success: true,
      properties: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });
  } catch (error) {
    console.error('Error fetching properties:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/properties/stats
app.get('/api/properties/stats', async (req, res) => {
  try {
    const totalResult = await pool.query('SELECT COUNT(*) FROM properties');
    const activeResult = await pool.query("SELECT COUNT(*) FROM properties WHERE LOWER(status) = 'active'");
    
    const citiesResult = await pool.query(`
      SELECT city, COUNT(*) as count 
      FROM properties 
      WHERE city IS NOT NULL AND city != '' 
      GROUP BY city 
      ORDER BY count DESC 
      LIMIT 20
    `);
    
    // Count properties with lot_size > 0 as "priced"
    const pricedResult = await pool.query(`
      SELECT COUNT(*) FROM properties 
      WHERE lot_size IS NOT NULL AND lot_size != '' AND lot_size != '0'
    `);
    
    res.json({
      success: true,
      stats: {
        total: parseInt(totalResult.rows[0].count),
        active: parseInt(activeResult.rows[0].count),
        inactive: parseInt(totalResult.rows[0].count) - parseInt(activeResult.rows[0].count),
        topCities: citiesResult.rows,
        citiesServed: citiesResult.rows.length,
        withPricing: parseInt(pricedResult.rows[0].count),
        revenue: {
          pricedProperties: parseInt(pricedResult.rows[0].count)
        }
      }
    });
  } catch (error) {
    console.error('Error fetching property stats:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/properties/:id
app.get('/api/properties/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT p.*, c.name as customer_display_name, c.email as customer_email, c.phone as customer_phone
      FROM properties p
      LEFT JOIN customers c ON p.customer_id = c.id
      WHERE p.id = $1
    `, [req.params.id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error fetching property:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/properties
app.post('/api/properties', async (req, res) => {
  try {
    const { property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id } = req.body;
    
    if (!street) {
      return res.status(400).json({ success: false, error: 'Street address is required' });
    }
    
    const result = await pool.query(`
      INSERT INTO properties (property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      RETURNING *
    `, [
      property_name || street, street, street2 || '', city || '', state || 'OH', country || 'US',
      zip || '', lot_size || '', tags || '', status || 'Active', notes || '', customer_id || null
    ]);
    
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error creating property:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PUT /api/properties/:id
app.put('/api/properties/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id,
            customer_name, address, postal_code, lawn_sqft, property_notes, mowing_price } = req.body;
    
    // Map new field names to your existing columns
    const actualStreet = street || address || '';
    const actualZip = zip || postal_code || '';
    const actualLotSize = lot_size || (lawn_sqft ? String(lawn_sqft) : '');
    const actualNotes = notes || property_notes || '';
    const actualPropertyName = property_name || customer_name || actualStreet;
    
    const result = await pool.query(`
      UPDATE properties SET
        property_name = $1, street = $2, street2 = $3, city = $4, state = $5,
        country = $6, zip = $7, lot_size = $8, tags = $9, status = $10, notes = $11, customer_id = $12
      WHERE id = $13
      RETURNING *
    `, [
      actualPropertyName, actualStreet, street2 || '', city || '', state || 'OH',
      country || 'US', actualZip, actualLotSize, tags || '', status || 'Active', actualNotes,
      customer_id || null, id
    ]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error updating property:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PATCH /api/properties/:id
app.patch('/api/properties/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;
    
    // Map field names
    const fieldMap = {
      'address': 'street', 'postal_code': 'zip', 'lawn_sqft': 'lot_size',
      'property_notes': 'notes', 'customer_name': 'property_name'
    };
    
    const allowedFields = ['property_name', 'street', 'street2', 'city', 'state', 'country', 'zip', 'lot_size', 'tags', 'status', 'notes', 'customer_id'];
    
    const setClause = [];
    const values = [];
    let paramCount = 1;
    
    Object.keys(updates).forEach(key => {
      const dbField = fieldMap[key] || key;
      if (allowedFields.includes(dbField)) {
        let value = updates[key];
        if (dbField === 'lot_size' && typeof value === 'number') value = String(value);
        setClause.push(`${dbField} = $${paramCount}`);
        values.push(value);
        paramCount++;
      }
    });
    
    if (setClause.length === 0) {
      return res.status(400).json({ success: false, error: 'No valid fields to update' });
    }
    
    values.push(id);
    const query = `UPDATE properties SET ${setClause.join(', ')} WHERE id = $${paramCount} RETURNING *`;
    const result = await pool.query(query, values);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error updating property:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// DELETE /api/properties/:id
app.delete('/api/properties/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM properties WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting property:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/import-properties - Using YOUR column names
app.post('/api/import-properties', upload.single('csvfile'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'No CSV file uploaded' });
    }
    
    const csvContent = req.file.buffer.toString('utf-8');
    const lines = csvContent.split('\n');
    const rawHeaders = parseCSVLine(lines[0]);
    const headers = rawHeaders.map(h => h.trim().toLowerCase().replace(/\s+/g, '_'));
    
    console.log('📋 CSV Headers:', headers);
    
    const properties = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      try {
        const values = parseCSVLine(line);
        if (values.length >= headers.length - 2) {
          const property = {};
          headers.forEach((h, idx) => {
            property[h] = (values[idx] || '').trim().replace(/^\t+/, '');
          });
          properties.push(property);
        }
      } catch (e) { console.log(`Skip line ${i + 1}`); }
    }
    
    console.log(`📊 Found ${properties.length} properties`);
    
    let imported = 0, updated = 0, skipped = 0;
    const errors = [];
    
    for (const prop of properties) {
      try {
        const street = prop['street'] || '';
        if (!street || street.toLowerCase() === 'primary') { skipped++; continue; }
        
        const propertyName = prop['property_name'] || street;
        const city = prop['city'] || '';
        const state = prop['state'] || 'OH';
        const country = prop['country'] || 'US';
        const zip = prop['zip'] || '';
        const street2 = prop['street2'] || '';
        const lotSize = prop['lot_size'] || '0';
        const status = prop['status'] || 'Active';
        const tags = prop['tags'] || '';
        const notes = prop['notes'] || '';
        
        const existing = await pool.query('SELECT id FROM properties WHERE street ILIKE $1', [street]);
        
        if (existing.rows.length > 0) {
          await pool.query(`
            UPDATE properties SET
              property_name = COALESCE(NULLIF($1, ''), property_name),
              city = COALESCE(NULLIF($2, ''), city),
              state = COALESCE(NULLIF($3, ''), state),
              country = COALESCE(NULLIF($4, ''), country),
              zip = COALESCE(NULLIF($5, ''), zip),
              street2 = COALESCE(NULLIF($6, ''), street2),
              lot_size = COALESCE(NULLIF($7, ''), lot_size),
              tags = COALESCE(NULLIF($8, ''), tags),
              status = COALESCE(NULLIF($9, ''), status),
              notes = COALESCE(NULLIF($10, ''), notes)
            WHERE id = $11
          `, [propertyName, city, state, country, zip, street2, lotSize, tags, status, notes, existing.rows[0].id]);
          updated++;
        } else {
          await pool.query(`
            INSERT INTO properties (property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `, [propertyName, street, street2, city, state, country, zip, lotSize, tags, status, notes]);
          imported++;
        }
      } catch (error) {
        console.error('Import error:', error.message);
        errors.push({ address: prop['street'], error: error.message });
        skipped++;
      }
    }
    
    console.log(`✅ Import: ${imported}, Updated: ${updated}, Skipped: ${skipped}`);
    
    res.json({
      success: true,
      message: 'Properties import completed',
      stats: { total: properties.length, imported, updated, skipped, errors: errors.slice(0, 10) }
    });
  } catch (error) {
    console.error('Import failed:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// QUOTES ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.post('/api/quotes', async (req, res) => {
  try {
    const { name, firstName, lastName, email, phone, address, package: pkg, services, questions, notes, source, recaptchaToken } = req.body;
    
    // Verify reCAPTCHA if token provided
    if (recaptchaToken) {
      const recaptchaResult = await verifyRecaptcha(recaptchaToken);
      if (!recaptchaResult.success || recaptchaResult.score < 0.5) {
        console.log('reCAPTCHA failed - likely bot. Score:', recaptchaResult.score);
        return res.status(403).json({ success: false, error: 'Spam detection triggered. Please try again.' });
      }
      console.log('reCAPTCHA passed. Score:', recaptchaResult.score);
    } else if (RECAPTCHA_SECRET_KEY) {
      // If reCAPTCHA is configured but no token provided, reject
      console.log('No reCAPTCHA token provided');
      return res.status(400).json({ success: false, error: 'Security verification required' });
    }
    
    const fullName = name || ((firstName || '') + ' ' + (lastName || '')).trim();
    if (!fullName || !email || !phone || !address) {
      return res.status(400).json({ success: false, error: 'Missing required fields' });
    }
    let servicesArray = null;
    if (services) {
      if (Array.isArray(services)) servicesArray = services;
      else if (typeof services === 'string' && services.length > 0) servicesArray = services.split(',').map(s => s.trim());
    }
    const result = await pool.query(
      `INSERT INTO quotes (name, email, phone, address, package, services, questions, notes, source) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [fullName, email, phone, address, pkg || null, servicesArray, JSON.stringify(questions || {}), notes || null, source || null]
    );
    res.json({ success: true, quote: result.rows[0] });
    
    // Send detailed notification email
    const servicesText = servicesArray ? servicesArray.join(', ') : 'None specified';
    const dashboardUrl = 'https://pappas-quote-backend-production.up.railway.app/quote-requests.html';
    
    const emailHtml = `
      <h2>New Quote Request</h2>
      <p><strong>Name:</strong> ${fullName}</p>
      <p><strong>Email:</strong> <a href="mailto:${email}">${email}</a></p>
      <p><strong>Phone:</strong> ${phone}</p>
      <p><strong>Address:</strong> ${address}</p>
      <p><strong>Package:</strong> ${pkg || 'None'}</p>
      <p><strong>Services:</strong> ${servicesText}</p>
      <p><strong>Notes:</strong> ${notes || 'No notes provided'}</p>
      <br>
      <p><a href="${dashboardUrl}">View Dashboard</a></p>
    `;
    
    sendEmail(NOTIFICATION_EMAIL, `New Quote Request from ${fullName}`, emailHtml);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/quotes', async (req, res) => {
  try {
    const { status } = req.query;
    let query = 'SELECT * FROM quotes';
    const params = [];
    if (status) { query += ' WHERE status = $1'; params.push(status); }
    query += ' ORDER BY created_at DESC';
    const result = await pool.query(query, params);
    res.json({ success: true, quotes: result.rows });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/quotes/:id', async (req, res) => {
  try {
    const { status } = req.body;
    const result = await pool.query('UPDATE quotes SET status = $1 WHERE id = $2 RETURNING *', [status, req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.delete('/api/quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM quotes WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/stats', async (req, res) => {
  try {
    const totalResult = await pool.query('SELECT COUNT(*) FROM quotes');
    const statusResult = await pool.query('SELECT status, COUNT(*) FROM quotes GROUP BY status');
    const byStatus = {};
    statusResult.rows.forEach(row => { byStatus[row.status] = parseInt(row.count); });
    res.json({ success: true, stats: { total: parseInt(totalResult.rows[0].count), byStatus } });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ═══════════════════════════════════════════════════════════
// CANCELLATIONS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.post('/api/cancellations', async (req, res) => {
  try {
    const { customer_name, customer_email, customer_address, cancellation_reason, original_email_body } = req.body;
    const result = await pool.query(
      `INSERT INTO cancellations (customer_name, customer_email, customer_address, cancellation_reason, original_email_body) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [customer_name, customer_email, customer_address, cancellation_reason, original_email_body]
    );
    res.json({ success: true, cancellation: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/cancellations', async (req, res) => {
  try {
    const { status } = req.query;
    let query = 'SELECT * FROM cancellations';
    if (status) query += ` WHERE status = '${status}'`;
    query += ' ORDER BY created_at DESC';
    const result = await pool.query(query);
    res.json({ success: true, cancellations: result.rows });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/cancellations/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM cancellations WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, cancellation: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/cancellations/:id', async (req, res) => {
  try {
    const { status, copilot_crm_updated } = req.body;
    const updates = [], values = [];
    let p = 1;
    if (status !== undefined) { updates.push(`status = $${p++}`); values.push(status); }
    if (copilot_crm_updated !== undefined) { updates.push(`copilot_crm_updated = $${p++}`); values.push(copilot_crm_updated); }
    if (updates.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    values.push(req.params.id);
    const result = await pool.query(`UPDATE cancellations SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`, values);
    res.json({ success: true, cancellation: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.delete('/api/cancellations/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM cancellations WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ═══════════════════════════════════════════════════════════
// CUSTOMERS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/api/customers', async (req, res) => {
  try {
    const { status, city, search, sort, limit = 1000, offset = 0 } = req.query;
    let query = 'SELECT * FROM customers WHERE 1=1';
    let countQuery = 'SELECT COUNT(*) FROM customers WHERE 1=1';
    const params = [], countParams = [];
    let p = 1, cp = 1;
    
    if (status) { query += ` AND status = $${p++}`; countQuery += ` AND status = $${cp++}`; params.push(status); countParams.push(status); }
    if (city) { query += ` AND city ILIKE $${p++}`; countQuery += ` AND city ILIKE $${cp++}`; params.push(`%${city}%`); countParams.push(`%${city}%`); }
    if (search) { query += ` AND (name ILIKE $${p} OR email ILIKE $${p} OR street ILIKE $${p})`; countQuery += ` AND (name ILIKE $${cp} OR email ILIKE $${cp})`; params.push(`%${search}%`); countParams.push(`%${search}%`); p++; cp++; }
    
    let orderBy = 'name ASC';
    if (sort === 'name_desc') orderBy = 'name DESC';
    else if (sort === 'newest') orderBy = 'created_at DESC';
    else if (sort === 'city_asc') orderBy = 'city ASC';
    
    query += ` ORDER BY ${orderBy} LIMIT $${p++} OFFSET $${p}`;
    params.push(limit, offset);
    
    const result = await pool.query(query, params);
    const countResult = await pool.query(countQuery, countParams);
    res.json({ success: true, customers: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/customers/stats', async (req, res) => {
  try {
    const total = await pool.query('SELECT COUNT(*) FROM customers');
    const active = await pool.query("SELECT COUNT(*) FROM customers WHERE LOWER(status) = 'active'");
    const cities = await pool.query('SELECT city, COUNT(*) as count FROM customers WHERE city IS NOT NULL GROUP BY city ORDER BY count DESC LIMIT 10');
    // Trend: new customers last 30d vs previous 30d
    const recent = await pool.query("SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '30 days'");
    const previous = await pool.query("SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '60 days' AND created_at < NOW() - INTERVAL '30 days'");
    const recentCount = parseInt(recent.rows[0].count);
    const prevCount = parseInt(previous.rows[0].count);
    let trendPct = 0;
    if (prevCount > 0) trendPct = Math.round(((recentCount - prevCount) / prevCount) * 100);
    else if (recentCount > 0) trendPct = 100;
    const inactive = parseInt(total.rows[0].count) - parseInt(active.rows[0].count);
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), active: parseInt(active.rows[0].count), inactive, topCities: cities.rows, trend: { recent: recentCount, previous: prevCount, pct: trendPct } } });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/customers/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// POST /api/customers/deduplicate - Merge duplicate customers (same name)
app.post('/api/customers/deduplicate', async (req, res) => {
  try {
    // Find groups of customers with the same name (case-insensitive, collapse whitespace, strip emails/QB IDs)
    const dupes = await pool.query(`
      SELECT norm_name, array_agg(id ORDER BY
        CASE WHEN qb_id IS NOT NULL THEN 0 ELSE 1 END, id ASC
      ) as ids, COUNT(*) as cnt
      FROM (
        SELECT id, qb_id,
          TRIM(LOWER(
            regexp_replace(
              regexp_replace(
                regexp_replace(name, '\\s*\\([^)]*@[^)]*\\)', '', 'g'),
              '\\s+#\\d+.*$', ''),
            '\\s+', ' ', 'g')
          )) as norm_name
        FROM customers
        WHERE name IS NOT NULL AND TRIM(name) != ''
      ) sub
      WHERE norm_name != ''
      GROUP BY norm_name
      HAVING COUNT(*) > 1
    `);

    let merged = 0;
    let deleted = 0;

    for (const row of dupes.rows) {
      const ids = row.ids; // First id = keeper (has qb_id or lowest id)
      const keepId = ids[0];
      const removeIds = ids.slice(1);

      // Merge any fields the keeper is missing from the duplicates
      const keeper = await pool.query('SELECT * FROM customers WHERE id = $1', [keepId]);
      const k = keeper.rows[0];

      for (const dupId of removeIds) {
        const dup = await pool.query('SELECT * FROM customers WHERE id = $1', [dupId]);
        const d = dup.rows[0];
        if (!d) continue;

        // Fill in any blank fields on keeper from duplicate
        const updates = [];
        const vals = [];
        let p = 1;
        const fields = ['email','phone','mobile','street','street2','city','state','postal_code','qb_id','notes','customer_company_name'];
        for (const f of fields) {
          if (!k[f] && d[f]) { updates.push(`${f}=$${p++}`); vals.push(d[f]); }
        }
        // Merge tags: combine from both records, deduplicate
        const kTags = (k.tags || '').split(',').map(t => t.trim()).filter(Boolean);
        const dTags = (d.tags || '').split(',').map(t => t.trim()).filter(Boolean);
        const mergedTags = [...new Set([...kTags, ...dTags])].join(', ');
        if (mergedTags && mergedTags !== (k.tags || '')) {
          updates.push(`tags=$${p++}`);
          vals.push(mergedTags);
          k.tags = mergedTags;
        }
        if (updates.length > 0) {
          vals.push(keepId);
          await pool.query(`UPDATE customers SET ${updates.join(',')} WHERE id=$${p}`, vals);
          k[fields.find((f,i) => updates[i])] = vals[0]; // keep local state updated
        }

        // Re-point all FK references from dupId → keepId
        await pool.query('UPDATE invoices SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]);
        await pool.query('UPDATE properties SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]);
        await pool.query('UPDATE scheduled_jobs SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]);
        await pool.query('UPDATE messages SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]);

        // Delete the duplicate
        await pool.query('DELETE FROM customers WHERE id=$1', [dupId]);
        deleted++;
      }
      merged++;
    }

    // --- Deduplicate invoices by qb_invoice_id (keep lowest id) ---
    let invoicesDuped = 0;
    const dupInvoices = await pool.query(`
      SELECT qb_invoice_id, array_agg(id ORDER BY id ASC) as ids
      FROM invoices
      WHERE qb_invoice_id IS NOT NULL
      GROUP BY qb_invoice_id HAVING COUNT(*) > 1
    `);
    for (const row of dupInvoices.rows) {
      const [keepId, ...removeIds] = row.ids;
      for (const rid of removeIds) {
        await pool.query('DELETE FROM invoices WHERE id=$1', [rid]);
        invoicesDuped++;
      }
    }

    // Also deduplicate invoices by invoice_number (keep the one with qb_invoice_id, else lowest id)
    const dupInvByNum = await pool.query(`
      SELECT invoice_number, array_agg(id ORDER BY
        CASE WHEN qb_invoice_id IS NOT NULL THEN 0 ELSE 1 END, id ASC
      ) as ids
      FROM invoices
      WHERE invoice_number IS NOT NULL
      GROUP BY invoice_number HAVING COUNT(*) > 1
    `);
    for (const row of dupInvByNum.rows) {
      const [keepId, ...removeIds] = row.ids;
      for (const rid of removeIds) {
        await pool.query('DELETE FROM invoices WHERE id=$1', [rid]);
        invoicesDuped++;
      }
    }

    // --- Deduplicate expenses by qb_id (keep lowest id) ---
    let expensesDuped = 0;
    const dupExpenses = await pool.query(`
      SELECT qb_id, array_agg(id ORDER BY id ASC) as ids
      FROM expenses
      WHERE qb_id IS NOT NULL
      GROUP BY qb_id HAVING COUNT(*) > 1
    `);
    for (const row of dupExpenses.rows) {
      const [keepId, ...removeIds] = row.ids;
      for (const rid of removeIds) {
        await pool.query('DELETE FROM expenses WHERE id=$1', [rid]);
        expensesDuped++;
      }
    }

    res.json({
      success: true,
      customers: { groupsMerged: merged, duplicatesRemoved: deleted },
      invoices: { duplicatesRemoved: invoicesDuped },
      expenses: { duplicatesRemoved: expensesDuped }
    });
  } catch (e) {
    console.error('Dedup error:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// POST /api/customers/clean-names - Strip QB ID junk and embedded emails from customer names
app.post('/api/customers/clean-names', async (req, res) => {
  try {
    let totalCleaned = 0;

    // Step 1: Strip " #digits ..." suffix  e.g. "John Smith #1053406 John Smith" → "John Smith"
    const r1 = await pool.query(`
      UPDATE customers
      SET name = TRIM(regexp_replace(name, '\\s+#\\d+.*$', ''))
      WHERE name ~ '\\s+#\\d+'
    `);
    totalCleaned += r1.rowCount;

    // Step 2: Strip embedded emails in parens  e.g. "Ada VanMoulken (ada@gmail.com)" → "Ada VanMoulken"
    const r2 = await pool.query(`
      UPDATE customers
      SET name = TRIM(regexp_replace(name, '\\s*\\([^)]*@[^)]*\\)', '', 'g'))
      WHERE name ~ '\\([^)]*@[^)]*\\)'
    `);
    totalCleaned += r2.rowCount;

    // Step 3: Collapse multiple spaces left over
    await pool.query(`
      UPDATE customers
      SET name = TRIM(regexp_replace(name, '\\s+', ' ', 'g'))
      WHERE name ~ '\\s{2,}'
    `);

    res.json({ success: true, cleaned: totalCleaned });
  } catch (e) {
    console.error('Clean names error:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// POST /api/customers - Create new customer (from Zapier/CopilotCRM sync)
app.post('/api/customers', async (req, res) => {
  try {
    const {
      customer_number, name, firstName, first_name, lastName, last_name,
      email, phone, mobile, fax, street, street2, city, state, 
      postal_code, zip, postalCode, country, type, tags, notes, status
    } = req.body;

    // Handle name variations from CopilotCRM
    const finalFirstName = firstName || first_name || null;
    const finalLastName = lastName || last_name || null;
    const finalName = name || (finalFirstName && finalLastName ? `${finalFirstName} ${finalLastName}` : finalFirstName || finalLastName || 'Unknown');
    const finalPostalCode = postal_code || zip || postalCode || null;
    const finalStatus = status || 'Active';

    // Check for duplicates by email or customer_number
    if (email) {
      const existing = await pool.query('SELECT id FROM customers WHERE email = $1', [email]);
      if (existing.rows.length > 0) {
        console.log('⚠️ Customer already exists with email:', email);
        return res.json({ success: true, message: 'Customer already exists', customer_id: existing.rows[0].id });
      }
    }

    const result = await pool.query(`
      INSERT INTO customers (
        customer_number, name, email, status, type, country,
        street, street2, city, state, postal_code, phone, fax, mobile,
        first_name, last_name, tags, notes, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *
    `, [
      customer_number || null,
      finalName,
      email || null,
      finalStatus,
      type || null,
      country || 'USA',
      street || null,
      street2 || null,
      city || null,
      state || null,
      finalPostalCode,
      phone || null,
      fax || null,
      mobile || null,
      finalFirstName,
      finalLastName,
      tags || null,
      notes || null
    ]);

    console.log('👤 Customer created from Zapier:', finalName, email);
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) {
    console.error('Error creating customer:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/customers/:id/properties', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM properties WHERE customer_id = $1', [req.params.id]);
    res.json({ success: true, properties: result.rows });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/customers/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ['name', 'status', 'email', 'phone', 'mobile', 'street', 'city', 'state', 'postal_code', 'tags', 'notes'];
    const sets = [], vals = [];
    let p = 1;
    Object.keys(req.body).forEach(k => {
      if (allowed.includes(k)) { sets.push(`${k} = $${p++}`); vals.push(req.body[k]); }
    });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(id);
    const result = await pool.query(`UPDATE customers SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.delete('/api/customers/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM customers WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// GET /api/customers/search - Search customers by name for auto-fill
app.get('/api/customers/search', async (req, res) => {
  try {
    const { name } = req.query;
    if (!name || name.length < 2) {
      return res.json({ success: true, customers: [] });
    }
    
    const result = await pool.query(
      `SELECT id, name, email, phone, mobile, street, city, state, postal_code
       FROM customers
       WHERE LOWER(name) LIKE LOWER($1)
       ORDER BY name
       LIMIT 10`,
      [`%${name}%`]
    );
    
    res.json({ success: true, customers: result.rows });
  } catch (error) { 
    console.error('Error searching customers:', error);
    res.status(500).json({ success: false, error: error.message }); 
  }
});

// GET /api/customers/:id/quotes - Get all quotes for a customer
app.get('/api/customers/:id/quotes', async (req, res) => {
  try {
    // First get the customer's email
    const customerResult = await pool.query('SELECT email FROM customers WHERE id = $1', [req.params.id]);
    if (customerResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Customer not found' });
    }
    const customerEmail = customerResult.rows[0].email;
    
    // Get all quotes for this customer by email
    const quotesResult = await pool.query(
      `SELECT id, quote_number, customer_name, customer_email, customer_address,
              services, subtotal, tax_amount, total, monthly_payment, quote_type,
              status, created_at, sent_at, viewed_at, contract_signed_at
       FROM sent_quotes
       WHERE LOWER(customer_email) = LOWER($1)
       ORDER BY created_at DESC`,
      [customerEmail]
    );
    
    res.json({ success: true, quotes: quotesResult.rows });
  } catch (error) { 
    console.error('Error fetching customer quotes:', error);
    res.status(500).json({ success: false, error: error.message }); 
  }
});

// GET /api/customers/:id/jobs - Get all scheduled jobs for a customer
app.get('/api/customers/:id/jobs', async (req, res) => {
  try {
    const customerResult = await pool.query('SELECT name, first_name, last_name FROM customers WHERE id = $1', [req.params.id]);
    if (customerResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const c = customerResult.rows[0];
    const customerName = c.name || ((c.first_name || '') + ' ' + (c.last_name || '')).trim();

    const jobsResult = await pool.query(
      `SELECT id, job_date, customer_name, service_type, service_price, address, status, completed_at, crew_assigned
       FROM scheduled_jobs
       WHERE customer_id = $1 OR LOWER(customer_name) = LOWER($2)
       ORDER BY job_date DESC LIMIT 50`,
      [req.params.id, customerName]
    );
    res.json({ success: true, jobs: jobsResult.rows });
  } catch (error) {
    console.error('Error fetching customer jobs:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/customers/:id/invoices - Get all invoices for a customer
app.get('/api/customers/:id/invoices', async (req, res) => {
  try {
    const customerResult = await pool.query('SELECT name, first_name, last_name, email FROM customers WHERE id = $1', [req.params.id]);
    if (customerResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const c = customerResult.rows[0];
    const customerName = c.name || ((c.first_name || '') + ' ' + (c.last_name || '')).trim();

    const invoicesResult = await pool.query(
      `SELECT id, invoice_number, customer_name, customer_email, total, status, due_date, paid_at, created_at
       FROM invoices
       WHERE LOWER(customer_name) = LOWER($1) OR LOWER(customer_email) = LOWER($2)
       ORDER BY created_at DESC LIMIT 50`,
      [customerName, c.email || '']
    );
    res.json({ success: true, invoices: invoicesResult.rows });
  } catch (error) {
    console.error('Error fetching customer invoices:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// CALLS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.post('/api/calls', async (req, res) => {
  try {
    const { call_sid, from_number, to_number, call_type, status, duration, recording_url, transcription } = req.body;
    const result = await pool.query(
      `INSERT INTO calls (call_sid, from_number, to_number, call_type, status, duration, recording_url, transcription) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *`,
      [call_sid, from_number, to_number, call_type || 'Unknown', status || 'new', duration, recording_url, transcription]
    );
    res.json({ success: true, call: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/calls', async (req, res) => {
  try {
    const { status, call_type, limit = 100 } = req.query;
    // Join customers table so each call returns the matching customer name
    // Match on from_number (inbound) or to_number (outbound) against customer phone/mobile
    let query = `
      SELECT calls.*,
        COALESCE(cu_from.name, cu_to.name) AS customer_name
      FROM calls
      LEFT JOIN customers cu_from
        ON REGEXP_REPLACE(cu_from.phone,  '[^0-9]', '', 'g') = RIGHT(REGEXP_REPLACE(calls.from_number, '[^0-9]', '', 'g'), 10)
        OR REGEXP_REPLACE(cu_from.mobile, '[^0-9]', '', 'g') = RIGHT(REGEXP_REPLACE(calls.from_number, '[^0-9]', '', 'g'), 10)
      LEFT JOIN customers cu_to
        ON REGEXP_REPLACE(cu_to.phone,  '[^0-9]', '', 'g') = RIGHT(REGEXP_REPLACE(calls.to_number, '[^0-9]', '', 'g'), 10)
        OR REGEXP_REPLACE(cu_to.mobile, '[^0-9]', '', 'g') = RIGHT(REGEXP_REPLACE(calls.to_number, '[^0-9]', '', 'g'), 10)
      WHERE 1=1`;
    const params = [];
    let p = 1;
    if (status) { query += ` AND calls.status = $${p++}`; params.push(status); }
    if (call_type) { query += ` AND calls.call_type = $${p++}`; params.push(call_type); }
    query += ` ORDER BY calls.created_at DESC LIMIT $${p}`;
    params.push(limit);
    const result = await pool.query(query, params);
    res.json({ success: true, calls: result.rows });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/calls/stats', async (req, res) => {
  try {
    const total = await pool.query('SELECT COUNT(*) FROM calls');
    const byStatus = await pool.query('SELECT status, COUNT(*) FROM calls GROUP BY status');
    const byType = await pool.query('SELECT call_type, COUNT(*) FROM calls GROUP BY call_type');
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), byStatus: Object.fromEntries(byStatus.rows.map(r => [r.status, parseInt(r.count)])), byType: Object.fromEntries(byType.rows.map(r => [r.call_type, parseInt(r.count)])) } });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/calls/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM calls WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, call: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/calls/:id', async (req, res) => {
  try {
    const { status, notes, transcription } = req.body;
    const sets = [], vals = [];
    let p = 1;
    if (status) { sets.push(`status = $${p++}`); vals.push(status); }
    if (notes) { sets.push(`notes = $${p++}`); vals.push(notes); }
    if (transcription) { sets.push(`transcription = $${p++}`); vals.push(transcription); }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE calls SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, call: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.delete('/api/calls/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM calls WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ═══════════════════════════════════════════════════════════
// SCHEDULED JOBS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/api/jobs', async (req, res) => {
  try {
    const { date, status, crew, start_date, end_date } = req.query;
    let query = 'SELECT * FROM scheduled_jobs WHERE 1=1';
    const params = [];
    let p = 1;
    if (date) { query += ` AND job_date::date = $${p++}::date`; params.push(date); }
    if (start_date && end_date) { query += ` AND job_date::date BETWEEN $${p++}::date AND $${p++}::date`; params.push(start_date, end_date); }
    if (status) { query += ` AND status = $${p++}`; params.push(status); }
    if (crew) { query += ` AND crew_assigned = $${p++}`; params.push(crew); }
    query += ' ORDER BY job_date ASC, route_order ASC NULLS LAST';
    const result = await pool.query(query, params);
    res.json({ success: true, jobs: result.rows });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/jobs/stats', async (req, res) => {
  try {
    const { date } = req.query;
    let filter = '';
    const params = [];
    if (date) { filter = ' WHERE job_date::date = $1::date'; params.push(date); }
    const total = await pool.query(`SELECT COUNT(*) FROM scheduled_jobs${filter}`, params);
    const byStatus = await pool.query(`SELECT status, COUNT(*) FROM scheduled_jobs${filter} GROUP BY status`, params);
    const revenue = await pool.query(`SELECT COALESCE(SUM(service_price), 0) as total FROM scheduled_jobs${filter}`, params);
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), byStatus: Object.fromEntries(byStatus.rows.map(r => [r.status, parseInt(r.count)])), totalRevenue: parseFloat(revenue.rows[0].total) } });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/jobs/dashboard', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const todayCount = await pool.query('SELECT COUNT(*) FROM scheduled_jobs WHERE job_date::date = $1::date', [today]);
    const weekCount = await pool.query("SELECT COUNT(*) FROM scheduled_jobs WHERE job_date::date BETWEEN $1::date AND ($1::date + interval '7 days')", [today]);
    const pending = await pool.query('SELECT COUNT(*) FROM scheduled_jobs WHERE status = $1 AND job_date::date >= $2::date', ['pending', today]);
    const upcoming = await pool.query(`SELECT id, job_date, customer_name, service_type, address, status, service_price FROM scheduled_jobs WHERE job_date::date >= $1::date ORDER BY job_date ASC LIMIT 5`, [today]);
    res.json({ success: true, stats: { today: parseInt(todayCount.rows[0].count), thisWeek: parseInt(weekCount.rows[0].count), pending: parseInt(pending.rows[0].count) }, upcoming: upcoming.rows });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.get('/api/jobs/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM scheduled_jobs WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.post('/api/jobs', async (req, res) => {
  try {
    const { job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned, latitude, longitude } = req.body;
    if (!job_date || !customer_name || !service_type || !address) return res.status(400).json({ success: false, error: 'Missing required fields' });
    const result = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING *`,
      [job_date, customer_name, customer_id, service_type, service_frequency, service_price || 0, address, phone, special_notes, property_notes, status || 'pending', route_order, estimated_duration || 30, crew_assigned, latitude, longitude]
    );
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.post('/api/jobs/bulk', async (req, res) => {
  try {
    const { jobs } = req.body;
    if (!jobs || !Array.isArray(jobs)) return res.status(400).json({ success: false, error: 'Must provide jobs array' });
    const created = [], errors = [];
    for (const job of jobs) {
      try {
        const result = await pool.query(
          `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) RETURNING *`,
          [job.job_date, job.customer_name, job.customer_id, job.service_type, job.service_frequency, job.service_price || 0, job.address, job.phone, job.special_notes, job.property_notes, job.status || 'pending', job.route_order, job.estimated_duration || 30, job.crew_assigned]
        );
        created.push(result.rows[0]);
      } catch (err) { errors.push({ customer: job.customer_name, error: err.message }); }
    }
    res.json({ success: true, created: created.length, errors: errors.length, jobs: created, errorDetails: errors });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/jobs/:id', async (req, res) => {
  try {
    const allowed = ['job_date', 'customer_name', 'service_type', 'service_price', 'address', 'phone', 'special_notes', 'property_notes', 'status', 'route_order', 'crew_assigned', 'completed_at'];
    const sets = [], vals = [];
    let p = 1;
    Object.keys(req.body).forEach(k => { if (allowed.includes(k)) { sets.push(`${k} = $${p++}`); vals.push(req.body[k]); } });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE scheduled_jobs SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/jobs/:id/complete', async (req, res) => {
  try {
    const { completion_lat, completion_lng, completion_notes, completed_by } = req.body;
    const result = await pool.query(
      `UPDATE scheduled_jobs SET status = 'completed', completed_at = CURRENT_TIMESTAMP, completion_lat = $2, completion_lng = $3, completion_notes = $4, completed_by = $5, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *`,
      [req.params.id, completion_lat, completion_lng, completion_notes, completed_by]
    );
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/jobs/reorder', async (req, res) => {
  try {
    const { jobs } = req.body;
    for (const job of jobs) { await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]); }
    res.json({ success: true, updated: jobs.length });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.post('/api/jobs/optimize-route', async (req, res) => {
  try {
    const { date, startAddress, crew } = req.body;
    const GOOGLE_MAPS_API_KEY = process.env.GOOGLE_MAPS_API_KEY;
    if (!GOOGLE_MAPS_API_KEY) return res.status(400).json({ success: false, error: 'Google Maps API key not configured' });
    
    let query = 'SELECT * FROM scheduled_jobs WHERE job_date::date = $1::date AND status != $2';
    let params = [date, 'completed'];
    if (crew) { query += ' AND crew_assigned = $3'; params.push(crew); }
    query += ' ORDER BY route_order ASC NULLS LAST';
    
    const jobsResult = await pool.query(query, params);
    const jobs = jobsResult.rows;
    if (jobs.length < 2) return res.json({ success: true, message: 'Not enough jobs', jobs });
    
    const addresses = jobs.map(j => j.address);
    const origin = startAddress || '9523 Clinton Rd, Cleveland, OH 44144';
    const waypoints = addresses.join('|');
    const url = `https://maps.googleapis.com/maps/api/directions/json?origin=${encodeURIComponent(origin)}&destination=${encodeURIComponent(origin)}&waypoints=optimize:true|${encodeURIComponent(waypoints)}&key=${GOOGLE_MAPS_API_KEY}`;
    
    const response = await fetch(url);
    const data = await response.json();
    if (data.status !== 'OK') return res.status(400).json({ success: false, error: `Google Maps error: ${data.status}` });
    
    const waypointOrder = data.routes[0].waypoint_order;
    const optimizedJobs = waypointOrder.map((idx, order) => ({ ...jobs[idx], route_order: order + 1 }));
    for (const job of optimizedJobs) { await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]); }
    
    const legs = data.routes[0].legs;
    const totalDistance = legs.reduce((sum, leg) => sum + leg.distance.value, 0);
    const totalDuration = legs.reduce((sum, leg) => sum + leg.duration.value, 0);
    
    res.json({ success: true, message: 'Route optimized', jobs: optimizedJobs, stats: { totalStops: jobs.length, totalDistance: (totalDistance / 1609.34).toFixed(1) + ' miles', totalDriveTime: Math.round(totalDuration / 60) + ' minutes' } });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.delete('/api/jobs/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM scheduled_jobs WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ═══════════════════════════════════════════════════════════
// CREWS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/api/crews', async (req, res) => {
  try {
    const { active_only } = req.query;
    let query = 'SELECT * FROM crews';
    if (active_only === 'true') query += ' WHERE is_active = true';
    query += ' ORDER BY name ASC';
    const result = await pool.query(query);
    res.json({ success: true, crews: result.rows });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.post('/api/crews', async (req, res) => {
  try {
    const { name, members, crew_type, notes } = req.body;
    if (!name) return res.status(400).json({ success: false, error: 'Crew name required' });
    const result = await pool.query('INSERT INTO crews (name, members, crew_type, notes) VALUES ($1, $2, $3, $4) RETURNING *', [name, members, crew_type, notes]);
    res.json({ success: true, crew: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.patch('/api/crews/:id', async (req, res) => {
  try {
    const { name, members, crew_type, notes, is_active } = req.body;
    const sets = [], vals = [];
    let p = 1;
    if (name !== undefined) { sets.push(`name = $${p++}`); vals.push(name); }
    if (members !== undefined) { sets.push(`members = $${p++}`); vals.push(members); }
    if (crew_type !== undefined) { sets.push(`crew_type = $${p++}`); vals.push(crew_type); }
    if (notes !== undefined) { sets.push(`notes = $${p++}`); vals.push(notes); }
    if (is_active !== undefined) { sets.push(`is_active = $${p++}`); vals.push(is_active); }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE crews SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, crew: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.delete('/api/crews/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM crews WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ═══════════════════════════════════════════════════════════
// CSV IMPORT ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.post('/api/import-customers', upload.single('csvfile'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No CSV file' });
    const csvContent = req.file.buffer.toString('utf-8');
    const lines = csvContent.split('\n');
    const headers = parseCSVLine(lines[0]);
    
    const customers = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      try {
        const values = parseCSVLine(line);
        const customer = {};
        headers.forEach((h, idx) => { customer[h] = values[idx] || ''; });
        customers.push(customer);
      } catch (e) {}
    }
    
    let imported = 0, updated = 0, skipped = 0;
    for (const c of customers) {
      try {
        const email = c['Email'] || '';
        const customerNumber = c['Customer Number'] || '';
        let existing = { rows: [] };
        if (email) existing = await pool.query('SELECT id FROM customers WHERE email = $1', [email]);
        else if (customerNumber) existing = await pool.query('SELECT id FROM customers WHERE customer_number = $1', [customerNumber]);
        
        if (existing.rows.length > 0) {
          await pool.query(`UPDATE customers SET name = $1, status = $2, phone = $3, mobile = $4, street = $5, city = $6, state = $7, postal_code = $8, tags = $9, notes = $10, updated_at = CURRENT_TIMESTAMP WHERE id = $11`,
            [c['Name'], c['Status'] || 'Active', c['Phone'], c['Mobile'], c['Street'], c['City'], c['State'], c['Postal Code'], c['Tags'], c['Notes'], existing.rows[0].id]);
          updated++;
        } else {
          await pool.query(`INSERT INTO customers (customer_number, name, status, email, phone, mobile, street, street2, city, state, postal_code, tags, notes) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
            [customerNumber, c['Name'], c['Status'] || 'Active', email, c['Phone'], c['Mobile'], c['Street'], c['Street2'], c['City'], c['State'], c['Postal Code'], c['Tags'], c['Notes']]);
          imported++;
        }
      } catch (e) { skipped++; }
    }
    res.json({ success: true, message: 'Import complete', stats: { total: customers.length, imported, updated, skipped } });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

app.post('/api/import-scheduling', upload.single('csvfile'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No CSV file' });
    const csvContent = req.file.buffer.toString('utf-8');
    const lines = csvContent.split('\n');
    const headers = parseCSVLine(lines[0]);
    
    const jobs = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      try {
        const values = parseCSVLine(line);
        const job = {};
        headers.forEach((h, idx) => { job[h] = values[idx] || ''; });
        jobs.push(job);
      } catch (e) {}
    }
    
    function parseDate(dateStr) {
      if (!dateStr) return null;
      let cleaned = dateStr.replace(/^="?"?/, '').replace(/"?"?$/, '').trim();
      try { const d = new Date(cleaned); return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0]; } catch { return null; }
    }
    
    function parseNameAddress(details) {
      if (!details) return { name: '', address: '' };
      const parts = details.split(/\s{2,}/);
      return parts.length >= 2 ? { name: parts[0].trim(), address: parts.slice(1).join(' ').trim() } : { name: details, address: '' };
    }
    
    let imported = 0, updated = 0, skipped = 0;
    for (const job of jobs) {
      try {
        const jobDate = parseDate(job['Date of Service']);
        if (!jobDate) { skipped++; continue; }
        const { name, address } = parseNameAddress(job['Name / Details']);
        const serviceType = job['Title'] || 'Service';
        const price = parseFloat((job['Visit Total'] || '0').replace(/[^0-9.]/g, '')) || 0;
        
        const existing = await pool.query('SELECT id FROM scheduled_jobs WHERE job_date = $1 AND customer_name = $2 AND service_type = $3', [jobDate, name, serviceType]);
        if (existing.rows.length > 0) {
          await pool.query('UPDATE scheduled_jobs SET address = $1, service_price = $2 WHERE id = $3', [address, price, existing.rows[0].id]);
          updated++;
        } else {
          await pool.query('INSERT INTO scheduled_jobs (job_date, customer_name, service_type, service_price, address, status) VALUES ($1, $2, $3, $4, $5, $6)', [jobDate, name, serviceType, price, address, 'pending']);
          imported++;
        }
      } catch (e) { skipped++; }
    }
    res.json({ success: true, message: 'Import complete', stats: { total: jobs.length, imported, updated, skipped } });
  } catch (error) { res.status(500).json({ success: false, error: error.message }); }
});

// ═══════════════════════════════════════════════════════════
// PROPERTY ANALYSIS WITH REGRID PARCEL API + FAL.AI SAM 3
// ═══════════════════════════════════════════════════════════
// Hybrid approach:
// 1. Regrid API provides accurate lot size from county tax records
// 2. SAM 3 detects lawn/bed/hardscape RATIOS from satellite image
// 3. Combine: ratios × lot size = accurate measurements

// Get parcel data from Regrid API v2
async function getParcelData(lat, lng) {
  const regridToken = process.env.REGRID_API_TOKEN;
  
  if (!regridToken) {
    console.log('REGRID_API_TOKEN not configured');
    return null;
  }
  
  try {
    // Regrid API v2 endpoint for lat/lon lookup
    // Using US country code and parcels/point endpoint
    const url = `https://app.regrid.com/api/v2/us/parcels/point?lat=${lat}&lon=${lng}&token=${regridToken}&limit=1&radius=50`;
    console.log('Calling Regrid API v2 for coordinates:', lat, lng);
    
    const response = await fetch(url);
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error('Regrid API error:', response.status, errorText);
      return null;
    }
    
    const data = await response.json();
    console.log('Regrid API response keys:', Object.keys(data));
    
    // v2 returns { parcels: { type: "FeatureCollection", features: [...] } }
    const features = data.parcels?.features || data.features || data.results || [];
    
    if (features.length === 0) {
      console.log('No parcel found at coordinates');
      return null;
    }
    
    const parcel = features[0];
    const props = parcel.properties || {};
    
    console.log('Regrid parcel properties keys:', Object.keys(props).slice(0, 20));
    
    // Extract lot size - try multiple fields from Regrid schema
    // ll_gissqft = Regrid calculated square feet (most accurate)
    // sqft = County-provided square feet
    // ll_gisacre = Regrid calculated acres (convert to sq ft)
    const lotSizeSqFt = props.ll_gissqft || props.sqft || props.lotsqft || props.lotsizearea ||
                        (props.ll_gisacre ? parseFloat(props.ll_gisacre) * 43560 : null);
    
    console.log('Regrid parcel data:', {
      lotSize: lotSizeSqFt,
      ll_gissqft: props.ll_gissqft,
      sqft: props.sqft,
      ll_gisacre: props.ll_gisacre,
      owner: props.owner,
      address: props.address,
      parcelId: props.parcelnumb
    });
    
    return {
      lotSizeSqFt: lotSizeSqFt ? Math.round(parseFloat(lotSizeSqFt)) : null,
      owner: props.owner || null,
      address: props.address || null,
      parcelId: props.parcelnumb || props.parcel_id || null,
      yearBuilt: props.yearbuilt || null,
      buildingSqFt: props.ll_bldg_footprint_sqft || props.bldg_sqft || null,
      acres: props.ll_gisacre || null,
      rawData: props
    };
  } catch (error) {
    console.error('Regrid API error:', error);
    return null;
  }
}

app.post('/api/analyze-property', async (req, res) => {
  try {
    const { address, imageUrl, lat, lng, lotSize: userLotSize, zoom = 19 } = req.body;
    
    let latitude = lat;
    let longitude = lng;
    
    // If we have an address but no coordinates, geocode it
    if (address && (!lat || !lng)) {
      const googleApiKey = process.env.GOOGLE_MAPS_API_KEY;
      if (googleApiKey) {
        try {
          const geocodeUrl = `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${googleApiKey}`;
          const geocodeResponse = await fetch(geocodeUrl);
          const geocodeData = await geocodeResponse.json();
          
          if (geocodeData.status === 'OK' && geocodeData.results.length > 0) {
            latitude = geocodeData.results[0].geometry.location.lat;
            longitude = geocodeData.results[0].geometry.location.lng;
            console.log('Geocoded address:', address, '->', latitude, longitude);
          }
        } catch (e) {
          console.error('Geocoding error:', e);
        }
      }
    }
    
    // Step 1: Get lot size from Regrid (county records) or use user-provided
    let lotSizeSqFt = userLotSize ? parseInt(userLotSize) : null;
    let parcelData = null;
    let lotSizeSource = 'user';
    
    if (!lotSizeSqFt && latitude && longitude) {
      parcelData = await getParcelData(latitude, longitude);
      if (parcelData && parcelData.lotSizeSqFt) {
        lotSizeSqFt = parcelData.lotSizeSqFt;
        lotSizeSource = 'regrid';
        console.log('Got lot size from Regrid:', lotSizeSqFt, 'sq ft');
      }
    }
    
    // Fallback to Cleveland average if no lot size
    if (!lotSizeSqFt) {
      lotSizeSqFt = 8500; // Cleveland average lot size
      lotSizeSource = 'estimate';
      console.log('Using estimated lot size:', lotSizeSqFt, 'sq ft');
    }
    
    // Step 2: Use SAM 3 to detect ratios (what % is lawn vs beds vs hardscape)
    const falApiKey = process.env.FAL_API_KEY;
    let ratios = { lawn: 0.60, bed: 0.10, hardscape: 0.20 }; // Default ratios
    let ratioSource = 'estimate';
    let samDebug = {};
    
    if (falApiKey && imageUrl) {
      console.log('Running SAM 3 analysis for ratios...');
      
      try {
        // Run SAM 3 segmentation for each category
        let lawnResult, bedsResult, hardscapeResult;
        
        try {
          lawnResult = await segmentWithSAM3(falApiKey, imageUrl, 'green grass lawn yard turf');
        } catch (e) {
          console.error('Lawn segmentation failed:', e.message);
          lawnResult = { pixelRatio: 0 };
        }
        
        try {
          bedsResult = await segmentWithSAM3(falApiKey, imageUrl, 'brown mulch garden bed landscaping bark');
        } catch (e) {
          console.error('Beds segmentation failed:', e.message);
          bedsResult = { pixelRatio: 0 };
        }
        
        try {
          hardscapeResult = await segmentWithSAM3(falApiKey, imageUrl, 'gray concrete driveway sidewalk pavement asphalt');
        } catch (e) {
          console.error('Hardscape segmentation failed:', e.message);
          hardscapeResult = { pixelRatio: 0 };
        }
        
        // Calculate raw ratios
        const rawLawn = lawnResult.pixelRatio || 0;
        const rawBed = bedsResult.pixelRatio || 0;
        const rawHardscape = hardscapeResult.pixelRatio || 0;
        const rawTotal = rawLawn + rawBed + rawHardscape;
        
        samDebug = {
          rawLawn,
          rawBed,
          rawHardscape,
          rawTotal
        };
        
        // Normalize ratios to sum to ~0.90 (leaving 10% for house/other)
        if (rawTotal > 0.1) {
          const normalizer = 0.90 / rawTotal;
          ratios = {
            lawn: rawLawn * normalizer,
            bed: rawBed * normalizer,
            hardscape: rawHardscape * normalizer
          };
          ratioSource = 'sam3';
          console.log('SAM 3 normalized ratios:', ratios);
        }
      } catch (e) {
        console.error('SAM 3 analysis failed:', e);
      }
    }
    
    // Step 3: Calculate final areas by applying ratios to lot size
    const lawnArea = Math.round(lotSizeSqFt * ratios.lawn);
    const bedArea = Math.round(lotSizeSqFt * ratios.bed);
    const hardscapeArea = Math.round(lotSizeSqFt * ratios.hardscape);
    const shrubCount = Math.max(5, Math.round(bedArea / 55)); // ~1 shrub per 55 sq ft of bed
    
    console.log(`Final analysis: Lot=${lotSizeSqFt}, Lawn=${lawnArea}, Bed=${bedArea}, Hardscape=${hardscapeArea}`);
    
    res.json({
      success: true,
      analysis: {
        totalLot: lotSizeSqFt,
        lawnArea,
        bedArea,
        hardscapeArea,
        shrubCount,
        ratios,
        confidence: {
          lotSize: lotSizeSource === 'regrid' ? 0.95 : (lotSizeSource === 'user' ? 0.90 : 0.5),
          ratios: ratioSource === 'sam3' ? 0.80 : 0.5
        }
      },
      method: `${lotSizeSource}+${ratioSource}`,
      parcel: parcelData ? {
        owner: parcelData.owner,
        address: parcelData.address,
        parcelId: parcelData.parcelId,
        yearBuilt: parcelData.yearBuilt,
        buildingSqFt: parcelData.buildingSqFt
      } : null,
      debug: {
        lotSizeSource,
        ratioSource,
        coordinates: { lat: latitude, lng: longitude },
        ...samDebug
      }
    });
    
  } catch (error) {
    console.error('Property analysis error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Call fal.ai SAM 3 API for segmentation
async function segmentWithSAM3(apiKey, imageUrl, prompt) {
  const response = await fetch('https://fal.run/fal-ai/sam-3/image', {
    method: 'POST',
    headers: {
      'Authorization': `Key ${apiKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      image_url: imageUrl,
      prompt: prompt,
      apply_mask: true,
      output_format: 'png',
      return_multiple_masks: false,
      include_scores: true
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error('SAM 3 API error:', response.status, errorText);
    throw new Error(`SAM 3 API error: ${response.status}`);
  }

  const data = await response.json();
  
  const maskUrl = data.masks?.[0]?.url || data.image?.url;
  const score = data.scores?.[0] || data.metadata?.[0]?.score || 0.5;
  
  let pixelRatio = 0;
  if (maskUrl) {
    pixelRatio = await estimatePixelRatio(maskUrl);
  }
  
  return { maskUrl, score, pixelRatio };
}

// Estimate pixel coverage from mask file size
async function estimatePixelRatio(maskUrl) {
  try {
    const response = await fetch(maskUrl);
    const buffer = await response.arrayBuffer();
    const bytes = new Uint8Array(buffer);
    const fileSize = bytes.length;
    
    console.log(`Mask file size: ${fileSize} bytes`);
    
    // Calibrated thresholds for PNG masks
    if (fileSize < 5000) return 0.02;
    if (fileSize < 10000) return 0.08;
    if (fileSize < 20000) return 0.15;
    if (fileSize < 40000) return 0.25;
    if (fileSize < 60000) return 0.35;
    if (fileSize < 80000) return 0.45;
    if (fileSize < 100000) return 0.55;
    return 0.65;
    
  } catch (error) {
    console.error('Error estimating pixel ratio:', error);
    return 0;
  }
}

// ═══════════════════════════════════════════════════════════
// EXPENSES ENDPOINTS
// ═══════════════════════════════════════════════════════════

// GET /api/expenses
app.get('/api/expenses', async (req, res) => {
  try {
    const { year, category } = req.query;
    let query = 'SELECT * FROM expenses WHERE 1=1';
    const params = [];
    let p = 1;
    
    if (year) {
      query += ` AND EXTRACT(YEAR FROM expense_date) = $${p++}`;
      params.push(year);
    }
    if (category) {
      query += ` AND category = $${p++}`;
      params.push(category);
    }
    
    query += ' ORDER BY expense_date DESC, created_at DESC';
    const result = await pool.query(query, params);
    res.json({ success: true, expenses: result.rows });
  } catch (error) {
    console.error('Error fetching expenses:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/expenses/stats
app.get('/api/expenses/stats', async (req, res) => {
  try {
    const { year } = req.query;
    const currentYear = year || new Date().getFullYear();
    const currentMonth = new Date().getMonth() + 1;
    
    const yearTotal = await pool.query(
      'SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM expenses WHERE EXTRACT(YEAR FROM expense_date) = $1',
      [currentYear]
    );
    
    const monthTotal = await pool.query(
      'SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM expenses WHERE EXTRACT(YEAR FROM expense_date) = $1 AND EXTRACT(MONTH FROM expense_date) = $2',
      [currentYear, currentMonth]
    );
    
    const byCategory = await pool.query(
      'SELECT category, COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM expenses WHERE EXTRACT(YEAR FROM expense_date) = $1 GROUP BY category',
      [currentYear]
    );
    
    res.json({
      success: true,
      stats: {
        yearTotal: parseFloat(yearTotal.rows[0].total),
        yearCount: parseInt(yearTotal.rows[0].count),
        monthTotal: parseFloat(monthTotal.rows[0].total),
        monthCount: parseInt(monthTotal.rows[0].count),
        byCategory: byCategory.rows
      }
    });
  } catch (error) {
    console.error('Error fetching expense stats:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/expenses
app.post('/api/expenses', async (req, res) => {
  try {
    const { vendor, amount, expense_date, category, receipt_image, notes } = req.body;
    
    if (!vendor || !amount || !expense_date) {
      return res.status(400).json({ success: false, error: 'Vendor, amount, and date are required' });
    }
    
    const result = await pool.query(
      `INSERT INTO expenses (vendor, amount, expense_date, category, receipt_image, notes)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
      [vendor, amount, expense_date, category || 'Other', receipt_image || null, notes || null]
    );
    
    res.json({ success: true, expense: result.rows[0] });
  } catch (error) {
    console.error('Error creating expense:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/expenses/:id
app.get('/api/expenses/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM expenses WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Expense not found' });
    }
    res.json({ success: true, expense: result.rows[0] });
  } catch (error) {
    console.error('Error fetching expense:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PATCH /api/expenses/:id
app.patch('/api/expenses/:id', async (req, res) => {
  try {
    const { vendor, amount, expense_date, category, receipt_image, notes } = req.body;
    const sets = [], vals = [];
    let p = 1;
    
    if (vendor !== undefined) { sets.push(`vendor = $${p++}`); vals.push(vendor); }
    if (amount !== undefined) { sets.push(`amount = $${p++}`); vals.push(amount); }
    if (expense_date !== undefined) { sets.push(`expense_date = $${p++}`); vals.push(expense_date); }
    if (category !== undefined) { sets.push(`category = $${p++}`); vals.push(category); }
    if (receipt_image !== undefined) { sets.push(`receipt_image = $${p++}`); vals.push(receipt_image); }
    if (notes !== undefined) { sets.push(`notes = $${p++}`); vals.push(notes); }
    
    if (sets.length === 0) {
      return res.status(400).json({ success: false, error: 'No fields to update' });
    }
    
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    
    const result = await pool.query(
      `UPDATE expenses SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`,
      vals
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Expense not found' });
    }
    res.json({ success: true, expense: result.rows[0] });
  } catch (error) {
    console.error('Error updating expense:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// DELETE /api/expenses/:id
app.delete('/api/expenses/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM expenses WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Expense not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting expense:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// RECEIPT OCR ENDPOINT (Google Cloud Vision)
// ═══════════════════════════════════════════════════════════

app.post('/api/ocr/receipt', async (req, res) => {
  try {
    const { image } = req.body;
    
    if (!image) {
      return res.status(400).json({ success: false, error: 'No image provided' });
    }

    const apiKey = process.env.GOOGLE_CLOUD_VISION_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ success: false, error: 'Vision API key not configured' });
    }

    // Remove data URL prefix if present
    const base64Data = image.replace(/^data:image\/\w+;base64,/, '');

    // Call Google Cloud Vision API
    const visionResponse = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          requests: [{
            image: { content: base64Data },
            features: [{ type: 'TEXT_DETECTION' }]
          }]
        })
      }
    );

    const visionData = await visionResponse.json();

    if (visionData.error) {
      console.error('Vision API error:', visionData.error);
      return res.status(500).json({ success: false, error: visionData.error.message });
    }

    const fullText = visionData.responses?.[0]?.fullTextAnnotation?.text || '';
    
    if (!fullText) {
      return res.json({ success: true, data: { vendor: '', amount: '', date: '', category: 'Other' } });
    }

    // Parse the receipt
    const extracted = parseReceiptText(fullText);

    res.json({
      success: true,
      data: {
        vendor: extracted.vendor,
        amount: extracted.amount,
        date: extracted.date,
        category: extracted.category
      }
    });

  } catch (error) {
    console.error('OCR error:', error);
    res.status(500).json({ success: false, error: 'Failed to process receipt' });
  }
});

// Parse receipt text to extract vendor, amount, date, category
function parseReceiptText(text) {
  const lines = text.split('\n').map(l => l.trim()).filter(l => l);
  
  let vendor = '';
  let amount = '';
  let date = '';
  let category = 'Other';

  // Known vendors
  const knownVendors = [
    'home depot', 'lowes', 'menards', 'ace hardware', 'true value',
    'sunoco', 'shell', 'bp', 'speedway', 'marathon', 'circle k', 'sheetz', 'wawa', 'getgo', 'giant eagle',
    'costco', 'sams club', 'walmart', 'target', 'amazon',
    'autozone', 'advance auto', 'oreilly', 'napa',
    'staples', 'office depot', 'office max',
    'tractor supply', 'rural king', 'northern tool',
    'fastenal', 'grainger', 'uline', 'siteone', 'john deere', 'lesco'
  ];

  // Find vendor in first 5 lines
  for (let i = 0; i < Math.min(5, lines.length); i++) {
    const line = lines[i].toLowerCase();
    for (const kv of knownVendors) {
      if (line.includes(kv)) {
        vendor = lines[i];
        break;
      }
    }
    if (vendor) break;
    if (!vendor && i < 3 && lines[i].length > 3 && /^[A-Z]/.test(lines[i]) && !/^\d/.test(lines[i])) {
      vendor = lines[i];
    }
  }

  // Find amount - look for TOTAL
  const fullTextLower = text.toLowerCase();
  const totalMatch = fullTextLower.match(/(?:total|amount due|balance due|grand total)[:\s]*\$?([\d,]+\.?\d{0,2})/i);
  if (totalMatch) {
    amount = totalMatch[1].replace(',', '');
  } else {
    const amounts = [];
    const dollarMatches = text.matchAll(/\$?\s*([\d,]+\.\d{2})/g);
    for (const match of dollarMatches) {
      const val = parseFloat(match[1].replace(',', ''));
      if (val > 0 && val < 10000) amounts.push(val);
    }
    if (amounts.length > 0) amount = Math.max(...amounts).toFixed(2);
  }

  // Find date
  const datePatterns = [
    /(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/,
    /(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})/,
    /(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s*(\d{1,2}),?\s*(\d{2,4})/i
  ];

  for (const pattern of datePatterns) {
    const match = text.match(pattern);
    if (match) {
      try {
        const dateStr = match[0];
        const slashMatch = dateStr.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
        if (slashMatch) {
          let [_, m, d, y] = slashMatch;
          if (y.length === 2) y = '20' + y;
          if (parseInt(m) > 12) [m, d] = [d, m]; // Swap if month > 12
          date = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
          break;
        }
        const parsed = new Date(dateStr);
        if (!isNaN(parsed.getTime())) {
          date = parsed.toISOString().split('T')[0];
          break;
        }
      } catch (e) {}
    }
  }

  // Detect category
  const categoryKeywords = {
    'Fuel': ['gas', 'fuel', 'diesel', 'unleaded', 'premium', 'gallon', 'sunoco', 'shell', 'bp', 'speedway', 'marathon', 'circle k', 'getgo', 'sheetz', 'wawa'],
    'Equipment': ['mower', 'trimmer', 'blower', 'chainsaw', 'edger', 'aerator', 'spreader', 'trailer', 'truck', 'stihl', 'husqvarna', 'john deere', 'toro', 'exmark', 'scag'],
    'Service Repairs': ['repair', 'service', 'maintenance', 'parts', 'blade', 'filter', 'belt', 'spark plug', 'oil change', 'tune up', 'fix'],
    'Supplies': ['mulch', 'fertilizer', 'seed', 'soil', 'plant', 'landscape', 'stone', 'gravel', 'siteone', 'lesco', 'bags', 'gloves', 'safety'],
    'Cost of Goods Sold': ['wholesale', 'resale', 'inventory', 'stock'],
    'Operating Costs': ['insurance', 'license', 'permit', 'registration', 'toll', 'parking'],
    'Utilities': ['electric', 'water', 'internet', 'phone', 'utility', 'gas bill', 'heating'],
    'Advertising/Accounting': ['advertising', 'marketing', 'promo', 'print', 'signs', 'quickbooks', 'accounting', 'tax prep', 'bookkeeping'],
    'Losses': ['damage', 'loss', 'theft', 'write off', 'disposal']
  };

  const textLower = text.toLowerCase();
  for (const [cat, keywords] of Object.entries(categoryKeywords)) {
    if (keywords.some(kw => textLower.includes(kw))) {
      category = cat;
      break;
    }
  }

  return { vendor: vendor.substring(0, 100), amount, date, category };
}

// ═══════════════════════════════════════════════════════════
// CAMPAIGNS ENDPOINTS
// ═══════════════════════════════════════════════════════════

// GET /api/campaigns - List all campaigns with submission counts
app.get('/api/campaigns', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        c.*,
        COUNT(s.id) as submission_count,
        COUNT(CASE WHEN s.status = 'new' THEN 1 END) as new_count,
        COUNT(CASE WHEN s.status = 'enrolled' THEN 1 END) as enrolled_count
      FROM campaigns c
      LEFT JOIN campaign_submissions s ON c.name = s.campaign_id OR c.id::text = s.campaign_id
      GROUP BY c.id
      ORDER BY c.created_at DESC
    `);

    const weekResult = await pool.query(`
      SELECT COUNT(*) as count 
      FROM campaign_submissions 
      WHERE created_at >= NOW() - INTERVAL '7 days'
    `);

    res.json({
      success: true,
      campaigns: result.rows,
      new_this_week: parseInt(weekResult.rows[0]?.count || 0)
    });
  } catch (error) {
    console.error('Error fetching campaigns:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/campaigns - Create a new campaign
app.post('/api/campaigns', async (req, res) => {
  try {
    const { name, description, form_url, status = 'active' } = req.body;
    if (!name) {
      return res.status(400).json({ success: false, error: 'Campaign name is required' });
    }
    const result = await pool.query(`
      INSERT INTO campaigns (name, description, form_url, status)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `, [name, description, form_url, status]);
    res.json({ success: true, campaign: result.rows[0] });
  } catch (error) {
    console.error('Error creating campaign:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/campaigns/:id - Get single campaign
app.get('/api/campaigns/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('SELECT * FROM campaigns WHERE id = $1 OR name = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    res.json({ success: true, campaign: result.rows[0] });
  } catch (error) {
    console.error('Error fetching campaign:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PATCH /api/campaigns/:id - Update campaign
app.patch('/api/campaigns/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { name, description, form_url, status } = req.body;
    const updates = [];
    const values = [];
    let p = 1;
    if (name !== undefined) { updates.push(`name = $${p++}`); values.push(name); }
    if (description !== undefined) { updates.push(`description = $${p++}`); values.push(description); }
    if (form_url !== undefined) { updates.push(`form_url = $${p++}`); values.push(form_url); }
    if (status !== undefined) { updates.push(`status = $${p++}`); values.push(status); }
    if (updates.length === 0) {
      return res.status(400).json({ success: false, error: 'No fields to update' });
    }
    updates.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);
    const result = await pool.query(
      `UPDATE campaigns SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`,
      values
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    res.json({ success: true, campaign: result.rows[0] });
  } catch (error) {
    console.error('Error updating campaign:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// DELETE /api/campaigns/:id - Delete campaign
app.delete('/api/campaigns/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM campaigns WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting campaign:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/campaigns/:id/submissions - Get submissions for a campaign
app.get('/api/campaigns/:id/submissions', async (req, res) => {
  try {
    const { id } = req.params;
    const { status, limit = 100, offset = 0 } = req.query;
    let query = 'SELECT * FROM campaign_submissions WHERE campaign_id = $1';
    const params = [id];
    if (status) {
      query += ' AND status = $2';
      params.push(status);
    }
    query += ` ORDER BY created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
    params.push(limit, offset);
    const result = await pool.query(query, params);
    const countResult = await pool.query(
      'SELECT COUNT(*) as total FROM campaign_submissions WHERE campaign_id = $1',
      [id]
    );
    res.json({
      success: true,
      submissions: result.rows,
      total: parseInt(countResult.rows[0]?.total || 0)
    });
  } catch (error) {
    console.error('Error fetching submissions:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/campaigns/submissions - Create a new submission (from customer form)
app.post('/api/campaigns/submissions', async (req, res) => {
  try {
    const { campaign_id, name, firstName, lastName, email, phone, address, services = [], notes } = req.body;
    if (!campaign_id) {
      return res.status(400).json({ success: false, error: 'Campaign ID is required' });
    }
    if (!email && !phone) {
      return res.status(400).json({ success: false, error: 'Email or phone is required' });
    }
    let servicesArray = null;
    if (services) {
      if (Array.isArray(services)) servicesArray = services;
      else if (typeof services === 'string' && services.length > 0) servicesArray = services.split(',').map(s => s.trim());
    }
    const fullName = name || ((firstName || '') + ' ' + (lastName || '')).trim();
    const result = await pool.query(`
      INSERT INTO campaign_submissions 
      (campaign_id, name, first_name, last_name, email, phone, address, services, notes, status)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'new')
      RETURNING *
    `, [campaign_id, fullName, firstName || null, lastName || null, email || null, phone || null, address || null, servicesArray, notes || null]);
    res.json({ success: true, submission: result.rows[0] });

    // Send notification email
    const servicesText = servicesArray ? servicesArray.join(', ') : 'None specified';
    const dashboardUrl = 'https://pappas-quote-backend-production.up.railway.app/campaigns.html';
    const emailHtml = `
      <h2>New Campaign Submission</h2>
      <p><strong>Campaign:</strong> ${campaign_id}</p>
      <p><strong>Name:</strong> ${fullName}</p>
      <p><strong>Email:</strong> <a href="mailto:${email}">${email}</a></p>
      <p><strong>Phone:</strong> ${phone || 'Not provided'}</p>
      <p><strong>Address:</strong> ${address || 'Not provided'}</p>
      <p><strong>Services:</strong> ${servicesText}</p>
      <p><strong>Notes:</strong> ${notes || 'None'}</p>
      <br>
      <p><a href="${dashboardUrl}">View in Dashboard</a></p>
    `;
    sendEmail(NOTIFICATION_EMAIL, `New ${campaign_id} Request from ${fullName}`, emailHtml);
  } catch (error) {
    console.error('Error creating submission:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PATCH /api/campaigns/submissions/:id - Update submission status
app.patch('/api/campaigns/submissions/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { status, notes } = req.body;
    const updates = [];
    const values = [];
    let p = 1;
    if (status !== undefined) { updates.push(`status = $${p++}`); values.push(status); }
    if (notes !== undefined) { updates.push(`notes = $${p++}`); values.push(notes); }
    if (updates.length === 0) {
      return res.status(400).json({ success: false, error: 'No fields to update' });
    }
    updates.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);
    const result = await pool.query(
      `UPDATE campaign_submissions SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`,
      values
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Submission not found' });
    }
    res.json({ success: true, submission: result.rows[0] });
  } catch (error) {
    console.error('Error updating submission:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// DELETE /api/campaigns/submissions/:id - Delete a submission
app.delete('/api/campaigns/submissions/:id', async (req, res) => {
  try {
    const result = await pool.query(
      'DELETE FROM campaign_submissions WHERE id = $1 RETURNING *',
      [req.params.id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Submission not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting submission:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// SENT QUOTES ENDPOINTS - For tracking quotes sent to customers
// ═══════════════════════════════════════════════════════════

// Helper to generate unique token for signing links
function generateToken() {
  return require('crypto').randomBytes(32).toString('hex');
}

// GET /api/sent-quotes - List all sent quotes
app.get('/api/sent-quotes', async (req, res) => {
  try {
    const { status, quote_type, search, limit = 50, offset = 0 } = req.query;
    let query = `
      SELECT sq.*, c.name as customer_name_lookup, c.email as customer_email_lookup
      FROM sent_quotes sq
      LEFT JOIN customers c ON sq.customer_id = c.id
      WHERE 1=1
    `;
    const values = [];
    let p = 1;

    if (status) {
      query += ` AND sq.status = $${p++}`;
      values.push(status);
    }
    if (quote_type) {
      query += ` AND sq.quote_type = $${p++}`;
      values.push(quote_type);
    }
    if (search) {
      query += ` AND (sq.customer_name ILIKE $${p} OR sq.customer_email ILIKE $${p})`;
      values.push(`%${search}%`);
      p++;
    }

    query += ` ORDER BY sq.created_at DESC LIMIT $${p++} OFFSET $${p++}`;
    values.push(parseInt(limit), parseInt(offset));

    const result = await pool.query(query, values);
    
    // Get counts by status
    const countsResult = await pool.query(`
      SELECT status, COUNT(*) as count FROM sent_quotes GROUP BY status
    `);
    const counts = { total: 0, draft: 0, sent: 0, viewed: 0, signed: 0, declined: 0 };
    countsResult.rows.forEach(row => {
      counts[row.status] = parseInt(row.count);
      counts.total += parseInt(row.count);
    });

    res.json({ success: true, quotes: result.rows, counts });
  } catch (error) {
    console.error('Error fetching sent quotes:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sent-quotes/:id - Get single quote
app.get('/api/sent-quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error fetching quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/sent-quotes - Create new quote
app.post('/api/sent-quotes', async (req, res) => {
  try {
    const {
      customer_name, customer_email, customer_phone, customer_address,
      quote_type, services, subtotal, tax_rate, tax_amount, total, monthly_payment, notes, quote_number
    } = req.body;

    // Look up or create customer
    let customer_id = null;
    if (customer_email) {
      const existingCustomer = await pool.query(
        'SELECT id FROM customers WHERE email = $1',
        [customer_email]
      );
      if (existingCustomer.rows.length > 0) {
        customer_id = existingCustomer.rows[0].id;
      } else {
        // Create new customer
        const newCustomer = await pool.query(
          `INSERT INTO customers (name, email, phone, street, created_at)
           VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP) RETURNING id`,
          [customer_name, customer_email, customer_phone, customer_address]
        );
        customer_id = newCustomer.rows[0].id;
        console.log('Created new customer:', customer_id);
        
        // Trigger Zapier webhook for CopilotCRM sync if configured
        if (process.env.ZAPIER_CUSTOMER_WEBHOOK) {
          try {
            await fetch(process.env.ZAPIER_CUSTOMER_WEBHOOK, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                customer_id,
                name: customer_name,
                email: customer_email,
                phone: customer_phone,
                address: customer_address,
                source: 'quote_generator'
              })
            });
          } catch (e) { console.error('Zapier webhook failed:', e); }
        }
      }
    }

    const sign_token = generateToken();

    const result = await pool.query(
      `INSERT INTO sent_quotes (
        customer_id, customer_name, customer_email, customer_phone, customer_address,
        quote_type, services, subtotal, tax_rate, tax_amount, total, monthly_payment,
        status, sign_token, notes, quote_number, created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'draft', $13, $14, $15, CURRENT_TIMESTAMP)
      RETURNING *`,
      [
        customer_id, customer_name, customer_email, customer_phone, customer_address,
        quote_type || 'regular', JSON.stringify(services), subtotal, tax_rate || 8, tax_amount, total, monthly_payment,
        sign_token, notes, quote_number || null
      ]
    );

    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error creating quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PUT /api/sent-quotes/:id - Update quote
app.put('/api/sent-quotes/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = [];
    const values = [];
    let p = 1;

    const allowedFields = [
      'customer_name', 'customer_email', 'customer_phone', 'customer_address',
      'quote_type', 'services', 'subtotal', 'tax_rate', 'tax_amount', 'total',
      'monthly_payment', 'status', 'notes', 'quote_number'
    ];

    for (const field of allowedFields) {
      if (req.body[field] !== undefined) {
        if (field === 'services') {
          updates.push(`${field} = $${p++}`);
          values.push(JSON.stringify(req.body[field]));
        } else {
          updates.push(`${field} = $${p++}`);
          values.push(req.body[field]);
        }
      }
    }

    if (updates.length === 0) {
      return res.status(400).json({ success: false, error: 'No fields to update' });
    }

    values.push(id);
    const result = await pool.query(
      `UPDATE sent_quotes SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error updating quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/sent-quotes/:id/send - Send quote via email
app.post('/api/sent-quotes/:id/send', async (req, res) => {
  try {
    const { id } = req.params;
    
    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [id]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = quoteResult.rows[0];
    
    if (!quote.customer_email) {
      return res.status(400).json({ success: false, error: 'No customer email address' });
    }

    const signUrl = `${process.env.BASE_URL || 'https://pappas-quote-backend-production.up.railway.app'}/sign-quote.html?token=${quote.sign_token}`;
    const quoteNumber = quote.quote_number || `Q-${quote.id}`;

    // Clean email - detailed but warm tone
    const firstName = quote.customer_name.split(' ')[0];
    
    const emailContent = `
      <h2 style="font-family:'Playfair Display',Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:26px;font-weight:400;text-align:center;">Your Quote is Ready</h2>
      <p style="font-size:15px;color:#374151;line-height:1.6;">Hi ${firstName},</p>
      
      <p style="font-size:15px;color:#374151;line-height:1.6;">Thanks for reaching out to <strong>Pappas & Co. Landscaping</strong>! We've put together a custom quote for your property that includes the scope of work and pricing for your requested services.</p>
      
      <p style="font-size:15px;color:#374151;line-height:1.6;">Click the button below to view your full quote:</p>
      
      <div style="text-align:center;margin:32px 0;">
        <a href="${signUrl}" style="background:#c9dd80;color:#2e403d;padding:16px 48px;text-decoration:none;border-radius:6px;font-weight:bold;font-size:16px;display:inline-block;">View Your Quote →</a>
      </div>
      
      <p style="font-size:15px;color:#374151;line-height:1.6;font-weight:600;">What's Next?</p>
      
      <p style="font-size:15px;color:#374151;line-height:1.6;">From the quote page, you can:</p>
      
      <ul style="color:#374151;font-size:15px;line-height:1.8;padding-left:20px;">
        <li><strong>Accept the quote</strong> to secure your spot on our schedule and sign the service agreement</li>
        <li><strong>Request changes</strong> if you'd like us to adjust the scope of work</li>
      </ul>
      
      <p style="font-size:15px;color:#374151;line-height:1.6;">If you have any questions, feel free to call or text us at <strong>440-886-7318</strong>. We're always happy to help!</p>
      
      <p style="font-size:15px;color:#374151;line-height:1.6;">We look forward to working with you!</p>
    `;

    // Generate branded PDF attachment
    const pdfBytes = await generateQuotePDF(quote);
    let attachments = null;
    
    if (pdfBytes) {
      attachments = [{
        filename: `Quote-${quoteNumber}-${quote.customer_name.replace(/[^a-zA-Z0-9]/g, '-')}.pdf`,
        content: Buffer.from(pdfBytes).toString('base64'),
        type: 'application/pdf'
      }];
    }

    await sendEmail(
      quote.customer_email,
      `Your ${quote.quote_type === 'monthly_plan' ? 'Annual Care Plan' : 'Quote'} from ${COMPANY_NAME}`,
      emailTemplate(emailContent),
      attachments
    );

    // Update status to sent
    await pool.query(
      'UPDATE sent_quotes SET status = $1, sent_at = CURRENT_TIMESTAMP WHERE id = $2',
      ['sent', id]
    );

    res.json({ success: true, message: 'Quote sent successfully' });
  } catch (error) {
    console.error('Error sending quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// DELETE /api/sent-quotes/:id - Delete quote
app.delete('/api/sent-quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM sent_quotes WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    console.error('Error deleting quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sign/:token - Get quote for signing (public)
app.get('/api/sign/:token', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE sign_token = $1', [req.params.token]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = result.rows[0];

    // Ensure quote_views table exists
    await pool.query(`CREATE TABLE IF NOT EXISTS quote_views (
      id SERIAL PRIMARY KEY,
      sent_quote_id INTEGER NOT NULL,
      viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      ip_address VARCHAR(45),
      user_agent TEXT
    )`);

    // Log every view
    const ip = req.headers['x-forwarded-for'] || req.connection?.remoteAddress || '';
    const ua = req.headers['user-agent'] || '';
    await pool.query(
      'INSERT INTO quote_views (sent_quote_id, ip_address, user_agent) VALUES ($1, $2, $3)',
      [quote.id, ip.split(',')[0].trim(), ua]
    );

    // Mark as viewed if first time
    if (quote.status === 'sent' && !quote.viewed_at) {
      await pool.query(
        'UPDATE sent_quotes SET status = $1, viewed_at = CURRENT_TIMESTAMP WHERE id = $2',
        ['viewed', quote.id]
      );
      quote.status = 'viewed';
    }

    // Enrich services with descriptions
    let services = typeof quote.services === 'string' ? JSON.parse(quote.services) : quote.services;
    if (Array.isArray(services)) {
      services = services.map(s => ({
        ...s,
        description: s.description || getServiceDescription(s.name)
      }));
      quote.services = services;
    }

    // Don't expose internal fields
    delete quote.sign_token;
    
    res.json({ success: true, quote });
  } catch (error) {
    console.error('Error fetching quote for signing:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/sign/:token - Accept quote (public) - no signature needed, just name confirmation
app.post('/api/sign/:token', async (req, res) => {
  try {
    const { signed_by_name } = req.body;
    
    if (!signed_by_name) {
      return res.status(400).json({ success: false, error: 'Name confirmation required' });
    }

    const result = await pool.query(
      `UPDATE sent_quotes 
       SET status = 'signed', signed_by_name = $1, signed_at = CURRENT_TIMESTAMP
       WHERE sign_token = $2 AND status IN ('sent', 'viewed')
       RETURNING *`,
      [signed_by_name, req.params.token]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found or already signed' });
    }

    const quote = result.rows[0];
    const quoteNumber = quote.quote_number || `Q-${quote.id}`;

    // Notify Pappas team - CopilotCRM style
    const adminContent = `
      <h2 style="font-family:Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:28px;font-weight:400;text-align:center;">✅ Quote Accepted</h2>
      <p style="color:#64748b;margin:0 0 20px;text-align:center;">Customer will now sign the service agreement.</p>
      <div style="background:#f8fafc;border-radius:8px;padding:24px;">
        <p style="margin:0 0 12px;"><strong>Quote #:</strong> ${quoteNumber}</p>
        <p style="margin:0 0 12px;"><strong>Customer:</strong> ${quote.customer_name}</p>
        <p style="margin:0 0 12px;"><strong>Email:</strong> <a href="mailto:${quote.customer_email}" style="color:#2e403d;">${quote.customer_email}</a></p>
        <p style="margin:0 0 12px;"><strong>Phone:</strong> ${quote.customer_phone}</p>
        <p style="margin:0 0 12px;"><strong>Address:</strong> ${quote.customer_address}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0 0 12px;"><strong>Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
        ${quote.monthly_payment ? `<p style="margin:0 0 12px;"><strong>Monthly:</strong> $${parseFloat(quote.monthly_payment).toFixed(2)}/mo</p>` : ''}
        <p style="margin:0;"><strong>Accepted:</strong> ${new Date().toLocaleString()}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `✅ Quote #${quoteNumber} Accepted: ${quote.customer_name}`, emailTemplate(adminContent, { showSignature: false }));

    // Return success with contract URL for redirect
    const contractUrl = `/sign-contract.html?token=${req.params.token}`;
    res.json({ success: true, message: 'Quote accepted successfully', contractUrl });
  } catch (error) {
    console.error('Error signing quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/sign/:token/decline - Decline quote (public)
app.post('/api/sign/:token/decline', async (req, res) => {
  try {
    const { decline_reason, decline_comments } = req.body;
    
    if (!decline_reason) {
      return res.status(400).json({ success: false, error: 'Reason required' });
    }

    const result = await pool.query(
      `UPDATE sent_quotes 
       SET status = 'declined', decline_reason = $1, decline_comments = $2, declined_at = CURRENT_TIMESTAMP
       WHERE sign_token = $3 AND status NOT IN ('signed', 'contracted', 'declined')
       RETURNING *`,
      [decline_reason, decline_comments || '', req.params.token]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found or already actioned' });
    }

    const quote = result.rows[0];
    const reasonLabels = {
      'price_too_high': 'Price is too high',
      'found_another': 'Found another provider',
      'not_needed': 'No longer need the service',
      'timing': 'Timing doesn\'t work',
      'selling_home': 'Selling the home',
      'diy': 'Going to do it myself',
      'budget': 'Budget constraints',
      'other': 'Other'
    };

    const adminContent = `
      <h2 style="color:#dc2626;margin:0 0 16px;">❌ Quote Declined</h2>
      <div style="background:#fef2f2;border-radius:8px;padding:20px;margin-bottom:20px;">
        <p style="margin:0 0 8px;"><strong>Reason:</strong> ${reasonLabels[decline_reason] || decline_reason}</p>
        ${decline_comments ? `<p style="margin:0;"><strong>Comments:</strong> ${decline_comments}</p>` : ''}
      </div>
      <div style="background:#f8fafc;border-radius:8px;padding:20px;">
        <p style="margin:0 0 8px;"><strong>Customer:</strong> ${quote.customer_name}</p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> ${quote.customer_email}</p>
        <p style="margin:0 0 8px;"><strong>Phone:</strong> ${quote.customer_phone}</p>
        <p style="margin:0 0 8px;"><strong>Address:</strong> ${quote.customer_address}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0;"><strong>Quote Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `❌ Quote Declined: ${quote.customer_name}`, emailTemplate(adminContent, { showSignature: false }));

    res.json({ success: true, message: 'Quote declined' });
  } catch (error) {
    console.error('Error declining quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sent-quotes/:id/views - Full view history for a quote
app.get('/api/sent-quotes/:id/views', async (req, res) => {
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS quote_views (
      id SERIAL PRIMARY KEY, sent_quote_id INTEGER NOT NULL,
      viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      ip_address VARCHAR(45), user_agent TEXT
    )`);
    const views = await pool.query(
      'SELECT id, viewed_at, ip_address, user_agent FROM quote_views WHERE sent_quote_id = $1 ORDER BY viewed_at DESC',
      [req.params.id]
    );
    res.json({ success: true, views: views.rows, total: views.rows.length });
  } catch (error) {
    console.error('Error fetching quote views:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sent-quotes/view-counts - Bulk view counts for all sent quotes
app.get('/api/sent-quotes/view-counts', async (req, res) => {
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS quote_views (
      id SERIAL PRIMARY KEY, sent_quote_id INTEGER NOT NULL,
      viewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      ip_address VARCHAR(45), user_agent TEXT
    )`);
    const counts = await pool.query(
      'SELECT sent_quote_id, COUNT(*) as view_count, MAX(viewed_at) as last_viewed FROM quote_views GROUP BY sent_quote_id'
    );
    const map = {};
    counts.rows.forEach(r => { map[r.sent_quote_id] = { count: parseInt(r.view_count), lastViewed: r.last_viewed }; });
    res.json({ success: true, viewCounts: map });
  } catch (error) {
    console.error('Error fetching view counts:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/sign/:token/request-changes - Request changes to quote (public)
app.post('/api/sign/:token/request-changes', async (req, res) => {
  try {
    const { change_type, change_details } = req.body;
    
    if (!change_type || !change_details) {
      return res.status(400).json({ success: false, error: 'Change type and details required' });
    }

    const result = await pool.query(
      `UPDATE sent_quotes 
       SET status = 'changes_requested', change_type = $1, change_details = $2, changes_requested_at = CURRENT_TIMESTAMP
       WHERE sign_token = $3 AND status NOT IN ('signed', 'contracted', 'declined')
       RETURNING *`,
      [change_type, change_details, req.params.token]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found or already actioned' });
    }

    const quote = result.rows[0];
    const typeLabels = {
      'add_services': 'Add more services',
      'remove_services': 'Remove some services',
      'pricing': 'Question about pricing',
      'schedule': 'Change schedule/frequency',
      'scope': 'Adjust scope of work',
      'other': 'Other'
    };

    const adminContent = `
      <h2 style="color:#f59e0b;margin:0 0 16px;">📝 Change Request</h2>
      <div style="background:#fffbeb;border-radius:8px;padding:20px;margin-bottom:20px;">
        <p style="margin:0 0 8px;"><strong>Type:</strong> ${typeLabels[change_type] || change_type}</p>
        <p style="margin:0;"><strong>Details:</strong></p>
        <p style="margin:8px 0 0;padding:12px;background:white;border-radius:6px;">${change_details.replace(/\n/g, '<br>')}</p>
      </div>
      <div style="background:#f8fafc;border-radius:8px;padding:20px;">
        <p style="margin:0 0 8px;"><strong>Customer:</strong> ${quote.customer_name}</p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> <a href="mailto:${quote.customer_email}" style="color:#2e403d;">${quote.customer_email}</a></p>
        <p style="margin:0 0 8px;"><strong>Phone:</strong> <a href="tel:${quote.customer_phone}" style="color:#2e403d;">${quote.customer_phone}</a></p>
        <p style="margin:0;"><strong>Original Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
      </div>
      <p style="margin-top:20px;"><a href="https://pappas-quote-backend-production.up.railway.app/sent-quotes.html" style="background:#f59e0b;color:white;padding:12px 24px;text-decoration:none;border-radius:6px;display:inline-block;font-weight:600;">Review Quote</a></p>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `📝 Change Request: ${quote.customer_name}`, emailTemplate(adminContent, { showSignature: false }));

    res.json({ success: true, message: 'Changes requested' });
  } catch (error) {
    console.error('Error requesting changes:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// CONTRACT SIGNING ENDPOINTS
// ═══════════════════════════════════════════════════════════

// POST /api/sent-quotes/:id/sign-contract - Sign the service agreement
app.post('/api/sent-quotes/:id/sign-contract', async (req, res) => {
  try {
    const { id } = req.params;
    const { signature_data, signature_type, printed_name, consent_given } = req.body;

    if (!signature_data || !printed_name || !consent_given) {
      return res.status(400).json({ success: false, error: 'Missing required fields' });
    }

    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [id]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = quoteResult.rows[0];
    if (quote.contract_signed_at) {
      return res.status(400).json({ success: false, error: 'Contract already signed' });
    }

    const signerIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';

    const updateResult = await pool.query(`
      UPDATE sent_quotes SET
        contract_signed_at = CURRENT_TIMESTAMP,
        contract_signature_data = $1,
        contract_signature_type = $2,
        contract_signer_ip = $3,
        contract_signer_name = $4,
        status = 'contracted'
      WHERE id = $5
      RETURNING *
    `, [signature_data, signature_type, signerIp, printed_name, id]);

    const updatedQuote = updateResult.rows[0];
    const signedDate = new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
    const signedTime = new Date().toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });

    let servicesText = 'See agreement for details';
    let servicesHtml = '';
    try {
      const services = typeof updatedQuote.services === 'string' ? JSON.parse(updatedQuote.services) : updatedQuote.services;
      if (Array.isArray(services)) {
        servicesText = services.map(s => s.name || s).join(', ');
        servicesHtml = services.map(s => `<li style="margin:6px 0;">${s.name} - $${parseFloat(s.amount).toFixed(2)}</li>`).join('');
      }
    } catch (e) {}

    // ── Auto-create a scheduled job from the signed contract ──
    try {
      const nextMonday = new Date();
      nextMonday.setDate(nextMonday.getDate() + ((8 - nextMonday.getDay()) % 7 || 7));
      const jobDate = nextMonday.toISOString().split('T')[0];

      await pool.query(
        `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, status, estimated_duration)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          jobDate,
          updatedQuote.customer_name,
          updatedQuote.customer_id || null,
          servicesText || 'Landscaping Services',
          updatedQuote.quote_type === 'monthly_plan' ? 'weekly' : 'one-time',
          parseFloat(updatedQuote.total) || 0,
          updatedQuote.customer_address || '',
          updatedQuote.customer_phone || '',
          `Auto-created from signed contract. Quote #${updatedQuote.quote_number || updatedQuote.id}`,
          'pending',
          60
        ]
      );
      console.log(`Auto-created job for ${updatedQuote.customer_name} on ${jobDate}`);
    } catch (jobErr) {
      console.error('Failed to auto-create job from contract:', jobErr.message);
      // Don't fail the contract signing if job creation fails
    }

    // Generate the contract HTML attachment (matches Canva template style)
    const quoteNumber = updatedQuote.quote_number || 'Q-' + updatedQuote.id;
    const isDrawnSignature = signature_data && signature_data.startsWith('data:image');
    const signatureHtml = isDrawnSignature 
      ? `<img src="${signature_data}" style="max-height:60px;margin:8px 0;" alt="Signature">`
      : `<p style="font-family:'Brush Script MT',cursive;font-size:28px;margin:8px 0;color:#2e403d;">${signature_data || printed_name}</p>`;

    const contractHtml = `<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Service Agreement - ${updatedQuote.customer_name}</title>
<style>
body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 40px; color: #333; font-size: 11px; line-height: 1.5; }
.header { display: flex; justify-content: space-between; align-items: center; padding-bottom: 16px; border-bottom: 4px solid #c9dd80; margin-bottom: 28px; }
.logo img { max-height: 56px; max-width: 180px; display: block; }
.contact-info { text-align: right; font-size: 10.5px; color: #666; line-height: 1.7; }
h1 { text-align: center; color: #2e403d; font-size: 22px; margin: 0 0 8px; font-weight: 700; letter-spacing: 0.5px; }
.intro { text-align: center; color: #666; font-size: 11px; margin-bottom: 20px; }
.parties { display: flex; gap: 40px; margin: 20px 0 30px; }
.party { flex: 1; }
.party-label { font-weight: bold; color: #333; margin-bottom: 8px; }
h2 { color: #2e403d; font-size: 13px; margin: 22px 0 10px; padding-bottom: 4px; border-bottom: 2px solid #c9dd80; }
.accent-bar { height: 4px; background: linear-gradient(90deg, #c9dd80, #bef264); margin: 0 0 24px; }
.section { margin-bottom: 16px; }
.section p { margin: 6px 0; text-align: justify; }
.section ul { margin: 8px 0 8px 20px; padding: 0; }
.section li { margin: 4px 0; }
.highlight { background: #f8fafc; border-left: 3px solid #84cc16; padding: 10px 12px; margin: 10px 0; font-size: 10px; }
.signature-section { margin-top: 40px; border: 2px solid #2e403d; border-radius: 8px; padding: 24px; background: #fafafa; }
.sig-row { display: flex; gap: 40px; margin-top: 20px; }
.sig-block { flex: 1; }
.sig-label { font-weight: bold; margin-bottom: 8px; }
.sig-line { border-bottom: 1px solid #333; height: 40px; margin-bottom: 4px; }
.footer { margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; text-align: center; font-size: 10px; color: #666; }
@media print { body { padding: 20px; } }
</style>
</head>
<body>
<div class="header">
  <div class="logo"><img src="${LOGO_URL}" alt="Pappas &amp; Co. Landscaping"></div>
  <div class="contact-info">pappaslandscaping.com<br>hello@pappaslandscaping.com<br>(440) 886-7318</div>
</div>

<h1>Service Agreement</h1>
<p class="intro">This Agreement is made effective on the date the Client accepts a<br>quote from Pappas & Co. Landscaping (the "Effective Date") between:</p>

<div class="parties">
  <div class="party">
    <p class="party-label">Contractor:</p>
    <p>Pappas & Co. Landscaping<br>PO Box 770057<br>Lakewood, OH 44107</p>
  </div>
  <div class="party">
    <p class="party-label">Client:</p>
    <p><strong>${updatedQuote.customer_name}</strong><br>${updatedQuote.customer_address || ''}<br>${updatedQuote.customer_email || ''}<br>${updatedQuote.customer_phone || ''}</p>
  </div>
</div>

<h2>Services & Pricing (Quote #${quoteNumber})</h2>
<div class="section">
  <ul>${servicesHtml}</ul>
  <p><strong>Total: $${parseFloat(updatedQuote.total).toFixed(2)}</strong>${updatedQuote.monthly_payment ? ` (Monthly: $${parseFloat(updatedQuote.monthly_payment).toFixed(2)}/mo)` : ''}</p>
</div>

<h2>I. Scope of Agreement</h2>
<div class="section">
  <p><strong>A. Associated Quote:</strong> This Agreement is directly tied to Quote/Proposal Number: <strong>${quoteNumber}</strong>.</p>
  <p><strong>B. Scope of Services:</strong> The Contractor agrees to provide services at the Client Service Address as detailed in the Proposal, which outlines the specific services, schedule, and pricing. This Proposal is hereby incorporated into and made a part of this Agreement.</p>
  <p><strong>C. Additional Work:</strong> Additional work requested by the Client outside of the scope defined in the Proposal will be performed at an additional cost, requiring a separate, pre-approved quote.</p>
</div>

<h2>II. Terms and Renewal</h2>
<div class="section">
  <p><strong>A. Term:</strong> This Agreement begins on the Effective Date and remains in effect until canceled as outlined in Section IX.</p>
  <p><strong>B. Automatic Renewal:</strong> The Agreement automatically renews each year at the start of the new season, which begins in <strong>March</strong>, unless canceled in writing by either party at least <strong>30 days before the new season begins</strong>.</p>
</div>

<h2>III. Payment Terms</h2>
<div class="section">
  <p>A. Mowing Services Invoicing:</p>
  <ul>
    <li><strong>Per-Service Mowing:</strong> Invoices will be sent on the <strong>final day of each month</strong>.</li>
    <li><strong>Monthly Mowing Contracts:</strong> Invoices will be sent on the <strong>first day of each month</strong>.</li>
  </ul>
  <p><strong>B. All Other Services Invoicing:</strong> Invoices will be sent upon job completion.</p>
  <p><strong>C. Due Date:</strong> Payments are due upon receipt of the invoice.</p>
  <p><strong>D. Accepted Payment Methods:</strong> Major credit cards, Zelle, cash, checks, money orders, and bank transfers.</p>
  <p><strong>E. Fuel Surcharge:</strong> A small flat-rate fuel surcharge will be added to each invoice to help offset transportation-related costs.</p>
  <p><strong>F. Returned Checks:</strong> A $25 fee will be applied for any returned checks.</p>
</div>

<h2>IV. Card on File Authorization and Fees</h2>
<div class="section">
  <p>By placing a credit or debit card on file, the Client authorizes Pappas & Co. Landscaping to charge that card for any services rendered under this Agreement, including applicable fees and surcharges.</p>
  <p><strong>Processing Fee:</strong> A processing fee of <strong>2.9% + $0.30</strong> applies to each successful domestic card transaction.</p>
  <div class="highlight">
    <strong>For Monthly Service Contracts with card-on-file billing:</strong> If a scheduled payment fails, the Client will be notified and given 5 business days to update payment information. If payment is not resolved, the account will revert to per-service invoicing and standard late fee terms (Section V) will apply.
  </div>
</div>

<h2>V. Late Fees and Suspension of Service</h2>
<div class="section">
  <p>Pappas & Co. Landscaping incurs upfront costs for labor, materials, and equipment. Late payments disrupt business operations, and the following fees and policies apply:</p>
  <ul>
    <li><strong>30-Day Late Fee:</strong> A <strong>10% late fee</strong> will be applied if payment is not received within 30 days of the invoice date.</li>
    <li><strong>Recurring Late Fee:</strong> An additional <strong>5% late fee</strong> will be applied for each additional 30-day period past due.</li>
    <li><strong>Service Suspension and Collections:</strong> If payment is <strong>not received within 60 days</strong>, services will be <strong>suspended</strong>, and Pappas & Co. Landscaping reserves the right to initiate collection proceedings.</li>
  </ul>
</div>

<h2>VI. Client Responsibilities</h2>
<div class="section">
  <p>The Client agrees to the following:</p>
  <ul>
    <li><strong>Accessibility:</strong> All gates must be unlocked, and service areas must be accessible on the scheduled service day.</li>
    <li><strong>Return Trip Fee:</strong> A <strong>$25 return trip fee</strong> may be charged if rescheduling is needed due to Client-related access issues.</li>
    <li><strong>Property Clearance:</strong> The property must be free of hazards, obstacles, and pre-existing damage.</li>
    <li><strong>Personal Items:</strong> Our crew may move personal items if necessary to perform work, but <strong>we are not responsible for any damage caused by moving such items</strong>.</li>
    <li><strong>Pet Waste:</strong> All dog feces must be picked up prior to service. A <strong>$15 cleanup fee</strong> may be added if pet waste is present.</li>
    <li><strong>Underground Infrastructure:</strong> Pappas & Co. Landscaping is not liable for damage to underground utilities, irrigation lines, or invisible fences <strong>unless they are clearly marked and disclosed in advance</strong> by the Client.</li>
  </ul>
</div>

<h2>VII. Lawn/Plant Installs (If Applicable)</h2>
<div class="section">
  <p>The Client is responsible for watering newly installed lawns and plants <strong>twice daily or as recommended</strong> to ensure proper growth. Pappas & Co. Landscaping is <strong>not responsible</strong> for plant or lawn failure due to lack of watering or improper care after installation.</p>
</div>

<h2>VIII. Weather and Materials</h2>
<div class="section">
  <p><strong>A. Materials and Equipment:</strong> Pappas & Co. Landscaping will supply all materials, tools, and equipment necessary to perform the agreed-upon services unless specified otherwise.</p>
  <p><strong>B. Weather Disruptions:</strong> If inclement weather prevents services, Pappas & Co. Landscaping will make <strong>reasonable efforts</strong> to complete the service the following business day. Service on the next day is <strong>not guaranteed</strong> and will be rescheduled based on availability. Refunds or credits will not be issued for weather-related delays unless the service is permanently canceled.</p>
</div>

<h2>IX. Cancellation and Termination</h2>
<div class="section">
  <p><strong>A. Non-Renewal:</strong> To stop the automatic renewal of this Agreement, the Client must provide <strong>written notice at least 30 days before your renewal date</strong> (which occurs in March).</p>
  <p><strong>B. Mid-Season Cancellation by Client:</strong> To cancel service mid-season, the Client must provide <strong>15 days' written notice</strong> at any time. Services will continue through the notice period, and the final invoice will include any completed work. No refunds are given for prepaid services or unused portions of seasonal contracts.</p>
  <p><strong>C. Termination by Contractor:</strong> Pappas & Co. Landscaping may cancel service at any time with <strong>15 days' notice</strong>.</p>
</div>

<h2>X. Liability, Insurance, and Quality</h2>
<div class="section">
  <p><strong>A. Quality of Workmanship:</strong> Pappas & Co. Landscaping will perform all services with due care and in accordance with industry standards.</p>
  <ul>
    <li>If defects or deficiencies in workmanship occur, the Client must notify Pappas & Co. Landscaping <strong>within 7 days</strong> of service completion. If the issue is due to improper workmanship, it will be corrected at no additional cost.</li>
    <li>Issues resulting from <strong>natural wear, environmental conditions, or improper client maintenance</strong> are not covered under this clause.</li>
  </ul>
  <p><strong>B. Independent Contractor:</strong> Pappas & Co. Landscaping is an independent contractor and is not an employee, partner, or agent of the Client.</p>
  <p><strong>C. Indemnification:</strong> Pappas & Co. Landscaping agrees to indemnify and hold harmless the Client from claims arising directly from its performance of work.</p>
  <p><strong>D. Limitation of Liability:</strong> The total liability of Pappas & Co. Landscaping for any claim shall <strong>not exceed the total amount paid by the Client</strong> under this agreement. Pappas & Co. Landscaping is <strong>not liable</strong> for indirect, incidental, consequential, or special damages.</p>
  <p><strong>E. Insurance:</strong> Pappas & Co. Landscaping carries general liability insurance, automobile liability insurance, and workers' compensation insurance as required by law.</p>
  <p><strong>F. Force Majeure:</strong> Neither party shall be held liable for delays or failure in performance caused by events beyond their reasonable control.</p>
</div>

<h2>XI. Governing Law and Dispute Resolution</h2>
<div class="section">
  <p><strong>A. Jurisdiction:</strong> This agreement shall be governed by the laws of the <strong>State of Ohio</strong>. Any disputes shall be resolved in the county courts of <strong>Cuyahoga County, Ohio</strong>.</p>
  <p><strong>B. Dispute Resolution:</strong> Any disputes will first be subject to <strong>good-faith negotiations</strong> between the parties. If a resolution cannot be reached, the dispute may be subject to <strong>mediation or arbitration</strong> before legal action is pursued.</p>
</div>

<h2>XII. Acceptance of Agreement</h2>
<div class="section">
  <p>By signing below, the parties acknowledge that they have read, understand, and agree to the terms and conditions of this Landscaping Services Agreement and the incorporated Proposal/Quote.</p>
</div>

<div class="signature-section">
  <div class="sig-row">
    <div class="sig-block">
      <p class="sig-label">Pappas & Co. Landscaping:</p>
      <p style="font-family:'Brush Script MT',cursive;font-size:24px;margin:8px 0;">Timothy Pappas</p>
      <div class="sig-line"></div>
      <p>Name: <strong>Timothy Pappas</strong></p>
    </div>
    <div class="sig-block">
      <p class="sig-label">Client:</p>
      ${signatureHtml}
      <div class="sig-line"></div>
      <p>Name: <strong>${printed_name}</strong></p>
      <p>Date: <strong>${signedDate}</strong></p>
    </div>
  </div>
</div>

<div class="footer">
  <div class="accent-bar" style="margin:15px 0;"></div>
</div>
</body>
</html>`;

    // Generate PDF from template
    console.log('📄 Attempting to generate contract PDF for quote', id);
    let pdfBytes = null;
    try {
      pdfBytes = await generateContractPDF(updatedQuote, signature_data, printed_name, signedDate);
      if (pdfBytes) {
        console.log('✅ Contract PDF generated successfully, size:', pdfBytes.length, 'bytes');
      } else {
        console.log('⚠️ generateContractPDF returned null');
      }
    } catch (pdfError) {
      console.error('❌ PDF generation threw an error:', pdfError.message);
      console.error('Stack:', pdfError.stack);
    }
    
    // Create attachment - use PDF if available, otherwise HTML
    let contractAttachment;
    if (pdfBytes && pdfBytes.length > 0) {
      contractAttachment = {
        filename: `Service-Agreement-${quoteNumber}.pdf`,
        content: Buffer.from(pdfBytes).toString('base64'),
        type: 'application/pdf'
      };
      console.log('📎 Using PDF attachment');
    } else {
      // Fallback to HTML if PDF generation fails
      console.log('⚠️ Falling back to HTML attachment');
      contractAttachment = {
        filename: `Service-Agreement-${quoteNumber}.html`,
        content: Buffer.from(contractHtml).toString('base64'),
        type: 'text/html'
      };
    }

    // Email to customer with contract signed confirmation
    if (updatedQuote.customer_email) {
      const firstName = updatedQuote.customer_name.split(' ')[0];
      const customerContent = `
        <h2 style="font-family:'Playfair Display',Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:26px;font-weight:400;text-align:center;">Welcome to the Pappas Family!</h2>
        
        <p style="font-size:15px;color:#374151;line-height:1.6;">Hi ${firstName},</p>
        
        <p style="font-size:15px;color:#374151;line-height:1.6;">Thank you for signing your service agreement! We're excited to have you as a customer.</p>
        
        <p style="background:#e8f5e9;padding:16px;border-radius:8px;color:#166534;font-size:14px;margin:24px 0;">📎 Your signed service agreement is attached to this email.</p>
        
        <div style="background:#f8fafc;border-radius:12px;padding:24px;margin:24px 0;">
          <h3 style="font-family:'Playfair Display',Georgia,serif;margin:0 0 16px;color:#2e403d;font-size:18px;font-weight:400;border-bottom:1px solid #e2e8f0;padding-bottom:12px;">Agreement Details</h3>
          <table style="width:100%;border-collapse:collapse;">
            <tr><td style="padding:10px 0;color:#64748b;font-size:14px;">Quote Number</td><td style="padding:10px 0;color:#1e293b;font-size:14px;text-align:right;font-weight:600;">${quoteNumber}</td></tr>
            <tr><td style="padding:10px 0;color:#64748b;font-size:14px;">Service Address</td><td style="padding:10px 0;color:#1e293b;font-size:14px;text-align:right;">${updatedQuote.customer_address}</td></tr>
            <tr><td style="padding:10px 0;color:#64748b;font-size:14px;">Services</td><td style="padding:10px 0;color:#1e293b;font-size:14px;text-align:right;">${servicesText}</td></tr>
            <tr style="border-top:2px solid #e2e8f0;"><td style="padding:16px 0 10px;color:#64748b;font-size:14px;">Total</td><td style="padding:16px 0 10px;color:#2e403d;font-size:22px;text-align:right;font-weight:700;">$${parseFloat(updatedQuote.total).toFixed(2)}</td></tr>
            ${updatedQuote.monthly_payment ? `<tr><td style="padding:10px 0;color:#64748b;font-size:14px;">Monthly Payment</td><td style="padding:10px 0;color:#2e403d;font-size:16px;text-align:right;font-weight:600;">$${parseFloat(updatedQuote.monthly_payment).toFixed(2)}/mo</td></tr>` : ''}
          </table>
        </div>
        
        <p style="font-size:15px;color:#374151;line-height:1.6;"><strong>What's next?</strong> If you haven't already, please add a payment method in your customer portal to complete your setup. Once that's done, we'll add you to the schedule!</p>
        
        <p style="font-size:15px;color:#374151;line-height:1.6;">We can't wait to get started!</p>
      `;
      await sendEmail(updatedQuote.customer_email, `You're All Set! Welcome to Pappas & Co. Landscaping`, emailTemplate(customerContent), [contractAttachment]);
    }

    // Email to admin - matches Quote Accepted style
    const adminContent = `
      <h2 style="font-family:Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:28px;font-weight:400;text-align:center;">🎉 Contract Signed</h2>
      <p style="color:#64748b;margin:0 0 20px;text-align:center;">Ready to schedule services.</p>
      <div style="background:#f8fafc;border-radius:8px;padding:24px;">
        <p style="margin:0 0 12px;"><strong>Quote #:</strong> ${quoteNumber}</p>
        <p style="margin:0 0 12px;"><strong>Customer:</strong> ${updatedQuote.customer_name}</p>
        <p style="margin:0 0 12px;"><strong>Email:</strong> <a href="mailto:${updatedQuote.customer_email}" style="color:#2e403d;">${updatedQuote.customer_email}</a></p>
        <p style="margin:0 0 12px;"><strong>Phone:</strong> ${updatedQuote.customer_phone}</p>
        <p style="margin:0 0 12px;"><strong>Address:</strong> ${updatedQuote.customer_address}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0 0 12px;"><strong>Services:</strong> ${servicesText}</p>
        <p style="margin:0 0 12px;"><strong>Total:</strong> $${parseFloat(updatedQuote.total).toFixed(2)}</p>
        ${updatedQuote.monthly_payment ? `<p style="margin:0 0 12px;"><strong>Monthly:</strong> $${parseFloat(updatedQuote.monthly_payment).toFixed(2)}/mo</p>` : ''}
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0 0 12px;"><strong>Signed by:</strong> ${printed_name}</p>
        <p style="margin:0;"><strong>Signed:</strong> ${signedDate} at ${signedTime}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `🎉 Contract Signed: ${updatedQuote.customer_name}`, emailTemplate(adminContent, { showSignature: false }));

    // Trigger Zapier webhook for CopilotCRM customer portal invite
    if (process.env.ZAPIER_CONTRACT_SIGNED_WEBHOOK) {
      try {
        await fetch(process.env.ZAPIER_CONTRACT_SIGNED_WEBHOOK, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            event: 'contract_signed',
            customer_name: updatedQuote.customer_name,
            customer_email: updatedQuote.customer_email,
            customer_phone: updatedQuote.customer_phone,
            customer_address: updatedQuote.customer_address,
            quote_number: updatedQuote.quote_number || 'Q-' + updatedQuote.id,
            total: updatedQuote.total,
            monthly_payment: updatedQuote.monthly_payment,
            services: servicesText,
            signed_by: printed_name,
            signed_at: new Date().toISOString()
          })
        });
        console.log('✅ Zapier webhook sent for contract signed');
      } catch (e) { 
        console.error('Zapier contract webhook failed:', e); 
      }
    }

    // Stop quote follow-up sequence since quote was accepted
    try {
      const quoteNum = updatedQuote.quote_number || 'Q-' + updatedQuote.id;
      await pool.query(`
        UPDATE quote_followups 
        SET status = 'accepted', stopped_at = NOW(), stopped_reason = 'accepted', stopped_by = 'contract_signed', updated_at = NOW()
        WHERE (quote_number = $1 OR customer_email = $2) AND status = 'pending'
      `, [quoteNum, updatedQuote.customer_email]);
      console.log(`✅ Follow-up sequence stopped for accepted quote ${quoteNum}`);
    } catch (followupErr) {
      console.log('Follow-up stop skipped:', followupErr.message);
    }

    console.log(`📝 Contract signed for quote ${id} by ${printed_name}`);
    res.json({ success: true, quote: updatedQuote });

  } catch (error) {
    console.error('Error signing contract:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sent-quotes/:id/contract-status - Check contract status
app.get('/api/sent-quotes/:id/contract-status', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, quote_number, status, contract_signed_at, contract_signer_name, contract_signature_type
      FROM sent_quotes WHERE id = $1
    `, [req.params.id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = result.rows[0];
    res.json({
      success: true,
      contract_signed: !!quote.contract_signed_at,
      signed_at: quote.contract_signed_at,
      signer_name: quote.contract_signer_name,
      signature_type: quote.contract_signature_type,
      status: quote.status
    });

  } catch (error) {
    console.error('Error getting contract status:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sent-quotes/:id/download-pdf - Download signed contract PDF
app.get('/api/sent-quotes/:id/download-pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    
    const quote = result.rows[0];
    
    if (!quote.contract_signed_at) {
      return res.status(400).json({ success: false, error: 'Contract not yet signed' });
    }
    
    const signedDate = new Date(quote.contract_signed_at).toLocaleDateString();
    const pdfBytes = await generateContractPDF(
      quote, 
      quote.contract_signature_data, 
      quote.contract_signer_name, 
      signedDate
    );
    
    if (!pdfBytes) {
      return res.status(500).json({ success: false, error: 'PDF generation failed - template may be missing' });
    }
    
    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="Service-Agreement-${quoteNumber}.pdf"`);
    res.send(Buffer.from(pdfBytes));
    
  } catch (error) {
    console.error('Error downloading PDF:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sent-quotes/:id/download-quote - Download quote PDF
app.get('/api/sent-quotes/:id/download-quote', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    const quote = result.rows[0];
    
    // Return quote data for client-side PDF generation
    res.json({ success: true, quote, type: 'quote' });
  } catch (error) {
    console.error('Error downloading quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/sent-quotes/:id/download-contract - Download signed contract PDF
app.get('/api/sent-quotes/:id/download-contract', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    const quote = result.rows[0];
    
    if (!quote.contract_signed_at) {
      return res.status(400).json({ success: false, error: 'Contract not yet signed' });
    }
    
    // Return contract data for client-side PDF generation
    res.json({ success: true, quote, type: 'contract' });
  } catch (error) {
    console.error('Error downloading contract:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// TWILIOCONNECT APP API
// ═══════════════════════════════════════════════════════════

// Login for TwilioConnect app
app.post('/api/app/login', async (req, res) => {
  const { email, password } = req.body;
  const authorizedUsers = [
    { email: 'hello@pappaslandscaping.com', password: 'PappasPhone2026!', name: 'Theresa Pappas', phone: '+12163150451' },
    { email: 'montague.theresa@gmail.com', password: 'PappasPhone2026!', name: 'Theresa Pappas', phone: '+12163150451' },
    { email: 'tim@pappaslandscaping.com', password: 'PappasPhone2026!', name: 'Tim Pappas', phone: '+12169057395' },
  ];
  const user = authorizedUsers.find(u => u.email.toLowerCase() === email.toLowerCase() && u.password === password);
  if (!user) return res.status(401).json({ message: 'Invalid email or password' });
  const token = jwt.sign({ email: user.email, name: user.name, phone: user.phone }, JWT_SECRET, { expiresIn: '30d' });
  res.json({ token, name: user.name, email: user.email });
});

// Initiate outbound call
app.post('/api/app/calls/outbound', authenticateToken, async (req, res) => {
  const { to, fromNumber } = req.body;
  const userPhone = req.user.phone;
  try {
    let contactName = null;
    const cleanedTo = to.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`SELECT customer_name FROM sent_quotes WHERE REPLACE(REPLACE(REPLACE(customer_phone, '-', ''), '(', ''), ')', '') LIKE $1 ORDER BY created_at DESC LIMIT 1`, [`%${cleanedTo}`]);
    if (customerResult.rows.length > 0) contactName = customerResult.rows[0].customer_name;
    
    // Determine which Twilio number to call from
    let callFromNumber = TWILIO_PHONE_NUMBER; // Default
    if (fromNumber) {
      const normalizedFrom = fromNumber.replace(/\D/g, '').slice(-10);
      if (TWILIO_NUMBERS[normalizedFrom]) {
        callFromNumber = TWILIO_NUMBERS[normalizedFrom];
      }
    }
    
    const call = await twilioClient.calls.create({
      url: `https://pappas-quote-backend-production.up.railway.app/api/app/calls/connect?to=${encodeURIComponent(to)}&from=${encodeURIComponent(callFromNumber)}`,
      to: userPhone,
      from: callFromNumber,
      statusCallback: `https://pappas-quote-backend-production.up.railway.app/api/app/calls/status-callback`,
      statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
    });
    console.log(`📞 Outbound call initiated: ${call.sid} to ${to} via ${userPhone} from ${callFromNumber}`);
    res.json({ success: true, callSid: call.sid, contactName });
  } catch (error) {
    console.error('Outbound call error:', error);
    res.status(500).json({ message: 'Failed to initiate call', error: error.message });
  }
});

// TwiML to connect the call
app.all('/api/app/calls/connect', (req, res) => {
  const to = req.query.to || req.body.To;
  const from = req.query.from || req.body.From || TWILIO_PHONE_NUMBER;
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  twiml.say({ voice: 'alice' }, 'Connecting your call.');
  twiml.dial({ callerId: from }).number(to);
  res.type('text/xml');
  res.send(twiml.toString());
});

// Get call status
app.get('/api/app/calls/status/:callSid', authenticateToken, async (req, res) => {
  try {
    const call = await twilioClient.calls(req.params.callSid).fetch();
    res.json({ status: call.status, duration: call.duration, direction: call.direction });
  } catch (error) {
    console.error('Call status error:', error);
    res.status(500).json({ message: 'Failed to get call status' });
  }
});

// End call
app.post('/api/app/calls/end/:callSid', authenticateToken, async (req, res) => {
  try {
    await twilioClient.calls(req.params.callSid).update({ status: 'completed' });
    res.json({ success: true });
  } catch (error) {
    console.error('End call error:', error);
    res.status(500).json({ message: 'Failed to end call' });
  }
});

// Hold call
app.post('/api/app/calls/hold/:callSid', authenticateToken, async (req, res) => {
  try {
    if (req.body.hold) {
      await twilioClient.calls(req.params.callSid).update({
        url: 'https://pappas-quote-backend-production.up.railway.app/api/app/calls/hold-music',
        method: 'POST',
      });
    }
    res.json({ success: true, onHold: req.body.hold });
  } catch (error) {
    console.error('Hold call error:', error);
    res.status(500).json({ message: 'Failed to hold call' });
  }
});

// Hold music
app.all('/api/app/calls/hold-music', (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  twiml.say({ voice: 'alice' }, 'Please hold.');
  twiml.play({ loop: 10 }, 'https://api.twilio.com/cowbell.mp3');
  res.type('text/xml');
  res.send(twiml.toString());
});

// Recent calls
app.get('/api/app/calls/recent', authenticateToken, async (req, res) => {
  const limit = parseInt(req.query.limit) || 5;
  try {
    const calls = await twilioClient.calls.list({ limit });
    const enrichedCalls = await Promise.all(calls.map(async (call) => {
      const phoneNumber = call.direction === 'inbound' ? call.from : call.to;
      const cleanedPhone = phoneNumber.replace(/\D/g, '').slice(-10);
      let contactName = null;
     const customerResult = await pool.query(`SELECT name FROM customers WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 LIMIT 1`, [`%${cleanedPhone}`]);
if (customerResult.rows.length > 0) contactName = customerResult.rows[0].name;
      return { id: call.sid, phoneNumber, direction: call.direction, status: call.status, duration: parseInt(call.duration) || 0, timestamp: call.startTime, contactName };
    }));
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const todayCalls = enrichedCalls.filter(c => new Date(c.timestamp) >= today).length;
    const missedCalls = enrichedCalls.filter(c => c.status === 'no-answer' || c.status === 'busy' || c.status === 'canceled').length;
    res.json({ calls: enrichedCalls, todayCalls, missedCalls });
  } catch (error) {
    console.error('Recent calls error:', error);
    res.status(500).json({ message: 'Failed to fetch calls', calls: [], todayCalls: 0, missedCalls: 0 });
  }
});

// Call history
app.get('/api/app/calls/history', authenticateToken, async (req, res) => {
  try {
    const calls = await twilioClient.calls.list({ limit: 100 });
    const enrichedCalls = await Promise.all(calls.map(async (call) => {
      const phoneNumber = call.direction === 'inbound' ? call.from : call.to;
      const cleanedPhone = phoneNumber.replace(/\D/g, '').slice(-10);
      let contactName = null;
      const customerResult = await pool.query(`SELECT name FROM customers WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 LIMIT 1`, [`%${cleanedPhone}`]);
      if (customerResult.rows.length > 0) contactName = customerResult.rows[0].name;
      
      // Include from/to numbers for filtering by Twilio line
      return { 
        id: call.sid, 
        phoneNumber, 
        direction: call.direction, 
        status: call.status, 
        duration: parseInt(call.duration) || 0, 
        timestamp: call.startTime, 
        contactName,
        from_number: call.from,  // The originating number
        to_number: call.to       // The destination number
      };
    }));
    res.json({ calls: enrichedCalls });
  } catch (error) {
    console.error('Call history error:', error);
    res.status(500).json({ message: 'Failed to fetch call history', calls: [] });
  }
});

// Status callback (no auth - called by Twilio)
app.post('/api/app/calls/status-callback', (req, res) => {
  const { CallSid, CallStatus, CallDuration, From, To } = req.body;
  console.log(`📞 Call ${CallSid}: ${CallStatus} (${CallDuration || 0}s) From: ${From} To: ${To}`);
  res.sendStatus(200);
});

// Get customers
app.get('/api/app/customers', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, name, COALESCE(NULLIF(mobile, ''), phone) as phone, email, street as address, city, state
      FROM customers 
      WHERE (phone IS NOT NULL AND phone != '' AND TRIM(phone) != '')
         OR (mobile IS NOT NULL AND mobile != '' AND TRIM(mobile) != '')
      ORDER BY name ASC
    `);
    res.json({ customers: result.rows });
  } catch (error) {
    console.error('Customers error:', error);
    res.status(500).json({ message: 'Failed to fetch customers', customers: [] });
  }
});

// Register device for push notifications
app.post('/api/app/devices/register', authenticateToken, async (req, res) => {
  const { pushToken, platform } = req.body;
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS app_devices (id SERIAL PRIMARY KEY, email VARCHAR(255) NOT NULL, push_token TEXT NOT NULL, platform VARCHAR(50), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, UNIQUE(email, platform))`);
    await pool.query(`INSERT INTO app_devices (email, push_token, platform, updated_at) VALUES ($1, $2, $3, CURRENT_TIMESTAMP) ON CONFLICT (email, platform) DO UPDATE SET push_token = $2, updated_at = CURRENT_TIMESTAMP`, [req.user.email, pushToken, platform]);
    console.log(`📱 Device registered for ${req.user.email} (${platform})`);
    res.json({ success: true });
  } catch (error) {
    console.error('Device registration error:', error);
    res.status(500).json({ message: 'Failed to register device' });
  }
});
// ═══════════════════════════════════════════════════════════════════════════════
// SMS MESSAGES - Add this to your server.js before the "GENERAL ROUTES" section
// ═══════════════════════════════════════════════════════════════════════════════

// Create messages table if it doesn't exist
async function createMessagesTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      twilio_sid VARCHAR(100) UNIQUE,
      direction VARCHAR(20) NOT NULL,
      from_number VARCHAR(20) NOT NULL,
      to_number VARCHAR(20) NOT NULL,
      body TEXT,
      media_urls TEXT[],
      status VARCHAR(50),
      customer_id INTEGER REFERENCES customers(id),
      read BOOLEAN DEFAULT false,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_number)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_number)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at DESC)`);
}
createMessagesTable();

// Send Expo Push Notification
async function sendPushNotification(expoPushToken, title, body, data = {}) {
  try {
    const response = await fetch('https://exp.host/--/api/v2/push/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to: expoPushToken, sound: 'default', title, body, data }),
    });
    console.log('📲 Push sent to', expoPushToken.substring(0, 20) + '...');
  } catch (error) {
    console.error('Push error:', error.message);
  }
}

// Send push to all registered devices
async function sendPushToAllDevices(title, body, data = {}) {
  try {
    const devices = await pool.query('SELECT push_token FROM app_devices WHERE push_token IS NOT NULL');
    for (const device of devices.rows) {
      if (device.push_token) await sendPushNotification(device.push_token, title, body, data);
    }
  } catch (error) {
    console.error('Push to all devices error:', error.message);
  }
}

// Twilio SMS Webhook - Receives incoming texts
app.post('/api/sms/webhook', async (req, res) => {
  const { MessageSid, From, To, Body, NumMedia } = req.body;
  
  try {
    // Get media URLs if any
    const mediaUrls = [];
    const numMedia = parseInt(NumMedia) || 0;
    for (let i = 0; i < numMedia; i++) {
      if (req.body[`MediaUrl${i}`]) {
        mediaUrls.push(req.body[`MediaUrl${i}`]);
      }
    }

    // Find customer by phone number
    const cleanedPhone = From.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`
      SELECT id, name FROM customers 
      WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
         OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
      LIMIT 1
    `, [`%${cleanedPhone}`]);
    
    const customerId = customerResult.rows[0]?.id || null;
    const customerName = customerResult.rows[0]?.name || 'Unknown';

    // Store message
    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, media_urls, status, customer_id, read)
      VALUES ($1, 'inbound', $2, $3, $4, $5, 'received', $6, false)
      ON CONFLICT (twilio_sid) DO NOTHING
    `, [MessageSid, From, To, Body, mediaUrls, customerId]);

    console.log(`📨 Incoming SMS from ${customerName} (${From}): ${Body?.substring(0, 50)}...`);

    // Send push notification
    sendPushToAllDevices(`💬 ${customerName}`, Body?.substring(0, 100) || 'New message', { type: 'sms', phoneNumber: cleanedPhone, contactName: customerName });

    // Send TwiML response (empty - don't auto-reply)
    res.type('text/xml').send('<Response></Response>');
  } catch (error) {
    console.error('SMS webhook error:', error);
    res.type('text/xml').send('<Response></Response>');
  }
});

// Get message conversations (grouped by NORMALIZED phone number) - for app
// FIX: Now properly groups by last 10 digits to prevent duplicate conversations
app.get('/api/app/messages/conversations', authenticateToken, async (req, res) => {
  try {
    const { twilio_number } = req.query;
    
    // Normalize the twilio_number filter if provided (for multi-number support)
    const twilioFilter = twilio_number 
      ? twilio_number.replace(/\D/g, '').slice(-10) 
      : null;
    
    let query = `
      WITH normalized_messages AS (
        SELECT 
          m.*,
          -- Normalize the "other party" phone number (strip to last 10 digits)
          RIGHT(REGEXP_REPLACE(
            CASE 
              WHEN m.direction = 'inbound' THEN m.from_number 
              ELSE m.to_number 
            END, '[^0-9]', '', 'g'), 10
          ) AS normalized_phone,
          -- Track which Twilio number was used (for multi-number filtering)
          RIGHT(REGEXP_REPLACE(
            CASE 
              WHEN m.direction = 'inbound' THEN m.to_number 
              ELSE m.from_number 
            END, '[^0-9]', '', 'g'), 10
          ) AS twilio_number_used,
          -- Keep original contact number for display
          CASE 
            WHEN m.direction = 'inbound' THEN m.from_number 
            ELSE m.to_number 
          END AS contact_number
        FROM messages m
    `;
    
    const params = [];
    
    // Add filter for specific Twilio number if provided
    if (twilioFilter) {
      query += `
        WHERE (
          (m.direction = 'inbound' AND RIGHT(REGEXP_REPLACE(m.to_number, '[^0-9]', '', 'g'), 10) = $1)
          OR 
          (m.direction = 'outbound' AND RIGHT(REGEXP_REPLACE(m.from_number, '[^0-9]', '', 'g'), 10) = $1)
        )
      `;
      params.push(twilioFilter);
    }
    
    query += `
      ),
      latest_per_conversation AS (
        SELECT DISTINCT ON (normalized_phone)
          id, twilio_sid, direction, from_number, to_number, body, 
          media_urls, status, customer_id, read, created_at,
          normalized_phone, twilio_number_used, contact_number
        FROM normalized_messages
        ORDER BY normalized_phone, created_at DESC
      ),
      unread_counts AS (
        SELECT 
          RIGHT(REGEXP_REPLACE(
            CASE 
              WHEN direction = 'inbound' THEN from_number 
              ELSE to_number 
            END, '[^0-9]', '', 'g'), 10
          ) AS normalized_phone,
          COUNT(*) FILTER (WHERE read = false AND direction = 'inbound') AS unread_count
        FROM messages
    `;
    
    // Repeat filter for unread counts if needed
    if (twilioFilter) {
      query += `
        WHERE (
          (direction = 'inbound' AND RIGHT(REGEXP_REPLACE(to_number, '[^0-9]', '', 'g'), 10) = $1)
          OR 
          (direction = 'outbound' AND RIGHT(REGEXP_REPLACE(from_number, '[^0-9]', '', 'g'), 10) = $1)
        )
      `;
    }
    
    query += `
        GROUP BY 1
      )
      SELECT 
        lpc.*,
        c.name as customer_name,
        COALESCE(uc.unread_count, 0) AS unread_count
      FROM latest_per_conversation lpc
      LEFT JOIN customers c ON lpc.customer_id = c.id
      LEFT JOIN unread_counts uc ON lpc.normalized_phone = uc.normalized_phone
      ORDER BY lpc.created_at DESC
      LIMIT 100
    `;

    const result = await pool.query(query, params);

    // Enrich with customer names where missing
    const conversations = await Promise.all(result.rows.map(async (conv) => {
      if (!conv.customer_name) {
        const customerResult = await pool.query(`
          SELECT name FROM customers 
          WHERE RIGHT(REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g'), 10) = $1 
             OR RIGHT(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g'), 10) = $1 
          LIMIT 1
        `, [conv.normalized_phone]);
        conv.customer_name = customerResult.rows[0]?.name || null;
      }
      return conv;
    }));

    res.json({ conversations });
  } catch (error) {
    console.error('Get conversations error:', error);
    res.status(500).json({ message: 'Failed to fetch conversations', conversations: [] });
  }
});

// Get messages for a specific conversation - for app
// FIX: Now uses normalized phone matching (last 10 digits)
app.get('/api/app/messages/thread/:phoneNumber', authenticateToken, async (req, res) => {
  const { phoneNumber } = req.params;
  // Normalize to last 10 digits
  const normalizedPhone = phoneNumber.replace(/\D/g, '').slice(-10);
  
  try {
    const result = await pool.query(`
      SELECT * FROM messages 
      WHERE RIGHT(REGEXP_REPLACE(from_number, '[^0-9]', '', 'g'), 10) = $1 
         OR RIGHT(REGEXP_REPLACE(to_number, '[^0-9]', '', 'g'), 10) = $1
      ORDER BY created_at ASC
      LIMIT 100
    `, [normalizedPhone]);

    // Mark messages as read
    await pool.query(`
      UPDATE messages SET read = true 
      WHERE direction = 'inbound' 
      AND (RIGHT(REGEXP_REPLACE(from_number, '[^0-9]', '', 'g'), 10) = $1 
           OR RIGHT(REGEXP_REPLACE(to_number, '[^0-9]', '', 'g'), 10) = $1)
      AND read = false
    `, [normalizedPhone]);

    // Get customer info
    const customerResult = await pool.query(`
      SELECT id, name, mobile, phone, email, street, city, state 
      FROM customers 
      WHERE RIGHT(REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g'), 10) = $1 
         OR RIGHT(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g'), 10) = $1 
      LIMIT 1
    `, [normalizedPhone]);

    res.json({ 
      messages: result.rows,
      customer: customerResult.rows[0] || null
    });
  } catch (error) {
    console.error('Get thread error:', error);
    res.status(500).json({ message: 'Failed to fetch messages', messages: [] });
  }
});

// Send SMS - for app (with multi-number support)
app.post('/api/app/messages/send', authenticateToken, async (req, res) => {
  const { to, body, mediaUrls, fromNumber } = req.body;

  if (!to || !body) {
    return res.status(400).json({ message: 'Phone number and message body required' });
  }

  try {
    // Format recipient phone number
    let formattedTo = to.replace(/\D/g, '');
    if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
    else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

    // Determine which Twilio number to send from
    let sendFromNumber = TWILIO_PHONE_NUMBER; // Default
    if (fromNumber) {
      const normalizedFrom = fromNumber.replace(/\D/g, '').slice(-10);
      if (TWILIO_NUMBERS[normalizedFrom]) {
        sendFromNumber = TWILIO_NUMBERS[normalizedFrom];
      }
    }

    // Send via Twilio
    const messageOptions = {
      body,
      from: sendFromNumber,
      to: formattedTo
    };
    
    if (mediaUrls && mediaUrls.length > 0) {
      messageOptions.mediaUrl = mediaUrls;
    }

    const twilioMessage = await twilioClient.messages.create(messageOptions);

    // Find customer
    const cleanedPhone = formattedTo.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`
      SELECT id FROM customers 
      WHERE RIGHT(REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g'), 10) = $1 
         OR RIGHT(REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g'), 10) = $1 
      LIMIT 1
    `, [cleanedPhone]);

    // Store in database
    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, media_urls, status, customer_id, read)
      VALUES ($1, 'outbound', $2, $3, $4, $5, $6, $7, true)
    `, [
      twilioMessage.sid,
      sendFromNumber,
      formattedTo,
      body,
      mediaUrls || [],
      twilioMessage.status,
      customerResult.rows[0]?.id || null
    ]);

    console.log(`📤 Sent SMS from ${sendFromNumber} to ${formattedTo}: ${body.substring(0, 50)}...`);

    res.json({ 
      success: true, 
      sid: twilioMessage.sid,
      message: {
        id: twilioMessage.sid,
        direction: 'outbound',
        from_number: sendFromNumber,
        to_number: formattedTo,
        body,
        status: twilioMessage.status,
        created_at: new Date().toISOString()
      }
    });
  } catch (error) {
    console.error('Send SMS error:', error);
    res.status(500).json({ message: 'Failed to send message', error: error.message });
  }
});

// Get unread message count - for app badge
app.get('/api/app/messages/unread-count', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT COUNT(*) FROM messages WHERE direction = 'inbound' AND read = false
    `);
    res.json({ count: parseInt(result.rows[0].count) });
  } catch (error) {
    res.status(500).json({ count: 0 });
  }
});

// Get available Twilio phone numbers - for multi-number toggle
app.get('/api/app/twilio-numbers', authenticateToken, async (req, res) => {
  try {
    const numbers = Object.entries(TWILIO_NUMBERS).map(([key, value]) => ({
      id: key,
      number: value,
      formatted: `(${key.slice(0,3)}) ${key.slice(3,6)}-${key.slice(6)}`,
      label: key === '4408867318' ? 'Primary' : 'Secondary'
    }));
    res.json({ numbers });
  } catch (error) {
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// WEB DASHBOARD MESSAGES ENDPOINTS (for communications.html)
// ═══════════════════════════════════════════════════════════════════════════════

// Get conversations grouped by phone number for web dashboard
app.get('/api/messages/conversations', async (req, res) => {
  try {
    const result = await pool.query(`
      WITH latest_messages AS (
        SELECT DISTINCT ON (
          CASE 
            WHEN direction = 'inbound' THEN from_number 
            ELSE to_number 
          END
        )
        id, twilio_sid, direction, from_number, to_number, body, media_urls, status, customer_id, read, created_at,
        CASE 
          WHEN direction = 'inbound' THEN from_number 
          ELSE to_number 
        END as contact_number
        FROM messages
        ORDER BY contact_number, created_at DESC
      )
      SELECT 
        lm.*,
        c.name as customer_name,
        (SELECT COUNT(*) FROM messages m2 
         WHERE m2.read = false 
         AND m2.direction = 'inbound'
         AND (m2.from_number = lm.contact_number OR m2.to_number = lm.contact_number)
        ) as unread_count,
        (SELECT COUNT(*) FROM messages m3 
         WHERE m3.from_number = lm.contact_number OR m3.to_number = lm.contact_number
        ) as message_count
      FROM latest_messages lm
      LEFT JOIN customers c ON lm.customer_id = c.id
      ORDER BY lm.created_at DESC
      LIMIT 100
    `);

    // Enrich with customer names where missing
    const conversations = await Promise.all(result.rows.map(async (conv) => {
      if (!conv.customer_name) {
        const cleanedPhone = conv.contact_number.replace(/\D/g, '').slice(-10);
        const customerResult = await pool.query(`
          SELECT name FROM customers 
          WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
             OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
          LIMIT 1
        `, [`%${cleanedPhone}`]);
        conv.customer_name = customerResult.rows[0]?.name || null;
      }
      return {
        id: conv.id,
        phoneNumber: conv.contact_number,
        customerName: conv.customer_name,
        lastMessage: conv.body,
        lastMessageTime: conv.created_at,
        direction: conv.direction,
        unreadCount: parseInt(conv.unread_count) || 0,
        messageCount: parseInt(conv.message_count) || 0,
        read: conv.read
      };
    }));

    res.json({ success: true, conversations });
  } catch (error) {
    console.error('Get conversations error:', error);
    res.status(500).json({ success: false, conversations: [] });
  }
});

// Get all messages for a specific conversation thread
app.get('/api/messages/thread/:phoneNumber', async (req, res) => {
  const { phoneNumber } = req.params;
  try {
    const result = await pool.query(`
      SELECT m.*, c.name as customer_name
      FROM messages m
      LEFT JOIN customers c ON m.customer_id = c.id
      WHERE m.from_number = $1 OR m.to_number = $1
      ORDER BY m.created_at ASC
    `, [phoneNumber]);

    // Mark inbound messages as read
    await pool.query(`
      UPDATE messages SET read = true 
      WHERE (from_number = $1 OR to_number = $1) AND direction = 'inbound' AND read = false
    `, [phoneNumber]);

    const messages = result.rows.map(msg => ({
      id: msg.id,
      sid: msg.twilio_sid,
      direction: msg.direction,
      from: msg.from_number,
      to: msg.to_number,
      body: msg.body,
      mediaUrls: msg.media_urls,
      status: msg.status,
      customerName: msg.customer_name,
      timestamp: msg.created_at,
      read: msg.read
    }));

    res.json({ success: true, messages });
  } catch (error) {
    console.error('Get thread error:', error);
    res.status(500).json({ success: false, messages: [] });
  }
});

// Get all messages for web dashboard (legacy - flat list)
app.get('/api/messages', async (req, res) => {
  const limit = parseInt(req.query.limit) || 200;
  try {
    const result = await pool.query(`
      SELECT 
        m.*,
        c.name as customer_name
      FROM messages m
      LEFT JOIN customers c ON m.customer_id = c.id
      ORDER BY m.created_at DESC
      LIMIT $1
    `, [limit]);

    // Enrich with customer names where missing
    const messages = await Promise.all(result.rows.map(async (msg) => {
      if (!msg.customer_name) {
        const phoneToSearch = msg.direction === 'inbound' ? msg.from_number : msg.to_number;
        const cleanedPhone = phoneToSearch.replace(/\D/g, '').slice(-10);
        const customerResult = await pool.query(`
          SELECT name FROM customers 
          WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
             OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
          LIMIT 1
        `, [`%${cleanedPhone}`]);
        msg.customer_name = customerResult.rows[0]?.name || null;
      }
      return {
        id: msg.id,
        sid: msg.twilio_sid,
        direction: msg.direction,
        from: msg.from_number,
        to: msg.to_number,
        body: msg.body,
        status: msg.status,
        customerName: msg.customer_name,
        timestamp: msg.created_at,
        read: msg.read
      };
    }));

    res.json({ success: true, messages });
  } catch (error) {
    console.error('Get messages error:', error);
    res.status(500).json({ success: false, messages: [] });
  }
});

// Send SMS from web dashboard
app.post('/api/messages/send', async (req, res) => {
  const { to, body } = req.body;

  if (!to || !body) {
    return res.status(400).json({ success: false, message: 'Phone number and message required' });
  }

  try {
    let formattedTo = to.replace(/\D/g, '');
    if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
    else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

    const twilioMessage = await twilioClient.messages.create({
      body,
      from: TWILIO_PHONE_NUMBER,
      to: formattedTo
    });

    // Find customer
    const cleanedPhone = formattedTo.replace(/\D/g, '').slice(-10);
    const customerResult = await pool.query(`
      SELECT id FROM customers 
      WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 
         OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 
      LIMIT 1
    `, [`%${cleanedPhone}`]);

    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
      VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
    `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, formattedTo, body, twilioMessage.status, customerResult.rows[0]?.id || null]);

    res.json({ success: true, sid: twilioMessage.sid });
  } catch (error) {
    console.error('Send SMS error:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════════
// QUOTE FOLLOW-UP SYSTEM - Automated email/SMS reminders for pending quotes
// ═══════════════════════════════════════════════════════════════════════════════

// Create quote_followups table
async function createQuoteFollowupsTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS quote_followups (
      id SERIAL PRIMARY KEY,
      quote_id VARCHAR(50) NOT NULL,
      quote_number VARCHAR(50),
      customer_name VARCHAR(255) NOT NULL,
      customer_email VARCHAR(255) NOT NULL,
      customer_phone VARCHAR(50),
      quote_amount DECIMAL(10,2),
      services TEXT,
      
      quote_sent_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      status VARCHAR(50) DEFAULT 'pending',
      current_stage INT DEFAULT 0,
      
      followup_1_date TIMESTAMP,
      followup_1_sent BOOLEAN DEFAULT false,
      followup_2_date TIMESTAMP,
      followup_2_sent BOOLEAN DEFAULT false,
      followup_3_date TIMESTAMP,
      followup_3_sent BOOLEAN DEFAULT false,
      followup_4_date TIMESTAMP,
      followup_4_sent BOOLEAN DEFAULT false,
      
      stopped_at TIMESTAMP,
      stopped_reason VARCHAR(100),
      stopped_by VARCHAR(100),
      
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      notes TEXT
    )
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_followups_status ON quote_followups(status)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_followups_email ON quote_followups(customer_email)`);
  console.log('✅ Quote followups table ready');
}
createQuoteFollowupsTable();

// POST /api/quote-followups - Create a new follow-up sequence when quote is sent
app.post('/api/quote-followups', async (req, res) => {
  try {
    const { quote_id, quote_number, customer_name, customer_email, customer_phone, quote_amount, services } = req.body;
    
    if (!customer_email || !customer_name) {
      return res.status(400).json({ success: false, error: 'Customer name and email required' });
    }
    
    // Calculate follow-up dates: Day 3, 7, 14, 25
    const now = new Date();
    const day3 = new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000);
    const day7 = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
    const day14 = new Date(now.getTime() + 14 * 24 * 60 * 60 * 1000);
    const day25 = new Date(now.getTime() + 25 * 24 * 60 * 60 * 1000);
    
    const result = await pool.query(`
      INSERT INTO quote_followups (
        quote_id, quote_number, customer_name, customer_email, customer_phone,
        quote_amount, services, quote_sent_date,
        followup_1_date, followup_2_date, followup_3_date, followup_4_date
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      RETURNING *
    `, [
      quote_id || `Q-${Date.now()}`, quote_number, customer_name, customer_email, customer_phone,
      quote_amount, services, now, day3, day7, day14, day25
    ]);
    
    console.log(`📧 Follow-up sequence created for ${customer_name}`);
    res.json({ success: true, followup: result.rows[0] });
  } catch (error) {
    console.error('Error creating follow-up:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/quote-followups - Get all follow-ups with optional filters
app.get('/api/quote-followups', async (req, res) => {
  try {
    const { status, due_today } = req.query;
    let query = 'SELECT * FROM quote_followups';
    const params = [];
    const conditions = [];
    
    if (status) {
      conditions.push(`status = $${params.length + 1}`);
      params.push(status);
    }
    
    if (due_today === 'true') {
      const today = new Date().toISOString().split('T')[0];
      conditions.push(`(
        (current_stage = 0 AND followup_1_date::date <= $${params.length + 1} AND NOT followup_1_sent) OR
        (current_stage = 1 AND followup_2_date::date <= $${params.length + 1} AND NOT followup_2_sent) OR
        (current_stage = 2 AND followup_3_date::date <= $${params.length + 1} AND NOT followup_3_sent) OR
        (current_stage = 3 AND followup_4_date::date <= $${params.length + 1} AND NOT followup_4_sent)
      )`);
      params.push(today);
    }
    
    if (conditions.length > 0) query += ' WHERE ' + conditions.join(' AND ');
    query += ' ORDER BY created_at DESC';
    
    const result = await pool.query(query, params);
    res.json({ success: true, followups: result.rows });
  } catch (error) {
    console.error('Error fetching follow-ups:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/quote-followups/stats - Dashboard stats
app.get('/api/quote-followups/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT 
        COUNT(*) FILTER (WHERE status = 'pending') as pending,
        COUNT(*) FILTER (WHERE status = 'accepted') as accepted,
        COUNT(*) FILTER (WHERE status = 'declined') as declined,
        COUNT(*) FILTER (WHERE status = 'replied') as replied,
        COUNT(*) FILTER (WHERE status = 'paused') as paused,
        COUNT(*) FILTER (WHERE status = 'expired') as expired,
        COUNT(*) FILTER (WHERE status = 'completed') as completed,
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE status = 'pending' AND (
          (current_stage = 0 AND followup_1_date::date = CURRENT_DATE) OR
          (current_stage = 1 AND followup_2_date::date = CURRENT_DATE) OR
          (current_stage = 2 AND followup_3_date::date = CURRENT_DATE) OR
          (current_stage = 3 AND followup_4_date::date = CURRENT_DATE)
        )) as due_today
      FROM quote_followups
    `);
    res.json({ success: true, stats: stats.rows[0] });
  } catch (error) {
    console.error('Error fetching stats:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PATCH /api/quote-followups/:id/stop - Stop a follow-up sequence (manual pause)
app.patch('/api/quote-followups/:id/stop', async (req, res) => {
  try {
    const { reason, stopped_by } = req.body;
    const newStatus = reason === 'manual_pause' ? 'paused' : reason;
    
    const result = await pool.query(`
      UPDATE quote_followups 
      SET status = $1, stopped_at = NOW(), stopped_reason = $2, stopped_by = $3, updated_at = NOW()
      WHERE id = $4
      RETURNING *
    `, [newStatus, reason, stopped_by || 'manual', req.params.id]);
    
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Follow-up not found' });
    
    console.log(`⏹️ Follow-up stopped: ${result.rows[0].customer_name} - ${reason}`);
    res.json({ success: true, followup: result.rows[0] });
  } catch (error) {
    console.error('Error stopping follow-up:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PATCH /api/quote-followups/:id/resume - Resume a paused follow-up
app.patch('/api/quote-followups/:id/resume', async (req, res) => {
  try {
    const result = await pool.query(`
      UPDATE quote_followups 
      SET status = 'pending', stopped_at = NULL, stopped_reason = NULL, updated_at = NOW()
      WHERE id = $1 AND status = 'paused'
      RETURNING *
    `, [req.params.id]);
    
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Follow-up not found or not paused' });
    
    console.log(`▶️ Follow-up resumed: ${result.rows[0].customer_name}`);
    res.json({ success: true, followup: result.rows[0] });
  } catch (error) {
    console.error('Error resuming follow-up:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/webhooks/quote-accepted - Stop follow-ups when quote is accepted
app.post('/api/webhooks/quote-accepted', async (req, res) => {
  try {
    const { quote_id, quote_number, customer_email } = req.body;
    
    let whereClause = quote_id ? 'quote_id = $1' : (quote_number ? 'quote_number = $1' : 'customer_email = $1');
    let params = [quote_id || quote_number || customer_email];
    
    if (!params[0]) return res.status(400).json({ success: false, error: 'Need quote_id, quote_number, or customer_email' });
    
    const result = await pool.query(`
      UPDATE quote_followups 
      SET status = 'accepted', stopped_at = NOW(), stopped_reason = 'accepted', stopped_by = 'webhook', updated_at = NOW()
      WHERE ${whereClause} AND status = 'pending'
      RETURNING *
    `, params);
    
    console.log(`✅ Quote accepted webhook: ${result.rowCount} follow-up(s) stopped`);
    res.json({ success: true, stopped: result.rowCount });
  } catch (error) {
    console.error('Error processing quote-accepted webhook:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/webhooks/quote-declined - Stop follow-ups when quote is declined
app.post('/api/webhooks/quote-declined', async (req, res) => {
  try {
    const { quote_id, quote_number, customer_email } = req.body;
    
    let whereClause = quote_id ? 'quote_id = $1' : (quote_number ? 'quote_number = $1' : 'customer_email = $1');
    let params = [quote_id || quote_number || customer_email];
    
    const result = await pool.query(`
      UPDATE quote_followups 
      SET status = 'declined', stopped_at = NOW(), stopped_reason = 'declined', stopped_by = 'webhook', updated_at = NOW()
      WHERE ${whereClause} AND status = 'pending'
      RETURNING *
    `, params);
    
    console.log(`❌ Quote declined webhook: ${result.rowCount} follow-up(s) stopped`);
    res.json({ success: true, stopped: result.rowCount });
  } catch (error) {
    console.error('Error processing quote-declined webhook:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/webhooks/customer-replied - Stop follow-ups when customer replies
app.post('/api/webhooks/customer-replied', async (req, res) => {
  try {
    const { customer_email, from_email } = req.body;
    const email = customer_email || from_email;
    
    if (!email) return res.status(400).json({ success: false, error: 'Email required' });
    
    const result = await pool.query(`
      UPDATE quote_followups 
      SET status = 'replied', stopped_at = NOW(), stopped_reason = 'replied', stopped_by = 'email_webhook', updated_at = NOW()
      WHERE customer_email = $1 AND status = 'pending'
      RETURNING *
    `, [email]);
    
    console.log(`💬 Customer replied webhook: ${result.rowCount} follow-up(s) stopped for ${email}`);
    res.json({ success: true, stopped: result.rowCount });
  } catch (error) {
    console.error('Error processing customer-replied webhook:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Follow-up email templates - CopilotCRM style
function getFollowupEmailContent(followup, stage) {
  // Smaller, simpler quote reference - just inline text
  const quoteRef = `
    <p style="font-size:14px;color:#64748b;text-align:center;margin:20px 0;">
      <strong>Quote #${followup.quote_number || 'N/A'}</strong> • $${parseFloat(followup.quote_amount || 0).toFixed(2)}
    </p>
  `;
  
  const viewQuoteButton = followup.sign_url ? `
    <div style="text-align:center;margin:28px 0;">
      <a href="${followup.sign_url}" style="background:#c9dd80;color:#2e403d;padding:14px 40px;text-decoration:none;border-radius:6px;font-weight:bold;font-size:15px;display:inline-block;">View Your Quote →</a>
    </div>
  ` : '';
  
  const templates = {
    1: {
      subject: `Following up on your quote - Pappas & Co. Landscaping`,
      html: emailTemplate(`
        <h2 style="font-family:'Playfair Display',Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:26px;font-weight:400;text-align:center;">Following Up on Your Quote</h2>
        <p style="font-size:15px;color:#374151;line-height:1.6;">Hi ${followup.customer_name},</p>
        <p style="font-size:15px;color:#374151;line-height:1.6;">I wanted to follow up on the quote we sent you a few days ago for your property.</p>
        ${quoteRef}
        <p style="font-size:15px;color:#374151;line-height:1.6;">If you have any questions about the services or pricing, please don't hesitate to reach out. We'd love the opportunity to work with you!</p>
        ${viewQuoteButton}
      `)
    },
    2: {
      subject: `Your quote is still available - Pappas & Co. Landscaping`,
      html: emailTemplate(`
        <h2 style="font-family:'Playfair Display',Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:26px;font-weight:400;text-align:center;">Your Quote is Still Available</h2>
        <p style="font-size:15px;color:#374151;line-height:1.6;">Hi ${followup.customer_name},</p>
        <p style="font-size:15px;color:#374151;line-height:1.6;">Just a friendly reminder that your landscaping quote is still available and valid for the next few weeks.</p>
        ${quoteRef}
        <p style="font-size:15px;color:#374151;line-height:1.6;">If you'd like to move forward or have any questions, simply reply to this email or give us a call.</p>
        ${viewQuoteButton}
      `)
    },
    3: {
      subject: `Still interested? - Pappas & Co. Landscaping`,
      html: emailTemplate(`
        <h2 style="font-family:'Playfair Display',Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:26px;font-weight:400;text-align:center;">Checking In</h2>
        <p style="font-size:15px;color:#374151;line-height:1.6;">Hi ${followup.customer_name},</p>
        <p style="font-size:15px;color:#374151;line-height:1.6;">We haven't heard back from you regarding your landscaping quote, and wanted to check in.</p>
        <p style="font-size:15px;color:#374151;line-height:1.6;">Is there anything we can help clarify? We're happy to adjust the scope or answer any questions you might have.</p>
        ${quoteRef}
        ${viewQuoteButton}
      `)
    },
    4: {
      subject: `Your quote expires soon - Pappas & Co. Landscaping`,
      html: emailTemplate(`
        <h2 style="font-family:'Playfair Display',Georgia,serif;color:#1e293b;margin:0 0 24px;font-size:26px;font-weight:400;text-align:center;">Your Quote Expires Soon</h2>
        <p style="font-size:15px;color:#374151;line-height:1.6;">Hi ${followup.customer_name},</p>
        <p style="font-size:15px;color:#374151;line-height:1.6;">This is a final reminder that your landscaping quote will expire in just <strong>5 days</strong>.</p>
        <div style="background:#fff8e6;border-left:4px solid #f59e0b;padding:16px;margin:20px 0;">
          <p style="margin:0;font-size:14px;color:#92400e;">
            <strong>Quote #${followup.quote_number || 'N/A'}</strong> • $${parseFloat(followup.quote_amount || 0).toFixed(2)}<br>
            <span style="font-size:13px;">After expiration, prices may change based on availability.</span>
          </p>
        </div>
        <p style="font-size:15px;color:#374151;line-height:1.6;">If you'd like to lock in this rate, please let us know before it expires!</p>
        ${viewQuoteButton}
      `)
    }
  };
  return templates[stage];
}

// Follow-up SMS templates
function getFollowupSMS(followup, stage) {
  const templates = {
    2: `Hi ${followup.customer_name}, just checking in on your landscaping quote (#${followup.quote_number || 'pending'}). Let us know if you have any questions! - Pappas & Co. (440) 886-7318`,
    3: `Hi ${followup.customer_name}, we haven't heard back about your quote. Still interested? Reply or call us at (440) 886-7318. - Pappas & Co.`,
    4: `Hi ${followup.customer_name}, your landscaping quote expires in 5 days! Reply to lock in your rate. - Pappas & Co. (440) 886-7318`
  };
  return templates[stage];
}

// POST /api/cron/process-followups - Daily cron job to send due follow-ups
app.post('/api/cron/process-followups', async (req, res) => {
  try {
    // Get all pending follow-ups that are due
    const dueFollowups = await pool.query(`
      SELECT * FROM quote_followups 
      WHERE status = 'pending' AND (
        (current_stage = 0 AND followup_1_date <= NOW() AND NOT followup_1_sent) OR
        (current_stage = 1 AND followup_2_date <= NOW() AND NOT followup_2_sent) OR
        (current_stage = 2 AND followup_3_date <= NOW() AND NOT followup_3_sent) OR
        (current_stage = 3 AND followup_4_date <= NOW() AND NOT followup_4_sent)
      )
    `);
    
    console.log(`📬 Processing ${dueFollowups.rows.length} due follow-ups`);
    
    const results = { processed: 0, emails_sent: 0, sms_sent: 0, errors: [] };
    
    for (const followup of dueFollowups.rows) {
      try {
        const stage = followup.current_stage + 1;
        
        // Send email
        const emailContent = getFollowupEmailContent(followup, stage);
        if (emailContent && followup.customer_email) {
          await sendEmail(followup.customer_email, emailContent.subject, emailContent.html);
          results.emails_sent++;
          console.log(`📧 Email sent to ${followup.customer_email} (Stage ${stage})`);
        }
        
        // Send SMS for stages 2-4 (when Twilio A2P is approved)
        if (stage >= 2 && followup.customer_phone) {
          const smsText = getFollowupSMS(followup, stage);
          if (smsText && TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN) {
            try {
              let formattedPhone = followup.customer_phone.replace(/\D/g, '');
              if (formattedPhone.length === 10) formattedPhone = '+1' + formattedPhone;
              else if (!formattedPhone.startsWith('+')) formattedPhone = '+' + formattedPhone;
              
              await twilioClient.messages.create({
                body: smsText,
                from: TWILIO_PHONE_NUMBER,
                to: formattedPhone
              });
              results.sms_sent++;
              console.log(`📱 SMS sent to ${followup.customer_phone} (Stage ${stage})`);
            } catch (smsError) {
              console.log(`SMS skipped for ${followup.customer_name}: ${smsError.message}`);
            }
          }
        }
        
        // Update the follow-up record
        const stageField = `followup_${stage}_sent`;
        const newStatus = stage >= 4 ? 'completed' : 'pending';
        
        await pool.query(`
          UPDATE quote_followups 
          SET ${stageField} = true, current_stage = $1, status = $2, updated_at = NOW()
          WHERE id = $3
        `, [stage, newStatus, followup.id]);
        
        results.processed++;
        
      } catch (err) {
        console.error(`Error processing follow-up ${followup.id}:`, err);
        results.errors.push({ id: followup.id, error: err.message });
      }
    }
    
    // Mark expired quotes (30+ days old, still pending)
    const expireResult = await pool.query(`
      UPDATE quote_followups 
      SET status = 'expired', updated_at = NOW()
      WHERE status = 'pending' AND quote_sent_date < NOW() - INTERVAL '30 days'
      RETURNING id
    `);
    results.expired = expireResult.rowCount;
    
    console.log(`📊 Cron complete: ${results.processed} processed, ${results.emails_sent} emails, ${results.sms_sent} SMS, ${results.expired} expired`);
    res.json({ success: true, results });
    
  } catch (error) {
    console.error('Error in cron job:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/setup-quote-followups - Run once to ensure table exists
app.get('/api/setup-quote-followups', async (req, res) => {
  try {
    await createQuoteFollowupsTable();
    res.json({ success: true, message: 'Quote followups table ready!' });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/cron/process-followups - Allow GET for easy cron-job.org setup
app.get('/api/cron/process-followups', async (req, res) => {
  // Redirect to POST handler
  try {
    // Get all pending follow-ups that are due
    const dueFollowups = await pool.query(`
      SELECT * FROM quote_followups 
      WHERE status = 'pending' AND (
        (current_stage = 0 AND followup_1_date <= NOW() AND NOT followup_1_sent) OR
        (current_stage = 1 AND followup_2_date <= NOW() AND NOT followup_2_sent) OR
        (current_stage = 2 AND followup_3_date <= NOW() AND NOT followup_3_sent) OR
        (current_stage = 3 AND followup_4_date <= NOW() AND NOT followup_4_sent)
      )
    `);
    
    console.log(`📬 [CRON-GET] Processing ${dueFollowups.rows.length} due follow-ups`);
    
    const results = { processed: 0, emails_sent: 0, sms_sent: 0, errors: [] };
    
    for (const followup of dueFollowups.rows) {
      try {
        const stage = followup.current_stage + 1;
        
        // Send email
        const emailContent = getFollowupEmailContent(followup, stage);
        if (emailContent && followup.customer_email) {
          await sendEmail(followup.customer_email, emailContent.subject, emailContent.html);
          results.emails_sent++;
          console.log(`📧 Email sent to ${followup.customer_email} (Stage ${stage})`);
        }
        
        // Send SMS for stages 2-4 (when Twilio A2P is approved)
        if (stage >= 2 && followup.customer_phone) {
          const smsText = getFollowupSMS(followup, stage);
          if (smsText && TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN) {
            try {
              let formattedPhone = followup.customer_phone.replace(/\D/g, '');
              if (formattedPhone.length === 10) formattedPhone = '+1' + formattedPhone;
              else if (!formattedPhone.startsWith('+')) formattedPhone = '+' + formattedPhone;
              
              await twilioClient.messages.create({
                body: smsText,
                from: TWILIO_PHONE_NUMBER,
                to: formattedPhone
              });
              results.sms_sent++;
              console.log(`📱 SMS sent to ${followup.customer_phone} (Stage ${stage})`);
            } catch (smsError) {
              console.log(`SMS skipped: ${smsError.message}`);
            }
          }
        }
        
        // Update the follow-up record
        const stageField = `followup_${stage}_sent`;
        const newStatus = stage >= 4 ? 'completed' : 'pending';
        
        await pool.query(`
          UPDATE quote_followups 
          SET ${stageField} = true, current_stage = $1, status = $2, updated_at = NOW()
          WHERE id = $3
        `, [stage, newStatus, followup.id]);
        
        results.processed++;
        
      } catch (err) {
        console.error(`Error processing follow-up ${followup.id}:`, err);
        results.errors.push({ id: followup.id, error: err.message });
      }
    }
    
    // Mark expired quotes (30+ days old, still pending)
    const expireResult = await pool.query(`
      UPDATE quote_followups 
      SET status = 'expired', updated_at = NOW()
      WHERE status = 'pending' AND quote_sent_date < NOW() - INTERVAL '30 days'
      RETURNING id
    `);
    results.expired = expireResult.rowCount;
    
    console.log(`📊 Cron complete: ${results.processed} processed, ${results.emails_sent} emails, ${results.sms_sent} SMS, ${results.expired} expired`);
    res.json({ success: true, results, timestamp: new Date().toISOString() });
    
  } catch (error) {
    console.error('Error in cron job:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// GENERAL ROUTES
// ═══════════════════════════════════════════════════════════
// ═══ INVOICING ════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════

const INVOICES_TABLE = `CREATE TABLE IF NOT EXISTS invoices (
  id SERIAL PRIMARY KEY,
  invoice_number VARCHAR(50) UNIQUE,
  customer_id INTEGER,
  customer_name VARCHAR(255),
  customer_email VARCHAR(255),
  customer_address TEXT,
  sent_quote_id INTEGER,
  job_id INTEGER,
  status VARCHAR(20) DEFAULT 'draft',
  subtotal DECIMAL(10,2) DEFAULT 0,
  tax_rate DECIMAL(5,3) DEFAULT 0,
  tax_amount DECIMAL(10,2) DEFAULT 0,
  total DECIMAL(10,2) DEFAULT 0,
  amount_paid DECIMAL(10,2) DEFAULT 0,
  due_date DATE,
  paid_at TIMESTAMP,
  sent_at TIMESTAMP,
  qb_invoice_id VARCHAR(100),
  notes TEXT,
  line_items JSONB DEFAULT '[]',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)`;

async function ensureInvoicesTable() {
  await pool.query(INVOICES_TABLE);
}

async function nextInvoiceNumber() {
  const r = await pool.query("SELECT invoice_number FROM invoices ORDER BY id DESC LIMIT 1");
  if (r.rows.length === 0) return 'INV-1001';
  const last = r.rows[0].invoice_number || 'INV-1000';
  const num = parseInt(last.replace(/\D/g, '')) || 1000;
  return `INV-${num + 1}`;
}

// GET /api/payments - List received payments (paid/partial invoices)
app.get('/api/payments', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const { search, year, month, limit = 200, offset = 0 } = req.query;

    const params = [];
    const where = ['amount_paid > 0'];
    let p = 1;

    if (search) {
      where.push(`(customer_name ILIKE $${p} OR invoice_number ILIKE $${p})`);
      params.push('%' + search + '%'); p++;
    }
    if (year) {
      where.push(`EXTRACT(YEAR FROM COALESCE(paid_at, updated_at)) = $${p}`);
      params.push(parseInt(year)); p++;
    }
    if (month) {
      where.push(`EXTRACT(MONTH FROM COALESCE(paid_at, updated_at)) = $${p}`);
      params.push(parseInt(month)); p++;
    }

    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    params.push(parseInt(limit)); params.push(parseInt(offset));

    const result = await pool.query(
      `SELECT id, invoice_number, customer_id, customer_name, customer_email,
              total, amount_paid, status, paid_at, due_date, created_at, qb_invoice_id
       FROM invoices ${whereClause}
       ORDER BY COALESCE(paid_at, updated_at) DESC
       LIMIT $${p} OFFSET $${p+1}`,
      params
    );

    const countResult = await pool.query(
      `SELECT COUNT(*) as cnt, COALESCE(SUM(amount_paid),0) as total_received
       FROM invoices ${whereClause}`,
      params.slice(0, -2)
    );

    const monthly = await pool.query(`
      SELECT to_char(COALESCE(paid_at, updated_at),'YYYY-MM') as month,
             COUNT(*) as count, SUM(amount_paid) as total
      FROM invoices
      WHERE amount_paid > 0 AND COALESCE(paid_at, updated_at) >= NOW() - INTERVAL '12 months'
      GROUP BY month ORDER BY month
    `);

    res.json({
      success: true,
      payments: result.rows,
      total: parseInt(countResult.rows[0].cnt),
      totalReceived: parseFloat(countResult.rows[0].total_received),
      monthly: monthly.rows
    });
  } catch (e) {
    console.error('Payments API error:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// GET /api/invoices - List invoices
app.get('/api/invoices', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const { status, customer_id, search, limit = 9999, offset = 0 } = req.query;
    let q = 'SELECT i.*, c.name as customer_name FROM invoices i LEFT JOIN customers c ON i.customer_id = c.id';
    const params = [];
    const where = [];
    if (status) { params.push(status); where.push(`i.status = $${params.length}`); }
    if (customer_id) { params.push(customer_id); where.push(`i.customer_id = $${params.length}`); }
    if (search) {
      params.push(`%${search}%`);
      where.push(`(i.invoice_number ILIKE $${params.length} OR c.name ILIKE $${params.length})`);
    }
    if (where.length) q += ' WHERE ' + where.join(' AND ');
    q += ' ORDER BY i.created_at DESC';
    params.push(limit); q += ` LIMIT $${params.length}`;
    params.push(offset); q += ` OFFSET $${params.length}`;
    const result = await pool.query(q, params);
    res.json({ success: true, invoices: result.rows });
  } catch (error) {
    console.error('Error fetching invoices:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/invoices/stats - Invoice statistics
app.get('/api/invoices/stats', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const all = await pool.query('SELECT status, total, amount_paid, due_date FROM invoices');
    const now = new Date();
    const thisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    let stats = { total: 0, draft: 0, sent: 0, viewed: 0, paid: 0, overdue: 0, void: 0,
      outstanding: 0, overdueAmount: 0, paidThisMonth: 0, totalRevenue: 0 };
    all.rows.forEach(inv => {
      stats.total++;
      stats[inv.status] = (stats[inv.status] || 0) + 1;
      const t = parseFloat(inv.total) || 0;
      const p = parseFloat(inv.amount_paid) || 0;
      if (inv.status === 'paid') {
        stats.totalRevenue += t;
        if (inv.paid_at && new Date(inv.paid_at) >= thisMonth) stats.paidThisMonth += t;
      }
      if (['sent', 'viewed'].includes(inv.status)) {
        stats.outstanding += (t - p);
        if (inv.due_date && new Date(inv.due_date) < now) {
          stats.overdue++;
          stats.overdueAmount += (t - p);
        }
      }
    });
    res.json({ success: true, stats });
  } catch (error) {
    console.error('Error fetching invoice stats:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/invoices/:id - Single invoice
app.get('/api/invoices/:id', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const r = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/invoices - Create invoice
app.post('/api/invoices', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const { customer_id, customer_name, customer_email, customer_address, sent_quote_id, job_id,
      subtotal, tax_rate, tax_amount, total, due_date, notes, line_items } = req.body;
    const invNum = await nextInvoiceNumber();
    const r = await pool.query(`INSERT INTO invoices
      (invoice_number, customer_id, customer_name, customer_email, customer_address, sent_quote_id, job_id,
       subtotal, tax_rate, tax_amount, total, due_date, notes, line_items)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING *`,
      [invNum, customer_id||null, customer_name, customer_email, customer_address||'',
       sent_quote_id||null, job_id||null, subtotal||0, tax_rate||0, tax_amount||0, total||0,
       due_date||null, notes||'', JSON.stringify(line_items||[])]);
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    console.error('Error creating invoice:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/invoices/from-quote/:quoteId - Create invoice from signed quote
app.post('/api/invoices/from-quote/:quoteId', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const q = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.quoteId]);
    if (q.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    const quote = q.rows[0];
    const services = typeof quote.services === 'string' ? JSON.parse(quote.services) : (quote.services || []);
    const lineItems = services.map(s => ({ description: s.name || s.description, amount: parseFloat(s.price || s.amount || 0) }));
    const invNum = await nextInvoiceNumber();
    const dueDate = new Date(); dueDate.setDate(dueDate.getDate() + 30);
    const r = await pool.query(`INSERT INTO invoices
      (invoice_number, customer_id, customer_name, customer_email, customer_address, sent_quote_id,
       subtotal, tax_rate, tax_amount, total, due_date, line_items, notes)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *`,
      [invNum, quote.customer_id||null, quote.customer_name, quote.customer_email, quote.customer_address||'',
       quote.id, parseFloat(quote.subtotal)||0, 0, parseFloat(quote.tax_amount)||0,
       parseFloat(quote.total)||0, dueDate.toISOString().split('T')[0], JSON.stringify(lineItems),
       `Generated from Quote ${quote.quote_number || 'Q-'+quote.id}`]);
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    console.error('Error creating invoice from quote:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// PATCH /api/invoices/:id - Update invoice
app.patch('/api/invoices/:id', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const fields = ['status','customer_name','customer_email','customer_address','subtotal','tax_rate','tax_amount','total','amount_paid','due_date','paid_at','sent_at','qb_invoice_id','notes','line_items'];
    const sets = []; const params = [];
    fields.forEach(f => {
      if (req.body[f] !== undefined) {
        params.push(f === 'line_items' ? JSON.stringify(req.body[f]) : req.body[f]);
        sets.push(`${f} = $${params.length}`);
      }
    });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields to update' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    params.push(req.params.id);
    const r = await pool.query(`UPDATE invoices SET ${sets.join(', ')} WHERE id = $${params.length} RETURNING *`, params);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    console.error('Error updating invoice:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/invoices/:id/send - Email invoice to customer
app.post('/api/invoices/:id/send', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const r = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = r.rows[0];
    if (!inv.customer_email) return res.status(400).json({ success: false, error: 'No customer email' });
    const items = typeof inv.line_items === 'string' ? JSON.parse(inv.line_items) : (inv.line_items || []);
    const itemsHtml = items.map(i => `<tr><td style="padding:8px 0;border-bottom:1px solid #e5e7eb;">${i.description}</td><td style="padding:8px 0;border-bottom:1px solid #e5e7eb;text-align:right;">$${parseFloat(i.amount).toFixed(2)}</td></tr>`).join('');
    const content = `
      <h2 style="color:#2e403d;margin:0 0 16px;">Invoice ${inv.invoice_number}</h2>
      <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
      <p>Here's your invoice from <strong>Pappas & Co. Landscaping</strong>.</p>
      <div style="background:#f8fafc;border-radius:8px;padding:20px;margin:20px 0;">
        <table style="width:100%;border-collapse:collapse;">
          <thead><tr><th style="text-align:left;padding:8px 0;border-bottom:2px solid #2e403d;">Description</th><th style="text-align:right;padding:8px 0;border-bottom:2px solid #2e403d;">Amount</th></tr></thead>
          <tbody>${itemsHtml}</tbody>
        </table>
        <div style="margin-top:16px;text-align:right;">
          ${inv.tax_amount > 0 ? `<p style="margin:4px 0;color:#666;">Subtotal: $${parseFloat(inv.subtotal).toFixed(2)}</p><p style="margin:4px 0;color:#666;">Tax: $${parseFloat(inv.tax_amount).toFixed(2)}</p>` : ''}
          <p style="margin:8px 0 0;font-size:20px;font-weight:700;color:#2e403d;">Total: $${parseFloat(inv.total).toFixed(2)}</p>
          ${inv.due_date ? `<p style="margin:4px 0;font-size:13px;color:#666;">Due by ${new Date(inv.due_date).toLocaleDateString('en-US', {month:'long',day:'numeric',year:'numeric'})}</p>` : ''}
        </div>
      </div>
      <p>If you have any questions, feel free to reply to this email or call us.</p>
    `;
    await sendEmail(inv.customer_email, `Invoice ${inv.invoice_number} from Pappas & Co.`, emailTemplate(content));
    await pool.query("UPDATE invoices SET status = 'sent', sent_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1", [inv.id]);
    res.json({ success: true, message: 'Invoice sent' });
  } catch (error) {
    console.error('Error sending invoice:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// POST /api/invoices/:id/mark-paid - Mark invoice as paid
app.post('/api/invoices/:id/mark-paid', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const r = await pool.query(
      "UPDATE invoices SET status = 'paid', amount_paid = total, paid_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *",
      [req.params.id]
    );
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, invoice: r.rows[0] });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// DELETE /api/invoices/:id - Delete invoice
app.delete('/api/invoices/:id', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const r = await pool.query('DELETE FROM invoices WHERE id = $1 RETURNING id', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ FINANCE / REPORTS ════════════════════════════════════
// ═══════════════════════════════════════════════════════════

// GET /api/finance/summary - Financial overview
app.get('/api/finance/summary', async (req, res) => {
  try {
    await ensureInvoicesTable();
    await pool.query(`CREATE TABLE IF NOT EXISTS expenses (
      id SERIAL PRIMARY KEY, description TEXT, amount NUMERIC(10,2) DEFAULT 0,
      category VARCHAR(100), vendor VARCHAR(255), expense_date DATE DEFAULT CURRENT_DATE,
      receipt_url TEXT, notes TEXT, qb_id VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    try { await pool.query(`ALTER TABLE expenses ADD COLUMN IF NOT EXISTS qb_id VARCHAR(100)`); } catch(e) {}
    const now = new Date();
    const thisMonthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString();
    const lastMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1).toISOString();
    const thisYearStart = new Date(now.getFullYear(), 0, 1).toISOString();

    const [paidMonth, paidLastMonth, paidYear, expMonth, expLastMonth, expYear, outstanding, overdue, byService, monthly] = await Promise.all([
      pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid' AND paid_at >= $1", [thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid' AND paid_at >= $1 AND paid_at < $2", [lastMonthStart, thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid' AND paid_at >= $1", [thisYearStart]),
      pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses WHERE expense_date >= $1", [thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses WHERE expense_date >= $1 AND expense_date < $2", [lastMonthStart, thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses WHERE expense_date >= $1", [thisYearStart]),
      pool.query("SELECT COALESCE(SUM(total - amount_paid),0) as amt FROM invoices WHERE status IN ('sent','viewed')"),
      pool.query("SELECT COUNT(*) as cnt FROM invoices WHERE status IN ('sent','viewed') AND due_date < CURRENT_DATE"),
      pool.query(`SELECT COALESCE(li->>'description','Other') as name, SUM((li->>'amount')::numeric) as revenue
        FROM invoices, jsonb_array_elements(line_items) li WHERE status='paid' AND paid_at >= $1
        GROUP BY name ORDER BY revenue DESC`, [thisYearStart]),
      pool.query(`SELECT to_char(paid_at,'YYYY-MM') as month,
        SUM(total) as revenue FROM invoices WHERE status='paid' AND paid_at >= NOW() - INTERVAL '12 months'
        GROUP BY month ORDER BY month`)
    ]);

    const expMonthly = await pool.query(`SELECT to_char(expense_date,'YYYY-MM') as month,
      SUM(amount) as expenses FROM expenses WHERE expense_date >= NOW() - INTERVAL '12 months'
      GROUP BY month ORDER BY month`);

    // Build 12-month arrays (current month backwards)
    const monthlyRevenueArr = new Array(12).fill(0);
    const monthlyExpensesArr = new Array(12).fill(0);
    monthly.rows.forEach(r => {
      const [y, m] = r.month.split('-').map(Number);
      const idx = (y - now.getFullYear()) * 12 + (m - 1);
      const currentIdx = now.getMonth();
      const offset = m - 1;
      if (offset >= 0 && offset < 12) monthlyRevenueArr[offset] = parseFloat(r.revenue);
    });
    expMonthly.rows.forEach(r => {
      const [y, m] = r.month.split('-').map(Number);
      const offset = m - 1;
      if (offset >= 0 && offset < 12) monthlyExpensesArr[offset] = parseFloat(r.expenses);
    });

    const revenueMonth = parseFloat(paidMonth.rows[0].amt);
    const expensesMonth = parseFloat(expMonth.rows[0].amt);
    const revenueLastMonth = parseFloat(paidLastMonth.rows[0].amt);
    const expensesLastMonth = parseFloat(expLastMonth.rows[0].amt);

    // All-time totals (useful when current period has no data, e.g. sandbox QB data)
    const allTimeRev = await pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid'");
    const allTimeExp = await pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses");
    const totalInvoices = await pool.query("SELECT COUNT(*) as cnt FROM invoices");
    const totalCustomers = await pool.query("SELECT COUNT(*) as cnt FROM customers");

    res.json({
      thisMonth: { revenue: revenueMonth, expenses: expensesMonth },
      lastMonth: { revenue: revenueLastMonth, expenses: expensesLastMonth },
      yearToDate: {
        revenue: parseFloat(paidYear.rows[0].amt),
        expenses: parseFloat(expYear.rows[0].amt)
      },
      allTime: {
        revenue: parseFloat(allTimeRev.rows[0].amt),
        expenses: parseFloat(allTimeExp.rows[0].amt),
        invoiceCount: parseInt(totalInvoices.rows[0].cnt),
        customerCount: parseInt(totalCustomers.rows[0].cnt)
      },
      monthlyRevenue: monthlyRevenueArr,
      monthlyExpenses: monthlyExpensesArr,
      serviceBreakdown: byService.rows,
      outstanding: {
        totalOutstanding: parseFloat(outstanding.rows[0].amt),
        overdueCount: parseInt(overdue.rows[0].cnt)
      }
    });
  } catch (error) {
    console.error('Error fetching finance summary:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/reports/business-summary - KPIs for a period
app.get('/api/reports/business-summary', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const { period = 'month' } = req.query;
    const now = new Date();
    let start;
    if (period === 'year') start = new Date(now.getFullYear(), 0, 1);
    else if (period === 'quarter') start = new Date(now.getFullYear(), Math.floor(now.getMonth()/3)*3, 1);
    else start = new Date(now.getFullYear(), now.getMonth(), 1);
    const s = start.toISOString();

    const [quotes, invoices, jobs, expenses, customers] = await Promise.all([
      pool.query("SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE status='signed') as signed FROM sent_quotes WHERE created_at >= $1", [s]),
      pool.query("SELECT COUNT(*) as total, COALESCE(SUM(CASE WHEN status='paid' THEN total ELSE 0 END),0) as revenue, COALESCE(SUM(CASE WHEN status IN ('sent','viewed') THEN total-amount_paid ELSE 0 END),0) as outstanding FROM invoices WHERE created_at >= $1", [s]),
      pool.query("SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE status='completed') as completed FROM scheduled_jobs WHERE job_date >= $1", [s]),
      pool.query("SELECT COALESCE(SUM(amount),0) as total FROM expenses WHERE expense_date >= $1", [s]),
      pool.query("SELECT COUNT(*) as new_customers FROM customers WHERE created_at >= $1", [s])
    ]);

    const qr = quotes.rows[0]; const ir = invoices.rows[0]; const jr = jobs.rows[0];
    res.json({ success: true, summary: {
      period, start: s,
      quotesSent: parseInt(qr.total), quotesSigned: parseInt(qr.signed),
      conversionRate: qr.total > 0 ? Math.round((qr.signed / qr.total) * 100) : 0,
      invoicesTotal: parseInt(ir.total), revenue: parseFloat(ir.revenue), outstanding: parseFloat(ir.outstanding),
      jobsTotal: parseInt(jr.total), jobsCompleted: parseInt(jr.completed),
      expenses: parseFloat(expenses.rows[0].total),
      profit: parseFloat(ir.revenue) - parseFloat(expenses.rows[0].total),
      newCustomers: parseInt(customers.rows[0].new_customers)
    }});
  } catch (error) {
    console.error('Error fetching business summary:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/reports/crew-performance - Crew stats
app.get('/api/reports/crew-performance', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT crew_assigned as crew, COUNT(*) as jobs_total,
        COUNT(*) FILTER (WHERE status='completed') as jobs_completed,
        COALESCE(SUM(service_price),0) as total_revenue
      FROM scheduled_jobs WHERE crew_assigned IS NOT NULL
      GROUP BY crew_assigned ORDER BY total_revenue DESC
    `);
    res.json({ success: true, crews: result.rows });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/reports/customer-acquisition - New customers per month
app.get('/api/reports/customer-acquisition', async (req, res) => {
  try {
    const { months = 12 } = req.query;
    const result = await pool.query(`
      SELECT to_char(created_at,'YYYY-MM') as month, COUNT(*) as count
      FROM customers WHERE created_at >= NOW() - INTERVAL '${parseInt(months)} months'
      GROUP BY month ORDER BY month
    `);
    res.json({ success: true, months: result.rows });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ═══════════════════════════════════════════════════════════
// QUICKBOOKS INTEGRATION (One-Way Sync: QB → Pappas)
// ═══════════════════════════════════════════════════════════

// --- QB Database Tables ---
async function ensureQBTables() {
  await pool.query(`CREATE TABLE IF NOT EXISTS qb_tokens (
    id SERIAL PRIMARY KEY,
    realm_id VARCHAR(100) NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    token_type VARCHAR(50) DEFAULT 'bearer',
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS qb_sync_log (
    id SERIAL PRIMARY KEY,
    sync_type VARCHAR(50),
    customers_synced INTEGER DEFAULT 0,
    invoices_synced INTEGER DEFAULT 0,
    payments_synced INTEGER DEFAULT 0,
    expenses_synced INTEGER DEFAULT 0,
    errors TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
  )`);
  // Add qb_id columns if they don't exist
  try { await pool.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS qb_id VARCHAR(100)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ADD COLUMN IF NOT EXISTS qb_id VARCHAR(100)`); } catch(e) {}
}
ensureQBTables().catch(e => console.error('QB tables init error:', e));

// --- QB OAuth Client Factory ---
function createOAuthClient() {
  return new OAuthClient({
    clientId: process.env.QB_CLIENT_ID || '',
    clientSecret: process.env.QB_CLIENT_SECRET || '',
    environment: process.env.QB_ENVIRONMENT || 'sandbox',
    redirectUri: process.env.QB_REDIRECT_URI || 'http://localhost:3000/api/quickbooks/callback',
    logging: false
  });
}

// --- Get authenticated QB client with auto-refresh ---
async function getQBClient() {
  const tokenRow = await pool.query('SELECT * FROM qb_tokens ORDER BY id DESC LIMIT 1');
  if (tokenRow.rows.length === 0) throw new Error('QuickBooks not connected');

  const t = tokenRow.rows[0];
  const oauthClient = createOAuthClient();
  oauthClient.setToken({
    access_token: t.access_token,
    refresh_token: t.refresh_token,
    token_type: t.token_type,
    expires_in: Math.floor((new Date(t.expires_at) - new Date()) / 1000),
    realmId: t.realm_id
  });

  // Auto-refresh if expired or expiring within 5 minutes
  if (new Date(t.expires_at) <= new Date(Date.now() + 5 * 60 * 1000)) {
    try {
      const authResponse = await oauthClient.refresh();
      const newToken = authResponse.getJson();
      const expiresAt = new Date(Date.now() + (newToken.expires_in || 3600) * 1000);
      await pool.query(
        `UPDATE qb_tokens SET access_token=$1, refresh_token=$2, expires_at=$3, updated_at=NOW() WHERE id=$4`,
        [newToken.access_token, newToken.refresh_token || t.refresh_token, expiresAt, t.id]
      );
    } catch (e) {
      console.error('QB token refresh failed:', e.message);
      throw new Error('QuickBooks token expired. Please reconnect.');
    }
  }

  return { oauthClient, realmId: t.realm_id };
}

// --- QB API Helper ---
async function qbApiGet(endpoint) {
  const { oauthClient, realmId } = await getQBClient();
  const baseUrl = process.env.QB_ENVIRONMENT === 'production'
    ? 'https://quickbooks.api.intuit.com'
    : 'https://sandbox-quickbooks.api.intuit.com';
  const url = `${baseUrl}/v3/company/${realmId}/${endpoint}`;
  const response = await oauthClient.makeApiCall({ url, method: 'GET' });
  // Handle different intuit-oauth response formats
  if (response.getJson) return response.getJson();
  if (response.json) return typeof response.json === 'function' ? await response.json() : response.json;
  if (response.body) return typeof response.body === 'string' ? JSON.parse(response.body) : response.body;
  if (typeof response.text === 'function') return JSON.parse(response.text());
  if (typeof response === 'string') return JSON.parse(response);
  return response;
}

// --- OAuth Routes ---

// GET /api/quickbooks/debug - Show QB config for debugging redirect_uri issues
app.get('/api/quickbooks/debug', (req, res) => {
  const redirectUri = process.env.QB_REDIRECT_URI || 'http://localhost:3000/api/quickbooks/callback';
  const env = process.env.QB_ENVIRONMENT || 'sandbox';
  res.json({
    redirectUri,
    environment: env,
    hasClientId: !!process.env.QB_CLIENT_ID,
    hasClientSecret: !!process.env.QB_CLIENT_SECRET,
    clientIdPrefix: process.env.QB_CLIENT_ID ? process.env.QB_CLIENT_ID.substring(0, 8) + '...' : null
  });
});

// GET /api/quickbooks/auth - Start OAuth flow
app.get('/api/quickbooks/auth', (req, res) => {
  if (!process.env.QB_CLIENT_ID) {
    return res.status(400).json({ success: false, error: 'QuickBooks credentials not configured. Set QB_CLIENT_ID and QB_CLIENT_SECRET.' });
  }
  const oauthClient = createOAuthClient();
  const authUri = oauthClient.authorizeUri({
    scope: [OAuthClient.scopes.Accounting, OAuthClient.scopes.OpenId],
    state: 'pappas-qb-connect'
  });
  console.log('🔑 QB Auth - redirect_uri:', process.env.QB_REDIRECT_URI);
  console.log('🔑 QB Auth - environment:', process.env.QB_ENVIRONMENT);
  res.redirect(authUri);
});

// GET /api/quickbooks/callback - Handle OAuth callback
app.get('/api/quickbooks/callback', async (req, res) => {
  try {
    const oauthClient = createOAuthClient();
    const authResponse = await oauthClient.createToken(req.url);
    const token = authResponse.getJson();
    const realmId = req.query.realmId;
    const expiresAt = new Date(Date.now() + (token.expires_in || 3600) * 1000);

    // Clear old tokens and store new ones
    await pool.query('DELETE FROM qb_tokens');
    await pool.query(
      `INSERT INTO qb_tokens (realm_id, access_token, refresh_token, token_type, expires_at) VALUES ($1,$2,$3,$4,$5)`,
      [realmId, token.access_token, token.refresh_token, token.token_type || 'bearer', expiresAt]
    );

    console.log('✅ QuickBooks connected. Realm ID:', realmId);
    res.redirect('/settings.html?qb=connected');
  } catch (e) {
    console.error('QB callback error:', e);
    res.redirect('/settings.html?qb=error&msg=' + encodeURIComponent(e.message));
  }
});

// GET /api/quickbooks/status - Check connection
app.get('/api/quickbooks/status', async (req, res) => {
  try {
    const tokenRow = await pool.query('SELECT realm_id, expires_at, updated_at FROM qb_tokens ORDER BY id DESC LIMIT 1');
    const lastSync = await pool.query('SELECT * FROM qb_sync_log ORDER BY id DESC LIMIT 1');

    if (tokenRow.rows.length === 0) {
      return res.json({ success: true, connected: false });
    }

    const t = tokenRow.rows[0];
    const isExpired = new Date(t.expires_at) <= new Date();

    res.json({
      success: true,
      connected: !isExpired,
      realmId: t.realm_id,
      tokenExpiresAt: t.expires_at,
      connectedAt: t.updated_at,
      lastSync: lastSync.rows[0] || null
    });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// POST /api/quickbooks/disconnect - Remove tokens
app.post('/api/quickbooks/disconnect', async (req, res) => {
  try {
    await pool.query('DELETE FROM qb_tokens');
    res.json({ success: true, message: 'QuickBooks disconnected' });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// --- Sync Functions ---

async function syncQBCustomers(changedSince = null) {
  let count = 0;
  let startPos = 1;
  const pageSize = 100;

  // Ensure qb_id column exists on customers table
  try { await pool.query(`ALTER TABLE customers ADD COLUMN IF NOT EXISTS qb_id VARCHAR(100)`); } catch(e) {}

  const sinceFilter = changedSince ? ` WHERE Metadata.LastUpdatedTime >= '${changedSince}'` : '';
  while (true) {
    const query = `SELECT * FROM Customer${sinceFilter} STARTPOSITION ${startPos} MAXRESULTS ${pageSize}`;
    const data = await qbApiGet(`query?query=${encodeURIComponent(query)}`);
    const customers = data?.QueryResponse?.Customer || [];
    if (customers.length === 0) break;

    for (const c of customers) {
      const qbId = String(c.Id);
      const name = c.DisplayName || ((c.GivenName || '') + ' ' + (c.FamilyName || '')).trim();
      const email = c.PrimaryEmailAddr?.Address || null;
      const phone = c.PrimaryPhone?.FreeFormNumber || null;
      const mobile = c.Mobile?.FreeFormNumber || null;
      const addr = c.BillAddr || {};
      const street = addr.Line1 || null;
      const street2 = addr.Line2 || null;
      const city = addr.City || null;
      const state = addr.CountrySubDivisionCode || null;
      const zip = addr.PostalCode || null;
      const company = c.CompanyName || null;

      // Upsert: match on qb_id, or insert new
      const existing = await pool.query('SELECT id FROM customers WHERE qb_id = $1', [qbId]);
      if (existing.rows.length > 0) {
        await pool.query(
          `UPDATE customers SET name=$1, email=$2, phone=$3, mobile=$4, street=$5, street2=$6,
           city=$7, state=$8, postal_code=$9, customer_company_name=$10, updated_at=NOW() WHERE qb_id=$11`,
          [name, email, phone, mobile, street, street2, city, state, zip, company, qbId]
        );
      } else {
        await pool.query(
          `INSERT INTO customers (name, email, phone, mobile, street, street2, city, state, postal_code,
           customer_company_name, qb_id, status, type, created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'Active','Customer',NOW())`,
          [name, email, phone, mobile, street, street2, city, state, zip, company, qbId]
        );
      }
      count++;
    }

    if (customers.length < pageSize) break;
    startPos += pageSize;
  }
  return count;
}

async function syncQBInvoices(changedSince = null) {
  let count = 0;
  let startPos = 1;
  const pageSize = 100;

  const sinceFilter = changedSince ? ` WHERE Metadata.LastUpdatedTime >= '${changedSince}'` : '';
  while (true) {
    const query = `SELECT * FROM Invoice${sinceFilter} STARTPOSITION ${startPos} MAXRESULTS ${pageSize}`;
    const data = await qbApiGet(`query?query=${encodeURIComponent(query)}`);
    const invoices = data?.QueryResponse?.Invoice || [];
    if (invoices.length === 0) break;

    for (const inv of invoices) {
      const qbId = String(inv.Id);
      const custRef = inv.CustomerRef;

      // Find local customer by QB customer ID
      let customerId = null;
      let customerName = custRef?.name || 'Unknown';
      let customerEmail = inv.BillEmail?.Address || null;
      if (custRef?.value) {
        const localCust = await pool.query('SELECT id, name, email FROM customers WHERE qb_id = $1', [String(custRef.value)]);
        if (localCust.rows.length > 0) {
          customerId = localCust.rows[0].id;
          customerName = localCust.rows[0].name || customerName;
          customerEmail = localCust.rows[0].email || customerEmail;
        }
      }

      // Build line items
      const lineItems = (inv.Line || [])
        .filter(l => l.DetailType === 'SalesItemLineDetail')
        .map(l => ({
          name: l.Description || l.SalesItemLineDetail?.ItemRef?.name || 'Service',
          amount: l.Amount || 0,
          quantity: l.SalesItemLineDetail?.Qty || 1,
          rate: l.SalesItemLineDetail?.UnitPrice || l.Amount || 0
        }));

      const total = parseFloat(inv.TotalAmt) || 0;
      const balance = parseFloat(inv.Balance) || 0;
      const amountPaid = total - balance;
      const status = balance <= 0 && total > 0 ? 'paid' : (inv.DueDate && new Date(inv.DueDate) < new Date() ? 'overdue' : 'sent');
      const invoiceNumber = inv.DocNumber || `QB-${qbId}`;

      // Upsert: match on qb_invoice_id first, then fall back to invoice_number
      // Use ON CONFLICT to handle the unique constraint on invoice_number
      const existing = await pool.query(
        'SELECT id FROM invoices WHERE qb_invoice_id = $1 OR invoice_number = $2 LIMIT 1',
        [qbId, invoiceNumber]
      );
      if (existing.rows.length > 0) {
        await pool.query(
          `UPDATE invoices SET qb_invoice_id=$1, customer_id=$2, customer_name=$3, customer_email=$4,
           status=$5, subtotal=$6, total=$7, amount_paid=$8, due_date=$9, line_items=$10,
           paid_at=$11, updated_at=NOW() WHERE id=$12`,
          [qbId, customerId, customerName, customerEmail, status,
           total, total, amountPaid, inv.DueDate || null, JSON.stringify(lineItems),
           status === 'paid' ? (inv.MetaData?.LastUpdatedTime || new Date()) : null,
           existing.rows[0].id]
        );
      } else {
        await pool.query(
          `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, status,
           subtotal, total, amount_paid, due_date, qb_invoice_id, line_items, paid_at, created_at, updated_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW(),NOW())
           ON CONFLICT (invoice_number) DO UPDATE SET
             qb_invoice_id=EXCLUDED.qb_invoice_id, customer_id=EXCLUDED.customer_id,
             customer_name=EXCLUDED.customer_name, customer_email=EXCLUDED.customer_email,
             status=EXCLUDED.status, subtotal=EXCLUDED.subtotal, total=EXCLUDED.total,
             amount_paid=EXCLUDED.amount_paid, due_date=EXCLUDED.due_date,
             line_items=EXCLUDED.line_items, paid_at=EXCLUDED.paid_at, updated_at=NOW()`,
          [invoiceNumber, customerId, customerName, customerEmail, status,
           total, total, amountPaid, inv.DueDate || null, qbId, JSON.stringify(lineItems),
           status === 'paid' ? (inv.MetaData?.LastUpdatedTime || new Date()) : null]
        );
      }
      count++;
    }

    if (invoices.length < pageSize) break;
    startPos += pageSize;
  }
  return count;
}

async function syncQBPayments(changedSince = null) {
  let count = 0;
  let startPos = 1;
  const pageSize = 100;

  while (true) {
    const sinceFilter = changedSince ? ` WHERE Metadata.LastUpdatedTime >= '${changedSince}'` : '';
    const query = `SELECT * FROM Payment${sinceFilter} STARTPOSITION ${startPos} MAXRESULTS ${pageSize}`;
    const data = await qbApiGet(`query?query=${encodeURIComponent(query)}`);
    const payments = data?.QueryResponse?.Payment || [];
    if (payments.length === 0) break;

    for (const pmt of payments) {
      const lines = pmt.Line || [];
      for (const line of lines) {
        const invoiceRef = line.LinkedTxn?.find(lt => lt.TxnType === 'Invoice');
        if (!invoiceRef) continue;

        const qbInvId = String(invoiceRef.TxnId);
        const localInv = await pool.query('SELECT id, total FROM invoices WHERE qb_invoice_id = $1', [qbInvId]);
        if (localInv.rows.length === 0) continue;

        const invId = localInv.rows[0].id;
        const invTotal = parseFloat(localInv.rows[0].total) || 0;

        // Sum all payments for this invoice
        const paidAmount = parseFloat(line.Amount) || 0;
        await pool.query(
          `UPDATE invoices SET amount_paid = LEAST(amount_paid + $1, total),
           status = CASE WHEN amount_paid + $1 >= total THEN 'paid' ELSE status END,
           paid_at = CASE WHEN amount_paid + $1 >= total THEN $2 ELSE paid_at END,
           updated_at = NOW() WHERE id = $3 AND qb_invoice_id = $4`,
          [paidAmount, pmt.TxnDate || new Date(), invId, qbInvId]
        );
        count++;
      }
    }

    if (payments.length < pageSize) break;
    startPos += pageSize;
  }
  return count;
}

async function syncQBExpenses(changedSince = null) {
  let count = 0;

  // Ensure all required columns exist on expenses table (may have been created with different schema)
  const expCols = [
    ['description', 'TEXT'], ['amount', 'NUMERIC(10,2) DEFAULT 0'], ['category', 'VARCHAR(100)'],
    ['vendor', 'VARCHAR(255)'], ['expense_date', 'DATE'], ['receipt_url', 'TEXT'],
    ['notes', 'TEXT'], ['qb_id', 'VARCHAR(100)']
  ];
  for (const [col, type] of expCols) {
    try { await pool.query(`ALTER TABLE expenses ADD COLUMN IF NOT EXISTS ${col} ${type}`); } catch(e) {}
  }
  // Drop NOT NULL constraints and widen columns that may be too narrow
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN vendor DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN description DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN category DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN expense_date DROP NOT NULL`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN vendor TYPE VARCHAR(500)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN category TYPE VARCHAR(500)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN qb_id TYPE VARCHAR(255)`); } catch(e) {}
  try { await pool.query(`ALTER TABLE expenses ALTER COLUMN description TYPE TEXT`); } catch(e) {}

  // Sync Purchases (Bills, Expenses, Checks)
  for (const entityType of ['Purchase', 'Bill']) {
    let startPos = 1;
    const pageSize = 100;

    while (true) {
      const sinceFilter = changedSince ? ` WHERE Metadata.LastUpdatedTime >= '${changedSince}'` : '';
      const query = `SELECT * FROM ${entityType}${sinceFilter} STARTPOSITION ${startPos} MAXRESULTS ${pageSize}`;
      const data = await qbApiGet(`query?query=${encodeURIComponent(query)}`);
      const items = data?.QueryResponse?.[entityType] || [];
      if (items.length === 0) break;

      for (const item of items) {
        const qbId = `${entityType}-${item.Id}`;
        const lines = item.Line || [];
        const description = lines.map(l => l.Description).filter(Boolean).join('; ') || `${entityType} #${item.Id}`;
        const amount = parseFloat(item.TotalAmt) || 0;
        const vendor = item.EntityRef?.name || null;
        const category = lines[0]?.AccountBasedExpenseLineDetail?.AccountRef?.name
                       || lines[0]?.ItemBasedExpenseLineDetail?.ItemRef?.name
                       || entityType;
        const expenseDate = item.TxnDate || null;

        const existing = await pool.query('SELECT id FROM expenses WHERE qb_id = $1', [qbId]);
        if (existing.rows.length > 0) {
          await pool.query(
            `UPDATE expenses SET description=$1, amount=$2, category=$3, vendor=$4, expense_date=$5 WHERE qb_id=$6`,
            [description, amount, category, vendor, expenseDate, qbId]
          );
        } else {
          await pool.query(
            `INSERT INTO expenses (description, amount, category, vendor, expense_date, qb_id, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,NOW())`,
            [description, amount, category, vendor, expenseDate, qbId]
          );
        }
        count++;
      }

      if (items.length < pageSize) break;
      startPos += pageSize;
    }
  }
  return count;
}

// Track active sync state in memory
let activeSyncLogId = null;
let activeSyncProgress = null; // { stage, customers, invoices, payments, expenses, errors }

// POST /api/quickbooks/sync - Start background sync (returns immediately)
app.post('/api/quickbooks/sync', async (req, res) => {
  try {
    // Verify connection first
    await getQBClient();

    // If a sync is already running, return its log ID
    if (activeSyncLogId !== null) {
      return res.json({ success: true, logId: activeSyncLogId, status: 'already_running' });
    }

    // Create sync log entry
    const logEntry = await pool.query(
      `INSERT INTO qb_sync_log (sync_type, started_at) VALUES ('full', NOW()) RETURNING id`
    );
    const logId = logEntry.rows[0].id;
    activeSyncLogId = logId;
    activeSyncProgress = { stage: 'customers', customers: 0, invoices: 0, payments: 0, expenses: 0, errors: [] };

    // Return immediately — sync runs in background
    res.json({ success: true, logId, status: 'started' });

    // Run sync in background (no await here)
    (async () => {
      const results = { customers: 0, invoices: 0, payments: 0, expenses: 0, errors: [] };

      // Get last successful sync time so we only fetch records changed since then
      let changedSince = null;
      try {
        const lastSync = await pool.query(
          `SELECT completed_at FROM qb_sync_log WHERE completed_at IS NOT NULL ORDER BY id DESC LIMIT 1`
        );
        if (lastSync.rows.length > 0) {
          changedSince = lastSync.rows[0].completed_at.toISOString().split('T')[0]; // YYYY-MM-DD
          console.log(`QB incremental sync: only fetching records changed since ${changedSince}`);
        } else {
          console.log('QB full sync: no previous sync found, fetching all records');
        }
      } catch (e) {
        console.error('Could not determine last sync time, running full sync:', e.message);
      }

      try {
        activeSyncProgress.stage = 'customers';
        results.customers = await syncQBCustomers(changedSince);
        activeSyncProgress.customers = results.customers;
      } catch (e) {
        results.errors.push('Customers: ' + e.message);
        activeSyncProgress.errors.push('Customers: ' + e.message);
        console.error('QB sync customers error:', e);
      }

      try {
        activeSyncProgress.stage = 'invoices';
        results.invoices = await syncQBInvoices(changedSince);
        activeSyncProgress.invoices = results.invoices;
      } catch (e) {
        results.errors.push('Invoices: ' + e.message);
        activeSyncProgress.errors.push('Invoices: ' + e.message);
        console.error('QB sync invoices error:', e);
      }

      try {
        activeSyncProgress.stage = 'payments';
        results.payments = await syncQBPayments(changedSince);
        activeSyncProgress.payments = results.payments;
      } catch (e) {
        results.errors.push('Payments: ' + e.message);
        activeSyncProgress.errors.push('Payments: ' + e.message);
        console.error('QB sync payments error:', e);
      }

      try {
        activeSyncProgress.stage = 'expenses';
        results.expenses = await syncQBExpenses(changedSince);
        activeSyncProgress.expenses = results.expenses;
      } catch (e) {
        results.errors.push('Expenses: ' + e.message);
        activeSyncProgress.errors.push('Expenses: ' + e.message);
        console.error('QB sync expenses error:', e);
      }

      // Update sync log as completed
      await pool.query(
        `UPDATE qb_sync_log SET customers_synced=$1, invoices_synced=$2, payments_synced=$3,
         expenses_synced=$4, errors=$5, completed_at=NOW() WHERE id=$6`,
        [results.customers, results.invoices, results.payments, results.expenses,
         results.errors.length ? results.errors.join('; ') : null, logId]
      );

      activeSyncProgress.stage = 'done';
      activeSyncLogId = null;
      console.log('✅ QB sync complete:', results);
    })();

  } catch (e) {
    activeSyncLogId = null;
    activeSyncProgress = null;
    console.error('QB sync error:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// GET /api/quickbooks/sync-progress - Poll progress of running sync
app.get('/api/quickbooks/sync-progress', async (req, res) => {
  if (activeSyncLogId === null) {
    // No active sync — return last completed log entry
    const last = await pool.query('SELECT * FROM qb_sync_log ORDER BY id DESC LIMIT 1');
    return res.json({
      running: false,
      lastSync: last.rows[0] || null
    });
  }
  res.json({
    running: true,
    logId: activeSyncLogId,
    progress: activeSyncProgress
  });
});

// GET /api/quickbooks/sync-log - Get sync history
app.get('/api/quickbooks/sync-log', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM qb_sync_log ORDER BY id DESC LIMIT 20');
    res.json({ success: true, logs: result.rows });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════

app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));
app.get('/api/config/maps-key', (req, res) => res.json({ key: process.env.GOOGLE_MAPS_API_KEY || '' }));
app.get('*', (req, res) => {
  // Only fall back to index.html for routes that don't match a static file
  const filePath = path.join(__dirname, 'public', req.path);
  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    return res.sendFile(filePath);
  }
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));

process.on('SIGTERM', async () => { await pool.end(); process.exit(0); });
