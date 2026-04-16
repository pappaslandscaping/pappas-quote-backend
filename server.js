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
const crypto = require('crypto');
const rateLimit = require('express-rate-limit');
const cheerio = require('cheerio');
const { ApiError, ValidationError, NotFoundError, IntegrationError } = require('./lib/api-error');
const { validate, schemas } = require('./lib/validate');
const {
  ADMIN_USERS_TABLE,
  hashPassword,
  ensurePaymentsTables: _ensurePaymentsTables,
  ensureInvoicesTable: _ensureInvoicesTable,
  ensureQuoteEventsTable: _ensureQuoteEventsTable,
  ensureCustomerReviewsTable: _ensureCustomerReviewsTable,
  ensureQBTables: _ensureQBTables,
  ensureCopilotSyncTables: _ensureCopilotSyncTables,
  runStartupMigrations,
  runStartupTableInit,
} = require('./lib/startup-schema');

// ═══════════════════════════════════════════════════════════
// SECURITY HELPERS
// ═══════════════════════════════════════════════════════════

// HTML-escape user input before inserting into email templates
function escapeHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Safe error response — never leak internal details to clients
function serverError(res, error, context = 'Server error') {
  console.error(`${context}:`, error);
  res.status(500).json({ success: false, error: 'Something went wrong. Please try again.' });
}

// Square Payments Configuration (optional — server runs fine without it)
let squareClient = null;
let SquareApiError = null;
const SQUARE_APP_ID = process.env.SQUARE_APPLICATION_ID || '';
const SQUARE_LOCATION_ID = process.env.SQUARE_LOCATION_ID || '';
try {
  const square = require('square');
  SquareApiError = square.SquareError || square.ApiError;
  if (process.env.SQUARE_ACCESS_TOKEN) {
    // New SDK uses SquareClient, old SDK uses Client
    if (square.SquareClient) {
      const env = process.env.SQUARE_ENVIRONMENT === 'production' ? square.SquareEnvironment.Production : square.SquareEnvironment.Sandbox;
      squareClient = new square.SquareClient({
        token: process.env.SQUARE_ACCESS_TOKEN,
        environment: env,
      });
    } else {
      squareClient = new square.Client({
        accessToken: process.env.SQUARE_ACCESS_TOKEN,
        environment: process.env.SQUARE_ENVIRONMENT === 'production' ? square.Environment.Production : square.Environment.Sandbox,
      });
    }
    // Add compatibility shims for new SDK (v41+) to match old API style
    if (squareClient && !squareClient.cardsApi && squareClient.cards) {
      const wrapApi = (api, methodMap) => {
        return new Proxy({}, {
          get(_, prop) {
            // Map old method names to new (e.g. createCustomer -> create, createCard -> create, createPayment -> create)
            const mapped = methodMap[prop] || prop;
            if (typeof api[mapped] === 'function') {
              return async (...args) => {
                const res = await api[mapped](...args);
                return { result: res };
              };
            }
            return undefined;
          }
        });
      };
      squareClient.customersApi = wrapApi(squareClient.customers, {
        createCustomer: 'create', listCustomers: 'list', searchCustomers: 'search',
        retrieveCustomer: 'get', updateCustomer: 'update', deleteCustomer: 'delete'
      });
      squareClient.cardsApi = wrapApi(squareClient.cards, {
        createCard: 'create', listCards: 'list', retrieveCard: 'get', disableCard: 'disable'
      });
      squareClient.paymentsApi = wrapApi(squareClient.payments, {
        createPayment: 'create', listPayments: 'list', getPayment: 'get', cancelPayment: 'cancel'
      });
      squareClient.locationsApi = wrapApi(squareClient.locations, {
        listLocations: 'list', retrieveLocation: 'get'
      });
    }
    console.log('✅ Square SDK initialized (' + (process.env.SQUARE_ENVIRONMENT || 'sandbox') + ')');
  } else {
    console.log('⚠️ Square SDK loaded but no SQUARE_ACCESS_TOKEN set');
  }
} catch (err) {
  console.error('⚠️ Square SDK not available:', err.message);
}

// Anthropic Claude AI Configuration (optional — server runs fine without it)
let anthropicClient = null;
try {
  const Anthropic = require('@anthropic-ai/sdk');
  if (process.env.ANTHROPIC_API_KEY) {
    anthropicClient = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
    console.log('✅ Anthropic Claude AI initialized');
  } else {
    console.log('⚠️ Anthropic SDK loaded but no ANTHROPIC_API_KEY set');
  }
} catch (err) {
  console.log('⚠️ Anthropic SDK not available:', err.message);
}

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

const uploadPdf = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'application/pdf' || file.originalname.endsWith('.pdf')) {
      cb(null, true);
    } else {
      cb(new Error('Only PDF files are allowed'));
    }
  }
});

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors({
  origin: process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',') : true,
  credentials: true
}));

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  next();
});

// Square webhook — needs raw body for signature verification, must be before express.json()
app.post('/api/webhooks/square', express.raw({ type: 'application/json' }), async (req, res) => {
  try {
    const signature = req.headers['x-square-hmacsha256-signature'];
    const body = req.body.toString('utf8');
    const sigKey = process.env.SQUARE_WEBHOOK_SIGNATURE_KEY;

    // Verify signature if key is configured
    if (sigKey && signature) {
      const hmac = crypto.createHmac('sha256', sigKey);
      const notificationUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com') + '/api/webhooks/square';
      hmac.update(notificationUrl + body);
      const expectedSig = hmac.digest('base64');
      if (signature !== expectedSig) {
        console.error('Square webhook signature mismatch');
        return res.status(401).send('Invalid signature');
      }
    }

    const event = JSON.parse(body);
    console.log('Square webhook:', event.type, event.data?.id);

    if (event.type === 'payment.completed') {
      const payment = event.data?.object?.payment;
      if (payment) {
        await pool.query(
          `UPDATE payments SET status = 'completed', paid_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE square_payment_id = $1`,
          [payment.id]
        );
        console.log('Webhook: payment.completed for', payment.id);
      }
    } else if (event.type === 'payment.failed') {
      const payment = event.data?.object?.payment;
      if (payment) {
        await pool.query(
          `UPDATE payments SET status = 'failed', failure_reason = $1, updated_at = CURRENT_TIMESTAMP WHERE square_payment_id = $2`,
          [payment.status || 'failed', payment.id]
        );
      }
    } else if (event.type === 'refund.created' || event.type === 'refund.updated') {
      const refund = event.data?.object?.refund;
      if (refund && refund.payment_id) {
        const refundAmount = (refund.amount_money?.amount || 0) / 100;
        await pool.query(
          `UPDATE payments SET refund_amount = $1, updated_at = CURRENT_TIMESTAMP WHERE square_payment_id = $2`,
          [refundAmount, refund.payment_id]
        );
      }
    }

    res.status(200).json({ received: true });
  } catch (error) {
    console.error('Square webhook error:', error);
    res.status(200).json({ received: true });
  }
});

// Force HTTPS in production (Railway terminates SSL at proxy)
if (process.env.NODE_ENV === 'production') {
  app.use((req, res, next) => {
    if (req.headers['x-forwarded-proto'] !== 'https') {
      return res.redirect(301, `https://${req.headers.host}${req.url}`);
    }
    // HSTS: tell browser to always use HTTPS for 1 year
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
    next();
  });
}

app.use(express.json({ limit: '15mb' }));
app.use(express.urlencoded({ limit: '15mb', extended: true }));
app.use(express.static('public', {
  dotfiles: 'allow',
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

// Handle unexpected pool errors so the server doesn't crash
pool.on('error', (err) => {
  console.error('Unexpected database pool error:', err);
});

// ═══════════════════════════════════════════════════════════
// RATE LIMITING — protect public endpoints from abuse
// ═══════════════════════════════════════════════════════════
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 15,
  message: { success: false, error: 'Too many login attempts. Please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});
const publicApiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 30,
  message: { success: false, error: 'Too many requests. Please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});
const paymentLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  message: { success: false, error: 'Too many payment attempts. Please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});

// Apply rate limiters to public-facing routes
app.use('/api/auth/login', loginLimiter);
app.use('/api/quotes', publicApiLimiter);
app.use('/api/sign', publicApiLimiter);
app.use('/api/pay', paymentLimiter);

// ═══════════════════════════════════════════════════════════
// GLOBAL AUTH MIDDLEWARE — require auth by default, allowlist public routes
// ═══════════════════════════════════════════════════════════
const PUBLIC_ROUTE_PREFIXES = [
  '/api/webhooks/',        // Square, quote-accepted, quote-declined, customer-replied
  '/api/sign/',            // Customer quote signing (token auth)
  '/api/pay/',             // Customer payment (token auth)
  '/api/portal/',          // Customer portal (token auth)
  '/api/cron/',            // Cron jobs (called by cron service)
  '/api/t/',               // Email tracking pixels/clicks
  '/api/season-kickoff/confirm/', // Customer confirmation (token auth)
  '/api/season-kickoff/track/',   // Tracking pixel
  '/api/mms-image/',       // Media serving
];

const PUBLIC_ROUTE_EXACT = new Set([
  '/api/auth/login',
  '/api/auth/forgot-password',
  '/api/auth/reset-password',
  '/api/services',                  // Public service list
  '/api/sms/webhook',               // Twilio inbound
  '/api/app/login',                 // Mobile app login
  '/api/app/voice/debug',           // Twilio debug
  '/api/app/calls/status-callback', // Twilio callback
  '/api/app/calls/connect',         // Twilio TwiML connect
  '/api/app/voice/connect',         // Twilio TwiML voice connect
  '/api/app/calls/hold-music',      // Twilio hold music
  '/api/unsubscribe',               // Customer unsubscribe
  '/api/config/maps-key',           // Public config
  '/api/pay/config',                // Payment config (Square app ID)
  '/api/square/status',             // Square connection status (pay page needs it)
  '/api/quickbooks/auth',           // QB OAuth initiation
  '/api/quickbooks/callback',       // QB OAuth callback
  '/api/service-complete-email',    // Service completion email trigger
  '/health',                        // Health check
]);

// Routes that are public only for specific HTTP methods
const PUBLIC_ROUTE_METHODS = {
  'POST /api/quotes': true,              // Public quote request form
  'POST /api/campaigns/submissions': true, // Public campaign form
};

function isPublicRoute(method, path) {
  // Exact match
  if (PUBLIC_ROUTE_EXACT.has(path)) return true;

  // Prefix match
  for (const prefix of PUBLIC_ROUTE_PREFIXES) {
    if (path.startsWith(prefix)) return true;
  }

  // Method-specific match
  if (PUBLIC_ROUTE_METHODS[`${method} ${path}`]) return true;

  // Pattern match for customer-facing parameterized routes
  if (/^\/api\/sent-quotes\/\d+\/sign-contract$/.test(path)) return true;
  if (/^\/api\/sent-quotes\/by-token\//.test(path)) return true;
  if (/^\/api\/customers\/\d+\/card-on-file$/.test(path)) return true;
  if (/^\/api\/test-quote-pdf\//.test(path)) return true;

  return false;
}

app.use((req, res, next) => {
  // Only apply to /api routes
  if (!req.path.startsWith('/api/') && req.path !== '/health') return next();

  // Skip auth for public routes
  if (isPublicRoute(req.method, req.path)) return next();

  // Require valid JWT for everything else
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ success: false, error: 'Authentication required' });

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ success: false, error: 'Invalid or expired token' });
  }
});

// ═══════════════════════════════════════════════════════════
// ADMIN-ONLY GATE — restrict sensitive routes to non-employee admins
// ═══════════════════════════════════════════════════════════
function requireAdmin(req, res, next) {
  if (!req.user || req.user.isEmployee) {
    return res.status(403).json({ success: false, error: 'Admin access required' });
  }
  next();
}

// Sensitive routes that only owner/admin (not employee) should access
app.use('/api/employees', requireAdmin);
app.use('/api/settings', requireAdmin);
app.use('/api/quickbooks', (req, res, next) => {
  // Allow public OAuth flow endpoints
  if (req.path === '/auth' || req.path === '/callback') return next();
  return requireAdmin(req, res, next);
});
app.use('/api/copilot', requireAdmin);
app.use('/api/copilotcrm', requireAdmin);
app.use('/api/import-customers', requireAdmin);
app.use('/api/import-scheduling', requireAdmin);
app.use('/api/import-properties', requireAdmin);
app.use('/api/broadcasts/send', requireAdmin);
app.use('/api/auth/service-token', requireAdmin);

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
const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) { console.error('❌ FATAL: JWT_SECRET environment variable is required'); process.exit(1); }
let twilioClient = null;
try {
  if (TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN) {
    twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
    console.log('Twilio client initialized');
  } else {
    console.log('Twilio credentials not set - SMS/voice features disabled');
  }
} catch (err) {
  console.log('Twilio init failed:', err.message);
}

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

// ═══════════════════════════════════════════════════════════
// ADMIN AUTHENTICATION SYSTEM
// ═══════════════════════════════════════════════════════════

// ADMIN_USERS_TABLE and hashPassword imported from lib/startup-schema.js

function verifyPassword(password, stored) {
  const [salt, hash] = stored.split(':');
  if (!salt || !hash) {
    // Legacy format (old hardcoded salt) — rehash on next login
    const legacySalt = 'pappas-admin-salt-2026';
    return crypto.pbkdf2Sync(password, legacySalt, 100000, 64, 'sha512').toString('hex') === stored;
  }
  return crypto.pbkdf2Sync(password, salt, 100000, 64, 'sha512').toString('hex') === hash;
}

// Admin + Employee login
app.post('/api/auth/login', validate(schemas.login), async (req, res) => {
  try {
    const { email, password } = req.body;
    console.log(`🔐 Login attempt for: ${email || '(no email)'}`);
    const emailLower = email.toLowerCase().trim();

    // Try admin_users first
    await pool.query(ADMIN_USERS_TABLE);
    const adminResult = await pool.query('SELECT * FROM admin_users WHERE email = $1', [emailLower]);
    if (adminResult.rows.length > 0) {
      const user = adminResult.rows[0];
      if (!verifyPassword(password, user.password_hash)) {
        console.log(`🔐 Login failed: password mismatch for ${email}`);
        return res.status(401).json({ success: false, error: 'Invalid email or password' });
      }
      // Rehash with random salt if still using legacy format
      if (!user.password_hash.includes(':')) {
        const newHash = hashPassword(password);
        await pool.query('UPDATE admin_users SET password_hash = $1 WHERE id = $2', [newHash, user.id]);
      }
      await pool.query('UPDATE admin_users SET last_login = NOW() WHERE id = $1', [user.id]);
      const token = jwt.sign({ id: user.id, email: user.email, name: user.name, role: user.role, isAdmin: true }, JWT_SECRET, { expiresIn: '7d' });
      console.log(`🔐 Admin login successful: ${user.email}`);
      return res.json({ success: true, token, name: user.name, email: user.email, role: user.role, isAdmin: true });
    }

    // Try employees table
    const empResult = await pool.query('SELECT * FROM employees WHERE login_email = $1 AND is_active = true', [emailLower]);
    if (empResult.rows.length > 0) {
      const emp = empResult.rows[0];
      if (!emp.password_hash || !verifyPassword(password, emp.password_hash)) {
        console.log(`🔐 Login failed: password mismatch for employee ${email}`);
        return res.status(401).json({ success: false, error: 'Invalid email or password' });
      }
      // Rehash with random salt if still using legacy format
      if (!emp.password_hash.includes(':')) {
        const newHash = hashPassword(password);
        await pool.query('UPDATE employees SET password_hash = $1 WHERE id = $2', [newHash, emp.id]);
      }
      await pool.query('UPDATE employees SET updated_at = NOW() WHERE id = $1', [emp.id]);
      const empName = emp.first_name + ' ' + emp.last_name;
      const token = jwt.sign({ id: emp.id, email: emp.login_email, name: empName, role: emp.title || 'employee', isAdmin: true, isEmployee: true, employeeId: emp.id, permissions: emp.permissions }, JWT_SECRET, { expiresIn: '7d' });
      console.log(`🔐 Employee login successful: ${emp.login_email} (${empName})`);
      return res.json({ success: true, token, name: empName, email: emp.login_email, role: emp.title || 'Employee', isEmployee: true, permissions: emp.permissions });
    }

    console.log(`🔐 Login failed: no user found for ${email}`);
    res.status(401).json({ success: false, error: 'Invalid email or password' });
  } catch (error) {
    console.error('Login error:', error);
    serverError(res, error);
  }
});

// Verify token (admin or employee) — global middleware already verified JWT and set req.user
app.get('/api/auth/me', (req, res) => {
  if (!req.user?.isAdmin) return res.status(403).json({ success: false, error: 'Not authorized' });
  res.json({ success: true, user: { email: req.user.email, name: req.user.name, role: req.user.role, isEmployee: !!req.user.isEmployee, permissions: req.user.permissions || null } });
});

// Generate long-lived service token for N8N / automation use
app.post('/api/auth/service-token', authenticateToken, (req, res) => {
  if (!req.user.isAdmin) return res.status(403).json({ success: false, error: 'Admin access required' });
  const payload = { id: req.user.id, email: req.user.email, name: req.user.name, role: req.user.role, isAdmin: true, isServiceToken: true };
  const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '1y' });
  const decoded = jwt.decode(token);
  const expiresAt = new Date(decoded.exp * 1000).toISOString();
  console.log(`🔑 Service token generated by ${req.user.email}, expires ${expiresAt}`);
  res.json({ success: true, token, expiresAt });
});

// Change password — global middleware already verified JWT and set req.user
app.post('/api/auth/change-password', validate(schemas.changePassword), async (req, res) => {
  try {
    if (!req.user?.isAdmin) return res.status(403).json({ success: false, error: 'Not admin' });
    const { current_password, new_password } = req.body;

    const result = await pool.query('SELECT password_hash FROM admin_users WHERE id = $1', [req.user.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'User not found' });
    if (!verifyPassword(current_password, result.rows[0].password_hash)) return res.status(401).json({ success: false, error: 'Current password is incorrect' });
    const newHash = hashPassword(new_password);
    await pool.query('UPDATE admin_users SET password_hash = $1 WHERE id = $2', [newHash, req.user.id]);
    res.json({ success: true, message: 'Password changed' });
  } catch (err) {
    serverError(res, err, 'Change password error');
  }
});

// Forgot password — sends reset link via email
app.post('/api/auth/forgot-password', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ success: false, error: 'Email required' });
    const emailLower = email.toLowerCase().trim();

    // Always return success to avoid email enumeration
    const successMsg = { success: true, message: 'If an account exists, a reset link has been sent.' };

    // Check admin_users
    const result = await pool.query('SELECT id, email, name FROM admin_users WHERE email = $1', [emailLower]);
    if (result.rows.length === 0) {
      // Check employees
      const empResult = await pool.query('SELECT id, login_email, first_name FROM employees WHERE login_email = $1 AND is_active = true', [emailLower]);
      if (empResult.rows.length === 0) return res.json(successMsg);
    }

    const user = result.rows[0];
    const isEmployee = !user;
    const userId = user ? user.id : (await pool.query('SELECT id FROM employees WHERE login_email = $1 AND is_active = true', [emailLower])).rows[0]?.id;
    if (!userId) return res.json(successMsg);

    // Generate secure token
    const token = crypto.randomBytes(32).toString('hex');
    const expiresAt = new Date(Date.now() + 60 * 60 * 1000); // 1 hour

    // Store token (create table if needed)
    await pool.query(`CREATE TABLE IF NOT EXISTS password_reset_tokens (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      user_type VARCHAR(20) NOT NULL DEFAULT 'admin',
      token VARCHAR(255) UNIQUE NOT NULL,
      expires_at TIMESTAMP NOT NULL,
      used BOOLEAN DEFAULT false,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);

    // Invalidate any existing tokens for this user
    await pool.query('UPDATE password_reset_tokens SET used = true WHERE user_id = $1 AND user_type = $2', [userId, isEmployee ? 'employee' : 'admin']);

    // Insert new token
    await pool.query('INSERT INTO password_reset_tokens (user_id, user_type, token, expires_at) VALUES ($1, $2, $3, $4)', [userId, isEmployee ? 'employee' : 'admin', token, expiresAt]);

    // Send reset email
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const resetUrl = `${baseUrl}/reset-password.html?token=${token}`;
    const userName = user ? (user.name || emailLower) : emailLower;

    const emailContent = `
      <h2 style="color:#1e293b;font-size:22px;font-weight:600;margin:0 0 12px;">Reset Your Password</h2>
      <p style="color:#475569;font-size:15px;line-height:1.6;">Hi ${escapeHtml(userName.split(' ')[0] || 'there')},</p>
      <p style="color:#475569;font-size:15px;line-height:1.6;">We received a request to reset your YardDesk password. Click the button below to choose a new password:</p>
      <div style="text-align:center;margin:28px 0;">
        <a href="${resetUrl}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:#ffffff;text-decoration:none;border-radius:8px;font-size:15px;font-weight:600;">Reset Password</a>
      </div>
      <p style="color:#94a3b8;font-size:13px;line-height:1.5;">This link expires in 1 hour. If you didn't request this, you can safely ignore this email.</p>
    `;

    await sendEmail(emailLower, 'Reset Your YardDesk Password', emailTemplate(emailContent, { showSignature: false }));
    console.log(`🔐 Password reset email sent to ${emailLower}`);
    res.json(successMsg);
  } catch (error) {
    console.error('Forgot password error:', error);
    serverError(res, error, 'Forgot password error');
  }
});

// Reset password — validates token and sets new password
app.post('/api/auth/reset-password', async (req, res) => {
  try {
    const { token, new_password } = req.body;
    if (!token || !new_password) return res.status(400).json({ success: false, error: 'Token and new password required' });
    if (new_password.length < 8) return res.status(400).json({ success: false, error: 'Password must be at least 8 characters' });

    // Look up token
    const result = await pool.query('SELECT * FROM password_reset_tokens WHERE token = $1 AND used = false AND expires_at > NOW()', [token]);
    if (result.rows.length === 0) {
      return res.status(400).json({ success: false, error: 'This reset link has expired or already been used. Please request a new one.' });
    }

    const resetRecord = result.rows[0];
    const newHash = hashPassword(new_password);

    // Update password based on user type
    if (resetRecord.user_type === 'employee') {
      await pool.query('UPDATE employees SET password_hash = $1 WHERE id = $2', [newHash, resetRecord.user_id]);
    } else {
      await pool.query('UPDATE admin_users SET password_hash = $1 WHERE id = $2', [newHash, resetRecord.user_id]);
    }

    // Mark token as used
    await pool.query('UPDATE password_reset_tokens SET used = true WHERE id = $1', [resetRecord.id]);

    console.log(`🔐 Password reset successful for ${resetRecord.user_type} id=${resetRecord.user_id}`);
    res.json({ success: true, message: 'Password reset successfully' });
  } catch (error) {
    console.error('Reset password error:', error);
    serverError(res, error, 'Reset password error');
  }
});

// Old requireAdmin removed — auth now handled by global middleware at top of file


// PUBLIC: Season kickoff confirmation (no auth — customers click from email)
app.get('/api/season-kickoff/confirm/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const result = await pool.query('SELECT * FROM season_kickoff_responses WHERE token = $1', [token]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Invalid or expired link' });
    const row = result.rows[0];
    // Track views: first view timestamp + total count
    await pool.query('UPDATE season_kickoff_responses SET viewed_at = COALESCE(viewed_at, NOW()), view_count = COALESCE(view_count, 0) + 1 WHERE token = $1', [token]);
    res.json({ success: true, customerName: row.customer_name, services: row.services, status: row.status });
  } catch (error) {
    console.error('Error loading kickoff confirmation:', error);
    res.status(500).json({ success: false, error: 'Something went wrong' });
  }
});

app.post('/api/season-kickoff/confirm/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const { response, notes } = req.body;
    const result = await pool.query(
      `UPDATE season_kickoff_responses SET status = $1, notes = $2, responded_at = NOW() WHERE token = $3 RETURNING *`,
      [response, notes || '', token]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Invalid link' });

    // Notify admin about the response
    const row = result.rows[0];
    const statusLabel = response === 'confirmed' ? 'Confirmed Services' : 'Requested Changes';
    let svcList = [];
    try { svcList = typeof row.services === 'string' ? JSON.parse(row.services) : (row.services || []); } catch(e) {}
    const svcRows = svcList.filter(s => { const l = (s.name||'').toLowerCase(); return !l.includes('snow') && !l.includes('salt') && !l.includes('deic'); })
      .map(s => `<tr><td style="padding:6px 12px;border-bottom:1px solid #e5e5e5;font-size:14px;color:#334155;">${escapeHtml(s.name)}</td><td style="padding:6px 12px;border-bottom:1px solid #e5e5e5;font-size:14px;color:#334155;text-align:right;font-weight:600;">${parseFloat(s.rate).toFixed(2)}</td></tr>`).join('');
    const svcTable = svcRows ? `<table style="width:100%;border-collapse:collapse;margin:12px 0 16px;"><thead><tr style="background:#f8fafc;"><th style="padding:6px 12px;text-align:left;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;border-bottom:2px solid #e5e5e5;">Service</th><th style="padding:6px 12px;text-align:right;font-size:11px;font-weight:700;color:#64748b;text-transform:uppercase;border-bottom:2px solid #e5e5e5;">Rate</th></tr></thead><tbody>${svcRows}</tbody></table>` : '';
    let propList = [];
    try { propList = typeof row.properties === 'string' ? JSON.parse(row.properties) : (row.properties || []); } catch(e) {}
    const addrHtml = propList.filter(Boolean).map(p => escapeHtml(p)).join('<br>');
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const appLink = `${baseUrl}/season-kickoff.html?tab=responses`;
    const replyMailto = row.customer_email ? `mailto:${row.customer_email}?subject=${encodeURIComponent(`Re: Your 2026 Services — Pappas & Co. Landscaping`)}&body=${encodeURIComponent(`Hi ${(row.customer_name || '').split(' ')[0]},\n\nThank you for letting us know! `)}` : '';
    const actionButtons = `<div style="margin:20px 0 8px;text-align:center;">
      ${replyMailto ? `<a href="${replyMailto}" style="display:inline-block;padding:12px 24px;background:#2e403d;color:#ffffff;text-decoration:none;border-radius:8px;font-size:14px;font-weight:600;margin-right:10px;">Reply to Customer</a>` : ''}
      <a href="${appLink}" style="display:inline-block;padding:12px 24px;background:#f1f5f9;color:#2e403d;text-decoration:none;border-radius:8px;font-size:14px;font-weight:600;border:1px solid #e2e8f0;">View in App</a>
    </div>`;
    const notifyHtml = emailTemplate(`
      <h2 style="font-size:20px;color:#1e293b;font-weight:700;margin:0 0 16px;">Season Kickoff Response</h2>
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 12px;">
        <strong>${escapeHtml(row.customer_name)}</strong> has <strong>${statusLabel.toLowerCase()}</strong>.
      </p>
      ${row.customer_email ? `<p style="font-size:14px;color:#64748b;margin:0 0 8px;">Contact: <a href="mailto:${escapeHtml(row.customer_email)}" style="color:#2e403d;">${escapeHtml(row.customer_email)}</a></p>` : ''}
      ${addrHtml ? `<p style="font-size:14px;color:#64748b;margin:0 0 12px;">Address: ${addrHtml}</p>` : ''}
      ${svcTable}
      ${notes ? `<p style="font-size:14px;color:#475569;margin:12px 0;padding:12px;background:#f8fafc;border-radius:8px;border-left:3px solid #2e403d;"><strong>Notes:</strong> ${escapeHtml(notes)}</p>` : ''}
      ${actionButtons}
    `);
    sendEmail('hello@pappaslandscaping.com', `Season Kickoff: ${escapeHtml(row.customer_name)} — ${statusLabel}`, notifyHtml).catch(err => console.error('Notify email error:', err));

    // Log to customer profile notes
    const today = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    const noteText = response === 'confirmed'
      ? `[${today}] Season Kickoff: Confirmed services for 2026.`
      : `[${today}] Season Kickoff: Requested changes — ${notes || 'no details provided'}.`;
    // Find customer_id by name match if not stored
    const custLookup = row.customer_id
      ? { rows: [{ id: row.customer_id }] }
      : await pool.query(`SELECT id FROM customers WHERE LOWER(TRIM(COALESCE(name, first_name || ' ' || last_name))) = LOWER(TRIM($1)) LIMIT 1`, [row.customer_name]);
    if (custLookup.rows.length > 0) {
      const custId = custLookup.rows[0].id;
      await pool.query(
        `UPDATE customers SET notes = CASE WHEN notes IS NULL OR notes = '' THEN $1 ELSE notes || E'\n' || $1 END, updated_at = CURRENT_TIMESTAMP WHERE id = $2`,
        [noteText, custId]
      );
    }

    res.json({ success: true });
  } catch (error) {
    console.error('Error confirming kickoff:', error);
    res.status(500).json({ success: false, error: 'Something went wrong' });
  }
});

// GET /api/season-kickoff/track/:token - Email open tracking pixel (public)
app.get('/api/season-kickoff/track/:token', async (req, res) => {
  try {
    const { token } = req.params;
    await pool.query(
      `UPDATE season_kickoff_responses SET email_opened_at = COALESCE(email_opened_at, NOW()), email_open_count = COALESCE(email_open_count, 0) + 1 WHERE token = $1`,
      [token]
    );
  } catch (e) { /* silent */ }
  // Return 1x1 transparent GIF
  const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 'base64');
  res.set({ 'Content-Type': 'image/gif', 'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate', 'Pragma': 'no-cache', 'Expires': '0' });
  res.send(pixel);
});

// Auth middleware applied globally at top of file — see PUBLIC_ROUTE_PREFIXES and PUBLIC_ROUTE_EXACT

async function sendEmail(to, subject, html, attachments = null, meta = {}) {
  if (!RESEND_API_KEY) return;
  try {
    const payload = { from: FROM_EMAIL, to: [to], subject, html };
    if (attachments) {
      payload.attachments = attachments;
      console.log(`📎 Email attachments: ${attachments.length} file(s), sizes: ${attachments.map(a => a.content ? Math.round(a.content.length * 0.75 / 1024) + 'KB' : 'unknown').join(', ')}`);
    }
    const resp = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const respBody = await resp.text();
    if (!resp.ok) {
      console.error(`❌ Resend API error (${resp.status}):`, respBody);
      // Log failed email
      try { await pool.query(`INSERT INTO email_log (recipient_email, subject, email_type, customer_id, customer_name, invoice_id, quote_id, status, error_message, html_body) VALUES ($1,$2,$3,$4,$5,$6,$7,'failed',$8,$9)`,
        [to, subject, meta.type||'general', meta.customer_id||null, meta.customer_name||null, meta.invoice_id||null, meta.quote_id||null, respBody, html]); } catch(e) {}
    } else {
      console.log(`✅ Email sent to ${to}:`, respBody);
      // Log successful email
      try { await pool.query(`INSERT INTO email_log (recipient_email, subject, email_type, customer_id, customer_name, invoice_id, quote_id, status, html_body) VALUES ($1,$2,$3,$4,$5,$6,$7,'sent',$8)`,
        [to, subject, meta.type||'general', meta.customer_id||null, meta.customer_name||null, meta.invoice_id||null, meta.quote_id||null, html]); } catch(e) {}
    }
  } catch (err) { console.error('Email failed:', err); }
}

// Helper: split address into street line + city/state/zip line
function formatAddressLines(addr) {
  if (!addr) return { line1: '', line2: '' };
  const trimmed = addr.trim();

  // Normalize: remove all commas and work with clean string
  // This handles "123 Main St, Valley View, OH 44107" AND "123 Main St Valley View OH, 44125"
  const clean = trimmed.replace(/,/g, ' ').replace(/\s+/g, ' ').trim();

  // Try to detect "STATE ZIP" at the end
  const stateZipMatch = clean.match(/^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
  if (stateZipMatch) {
    const beforeState = stateZipMatch[1];
    const state = stateZipMatch[2];
    const zip = stateZipMatch[3];
    // Try to find where street ends and city begins using road suffixes
    const roadSuffixes = /^(.+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Boulevard|Blvd|Lane|Ln|Court|Ct|Way|Place|Pl|Circle|Cir|Terrace|Ter|Trail|Trl|Parkway|Pkwy|Highway|Hwy)\.?)\s+(.+)$/i;
    const roadMatch = beforeState.match(roadSuffixes);
    if (roadMatch) {
      return { line1: roadMatch[1], line2: roadMatch[2] + ', ' + state + ' ' + zip };
    }
    // No road suffix found — just put everything before state on line 1
    return { line1: beforeState, line2: state + ' ' + zip };
  }

  // If original had commas but no state+zip pattern, use comma split
  const commaParts = trimmed.split(',').map(p => p.trim());
  if (commaParts.length >= 3) {
    return { line1: commaParts[0], line2: commaParts.slice(1).join(', ') };
  }
  if (commaParts.length === 2) {
    return { line1: commaParts[0], line2: commaParts[1] };
  }

  // Can't parse — return as single line
  return { line1: trimmed, line2: '' };
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
    <div style="margin-top:32px;padding-top:24px;border-top:1px solid #e5e7eb;">
      <img src="${SIGNATURE_IMAGE}" alt="Timothy Pappas" style="max-width:420px;width:100%;height:auto;">
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

  const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
  const assetsUrl = process.env.EMAIL_ASSETS_URL || baseUrl;
  const SOCIAL_FB_WHITE = `${assetsUrl}/email-assets/fb-white.png`;
  const SOCIAL_IG_WHITE = `${assetsUrl}/email-assets/ig-white.png`;
  const SOCIAL_ND_WHITE = `${assetsUrl}/email-assets/nd-white.png`;

  return `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');</style>
</head>
<body style="margin:0;padding:0;background:#f2f4f3;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f2f4f3;padding:40px 16px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.06);">
  <!-- Header -->
  <tr><td style="background:#2e403d;padding:36px 48px;text-align:center;">
    <img src="${LOGO_URL}" alt="Pappas & Co. Landscaping" style="max-height:90px;max-width:360px;width:auto;">
  </td></tr>
  <!-- Lime accent bar -->
  <tr><td style="background:#c9dd80;height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
  <!-- Content -->
  <tr><td style="padding:44px 48px 36px;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;font-size:15px;line-height:1.8;color:#374151;">
    ${content}
    ${signatureHtml}
  </td></tr>
  ${featuresSection}
  <!-- Footer -->
  <tr><td style="background:#243330;padding:32px 40px;text-align:center;">
    <p style="margin:0 0 16px;font-size:13px;color:#a3b8a0;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Questions? Reply to this email or call <a href="tel:4408867318" style="color:#c9dd80;font-weight:600;text-decoration:none;">(440) 886-7318</a></p>
    <table cellpadding="0" cellspacing="0" style="margin:0 auto 20px;">
      <tr>
        <td style="padding:0 10px;"><a href="https://www.facebook.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_FB_WHITE}" alt="Facebook" style="width:30px;height:30px;"></a></td>
        <td style="padding:0 10px;"><a href="https://www.instagram.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_IG_WHITE}" alt="Instagram" style="width:30px;height:30px;"></a></td>
        <td style="padding:0 10px;"><a href="https://nextdoor.com/profile/01ZjZkwxhPWdnML2k" style="text-decoration:none;"><img src="${SOCIAL_ND_WHITE}" alt="Nextdoor" style="width:30px;height:30px;"></a></td>
      </tr>
    </table>
    <p style="margin:0 0 4px;font-size:13px;color:#8fad8c;font-weight:600;font-family:'DM Sans',-apple-system,Arial,sans-serif;">Pappas & Co. Landscaping</p>
    <p style="margin:0 0 4px;font-size:11px;color:#5f8a5c;font-family:'DM Sans',-apple-system,Arial,sans-serif;">PO Box 770057 &bull; Lakewood, Ohio 44107</p>
    <p style="margin:0 0 14px;font-size:12px;"><a href="https://pappaslandscaping.com" style="color:#c9dd80;text-decoration:none;font-weight:500;font-family:'DM Sans',-apple-system,Arial,sans-serif;">pappaslandscaping.com</a></p>
    <p style="margin:0;font-size:10px;color:#5a7a57;font-family:'DM Sans',-apple-system,Arial,sans-serif;"><a href="${baseUrl}/unsubscribe.html?email={unsubscribe_email}" style="color:#7a9477;text-decoration:underline;">Unsubscribe</a> from marketing emails</p>
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
      console.log('Contract PDF - Looking for Qualy at:', qualyPath, 'exists:', fs.existsSync(qualyPath));
      if (fs.existsSync(qualyPath)) {
        const qualyBytes = fs.readFileSync(qualyPath);
        console.log('Contract PDF - Qualy font bytes:', qualyBytes.length);
        qualyFont = await pdfDoc.embedFont(qualyBytes);
        console.log('Contract PDF - Qualy font embedded successfully');
      } else {
        console.log('Contract PDF - Qualy font NOT FOUND, using fallback');
      }
    } catch (fontErr) {
      console.log('Could not embed Qualy font in contract:', fontErr.message);
    }
    console.log('Fonts embedded, qualyFont is:', qualyFont === helveticaBold ? 'FALLBACK (helveticaBold)' : 'QUALY');

    // Try to embed logo
    let logoImage = null;
    try {
      const logoPath = path.join(__dirname, 'public', 'logo.png');
      console.log('Contract PDF - Looking for logo at:', logoPath, 'exists:', fs.existsSync(logoPath));
      if (fs.existsSync(logoPath)) {
        logoImage = await pdfDoc.embedPng(fs.readFileSync(logoPath));
        console.log('Contract PDF - Logo embedded successfully');
      } else {
        console.log('Contract PDF - Logo NOT FOUND');
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
      const words = pdfSafe(text).split(' ');
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

    // Header: logo on left with contact info to the right (same layout as quote PDF)
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

    // Lime green accent line (matching quote PDF)
    page.drawRectangle({ x: margin, y, width: contentWidth, height: 4, color: limeGreen });
    y -= 30;

    // Service Agreement badge (dark green bar with Qualy font)
    page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 26, color: darkGreen });
    page.drawText(`Service Agreement  #${quoteNumber}`, { x: margin + 12, y: y - 1, size: 11, font: qualyFont, color: limeGreen });
    y -= 46;
    
    // Two column layout for parties
    const colWidth = (contentWidth - 20) / 2;
    const partiesY = y; // save Y for both columns to start at same level

    // Service Provider (left column)
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

    // Client (right column — starts at same Y as service provider)
    const cx = margin + colWidth + 20;
    let clientY = partiesY;
    page.drawText('CLIENT', { x: cx, y: clientY, size: 8, font: helveticaBold, color: gray });
    clientY -= 14;
    page.drawText(pdfSafe(quote.customer_name || ''), { x: cx, y: clientY, size: 10, font: helveticaBold, color: black });
    clientY -= 12;
    // Split address into street line and city/state/zip line
    const addrLines = formatAddressLines(quote.customer_address);
    page.drawText(pdfSafe(addrLines.line1), { x: cx, y: clientY, size: 9, font: helvetica, color: black });
    if (addrLines.line2) {
      clientY -= 11;
      page.drawText(pdfSafe(addrLines.line2), { x: cx, y: clientY, size: 9, font: helvetica, color: black });
    }
    clientY -= 11;
    page.drawText(pdfSafe(quote.customer_email || ''), { x: cx, y: clientY, size: 9, font: helvetica, color: black });
    clientY -= 11;
    page.drawText(pdfSafe(quote.customer_phone || ''), { x: cx, y: clientY, size: 9, font: helvetica, color: black });

    y -= 40;
    
    // ===== SERVICES & PRICING - Two Column Table =====
    // Dark green header bar with Qualy font in lime green (matching Service Agreement header)
    page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 26, color: darkGreen });
    page.drawText('Services & Pricing', { x: margin + 12, y: y - 1, size: 11, font: qualyFont, color: limeGreen });
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

      // Vertical ordering: fill left column top-to-bottom, then right column
      const halfLen = Math.ceil(services.length / 2);
      const numRows = halfLen;
      for (let row = 0; row < numRows; row++) {
        const bgColor = row % 2 === 0 ? rgb(1, 1, 1) : rgb(0.98, 0.98, 0.98);
        page.drawRectangle({ x: margin, y: y - svcRowHeight + 15, width: contentWidth, height: svcRowHeight, color: bgColor });

        // Left column: services[row]
        const svc1 = services[row];
        page.drawText(pdfSafe(svc1.name), { x: margin + 10, y: y, size: 9, font: helvetica, color: black });
        page.drawText(`$${parseFloat(svc1.amount).toFixed(2)}`, { x: margin + svcColWidth - 50, y: y, size: 9, font: helveticaBold, color: black });

        // Right column: services[row + halfLen]
        const rightIdx = row + halfLen;
        if (rightIdx < services.length) {
          const svc2 = services[rightIdx];
          page.drawText(pdfSafe(svc2.name), { x: margin + svcColWidth + 10, y: y, size: 9, font: helvetica, color: black });
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
        page.drawText(pdfSafe(svc.name), { x: margin + 10, y: y, size: 9, font: helvetica, color: black });
        page.drawText(`$${parseFloat(svc.amount).toFixed(2)}`, { x: pageWidth - margin - 60, y: y, size: 9, font: helveticaBold, color: black });
        y -= svcRowHeight;
      }
    }

    y -= 10;

    // Total bar
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: rgb(0.98, 0.98, 0.98), borderColor: limeGreen, borderWidth: 1 });
    page.drawText(`Total: $${parseFloat(quote.total).toFixed(2)}${quote.monthly_payment ? ` | Monthly: $${parseFloat(quote.monthly_payment).toFixed(2)}/mo` : ''}`, { x: margin + 15, y: y + 2, size: 11, font: helveticaBold, color: darkGreen });

    y -= 40;
    
    // Contract sections - FULL legal text matching sign-contract.html
    const sections = [
      { title: 'I. Scope of Agreement', content: `A. Associated Quote: This Agreement is directly tied to Quote/Proposal Number: ${quoteNumber}.\n\nB. Scope of Services: The Contractor agrees to provide services at the Client Service Address as detailed in the Proposal, which outlines the specific services, schedule, and pricing. This Proposal is hereby incorporated into and made a part of this Agreement.\n\nC. Additional Work: Additional work requested by the Client outside of the scope defined in the Proposal will be performed at an additional cost, requiring a separate, pre-approved quote.` },
      { title: 'II. Terms and Renewal', content: 'A. Term: This Agreement begins on the Effective Date and remains in effect until canceled as outlined in Section IX.\n\nB. Automatic Renewal: The Agreement automatically renews each year at the start of the new season, which begins in March, unless canceled in writing by either party at least 30 days before the new season begins.' },
      { title: 'III. Payment Terms', content: 'A. Mowing Services Invoicing:\n  - Per-Service Mowing: Invoices will be sent on the final day of each month.\n  - Monthly Mowing Contracts: Invoices will be sent on the first day of each month.\n\nB. All Other Services Invoicing: Invoices will be sent upon job completion.\n\nC. Due Date: Payments are due upon receipt of the invoice.\n\nD. Accepted Payment Methods: Major credit cards, Zelle, cash, checks, money orders, and bank transfers.\n\nE. Fuel Surcharge: A small flat-rate fuel surcharge will be added to each invoice to help offset transportation-related costs, including fuel, vehicle maintenance, and insurance.\n\nF. Returned Checks: A $25 fee will be applied for any returned checks.' },
      { title: 'IV. Card on File Authorization and Fees', content: 'By placing a credit or debit card on file, the Client authorizes Pappas & Co. Landscaping to charge that card for any services rendered under this Agreement, including applicable fees and surcharges.\n\nProcessing Fee: A processing fee of 2.9% + $0.30 applies to each successful domestic card transaction.\n\nFor Monthly Service Contracts with card-on-file billing: If a scheduled payment fails, the Client will be notified and given 5 business days to update payment information. If payment is not resolved, the account will revert to per-service invoicing and standard late fee terms (Section V) will apply.' },
      { title: 'V. Late Fees and Suspension of Service', content: 'Pappas & Co. Landscaping incurs upfront costs for labor, materials, and equipment. Late payments disrupt business operations, and the following fees and policies apply:\n\n  - 30-Day Late Fee: A 10% late fee will be applied if payment is not received within 30 days of the invoice date.\n  - Recurring Late Fee: An additional 5% late fee will be applied for each additional 30-day period past due (60 days, 90 days, etc.).\n  - Service Suspension and Collections: If payment is not received within 60 days, services will be suspended, and Pappas & Co. Landscaping reserves the right to initiate collection proceedings.' },
      { title: 'VI. Client Responsibilities', content: 'The Client agrees to the following:\n\n  - Accessibility: All gates must be unlocked, and service areas must be accessible on the scheduled service day. If access is blocked (e.g., locked gates, vehicles), that area may be skipped and the full service fee will still apply.\n  - Return Trip Fee: A $25 return trip fee may be charged if rescheduling is needed due to Client-related access issues.\n  - Property Clearance: The property must be free of hazards, obstacles, and pre-existing damage that may interfere with services.\n  - Personal Items: Our crew may move personal items (e.g., furniture, hoses, toys) if necessary to perform work, but we are not responsible for any damage caused by moving such items.\n  - Pet Waste: All dog feces must be picked up prior to service. If pet waste is present, a $15 cleanup fee may be added, and we may skip service in those areas if cleanup prevents safe work.\n  - Underground Infrastructure: Pappas & Co. Landscaping is not liable for damage to underground utilities, irrigation lines, invisible fences, or other hidden infrastructure unless they are clearly marked and disclosed in advance by the Client.' },
      { title: 'VII. Lawn/Plant Installs (If Applicable)', content: 'The Client is responsible for watering newly installed lawns (sod or seed) and plants twice daily or as recommended to ensure proper growth. Pappas & Co. Landscaping is not responsible for plant or lawn failure due to lack of watering or improper care after installation.' },
      { title: 'VIII. Weather and Materials', content: 'A. Materials and Equipment: Pappas & Co. Landscaping will supply all materials, tools, and equipment necessary to perform the agreed-upon services unless specified otherwise. Any specialized materials or equipment requested by the Client will incur additional charges.\n\nB. Weather Disruptions: If inclement weather prevents services from being performed, Pappas & Co. Landscaping will make reasonable efforts to complete the service the following business day. Service on the next day is not guaranteed and will be rescheduled based on availability. Refunds or credits will not be issued for weather-related delays unless the service is permanently canceled.' },
      { title: 'IX. Cancellation and Termination', content: 'A. Non-Renewal: To stop the automatic renewal of this Agreement, the Client must provide written notice at least 30 days before your renewal date (which occurs in March).\n\nB. Mid-Season Cancellation by Client: To cancel service mid-season, the Client must provide 15 days\' written notice at any time. Services will continue through the notice period, and the final invoice will include any completed work. No refunds are given for prepaid services or unused portions of seasonal contracts.\n\nC. Termination by Contractor: Pappas & Co. Landscaping may cancel service at any time with 15 days\' notice.' },
      { title: 'X. Liability, Insurance, and Quality', content: 'A. Quality of Workmanship: Pappas & Co. Landscaping will perform all services with due care and in accordance with industry standards.\n  - If defects or deficiencies in workmanship occur, the Client must notify Pappas & Co. Landscaping within 7 days of service completion. If the issue is due to improper workmanship, it will be corrected at no additional cost.\n  - Issues resulting from natural wear, environmental conditions, or improper client maintenance are not covered under this clause.\n\nB. Independent Contractor: Pappas & Co. Landscaping is an independent contractor and is not an employee, partner, or agent of the Client. This agreement does not establish a joint venture, partnership, or employment relationship.\n\nC. Indemnification: Pappas & Co. Landscaping agrees to indemnify and hold harmless the Client from claims, damages, and liabilities arising directly from its performance of work, except where such claims arise due to the Client\'s negligence or misconduct.\n\nD. Limitation of Liability: The total liability of Pappas & Co. Landscaping for any claim shall not exceed the total amount paid by the Client under this agreement. Pappas & Co. Landscaping is not liable for indirect, incidental, consequential, or special damages, including but not limited to loss of business, property damage due to external factors, or delays caused by third parties. These liability limitations will survive the termination of this agreement.\n\nE. Insurance: Pappas & Co. Landscaping carries general liability insurance, automobile liability insurance, and workers\' compensation insurance as required by law.\n\nF. Force Majeure: Neither party shall be held liable for delays or failure in performance caused by events beyond their reasonable control, including but not limited to: Acts of God, war, terrorism, riots, labor strikes, governmental restrictions or regulations, epidemics, pandemics, or public health emergencies.' },
      { title: 'XI. Governing Law and Dispute Resolution', content: 'A. Jurisdiction: This agreement shall be governed by the laws of the State of Ohio. Any disputes shall be resolved in the county courts of Cuyahoga County, Ohio.\n\nB. Dispute Resolution: Any disputes will first be subject to good-faith negotiations between the parties. If a resolution cannot be reached, the dispute may be subject to mediation or arbitration before legal action is pursued.' },
      { title: 'XII. Acceptance of Agreement', content: 'By signing below, the parties acknowledge that they have read, understand, and agree to the terms and conditions of this Landscaping Services Agreement and the incorporated Proposal/Quote.' }
    ];
    
    for (const section of sections) {
      // Check if we need a new page
      if (y < 120) {
        page = addPage();
        y = pageHeight - margin;
      }
      
      // Section title — dark green bar with Qualy font (matching Services & Pricing header)
      page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 22, color: darkGreen });
      page.drawText(section.title, { x: margin + 10, y: y - 1, size: 10, font: qualyFont, color: limeGreen });
      y -= 28;
      
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
        page.drawText(pdfSafe(signedBy || ''), { x: margin + 15, y, size: 14, font: helvetica, color: black });
        y -= 20;
      }
    } else {
      // Typed signature
      page.drawText(pdfSafe(signatureData || signedBy || ''), { x: margin + 15, y, size: 14, font: helvetica, color: black });
      y -= 20;
    }

    page.drawRectangle({ x: margin + 15, y: y + 5, width: 200, height: 1, color: black });
    y -= 15;
    page.drawText(`Name: ${pdfSafe(signedBy || '')}`, { x: margin + 15, y, size: 9, font: helvetica, color: black });
    y -= 12;
    page.drawText(`Date: ${signedDate || new Date().toLocaleDateString()}`, { x: margin + 15, y, size: 9, font: helvetica, color: black });
    y -= 20;
    
    // Signature verification
    page.drawRectangle({ x: margin + 15, y: y - 25, width: contentWidth - 30, height: 1, color: rgb(0.8, 0.8, 0.8) });
    y -= 35;
    const signerIp = quote.contract_signer_ip || 'Recorded';
    const signatureType = quote.contract_signature_type === 'draw' ? 'Hand-drawn' : 'Typed';
    const signedTimestamp = quote.contract_signed_at ? new Date(quote.contract_signed_at).toLocaleString() : signedDate;
    page.drawText(pdfSafe(`${signatureType} signature | IP: ${signerIp} | ${signedTimestamp}`), { x: margin + 15, y, size: 7, font: helvetica, color: gray });
    
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

// Sanitize text for PDF standard fonts (WinAnsi encoding)
// Strips tabs, newlines, emojis, zero-width chars, and other unsupported Unicode
function pdfSafe(text) {
  if (!text) return '';
  return String(text)
    .replace(/[\t\n\r\x00-\x08\x0B\x0C\x0E-\x1F]/g, ' ')  // control chars → space
    .replace(/[\u200B-\u200F\u2028-\u202F\uFEFF]/g, '')      // zero-width/BOM → remove
    .replace(/[\uD800-\uDFFF]./g, '')                          // surrogate pairs (emoji) → remove
    .replace(/[^\x00-\xFF]/g, function(ch) {                   // non-Latin1 → best effort
      const map = {'\u2018':"'",'\u2019':"'",'\u201C':'"','\u201D':'"','\u2013':'-','\u2014':'-','\u2026':'...','\u2022':'*','\u2122':'TM','\u00A9':'(c)','\u00AE':'(R)'};
      return map[ch] || '';
    })
    .replace(/\s+/g, ' ')
    .trim();
}

// Generate Quote PDF - Branded style with dark green and lime accents
async function generateQuotePDF(quote) {
  try {
    console.log('=== QUOTE PDF: Starting generation ===');
    console.log('Quote data: id=' + quote.id + ', name=' + quote.customer_name + ', services type=' + typeof quote.services);
    const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
    const fontkit = require('@pdf-lib/fontkit');
    const fs = require('fs');
    const path = require('path');
    console.log('=== QUOTE PDF: pdf-lib loaded ===');

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
      if (!Array.isArray(services)) services = [];
    } catch (e) {
      console.log('=== QUOTE PDF: services parse error:', e.message);
      services = [];
    }
    console.log('=== QUOTE PDF: services count=' + services.length);

    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    const quoteDate = new Date(quote.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
    console.log('=== QUOTE PDF: quoteNumber=' + quoteNumber + ', date=' + quoteDate);

    // Helper: word-wrap text and return final Y position
    function wrapText(page, text, x, y, maxWidth, font, size, color, lineHeight = 1.4) {
      const words = pdfSafe(text).split(' ');
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
      const words = pdfSafe(text).split(' ');
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

    // Helper: render description text with inline bold labels (e.g. "Mowing: text... Trimming: text...")
    function wrapTextWithLabels(page, desc, x, y, maxWidth, regularFont, boldFont, size, color, boldColor, lineHeight) {
      // Parse into segments: [{text, bold}, ...]
      const labelRegex = /([A-Z][A-Za-z]*(?:\s+(?:[A-Z&\/][A-Za-z]*|\([A-Za-z]+\))){0,4}):\s*/g;
      const segments = [];
      let lastEnd = 0;
      let match;
      while ((match = labelRegex.exec(desc)) !== null) {
        if (match.index > lastEnd) {
          segments.push({ text: desc.slice(lastEnd, match.index).trim(), bold: false });
        }
        segments.push({ text: match[1] + ':', bold: true });
        lastEnd = match.index + match[0].length;
      }
      if (lastEnd < desc.length) {
        segments.push({ text: desc.slice(lastEnd).trim(), bold: false });
      }
      if (segments.length === 0) return y;

      // If no labels found, just render as plain text
      const hasLabels = segments.some(s => s.bold);
      if (!hasLabels) {
        return wrapText(page, desc, x, y, maxWidth, regularFont, size, color, lineHeight);
      }

      // Render segments inline with word wrapping
      let curX = x;
      let curY = y;
      const spaceW = regularFont.widthOfTextAtSize(' ', size);

      for (const seg of segments) {
        if (!seg.text) continue;
        const font = seg.bold ? boldFont : regularFont;
        const segColor = seg.bold ? boldColor : color;

        // Start bold labels on a new line with extra gap
        if (seg.bold) {
          if (curX > x) {
            // Mid-line: drop to next line first
            curY -= size * lineHeight;
          }
          curY -= size * 0.4; // extra gap before label section
          curX = x;
        }

        const words = seg.text.split(/\s+/).filter(w => w);
        for (let wi = 0; wi < words.length; wi++) {
          const word = words[wi];
          const wordW = font.widthOfTextAtSize(word, size);

          // Wrap to next line if this word doesn't fit
          if (curX + wordW > x + maxWidth && curX > x) {
            curX = x;
            curY -= size * lineHeight;
          }

          page.drawText(word, { x: curX, y: curY, size, font, color: segColor });
          curX += wordW + spaceW;
        }
      }

      curY -= size * lineHeight; // after last line
      return curY;
    }

    // Helper: add a new continuation page
    function addContinuationPage() {
      const newPage = pdfDoc.addPage([pageWidth, pageHeight]);
      let py = pageHeight - margin;
      try {
        if (logoImage) {
          const logoDims = logoImage.scale(0.18);
          newPage.drawImage(logoImage, { x: margin, y: py - logoDims.height, width: logoDims.width, height: logoDims.height });
          newPage.drawText('Quote #' + quoteNumber + '  continued', { x: margin + logoDims.width + 12, y: py - 14, size: 10, font: qualyFont, color: darkGreen });
          const contRight = pageWidth - margin;
          const ct1 = 'pappaslandscaping.com';
          const ct2 = '(440) 886-7318';
          newPage.drawText(ct1, { x: contRight - helvetica.widthOfTextAtSize(ct1, 8), y: py, size: 8, font: helvetica, color: gray });
          newPage.drawText(ct2, { x: contRight - helvetica.widthOfTextAtSize(ct2, 8), y: py - 11, size: 8, font: helvetica, color: gray });
          py -= logoDims.height + 8;
        } else {
          newPage.drawText('Quote #' + quoteNumber + '  continued', { x: margin, y: py - 10, size: 10, font: qualyFont, color: darkGreen });
          py -= 30;
        }
        newPage.drawRectangle({ x: margin, y: py, width: contentWidth, height: 3, color: limeGreen });
      } catch (contErr) {
        console.error('=== QUOTE PDF: continuation page header error:', contErr.message);
      }
      py -= 20;
      return { page: newPage, y: py };
    }

    let page = pdfDoc.addPage([pageWidth, pageHeight]);
    let y = pageHeight - margin;

    // ===== HEADER =====
    try {
      // Right-aligned contact info helper
      const rightEdge = pageWidth - margin;
      const contactTexts = ['pappaslandscaping.com', 'hello@pappaslandscaping.com', '(440) 886-7318'];
      const contactSize = 9;

      if (logoImage) {
        const logoDims = logoImage.scale(0.28);
        page.drawImage(logoImage, { x: margin, y: y - logoDims.height, width: logoDims.width, height: logoDims.height });
        for (let ci = 0; ci < contactTexts.length; ci++) {
          const tw = helvetica.widthOfTextAtSize(contactTexts[ci], contactSize);
          page.drawText(contactTexts[ci], { x: rightEdge - tw, y: y - (ci * 13), size: contactSize, font: helvetica, color: gray });
        }
        y -= logoDims.height + 8;
      } else {
        page.drawText('Pappas & Co. Landscaping', { x: margin, y, size: 20, font: qualyFont, color: darkGreen });
        for (let ci = 0; ci < contactTexts.length; ci++) {
          const tw = helvetica.widthOfTextAtSize(contactTexts[ci], contactSize);
          page.drawText(contactTexts[ci], { x: rightEdge - tw, y: y - (ci * 13), size: contactSize, font: helvetica, color: gray });
        }
        y -= 28;
      }
      page.drawRectangle({ x: margin, y, width: contentWidth, height: 4, color: limeGreen });
      y -= 30;
    } catch (headerErr) {
      console.error('=== QUOTE PDF: header error:', headerErr.message);
      y = pageHeight - margin - 60;
    }
    console.log('=== QUOTE PDF: header drawn, y=' + y);

    // ===== QUOTE BADGE =====
    try {
      page.drawRectangle({ x: margin, y: y - 8, width: 160, height: 28, color: darkGreen });
      page.drawText('QUOTE  #' + quoteNumber, { x: margin + 14, y: y, size: 12, font: qualyFont, color: limeGreen });
    } catch (badgeErr) {
      console.error('=== QUOTE PDF: badge error:', badgeErr.message);
    }
    y -= 48;

    // ===== PREPARED FOR / QUOTE DETAILS =====
    const infoBoxH = 95;
    try {
      page.drawRectangle({ x: margin, y: y - infoBoxH, width: 250, height: infoBoxH, color: lightGray, borderColor: limeGreen, borderWidth: 2 });
      page.drawText('Prepared For', { x: margin + 14, y: y - 10, size: 9, font: qualyFont, color: darkGreen });
      page.drawText(pdfSafe(quote.customer_name || ''), { x: margin + 14, y: y - 26, size: 13, font: helveticaBold, color: darkGreen });
      let infoY = y - 42;
      if (quote.customer_address) {
        const addrLines = formatAddressLines(quote.customer_address);
        page.drawText(pdfSafe(addrLines.line1), { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
        if (addrLines.line2) {
          infoY -= 12;
          page.drawText(pdfSafe(addrLines.line2), { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
        }
        infoY -= 14;
      }
      if (quote.customer_email) {
        page.drawText(pdfSafe(quote.customer_email), { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
        infoY -= 14;
      }
      if (quote.customer_phone) {
        page.drawText(pdfSafe(quote.customer_phone), { x: margin + 14, y: infoY, size: 9, font: helvetica, color: black });
      }

      // Right side - Quote Details
      const dx = margin + 275;
      page.drawText('Quote Details', { x: dx, y: y - 10, size: 9, font: qualyFont, color: darkGreen });
      page.drawText('Date:', { x: dx, y: y - 26, size: 9, font: helveticaBold, color: gray });
      page.drawText(String(quoteDate), { x: dx + 30, y: y - 26, size: 9, font: helvetica, color: black });
      page.drawText('Valid For:', { x: dx, y: y - 40, size: 9, font: helveticaBold, color: gray });
      page.drawText('30 Days', { x: dx + 48, y: y - 40, size: 9, font: helvetica, color: black });
      page.drawText('Quote #:', { x: dx, y: y - 54, size: 9, font: helveticaBold, color: gray });
      page.drawText(String(quoteNumber), { x: dx + 44, y: y - 54, size: 9, font: helvetica, color: black });
      page.drawText('Type:', { x: dx, y: y - 68, size: 9, font: helveticaBold, color: gray });
      page.drawText(quote.quote_type === 'monthly_plan' ? 'Annual Care Plan' : 'Standard Quote', { x: dx + 28, y: y - 68, size: 9, font: helvetica, color: black });
    } catch (infoErr) {
      console.error('=== QUOTE PDF: info box error:', infoErr.message);
    }
    y -= infoBoxH + 18;
    console.log('=== QUOTE PDF: info box drawn, y=' + y);

    // ===== SERVICES SECTION HEADER =====
    try {
      page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: darkGreen });
      page.drawText('Services Included', { x: margin + 14, y: y + 2, size: 12, font: qualyFont, color: limeGreen });
    } catch (svcHdrErr) {
      console.error('=== QUOTE PDF: services header error:', svcHdrErr.message);
    }
    y -= 33;

    // Table column header
    try {
      page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 20, color: rgb(0.93, 0.95, 0.93) });
      page.drawText('SERVICE', { x: margin + 10, y: y - 1, size: 8, font: qualyFont, color: darkGreen });
      page.drawText('AMOUNT', { x: pageWidth - margin - 55, y: y - 1, size: 8, font: qualyFont, color: darkGreen });
    } catch (colHdrErr) {
      console.error('=== QUOTE PDF: column header error:', colHdrErr.message);
    }
    y -= 22;

    // ===== SERVICE ROWS =====
    console.log('=== QUOTE PDF: starting service rows, count=' + services.length);
    const descLineHeight = 1.4;
    const descSize = 8.5;
    const nameSize = 9.5;
    const descX = margin + 14;
    const descMaxWidth = contentWidth - 28;

    for (let i = 0; i < services.length; i++) {
      const svc = services[i];
      if (!svc || typeof svc !== 'object') { console.log('=== QUOTE PDF: skipping invalid service at index ' + i); continue; }
      const svcName = pdfSafe(svc.name || 'Service ' + (i + 1));
      const svcAmount = svc.amount != null ? parseFloat(svc.amount) : 0;
      const desc = pdfSafe(svc.description || '');

      // Calculate row height: name bar (28) + description text + padding
      let rowH = 28; // service name bar height + gap
      if (desc) {
        // Add 20% buffer for bold label line breaks
        rowH += wrapHeight(desc, descMaxWidth, helvetica, descSize, descLineHeight) * 1.2 + 12;
      }

      // New page if needed
      try {
      if (y - rowH < 100) {
        const cont = addContinuationPage();
        page = cont.page;
        y = cont.y;
      }

      const cp = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];

      // Service name bar — light green-gray bg with name + amount
      const barH = 22;
      const barY = y - 4;
      cp.drawRectangle({ x: margin, y: barY - barH + 10, width: contentWidth, height: barH, color: rgb(0.94, 0.96, 0.94) });
      // Left accent stripe
      cp.drawRectangle({ x: margin, y: barY - barH + 10, width: 3, height: barH, color: limeGreen });
      cp.drawText(svcName, { x: margin + 12, y: barY - 5, size: nameSize, font: helveticaBold, color: darkGreen });
      const amtStr = '$' + svcAmount.toFixed(2);
      cp.drawText(amtStr, { x: pageWidth - margin - 65, y: barY - 5, size: nameSize, font: helveticaBold, color: darkGreen });
      y -= 28;

      // Description — with inline bold labels (Mowing:, Trimming:, etc.)
      if (desc) {
        y -= 2;
        y = wrapTextWithLabels(cp, desc, descX, y, descMaxWidth, helvetica, helveticaBold, descSize, midGray, rgb(0.15, 0.2, 0.25), descLineHeight);
        y -= 8; // padding after description
      }

      } catch (svcErr) {
        console.error('=== QUOTE PDF: error drawing service ' + (i+1) + ' (' + svcName + '):', svcErr.message);
        y -= 30;
      }
      console.log('=== QUOTE PDF: service ' + (i+1) + '/' + services.length + ' done');
    }

    console.log('=== QUOTE PDF: all services drawn, y=' + y);
    y -= 10;

    // ===== TOTALS BOX =====
    try {
      let cp = pdfDoc.getPages()[pdfDoc.getPageCount() - 1];
      if (y < 180) {
        const cont = addContinuationPage();
        cp = cont.page;
        y = cont.y;
      }

      const safeSubtotal = (parseFloat(quote.subtotal) || 0).toFixed(2);
      const safeTax = (parseFloat(quote.tax_amount) || 0).toFixed(2);
      const safeTotal = (parseFloat(quote.total) || 0).toFixed(2);

      cp.drawRectangle({ x: margin, y: y - 100, width: contentWidth, height: 105, color: lightGray, borderColor: limeGreen, borderWidth: 2 });
      cp.drawText('Subtotal', { x: margin + 15, y: y - 16, size: 10, font: helvetica, color: gray });
      cp.drawText('$' + safeSubtotal, { x: pageWidth - margin - 80, y: y - 16, size: 10, font: helvetica, color: black });
      cp.drawText('Tax (' + (quote.tax_rate || 8) + '%)', { x: margin + 15, y: y - 33, size: 10, font: helvetica, color: gray });
      cp.drawText('$' + safeTax, { x: pageWidth - margin - 80, y: y - 33, size: 10, font: helvetica, color: black });
      cp.drawRectangle({ x: margin + 15, y: y - 48, width: contentWidth - 30, height: 2, color: limeGreen });
      cp.drawText('TOTAL', { x: margin + 15, y: y - 70, size: 14, font: qualyFont, color: darkGreen });
      cp.drawText('$' + safeTotal, { x: pageWidth - margin - 95, y: y - 70, size: 18, font: helveticaBold, color: darkGreen });
      y -= 115;

      // Monthly payment banner
      if (quote.monthly_payment) {
        cp.drawRectangle({ x: margin, y: y - 6, width: contentWidth, height: 32, color: darkGreen });
        cp.drawText('Monthly Payment Plan', { x: margin + 14, y: y + 3, size: 11, font: qualyFont, color: limeGreen });
        cp.drawText('$' + (parseFloat(quote.monthly_payment) || 0).toFixed(2) + '/mo', { x: pageWidth - margin - 100, y: y + 3, size: 14, font: helveticaBold, color: limeGreen });
        y -= 46;
      }

      y -= 10;

      // ===== NEXT STEPS =====
      cp.drawRectangle({ x: margin, y: y - 48, width: contentWidth, height: 52, color: rgb(0.97, 0.99, 0.97), borderColor: limeGreen, borderWidth: 1 });
      cp.drawText('How to Accept This Quote', { x: margin + 14, y: y - 10, size: 10, font: qualyFont, color: darkGreen });
      cp.drawText('Review your quote email and click "View Your Quote" to accept online and sign your service agreement.', { x: margin + 14, y: y - 25, size: 8, font: helvetica, color: gray });
      cp.drawText('Questions? Call or text (440) 886-7318', { x: margin + 14, y: y - 38, size: 8, font: helvetica, color: gray });
      y -= 65;

      // ===== FOOTER =====
      cp.drawRectangle({ x: margin, y: y + 5, width: contentWidth, height: 3, color: limeGreen });
      y -= 14;
      cp.drawText('Pappas & Co. Landscaping  |  PO Box 770057, Lakewood, OH 44107  |  (440) 886-7318  |  hello@pappaslandscaping.com', { x: margin, y, size: 8, font: helvetica, color: gray });
      cp.drawText('This quote is valid for 30 days from ' + quoteDate + '. Prices subject to change after expiration.', { x: margin, y: y - 12, size: 7.5, font: helvetica, color: rgb(0.65, 0.67, 0.67) });
      console.log('=== QUOTE PDF: totals and footer drawn OK');
    } catch (totalsErr) {
      console.error('=== QUOTE PDF: totals/footer error:', totalsErr.message);
      // Don't return null — the PDF still has the services section
    }

    console.log('=== QUOTE PDF: saving document...');
    try {
      const pdfBytes = await pdfDoc.save();
      console.log('=== QUOTE PDF: saved successfully, size=' + pdfBytes.length + ' bytes');
      return { bytes: pdfBytes, error: null, type: 'branded' };
    } catch (saveErr) {
      console.error('=== QUOTE PDF: pdfDoc.save() FAILED:', saveErr.message);
      console.error('=== QUOTE PDF: save stack:', saveErr.stack);
      throw saveErr;
    }

  } catch (error) {
    console.error('=== QUOTE PDF FATAL ERROR:', error.message);
    console.error('=== QUOTE PDF STACK:', error.stack);

    // FALLBACK: Try to generate a simple text-only PDF so the email still has an attachment
    try {
      console.log('=== QUOTE PDF: attempting simple fallback PDF...');
      const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
      const fallbackDoc = await PDFDocument.create();
      const font = await fallbackDoc.embedFont(StandardFonts.Helvetica);
      const fontBold = await fallbackDoc.embedFont(StandardFonts.HelveticaBold);
      const pg = fallbackDoc.addPage([612, 792]);
      let fy = 720;
      pg.drawText('Pappas & Co. Landscaping', { x: 50, y: fy, size: 20, font: fontBold, color: rgb(0.18, 0.25, 0.24) });
      fy -= 30;
      pg.drawText('Quote #' + (quote.quote_number || 'Q-' + quote.id), { x: 50, y: fy, size: 14, font: fontBold, color: rgb(0, 0, 0) });
      fy -= 25;
      pg.drawText('Prepared for: ' + pdfSafe(quote.customer_name || ''), { x: 50, y: fy, size: 12, font, color: rgb(0, 0, 0) });
      fy -= 18;
      pg.drawText(pdfSafe(quote.customer_address || ''), { x: 50, y: fy, size: 10, font, color: rgb(0.3, 0.3, 0.3) });
      fy -= 30;
      let svcs = [];
      try { svcs = typeof quote.services === 'string' ? JSON.parse(quote.services) : (quote.services || []); } catch(e) { svcs = []; }
      if (Array.isArray(svcs)) {
        for (const s of svcs) {
          if (!s) continue;
          pg.drawText(pdfSafe(s.name || 'Service') + '  $' + (parseFloat(s.amount) || 0).toFixed(2), { x: 50, y: fy, size: 10, font, color: rgb(0, 0, 0) });
          fy -= 16;
          if (fy < 80) break;
        }
      }
      fy -= 10;
      pg.drawText('Total: $' + (parseFloat(quote.total) || 0).toFixed(2), { x: 50, y: fy, size: 14, font: fontBold, color: rgb(0.18, 0.25, 0.24) });
      fy -= 30;
      pg.drawText('View your full quote online to accept and sign your service agreement.', { x: 50, y: fy, size: 10, font, color: rgb(0.4, 0.4, 0.4) });
      fy -= 14;
      pg.drawText('(440) 886-7318 | hello@pappaslandscaping.com', { x: 50, y: fy, size: 9, font, color: rgb(0.5, 0.5, 0.5) });
      const fallbackBytes = await fallbackDoc.save();
      console.log('=== QUOTE PDF: fallback PDF generated, size=' + fallbackBytes.length);
      return { bytes: fallbackBytes, error: error.message, type: 'fallback' };
    } catch (fallbackErr) {
      console.error('=== QUOTE PDF: even fallback failed:', fallbackErr.message);
      return { bytes: null, error: error.message + ' | fallback also failed: ' + fallbackErr.message, type: 'failed' };
    }
  }
}

// Generate Invoice PDF - Branded style matching quote PDF
// ═══════════════════════════════════════════════════════════
// SHARED PDF HELPERS — contract-style header/footer for all documents
// ═══════════════════════════════════════════════════════════
async function initPdfDoc() {
  const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
  const fontkit = require('@pdf-lib/fontkit');
  const fs = require('fs');
  const path = require('path');

  const pdfDoc = await PDFDocument.create();
  pdfDoc.registerFontkit(fontkit);
  const helvetica = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const helveticaBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  let qualyFont = helveticaBold;
  try {
    const qualyPath = path.join(__dirname, 'public', 'Qualy.otf');
    if (fs.existsSync(qualyPath)) {
      qualyFont = await pdfDoc.embedFont(fs.readFileSync(qualyPath));
    }
  } catch (e) { /* use fallback */ }

  let logoImage = null;
  try {
    const logoPath = path.join(__dirname, 'public', 'logo.png');
    if (fs.existsSync(logoPath)) {
      logoImage = await pdfDoc.embedPng(fs.readFileSync(logoPath));
    }
  } catch (e) { /* no logo */ }

  const pageWidth = 612;
  const pageHeight = 792;
  const margin = 50;
  const contentWidth = pageWidth - (margin * 2);

  const colors = {
    darkGreen: rgb(0.18, 0.25, 0.24),
    limeGreen: rgb(0.79, 0.87, 0.50),
    black: rgb(0, 0, 0),
    gray: rgb(0.4, 0.4, 0.4),
    lightGray: rgb(0.97, 0.98, 0.96),
    white: rgb(1, 1, 1),
    green: rgb(0.02, 0.59, 0.41)
  };

  return { pdfDoc, helvetica, helveticaBold, qualyFont, logoImage, pageWidth, pageHeight, margin, contentWidth, colors, rgb };
}

// Draw contract-style header: logo left, contact right, lime accent, dark green badge
function drawPdfHeader(page, ctx, badgeLabel) {
  const { helvetica, helveticaBold, qualyFont, logoImage, pageWidth, pageHeight, margin, contentWidth, colors } = ctx;
  let y = pageHeight - margin;

  if (logoImage) {
    const logoDims = logoImage.scale(0.28);
    page.drawImage(logoImage, { x: margin, y: y - logoDims.height, width: logoDims.width, height: logoDims.height });
    const cx = pageWidth - margin - 145;
    page.drawText('pappaslandscaping.com', { x: cx, y, size: 9, font: helvetica, color: colors.gray });
    page.drawText('hello@pappaslandscaping.com', { x: cx, y: y - 13, size: 9, font: helvetica, color: colors.gray });
    page.drawText('(440) 886-7318', { x: cx, y: y - 26, size: 9, font: helvetica, color: colors.gray });
    y -= logoDims.height + 8;
  } else {
    page.drawText('Pappas & Co. Landscaping', { x: margin, y, size: 20, font: qualyFont, color: colors.darkGreen });
    const cx = pageWidth - margin - 145;
    page.drawText('pappaslandscaping.com', { x: cx, y, size: 9, font: helvetica, color: colors.gray });
    page.drawText('hello@pappaslandscaping.com', { x: cx, y: y - 13, size: 9, font: helvetica, color: colors.gray });
    page.drawText('(440) 886-7318', { x: cx, y: y - 26, size: 9, font: helvetica, color: colors.gray });
    y -= 28;
  }

  // Lime accent line
  page.drawRectangle({ x: margin, y, width: contentWidth, height: 4, color: colors.limeGreen });
  y -= 30;

  // Dark green badge bar
  page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 26, color: colors.darkGreen });
  page.drawText(badgeLabel, { x: margin + 12, y: y - 1, size: 11, font: qualyFont, color: colors.limeGreen });
  y -= 46;

  return y;
}

// Draw two-column provider/client info (matching contract style)
function drawPdfParties(page, ctx, y, customerName, customerAddress, customerEmail, customerPhone) {
  const { helvetica, helveticaBold, margin, contentWidth, colors } = ctx;
  const colWidth = (contentWidth - 20) / 2;
  const partiesY = y;

  // Service Provider (left)
  page.drawText('SERVICE PROVIDER', { x: margin, y, size: 8, font: helveticaBold, color: colors.gray });
  y -= 14;
  page.drawText('Pappas & Co. Landscaping', { x: margin, y, size: 10, font: helveticaBold, color: colors.black });
  y -= 12;
  page.drawText('T T Pappas Enterprises LLC', { x: margin, y, size: 9, font: helvetica, color: colors.black });
  y -= 11;
  page.drawText('PO Box 770057', { x: margin, y, size: 9, font: helvetica, color: colors.black });
  y -= 11;
  page.drawText('Lakewood, OH 44107', { x: margin, y, size: 9, font: helvetica, color: colors.black });
  y -= 11;
  page.drawText('(440) 886-7318', { x: margin, y, size: 9, font: helvetica, color: colors.black });
  y -= 11;
  page.drawText('hello@pappaslandscaping.com', { x: margin, y, size: 9, font: helvetica, color: colors.black });

  // Client (right)
  const cx = margin + colWidth + 20;
  let clientY = partiesY;
  page.drawText('CLIENT', { x: cx, y: clientY, size: 8, font: helveticaBold, color: colors.gray });
  clientY -= 14;
  page.drawText(pdfSafe(customerName || ''), { x: cx, y: clientY, size: 10, font: helveticaBold, color: colors.black });
  clientY -= 12;
  if (customerAddress) {
    const addrLines = formatAddressLines(customerAddress);
    page.drawText(pdfSafe(addrLines.line1), { x: cx, y: clientY, size: 9, font: helvetica, color: colors.black });
    if (addrLines.line2) { clientY -= 11; page.drawText(pdfSafe(addrLines.line2), { x: cx, y: clientY, size: 9, font: helvetica, color: colors.black }); }
    clientY -= 11;
  }
  if (customerEmail) { page.drawText(pdfSafe(customerEmail), { x: cx, y: clientY, size: 9, font: helvetica, color: colors.black }); clientY -= 11; }
  if (customerPhone) { page.drawText(pdfSafe(customerPhone), { x: cx, y: clientY, size: 9, font: helvetica, color: colors.black }); }

  return y - 40;
}

// Draw contract-style footer
function drawPdfFooter(page, ctx) {
  const { helvetica, helveticaBold, margin, contentWidth, colors } = ctx;
  const y = 40;
  page.drawRectangle({ x: margin, y: y + 10, width: contentWidth, height: 3, color: colors.limeGreen });
  page.drawText('Pappas & Co. Landscaping | T T Pappas Enterprises LLC | PO Box 770057, Lakewood, OH 44107', { x: margin, y: y - 5, size: 8, font: helvetica, color: colors.gray });
  page.drawText('(440) 886-7318 | hello@pappaslandscaping.com | pappaslandscaping.com', { x: margin, y: y - 15, size: 8, font: helvetica, color: colors.gray });
}

// Wrap text helper for PDFs
function pdfWrapText(text, font, fontSize, maxWidth) {
  const words = text.split(' ');
  const lines = [];
  let currentLine = '';
  for (const word of words) {
    const testLine = currentLine ? currentLine + ' ' + word : word;
    if (font.widthOfTextAtSize(testLine, fontSize) > maxWidth && currentLine) {
      lines.push(currentLine);
      currentLine = word;
    } else {
      currentLine = testLine;
    }
  }
  if (currentLine) lines.push(currentLine);
  return lines;
}

// ═══════════════════════════════════════════════════════════
// INVOICE PDF — contract-style layout
// ═══════════════════════════════════════════════════════════
async function generateInvoicePDF(invoice) {
  try {
    const ctx = await initPdfDoc();
    const { pdfDoc, helvetica, helveticaBold, qualyFont, pageWidth, pageHeight, margin, contentWidth, colors, rgb } = ctx;

    let page = pdfDoc.addPage([pageWidth, pageHeight]);
    const invNum = invoice.invoice_number || 'INV-' + invoice.id;
    const invDate = new Date(invoice.created_at).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
    const dueDate = invoice.due_date ? new Date(invoice.due_date).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }) : '';

    // Header with badge
    let y = drawPdfHeader(page, ctx, `Invoice`);

    const rightCol = pageWidth - margin;
    const total = parseFloat(invoice.total || 0);
    const amountPaid = parseFloat(invoice.amount_paid || 0);
    const balance = total - amountPaid;

    // Right-aligned invoice info table (CopilotCRM style)
    const infoTableX = pageWidth - margin - 200;
    const infoLabelX = infoTableX + 8;
    const infoValueX = rightCol - 8;
    const infoRowH = 20;
    let infoY = y;

    // Invoice # row
    page.drawRectangle({ x: infoTableX, y: infoY - 5, width: 200, height: infoRowH, color: colors.lightGray });
    page.drawText('Invoice #', { x: infoLabelX, y: infoY + 1, size: 9, font: helvetica, color: colors.gray });
    page.drawText(pdfSafe(invNum), { x: infoValueX - helveticaBold.widthOfTextAtSize(invNum, 9), y: infoY + 1, size: 9, font: helveticaBold, color: colors.black });
    infoY -= infoRowH;

    // Invoice Date row
    page.drawText('Invoice Date', { x: infoLabelX, y: infoY + 1, size: 9, font: helvetica, color: colors.gray });
    page.drawText(invDate, { x: infoValueX - helvetica.widthOfTextAtSize(invDate, 9), y: infoY + 1, size: 9, font: helvetica, color: colors.black });
    infoY -= infoRowH;

    // Due row
    page.drawRectangle({ x: infoTableX, y: infoY - 5, width: 200, height: infoRowH, color: colors.lightGray });
    const dueLabel = dueDate || 'Due Upon Receipt';
    page.drawText('Due', { x: infoLabelX, y: infoY + 1, size: 9, font: helvetica, color: colors.gray });
    page.drawText(dueLabel, { x: infoValueX - helveticaBold.widthOfTextAtSize(dueLabel, 9), y: infoY + 1, size: 9, font: helveticaBold, color: colors.black });
    infoY -= infoRowH;

    // Outstanding Balance row — only show if past due (due date passed and balance > 0)
    const isPastDue = balance > 0 && invoice.due_date && new Date(invoice.due_date) < new Date();
    const hasPartialPayment = amountPaid > 0 && balance > 0;
    let infoRows = 3;
    if (isPastDue || hasPartialPayment) {
      const balStr = '$' + balance.toFixed(2);
      const balColor = isPastDue ? rgb(0.8, 0.15, 0.15) : colors.darkGreen;
      page.drawText('Outstanding Balance', { x: infoLabelX, y: infoY + 1, size: 9, font: helvetica, color: colors.gray });
      page.drawText(balStr, { x: infoValueX - helveticaBold.widthOfTextAtSize(balStr, 10), y: infoY, size: 10, font: helveticaBold, color: balColor });
      infoY -= infoRowH;
      infoRows = 4;
    }

    // Company info (left side, same Y as info table)
    page.drawText('Pappas & Co. Landscaping', { x: margin, y, size: 10, font: helveticaBold, color: colors.black });
    page.drawText('PO Box 770057', { x: margin, y: y - 13, size: 9, font: helvetica, color: colors.black });
    page.drawText('Lakewood, OH 44107', { x: margin, y: y - 24, size: 9, font: helvetica, color: colors.black });
    page.drawText('(440) 886-7318', { x: margin, y: y - 35, size: 9, font: helvetica, color: colors.black });
    page.drawText('hello@pappaslandscaping.com', { x: margin, y: y - 46, size: 9, font: helvetica, color: colors.black });
    page.drawText('pappaslandscaping.com', { x: margin, y: y - 57, size: 9, font: helvetica, color: colors.black });

    y = Math.min(y - 75, infoY) - 10;

    // Customer info
    page.drawText('BILL TO', { x: margin, y, size: 8, font: helveticaBold, color: colors.gray });
    y -= 14;
    page.drawText(pdfSafe(invoice.customer_name || 'Customer'), { x: margin, y, size: 10, font: helveticaBold, color: colors.black });
    y -= 13;
    if (invoice.customer_address) {
      const addrLines = formatAddressLines(invoice.customer_address);
      if (addrLines.line1) { page.drawText(pdfSafe(addrLines.line1), { x: margin, y, size: 9, font: helvetica, color: colors.black }); y -= 12; }
      if (addrLines.line2) { page.drawText(pdfSafe(addrLines.line2), { x: margin, y, size: 9, font: helvetica, color: colors.black }); y -= 12; }
    }
    if (invoice.customer_email) { page.drawText(pdfSafe(invoice.customer_email), { x: margin, y, size: 9, font: helvetica, color: colors.gray }); y -= 12; }
    y -= 15;

    // Line items table
    let lineItems = invoice.line_items || [];
    if (typeof lineItems === 'string') try { lineItems = JSON.parse(lineItems); } catch(e) { lineItems = []; }

    // Table header bar
    page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 26, color: colors.darkGreen });
    page.drawText('Description', { x: margin + 10, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    page.drawText('Date', { x: pageWidth - margin - 195, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    page.drawText('Qty', { x: pageWidth - margin - 140, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    page.drawText('Rate', { x: pageWidth - margin - 100, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    const amtHeader = 'Amount';
    page.drawText(amtHeader, { x: rightCol - helveticaBold.widthOfTextAtSize(amtHeader, 9) - 8, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    y -= 35;

    // Table rows
    const descMaxWidth = pageWidth - margin - 220 - margin;
    for (let idx = 0; idx < lineItems.length; idx++) {
      const item = lineItems[idx];
      const name = pdfSafe(item.name || '');
      const desc = pdfSafe(item.description || '');
      const qty = item.quantity || item.qty || 1;
      const rate = parseFloat(item.rate || item.unit_price || item.amount || 0);
      const amount = parseFloat(item.amount || (qty * rate) || 0);
      const dateStr = item.service_date ? new Date(item.service_date + 'T00:00:00').toLocaleDateString('en-US', {month:'short', day:'numeric', year:'numeric'}) : '';

      const nameLines = name ? pdfWrapText(name, helveticaBold, 9, descMaxWidth) : [];
      const lineCount = nameLines.length;
      const rowHeight = Math.max(22, lineCount * 12 + 8);

      // Alternate row background
      if (idx % 2 === 0) {
        page.drawRectangle({ x: margin, y: y - rowHeight + 15, width: contentWidth, height: rowHeight, color: colors.lightGray });
      }

      // Draw name only (no description on invoices)
      let textY = y;
      for (const line of nameLines) {
        page.drawText(line, { x: margin + 10, y: textY, size: 9, font: helveticaBold, color: colors.black });
        textY -= 12;
      }

      if (dateStr) page.drawText(dateStr, { x: pageWidth - margin - 195, y, size: 8, font: helvetica, color: colors.gray });
      page.drawText(String(qty), { x: pageWidth - margin - 140, y, size: 9, font: helvetica, color: colors.gray });
      page.drawText('$' + rate.toFixed(2), { x: pageWidth - margin - 100, y, size: 9, font: helvetica, color: colors.gray });
      const amtStr = '$' + amount.toFixed(2);
      page.drawText(amtStr, { x: rightCol - helvetica.widthOfTextAtSize(amtStr, 9) - 8, y, size: 9, font: helvetica, color: colors.black });
      y -= rowHeight;
    }

    y -= 15;

    // Totals
    const subtotal = parseFloat(invoice.subtotal || invoice.total || 0);
    const taxAmount = parseFloat(invoice.tax_amount || 0);
    const totalsX = pageWidth - margin - 180;

    if (taxAmount > 0) {
      page.drawText('Subtotal:', { x: totalsX, y, size: 10, font: helvetica, color: colors.gray });
      const subStr = '$' + subtotal.toFixed(2);
      page.drawText(subStr, { x: rightCol - helvetica.widthOfTextAtSize(subStr, 10) - 8, y, size: 10, font: helvetica, color: colors.black });
      y -= 20;
      const taxLabel = invoice.tax_rate ? `Tax (${parseFloat(invoice.tax_rate).toFixed(3)}%):` : 'Tax:';
      page.drawText(taxLabel, { x: totalsX, y, size: 10, font: helvetica, color: colors.gray });
      const taxStr = '$' + taxAmount.toFixed(2);
      page.drawText(taxStr, { x: rightCol - helvetica.widthOfTextAtSize(taxStr, 10) - 8, y, size: 10, font: helvetica, color: colors.black });
      y -= 24;
    }

    // Total bar (lime green with border, matching contract style)
    page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 28, color: rgb(0.98, 0.98, 0.98), borderColor: colors.limeGreen, borderWidth: 1 });
    page.drawText(`Total: $${total.toFixed(2)}`, { x: margin + 15, y: y - 1, size: 11, font: helveticaBold, color: colors.darkGreen });
    y -= 42;

    if (amountPaid > 0 && amountPaid < total) {
      page.drawText('Amount Paid:', { x: totalsX, y, size: 10, font: helvetica, color: colors.green });
      const paidStr = '-$' + amountPaid.toFixed(2);
      page.drawText(paidStr, { x: rightCol - helvetica.widthOfTextAtSize(paidStr, 10) - 8, y, size: 10, font: helvetica, color: colors.green });
      y -= 18;
      page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: colors.darkGreen });
      page.drawText(`Balance Due: $${balance.toFixed(2)}`, { x: margin + 15, y: y + 2, size: 11, font: qualyFont, color: colors.limeGreen });
      y -= 40;
    }

    // PAID watermark
    if (invoice.status === 'paid' || amountPaid >= total) {
      const paidSize = 72;
      const paidText = 'PAID';
      const paidWidth = helveticaBold.widthOfTextAtSize(paidText, paidSize);
      page.drawText(paidText, {
        x: (pageWidth - paidWidth) / 2, y: pageHeight / 2,
        size: paidSize, font: helveticaBold, color: colors.green, opacity: 0.15,
        rotate: { type: 'degrees', angle: -35 }
      });
    }

    // Notes
    if (invoice.notes) {
      page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 26, color: colors.darkGreen });
      page.drawText('Notes', { x: margin + 10, y: y - 1, size: 10, font: qualyFont, color: colors.limeGreen });
      y -= 35;
      const noteLines = pdfWrapText(pdfSafe(invoice.notes), helvetica, 9, contentWidth);
      for (const line of noteLines.slice(0, 5)) {
        page.drawText(line, { x: margin, y, size: 9, font: helvetica, color: colors.gray });
        y -= 13;
      }
    }

    // Invoice Terms section (clean text, no bar)
    if (y < 200) {
      drawPdfFooter(page, ctx);
      page = pdfDoc.addPage([pageWidth, pageHeight]);
      y = pageHeight - margin;
    }
    y -= 10;
    page.drawRectangle({ x: margin, y, width: contentWidth, height: 1, color: rgb(0.85, 0.87, 0.85) });
    y -= 16;
    page.drawText('Invoice Terms - Pappas & Co. Landscaping', { x: margin, y, size: 9, font: helveticaBold, color: colors.darkGreen });
    y -= 16;

    const termsLines = [
      'Payments are due upon receipt.',
      'A 10% late fee will be applied if payment is not received within 30 days of the invoice date.',
      'An additional 5% late fee will be applied for each additional 30-day period past due (60 days, 90 days, etc.).',
      'If payment is not received within 60 days, services will be suspended, and collection proceedings may be initiated.',
      'Accepted payment methods: Major credit cards, Zelle, cash, checks, money orders, and bank transfers.',
      'Returned checks will incur a $25 fee.',
      'By making payment, the client acknowledges acceptance of the services provided and agrees to these terms.'
    ];
    for (const line of termsLines) {
      const wrapped = pdfWrapText(pdfSafe(line), helvetica, 8, contentWidth);
      for (const wl of wrapped) {
        if (y < 65) { drawPdfFooter(page, ctx); page = pdfDoc.addPage([pageWidth, pageHeight]); y = pageHeight - margin; }
        page.drawText(wl, { x: margin, y, size: 8, font: helvetica, color: colors.gray });
        y -= 11;
      }
      y -= 3;
    }

    // Footer
    drawPdfFooter(page, ctx);

    const bytes = await pdfDoc.save();
    return { bytes, type: 'complete' };
  } catch (error) {
    console.error('Invoice PDF error:', error);
    try {
      const { PDFDocument, rgb, StandardFonts } = require('pdf-lib');
      const doc = await PDFDocument.create();
      const page = doc.addPage([612, 792]);
      const font = await doc.embedFont(StandardFonts.Helvetica);
      page.drawText('INVOICE', { x: 50, y: 720, size: 24, font, color: rgb(0, 0, 0) });
      page.drawText(invoice.invoice_number || 'Invoice', { x: 50, y: 690, size: 14, font });
      page.drawText('Customer: ' + (invoice.customer_name || ''), { x: 50, y: 660, size: 12, font });
      page.drawText('Total: $' + parseFloat(invoice.total || 0).toFixed(2), { x: 50, y: 630, size: 14, font });
      return { bytes: await doc.save(), type: 'fallback', error: error.message };
    } catch (e2) {
      return { bytes: null, type: 'none', error: e2.message };
    }
  }
}

// ═══════════════════════════════════════════════════════════
// RECEIPT PDF — contract-style layout for payment receipts
// ═══════════════════════════════════════════════════════════
async function generateReceiptPDF(invoice, payment) {
  try {
    const ctx = await initPdfDoc();
    const { pdfDoc, helvetica, helveticaBold, qualyFont, pageWidth, pageHeight, margin, contentWidth, colors, rgb } = ctx;

    const page = pdfDoc.addPage([pageWidth, pageHeight]);
    const invNum = invoice.invoice_number || 'INV-' + invoice.id;
    const paidDate = payment.paid_at ? new Date(payment.paid_at).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }) : new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });

    // Header
    let y = drawPdfHeader(page, ctx, `Payment Receipt  #${invNum}`);

    // Two-column provider/client
    y = drawPdfParties(page, ctx, y, invoice.customer_name, invoice.customer_address, invoice.customer_email);

    const rightCol = pageWidth - margin;

    // Payment details section
    page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 26, color: colors.darkGreen });
    page.drawText('Payment Details', { x: margin + 10, y: y - 1, size: 10, font: qualyFont, color: colors.limeGreen });
    y -= 40;

    // Payment info rows
    const labelX = margin + 10;
    const valueX = margin + 150;
    const rowH = 22;

    page.drawRectangle({ x: margin, y: y - rowH + 15, width: contentWidth, height: rowH, color: colors.lightGray });
    page.drawText('Payment Date:', { x: labelX, y, size: 9, font: helveticaBold, color: colors.gray });
    page.drawText(paidDate, { x: valueX, y, size: 9, font: helvetica, color: colors.black });
    y -= rowH;

    page.drawText('Payment Method:', { x: labelX, y, size: 9, font: helveticaBold, color: colors.gray });
    let methodStr = (payment.method || 'Card').charAt(0).toUpperCase() + (payment.method || 'card').slice(1);
    if (payment.card_brand && payment.card_last4) methodStr += ` (${payment.card_brand} ****${payment.card_last4})`;
    else if (payment.ach_bank_name) methodStr += ` (${payment.ach_bank_name})`;
    page.drawText(pdfSafe(methodStr), { x: valueX, y, size: 9, font: helvetica, color: colors.black });
    y -= rowH;

    page.drawRectangle({ x: margin, y: y - rowH + 15, width: contentWidth, height: rowH, color: colors.lightGray });
    page.drawText('Invoice:', { x: labelX, y, size: 9, font: helveticaBold, color: colors.gray });
    page.drawText(pdfSafe(invNum), { x: valueX, y, size: 9, font: helvetica, color: colors.black });
    y -= rowH;

    page.drawText('Payment ID:', { x: labelX, y, size: 9, font: helveticaBold, color: colors.gray });
    page.drawText(pdfSafe(payment.payment_id || ''), { x: valueX, y, size: 9, font: helvetica, color: colors.black });
    y -= rowH;

    if (payment.processing_fee && parseFloat(payment.processing_fee) > 0) {
      page.drawRectangle({ x: margin, y: y - rowH + 15, width: contentWidth, height: rowH, color: colors.lightGray });
      page.drawText('Processing Fee:', { x: labelX, y, size: 9, font: helveticaBold, color: colors.gray });
      page.drawText('$' + parseFloat(payment.processing_fee).toFixed(2), { x: valueX, y, size: 9, font: helvetica, color: colors.black });
      y -= rowH;
    }

    y -= 10;

    // Amount paid bar
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: rgb(0.98, 0.98, 0.98), borderColor: colors.limeGreen, borderWidth: 1 });
    const payAmt = parseFloat(payment.amount || invoice.amount_paid || invoice.total || 0);
    page.drawText(`Amount Paid: $${payAmt.toFixed(2)}`, { x: margin + 15, y: y + 2, size: 11, font: helveticaBold, color: colors.darkGreen });
    y -= 50;

    // Thank you message
    page.drawText('Thank you for your payment!', { x: margin, y, size: 11, font: helveticaBold, color: colors.darkGreen });
    y -= 16;
    page.drawText('This receipt confirms your payment has been received and processed.', { x: margin, y, size: 9, font: helvetica, color: colors.gray });

    // Footer
    drawPdfFooter(page, ctx);

    const bytes = await pdfDoc.save();
    return { bytes, type: 'complete' };
  } catch (error) {
    console.error('Receipt PDF error:', error);
    return { bytes: null, type: 'none', error: error.message };
  }
}

// ═══════════════════════════════════════════════════════════
// STATEMENT PDF — contract-style layout for account statements
// ═══════════════════════════════════════════════════════════
async function generateStatementPDF(customer, invoices, dateRange) {
  try {
    const ctx = await initPdfDoc();
    const { pdfDoc, helvetica, helveticaBold, qualyFont, pageWidth, pageHeight, margin, contentWidth, colors, rgb } = ctx;

    let page = pdfDoc.addPage([pageWidth, pageHeight]);
    const rightCol = pageWidth - margin;
    const stmtDate = new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });

    // Header
    let y = drawPdfHeader(page, ctx, `Account Statement`);

    // Two-column provider/client
    y = drawPdfParties(page, ctx, y, customer.name || ((customer.first_name || '') + ' ' + (customer.last_name || '')).trim(), customer.street ? `${customer.street}, ${customer.city || ''}, ${customer.state || ''} ${customer.postal_code || ''}` : '', customer.email, customer.phone);

    // Statement date
    page.drawText('Statement Date: ' + stmtDate, { x: margin, y, size: 9, font: helvetica, color: colors.gray });
    if (dateRange) {
      page.drawText(pdfSafe(dateRange), { x: rightCol - helvetica.widthOfTextAtSize(dateRange, 9), y, size: 9, font: helvetica, color: colors.gray });
    }
    y -= 25;

    // Invoices table header
    page.drawRectangle({ x: margin, y: y - 8, width: contentWidth, height: 26, color: colors.darkGreen });
    page.drawText('Invoice #', { x: margin + 10, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    page.drawText('Date', { x: margin + 120, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    page.drawText('Due Date', { x: margin + 220, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    page.drawText('Total', { x: margin + 330, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    page.drawText('Paid', { x: margin + 400, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    const balHdr = 'Balance';
    page.drawText(balHdr, { x: rightCol - helveticaBold.widthOfTextAtSize(balHdr, 9) - 8, y: y - 1, size: 9, font: helveticaBold, color: colors.white });
    y -= 35;

    // Table rows
    let totalBalance = 0;
    let totalAmount = 0;
    let totalPaid = 0;
    const rowHeight = 22;

    for (let idx = 0; idx < invoices.length; idx++) {
      // New page if needed
      if (y < 100) {
        drawPdfFooter(page, ctx);
        page = pdfDoc.addPage([pageWidth, pageHeight]);
        y = pageHeight - margin - 20;
      }

      const inv = invoices[idx];
      const invTotal = parseFloat(inv.total || 0);
      const invPaid = parseFloat(inv.amount_paid || 0);
      const invBalance = invTotal - invPaid;
      totalAmount += invTotal;
      totalPaid += invPaid;
      totalBalance += invBalance;

      const invDate = new Date(inv.created_at).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
      const invDue = inv.due_date ? new Date(inv.due_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '';
      const statusColor = inv.status === 'paid' ? colors.green : invBalance > 0 ? rgb(0.8, 0.2, 0.2) : colors.black;

      if (idx % 2 === 0) {
        page.drawRectangle({ x: margin, y: y - rowHeight + 15, width: contentWidth, height: rowHeight, color: colors.lightGray });
      }

      page.drawText(pdfSafe(inv.invoice_number || ''), { x: margin + 10, y, size: 9, font: helvetica, color: colors.black });
      page.drawText(invDate, { x: margin + 120, y, size: 8, font: helvetica, color: colors.gray });
      page.drawText(invDue, { x: margin + 220, y, size: 8, font: helvetica, color: colors.gray });
      page.drawText('$' + invTotal.toFixed(2), { x: margin + 330, y, size: 9, font: helvetica, color: colors.black });
      page.drawText('$' + invPaid.toFixed(2), { x: margin + 400, y, size: 9, font: helvetica, color: colors.green });
      const balStr = '$' + invBalance.toFixed(2);
      page.drawText(balStr, { x: rightCol - helvetica.widthOfTextAtSize(balStr, 9) - 8, y, size: 9, font: helveticaBold, color: statusColor });
      y -= rowHeight;
    }

    y -= 10;

    // Summary totals bar
    page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: rgb(0.98, 0.98, 0.98), borderColor: colors.limeGreen, borderWidth: 1 });
    page.drawText(`Total: $${totalAmount.toFixed(2)}`, { x: margin + 15, y: y + 2, size: 10, font: helveticaBold, color: colors.darkGreen });
    page.drawText(`Paid: $${totalPaid.toFixed(2)}`, { x: margin + 180, y: y + 2, size: 10, font: helvetica, color: colors.green });
    y -= 40;

    // Outstanding balance bar
    if (totalBalance > 0) {
      page.drawRectangle({ x: margin, y: y - 5, width: contentWidth, height: 28, color: colors.darkGreen });
      page.drawText(`Balance Due: $${totalBalance.toFixed(2)}`, { x: margin + 15, y: y + 2, size: 11, font: qualyFont, color: colors.limeGreen });
      y -= 40;
    }

    page.drawText('Terms: Due upon receipt. Please remit payment at your earliest convenience.', { x: margin, y, size: 8, font: helvetica, color: colors.gray });

    // Footer
    drawPdfFooter(page, ctx);

    const bytes = await pdfDoc.save();
    return { bytes, type: 'complete' };
  } catch (error) {
    console.error('Statement PDF error:', error);
    return { bytes: null, type: 'none', error: error.message };
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
// TAX CALCULATION HELPER
// ═══════════════════════════════════════════════════════════

async function calculateTax(customerId, propertyId, lineItems) {
  try {
    // Level 1: Customer tax exempt
    if (customerId) {
      const cust = await pool.query('SELECT tax_exempt FROM customers WHERE id = $1', [customerId]);
      if (cust.rows[0] && cust.rows[0].tax_exempt) {
        const subtotal = lineItems.reduce((s, i) => s + (parseFloat(i.amount) || 0), 0);
        return { lineItems: lineItems.map(i => ({...i, tax: 0, taxRate: 0})), subtotal, taxAmount: 0, total: subtotal, effectiveRate: 0 };
      }
    }

    // Level 3: Property tax rates
    let propertyTaxRate = null;
    if (propertyId) {
      const prop = await pool.query('SELECT county_tax, city_tax, state_tax FROM properties WHERE id = $1', [propertyId]);
      if (prop.rows[0]) {
        const p = prop.rows[0];
        if (p.county_tax !== null || p.city_tax !== null || p.state_tax !== null) {
          propertyTaxRate = (parseFloat(p.county_tax) || 0) + (parseFloat(p.city_tax) || 0) + (parseFloat(p.state_tax) || 0);
        }
      }
    }

    // Level 6: Default tax rate fallback
    let defaultRate = 0;
    const settingsResult = await pool.query("SELECT value FROM business_settings WHERE key = 'tax_defaults'");
    if (settingsResult.rows[0]) defaultRate = parseFloat(settingsResult.rows[0].value.default_rate) || 0;

    // Calculate per line item
    let taxTotal = 0;
    let subtotal = 0;
    const processedItems = lineItems.map(item => {
      const amount = parseFloat(item.amount) || 0;
      subtotal += amount;

      // Level 2: Line item non-taxable
      if (item.taxable === false) return {...item, tax: 0, taxRate: 0};

      // Level 3: Property tax rate
      if (propertyTaxRate !== null) {
        const tax = Math.round(amount * propertyTaxRate) / 100;
        taxTotal += tax;
        return {...item, tax, taxRate: propertyTaxRate};
      }

      // Level 4: Service item's own tax rate
      if (item.service_tax_rate !== undefined && item.service_tax_rate !== null && parseFloat(item.service_tax_rate) > 0) {
        const rate = parseFloat(item.service_tax_rate);
        const tax = Math.round(amount * rate) / 100;
        taxTotal += tax;
        return {...item, tax, taxRate: rate};
      }

      // Level 6: Default rate
      const tax = Math.round(amount * defaultRate) / 100;
      taxTotal += tax;
      return {...item, tax, taxRate: defaultRate};
    });

    const taxAmount = Math.round(taxTotal * 100) / 100;
    return {
      lineItems: processedItems,
      subtotal,
      taxAmount,
      total: subtotal + taxAmount,
      effectiveRate: subtotal > 0 ? Math.round((taxTotal / subtotal) * 10000) / 100 : 0
    };
  } catch(e) {
    console.error('Tax calculation error:', e);
    const subtotal = lineItems.reduce((s, i) => s + (parseFloat(i.amount) || 0), 0);
    return { lineItems, subtotal, taxAmount: 0, total: subtotal, effectiveRate: 0 };
  }
}

// ═══════════════════════════════════════════════════════════
// PROPERTIES ENDPOINTS - Using YOUR existing schema:
// id, property_name, country, state, street, street2, city, zip, tags, status, lot_size, notes, customer_id
// ═══════════════════════════════════════════════════════════

// GET /api/properties
app.get('/api/properties', async (req, res) => {
  try {
    const { status, city, search, sort, limit = 1000, offset = 0, customer_id } = req.query;

    let query = `
      SELECT p.*, c.name as customer_display_name, c.email as customer_email, c.phone as customer_phone,
        (SELECT MAX(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('completed','done') AND p.customer_id IS NOT NULL) as last_service_date,
        (SELECT MIN(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('pending','scheduled') AND sj.job_date >= CURRENT_DATE AND p.customer_id IS NOT NULL) as next_service_date
      FROM properties p
      LEFT JOIN customers c ON p.customer_id = c.id
      WHERE 1=1
    `;
    let countQuery = 'SELECT COUNT(*) FROM properties WHERE 1=1';
    const params = [];
    const countParams = [];
    let paramCount = 1;
    let countParamCount = 1;

    if (customer_id) {
      query += ` AND p.customer_id = $${paramCount}`;
      countQuery += ` AND customer_id = $${countParamCount}`;
      params.push(customer_id);
      countParams.push(customer_id);
      paramCount++;
      countParamCount++;
    }

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
    
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, countParams)
    ]);

    res.json({
      success: true,
      properties: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });
  } catch (error) {
    console.error('Error fetching properties:', error);
    serverError(res, error);
  }
});

// GET /api/properties/stats
app.get('/api/properties/stats', async (req, res) => {
  try {
    const [totalResult, activeResult, citiesResult, pricedResult] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM properties'),
      pool.query("SELECT COUNT(*) FROM properties WHERE LOWER(status) = 'active'"),
      pool.query(`SELECT city, COUNT(*) as count FROM properties WHERE city IS NOT NULL AND city != '' GROUP BY city ORDER BY count DESC LIMIT 20`),
      pool.query(`SELECT COUNT(*) FROM properties WHERE lot_size IS NOT NULL AND lot_size != '' AND lot_size != '0'`)
    ]);
    
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
    serverError(res, error);
  }
});

// GET /api/properties/:id
app.get('/api/properties/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT p.*, c.name as customer_display_name, c.email as customer_email, c.phone as customer_phone,
        (SELECT MAX(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('completed','done') AND p.customer_id IS NOT NULL) as last_service_date,
        (SELECT MIN(job_date) FROM scheduled_jobs sj WHERE (sj.customer_id = p.customer_id OR LOWER(TRIM(sj.address)) = LOWER(TRIM(p.street))) AND sj.status IN ('pending','scheduled') AND sj.job_date >= CURRENT_DATE AND p.customer_id IS NOT NULL) as next_service_date
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
    serverError(res, error);
  }
});

// POST /api/properties
app.post('/api/properties', async (req, res) => {
  try {
    const { property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id, stories, fence_type, assigned_crew, default_services, access_instructions, equipment_notes } = req.body;

    if (!street) {
      return res.status(400).json({ success: false, error: 'Street address is required' });
    }

    const result = await pool.query(`
      INSERT INTO properties (property_name, street, street2, city, state, country, zip, lot_size, tags, status, notes, customer_id, stories, fence_type, assigned_crew, default_services, access_instructions, equipment_notes)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
      RETURNING *
    `, [
      property_name || street, street, street2 || '', city || '', state || 'OH', country || 'US',
      zip || '', lot_size || '', tags || '', status || 'Active', notes || '', customer_id || null,
      stories || null, fence_type || null, assigned_crew || null, default_services || null, access_instructions || null, equipment_notes || null
    ]);

    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error creating property:', error);
    serverError(res, error);
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
        country = $6, zip = $7, lot_size = $8, tags = $9, status = $10, notes = $11, customer_id = $12,
        county_tax = $14, city_tax = $15, state_tax = $16
      WHERE id = $13
      RETURNING *
    `, [
      actualPropertyName, actualStreet, street2 || '', city || '', state || 'OH',
      country || 'US', actualZip, actualLotSize, tags || '', status || 'Active', actualNotes,
      customer_id || null, id,
      req.body.county_tax !== undefined ? req.body.county_tax : null,
      req.body.city_tax !== undefined ? req.body.city_tax : null,
      req.body.state_tax !== undefined ? req.body.state_tax : null
    ]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Property not found' });
    }
    res.json({ success: true, property: result.rows[0] });
  } catch (error) {
    console.error('Error updating property:', error);
    serverError(res, error);
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
    
    const allowedFields = ['property_name', 'street', 'street2', 'city', 'state', 'country', 'zip', 'lot_size', 'tags', 'status', 'notes', 'customer_id', 'county_tax', 'city_tax', 'state_tax', 'stories', 'fence_type', 'assigned_crew', 'default_services', 'access_instructions', 'equipment_notes', 'photos'];
    
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
    serverError(res, error);
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
    serverError(res, error);
  }
});

// POST /api/properties/:id/photos - Upload photos (base64 in JSONB)
app.post('/api/properties/:id/photos', upload.array('photos', 10), async (req, res) => {
  try {
    const { id } = req.params;
    const prop = await pool.query('SELECT photos FROM properties WHERE id = $1', [id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });

    const existing = prop.rows[0].photos || [];
    const newPhotos = [];
    for (const file of (req.files || [])) {
      const b64 = file.buffer.toString('base64');
      const dataUrl = `data:${file.mimetype};base64,${b64}`;
      newPhotos.push({ url: dataUrl, name: file.originalname, uploaded: new Date().toISOString() });
    }
    const allPhotos = [...existing, ...newPhotos];
    await pool.query('UPDATE properties SET photos = $1 WHERE id = $2', [JSON.stringify(allPhotos), id]);
    res.json({ success: true, photos: allPhotos });
  } catch (error) {
    console.error('Error uploading property photos:', error);
    serverError(res, error);
  }
});

// DELETE /api/properties/:id/photos/:index - Remove a photo
app.delete('/api/properties/:id/photos/:index', async (req, res) => {
  try {
    const { id, index } = req.params;
    const prop = await pool.query('SELECT photos FROM properties WHERE id = $1', [id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });
    const photos = prop.rows[0].photos || [];
    photos.splice(parseInt(index), 1);
    await pool.query('UPDATE properties SET photos = $1 WHERE id = $2', [JSON.stringify(photos), id]);
    res.json({ success: true, photos });
  } catch (error) { serverError(res, error); }
});

// GET /api/properties/:id/service-history
app.get('/api/properties/:id/service-history', async (req, res) => {
  try {
    const prop = await pool.query('SELECT * FROM properties WHERE id = $1', [req.params.id]);
    if (prop.rows.length === 0) return res.status(404).json({ success: false, error: 'Property not found' });
    const p = prop.rows[0];
    const result = await pool.query(`
      SELECT * FROM scheduled_jobs
      WHERE (customer_id = $1 OR LOWER(TRIM(address)) = LOWER(TRIM($2)))
      ORDER BY job_date DESC LIMIT 50
    `, [p.customer_id, p.street]);
    res.json({ success: true, jobs: result.rows });
  } catch (error) {
    console.error('Error fetching property service history:', error);
    serverError(res, error);
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
    serverError(res, error);
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
    const dashboardUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com') + '/quote-requests.html';
    
    const emailHtml = `
      <h2>New Quote Request</h2>
      <p><strong>Name:</strong> ${escapeHtml(fullName)}</p>
      <p><strong>Email:</strong> <a href="mailto:${escapeHtml(email)}">${escapeHtml(email)}</a></p>
      <p><strong>Phone:</strong> ${escapeHtml(phone)}</p>
      <p><strong>Address:</strong> ${escapeHtml(address)}</p>
      <p><strong>Package:</strong> ${escapeHtml(pkg || 'None')}</p>
      <p><strong>Services:</strong> ${escapeHtml(servicesText)}</p>
      <p><strong>Notes:</strong> ${escapeHtml(notes || 'No notes provided')}</p>
      <br>
      <p><a href="${dashboardUrl}">View Dashboard</a></p>
    `;

    sendEmail(NOTIFICATION_EMAIL, `New Quote Request from ${escapeHtml(fullName)}`, emailHtml);
  } catch (error) {
    serverError(res, error);
  }
});

// Admin-created quote request (authenticated, no reCAPTCHA)
app.post('/api/quotes/admin', authenticateToken, async (req, res) => {
  try {
    const { name, firstName, lastName, email, phone, address, package: pkg, services, questions, notes, source } = req.body;
    const fullName = name || ((firstName || '') + ' ' + (lastName || '')).trim();
    if (!fullName || !phone) {
      return res.status(400).json({ success: false, error: 'Name and phone are required' });
    }
    let servicesArray = null;
    if (services) {
      if (Array.isArray(services)) servicesArray = services;
      else if (typeof services === 'string' && services.length > 0) servicesArray = services.split(',').map(s => s.trim());
    }
    const result = await pool.query(
      `INSERT INTO quotes (name, email, phone, address, package, services, questions, notes, source) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [fullName, email || null, phone, address || null, pkg || null, servicesArray, JSON.stringify(questions || {}), notes || null, source || 'phone_call']
    );
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    serverError(res, error);
  }
});

// Admin-created quote request (authenticated, no reCAPTCHA)
app.post('/api/quotes/admin', authenticateToken, async (req, res) => {
  try {
    const { name, firstName, lastName, email, phone, address, package: pkg, services, questions, notes, source } = req.body;
    const fullName = name || ((firstName || '') + ' ' + (lastName || '')).trim();
    if (!fullName || !phone) {
      return res.status(400).json({ success: false, error: 'Name and phone are required' });
    }
    let servicesArray = null;
    if (services) {
      if (Array.isArray(services)) servicesArray = services;
      else if (typeof services === 'string' && services.length > 0) servicesArray = services.split(',').map(s => s.trim());
    }
    const result = await pool.query(
      `INSERT INTO quotes (name, email, phone, address, package, services, questions, notes, source) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [fullName, email || null, phone, address || null, pkg || null, servicesArray, JSON.stringify(questions || {}), notes || null, source || 'phone_call']
    );
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    serverError(res, error);
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
    serverError(res, error);
  }
});

app.get('/api/quotes/:id', async (req, res) => {
  try {
    if (!/^\d+$/.test(req.params.id)) return res.status(400).json({ success: false, error: 'Invalid quote ID' });
    const result = await pool.query('SELECT * FROM quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/quotes/:id', async (req, res) => {
  try {
    const { status } = req.body;
    const result = await pool.query('UPDATE quotes SET status = $1 WHERE id = $2 RETURNING *', [status, req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, quote: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/quotes/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM quotes WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Quote not found' });
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.get('/api/stats', async (req, res) => {
  try {
    const [totalResult, statusResult] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM quotes'),
      pool.query('SELECT status, COUNT(*) FROM quotes GROUP BY status')
    ]);
    const byStatus = {};
    statusResult.rows.forEach(row => { byStatus[row.status] = parseInt(row.count); });
    res.json({ success: true, stats: { total: parseInt(totalResult.rows[0].count), byStatus } });
  } catch (error) { serverError(res, error); }
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
  } catch (error) { serverError(res, error); }
});

app.get('/api/cancellations', async (req, res) => {
  try {
    const { status } = req.query;
    let query = 'SELECT * FROM cancellations';
    const params = [];
    if (status) { query += ' WHERE status = $1'; params.push(status); }
    query += ' ORDER BY created_at DESC';
    const result = await pool.query(query, params);
    res.json({ success: true, cancellations: result.rows });
  } catch (error) { serverError(res, error); }
});

app.get('/api/cancellations/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM cancellations WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, cancellation: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
  } catch (error) { serverError(res, error); }
});

app.delete('/api/cancellations/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM cancellations WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// CUSTOMERS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/api/customers', async (req, res) => {
  try {
    const { status, city, search, sort, type, limit = 1000, offset = 0 } = req.query;
    let query = 'SELECT * FROM customers WHERE 1=1';
    let countQuery = 'SELECT COUNT(*) FROM customers WHERE 1=1';
    const params = [], countParams = [];
    let p = 1, cp = 1;

    if (type) { query += ` AND customer_type = $${p++}`; countQuery += ` AND customer_type = $${cp++}`; params.push(type); countParams.push(type); }
    if (status) { query += ` AND status = $${p++}`; countQuery += ` AND status = $${cp++}`; params.push(status); countParams.push(status); }
    if (city) { query += ` AND city ILIKE $${p++}`; countQuery += ` AND city ILIKE $${cp++}`; params.push(`%${city}%`); countParams.push(`%${city}%`); }
    if (search) { query += ` AND (name ILIKE $${p} OR first_name ILIKE $${p} OR last_name ILIKE $${p} OR email ILIKE $${p} OR street ILIKE $${p})`; countQuery += ` AND (name ILIKE $${cp} OR first_name ILIKE $${cp} OR last_name ILIKE $${cp} OR email ILIKE $${cp})`; params.push(`%${search}%`); countParams.push(`%${search}%`); p++; cp++; }
    
    let orderBy = 'name ASC';
    if (sort === 'name_desc') orderBy = 'name DESC';
    else if (sort === 'newest') orderBy = 'created_at DESC';
    else if (sort === 'city_asc') orderBy = 'city ASC';
    
    query += ` ORDER BY ${orderBy} LIMIT $${p++} OFFSET $${p}`;
    params.push(limit, offset);
    
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, countParams)
    ]);
    res.json({ success: true, customers: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) { serverError(res, error); }
});

app.get('/api/customers/stats', async (req, res) => {
  try {
    const [total, active, cities, recent, previous] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM customers'),
      pool.query("SELECT COUNT(*) FROM customers WHERE LOWER(status) = 'active'"),
      pool.query('SELECT city, COUNT(*) as count FROM customers WHERE city IS NOT NULL GROUP BY city ORDER BY count DESC LIMIT 10'),
      pool.query("SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '30 days'"),
      pool.query("SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '60 days' AND created_at < NOW() - INTERVAL '30 days'")
    ]);
    const recentCount = parseInt(recent.rows[0].count);
    const prevCount = parseInt(previous.rows[0].count);
    let trendPct = 0;
    if (prevCount > 0) trendPct = Math.round(((recentCount - prevCount) / prevCount) * 100);
    else if (recentCount > 0) trendPct = 100;
    const inactive = parseInt(total.rows[0].count) - parseInt(active.rows[0].count);
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), active: parseInt(active.rows[0].count), inactive, topCities: cities.rows, trend: { recent: recentCount, previous: prevCount, pct: trendPct } } });
  } catch (error) { serverError(res, error); }
});

// GET /api/customers/pipeline-stats - Lead vs Customer pipeline metrics
app.get('/api/customers/pipeline-stats', async (req, res) => {
  try {
    const [leads, customers, newLeads, converted] = await Promise.all([
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'lead'"),
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'customer' OR customer_type IS NULL"),
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'lead' AND created_at >= NOW() - INTERVAL '30 days'"),
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'customer' AND created_at >= NOW() - INTERVAL '30 days'")
    ]);
    const totalLeads = parseInt(leads.rows[0].count);
    const totalCustomers = parseInt(customers.rows[0].count);
    const conversionRate = totalLeads + totalCustomers > 0 ? Math.round((totalCustomers / (totalLeads + totalCustomers)) * 100) : 0;
    res.json({ success: true, stats: {
      totalLeads, totalCustomers,
      newLeadsThisMonth: parseInt(newLeads.rows[0].count),
      convertedThisMonth: parseInt(converted.rows[0].count),
      conversionRate
    }});
  } catch (error) { serverError(res, error); }
});

// GET /api/customers/search - Search customers by name for auto-fill
// IMPORTANT: This must come BEFORE /api/customers/:id to avoid :id matching "search"
app.get('/api/customers/search', async (req, res) => {
  try {
    const query = req.query.name || req.query.q || req.query.search || '';
    if (!query || query.length < 2) {
      return res.json({ success: true, customers: [] });
    }

    const result = await pool.query(
      `SELECT id, COALESCE(name, TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')), 'Unknown') as name, email, phone, mobile, street, city, state, postal_code
       FROM customers
       WHERE LOWER(COALESCE(name, '')) LIKE LOWER($1)
          OR LOWER(COALESCE(first_name, '')) LIKE LOWER($1)
          OR LOWER(COALESCE(last_name, '')) LIKE LOWER($1)
          OR LOWER(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) LIKE LOWER($1)
          OR LOWER(COALESCE(email, '')) LIKE LOWER($1)
          OR COALESCE(phone, '') LIKE $1
       ORDER BY COALESCE(name, first_name, last_name, '')
       LIMIT 10`,
      [`%${query}%`]
    );

    res.json({ success: true, customers: result.rows });
  } catch (error) {
    console.error('Error searching customers:', error);
    serverError(res, error);
  }
});

app.get('/api/customers/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
        await Promise.all([
          pool.query('UPDATE invoices SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]),
          pool.query('UPDATE properties SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]),
          pool.query('UPDATE scheduled_jobs SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId]),
          pool.query('UPDATE messages SET customer_id=$1 WHERE customer_id=$2', [keepId, dupId])
        ]);

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
    serverError(res, e);
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
    serverError(res, e);
  }
});

// POST /api/customers - Create new customer (from Zapier/CopilotCRM sync)
app.post('/api/customers', validate(schemas.createCustomer), async (req, res) => {
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
        customer_number, name, email, status, customer_type,
        street, city, state, postal_code, phone, mobile,
        first_name, last_name, tags, notes, created_at, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *
    `, [
      customer_number || await nextCustomerNumber(),
      finalName,
      email || null,
      finalStatus,
      type || 'customer',
      street || null,
      city || null,
      state || null,
      finalPostalCode,
      phone || null,
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
    serverError(res, error);
  }
});

app.get('/api/customers/:id/properties', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM properties WHERE customer_id = $1', [req.params.id]);
    res.json({ success: true, properties: result.rows });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/customers/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ['name', 'first_name', 'last_name', 'status', 'email', 'phone', 'mobile', 'street', 'street2', 'city', 'state', 'postal_code', 'tags', 'notes', 'customer_type', 'customer_company_name', 'tax_exempt'];
    const sets = [], vals = [];
    let p = 1;
    // Map frontend field names to DB column names
    const fieldMap = { type: 'customer_type', company_name: 'customer_company_name' };
    Object.keys(req.body).forEach(k => {
      const dbCol = fieldMap[k] || k;
      if (allowed.includes(dbCol)) {
        let val = req.body[k];
        if (dbCol === 'tax_exempt') val = val === true || val === 'true';
        sets.push(`${dbCol} = $${p++}`);
        vals.push(val);
      }
    });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(id);
    const result = await pool.query(`UPDATE customers SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, customer: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/customers/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM customers WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
    serverError(res, error); 
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
       WHERE customer_id = $1
         OR LOWER(customer_name) = LOWER($2)
         OR LOWER(customer_name) LIKE LOWER($2) || ' %'
       ORDER BY job_date DESC LIMIT 50`,
      [req.params.id, customerName]
    );
    res.json({ success: true, jobs: jobsResult.rows });
  } catch (error) {
    console.error('Error fetching customer jobs:', error);
    serverError(res, error);
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
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// CALLS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.post('/api/calls', async (req, res) => {
  try {
    const { call_sid, twilio_sid, from_number, to_number, call_type, option_selected, status, duration, recording_url, transcription } = req.body;
    if (!from_number || !to_number) {
      return res.status(400).json({ success: false, error: 'from_number and to_number are required' });
    }
    const sid = twilio_sid || call_sid;
    const option = option_selected || call_type;
    const result = await pool.query(
      `INSERT INTO calls (twilio_sid, from_number, to_number, option_selected, status, duration, recording_url, transcription) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *`,
      [sid, from_number, to_number, option || 'Unknown', status || 'new', duration, recording_url, transcription]
    );
    res.json({ success: true, call: result.rows[0] });

    // Send push + email notification for voicemails (fire-and-forget)
    if (status === 'voicemail') {
      // Push notification
      const cleanedVmPhonePush = from_number.replace(/\D/g, '').slice(-10);
      let vmPushName = null;
      try {
        const vmPushCustomer = await pool.query(`SELECT name FROM customers WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 LIMIT 1`, [`%${cleanedVmPhonePush}`]);
        vmPushName = vmPushCustomer.rows[0]?.name || null;
      } catch (e) {}
      const vmPushTitle = `🎙️ ${vmPushName || from_number}`;
      const vmPushBody = transcription ? transcription.substring(0, 100) : 'New voicemail';
      sendPushToAllDevices(vmPushTitle, vmPushBody, { type: 'voicemail' }).catch(err => console.error('Voicemail push error:', err));
      const cleanedVmPhone = from_number.replace(/\D/g, '').slice(-10);
      let vmContactName = null;
      try {
        const vmCustomer = await pool.query(`SELECT name FROM customers WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 LIMIT 1`, [`%${cleanedVmPhone}`]);
        vmContactName = vmCustomer.rows[0]?.name || null;
      } catch (e) {}
      const vmDisplayName = vmContactName ? escapeHtml(vmContactName) : escapeHtml(from_number);
      const vmSubjectName = vmContactName || from_number;
      const vmTimestamp = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', dateStyle: 'medium', timeStyle: 'short' });
      const vmDuration = duration ? `${Math.floor(duration / 60)}m ${duration % 60}s` : 'Unknown';
      sendEmail(NOTIFICATION_EMAIL, `🎙️ New voicemail from ${vmSubjectName}`, emailTemplate(`
        <h2 style="color:#1e293b;margin:0 0 16px;">New Voicemail</h2>
        <table style="width:100%;border-collapse:collapse;">
          <tr><td style="padding:8px 0;color:#64748b;width:80px;">From</td><td style="padding:8px 0;color:#1e293b;font-weight:500;">${vmDisplayName}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;">Phone</td><td style="padding:8px 0;color:#1e293b;">${escapeHtml(from_number)}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;">Duration</td><td style="padding:8px 0;color:#1e293b;">${vmDuration}</td></tr>
          <tr><td style="padding:8px 0;color:#64748b;">Time</td><td style="padding:8px 0;color:#1e293b;">${vmTimestamp}</td></tr>
        </table>
        <div style="margin-top:20px;padding:16px;background:#f8fafc;border-radius:8px;border-left:4px solid #2e403d;">
          <p style="margin:0 0 4px;color:#64748b;font-size:12px;text-transform:uppercase;letter-spacing:0.5px;">Transcription</p>
          <p style="margin:0;color:#1e293b;line-height:1.6;">${transcription ? escapeHtml(transcription) : '<em style="color:#94a3b8;">No transcription available</em>'}</p>
        </div>
      `, { showSignature: false })).catch(err => console.error('Voicemail notification email error:', err));
    }
  } catch (error) { serverError(res, error); }
});

app.get('/api/calls', async (req, res) => {
  try {
    const { status, call_type, option_selected, limit = 100 } = req.query;
    const optFilter = option_selected || call_type;
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
    if (optFilter) { query += ` AND calls.option_selected = $${p++}`; params.push(optFilter); }
    query += ` ORDER BY calls.created_at DESC LIMIT $${p}`;
    params.push(limit);
    const result = await pool.query(query, params);
    res.json({ success: true, calls: result.rows });
  } catch (error) { serverError(res, error); }
});

app.get('/api/calls/stats', async (req, res) => {
  try {
    const [total, byStatus, byType] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM calls'),
      pool.query('SELECT status, COUNT(*) FROM calls GROUP BY status'),
      pool.query('SELECT option_selected, COUNT(*) FROM calls GROUP BY option_selected')
    ]);
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), byStatus: Object.fromEntries(byStatus.rows.map(r => [r.status, parseInt(r.count)])), byType: Object.fromEntries(byType.rows.map(r => [r.option_selected, parseInt(r.count)])) } });
  } catch (error) { serverError(res, error); }
});

app.get('/api/calls/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM calls WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, call: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
  } catch (error) { serverError(res, error); }
});

app.delete('/api/calls/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM calls WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// SCHEDULED JOBS ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/api/jobs', async (req, res) => {
  try {
    const { date, status, crew, start_date, end_date, search, limit } = req.query;
    let query = 'SELECT * FROM scheduled_jobs WHERE 1=1';
    const params = [];
    let p = 1;
    if (date) { query += ` AND job_date::date = $${p++}::date`; params.push(date); }
    if (start_date && end_date) { query += ` AND job_date::date BETWEEN $${p++}::date AND $${p++}::date`; params.push(start_date, end_date); }
    if (status) { query += ` AND status = $${p++}`; params.push(status); }
    if (crew) { query += ` AND crew_assigned = $${p++}`; params.push(crew); }
    if (search) { query += ` AND (customer_name ILIKE $${p} OR service_type ILIKE $${p} OR address ILIKE $${p})`; params.push(`%${search}%`); p++; }
    query += ' ORDER BY job_date ASC, route_order ASC NULLS LAST';
    if (limit) { query += ` LIMIT $${p++}`; params.push(parseInt(limit)); }
    const result = await pool.query(query, params);
    res.json({ success: true, jobs: result.rows });
  } catch (error) { serverError(res, error); }
});

app.get('/api/jobs/stats', async (req, res) => {
  try {
    const { date } = req.query;
    let filter = '';
    const params = [];
    if (date) { filter = ' WHERE job_date::date = $1::date'; params.push(date); }
    const [total, byStatus, revenue, byCrew] = await Promise.all([
      pool.query(`SELECT COUNT(*) FROM scheduled_jobs${filter}`, params),
      pool.query(`SELECT status, COUNT(*) FROM scheduled_jobs${filter} GROUP BY status`, params),
      pool.query(`SELECT COALESCE(SUM(service_price), 0) as total FROM scheduled_jobs${filter}`, params),
      pool.query(`SELECT COALESCE(crew_assigned, 'Unassigned') as crew, COUNT(*) FROM scheduled_jobs${filter} GROUP BY crew_assigned`, params)
    ]);
    res.json({ success: true, stats: { total: parseInt(total.rows[0].count), byStatus: Object.fromEntries(byStatus.rows.map(r => [r.status, parseInt(r.count)])), totalRevenue: parseFloat(revenue.rows[0].total), byCrew: Object.fromEntries(byCrew.rows.map(r => [r.crew, parseInt(r.count)])) } });
  } catch (error) { serverError(res, error); }
});

app.get('/api/jobs/dashboard', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const [todayCount, weekCount, pending, upcoming] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM scheduled_jobs WHERE job_date::date = $1::date', [today]),
      pool.query("SELECT COUNT(*) FROM scheduled_jobs WHERE job_date::date BETWEEN $1::date AND ($1::date + interval '7 days')", [today]),
      pool.query('SELECT COUNT(*) FROM scheduled_jobs WHERE status = $1 AND job_date::date >= $2::date', ['pending', today]),
      pool.query(`SELECT id, job_date, customer_name, service_type, address, status, service_price FROM scheduled_jobs WHERE job_date::date >= $1::date ORDER BY job_date ASC LIMIT 5`, [today])
    ]);
    res.json({ success: true, stats: { today: parseInt(todayCount.rows[0].count), thisWeek: parseInt(weekCount.rows[0].count), pending: parseInt(pending.rows[0].count) }, upcoming: upcoming.rows });
  } catch (error) { serverError(res, error); }
});

// GET /api/jobs/calendar-summary?month=YYYY-MM - Day-by-day job counts with crew colors (Phase 5)
app.get('/api/jobs/calendar-summary', async (req, res) => {
  try {
    const { month } = req.query;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return res.status(400).json({ success: false, error: 'Provide month as YYYY-MM' });
    }
    const startDate = month + '-01';
    const [year, mon] = month.split('-').map(Number);
    const lastDay = new Date(year, mon, 0).getDate();
    const endDate = month + '-' + String(lastDay).padStart(2, '0');

    const result = await pool.query(
      `SELECT
         job_date::date AS day,
         COUNT(*) AS total_jobs,
         COUNT(*) FILTER (WHERE status = 'completed') AS completed,
         COUNT(*) FILTER (WHERE status = 'pending') AS pending,
         COUNT(*) FILTER (WHERE status IN ('in_progress')) AS in_progress,
         COALESCE(SUM(service_price), 0) AS revenue,
         json_agg(json_build_object(
           'crew', COALESCE(crew_assigned, 'Unassigned'),
           'count', 1
         )) AS crew_details
       FROM scheduled_jobs
       WHERE job_date::date BETWEEN $1::date AND $2::date
       GROUP BY job_date::date
       ORDER BY job_date::date`,
      [startDate, endDate]
    );

    const days = result.rows.map(row => {
      const crewCounts = {};
      (row.crew_details || []).forEach(d => {
        crewCounts[d.crew] = (crewCounts[d.crew] || 0) + d.count;
      });
      return {
        day: row.day,
        total_jobs: parseInt(row.total_jobs),
        completed: parseInt(row.completed),
        pending: parseInt(row.pending),
        in_progress: parseInt(row.in_progress),
        revenue: parseFloat(row.revenue),
        crews: crewCounts
      };
    });

    res.json({ success: true, month, days });
  } catch (error) {
    serverError(res, error);
  }
});

// GET /api/jobs/completed-uninvoiced - Completed jobs without invoices (must be before :id)
app.get('/api/jobs/completed-uninvoiced', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, job_date, customer_name, customer_id, service_type, service_price, address
      FROM scheduled_jobs
      WHERE status IN ('completed', 'done')
        AND (invoice_id IS NULL OR invoice_id = 0)
      ORDER BY job_date DESC LIMIT 200
    `);
    res.json({ success: true, jobs: result.rows });
  } catch (error) {
    console.error('Error fetching completed uninvoiced jobs:', error);
    serverError(res, error);
  }
});

// GET /api/jobs/pipeline - Jobs grouped by pipeline stage (must be before :id route)
app.get('/api/jobs/pipeline', async (req, res) => {
  try {
    const stages = ['new', 'quoted', 'scheduled', 'in_progress', 'completed', 'invoiced'];
    // Map existing statuses to pipeline stages
    const result = await pool.query(`
      SELECT *,
        CASE
          WHEN pipeline_stage IS NOT NULL AND pipeline_stage != '' THEN pipeline_stage
          WHEN status = 'completed' THEN 'completed'
          WHEN status = 'in-progress' THEN 'in_progress'
          WHEN status = 'confirmed' THEN 'scheduled'
          ELSE 'new'
        END as stage
      FROM scheduled_jobs
      WHERE status != 'cancelled'
      ORDER BY job_date DESC
    `);
    const grouped = {};
    stages.forEach(s => grouped[s] = []);
    result.rows.forEach(j => {
      const s = stages.includes(j.stage) ? j.stage : 'new';
      grouped[s].push(j);
    });
    const counts = {};
    stages.forEach(s => counts[s] = grouped[s].length);
    res.json({ success: true, stages, pipeline: grouped, counts });
  } catch (error) { serverError(res, error); }
});

app.get('/api/jobs/:id', async (req, res) => {
  try {
    if (!/^\d+$/.test(req.params.id)) return res.status(400).json({ success: false, error: 'Invalid job ID' });
    const result = await pool.query('SELECT * FROM scheduled_jobs WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/jobs', validate(schemas.createJob), async (req, res) => {
  try {
    const { job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned, latitude, longitude } = req.body;
    const result = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, route_order, estimated_duration, crew_assigned, latitude, longitude) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING *`,
      [job_date, customer_name, customer_id, service_type, service_frequency, service_price || 0, address, phone, special_notes, property_notes, status || 'pending', route_order, estimated_duration || 30, crew_assigned, latitude, longitude]
    );
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
  } catch (error) { serverError(res, error); }
});

app.patch('/api/jobs/:id', async (req, res) => {
  try {
    const allowed = ['job_date', 'customer_name', 'service_type', 'service_price', 'address', 'phone', 'special_notes', 'property_notes', 'status', 'route_order', 'crew_assigned', 'completed_at', 'pipeline_stage', 'is_recurring', 'recurring_pattern', 'recurring_day_of_week', 'recurring_start_date', 'recurring_end_date', 'material_cost', 'labor_cost', 'expense_total', 'invoice_id', 'property_id', 'estimated_duration'];
    const sets = [], vals = [];
    let p = 1;
    Object.keys(req.body).forEach(k => { if (allowed.includes(k)) { sets.push(`${k} = $${p++}`); vals.push(req.body[k]); } });
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE scheduled_jobs SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/jobs/:id/complete', async (req, res) => {
  try {
    const { completion_notes } = req.body;
    const result = await pool.query(
      `UPDATE scheduled_jobs SET status = 'completed', completed_at = CURRENT_TIMESTAMP, completion_notes = COALESCE($2, completion_notes), updated_at = CURRENT_TIMESTAMP WHERE id = $1 RETURNING *`,
      [req.params.id, completion_notes || null]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Job not found' });
    }

    // --- Auto-add completed job to monthly invoice ---
    try {
      const completedJob = result.rows[0];
      const custId = completedJob.customer_id;
      const custName = completedJob.customer_name;

      if (custId) {
        // Find current month boundaries
        const now = new Date();
        const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split('T')[0];
        const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().split('T')[0];

        // Look for existing draft invoice for this customer in this month
        const existingInv = await pool.query(
          `SELECT id, line_items, subtotal, tax_rate, tax_amount, total FROM invoices
           WHERE customer_id = $1 AND status = 'draft'
           AND created_at >= $2 AND created_at <= ($3::date + interval '1 day')
           ORDER BY created_at DESC LIMIT 1`,
          [custId, monthStart, monthEnd]
        );

        const propertyId = completedJob.property_id || null;
        let propertyName = null;
        if (propertyId) {
          const propRow = await pool.query('SELECT property_name, street FROM properties WHERE id = $1', [propertyId]);
          if (propRow.rows[0]) propertyName = propRow.rows[0].property_name || propRow.rows[0].street || null;
        }

        const newItem = {
          name: completedJob.service_type || 'Service',
          description: 'Job #' + completedJob.id + (completedJob.job_date ? ' - ' + new Date(completedJob.job_date).toLocaleDateString('en-US', {month:'short', day:'numeric'}) : ''),
          quantity: 1,
          rate: parseFloat(completedJob.service_price) || 0,
          amount: parseFloat(completedJob.service_price) || 0,
          service_date: completedJob.completed_at ? new Date(completedJob.completed_at).toISOString().split('T')[0] : (completedJob.job_date ? new Date(completedJob.job_date).toISOString().split('T')[0] : null),
          property_name: propertyName
        };

        if (existingInv.rows.length > 0) {
          // Add line item to existing draft invoice
          const inv = existingInv.rows[0];
          let items = inv.line_items || [];
          if (typeof items === 'string') items = JSON.parse(items);
          items.push(newItem);
          const taxResult = await calculateTax(custId, propertyId, items);

          await pool.query(
            `UPDATE invoices SET line_items = $1, subtotal = $2, tax_amount = $3, total = $4, updated_at = CURRENT_TIMESTAMP WHERE id = $5`,
            [JSON.stringify(taxResult.lineItems), taxResult.subtotal, taxResult.taxAmount, taxResult.total, inv.id]
          );
          await pool.query(`UPDATE scheduled_jobs SET invoice_id = $1 WHERE id = $2`, [inv.id, completedJob.id]);
        } else {
          // Create new monthly draft invoice
          const custRow = await pool.query(`SELECT email, address FROM customers WHERE id = $1`, [custId]);
          const custEmail = custRow.rows[0]?.email || '';
          const custAddress = custRow.rows[0]?.address || completedJob.address || '';

          const invNum = await nextInvoiceNumber();
          const dueDate = new Date(now.getFullYear(), now.getMonth() + 1, 0); // End of current month
          const taxResult = await calculateTax(custId, propertyId, [newItem]);

          const invResult = await pool.query(
            `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, customer_address, job_id, status, subtotal, tax_rate, tax_amount, total, due_date, notes, line_items)
             VALUES ($1, $2, $3, $4, $5, $6, 'draft', $7, 0, $8, $9, $10, $11, $12) RETURNING id`,
            [invNum, custId, custName, custEmail, custAddress, completedJob.id, taxResult.subtotal, taxResult.taxAmount, taxResult.total, dueDate.toISOString().split('T')[0],
             'Monthly invoice - ' + now.toLocaleDateString('en-US', {month:'long', year:'numeric'}),
             JSON.stringify(taxResult.lineItems)]
          );
          await pool.query(`UPDATE scheduled_jobs SET invoice_id = $1 WHERE id = $2`, [invResult.rows[0].id, completedJob.id]);
        }
      }
    } catch (autoInvErr) {
      console.error('Auto-invoice error (non-fatal):', autoInvErr.message);
    }

    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/jobs/reorder', async (req, res) => {
  try {
    const { jobs } = req.body;
    for (const job of jobs) { await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]); }
    res.json({ success: true, updated: jobs.length });
  } catch (error) { serverError(res, error); }
});

app.post('/api/jobs/optimize-route', async (req, res) => {
  try {
    const { date, startAddress, crew } = req.body;

    let query = 'SELECT * FROM scheduled_jobs WHERE job_date::date = $1::date AND status != $2';
    let params = [date, 'completed'];
    if (crew) { query += ' AND crew_assigned = $3'; params.push(crew); }
    query += ' ORDER BY route_order ASC NULLS LAST';

    const jobsResult = await pool.query(query, params);
    const jobs = jobsResult.rows;
    if (jobs.length < 2) return res.json({ success: true, message: 'Not enough jobs', jobs });

    // Need lat/lng for all jobs — geocode any missing ones first
    const GMAPS_KEY = process.env.GOOGLE_MAPS_API_KEY;
    const needsGeocode = jobs.filter(j => !j.lat || !j.lng);
    for (const job of needsGeocode) {
      if (!job.address) continue;
      try {
        const q = encodeURIComponent(job.address);
        if (GMAPS_KEY) {
          const gRes = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${q}&key=${GMAPS_KEY}`);
          const gData = await gRes.json();
          if (gData.status === 'OK' && gData.results && gData.results.length > 0) {
            const loc = gData.results[0].geometry.location;
            job.lat = loc.lat;
            job.lng = loc.lng;
            await pool.query('UPDATE scheduled_jobs SET lat = $1, lng = $2 WHERE id = $3', [job.lat, job.lng, job.id]);
          }
        } else {
          const gRes = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${q}&limit=1&countrycodes=us`);
          const gData = await gRes.json();
          if (gData && gData.length > 0) {
            job.lat = parseFloat(gData[0].lat);
            job.lng = parseFloat(gData[0].lon);
            await pool.query('UPDATE scheduled_jobs SET lat = $1, lng = $2 WHERE id = $3', [job.lat, job.lng, job.id]);
          }
          await new Promise(r => setTimeout(r, 1100));
        }
      } catch (e) { /* skip */ }
    }

    const geocodedJobs = jobs.filter(j => j.lat && j.lng);
    if (geocodedJobs.length < 2) return res.status(400).json({ success: false, error: 'Not enough geocoded jobs. Check addresses.' });

    // Get home base from settings (default: Pappas HQ)
    let startLat = 41.4268;
    let startLng = -81.7356;
    try {
      const hbResult = await pool.query("SELECT value FROM business_settings WHERE key = 'home_base'");
      if (hbResult.rows.length > 0) {
        const hb = hbResult.rows[0].value;
        if (hb.lat) startLat = parseFloat(hb.lat);
        if (hb.lng) startLng = parseFloat(hb.lng);
      }
    } catch(e) { /* use defaults */ }

    if (GMAPS_KEY) {
      try {
        // Use Google Directions API with waypoint optimization
        const origin = `${startLat},${startLng}`;
        const waypoints = geocodedJobs.map(j => `${parseFloat(j.lat)},${parseFloat(j.lng)}`).join('|');
        const url = `https://maps.googleapis.com/maps/api/directions/json?origin=${origin}&destination=${origin}&waypoints=optimize:true|${waypoints}&key=${GMAPS_KEY}`;
        const gRes = await fetch(url);
        const gData = await gRes.json();
        if (gData.status === 'OK' && gData.routes && gData.routes.length > 0) {
          const optimizedOrder = gData.routes[0].waypoint_order;
          const optimizedJobs = optimizedOrder.map((origIdx, newOrder) => ({ ...geocodedJobs[origIdx], route_order: newOrder + 1 }));
          for (const job of optimizedJobs) { await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]); }
          const legs = gData.routes[0].legs;
          const totalDistance = legs.reduce((sum, l) => sum + (l.distance?.value || 0), 0);
          const totalDuration = legs.reduce((sum, l) => sum + (l.duration?.value || 0), 0);
          return res.json({ success: true, message: 'Route optimized via Google Directions', jobs: optimizedJobs, stats: { totalStops: geocodedJobs.length, totalDistance: (totalDistance / 1609.34).toFixed(1) + ' miles', totalDriveTime: Math.round(totalDuration / 60) + ' minutes' } });
        }
      } catch (e) { console.error('Google Directions optimize failed, using fallback:', e.message); }
    }

    // Fallback: nearest-neighbor TSP
    const stops = geocodedJobs.map(j => ({ ...j, lat: parseFloat(j.lat), lng: parseFloat(j.lng) }));
    const visited = new Set();
    const order = [];
    let curLat = startLat, curLng = startLng;
    while (order.length < stops.length) {
      let nearest = null, nearestDist = Infinity;
      for (const s of stops) {
        if (visited.has(s.id)) continue;
        const d = haversine(curLat, curLng, s.lat, s.lng);
        if (d < nearestDist) { nearestDist = d; nearest = s; }
      }
      if (!nearest) break;
      visited.add(nearest.id);
      order.push(nearest);
      curLat = nearest.lat; curLng = nearest.lng;
    }
    const optimizedJobs = order.map((j, i) => ({ ...j, route_order: i + 1 }));
    for (const job of optimizedJobs) { await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [job.route_order, job.id]); }
    res.json({ success: true, message: 'Route optimized (local algorithm)', jobs: optimizedJobs, stats: { totalStops: optimizedJobs.length } });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/jobs/bulk', async (req, res) => {
  try {
    const { ids } = req.body;
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ success: false, error: 'ids must be a non-empty array' });
    }
    const intIds = ids.map(id => parseInt(id)).filter(id => !isNaN(id));
    if (intIds.length === 0) {
      return res.status(400).json({ success: false, error: 'No valid job IDs provided' });
    }
    const result = await pool.query('DELETE FROM scheduled_jobs WHERE id = ANY($1::int[]) RETURNING id', [intIds]);
    res.json({ success: true, deleted: result.rowCount });
  } catch (error) { serverError(res, error, 'Bulk delete jobs'); }
});

app.delete('/api/jobs/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM scheduled_jobs WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
  } catch (error) { serverError(res, error); }
});

app.post('/api/crews', validate(schemas.createCrew), async (req, res) => {
  try {
    const { name, members, crew_type, notes } = req.body;
    const result = await pool.query('INSERT INTO crews (name, members, crew_type, notes) VALUES ($1, $2, $3, $4) RETURNING *', [name, members, crew_type, notes]);
    res.json({ success: true, crew: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
  } catch (error) { serverError(res, error); }
});

app.delete('/api/crews/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM crews WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// EMPLOYEE ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/api/employees', async (req, res) => {
  try {
    const { active_only, search } = req.query;
    let query = 'SELECT id, title, first_name, last_name, birth_date, hire_date, salary_amount, pay_type, chemical_license, email, phone, address, notes, login_email, permissions, is_active, created_at, updated_at FROM employees WHERE 1=1';
    const params = [];
    let p = 1;
    if (active_only === 'true') { query += ' AND is_active = true'; }
    if (search) { query += ` AND (first_name ILIKE $${p} OR last_name ILIKE $${p} OR email ILIKE $${p} OR title ILIKE $${p})`; params.push(`%${search}%`); p++; }
    query += ' ORDER BY last_name ASC, first_name ASC';
    const result = await pool.query(query, params);
    res.json({ success: true, employees: result.rows });
  } catch (error) { serverError(res, error); }
});

app.get('/api/employees/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, title, first_name, last_name, birth_date, hire_date, salary_amount, pay_type, chemical_license, email, phone, address, notes, login_email, permissions, is_active, created_at, updated_at FROM employees WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Employee not found' });
    res.json({ success: true, employee: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/employees', validate(schemas.createEmployee), async (req, res) => {
  try {
    const { title, first_name, last_name, birth_date, hire_date, salary_amount, pay_type, chemical_license, email, phone, address, notes, login_email, password, permissions } = req.body;
    const pw_hash = password ? hashPassword(password) : null;
    const result = await pool.query(
      `INSERT INTO employees (title, first_name, last_name, birth_date, hire_date, salary_amount, pay_type, chemical_license, email, phone, address, notes, login_email, password_hash, permissions)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING id, title, first_name, last_name, email, is_active`,
      [title, first_name, last_name, birth_date || null, hire_date || null, salary_amount || null, pay_type || 'hourly', chemical_license, email, phone, address, notes, login_email, pw_hash, JSON.stringify(permissions || [])]
    );
    res.json({ success: true, employee: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/employees/:id', async (req, res) => {
  try {
    const fields = ['title','first_name','last_name','birth_date','hire_date','salary_amount','pay_type','chemical_license','email','phone','address','notes','login_email','permissions','is_active'];
    const sets = [], vals = [];
    let p = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) {
        sets.push(`${f} = $${p++}`);
        vals.push(f === 'permissions' ? JSON.stringify(req.body[f]) : req.body[f]);
      }
    }
    if (req.body.password) { sets.push(`password_hash = $${p++}`); vals.push(hashPassword(req.body.password)); }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields to update' });
    sets.push('updated_at = CURRENT_TIMESTAMP');
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE employees SET ${sets.join(', ')} WHERE id = $${p} RETURNING id, title, first_name, last_name, email, is_active`, vals);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Employee not found' });
    res.json({ success: true, employee: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/employees/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM employees WHERE id = $1 RETURNING id, first_name, last_name', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Employee not found' });
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
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
  } catch (error) { serverError(res, error); }
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
      // Format: "FirstName LastName 1234 Street Name  City State Zip, Country"
      // Split on 2+ spaces first to separate street from city
      const parts = details.split(/\s{2,}/);
      const nameAndStreet = parts[0] || '';
      const cityStateZip = parts.slice(1).join(' ').trim();
      // Split name from street: name ends before the first house number (digits)
      const match = nameAndStreet.match(/^(.*?)\s+(\d+\s+.*)$/);
      if (match) {
        const street = cityStateZip ? match[2] + ', ' + cityStateZip : match[2];
        return { name: match[1].trim(), address: street };
      }
      return { name: nameAndStreet.trim(), address: cityStateZip };
    }
    
    let imported = 0, updated = 0, skipped = 0;
    const importedJobs = [];
    for (const job of jobs) {
      try {
        const jobDate = parseDate(job['Date of Service']);
        if (!jobDate) { skipped++; continue; }
        const { name, address } = parseNameAddress(job['Name / Details']);
        const rawTitle = job['Title'] || 'Service';
        // Extract crew name from title (e.g. "Spring Cleanup Rob Mowing Crew" -> service: "Spring Cleanup", crew: "Rob Mowing Crew")
        const crewMatch = rawTitle.match(/^(.+?)\s{1,2}(\w+\s+(?:Mowing|Cleanup|Lawn|Landscape|Plow|Snow)\s+Crew)$/i);
        const serviceType = crewMatch ? crewMatch[1].trim() : rawTitle.trim();
        const crewAssigned = crewMatch ? crewMatch[2].trim() : null;
        const price = parseFloat((job['Visit Total'] || '0').replace(/[^0-9.]/g, '')) || 0;

        // Try to match customer by name
        const nameParts = name.split(' ');
        const firstName = nameParts[0] || '';
        const lastName = nameParts.slice(1).join(' ') || '';
        let customerId = null, customerPhone = null, customerMobile = null;

        const custMatch = await pool.query(
          `SELECT id, phone, mobile FROM customers
           WHERE LOWER(TRIM(name)) = LOWER($1)
              OR (LOWER(TRIM(first_name)) = LOWER($2) AND LOWER(TRIM(last_name)) = LOWER($3))
           LIMIT 1`,
          [name, firstName, lastName]
        );
        if (custMatch.rows.length > 0) {
          customerId = custMatch.rows[0].id;
          customerPhone = custMatch.rows[0].phone;
          customerMobile = custMatch.rows[0].mobile;
        }

        // Check for existing by date + customer (ignore old service_type with crew name)
        const existing = await pool.query('SELECT id FROM scheduled_jobs WHERE job_date = $1 AND customer_name = $2', [jobDate, name]);
        let jobId;
        if (existing.rows.length > 0) {
          jobId = existing.rows[0].id;
          await pool.query('UPDATE scheduled_jobs SET address = $1, service_price = $2, customer_id = COALESCE($3, customer_id), service_type = $4, crew_assigned = COALESCE($5, crew_assigned) WHERE id = $6', [address, price, customerId, serviceType, crewAssigned, jobId]);
          updated++;
        } else {
          const insertResult = await pool.query('INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_price, address, status, crew_assigned) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id', [jobDate, name, customerId, serviceType, price, address, 'pending', crewAssigned]);
          jobId = insertResult.rows[0].id;
          imported++;
        }

        importedJobs.push({
          id: jobId,
          job_date: jobDate,
          customer_name: name,
          customer_id: customerId,
          service_type: serviceType,
          service_price: price,
          address,
          phone: customerMobile || customerPhone || null
        });
      } catch (e) { skipped++; }
    }
    res.json({ success: true, message: 'Import complete', stats: { total: jobs.length, imported, updated, skipped }, jobs: importedJobs });
  } catch (error) { serverError(res, error); }
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
    serverError(res, error);
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
    serverError(res, error);
  }
});

// GET /api/expenses/stats
app.get('/api/expenses/stats', async (req, res) => {
  try {
    const { year } = req.query;
    const currentYear = year || new Date().getFullYear();
    const currentMonth = new Date().getMonth() + 1;
    
    const [yearTotal, monthTotal, byCategory] = await Promise.all([
      pool.query('SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM expenses WHERE EXTRACT(YEAR FROM expense_date) = $1', [currentYear]),
      pool.query('SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM expenses WHERE EXTRACT(YEAR FROM expense_date) = $1 AND EXTRACT(MONTH FROM expense_date) = $2', [currentYear, currentMonth]),
      pool.query('SELECT category, COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM expenses WHERE EXTRACT(YEAR FROM expense_date) = $1 GROUP BY category', [currentYear])
    ]);
    
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
    serverError(res, error);
  }
});

// POST /api/expenses
app.post('/api/expenses', validate(schemas.createExpense), async (req, res) => {
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
      LEFT JOIN campaign_submissions s ON c.name = s.campaign_id::text OR c.id::text = s.campaign_id::text
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
    serverError(res, error);
  }
});

// POST /api/campaigns - Create a new campaign
app.post('/api/campaigns', validate(schemas.createCampaign), async (req, res) => {
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
    serverError(res, error);
  }
});

// GET /api/campaigns/:id - Get single campaign
app.get('/api/campaigns/:id', async (req, res) => {
  try {
    const { id } = req.params;
    // If id is numeric, search by id; otherwise search by name
    const isNumeric = /^\d+$/.test(id);
    const result = isNumeric
      ? await pool.query('SELECT * FROM campaigns WHERE id = $1', [id])
      : await pool.query('SELECT * FROM campaigns WHERE name = $1', [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    res.json({ success: true, campaign: result.rows[0] });
  } catch (error) {
    console.error('Error fetching campaign:', error);
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query('SELECT COUNT(*) as total FROM campaign_submissions WHERE campaign_id = $1', [id])
    ]);
    res.json({
      success: true,
      submissions: result.rows,
      total: parseInt(countResult.rows[0]?.total || 0)
    });
  } catch (error) {
    console.error('Error fetching submissions:', error);
    serverError(res, error);
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
    const dashboardUrl = (process.env.BASE_URL || 'https://app.pappaslandscaping.com') + '/campaigns.html';
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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

    const [result, countsResult] = await Promise.all([
      pool.query(query, values),
      pool.query(`SELECT status, COUNT(*) as count FROM sent_quotes GROUP BY status`)
    ]);
    const counts = { total: 0, draft: 0, sent: 0, viewed: 0, signed: 0, declined: 0 };
    countsResult.rows.forEach(row => {
      counts[row.status] = parseInt(row.count);
      counts.total += parseInt(row.count);
    });

    res.json({ success: true, quotes: result.rows, counts });
  } catch (error) {
    console.error('Error fetching sent quotes:', error);
    serverError(res, error);
  }
});

// GET /api/sent-quotes/view-counts - Bulk view counts for all sent quotes
// IMPORTANT: Must be registered BEFORE :id route
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
    serverError(res, error);
  }
});

// GET /api/sent-quotes/event-counts - Bulk event counts (resend/edit tracking)
// IMPORTANT: Must be registered BEFORE :id route
app.get('/api/sent-quotes/event-counts', async (req, res) => {
  try {
    await ensureQuoteEventsTable();
    const result = await pool.query(
      `SELECT sent_quote_id,
        COUNT(*) FILTER (WHERE event_type = 'resent') as resend_count,
        COUNT(*) FILTER (WHERE event_type = 'edited') as edit_count,
        COUNT(*) FILTER (WHERE event_type IN ('sent', 'resent')) as total_sends
       FROM quote_events
       GROUP BY sent_quote_id`
    );
    const map = {};
    result.rows.forEach(r => {
      map[r.sent_quote_id] = {
        resend_count: parseInt(r.resend_count),
        edit_count: parseInt(r.edit_count),
        total_sends: parseInt(r.total_sends)
      };
    });
    res.json({ success: true, eventCounts: map });
  } catch (error) {
    console.error('Error fetching event counts:', error);
    res.json({ success: true, eventCounts: {} });
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
    serverError(res, error);
  }
});

// GET /api/services - Get list of predefined services
app.get('/api/services', (req, res) => {
  const services = Object.entries(SERVICE_DESCRIPTIONS).map(([name, description]) => ({
    name,
    description: description.replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '')
  }));
  res.json({ success: true, services });
});

// ═══════════════════════════════════════════════════════════
// QUOTE EVENT TRACKING
// ═══════════════════════════════════════════════════════════
async function ensureQuoteEventsTable() {
  await _ensureQuoteEventsTable(pool);
}

async function logQuoteEvent(quoteId, eventType, description, details = null) {
  try {
    await ensureQuoteEventsTable();
    await pool.query(
      'INSERT INTO quote_events (sent_quote_id, event_type, description, details) VALUES ($1, $2, $3, $4)',
      [quoteId, eventType, description, details ? JSON.stringify(details) : null]
    );
  } catch (e) {
    console.error('Error logging quote event:', e);
  }
}

// POST /api/sent-quotes - Create new quote
app.post('/api/sent-quotes', validate(schemas.createSentQuote), async (req, res) => {
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
        const newCustNum = await nextCustomerNumber();
        const newCustomer = await pool.query(
          `INSERT INTO customers (customer_number, name, email, phone, street, created_at)
           VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) RETURNING id`,
          [newCustNum, customer_name, customer_email, customer_phone, customer_address]
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

    // Log creation event
    await logQuoteEvent(result.rows[0].id, 'created', 'Quote created', {
      total: total,
      services_count: services ? services.length : 0
    });

    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error creating quote:', error);
    serverError(res, error);
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

    // Log edit event with what changed
    const changedFields = allowedFields.filter(f => req.body[f] !== undefined);
    await logQuoteEvent(id, 'edited', 'Quote edited', {
      fields_changed: changedFields,
      new_total: result.rows[0].total
    });

    res.json({ success: true, quote: result.rows[0] });
  } catch (error) {
    console.error('Error updating quote:', error);
    serverError(res, error);
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

    const signUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-quote.html?token=${quote.sign_token}`;
    const quoteNumber = quote.quote_number || `Q-${quote.id}`;

    // Clean email - detailed but warm tone
    const firstName = (quote.customer_name || '').split(' ')[0] || 'there';
    
    const emailContent = `
      <div style="text-align:center;margin:0 0 28px;">
        <img src="${process.env.EMAIL_ASSETS_URL || process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/email-assets/heading-quote.png" alt="Your Quote is Ready" style="max-width:400px;width:auto;height:34px;" />
      </div>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Hi ${firstName},</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Thanks for reaching out to Pappas & Co. Landscaping! We've put together a custom quote for your property that includes the scope of work and pricing for your requested services.</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Click the button below to view your full quote:</p>

      <div style="text-align:center;margin:28px 0 20px;">
        <a href="${signUrl}" style="background:#c9dd80;color:#2e403d;padding:16px 52px;text-decoration:none;border-radius:50px;font-weight:700;font-size:15px;display:inline-block;letter-spacing:0.3px;">View Your Quote \u{2192}</a>
      </div>
      <p style="font-size:14px;color:#94a3b8;text-align:center;margin:0 0 24px;">Or just reply to this email with any questions</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 8px;font-weight:600;">From the quote page, you can:</p>

      <ul style="color:#4a5568;font-size:15px;line-height:1.8;padding-left:20px;margin:0 0 18px;">
        <li><strong>Accept the quote</strong> to secure your spot on our schedule and sign the service agreement</li>
        <li><strong>Request changes</strong> if you'd like us to adjust the scope of work</li>
      </ul>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">If you have any questions, feel free to call or text us at <strong>440-886-7318</strong>. We're always happy to help!</p>

      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0;">We look forward to working with you!</p>
    `;

    // Generate branded PDF attachment
    console.log('📄 Generating quote PDF for quote #' + quoteNumber + '...');
    const pdfResult = await generateQuotePDF(quote);
    let attachments = null;
    let pdfAttached = false;
    let pdfType = 'none';
    let pdfError = null;

    if (pdfResult && pdfResult.bytes) {
      const pdfSize = pdfResult.bytes.length;
      pdfType = pdfResult.type || 'unknown';
      pdfError = pdfResult.error || null;
      console.log('✅ Quote PDF generated (' + pdfType + '): ' + pdfSize + ' bytes (' + Math.round(pdfSize / 1024) + ' KB)');
      if (pdfError) console.log('⚠️ Main PDF error (fallback used): ' + pdfError);
      attachments = [{
        filename: 'Quote-' + quoteNumber + '-' + quote.customer_name.replace(/[^a-zA-Z0-9]/g, '-') + '.pdf',
        content: Buffer.from(pdfResult.bytes).toString('base64'),
        type: 'application/pdf'
      }];
      pdfAttached = true;
    } else {
      pdfError = pdfResult ? pdfResult.error : 'generateQuotePDF returned null';
      console.error('❌ Quote PDF generation failed:', pdfError);
    }

    await sendEmail(
      quote.customer_email,
      'Your ' + (quote.quote_type === 'monthly_plan' ? 'Annual Care Plan' : 'Quote') + ' from ' + COMPANY_NAME,
      emailTemplate(emailContent),
      attachments,
      { type: 'quote', customer_id: quote.customer_id, customer_name: quote.customer_name, quote_id: quote.id }
    );

    // Determine if this is first send or resend
    const isResend = quote.sent_at !== null;

    // Update status to sent
    await pool.query(
      'UPDATE sent_quotes SET status = $1, sent_at = CURRENT_TIMESTAMP WHERE id = $2',
      ['sent', id]
    );

    // Log send/resend event
    await logQuoteEvent(id, isResend ? 'resent' : 'sent',
      isResend ? 'Quote resent to ' + quote.customer_email : 'Quote sent to ' + quote.customer_email, {
      email: quote.customer_email,
      total: quote.total,
      pdf_attached: pdfAttached
    });

    res.json({ success: true, message: 'Quote sent successfully', pdfAttached, pdfType, pdfError, pdfSize: pdfResult && pdfResult.bytes ? pdfResult.bytes.length : 0 });
  } catch (error) {
    console.error('Error sending quote:', error);
    serverError(res, error);
  }
});

// POST /api/sent-quotes/:id/send-sms - Send quote via text message
app.post('/api/sent-quotes/:id/send-sms', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;

    if (!twilioClient) {
      return res.status(400).json({ success: false, error: 'SMS is not configured' });
    }

    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [id]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }

    const quote = quoteResult.rows[0];

    // Get customer phone — from quote or customer record
    let phone = quote.customer_phone;
    if (!phone && quote.customer_id) {
      const custResult = await pool.query('SELECT phone, mobile FROM customers WHERE id = $1', [quote.customer_id]);
      if (custResult.rows.length > 0) {
        phone = custResult.rows[0].mobile || custResult.rows[0].phone;
      }
    }

    if (!phone) {
      return res.status(400).json({ success: false, error: 'No phone number on file for this customer' });
    }

    // Format phone number
    const cleaned = phone.replace(/\D/g, '');
    const toNumber = cleaned.length === 10 ? '+1' + cleaned : '+' + cleaned;

    const signUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-quote.html?token=${quote.sign_token}`;
    const firstName = (quote.customer_name || '').split(' ')[0] || '';
    const quoteNumber = quote.quote_number || `Q-${quote.id}`;

    const smsBody = `Hi ${firstName}! This is Tim from Pappas & Co. Landscaping. Thanks for giving us the opportunity to quote your service!\n\nYou can view and accept your pricing here: ${signUrl}\n\nTo secure your spot on our route, please click "Accept" on the quote. If you have any questions while reviewing it, feel free to text me back here.\n\nWe look forward to servicing your property!`;

    const twilioMessage = await twilioClient.messages.create({
      body: smsBody,
      from: TWILIO_PHONE_NUMBER,
      to: toNumber
    });

    // Log to messages table
    await pool.query(`
      INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
      VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
    `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, toNumber, smsBody, twilioMessage.status, quote.customer_id]);

    // Update quote status to sent if still draft
    if (quote.status === 'draft') {
      await pool.query('UPDATE sent_quotes SET status = $1, sent_at = CURRENT_TIMESTAMP WHERE id = $2', ['sent', id]);
    }

    // Log event
    await logQuoteEvent(id, 'sent_sms', 'Quote sent via text to ' + phone, { phone, quote_number: quoteNumber });

    console.log(`📱 Quote ${quoteNumber} sent via SMS to ${phone}`);
    res.json({ success: true, message: 'Quote sent via text!' });
  } catch (error) {
    console.error('Error sending quote SMS:', error);
    serverError(res, error);
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
    serverError(res, error);
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
      await logQuoteEvent(quote.id, 'viewed', 'Quote viewed by customer');
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
    serverError(res, error);
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
        <p style="margin:0 0 12px;"><strong>Quote #:</strong> ${escapeHtml(quoteNumber)}</p>
        <p style="margin:0 0 12px;"><strong>Customer:</strong> ${escapeHtml(quote.customer_name)}</p>
        <p style="margin:0 0 12px;"><strong>Email:</strong> <a href="mailto:${escapeHtml(quote.customer_email)}" style="color:#2e403d;">${escapeHtml(quote.customer_email)}</a></p>
        <p style="margin:0 0 12px;"><strong>Phone:</strong> ${escapeHtml(quote.customer_phone)}</p>
        <p style="margin:0 0 12px;"><strong>Address:</strong> ${escapeHtml(quote.customer_address)}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0 0 12px;"><strong>Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
        ${quote.monthly_payment ? `<p style="margin:0 0 12px;"><strong>Monthly:</strong> $${parseFloat(quote.monthly_payment).toFixed(2)}/mo</p>` : ''}
        <p style="margin:0;"><strong>Accepted:</strong> ${new Date().toLocaleString()}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `✅ Quote #${escapeHtml(quoteNumber)} Accepted: ${escapeHtml(quote.customer_name)}`, emailTemplate(adminContent, { showSignature: false }));

    // Return success with contract URL for redirect
    const contractUrl = `/sign-contract.html?token=${req.params.token}`;
    res.json({ success: true, message: 'Quote accepted successfully', contractUrl });
  } catch (error) {
    console.error('Error signing quote:', error);
    serverError(res, error);
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
        <p style="margin:0 0 8px;"><strong>Reason:</strong> ${escapeHtml(reasonLabels[decline_reason] || decline_reason)}</p>
        ${decline_comments ? `<p style="margin:0;"><strong>Comments:</strong> ${escapeHtml(decline_comments)}</p>` : ''}
      </div>
      <div style="background:#f8fafc;border-radius:8px;padding:20px;">
        <p style="margin:0 0 8px;"><strong>Customer:</strong> ${escapeHtml(quote.customer_name)}</p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> ${escapeHtml(quote.customer_email)}</p>
        <p style="margin:0 0 8px;"><strong>Phone:</strong> ${escapeHtml(quote.customer_phone)}</p>
        <p style="margin:0 0 8px;"><strong>Address:</strong> ${escapeHtml(quote.customer_address)}</p>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:16px 0;">
        <p style="margin:0;"><strong>Quote Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
      </div>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `❌ Quote Declined: ${escapeHtml(quote.customer_name)}`, emailTemplate(adminContent, { showSignature: false }));

    // Log decline event
    await logQuoteEvent(quote.id, 'declined', 'Quote declined by customer', {
      reason: decline_reason,
      comments: decline_comments || null
    });

    res.json({ success: true, message: 'Quote declined' });
  } catch (error) {
    console.error('Error declining quote:', error);
    serverError(res, error);
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
    serverError(res, error);
  }
});

// GET /api/sent-quotes/:id/events - Full event history for a quote
app.get('/api/sent-quotes/:id/events', async (req, res) => {
  try {
    await ensureQuoteEventsTable();
    const result = await pool.query(
      'SELECT * FROM quote_events WHERE sent_quote_id = $1 ORDER BY created_at ASC',
      [req.params.id]
    );
    res.json({ success: true, events: result.rows });
  } catch (error) {
    console.error('Error fetching quote events:', error);
    res.json({ success: true, events: [] });
  }
});

// POST /api/sign/:token/request-changes - Request changes to quote (public)
app.post('/api/sign/:token/request-changes', async (req, res) => {
  try {
    const { change_type, change_details, change_request } = req.body;

    // Support both formats: {change_type, change_details} and {change_request}
    const type = change_type || 'general';
    const details = change_details || change_request;

    if (!details) {
      return res.status(400).json({ success: false, error: 'Please describe the changes you would like' });
    }

    const result = await pool.query(
      `UPDATE sent_quotes
       SET status = 'changes_requested', change_type = $1, change_details = $2, changes_requested_at = CURRENT_TIMESTAMP
       WHERE sign_token = $3 AND status NOT IN ('signed', 'contracted', 'declined')
       RETURNING *`,
      [type, details, req.params.token]
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
        <p style="margin:0 0 8px;"><strong>Type:</strong> ${escapeHtml(typeLabels[type] || type)}</p>
        <p style="margin:0;"><strong>Details:</strong></p>
        <p style="margin:8px 0 0;padding:12px;background:white;border-radius:6px;">${escapeHtml(details).replace(/\n/g, '<br>')}</p>
      </div>
      <div style="background:#f8fafc;border-radius:8px;padding:20px;">
        <p style="margin:0 0 8px;"><strong>Customer:</strong> ${escapeHtml(quote.customer_name)}</p>
        <p style="margin:0 0 8px;"><strong>Email:</strong> <a href="mailto:${escapeHtml(quote.customer_email)}" style="color:#2e403d;">${escapeHtml(quote.customer_email)}</a></p>
        <p style="margin:0 0 8px;"><strong>Phone:</strong> <a href="tel:${escapeHtml(quote.customer_phone)}" style="color:#2e403d;">${escapeHtml(quote.customer_phone)}</a></p>
        <p style="margin:0;"><strong>Original Total:</strong> $${parseFloat(quote.total).toFixed(2)}</p>
      </div>
      <p style="margin-top:20px;"><a href="${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sent-quotes.html" style="background:#f59e0b;color:white;padding:12px 24px;text-decoration:none;border-radius:6px;display:inline-block;font-weight:600;">Review Quote</a></p>
    `;
    await sendEmail(NOTIFICATION_EMAIL, `📝 Change Request: ${escapeHtml(quote.customer_name)}`, emailTemplate(adminContent, { showSignature: false }));

    // Log changes requested event
    await logQuoteEvent(quote.id, 'changes_requested', 'Customer requested changes', {
      change_type: type,
      change_details: details
    });

    res.json({ success: true, message: 'Changes requested' });
  } catch (error) {
    console.error('Error requesting changes:', error);
    serverError(res, error);
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

    // Log contract signed event
    await logQuoteEvent(id, 'contracted', 'Contract signed by ' + printed_name, {
      signer_name: printed_name,
      signer_ip: signerIp,
      signature_type: signature_type
    });

    const signedDate = new Date().toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'America/New_York' });
    const signedTime = new Date().toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' });

    let servicesText = 'See agreement for details';
    let servicesHtml = '';
    let services = [];
    try {
      services = typeof updatedQuote.services === 'string' ? JSON.parse(updatedQuote.services) : updatedQuote.services;
      if (Array.isArray(services)) {
        servicesText = services.map(s => s.name || s).join(', ');
        servicesHtml = services.map(s => `<li style="margin:6px 0;">${s.name} - $${parseFloat(s.amount).toFixed(2)}</li>`).join('');
      } else {
        services = [];
      }
    } catch (e) { services = []; }

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
      const firstName = (updatedQuote.customer_name || '').split(' ')[0] || 'there';
      const customerContent = `
        <div style="text-align:center;margin:0 0 28px;">
          <img src="${process.env.EMAIL_ASSETS_URL || process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/email-assets/heading-welcome.png" alt="Welcome to the Pappas Family" style="max-width:400px;width:auto;height:34px;" />
        </div>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Hi ${firstName},</p>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Thank you for signing your service agreement! We're excited to have you as a customer.</p>

        <p style="background:#f0f2eb;padding:16px;border-radius:8px;color:#2e403d;font-size:14px;margin:0 0 24px;">Your signed service agreement is attached to this email.</p>

        <div style="background:#f8faf6;border-radius:12px;padding:24px;margin:0 0 24px;">
          <p style="margin:0 0 16px;color:#2e403d;font-size:16px;font-weight:700;border-bottom:2px solid #c9dd80;padding-bottom:12px;">Agreement Details</p>
          <p style="margin:0 0 6px;"><span style="color:#64748b;font-size:13px;">Quote Number</span><br><span style="color:#1e293b;font-size:15px;font-weight:600;">#${quoteNumber}</span></p>
          <p style="margin:12px 0 6px;"><span style="color:#64748b;font-size:13px;">Service Address</span><br><span style="color:#1e293b;font-size:15px;">${(() => { const al = formatAddressLines(updatedQuote.customer_address); return al.line2 ? al.line1 + '<br>' + al.line2 : al.line1; })()}</span></p>
          <p style="margin:12px 0 6px;border-top:1px solid #e2e8f0;padding-top:12px;"><span style="color:#64748b;font-size:13px;">Services</span><br><span style="color:#1e293b;font-size:15px;">${servicesText}</span></p>
          <table style="width:100%;margin-top:16px;border-top:2px solid #c9dd80;border-collapse:collapse;">
            <tr><td style="padding:12px 0;color:#64748b;font-size:14px;">Total</td><td style="padding:12px 0;color:#2e403d;font-size:22px;text-align:right;font-weight:700;">$${parseFloat(updatedQuote.total).toFixed(2)}</td></tr>
            ${updatedQuote.monthly_payment ? `<tr><td style="padding:4px 0;color:#64748b;font-size:14px;">Monthly Payment</td><td style="padding:4px 0;color:#2e403d;font-size:16px;text-align:right;font-weight:600;">$${parseFloat(updatedQuote.monthly_payment).toFixed(2)}/mo</td></tr>` : ''}
          </table>
        </div>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;"><strong>What's next?</strong> If you haven't already, please add a payment method in your customer portal to complete your setup. Once that's done, we'll add you to the schedule!</p>

        <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0;">We can't wait to get started!</p>
      `;
      await sendEmail(updatedQuote.customer_email, `You're All Set! Welcome to Pappas & Co. Landscaping`, emailTemplate(customerContent), [contractAttachment], { type: 'welcome', customer_id: updatedQuote.customer_id, customer_name: updatedQuote.customer_name, quote_id: updatedQuote.id });
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

    // Sync to CopilotCRM — update estimate status to accepted + upload signed contract
    if (process.env.COPILOTCRM_USERNAME && process.env.COPILOTCRM_PASSWORD) {
      try {
        console.log(`🔄 CopilotCRM sync starting for "${updatedQuote.customer_name}" (quote ${updatedQuote.quote_number || id})`);
        // Step 1: Login to CopilotCRM
        const copilotLogin = await fetch('https://api.copilotcrm.com/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
          body: JSON.stringify({ username: process.env.COPILOTCRM_USERNAME, password: process.env.COPILOTCRM_PASSWORD })
        });
        const copilotLoginText = await copilotLogin.text();
        let copilotAuth;
        try { copilotAuth = JSON.parse(copilotLoginText); } catch (e) { throw new Error(`CopilotCRM login returned non-JSON: ${copilotLoginText.substring(0, 200)}`); }
        console.log(`🔑 CopilotCRM login status: ${copilotLogin.status}, hasToken: ${!!copilotAuth.accessToken}`);
        if (!copilotAuth.accessToken) throw new Error(`CopilotCRM login failed: ${copilotLoginText.substring(0, 200)}`);
        const copilotCookie = `copilotApiAccessToken=${copilotAuth.accessToken}`;
        const copilotHeaders = {
          'Cookie': copilotCookie,
          'Origin': 'https://secure.copilotcrm.com',
          'Referer': 'https://secure.copilotcrm.com/',
          'X-Requested-With': 'XMLHttpRequest'
        };

        // Step 2: Search for customer by name
        const customerName = updatedQuote.customer_name || '';
        const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
          method: 'POST',
          headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
          body: `query=${encodeURIComponent(customerName)}`
        });
        const searchText = await searchRes.text();
        let customers;
        try { customers = JSON.parse(searchText); } catch (e) { throw new Error(`CopilotCRM customer search returned non-JSON (status ${searchRes.status}): ${searchText.substring(0, 300)}`); }
        console.log(`🔍 CopilotCRM: Customer search for "${customerName}" returned ${Array.isArray(customers) ? customers.length : 'non-array'} results`);

        // Find matching customer
        const match = customers.find(c => c.id && String(c.id) !== '0');
        if (!match) {
          console.log(`⚠️ CopilotCRM: No customer found for "${customerName}". Search results:`, JSON.stringify(customers).substring(0, 500));
        } else {
          const copilotCustomerId = match.id;
          console.log(`🔍 CopilotCRM: Found customer ${copilotCustomerId} for "${customerName}"`);

          // Step 3: Get customer's estimates to find matching estimate number
          const estRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
            method: 'POST',
            headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `customer_id=${copilotCustomerId}`
          });
          const estText = await estRes.text();
          let estData;
          try { estData = JSON.parse(estText); } catch (e) { throw new Error(`CopilotCRM estimates returned non-JSON (status ${estRes.status}): ${estText.substring(0, 300)}`); }
          const estHtml = estData.html || '';
          console.log(`📋 CopilotCRM: Estimates response for customer ${copilotCustomerId}: status=${estRes.status}, htmlLength=${estHtml.length}`);

          // Parse estimate IDs and numbers from HTML
          const quoteNum = updatedQuote.quote_number || '';
          const paddedNum = quoteNum.replace(/^0+/, '').padStart(7, '0');

          const estimateRegex = /<tr\s+id="(\d+)"[\s\S]*?<a\s+href="\/finances\/estimates\/view\/\d+">\s*(\d+)\s*<\/a>/g;
          let estMatch;
          let copilotEstimateId = null;
          const allEstNums = [];
          while ((estMatch = estimateRegex.exec(estHtml)) !== null) {
            allEstNums.push({ id: estMatch[1], num: estMatch[2] });
            if (estMatch[2] === paddedNum || estMatch[2] === quoteNum) {
              copilotEstimateId = estMatch[1];
              break;
            }
          }
          console.log(`📋 CopilotCRM: Found estimates: ${JSON.stringify(allEstNums)}. Looking for quoteNum="${quoteNum}" / padded="${paddedNum}"`);

          if (!copilotEstimateId) {
            console.log(`⚠️ CopilotCRM: No estimate matching "${quoteNum}" (padded: ${paddedNum}) found for customer ${copilotCustomerId}`);
          } else {
            console.log(`🔍 CopilotCRM: Found estimate ${copilotEstimateId} for quote ${quoteNum}`);

            // Step 4: Accept the estimate
            const acceptRes = await fetch('https://secure.copilotcrm.com/finances/estimates/accept', {
              method: 'POST',
              headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
              body: `id=${copilotEstimateId}&key=`
            });
            if (acceptRes.ok) {
              console.log(`✅ CopilotCRM: Estimate ${copilotEstimateId} marked as accepted`);
            } else {
              console.error(`CopilotCRM: Accept failed with status ${acceptRes.status}`);
            }

            // Step 5: Upload signed contract PDF if available
            if (pdfBytes && pdfBytes.length > 0) {
              try {
                // Get signed upload URL from CopilotCRM (S3)
                const signUrlRes = await fetch('https://secure.copilotcrm.com/getSignedUploadUrl', {
                  method: 'POST',
                  headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
                  body: JSON.stringify({ contentType: 'application/pdf', size: pdfBytes.length })
                });
                const signUrlData = await signUrlRes.json();

                if (signUrlData.data && signUrlData.data.uploadUrl) {
                  // Upload PDF to S3
                  await fetch(signUrlData.data.uploadUrl, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/pdf' },
                    body: Buffer.from(pdfBytes)
                  });

                  // Link uploaded file to the estimate
                  const uploadRes = await fetch('https://secure.copilotcrm.com/finances/estimates/uploadImage', {
                    method: 'POST',
                    headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      estimateId: String(copilotEstimateId),
                      tempFileName: signUrlData.data.key,
                      contentType: 'application/pdf'
                    })
                  });
                  if (uploadRes.ok) {
                    console.log(`✅ CopilotCRM: Signed contract uploaded to estimate ${copilotEstimateId}`);
                  }
                }
              } catch (uploadErr) {
                console.error('CopilotCRM: Contract upload failed:', uploadErr.message);
              }
            }

            // Step 6: Send customer portal invite email
            try {
              const portalUrl = 'https://secure.copilotcrm.com/client/forget?co=5261';
              const customerFirstName = (updatedQuote.customer_name || '').split(' ')[0] || 'there';
              const portalEmailContent = `
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Hi ${customerFirstName},</p>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Welcome to Pappas & Co. Landscaping! Your service agreement has been signed and we're excited to get started.</p>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">To keep things running smoothly, <strong>please add a card on file</strong> to your client portal. Your card will only be charged when invoices are due — no surprise charges.</p>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Your portal also gives you access to:</p>
                <table width="100%" cellpadding="0" cellspacing="0" style="margin:16px 0;">
                  <tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">💳</td><td><strong style="color:#1e293b;">Card on File</strong><br><span style="color:#64748b;font-size:13px;">Add a payment method for seamless billing</span></td></tr></table></td></tr>
                  <tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">📄</td><td><strong style="color:#1e293b;">Quotes & Invoices</strong><br><span style="color:#64748b;font-size:13px;">View and pay invoices online anytime</span></td></tr></table></td></tr>
                  <tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">📅</td><td><strong style="color:#1e293b;">Service Schedule</strong><br><span style="color:#64748b;font-size:13px;">View upcoming visits and service history</span></td></tr></table></td></tr>
                  <tr><td style="padding:12px 0;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">💬</td><td><strong style="color:#1e293b;">Direct Messaging</strong><br><span style="color:#64748b;font-size:13px;">Send questions or requests to our team</span></td></tr></table></td></tr>
                </table>
                <p style="font-size:16px;color:#1e293b;line-height:1.7;">Click below to create your password and add your card.</p>
                <div style="text-align:center;margin:32px 0;">
                  <a href="${portalUrl}" style="display:inline-block;padding:14px 40px;background:#2e403d;color:#ffffff;text-decoration:none;border-radius:8px;font-size:16px;font-weight:600;">Set Up My Portal</a>
                </div>
              `;
              const portalEmailHtml = emailTemplate(portalEmailContent);
              const sendMailBody = new URLSearchParams({
                co_id: '5261',
                'to_customer[]': String(copilotCustomerId),
                type: 'email',
                subject: 'Get Started: Complete Your Client Portal Registration',
                content: portalEmailHtml
              });
              const sendMailRes = await fetch('https://secure.copilotcrm.com/emails/sendMail', {
                method: 'POST',
                headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
                body: sendMailBody.toString()
              });
              if (sendMailRes.ok) {
                console.log(`✅ CopilotCRM: Portal invite sent to customer ${copilotCustomerId}`);
              } else {
                console.error(`CopilotCRM: Portal invite failed with status ${sendMailRes.status}`);
              }
            } catch (portalErr) {
              console.error('CopilotCRM: Portal invite failed:', portalErr.message);
            }
          }
        }
      } catch (copilotErr) {
        console.error('❌ CopilotCRM sync failed:', copilotErr.message);
        console.error('CopilotCRM stack:', copilotErr.stack);
      }
    } else {
      console.log('⚠️ CopilotCRM sync skipped — COPILOTCRM_USERNAME or COPILOTCRM_PASSWORD not set');
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
    serverError(res, error);
  }
});

// POST /api/copilotcrm/backfill-contract - Manually trigger CopilotCRM sync for a signed quote
app.post('/api/copilotcrm/backfill-contract', authenticateToken, async (req, res) => {
  try {
    const { customer_name } = req.body;
    if (!customer_name) return res.status(400).json({ success: false, error: 'customer_name required' });
    if (!process.env.COPILOTCRM_USERNAME || !process.env.COPILOTCRM_PASSWORD) {
      return res.status(400).json({ success: false, error: 'CopilotCRM credentials not configured' });
    }

    // Find the signed quote
    const quoteResult = await pool.query(
      `SELECT * FROM sent_quotes WHERE LOWER(customer_name) = LOWER($1) AND contract_signed_at IS NOT NULL ORDER BY contract_signed_at DESC LIMIT 1`,
      [customer_name.trim()]
    );
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: `No signed contract found for "${customer_name}"` });
    }
    const quote = quoteResult.rows[0];
    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    console.log(`🔄 Backfilling CopilotCRM sync for ${customer_name}, quote ${quoteNumber}`);

    // Generate contract PDF
    let pdfBytes = null;
    try {
      const signedDate = new Date(quote.contract_signed_at).toLocaleDateString();
      pdfBytes = await generateContractPDF(quote, quote.contract_signature_data, quote.contract_signer_name, signedDate);
    } catch (pdfErr) {
      console.error('PDF generation failed:', pdfErr.message);
    }

    // Login to CopilotCRM
    const copilotLogin = await fetch('https://api.copilotcrm.com/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
      body: JSON.stringify({ username: process.env.COPILOTCRM_USERNAME, password: process.env.COPILOTCRM_PASSWORD })
    });
    const copilotAuth = await copilotLogin.json();
    if (!copilotAuth.accessToken) return res.status(500).json({ success: false, error: 'CopilotCRM login failed' });
    const copilotHeaders = {
      'Cookie': `copilotApiAccessToken=${copilotAuth.accessToken}`,
      'Origin': 'https://secure.copilotcrm.com',
      'Referer': 'https://secure.copilotcrm.com/',
      'X-Requested-With': 'XMLHttpRequest'
    };

    // Search for customer
    const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
      method: 'POST',
      headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `query=${encodeURIComponent(customer_name)}`
    });
    const customers = await searchRes.json();
    const match = customers.find(c => c.id && String(c.id) !== '0');
    if (!match) return res.status(404).json({ success: false, error: `No customer found in CopilotCRM for "${customer_name}"` });

    const copilotCustomerId = match.id;
    const log = [`Found customer ${copilotCustomerId}`];

    // Get estimates
    const estRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
      method: 'POST',
      headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `customer_id=${copilotCustomerId}`
    });
    const estData = await estRes.json();
    const estHtml = estData.html || '';

    const quoteNum = quote.quote_number || '';
    const paddedNum = quoteNum.replace(/^0+/, '').padStart(7, '0');
    const estimateRegex = /<tr\s+id="(\d+)"[\s\S]*?<a\s+href="\/finances\/estimates\/view\/\d+">\s*(\d+)\s*<\/a>/g;
    let estMatch;
    let copilotEstimateId = null;
    while ((estMatch = estimateRegex.exec(estHtml)) !== null) {
      if (estMatch[2] === paddedNum || estMatch[2] === quoteNum) {
        copilotEstimateId = estMatch[1];
        break;
      }
    }

    if (!copilotEstimateId) {
      return res.status(404).json({ success: false, error: `No estimate matching "${quoteNum}" found in CopilotCRM for customer ${copilotCustomerId}` });
    }
    log.push(`Found estimate ${copilotEstimateId} for quote ${quoteNum}`);

    // Accept estimate
    const acceptRes = await fetch('https://secure.copilotcrm.com/finances/estimates/accept', {
      method: 'POST',
      headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `id=${copilotEstimateId}&key=`
    });
    if (acceptRes.ok) {
      log.push(`Estimate ${copilotEstimateId} marked as accepted`);
    } else {
      log.push(`Accept failed: ${acceptRes.status}`);
    }

    // Upload PDF
    if (pdfBytes && pdfBytes.length > 0) {
      try {
        const signUrlRes = await fetch('https://secure.copilotcrm.com/getSignedUploadUrl', {
          method: 'POST',
          headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
          body: JSON.stringify({ contentType: 'application/pdf', size: pdfBytes.length })
        });
        const signUrlData = await signUrlRes.json();
        if (signUrlData.data && signUrlData.data.uploadUrl) {
          await fetch(signUrlData.data.uploadUrl, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/pdf' },
            body: Buffer.from(pdfBytes)
          });
          const uploadRes = await fetch('https://secure.copilotcrm.com/finances/estimates/uploadImage', {
            method: 'POST',
            headers: { ...copilotHeaders, 'Content-Type': 'application/json' },
            body: JSON.stringify({ estimateId: String(copilotEstimateId), tempFileName: signUrlData.data.key, contentType: 'application/pdf' })
          });
          if (uploadRes.ok) log.push('Signed contract PDF uploaded');
        }
      } catch (uploadErr) {
        log.push(`PDF upload failed: ${uploadErr.message}`);
      }
    } else {
      log.push('No PDF available to upload');
    }

    console.log(`✅ CopilotCRM backfill complete for ${customer_name}:`, log.join(' → '));
    res.json({ success: true, log });
  } catch (error) {
    serverError(res, error, 'CopilotCRM backfill error');
  }
});

// POST /api/copilotcrm/estimate-accepted - CopilotCRM estimate accepted → send YardDesk contract
// Required: customer_name, estimate_number, estimate_amount
// Optional: email, phone, address, services (if not provided, email/phone/address looked up from CopilotCRM)
// If services not provided, creates a single line item "Services per Estimate #XXXX"
app.post('/api/copilotcrm/estimate-accepted', authenticateToken, async (req, res) => {
  try {
    let { customer_name, phone, address, email, estimate_number, estimate_amount, services } = req.body;
    if (!customer_name || !estimate_number || !estimate_amount) {
      return res.status(400).json({ success: false, error: 'Missing required fields: customer_name, estimate_number, estimate_amount' });
    }

    // Dedupe check — don't create duplicate contracts for the same estimate
    const existing = await pool.query(
      `SELECT id, sign_token FROM sent_quotes WHERE quote_number = $1 AND status NOT IN ('declined') LIMIT 1`,
      [estimate_number]
    );
    if (existing.rows.length > 0) {
      const ex = existing.rows[0];
      const contractUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-contract.html?token=${ex.sign_token}`;
      return res.json({ success: true, message: 'Contract already exists for this estimate', quote_id: ex.id, contract_url: contractUrl });
    }

    // If no email, look up customer in CopilotCRM
    if (!email) {
      if (!process.env.COPILOTCRM_USERNAME || !process.env.COPILOTCRM_PASSWORD) {
        return res.status(400).json({ success: false, error: 'Email not provided and CopilotCRM credentials not configured' });
      }
      const copilotLogin = await fetch('https://api.copilotcrm.com/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
        body: JSON.stringify({ username: process.env.COPILOTCRM_USERNAME, password: process.env.COPILOTCRM_PASSWORD })
      });
      const copilotAuth = await copilotLogin.json();
      if (!copilotAuth.accessToken) {
        return res.status(500).json({ success: false, error: 'CopilotCRM login failed' });
      }
      const copilotHeaders = {
        'Cookie': `copilotApiAccessToken=${copilotAuth.accessToken}`,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest'
      };
      const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
        method: 'POST',
        headers: { ...copilotHeaders, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `query=${encodeURIComponent(customer_name)}`
      });
      const crmCustomers = await searchRes.json().catch(() => null);
      const crmMatch = Array.isArray(crmCustomers) ? crmCustomers.find(c => c.id && String(c.id) !== '0') : null;
      if (!crmMatch) {
        return res.status(404).json({ success: false, error: `Customer "${customer_name}" not found in CopilotCRM. Provide email manually.` });
      }
      if (crmMatch.email) {
        email = crmMatch.email;
      }
      if (!phone && crmMatch.phone) phone = crmMatch.phone;
      if (!address && crmMatch.address) address = crmMatch.address;
      console.log(`📧 CopilotCRM lookup: customer=${crmMatch.id}, email=${email || 'none'}, phone=${phone || 'n/a'}`);
    }

    // Fallback: look up email from our own customers table by name or phone
    if (!email) {
      const localLookup = await pool.query(
        `SELECT email FROM customers WHERE email IS NOT NULL AND email != '' AND (
          LOWER(name) = LOWER($1)
          OR LOWER(CONCAT(first_name, ' ', last_name)) = LOWER($1)
          ${phone ? `OR phone = $2 OR mobile = $2` : ''}
        ) LIMIT 1`,
        phone ? [customer_name, phone.replace(/\D/g, '').replace(/^1/, '')] : [customer_name]
      );
      if (localLookup.rows.length > 0) {
        email = localLookup.rows[0].email;
        console.log(`📧 Local DB lookup: Found email ${email} for "${customer_name}"`);
      }
    }

    // If no services provided, create a single line item from the estimate
    if (!services || services.length === 0) {
      services = [{ name: `Services per Estimate #${estimate_number}`, price: estimate_amount }];
    }

    // Validation
    if (!email) return res.status(400).json({ success: false, error: 'No customer email available. Provide email manually.' });

    // Find or create customer
    let customer_id = null;
    const existingCustomer = await pool.query('SELECT id FROM customers WHERE email = $1', [email]);
    if (existingCustomer.rows.length > 0) {
      customer_id = existingCustomer.rows[0].id;
    } else {
      const newCustNum = await nextCustomerNumber();
      const newCustomer = await pool.query(
        `INSERT INTO customers (customer_number, name, email, phone, street, created_at)
         VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) RETURNING id`,
        [newCustNum, customer_name, email, phone || null, address || null]
      );
      customer_id = newCustomer.rows[0].id;
      console.log('Created new customer from CopilotCRM estimate:', customer_id);

      if (process.env.ZAPIER_CUSTOMER_WEBHOOK) {
        try {
          await fetch(process.env.ZAPIER_CUSTOMER_WEBHOOK, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ customer_id, name: customer_name, email, phone, address, source: 'copilotcrm_estimate' })
          });
        } catch (e) { console.error('Zapier webhook failed:', e); }
      }
    }

    // Generate token and map services
    const sign_token = generateToken();
    const serviceItems = services.map(s => ({ name: s.name, amount: s.price || s.amount, price: s.price || s.amount }));

    // Create sent_quotes record — status='sent' since estimate is already accepted
    const result = await pool.query(
      `INSERT INTO sent_quotes (
        customer_id, customer_name, customer_email, customer_phone, customer_address,
        quote_type, services, subtotal, tax_rate, tax_amount, total,
        status, sign_token, notes, quote_number, created_at, sent_at
      ) VALUES ($1, $2, $3, $4, $5, 'regular', $6, $7, 0, 0, $7, 'sent', $8, $9, $10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *`,
      [
        customer_id, customer_name, email, phone || null, address || null,
        JSON.stringify(serviceItems), estimate_amount,
        sign_token, 'Auto-created from CopilotCRM estimate #' + estimate_number, estimate_number
      ]
    );
    const newQuote = result.rows[0];

    await logQuoteEvent(newQuote.id, 'created', 'Contract created from CopilotCRM estimate #' + estimate_number, {
      source: 'copilotcrm', estimate_number, total: estimate_amount, services_count: services.length
    });

    // Build and send contract email
    const contractUrl = `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/sign-contract.html?token=${sign_token}`;
    const firstName = escapeHtml((customer_name || '').split(' ')[0] || 'there');
    const assetsUrl = process.env.EMAIL_ASSETS_URL || process.env.BASE_URL || 'https://app.pappaslandscaping.com';

    const emailContent = `
      <div style="text-align:center;margin:0 0 28px;">
        <h2 style="font-family:'Open Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#2e403d;font-size:24px;font-weight:600;margin:0;">Your Service Agreement is Ready</h2>
      </div>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Hi ${firstName},</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">Thank you for accepting your estimate with Pappas & Co. Landscaping! Before we get started, please take a moment to review and sign your service agreement.</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">This agreement covers the scope of work, terms, and pricing for your accepted estimate.</p>
      <div style="text-align:center;margin:28px 0 20px;">
        <a href="${contractUrl}" style="background:#c9dd80;color:#2e403d;padding:16px 52px;text-decoration:none;border-radius:50px;font-weight:700;font-size:15px;display:inline-block;letter-spacing:0.3px;">Review & Sign Agreement \u{2192}</a>
      </div>
      <p style="font-size:14px;color:#94a3b8;text-align:center;margin:0 0 24px;">Or just reply to this email with any questions</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;">If you have any questions, feel free to call or text us at <strong>440-886-7318</strong>. We're always happy to help!</p>
      <p style="font-size:15px;color:#4a5568;line-height:1.8;margin:0;">We look forward to working with you!</p>
    `;

    await sendEmail(
      email,
      'Your Service Agreement from ' + COMPANY_NAME,
      emailTemplate(emailContent),
      null,
      { type: 'contract', customer_id, customer_name, quote_id: newQuote.id }
    );

    await logQuoteEvent(newQuote.id, 'sent', 'Contract sent to ' + email, { email, source: 'copilotcrm' });

    console.log(`✅ CopilotCRM estimate-accepted: Contract sent to ${email} for estimate #${estimate_number}`);
    res.json({ success: true, message: 'Contract sent to ' + email, quote_id: newQuote.id, contract_url: contractUrl });
  } catch (error) {
    serverError(res, error, 'Error creating contract from CopilotCRM estimate');
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
    serverError(res, error);
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
    serverError(res, error);
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

    const quoteNumber = quote.quote_number || 'Q-' + quote.id;
    const pdfResult = await generateQuotePDF(quote);

    if (!pdfResult || !pdfResult.bytes) {
      return res.status(500).json({ success: false, error: 'PDF generation failed' });
    }

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="Quote-${quoteNumber}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Error downloading quote:', error);
    serverError(res, error);
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
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// TWILIOCONNECT APP API
// ═══════════════════════════════════════════════════════════

// Login for TwilioConnect app
app.post('/api/app/login', async (req, res) => {
  const { email, password } = req.body;
  const APP_PASSWORD = process.env.APP_PASSWORD;
  if (!APP_PASSWORD) return res.status(503).json({ message: 'App login not configured' });
  const authorizedUsers = [
    { email: 'hello@pappaslandscaping.com', name: 'Theresa Pappas', phone: '+12163150451' },
    { email: 'montague.theresa@gmail.com', name: 'Theresa Pappas', phone: '+12163150451' },
    { email: 'tim@pappaslandscaping.com', name: 'Tim Pappas', phone: '+12169057395' },
  ];
  const user = authorizedUsers.find(u => u.email.toLowerCase() === email.toLowerCase()) && password === APP_PASSWORD
    ? authorizedUsers.find(u => u.email.toLowerCase() === email.toLowerCase())
    : null;
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
      url: `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/api/app/calls/connect?to=${encodeURIComponent(to)}&from=${encodeURIComponent(callFromNumber)}`,
      to: userPhone,
      from: callFromNumber,
      statusCallback: `${process.env.BASE_URL || 'https://app.pappaslandscaping.com'}/api/app/calls/status-callback`,
      statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
    });
    console.log(`📞 Outbound call initiated: ${call.sid} to ${to} via ${userPhone} from ${callFromNumber}`);
    res.json({ success: true, callSid: call.sid, contactName });
  } catch (error) {
    console.error('Outbound call error:', error);
    serverError(res, error, 'Failed to initiate call');
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
        url: (process.env.BASE_URL || 'https://app.pappaslandscaping.com') + '/api/app/calls/hold-music',
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

// ============================================================
// Voice SDK — Access Token & Client Routing
// ============================================================

// One-time setup: creates a TwiML App + API Key for Voice SDK
// Call this once, then store the returned values as env vars
app.post('/api/app/voice/setup', authenticateToken, async (req, res) => {
  try {
    // Create TwiML App
    const twimlApp = await twilioClient.applications.create({
      friendlyName: 'TwilioConnect Mobile',
      voiceUrl: (process.env.BASE_URL || 'https://app.pappaslandscaping.com') + '/api/app/voice/connect',
      voiceMethod: 'POST',
    });

    // Create API Key
    const apiKey = await twilioClient.newKeys.create({ friendlyName: 'TwilioConnect Voice' });

    console.log('=== VOICE SDK SETUP COMPLETE — check response for env vars ===');

    res.json({
      success: true,
      message: 'Save these as environment variables in Railway',
      twimlAppSid: twimlApp.sid,
      apiKeySid: apiKey.sid,
      apiKeySecret: apiKey.secret,
    });
  } catch (error) {
    console.error('Voice setup error:', error);
    serverError(res, error, 'Setup failed');
  }
});

// Generate Twilio Access Token for Voice SDK
app.get('/api/app/voice/token', authenticateToken, (req, res) => {
  try {
    const apiKeySid = process.env.TWILIO_API_KEY_SID;
    const apiKeySecret = process.env.TWILIO_API_KEY_SECRET;
    const twimlAppSid = process.env.TWILIO_TWIML_APP_SID;

    if (!apiKeySid || !apiKeySecret || !twimlAppSid) {
      return res.status(503).json({ message: 'Voice not configured. Run /api/app/voice/setup first.' });
    }

    const AccessToken = twilio.jwt.AccessToken;
    const VoiceGrant = AccessToken.VoiceGrant;

    const identity = req.user?.email || req.user?.id || 'pappas-user';

    const pushCredentialSid = process.env.TWILIO_PUSH_CREDENTIAL_SID || 'CR0cf89f77173745be7d6de6eac56cad7d';

    const voiceGrant = new VoiceGrant({
      outgoingApplicationSid: twimlAppSid,
      incomingAllow: true,
      pushCredentialSid: pushCredentialSid,
    });

    const token = new AccessToken(TWILIO_ACCOUNT_SID, apiKeySid, apiKeySecret, {
      identity: identity,
      ttl: 3600,
    });
    token.addGrant(voiceGrant);

    console.log(`🎙️ Voice token issued for identity: ${identity} with push credential: ${pushCredentialSid}`);
    res.json({ token: token.toJwt(), identity });
  } catch (error) {
    console.error('Voice token error:', error);
    res.status(500).json({ message: 'Failed to generate voice token' });
  }
});

// Debug endpoint for voice SDK registration status
app.post('/api/app/voice/debug', (req, res) => {
  console.log('📱 VOICE SDK DEBUG:', JSON.stringify(req.body));
  res.json({ ok: true });
});

// TwiML for incoming calls routed to the app Client
app.all('/api/app/voice/incoming', (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  const from = req.body.From || req.query.From || 'Unknown';
  const baseUrl = process.env.BASE_URL || 'https://pappas-quote-backend-production.up.railway.app';

  twiml.say({ voice: 'alice' }, 'Connecting you now.');
  const dial = twiml.dial({ callerId: from, timeout: 30, action: `${baseUrl}/api/app/voice/dial-status` });
  // Ring all registered app users — identity matches the email used in voice token
  dial.client('hello@pappaslandscaping.com');
  dial.client('montague.theresa@gmail.com');
  dial.client('tim@pappaslandscaping.com');

  res.type('text/xml');
  res.send(twiml.toString());
});

// Called after dial attempt — if nobody answered, record a voicemail
app.all('/api/app/voice/dial-status', async (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  const dialStatus = req.body.DialCallStatus || req.query.DialCallStatus || '';
  const from = req.body.From || req.query.From || 'Unknown';
  const baseUrl = process.env.BASE_URL || 'https://pappas-quote-backend-production.up.railway.app';

  if (dialStatus === 'completed' || dialStatus === 'answered') {
    twiml.hangup();
  } else {
    // No answer — record voicemail
    twiml.say({ voice: 'alice' }, 'No one is available right now. Please leave a message after the beep.');
    twiml.record({
      maxLength: 120,
      transcribe: true,
      transcribeCallback: `${baseUrl}/api/app/voice/transcription`,
      recordingStatusCallback: `${baseUrl}/api/app/voice/recording-complete`,
      recordingStatusCallbackEvent: ['completed'],
    });
    twiml.say({ voice: 'alice' }, 'Thank you. Goodbye.');
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

// Called when voicemail recording is complete — send push notification
app.all('/api/app/voice/recording-complete', async (req, res) => {
  const from = req.body.From || req.query.From || 'Unknown';
  const recordingUrl = req.body.RecordingUrl || '';
  const duration = parseInt(req.body.RecordingDuration || '0', 10);

  console.log(`🎙️ Voicemail recording complete from ${from} (${duration}s)`);

  // Look up customer name
  const cleanedPhone = from.replace(/\D/g, '').slice(-10);
  let contactName = null;
  try {
    const custResult = await pool.query(`SELECT name FROM customers WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 LIMIT 1`, [`%${cleanedPhone}`]);
    contactName = custResult.rows[0]?.name || null;
  } catch (e) {}

  // Send push notification
  await sendPushToAllDevices(
    `🎙️ ${contactName || from}`,
    `New voicemail (${duration}s)`,
    { type: 'voicemail' }
  ).catch(err => console.error('VM recording push error:', err));

  // Send email notification
  const vmDisplayName = contactName ? escapeHtml(contactName) : escapeHtml(from);
  const vmTimestamp = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', dateStyle: 'medium', timeStyle: 'short' });
  sendEmail(NOTIFICATION_EMAIL, `🎙️ New voicemail from ${contactName || from}`, emailTemplate(`
    <h2 style="color:#1e293b;margin:0 0 16px;">New Voicemail</h2>
    <table style="width:100%;border-collapse:collapse;">
      <tr><td style="padding:8px 0;color:#64748b;width:80px;">From</td><td style="padding:8px 0;color:#1e293b;font-weight:500;">${vmDisplayName}</td></tr>
      <tr><td style="padding:8px 0;color:#64748b;">Phone</td><td style="padding:8px 0;color:#1e293b;">${escapeHtml(from)}</td></tr>
      <tr><td style="padding:8px 0;color:#64748b;">Duration</td><td style="padding:8px 0;color:#1e293b;">${Math.floor(duration / 60)}m ${duration % 60}s</td></tr>
      <tr><td style="padding:8px 0;color:#64748b;">Time</td><td style="padding:8px 0;color:#1e293b;">${vmTimestamp}</td></tr>
    </table>
    ${recordingUrl ? `<p style="margin-top:16px;"><a href="${escapeHtml(recordingUrl)}" style="color:#2e403d;font-weight:500;">Listen to Recording</a></p>` : ''}
  `, { showSignature: false })).catch(err => console.error('VM recording email error:', err));

  res.sendStatus(200);
});

// TwiML for outbound calls from the app Client
app.all('/api/app/voice/connect', (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  let to = (req.body.To || req.query.To || '').trim();
  if (to && !to.startsWith('+')) to = '+' + to;

  console.log('📞 Voice connect TwiML for:', to);

  if (to) {
    const dial = twiml.dial({ callerId: TWILIO_PHONE_NUMBER });
    dial.number(to);
  } else {
    twiml.say({ voice: 'alice' }, 'No destination number provided.');
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

// Recent calls
app.get('/api/app/calls/recent', authenticateToken, async (req, res) => {
  const limit = parseInt(req.query.limit) || 5;
  try {
    // Fetch extra to account for client: calls that get filtered out
    const calls = await twilioClient.calls.list({ limit: Math.min(limit * 3, 200) });
    const enrichedCalls = (await Promise.all(calls.map(async (call) => {
      const phoneNumber = call.direction === 'inbound' ? call.from : call.to;
      // Skip calls to/from client: identities (IVR app forwarding legs)
      if (phoneNumber.startsWith('client:') || call.from.startsWith('client:') || call.to.startsWith('client:')) return null;
      const cleanedPhone = phoneNumber.replace(/\D/g, '').slice(-10);
      let contactName = null;
     const customerResult = await pool.query(`SELECT name FROM customers WHERE REGEXP_REPLACE(COALESCE(mobile, ''), '[^0-9]', '', 'g') LIKE $1 OR REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE $1 LIMIT 1`, [`%${cleanedPhone}`]);
if (customerResult.rows.length > 0) contactName = customerResult.rows[0].name;
      const twilioNumber = call.direction === 'inbound' ? call.to : call.from;
      return { id: call.sid, phoneNumber, twilioNumber, direction: call.direction, status: call.status, duration: parseInt(call.duration) || 0, timestamp: call.startTime, contactName };
    }))).filter(Boolean).slice(0, limit);
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
  if (!pushToken) {
    return res.status(400).json({ success: false, error: 'pushToken is required' });
  }
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

async function createCallsTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS calls (
        id SERIAL PRIMARY KEY,
        twilio_sid VARCHAR(100) UNIQUE,
        direction VARCHAR(10) DEFAULT 'inbound',
        from_number VARCHAR(50) NOT NULL,
        to_number VARCHAR(50) NOT NULL,
        status VARCHAR(50) DEFAULT 'completed',
        duration INTEGER,
        option_selected VARCHAR(100),
        recording_url TEXT,
        transcription TEXT,
        customer_id INTEGER,
        read BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_calls_from ON calls(from_number)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_calls_created ON calls(created_at DESC)`);
    console.log('✅ Calls table ready');
  } catch (err) {
    console.log('ℹ️ Calls table setup:', err.message);
  }
}
createCallsTable();

// Send Expo Push Notification
async function sendPushNotification(expoPushToken, title, body, data = {}, badge = undefined) {
  try {
    const payload = { to: expoPushToken, sound: 'default', title, body, data, ...(badge != null && { badge }) };
    console.log('📲 Sending push:', JSON.stringify({ to: expoPushToken.substring(0, 30) + '...', title, body }));
    const response = await fetch('https://exp.host/--/api/v2/push/send', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...(process.env.EXPO_ACCESS_TOKEN && { 'Authorization': `Bearer ${process.env.EXPO_ACCESS_TOKEN}` }),
      },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    console.log('📲 Push response:', JSON.stringify(result));
    if (result.data?.status === 'error') {
      console.error('📲 Push failed:', result.data.message, '| Details:', result.data.details);
    }
  } catch (error) {
    console.error('Push error:', error.message);
  }
}

// Send push to all registered devices
async function sendPushToAllDevices(title, body, data = {}) {
  try {
    const devices = await pool.query('SELECT push_token, email, platform FROM app_devices WHERE push_token IS NOT NULL');
    console.log(`📲 Sending push to ${devices.rows.length} device(s)`);
    if (devices.rows.length === 0) {
      console.log('📲 No devices registered for push notifications');
    }

    // Compute badge count: unread inbound messages + active voicemails
    let badge = 0;
    try {
      const unreadResult = await pool.query(`SELECT COUNT(*) FROM messages WHERE direction = 'inbound' AND read = false`);
      badge = parseInt(unreadResult.rows[0]?.count || '0', 10);
    } catch (e) {
      console.error('Badge count query error:', e.message);
    }

    for (const device of devices.rows) {
      if (device.push_token) {
        console.log(`📲 → ${device.email} (${device.platform}) badge=${badge}`);
        await sendPushNotification(device.push_token, title, body, data, badge);
      }
    }
  } catch (error) {
    console.error('Push to all devices error:', error.message);
  }
}

// Debug: Check registered devices (admin only)
app.get('/api/app/devices', authenticateToken, async (req, res) => {
  try {
    const devices = await pool.query('SELECT id, email, platform, push_token, updated_at FROM app_devices ORDER BY updated_at DESC');
    res.json({ devices: devices.rows.map(d => ({ ...d, push_token: d.push_token ? d.push_token.substring(0, 30) + '...' : null })) });
  } catch (error) {
    res.json({ devices: [], error: error.message });
  }
});

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
    await sendPushToAllDevices(`💬 ${customerName}`, Body?.substring(0, 100) || 'New message', { type: 'sms', phoneNumber: cleanedPhone, contactName: customerName });

    // Send email notification (fire-and-forget)
    const smsDisplayName = customerName !== 'Unknown' ? escapeHtml(customerName) : From;
    const smsTimestamp = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', dateStyle: 'medium', timeStyle: 'short' });
    sendEmail(NOTIFICATION_EMAIL, `💬 New text from ${customerName !== 'Unknown' ? customerName : From}`, emailTemplate(`
      <h2 style="color:#1e293b;margin:0 0 16px;">New Text Message</h2>
      <table style="width:100%;border-collapse:collapse;">
        <tr><td style="padding:8px 0;color:#64748b;width:80px;">From</td><td style="padding:8px 0;color:#1e293b;font-weight:500;">${smsDisplayName}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b;">Phone</td><td style="padding:8px 0;color:#1e293b;">${escapeHtml(From)}</td></tr>
        <tr><td style="padding:8px 0;color:#64748b;">Time</td><td style="padding:8px 0;color:#1e293b;">${smsTimestamp}</td></tr>
      </table>
      <div style="margin-top:20px;padding:16px;background:#f8fafc;border-radius:8px;border-left:4px solid #2e403d;">
        <p style="margin:0;color:#1e293b;line-height:1.6;">${escapeHtml(Body || 'No message content')}</p>
      </div>
    `, { showSignature: false })).catch(err => console.error('SMS notification email error:', err));

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

// AI reply suggestion - for app
app.post('/api/app/ai/reply', authenticateToken, async (req, res) => {
  if (!anthropicClient) {
    return res.status(503).json({ success: false, error: 'AI service not configured' });
  }
  const { messages, contactName, refinements } = req.body;
  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ success: false, error: 'messages array is required' });
  }
  try {
    const conversationContext = messages.slice(-15).map(m =>
      `${m.direction === 'outbound' ? 'Tim' : (contactName || 'Customer')}: ${m.body}`
    ).join('\n');

    const systemPrompt = `You are Tim from Pappas & Co. Landscaping in Cleveland, OH. You're drafting a text message reply to a customer conversation.

Customer name: ${contactName || 'Unknown'}

Recent conversation:
${conversationContext}

Write a short, friendly, professional text message reply as Tim. Keep it under 300 characters. Be helpful and personable — this is a small local business. Don't use emojis excessively. Just return the message text, nothing else.`;

    const apiMessages = [{ role: 'user', content: systemPrompt }];
    if (refinements && Array.isArray(refinements)) {
      for (const r of refinements) {
        apiMessages.push({ role: r.role, content: r.content });
      }
    }

    const message = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 256,
      messages: apiMessages,
    });
    const suggestion = message.content[0].text.trim();
    res.json({ success: true, suggestion });
  } catch (error) {
    console.error('AI reply error:', error);
    serverError(res, error, 'AI reply generation failed');
  }
});

// AI voicemail summary - for app
app.post('/api/app/ai/voicemail-summary', authenticateToken, async (req, res) => {
  if (!anthropicClient) {
    return res.status(503).json({ success: false, error: 'AI service not configured' });
  }
  const { transcription, contactName } = req.body;
  if (!transcription) {
    return res.status(400).json({ success: false, error: 'transcription is required' });
  }
  try {
    const message = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 100,
      messages: [{ role: 'user', content: `Summarize this voicemail in one short sentence (under 80 characters). Focus on what the caller wants or needs. No quotes, no "The caller..." prefix — just state what they want directly.\n\nCaller: ${contactName || 'Unknown'}\nTranscription: ${transcription}` }],
    });
    const summary = message.content[0].text.trim();
    res.json({ success: true, summary });
  } catch (error) {
    console.error('AI voicemail summary error:', error);
    serverError(res, error, 'AI voicemail summary failed');
  }
});

// AI text-from-voicemail draft - for app
app.post('/api/app/ai/text-from-voicemail', authenticateToken, async (req, res) => {
  if (!anthropicClient) {
    return res.status(503).json({ success: false, error: 'AI service not configured' });
  }
  const { transcription, contactName, refinements } = req.body;
  if (!transcription) {
    return res.status(400).json({ success: false, error: 'transcription is required' });
  }
  try {
    const systemPrompt = `You are Tim from Pappas & Co. Landscaping in Cleveland, OH. A customer left a voicemail and you need to text them back.\n\nCustomer name: ${contactName || 'Unknown'}\nVoicemail transcription: ${transcription}\n\nWrite a short, friendly, professional text message reply. Acknowledge what they said in the voicemail and address their needs. Keep it under 300 characters. Be helpful and personable. Just return the message text, nothing else.`;

    const apiMessages = [{ role: 'user', content: systemPrompt }];
    if (refinements && Array.isArray(refinements)) {
      for (const r of refinements) {
        apiMessages.push({ role: r.role, content: r.content });
      }
    }

    const message = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 256,
      messages: apiMessages,
    });
    const draft = message.content[0].text.trim();
    res.json({ success: true, draft });
  } catch (error) {
    console.error('AI text-from-voicemail error:', error);
    serverError(res, error, 'AI text-from-voicemail failed');
  }
});

// AI Business Assistant - for app
app.post('/api/app/ai/assistant', authenticateToken, async (req, res) => {
  if (!anthropicClient) {
    return res.status(503).json({ success: false, error: 'AI service not configured' });
  }
  const { question, conversationHistory } = req.body;
  if (!question) {
    return res.status(400).json({ success: false, error: 'question is required' });
  }
  try {
    // ─── Tool-use AI Assistant: Claude can query the database dynamically ───
    const DB_SCHEMA = `
DATABASE SCHEMA (PostgreSQL) — use these tables to answer questions:

customers: id, name, first_name, last_name, email, phone, mobile, street, city, state, postal_code, customer_number, customer_type, monthly_plan_amount, tax_exempt, created_at
scheduled_jobs: id, job_date, customer_id, customer_name, service_type, service_frequency, service_price, address, status, crew_assigned, estimated_duration, recurring_end_date, is_recurring, recurring_pattern, pipeline_stage, material_cost, labor_cost, invoice_id, property_id, completion_notes, created_at
invoices: id, invoice_number, customer_id, customer_name, customer_email, status (draft/sent/paid/overdue), subtotal, tax_amount, total, amount_paid, due_date, paid_at, sent_at, created_at, line_items (JSONB), billing_month, processing_fee
payments: id, invoice_id, customer_id, amount, method, status (pending/completed/failed), square_payment_id, card_brand, card_last4, paid_at, created_at
sent_quotes: id, quote_number, customer_name, customer_email, customer_phone, customer_address, status, services (JSONB), total, contract_signed_at, created_at, customer_id
quotes: id, name, email, phone, address, services (TEXT[]), status, source, created_at
properties: id, property_name, street, city, state, zip, lot_size, customer_id, status, tags, created_at
messages: id, direction, from_number, to_number, body, customer_id, read, created_at
calls: id, direction, from_number, to_number, status, duration, transcription, customer_id, created_at
employees: id, title, first_name, last_name, hire_date, pay_type, is_active, created_at
crews: id, name, members, crew_type, is_active, color, created_at
time_entries: id, crew_id, crew_name, job_id, customer_name, clock_in, clock_out, break_minutes, created_at
job_expenses: id, job_id, description, category, amount, created_at
campaigns: id, name, description, status, send_count, open_count, click_count, created_at
campaign_submissions: id, campaign_id, name, email, phone, address, status, created_at
season_kickoff_responses: id, customer_id, customer_name, customer_email, services (JSONB), status, responded_at, created_at
email_log: id, recipient_email, subject, email_type, customer_id, status, sent_at
service_requests: id, customer_id, type, service_type, description, urgency, status, created_at
late_fees: id, invoice_id, fee_amount, days_overdue, waived, created_at
cancellations: id, customer_name, cancellation_reason, status, created_at
business_settings: id, key (UNIQUE), value (JSONB)
internal_notes: id, entity_type, entity_id, author_name, content, pinned, created_at
automations: id, name, trigger_type, actions, enabled, created_at
social_media_posts: id, platform, content, tone, created_at
service_items: id, name, default_rate, duration_minutes, category, active, created_at

NOTES:
- Customer name fallback: use COALESCE(name, TRIM(CONCAT(first_name, ' ', last_name)), 'Unknown')
- invoice line_items is JSONB array: [{description, quantity, unit_price, amount, taxable}]
- sent_quotes services is JSONB array: [{name, price, frequency, description}]
- season_kickoff_responses services is JSONB: service confirmations for annual plans
- Use ILIKE for case-insensitive text matching
- Dates are timestamptz — use AT TIME ZONE 'America/New_York' when formatting
- "monthly plan" customers typically have monthly_plan_amount > 0 or recurring service_frequency
`;

    const SENSITIVE_COLUMNS = ['password_hash', 'password', 'token', 'access_token', 'refresh_token', 'sign_token', 'payment_token', 'push_token', 'secret'];

    const toolDefinition = {
      name: 'query_database',
      description: 'Run a read-only SQL SELECT query against the business database. Use this to look up customers, invoices, jobs, quotes, revenue, counts, and any business data. You can run multiple queries to drill down into data.',
      input_schema: {
        type: 'object',
        properties: {
          sql: { type: 'string', description: 'A PostgreSQL SELECT query. Only SELECT is allowed — no INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, or GRANT.' },
          purpose: { type: 'string', description: 'Brief description of what this query is checking (for logging)' }
        },
        required: ['sql']
      }
    };

    async function executeReadOnlyQuery(sql) {
      // Safety: only allow SELECT
      const normalized = sql.trim().replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '').trim();
      const firstWord = normalized.split(/\s+/)[0].toUpperCase();
      if (firstWord !== 'SELECT' && firstWord !== 'WITH' && firstWord !== '(') {
        return { error: 'Only SELECT queries are allowed.' };
      }
      const forbidden = /\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|CREATE|COPY)\b/i;
      if (forbidden.test(normalized)) {
        return { error: 'Query contains forbidden keywords. Only SELECT queries are allowed.' };
      }

      const client = await pool.connect();
      try {
        await client.query('SET statement_timeout = 5000');
        await client.query('BEGIN READ ONLY');
        const result = await client.query(sql);
        await client.query('COMMIT');
        let rows = result.rows.slice(0, 100);
        // Strip sensitive columns
        rows = rows.map(row => {
          const clean = { ...row };
          for (const key of Object.keys(clean)) {
            if (SENSITIVE_COLUMNS.some(s => key.toLowerCase().includes(s))) {
              clean[key] = '[REDACTED]';
            }
          }
          return clean;
        });
        return { rows, rowCount: result.rowCount, truncated: result.rowCount > 100 };
      } catch (queryErr) {
        await client.query('ROLLBACK').catch(() => {});
        return { error: queryErr.message };
      } finally {
        client.release();
      }
    }

    // ─── CopilotCRM tool: live dispatch, customer lookup, service history ───
    const copilotToolDefinition = {
      name: 'query_copilotcrm',
      description: 'Query CopilotCRM for live dispatch/route data, customer records, estimates, or service history. Use this when asked about today\'s schedule, crew routes, dispatch, what crews are doing, CRM notes, or past service history from CopilotCRM.',
      input_schema: {
        type: 'object',
        properties: {
          action: {
            type: 'string',
            enum: ['dispatch_today', 'dispatch_date_range', 'customer_lookup', 'customer_estimates', 'service_history'],
            description: 'What to fetch: dispatch_today (today\'s route/schedule), dispatch_date_range (jobs for a date range), customer_lookup (CRM record for a customer), customer_estimates (estimates for a CRM customer), service_history (past 12 months of jobs for a customer)'
          },
          customer_name: { type: 'string', description: 'Customer name (required for customer_lookup, customer_estimates, service_history)' },
          start_date: { type: 'string', description: 'Start date for dispatch_date_range (e.g. "Mar 1, 2026")' },
          end_date: { type: 'string', description: 'End date for dispatch_date_range (e.g. "Mar 31, 2026")' },
          purpose: { type: 'string', description: 'Brief description of what this lookup is for (for logging)' }
        },
        required: ['action']
      }
    };

    async function executeCopilotQuery(action, params) {
      try {
        await ensureCopilotSyncTables();
        const tokenInfo = await getCopilotToken();
        if (!tokenInfo || !tokenInfo.cookieHeader) {
          return { error: 'CopilotCRM not connected — no authentication token configured. Check copilot_sync_settings table.' };
        }
        const copilotHeaders = {
          'Cookie': tokenInfo.cookieHeader,
          'Origin': 'https://secure.copilotcrm.com',
          'Referer': 'https://secure.copilotcrm.com/',
          'X-Requested-With': 'XMLHttpRequest',
          'Content-Type': 'application/x-www-form-urlencoded',
        };

        if (action === 'dispatch_today' || action === 'dispatch_date_range') {
          const today = new Date();
          const fmt = (d) => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
          let sDate, eDate;
          if (action === 'dispatch_date_range' && params.start_date) {
            sDate = params.start_date;
            eDate = params.end_date || params.start_date;
          } else {
            sDate = fmt(today);
            eDate = fmt(today);
          }
          const formData = new URLSearchParams();
          formData.append('accessFrom', 'route');
          formData.append('bs4', '1');
          formData.append('sDate', sDate);
          formData.append('eDate', eDate);
          formData.append('optimizationFlag', '1');
          formData.append('count', '-1');
          for (const t of ['1', '2', '3', '4', '5', '0']) formData.append('evtypes_route[]', t);
          formData.append('isdate', '0');
          formData.append('sdate', sDate);
          formData.append('edate', eDate);
          formData.append('erec', 'all');
          formData.append('estatus', 'any');
          formData.append('esort', '');
          formData.append('einvstatus', 'any');

          const res = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
            method: 'POST', headers: copilotHeaders, body: formData.toString(),
          });
          if (!res.ok) return { error: `CopilotCRM dispatch returned ${res.status}` };
          const data = await res.json().catch(() => null);
          if (!data) return { error: 'CopilotCRM auth session may have expired. Token may need refresh.' };
          const jobs = parseCopilotRouteHtml(data.html || '', data.employees || []);
          return { jobs, jobCount: jobs.length, dateRange: `${sDate} to ${eDate}` };
        }

        if (action === 'customer_lookup' || action === 'customer_estimates') {
          if (!params.customer_name) return { error: 'customer_name is required for this action' };
          const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
            method: 'POST', headers: copilotHeaders,
            body: `query=${encodeURIComponent(params.customer_name)}`,
          });
          if (!searchRes.ok) return { error: `CopilotCRM customer search returned ${searchRes.status}` };
          const crmCustomers = await searchRes.json().catch(() => null);
          if (!crmCustomers) return { error: 'CopilotCRM auth session may have expired.' };
          const crmMatch = Array.isArray(crmCustomers) ? crmCustomers.find(c => c.id && String(c.id) !== '0') : null;
          if (!crmMatch) return { error: `No CopilotCRM record found for "${params.customer_name}"` };

          // Strip sensitive fields
          const cleanRecord = { ...crmMatch };
          for (const k of Object.keys(cleanRecord)) {
            if (['password', 'token', 'hash', 'secret'].some(s => k.toLowerCase().includes(s))) delete cleanRecord[k];
          }

          if (action === 'customer_lookup') return { customer: cleanRecord };

          // Fetch estimates
          try {
            const estRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
              method: 'POST', headers: copilotHeaders,
              body: `customer_id=${crmMatch.id}`,
            });
            if (estRes.ok) {
              const estData = await estRes.json().catch(() => null);
              if (estData && estData.html) {
                const $est = cheerio.load(estData.html);
                const estimates = [];
                $est('tr[id]').each((i, row) => {
                  const cells = [];
                  $est(row).find('td').each((_, td) => cells.push($est(td).text().trim()));
                  if (cells.length > 0 && cells.some(c => c)) estimates.push(cells.filter(c => c).join(' | '));
                });
                return { customer: cleanRecord, estimates };
              }
            }
            return { customer: cleanRecord, estimates: [] };
          } catch (estErr) {
            return { customer: cleanRecord, estimates: [], estimateError: estErr.message };
          }
        }

        if (action === 'service_history') {
          if (!params.customer_name) return { error: 'customer_name is required for service_history' };
          const now = new Date();
          const yearAgo = new Date(now);
          yearAgo.setFullYear(yearAgo.getFullYear() - 1);
          const fmt = (d) => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
          const formData = new URLSearchParams();
          formData.append('accessFrom', 'route');
          formData.append('bs4', '1');
          formData.append('sDate', fmt(yearAgo));
          formData.append('eDate', fmt(now));
          formData.append('optimizationFlag', '1');
          formData.append('count', '-1');
          for (const t of ['1', '2', '3', '4', '5', '0']) formData.append('evtypes_route[]', t);
          formData.append('isdate', '0');
          formData.append('sdate', fmt(yearAgo));
          formData.append('edate', fmt(now));
          formData.append('erec', 'all');
          formData.append('estatus', 'any');
          formData.append('esort', '');
          formData.append('einvstatus', 'any');

          const histRes = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
            method: 'POST', headers: copilotHeaders, body: formData.toString(),
          });
          if (!histRes.ok) return { error: `CopilotCRM service history returned ${histRes.status}` };
          const histData = await histRes.json().catch(() => null);
          if (!histData) return { error: 'CopilotCRM auth session may have expired.' };
          const allJobs = parseCopilotRouteHtml(histData.html || '', histData.employees || []);
          const custNameLower = params.customer_name.toLowerCase();
          const custJobs = allJobs.filter(j => j.customer_name && j.customer_name.toLowerCase().includes(custNameLower));
          return { jobs: custJobs, jobCount: custJobs.length, customer_name: params.customer_name, period: 'past 12 months' };
        }

        return { error: `Unknown action: ${action}` };
      } catch (err) {
        console.error('CopilotCRM tool error:', err.message);
        return { error: `CopilotCRM error: ${err.message}` };
      }
    }

    const todayStr = new Date().toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });

    const systemPrompt = `You are the Pappas & Co. Landscaping business assistant. You help Tim and Theresa manage their landscaping business in Cleveland, OH. Service areas: Lakewood, Brook Park, Bay Village, and Westpark.

You have TWO tools:
1. **query_database** — query the local PostgreSQL database for customers, invoices, jobs, quotes, revenue, schedules, communications, and more
2. **query_copilotcrm** — query CopilotCRM for live dispatch/route data, CRM customer records, estimates, and service history

USE THESE TOOLS. Never say you don't have access to data. Always query to answer questions.

Use query_database for: customer counts, invoice totals, revenue, payments, quotes, campaign data, communications, employee info, and any aggregate/analytical questions.
Use query_copilotcrm for: today's dispatch/route, what crews are doing, live schedule, CRM notes on customers, CopilotCRM estimates, and past service history.

${DB_SCHEMA}

Today's date: ${todayStr}

Guidelines:
- ALWAYS query to answer questions — never guess or say you can't access data
- Run multiple queries if needed to get the full picture
- Be friendly, concise, and professional
- Format numbers as currency ($X,XXX.XX) when showing money
- When listing items, use clear formatting
- If a query returns no results, say so clearly — don't make up data`;

    // Build messages with conversation history
    const messages = [];
    if (conversationHistory && Array.isArray(conversationHistory)) {
      for (const msg of conversationHistory.slice(0, -1)) {
        if (msg.role === 'user' || msg.role === 'assistant') {
          messages.push({ role: msg.role, content: String(msg.content || '') });
        }
      }
    }
    messages.push({ role: 'user', content: question });

    // Merge consecutive same-role messages
    const apiMessages = [];
    for (const msg of messages) {
      const prev = apiMessages[apiMessages.length - 1];
      if (prev && prev.role === msg.role) {
        prev.content += '\n\n' + msg.content;
      } else {
        apiMessages.push({ ...msg });
      }
    }
    if (apiMessages.length > 0 && apiMessages[0].role !== 'user') {
      apiMessages[0].role = 'user';
    }

    // Tool-use loop: let Claude query the database up to 5 times
    let currentMessages = [...apiMessages];
    let finalAnswer = '';
    const MAX_TOOL_ROUNDS = 8;

    for (let round = 0; round < MAX_TOOL_ROUNDS + 1; round++) {
      const apiParams = {
        model: 'claude-sonnet-4-20250514',
        max_tokens: 1500,
        system: systemPrompt,
        messages: currentMessages,
        tools: [toolDefinition, copilotToolDefinition],
      };
      // On last possible round, force no more tools
      if (round === MAX_TOOL_ROUNDS) {
        apiParams.tool_choice = { type: 'none' };
      }

      console.log(`[Assistant] Round ${round + 1}: sending ${currentMessages.length} messages`);
      const response = await anthropicClient.messages.create(apiParams);

      // Check if Claude wants to use tools
      const toolUseBlocks = response.content.filter(b => b.type === 'tool_use');
      const textBlocks = response.content.filter(b => b.type === 'text');

      if (toolUseBlocks.length === 0) {
        // No tool calls — Claude has the final answer
        finalAnswer = textBlocks.map(b => b.text).join('\n').trim();
        break;
      }

      // Process tool calls
      currentMessages.push({ role: 'assistant', content: response.content });
      const toolResults = [];
      for (const toolBlock of toolUseBlocks) {
        let result;
        if (toolBlock.name === 'query_database') {
          const { sql, purpose } = toolBlock.input;
          console.log(`[Assistant] DB query (${purpose || 'query'}): ${(sql || '').substring(0, 200)}`);
          result = await executeReadOnlyQuery(sql || '');
        } else if (toolBlock.name === 'query_copilotcrm') {
          const { action, purpose, ...params } = toolBlock.input;
          console.log(`[Assistant] CopilotCRM (${action}): ${purpose || params.customer_name || ''}`);
          result = await executeCopilotQuery(action, params);
        } else {
          result = { error: `Unknown tool: ${toolBlock.name}` };
        }
        toolResults.push({
          type: 'tool_result',
          tool_use_id: toolBlock.id,
          content: JSON.stringify(result),
        });
      }
      currentMessages.push({ role: 'user', content: toolResults });

      // If there was also text with the tool calls, capture it
      if (response.stop_reason === 'end_turn' && textBlocks.length > 0) {
        finalAnswer = textBlocks.map(b => b.text).join('\n').trim();
        break;
      }
    }

    // If we exhausted rounds without a text answer, force one final text-only call
    if (!finalAnswer) {
      try {
        const forceTextResponse = await anthropicClient.messages.create({
          model: 'claude-sonnet-4-20250514',
          max_tokens: 1500,
          system: systemPrompt,
          messages: currentMessages,
          tool_choice: { type: 'none' },
        });
        finalAnswer = forceTextResponse.content.filter(b => b.type === 'text').map(b => b.text).join('\n').trim();
      } catch { /* fall through to generic message */ }
    }
    if (!finalAnswer) {
      finalAnswer = 'I ran into an issue processing your question. Could you try rephrasing it?';
    }

    res.json({ success: true, answer: finalAnswer });
  } catch (error) {
    console.error('AI assistant error:', error);
    const errMsg = error.message || 'Unknown error';
    const errType = error.constructor?.name || 'Error';
    res.status(500).json({ success: false, error: `Assistant failed: ${errType}: ${errMsg}` });
  }
});

// ─── Legacy context-based assistant (kept for mobile app compatibility) ───
// The mobile app (TwilioConnect) still calls this endpoint format
// If mobile app is updated, this can be removed
/* REMOVED - old context-gathering assistant replaced by tool-use version above
    const customerResult = await pool.query(`SELECT id, name, phone, mobile, email, street, city, state, postal_code FROM customers ORDER BY name LIMIT 500`);

    // Try to identify a specific customer the question is about
    let focusCustomer = customerResult.rows.find(c => c.name && qLower.includes(c.name.toLowerCase()));
    if (!focusCustomer) {
      focusCustomer = customerResult.rows.find(c => {
        if (!c.name) return false;
        const parts = c.name.trim().split(/\s+/);
        const lastName = parts[parts.length - 1];
        return lastName.length > 3 && qLower.includes(lastName.toLowerCase());
      });
    }

    if (focusCustomer) {
      // FOCUSED MODE: question is about a specific customer — give detailed data for just them
      context.push(`TARGET CUSTOMER (this is who the user is asking about — use these exact details):\n  Name: ${focusCustomer.name}\n  Phone: ${focusCustomer.phone || focusCustomer.mobile || 'none'}\n  Mobile: ${focusCustomer.mobile || 'none'}\n  Email: ${focusCustomer.email || 'none'}\n  Address: ${[focusCustomer.street, focusCustomer.city, focusCustomer.state, focusCustomer.postal_code].filter(Boolean).join(', ')}`);

      // Messages for this customer
      const custMessages = await pool.query(`
        SELECT m.direction, m.from_number, m.to_number, m.body, m.created_at
        FROM messages m
        WHERE m.customer_id = $1
        ORDER BY m.created_at DESC LIMIT 20
      `, [focusCustomer.id]);
      if (custMessages.rows.length > 0) {
        context.push(`MESSAGES WITH ${focusCustomer.name} (${custMessages.rows.length} recent):\n` + custMessages.rows.map(m =>
          `[${fmtDate(m.created_at)}] ${m.direction === 'inbound' ? focusCustomer.name + ' →' : '← Tim →'}: ${(m.body || '').substring(0, 300)}`
        ).join('\n'));
      }

      // Calls for this customer
      const custPhone = (focusCustomer.mobile || focusCustomer.phone || '').replace(/\D/g, '').slice(-10);
      if (custPhone) {
        const custCalls = await pool.query(`
          SELECT from_number, to_number, status, duration, transcription, created_at
          FROM calls
          WHERE RIGHT(REGEXP_REPLACE(from_number, '[^0-9]', '', 'g'), 10) = $1
             OR RIGHT(REGEXP_REPLACE(to_number, '[^0-9]', '', 'g'), 10) = $1
          ORDER BY created_at DESC LIMIT 10
        `, [custPhone]);
        if (custCalls.rows.length > 0) {
          context.push(`CALLS WITH ${focusCustomer.name} (${custCalls.rows.length} recent):\n` + custCalls.rows.map(c =>
            `[${fmtDate(c.created_at)}] ${c.status}${c.duration ? `, ${c.duration}s` : ''}${c.transcription ? ` — "${c.transcription.substring(0, 300)}"` : ''}`
          ).join('\n'));
        }
      }

      // Invoices for this customer
      const custInvoices = await pool.query(`SELECT invoice_number, status, total, due_date, created_at FROM invoices WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 10`, [focusCustomer.id]);
      if (custInvoices.rows.length > 0) {
        context.push(`INVOICES FOR ${focusCustomer.name}:\n` + custInvoices.rows.map(i =>
          `#${i.invoice_number || 'N/A'} — $${parseFloat(i.total || 0).toFixed(2)} — ${i.status}${i.due_date ? ` — Due: ${new Date(i.due_date).toLocaleDateString('en-US')}` : ''}`
        ).join('\n'));
      }

      // Jobs for this customer
      const custJobs = await pool.query(`SELECT service_type, job_date, status, crew_assigned, service_price, address FROM scheduled_jobs WHERE customer_id = $1 OR customer_name ILIKE $2 ORDER BY job_date DESC LIMIT 20`, [focusCustomer.id, `%${focusCustomer.name}%`]);
      if (custJobs.rows.length > 0) {
        context.push(`JOBS FOR ${focusCustomer.name} (${custJobs.rows.length}):\n` + custJobs.rows.map(j =>
          `${j.job_date ? new Date(j.job_date).toLocaleDateString('en-US') : 'No date'} — ${j.service_type} — ${j.status || 'scheduled'}${j.crew_assigned ? ` — Crew: ${j.crew_assigned}` : ''}${j.service_price ? ` — $${j.service_price}` : ''} — ${j.address || ''}`
        ).join('\n'));
      }

      // Quotes for this customer
      const custQuotes = await pool.query(`SELECT quote_number, status, total, created_at FROM sent_quotes WHERE customer_id = $1 OR customer_name ILIKE $2 ORDER BY created_at DESC LIMIT 10`, [focusCustomer.id, `%${focusCustomer.name}%`]);
      if (custQuotes.rows.length > 0) {
        context.push(`QUOTES FOR ${focusCustomer.name}:\n` + custQuotes.rows.map(q =>
          `#${q.quote_number || 'N/A'} — $${parseFloat(q.total || 0).toFixed(2)} — ${q.status}`
        ).join('\n'));
      }
    } else {
      // GENERAL MODE: no specific customer — show recent activity across the business
      const customerNames = customerResult.rows.slice(0, 100).map(c => c.name).filter(Boolean).join(', ');
      context.push(`CUSTOMER LIST (${customerResult.rows.length} total, first 100): ${customerNames}`);

      const messagesResult = await pool.query(`
        SELECT m.direction, m.body, m.created_at, COALESCE(c.name, 'Unknown') as customer_name
        FROM messages m LEFT JOIN customers c ON m.customer_id = c.id
        ORDER BY m.created_at DESC LIMIT 25
      `);
      if (messagesResult.rows.length > 0) {
        context.push('RECENT MESSAGES:\n' + messagesResult.rows.map(m =>
          `[${fmtDate(m.created_at)}] ${m.direction === 'inbound' ? m.customer_name + ' →' : '← Tim →'}: ${(m.body || '').substring(0, 150)}`
        ).join('\n'));
      }

      const callsResult = await pool.query(`
        SELECT c.from_number, c.status, c.duration, c.transcription, c.created_at,
          COALESCE(cu.name, 'Unknown') as customer_name
        FROM calls c
        LEFT JOIN customers cu ON RIGHT(REGEXP_REPLACE(COALESCE(cu.mobile,''), '[^0-9]', '', 'g'), 10) = RIGHT(REGEXP_REPLACE(c.from_number, '[^0-9]', '', 'g'), 10)
          OR RIGHT(REGEXP_REPLACE(cu.phone, '[^0-9]', '', 'g'), 10) = RIGHT(REGEXP_REPLACE(c.from_number, '[^0-9]', '', 'g'), 10)
        ORDER BY c.created_at DESC LIMIT 15
      `);
      if (callsResult.rows.length > 0) {
        context.push('RECENT CALLS:\n' + callsResult.rows.map(c =>
          `[${fmtDate(c.created_at)}] ${c.customer_name} — ${c.status}${c.transcription ? ` — "${c.transcription.substring(0, 150)}"` : ''}`
        ).join('\n'));
      }

      const invoicesResult = await pool.query(`SELECT invoice_number, customer_name, status, total, due_date FROM invoices ORDER BY created_at DESC LIMIT 15`);
      if (invoicesResult.rows.length > 0) {
        context.push('RECENT INVOICES:\n' + invoicesResult.rows.map(i =>
          `#${i.invoice_number || 'N/A'} — ${i.customer_name} — $${parseFloat(i.total || 0).toFixed(2)} — ${i.status}`
        ).join('\n'));
      }

      const jobsResult = await pool.query(`SELECT customer_name, service_type, job_date, status, crew_assigned FROM scheduled_jobs WHERE job_date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY job_date ASC LIMIT 30`);
      if (jobsResult.rows.length > 0) {
        context.push('SCHEDULED JOBS (past week + upcoming):\n' + jobsResult.rows.map(j =>
          `${j.job_date ? new Date(j.job_date).toLocaleDateString('en-US') : '?'} — ${j.customer_name} — ${j.service_type} — ${j.status || 'scheduled'}${j.crew_assigned ? ` — ${j.crew_assigned}` : ''}`
        ).join('\n'));
      }

      const quotesResult = await pool.query(`SELECT quote_number, customer_name, status, total FROM sent_quotes ORDER BY created_at DESC LIMIT 10`);
      if (quotesResult.rows.length > 0) {
        context.push('RECENT QUOTES:\n' + quotesResult.rows.map(q =>
          `#${q.quote_number || 'N/A'} — ${q.customer_name} — $${parseFloat(q.total || 0).toFixed(2)} — ${q.status}`
        ).join('\n'));
      }
    }

    // CopilotCRM fallback — only query when question needs live dispatch, CRM notes, or service history
    const needsDispatch = /crew status|dispatch|route today|what.?s happening today|what are the crews|who.?s working/i.test(qLower);
    const needsCrmLookup = /copilot|crm|crm notes|notes on/i.test(qLower);
    const needsServiceHistory = /service history|past services|what did we do|jobs completed|previous work|work history|last service|when did we.*service|services did|did we do for/i.test(qLower);
    const needsCopilot = needsDispatch || needsCrmLookup || needsServiceHistory;

    if (needsCopilot) {
      try {
        await ensureCopilotSyncTables();
        const tokenInfo = await getCopilotToken();
        if (!tokenInfo || !tokenInfo.cookieHeader) {
          context.push('COPILOTCRM: Could not connect — no authentication token configured.');
        } else {
          const copilotHeaders = {
            'Cookie': tokenInfo.cookieHeader,
            'Origin': 'https://secure.copilotcrm.com',
            'Referer': 'https://secure.copilotcrm.com/',
            'X-Requested-With': 'XMLHttpRequest',
            'Content-Type': 'application/x-www-form-urlencoded',
          };

          // Live dispatch for today
          if (needsDispatch || needsServiceHistory) {
            try {
              const today = new Date();
              const dateStr = today.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
              const formData = new URLSearchParams();
              formData.append('accessFrom', 'route');
              formData.append('bs4', '1');
              formData.append('sDate', dateStr);
              formData.append('eDate', dateStr);
              formData.append('optimizationFlag', '1');
              formData.append('count', '-1');
              for (const t of ['1', '2', '3', '4', '5', '0']) formData.append('evtypes_route[]', t);
              formData.append('isdate', '0');
              formData.append('sdate', dateStr);
              formData.append('edate', dateStr);
              formData.append('erec', 'all');
              formData.append('estatus', 'any');
              formData.append('esort', '');
              formData.append('einvstatus', 'any');

              const dispatchRes = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
                method: 'POST',
                headers: copilotHeaders,
                body: formData.toString(),
              });
              if (dispatchRes.ok) {
                const dispatchData = await dispatchRes.json().catch(() => null);
                if (!dispatchData) {
                  context.push('COPILOTCRM DISPATCH: Auth session may have expired. Token may need refresh.');
                } else {
                const jobs = parseCopilotRouteHtml(dispatchData.html || '', dispatchData.employees || []);
                if (jobs.length > 0) {
                  context.push('COPILOTCRM LIVE DISPATCH (today):\n' + jobs.map(j =>
                    `Stop ${j.stop_order || '?'}: ${j.customer_name} — ${j.job_title || 'Service'} — ${j.address || ''} — Crew: ${j.crew_name || 'Unassigned'} — Status: ${j.status || 'Scheduled'}`
                  ).join('\n'));
                } else {
                  context.push('COPILOTCRM LIVE DISPATCH: No jobs scheduled for today.');
                }
                }
              }
            } catch (dispatchErr) {
              console.error('CopilotCRM dispatch fetch error:', dispatchErr.message);
              context.push('COPILOTCRM DISPATCH: Could not fetch live dispatch data.');
            }
          }

          // Customer search in CRM — extract name from question
          if (needsCrmLookup || needsServiceHistory) {
            try {
              // Match customer name from question against our customer list
              // Try full name first, then last name, then first name
              let matchedCustomer = customerResult.rows.find(c => c.name && qLower.includes(c.name.toLowerCase()));
              if (!matchedCustomer) {
                // Try matching by last name (more than 3 chars to avoid false positives)
                matchedCustomer = customerResult.rows.find(c => {
                  if (!c.name) return false;
                  const parts = c.name.trim().split(/\s+/);
                  const lastName = parts[parts.length - 1];
                  return lastName.length > 3 && qLower.includes(lastName.toLowerCase());
                });
              }
              console.log(`[Assistant] CopilotCRM customer match: ${matchedCustomer?.name || 'none found'} for question: "${question.substring(0, 80)}"`);

              if (matchedCustomer) {
                // Look up customer in CopilotCRM
                const searchRes = await fetch('https://secure.copilotcrm.com/customers/filter', {
                  method: 'POST',
                  headers: copilotHeaders,
                  body: `query=${encodeURIComponent(matchedCustomer.name)}`,
                });
                if (searchRes.ok) {
                  const crmCustomers = await searchRes.json().catch(() => null);
                  if (!crmCustomers) {
                    context.push('COPILOTCRM: Auth session may have expired — customer search returned invalid data. Token may need refresh.');
                  }
                  const crmMatch = crmCustomers && Array.isArray(crmCustomers) ? crmCustomers.find(c => c.id && String(c.id) !== '0') : null;
                  if (crmMatch) {
                    const crmFields = Object.entries(crmMatch)
                      .filter(([k, v]) => v && !['password', 'token', 'hash'].includes(k))
                      .map(([k, v]) => `  ${k}: ${typeof v === 'object' ? JSON.stringify(v) : v}`)
                      .join('\n');
                    context.push(`COPILOTCRM CUSTOMER RECORD (${matchedCustomer.name}):\n${crmFields}`);

                    // Fetch estimates for this customer
                    try {
                      const estRes = await fetch('https://secure.copilotcrm.com/finances/estimates/getEstimatesListAjax', {
                        method: 'POST',
                        headers: copilotHeaders,
                        body: `customer_id=${crmMatch.id}`,
                      });
                      if (estRes.ok) {
                        const estData = await estRes.json().catch(() => null);
                        if (estData && estData.html) {
                          const $est = cheerio.load(estData.html);
                          const estimates = [];
                          $est('tr[id]').each((i, row) => {
                            const $r = $est(row);
                            const cells = [];
                            $r.find('td').each((_, td) => cells.push($est(td).text().trim()));
                            if (cells.length > 0 && cells.some(c => c)) estimates.push(cells.filter(c => c).join(' | '));
                          });
                          if (estimates.length > 0) {
                            context.push(`COPILOTCRM ESTIMATES (${matchedCustomer.name}):\n` + estimates.join('\n'));
                          }
                        }
                      }
                    } catch (estErr) {
                      console.error('CopilotCRM estimate fetch error:', estErr.message);
                    }
                  } else {
                    context.push(`COPILOTCRM: No customer record found for "${matchedCustomer.name}".`);
                  }
                }

                // Fetch service/job history from scheduler — past 12 months filtered by customer name
                if (needsServiceHistory) {
                  try {
                    const now = new Date();
                    const yearAgo = new Date(now);
                    yearAgo.setFullYear(yearAgo.getFullYear() - 1);
                    const fmt = (d) => d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
                    const histFormData = new URLSearchParams();
                    histFormData.append('accessFrom', 'route');
                    histFormData.append('bs4', '1');
                    histFormData.append('sDate', fmt(yearAgo));
                    histFormData.append('eDate', fmt(now));
                    histFormData.append('optimizationFlag', '1');
                    histFormData.append('count', '-1');
                    for (const t of ['1', '2', '3', '4', '5', '0']) histFormData.append('evtypes_route[]', t);
                    histFormData.append('isdate', '0');
                    histFormData.append('sdate', fmt(yearAgo));
                    histFormData.append('edate', fmt(now));
                    histFormData.append('erec', 'all');
                    histFormData.append('estatus', 'any');
                    histFormData.append('esort', '');
                    histFormData.append('einvstatus', 'any');

                    const histRes = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
                      method: 'POST',
                      headers: copilotHeaders,
                      body: histFormData.toString(),
                    });
                    if (histRes.ok) {
                      const histData = await histRes.json().catch(() => null);
                      if (!histData) {
                        context.push('COPILOTCRM SERVICE HISTORY: Auth session may have expired — could not fetch job history. Token may need refresh.');
                      } else {
                      const allJobs = parseCopilotRouteHtml(histData.html || '', histData.employees || []);
                      // Filter to this customer
                      const custNameLower = matchedCustomer.name.toLowerCase();
                      const custJobs = allJobs.filter(j => j.customer_name && j.customer_name.toLowerCase().includes(custNameLower));
                      if (custJobs.length > 0) {
                        context.push(`COPILOTCRM SERVICE HISTORY (${matchedCustomer.name}, past 12 months, ${custJobs.length} jobs):\n` + custJobs.map(j =>
                          `${j.job_title || 'Service'} — ${j.address || ''} — Crew: ${j.crew_name || 'N/A'} — Status: ${j.status || 'N/A'}${j.visit_total ? ` — $${j.visit_total}` : ''}`
                        ).join('\n'));
                      } else {
                        context.push(`COPILOTCRM SERVICE HISTORY: No jobs found for "${matchedCustomer.name}" in the past 12 months.`);
                      }
                      }
                    }
                  } catch (histErr) {
                    console.error('CopilotCRM service history fetch error:', histErr.message);
                    context.push('COPILOTCRM SERVICE HISTORY: Could not fetch job history.');
                  }
                }
              } else {
                context.push('COPILOTCRM: Could not identify which customer you\'re asking about. Try using their full name.');
              }
            } catch (crmErr) {
              console.error('CopilotCRM customer lookup error:', crmErr.message);
              context.push('COPILOTCRM: Could not look up customer details.');
            }
          }
        }
      } catch (copilotErr) {
        console.error('CopilotCRM fallback error:', copilotErr.message);
        context.push('COPILOTCRM: Service unavailable right now. Answering from local records only.');
      }
    }

    const systemPrompt = `You are the Pappas & Co. Landscaping business assistant. You help Tim and Theresa's team answer questions about their landscaping business in Cleveland, OH. Service areas: Lakewood, Brook Park, Bay Village, and Westpark.

You have access to the following business data:

${context.join('\n\n')}

Answer the user's question based on this data. Be friendly, conversational, and professional. If you're not sure about something, say so honestly. Keep answers concise but thorough. Format important details clearly.

IMPORTANT: When a TARGET CUSTOMER section is provided, use ONLY the exact details from that section for their name, phone, address, etc. Do NOT paraphrase or approximate addresses — quote them exactly as shown. Include all relevant information from their messages, calls, invoices, and jobs.
${needsCopilot ? '\nIf CopilotCRM data is included above, mention what came from CopilotCRM vs local records when relevant.' : ''}
Today's date: ${new Date().toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' })}`;

    // Build messages array with proper alternating roles for Anthropic API
    const rawMessages = [];
    // System context as first user message
    rawMessages.push({ role: 'user', content: systemPrompt });
    // Prior conversation turns (skip last which is the current question)
    if (conversationHistory && Array.isArray(conversationHistory)) {
      for (const msg of conversationHistory.slice(0, -1)) {
        if (msg.role === 'user' || msg.role === 'assistant') {
          rawMessages.push({ role: msg.role, content: String(msg.content || '') });
        }
      }
    }
    // Current question
    rawMessages.push({ role: 'user', content: question });

    // Merge consecutive same-role messages to ensure strict alternation
    const apiMessages = [];
    for (const msg of rawMessages) {
      const prev = apiMessages[apiMessages.length - 1];
      if (prev && prev.role === msg.role) {
        prev.content += '\n\n' + msg.content;
      } else {
        apiMessages.push({ ...msg });
      }
    }
    // Ensure first message is user role (required by Anthropic)
    if (apiMessages.length > 0 && apiMessages[0].role !== 'user') {
      apiMessages[0].role = 'user';
    }

    console.log(`[Assistant] Sending ${apiMessages.length} messages to Anthropic (context: ${context.length} sections, copilot: ${needsCopilot})`);
    const message = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      messages: apiMessages,
    });
    const answer = message.content[0].text.trim();
    res.json({ success: true, answer });
  } catch (error) {
    console.error('AI assistant error:', error);
    // Return specific error for debugging — this is an internal tool, not customer-facing
    const errMsg = error.message || 'Unknown error';
    const errType = error.constructor?.name || 'Error';
    res.status(500).json({ success: false, error: `Assistant failed: ${errType}: ${errMsg}` });
  }
});
END OF OLD ASSISTANT */

// AI freeform draft - for app (new message compose)
app.post('/api/app/ai/draft', authenticateToken, async (req, res) => {
  if (!anthropicClient) {
    return res.status(503).json({ success: false, error: 'AI service not configured' });
  }
  const { contactName, prompt, refinements } = req.body;
  if (!prompt && (!refinements || refinements.length === 0)) {
    return res.status(400).json({ success: false, error: 'prompt is required' });
  }
  try {
    const systemPrompt = `You are Tim from Pappas & Co. Landscaping in Cleveland, OH. Draft a text message based on the user's description.${contactName ? `\n\nRecipient: ${contactName}` : ''}\n\nKeep it under 300 characters. Be friendly, professional, and personable. Just return the message text, nothing else.`;

    const apiMessages = [{ role: 'user', content: prompt ? `${systemPrompt}\n\nUser wants to say: ${prompt}` : systemPrompt }];
    if (refinements && Array.isArray(refinements)) {
      for (const r of refinements) {
        apiMessages.push({ role: r.role, content: r.content });
      }
    }

    const message = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 256,
      messages: apiMessages,
    });
    const draft = message.content[0].text.trim();
    res.json({ success: true, draft });
  } catch (error) {
    console.error('AI draft error:', error);
    serverError(res, error, 'AI draft generation failed');
  }
});

// Upload image for MMS - stores in DB and returns a public URL for Twilio
app.post('/api/app/messages/upload', authenticateToken, upload.single('image'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No image file provided' });
    const b64 = req.file.buffer.toString('base64');
    const mimeType = req.file.mimetype;
    // Store in DB
    await pool.query(`CREATE TABLE IF NOT EXISTS mms_uploads (
      id SERIAL PRIMARY KEY,
      mime_type VARCHAR(100) NOT NULL,
      data TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    const result = await pool.query(
      'INSERT INTO mms_uploads (mime_type, data) VALUES ($1, $2) RETURNING id',
      [mimeType, b64]
    );
    const imageId = result.rows[0].id;
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const publicUrl = `${baseUrl}/api/mms-image/${imageId}`;
    console.log(`📷 MMS image uploaded: ${req.file.originalname} (${Math.round(req.file.size / 1024)}KB) → ${publicUrl}`);
    res.json({ success: true, url: publicUrl });
  } catch (error) {
    console.error('MMS upload error:', error);
    serverError(res, error, 'MMS upload failed');
  }
});

// Serve MMS image — public (Twilio needs to fetch this)
app.get('/api/mms-image/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT mime_type, data FROM mms_uploads WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).send('Not found');
    const { mime_type, data } = result.rows[0];
    const buffer = Buffer.from(data, 'base64');
    res.set('Content-Type', mime_type);
    res.set('Cache-Control', 'public, max-age=86400');
    res.send(buffer);
  } catch (error) {
    res.status(500).send('Error');
  }
});

// Send SMS - for app (with multi-number support)
app.post('/api/app/messages/send', authenticateToken, async (req, res) => {
  const { to, body, mediaUrls, fromNumber } = req.body;

  if (!to || (!body && (!mediaUrls || mediaUrls.length === 0))) {
    return res.status(400).json({ message: 'Phone number and message body or image required' });
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
      body: body || '',
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
    serverError(res, error, 'Failed to send message');
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
app.post('/api/messages/send', validate(schemas.sendMessage), async (req, res) => {
  const { to, body } = req.body;

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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
  }
});

// Follow-up email templates - standalone design (not using shared emailTemplate)
function getFollowupEmailContent(followup, stage) {
  const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
  const assetsUrl = process.env.EMAIL_ASSETS_URL || baseUrl;

  // Qualy heading images (pre-generated PNGs)
  const headingImages = {
    1: `${assetsUrl}/email-assets/heading-1.png`,
    2: `${assetsUrl}/email-assets/heading-2.png`,
    3: `${assetsUrl}/email-assets/heading-3.png`,
    4: `${assetsUrl}/email-assets/heading-4.png`
  };

  // Shared elements
  const bs = 'font-size:15px;color:#4a5568;line-height:1.8;margin:0 0 18px;'; // body style

  const quoteRef = `
    <p style="font-size:13px;color:#94a3b8;text-align:center;margin:18px 0 0;letter-spacing:0.3px;">Quote #${followup.quote_number || 'N/A'} &nbsp;&middot;&nbsp; $${parseFloat(followup.quote_amount || 0).toFixed(2)}</p>
  `;

  const ctaButton = followup.sign_url ? `
    <div style="text-align:center;margin:28px 0 20px;">
      <a href="${followup.sign_url}" style="background:#c9dd80;color:#2e403d;padding:16px 52px;text-decoration:none;border-radius:50px;font-weight:700;font-size:15px;display:inline-block;letter-spacing:0.3px;">View Your Quote \u{2192}</a>
    </div>
    <p style="font-size:14px;color:#94a3b8;text-align:center;margin:0 0 8px;">Or just reply to this email with any questions</p>
  ` : '';

  const limeDivider = `
    <table width="100%" cellpadding="0" cellspacing="0" style="margin:16px 0;"><tr>
      <td style="width:30%;height:1px;background:transparent;"></td>
      <td style="width:40%;height:2px;background:#c9dd80;border-radius:1px;"></td>
      <td style="width:30%;height:1px;background:transparent;"></td>
    </tr></table>
  `;

  // White social media icons for dark footer
  const SOCIAL_FB_WHITE = `${assetsUrl}/email-assets/fb-white.png`;
  const SOCIAL_IG_WHITE = `${assetsUrl}/email-assets/ig-white.png`;
  const SOCIAL_ND_WHITE = `${assetsUrl}/email-assets/nd-white.png`;

  // Build full HTML for follow-up emails
  function followupTemplate(headingImg, bodyContent) {
    return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>@font-face{font-family:'Qualy';src:url('${baseUrl}/Qualy.otf') format('opentype');}</style>
</head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:32px 16px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.06);">
  <tr><td style="background:#2e403d;padding:36px 48px;text-align:center;">
    <img src="${LOGO_URL}" alt="Pappas & Co. Landscaping" style="max-height:100px;max-width:400px;width:auto;">
  </td></tr>
  <tr><td style="padding:40px 48px 8px;text-align:center;">
    <img src="${headingImg}" alt="" style="max-width:400px;width:auto;height:34px;" />
  </td></tr>
  <tr><td style="padding:24px 48px 12px;">
    ${bodyContent}
    ${limeDivider}
  </td></tr>
  <tr><td style="padding:0 48px 36px;">
    <img src="${SIGNATURE_IMAGE}" alt="Timothy Pappas" style="max-width:400px;width:100%;height:auto;">
  </td></tr>
  <tr><td style="background:#2e403d;padding:28px 40px;text-align:center;">
    <p style="margin:0 0 14px;font-size:13px;color:#a3b8a0;">Questions? Reply to this email or call <a href="tel:4408867318" style="color:#c9dd80;font-weight:600;text-decoration:none;">(440) 886-7318</a></p>
    <table cellpadding="0" cellspacing="0" style="margin:0 auto 16px;">
      <tr>
        <td style="padding:0 8px;"><a href="https://www.facebook.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_FB_WHITE}" alt="Facebook" style="width:28px;height:28px;"></a></td>
        <td style="padding:0 8px;"><a href="https://www.instagram.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_IG_WHITE}" alt="Instagram" style="width:28px;height:28px;"></a></td>
        <td style="padding:0 8px;"><a href="https://nextdoor.com/profile/01ZjZkwxhPWdnML2k" style="text-decoration:none;"><img src="${SOCIAL_ND_WHITE}" alt="Nextdoor" style="width:28px;height:28px;"></a></td>
      </tr>
    </table>
    <p style="margin:0 0 3px;font-size:12px;color:#7a9477;">Pappas & Co. Landscaping</p>
    <p style="margin:0 0 3px;font-size:11px;color:#5a7a57;">PO Box 770057 &bull; Lakewood, Ohio 44107</p>
    <p style="margin:0 0 10px;font-size:11px;"><a href="https://pappaslandscaping.com" style="color:#c9dd80;text-decoration:none;">pappaslandscaping.com</a></p>
    <p style="margin:0;font-size:10px;color:#5a7a57;"><a href="${baseUrl}/unsubscribe.html?email={unsubscribe_email}" style="color:#7a9477;text-decoration:underline;">Unsubscribe</a> from marketing emails</p>
  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>`;
  }

  const templates = {
    1: {
      subject: `Quick follow-up on your quote, Pappas & Co. Landscaping`,
      html: followupTemplate(headingImages[1], `
        <p style="${bs}">Hi ${followup.customer_name},</p>
        <p style="${bs}">Thanks for giving us the chance to put together a quote for you! I just wanted to check in and see if you had any questions about the services or pricing. We'd really love the opportunity to help with your property this season. Feel free to reply here or call us anytime.</p>
        ${quoteRef}
        ${ctaButton}
      `)
    },
    2: {
      subject: `Your quote is still available, Pappas & Co.`,
      html: followupTemplate(headingImages[2], `
        <p style="${bs}">Hi ${followup.customer_name},</p>
        <p style="${bs}">Just wanted to let you know your quote is still available whenever you're ready. If anything needs adjusting or you want to talk through the details, we're happy to help. We'd love to get you on the schedule!</p>
        ${quoteRef}
        ${ctaButton}
      `)
    },
    3: {
      subject: `Checking in on your quote, Pappas & Co.`,
      html: followupTemplate(headingImages[3], `
        <p style="${bs}">Hi ${followup.customer_name},</p>
        <p style="${bs}">It's been a couple weeks so I just wanted to touch base one more time. If the timing isn't right or you'd like to change anything about the quote, no problem at all. We're here whenever you're ready and would love the chance to work with you.</p>
        ${quoteRef}
        ${ctaButton}
      `)
    },
    4: {
      subject: `Your quote expires soon, Pappas & Co. Landscaping`,
      html: followupTemplate(headingImages[4], `
        <p style="${bs}">Hi ${followup.customer_name},</p>
        <p style="${bs}">Just a heads up that your quote will expire in about <strong>5 days</strong>. After that, pricing may change depending on our availability. If you'd like to lock in your rate, just let us know and we'll get you on the calendar right away!</p>
        <p style="font-size:13px;color:#92400e;text-align:center;margin:18px 0 0;letter-spacing:0.3px;">Quote #${followup.quote_number || 'N/A'} &nbsp;&middot;&nbsp; $${parseFloat(followup.quote_amount || 0).toFixed(2)}</p>
        ${ctaButton}
      `)
    }
  };
  return templates[stage];
}

// Follow-up SMS templates
function getFollowupSMS(followup, stage) {
  const templates = {
    2: `Hi ${followup.customer_name}, just wanted to let you know your quote is still available whenever you're ready. Any questions at all, we're here to help! View your quote: ${followup.sign_url || ''} - Tim, Pappas & Co.`,
    3: `Hi ${followup.customer_name}, just touching base one more time on your quote. We'd love the chance to work with you! View your quote: ${followup.sign_url || ''} - Tim, Pappas & Co.`,
    4: `Hi ${followup.customer_name}, heads up, your quote expires in about 5 days. Lock in your rate here: ${followup.sign_url || ''} - Tim, Pappas & Co.`
  };
  return templates[stage];
}

// GET /api/preview-followup-emails - Send all 4 follow-up email previews to owner
app.get('/api/preview-followup-emails', async (req, res) => {
  try {
    const previewFollowup = {
      customer_name: 'Jane Smith',
      quote_number: '1234',
      quote_amount: '2450.00',
      sign_url: 'https://pappaslandscaping.com/sign-contract.html?id=preview',
      customer_email: 'hello@pappaslandscaping.com',
      customer_phone: '(216) 555-0100'
    };

    for (let stage = 1; stage <= 4; stage++) {
      if (stage > 1) await new Promise(r => setTimeout(r, 2000)); // 2s delay between sends
      const email = getFollowupEmailContent(previewFollowup, stage);
      if (email) {
        await sendEmail(
          'hello@pappaslandscaping.com',
          `[PREVIEW ${stage}/4] ${email.subject}`,
          email.html
        );
      }
    }

    res.json({ success: true, message: 'All 4 follow-up email previews sent to hello@pappaslandscaping.com' });
  } catch (error) {
    console.error('Error sending preview emails:', error);
    serverError(res, error);
  }
});

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
        
        // Send email (only if automated emails are enabled)
        const emailsOn = await areAutomatedEmailsEnabled();
        const emailContent = getFollowupEmailContent(followup, stage);
        if (emailsOn && emailContent && followup.customer_email) {
          const finalFollowupHtml = emailContent.html.replace(/\{unsubscribe_email\}/g, encodeURIComponent(followup.customer_email));
          await sendEmail(followup.customer_email, emailContent.subject, finalFollowupHtml, null, { type: 'followup', customer_name: followup.customer_name, quote_id: followup.quote_id });
          results.emails_sent++;
          console.log(`📧 Email sent to ${followup.customer_email} (Stage ${stage})`);
        } else if (!emailsOn) {
          console.log(`📧 Email SKIPPED (automated emails OFF) for ${followup.customer_email} (Stage ${stage})`);
        }

        // Send SMS for stages 2-4 (only if automated emails are enabled)
        if (emailsOn && stage >= 2 && followup.customer_phone) {
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
    serverError(res, error);
  }
});

// GET /api/setup-quote-followups - Run once to ensure table exists
app.get('/api/setup-quote-followups', async (req, res) => {
  try {
    await createQuoteFollowupsTable();
    res.json({ success: true, message: 'Quote followups table ready!' });
  } catch (error) {
    serverError(res, error);
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
        
        // Send email (only if automated emails are enabled)
        const emailsOn = await areAutomatedEmailsEnabled();
        const emailContent = getFollowupEmailContent(followup, stage);
        if (emailsOn && emailContent && followup.customer_email) {
          const finalFollowupHtml = emailContent.html.replace(/\{unsubscribe_email\}/g, encodeURIComponent(followup.customer_email));
          await sendEmail(followup.customer_email, emailContent.subject, finalFollowupHtml, null, { type: 'followup', customer_name: followup.customer_name, quote_id: followup.quote_id });
          results.emails_sent++;
          console.log(`📧 Email sent to ${followup.customer_email} (Stage ${stage})`);
        } else if (!emailsOn) {
          console.log(`📧 Email SKIPPED (automated emails OFF) for ${followup.customer_email} (Stage ${stage})`);
        }

        // Send SMS for stages 2-4 (only if automated emails are enabled)
        if (emailsOn && stage >= 2 && followup.customer_phone) {
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
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ BUSINESS SETTINGS ══════════════════════════════════
// ═══════════════════════════════════════════════════════════

// GET /api/settings - Retrieve all settings
app.get('/api/settings', async (req, res) => {
  try {
    const result = await pool.query('SELECT key, value FROM business_settings ORDER BY key');
    const settings = {};
    for (const row of result.rows) settings[row.key] = row.value;
    res.json({ success: true, settings });
  } catch (error) { serverError(res, error); }
});

// PATCH /api/settings/:key - Update a setting
app.patch('/api/settings/:key', async (req, res) => {
  try {
    const { value } = req.body;
    if (value === undefined) return res.status(400).json({ success: false, error: 'value required' });
    const result = await pool.query(
      `INSERT INTO business_settings (key, value, updated_at) VALUES ($1, $2, NOW())
       ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW() RETURNING *`,
      [req.params.key, JSON.stringify(value)]
    );
    res.json({ success: true, setting: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// POST /api/tax/calculate - Calculate tax for line items
app.post('/api/tax/calculate', async (req, res) => {
  try {
    const { customer_id, property_id, line_items } = req.body;
    const result = await calculateTax(customer_id || null, property_id || null, line_items || []);
    res.json({ success: true, ...result });
  } catch(e) {
    serverError(res, e);
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ BULK WAIVE & APOLOGY EMAIL ═════════════════════════════
// ═══════════════════════════════════════════════════════════

// POST /api/late-fees/bulk-waive-today - Waive all late fees created today and send apology
app.post('/api/late-fees/bulk-waive-today', async (req, res) => {
  try {
    // 1. Find all late fees created today
    const todayFees = await pool.query(`
      SELECT lf.*, i.customer_email, i.customer_name, i.invoice_number
      FROM late_fees lf
      LEFT JOIN invoices i ON lf.invoice_id = i.id
      WHERE lf.created_at::date = CURRENT_DATE AND lf.waived = false
    `);

    console.log(`🔧 Bulk waiving ${todayFees.rows.length} late fees from today...`);

    // 2. Waive them all
    const waiveResult = await pool.query(`
      UPDATE late_fees SET waived = true, waived_at = NOW(), waived_by = 'system-rollback'
      WHERE created_at::date = CURRENT_DATE AND waived = false
      RETURNING id, invoice_id
    `);

    // 3. Recalculate invoice totals
    const invoiceIds = [...new Set(waiveResult.rows.map(r => r.invoice_id))];
    for (const invId of invoiceIds) {
      const totals = await pool.query('SELECT COALESCE(SUM(fee_amount), 0) as total FROM late_fees WHERE invoice_id = $1 AND waived = false', [invId]);
      await pool.query('UPDATE invoices SET late_fee_total = $1, updated_at = NOW() WHERE id = $2', [totals.rows[0].total, invId]);
    }

    // 4. Send apology email to each unique customer
    const uniqueEmails = {};
    for (const fee of todayFees.rows) {
      if (fee.customer_email && !uniqueEmails[fee.customer_email]) {
        uniqueEmails[fee.customer_email] = {
          email: fee.customer_email,
          name: fee.customer_name
        };
      }
    }

    let emailsSent = 0;
    let emailErrors = 0;
    const sendApology = req.body.send_apology !== false; // default true

    if (sendApology) {
      for (const cust of Object.values(uniqueEmails)) {
        try {
          const firstName = (cust.name || '').split(' ')[0] || 'Valued Customer';
          const content = `
            <h2 style="color:#2e403d;margin:0 0 16px;">Our Apologies — Please Disregard</h2>
            <p>Hi ${firstName},</p>
            <p>You may have recently received an email from Pappas & Co. Landscaping regarding a <strong>late fee notification</strong>. We sincerely apologize — <strong>this was sent in error</strong> during an internal system test.</p>
            <div style="background:#ecfdf5;border-radius:8px;padding:16px;margin:20px 0;border-left:4px solid #059669;">
              <p style="margin:0;font-weight:700;color:#059669;">No action is needed on your part.</p>
              <p style="margin:8px 0 0;color:#333;">Your account has not been affected. Any late fees referenced in that email have been removed from your account.</p>
            </div>
            <p>We take the accuracy of our communications seriously and are taking steps to ensure this does not happen again.</p>
            <p>If you have any questions or concerns, please don't hesitate to reach out to us directly.</p>
            <p>Thank you for your patience and understanding.</p>
            <p style="margin-top:24px;">Warm regards,<br><strong>Tim Pappas</strong><br>Pappas & Co. Landscaping<br>440-886-7318</p>
          `;
          await sendEmail(cust.email, 'Our Apologies — Please Disregard Previous Email', emailTemplate(content), null, { type: 'apology', customer_id: cust.customer_id, customer_name: cust.name });
          emailsSent++;
        } catch (err) {
          emailErrors++;
          console.error(`Failed to send apology to ${cust.email}:`, err.message);
        }
      }
    }

    console.log(`✅ Bulk waive complete: ${waiveResult.rowCount} fees waived, ${invoiceIds.length} invoices updated, ${emailsSent} apology emails sent`);

    res.json({
      success: true,
      fees_waived: waiveResult.rowCount,
      invoices_updated: invoiceIds.length,
      unique_customers: Object.keys(uniqueEmails).length,
      apology_emails_sent: emailsSent,
      apology_email_errors: emailErrors
    });
  } catch (error) {
    console.error('Bulk waive error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ LATE FEES ══════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════

// GET /api/late-fees - List late fees with filters
app.get('/api/late-fees', async (req, res) => {
  try {
    const { invoice_id, waived } = req.query;
    let query = `SELECT lf.*, i.invoice_number, i.customer_name FROM late_fees lf LEFT JOIN invoices i ON lf.invoice_id = i.id WHERE 1=1`;
    const params = [];
    let p = 1;
    if (invoice_id) { query += ` AND lf.invoice_id = $${p++}`; params.push(invoice_id); }
    if (waived !== undefined) { query += ` AND lf.waived = $${p++}`; params.push(waived === 'true'); }
    query += ' ORDER BY lf.created_at DESC';
    const result = await pool.query(query, params);
    res.json({ success: true, lateFees: result.rows });
  } catch (error) { serverError(res, error); }
});

// POST /api/late-fees/:id/waive - Waive a late fee
app.post('/api/late-fees/:id/waive', async (req, res) => {
  try {
    const fee = await pool.query('UPDATE late_fees SET waived = true, waived_at = NOW(), waived_by = $1 WHERE id = $2 RETURNING *', [req.body.waived_by || 'admin', req.params.id]);
    if (fee.rows.length === 0) return res.status(404).json({ success: false, error: 'Fee not found' });
    // Recalculate invoice late_fee_total
    const totals = await pool.query('SELECT COALESCE(SUM(fee_amount), 0) as total FROM late_fees WHERE invoice_id = $1 AND waived = false', [fee.rows[0].invoice_id]);
    await pool.query('UPDATE invoices SET late_fee_total = $1, updated_at = NOW() WHERE id = $2', [totals.rows[0].total, fee.rows[0].invoice_id]);
    res.json({ success: true, fee: fee.rows[0], new_total: parseFloat(totals.rows[0].total) });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// ═══ RECURRING JOBS ═════════════════════════════════════════
// ═══════════════════════════════════════════════════════════

// GET /api/jobs/recurring - List recurring job templates
app.get('/api/jobs/recurring', async (req, res) => {
  try {
    // Check if is_recurring column exists before querying
    const colCheck = await pool.query(`SELECT column_name FROM information_schema.columns WHERE table_name = 'scheduled_jobs' AND column_name = 'is_recurring'`);
    if (colCheck.rows.length === 0) {
      return res.json({ success: true, jobs: [] });
    }
    const result = await pool.query(`SELECT * FROM scheduled_jobs WHERE is_recurring = true ORDER BY customer_name ASC`);
    res.json({ success: true, jobs: result.rows });
  } catch (error) { serverError(res, error); }
});

// PATCH /api/jobs/:id/recurring - Configure recurring pattern
app.patch('/api/jobs/:id/recurring', async (req, res) => {
  try {
    const { is_recurring, recurring_pattern, recurring_end_date } = req.body;
    const result = await pool.query(
      `UPDATE scheduled_jobs SET is_recurring = COALESCE($1, is_recurring), recurring_pattern = COALESCE($2, recurring_pattern), recurring_end_date = COALESCE($3, recurring_end_date), updated_at = NOW() WHERE id = $4 RETURNING *`,
      [is_recurring, recurring_pattern, recurring_end_date, req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Job not found' });
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// ═══ AUTOMATED EMAIL KILL SWITCH ═════════════════════════════
// ═══════════════════════════════════════════════════════════

async function areAutomatedEmailsEnabled() {
  try {
    const result = await pool.query("SELECT value FROM business_settings WHERE key = 'automated_emails_enabled'");
    if (result.rows.length === 0) return false; // Default OFF for safety
    const val = result.rows[0].value;
    return val === true || val === 'true';
  } catch (err) {
    console.log('⚠️ Could not check automated_emails_enabled setting, defaulting to OFF');
    return false;
  }
}

// ═══ DAILY AUTOMATION CRON ══════════════════════════════════
// ═══════════════════════════════════════════════════════════

async function processRecurringJobs() {
  const results = { generated: 0, skipped: 0, errors: [] };
  try {
    const templates = await pool.query(`SELECT * FROM scheduled_jobs WHERE is_recurring = true AND (recurring_end_date IS NULL OR recurring_end_date >= CURRENT_DATE)`);
    for (const job of templates.rows) {
      try {
        const pattern = job.recurring_pattern || 'weekly';
        const lookAheadDays = 14;
        for (let d = 0; d <= lookAheadDays; d++) {
          const targetDate = new Date();
          targetDate.setDate(targetDate.getDate() + d);
          const dateStr = targetDate.toISOString().split('T')[0];
          const dayOfWeek = targetDate.getDay();
          let shouldGenerate = false;
          if (pattern === 'weekly' && dayOfWeek === new Date(job.job_date).getDay()) shouldGenerate = true;
          else if (pattern === 'biweekly') {
            const weeksDiff = Math.floor((targetDate - new Date(job.job_date)) / (7 * 24 * 60 * 60 * 1000));
            if (weeksDiff >= 0 && weeksDiff % 2 === 0 && dayOfWeek === new Date(job.job_date).getDay()) shouldGenerate = true;
          }
          else if (pattern === 'monthly' && targetDate.getDate() === new Date(job.job_date).getDate()) shouldGenerate = true;
          if (!shouldGenerate) continue;
          // Dedup check
          const existing = await pool.query('SELECT id FROM recurring_job_log WHERE source_job_id = $1 AND generated_for_date = $2', [job.id, dateStr]);
          if (existing.rows.length > 0) { results.skipped++; continue; }
          // Generate job
          const newJob = await pool.query(
            `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, estimated_duration, crew_assigned, parent_job_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending', $11, $12, $13) RETURNING id`,
            [dateStr, job.customer_name, job.customer_id, job.service_type, job.service_frequency, job.service_price, job.address, job.phone, job.special_notes, job.property_notes, job.estimated_duration, job.crew_assigned, job.id]
          );
          await pool.query('INSERT INTO recurring_job_log (source_job_id, generated_for_date, generated_job_id) VALUES ($1, $2, $3)', [job.id, dateStr, newJob.rows[0].id]);
          results.generated++;
        }
      } catch (err) { results.errors.push({ job_id: job.id, error: err.message }); }
    }
  } catch (err) { results.errors.push({ error: err.message }); }
  return results;
}

async function processMonthlyPlanInvoices() {
  const results = { generated: 0, skipped: 0, sent: 0, errors: [] };
  try {
    const billingMonth = new Date().toISOString().slice(0, 7); // YYYY-MM
    const customers = await pool.query(`SELECT * FROM customers WHERE monthly_plan_amount > 0`);
    for (const cust of customers.rows) {
      try {
        // Dedup check
        const existing = await pool.query('SELECT id FROM recurring_invoice_log WHERE customer_id = $1 AND billing_month = $2', [cust.id, billingMonth]);
        if (existing.rows.length > 0) { results.skipped++; continue; }
        await ensureInvoicesTable();
        const invNum = await nextInvoiceNumber();
        const total = parseFloat(cust.monthly_plan_amount);
        const paymentToken = generateToken();
        const inv = await pool.query(
          `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, status, subtotal, total, due_date, is_auto_generated, auto_gen_source, billing_month, payment_token, payment_token_created_at, line_items)
           VALUES ($1, $2, $3, $4, 'sent', $5, $5, CURRENT_DATE, true, 'monthly_plan', $6, $7, NOW(), $8) RETURNING *`,
          [invNum, cust.id, cust.name, cust.email, total, billingMonth, paymentToken, JSON.stringify([{description: 'Monthly Lawn Care Plan - ' + billingMonth, amount: total}])]
        );
        await pool.query('INSERT INTO recurring_invoice_log (customer_id, billing_month, invoice_id) VALUES ($1, $2, $3)', [cust.id, billingMonth, inv.rows[0].id]);
        results.generated++;
        // Auto-send with payment link (only if automated emails are enabled)
        const emailsOn = await areAutomatedEmailsEnabled();
        if (emailsOn && cust.email) {
          const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
          const payUrl = `${baseUrl}/pay-invoice.html?token=${paymentToken}`;
          const content = `
            <h2 style="color:#2e403d;margin:0 0 16px;">Monthly Invoice ${invNum}</h2>
            <p>Hi ${(cust.name || '').split(' ')[0]},</p>
            <p>Your monthly lawn care invoice for <strong>${new Date().toLocaleDateString('en-US', {month:'long',year:'numeric'})}</strong> is ready.</p>
            <div style="background:#f8fafc;border-radius:8px;padding:20px;margin:20px 0;text-align:center;">
              <p style="font-size:28px;font-weight:700;color:#2e403d;margin:0;">$${total.toFixed(2)}</p>
              <p style="color:#666;margin:4px 0;">Monthly Lawn Care Plan</p>
            </div>
            <div style="text-align:center;margin:28px 0;">
              <a href="${payUrl}" style="display:inline-block;padding:16px 40px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">Pay Now</a>
            </div>
          `;
          await sendEmail(cust.email, `Monthly Invoice ${invNum} — $${total.toFixed(2)}`, emailTemplate(content), null, { type: 'invoice', customer_id: cust.id, customer_name: cust.name, invoice_id: inv.rows[0].id });
          await pool.query("UPDATE invoices SET sent_at = NOW() WHERE id = $1", [inv.rows[0].id]);
          results.sent++;
        }
      } catch (err) { results.errors.push({ customer_id: cust.id, error: err.message }); }
    }
  } catch (err) { results.errors.push({ error: err.message }); }
  return results;
}

async function processLateFees() {
  const results = { applied: 0, emails_sent: 0, errors: [] };
  try {
    const settingsResult = await pool.query("SELECT value FROM business_settings WHERE key = 'late_fee_rules'");
    const rules = settingsResult.rows.length > 0 ? settingsResult.rows[0].value : { grace_period_days: 30, initial_fee_percent: 10, recurring_fee_percent: 5, recurring_interval_days: 30, max_fees: 3, enabled: true };
    if (!rules.enabled) return results;

    const overdue = await pool.query(`
      SELECT i.*, (SELECT COUNT(*) FROM late_fees WHERE invoice_id = i.id AND waived = false) as fee_count
      FROM invoices i
      WHERE i.status IN ('sent', 'overdue', 'partial') AND i.amount_paid < i.total AND i.due_date IS NOT NULL
        AND i.due_date < CURRENT_DATE - ($1 || ' days')::interval
    `, [rules.grace_period_days]);

    for (const inv of overdue.rows) {
      try {
        const daysOverdue = Math.floor((Date.now() - new Date(inv.due_date).getTime()) / (24 * 60 * 60 * 1000));
        const feeCount = parseInt(inv.fee_count);
        if (feeCount >= (rules.max_fees || 3)) continue;
        // Check if fee already applied for this period
        const expectedFees = Math.min(Math.floor(daysOverdue / (rules.recurring_interval_days || 30)), rules.max_fees || 3);
        if (feeCount >= expectedFees) continue;
        const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
        const feePercent = feeCount === 0 ? (rules.initial_fee_percent || 10) : (rules.recurring_fee_percent || 5);
        const feeAmount = Math.round(balance * feePercent) / 100;
        await pool.query(
          'INSERT INTO late_fees (invoice_id, fee_amount, fee_type, fee_percentage, days_overdue) VALUES ($1, $2, $3, $4, $5)',
          [inv.id, feeAmount, 'percentage', feePercent, daysOverdue]
        );
        const newTotal = await pool.query('SELECT COALESCE(SUM(fee_amount), 0) as total FROM late_fees WHERE invoice_id = $1 AND waived = false', [inv.id]);
        await pool.query("UPDATE invoices SET late_fee_total = $1, status = 'overdue', updated_at = NOW() WHERE id = $2", [newTotal.rows[0].total, inv.id]);
        results.applied++;
        // Send late fee email (only if automated emails are enabled)
        const emailsOn = await areAutomatedEmailsEnabled();
        if (emailsOn && inv.customer_email) {
          const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
          const payUrl = inv.payment_token ? `${baseUrl}/pay-invoice.html?token=${inv.payment_token}` : '';
          const content = `
            <h2 style="color:#dc4a4a;margin:0 0 16px;">Late Fee Applied</h2>
            <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
            <p>A <strong>${feePercent}% late fee ($${feeAmount.toFixed(2)})</strong> has been applied to invoice <strong>${inv.invoice_number}</strong>, which is now <strong>${daysOverdue} days past due</strong>.</p>
            <div style="background:#fef0f0;border-radius:8px;padding:16px;margin:16px 0;border-left:4px solid #dc4a4a;">
              <p style="margin:0;"><strong>Original Balance:</strong> $${balance.toFixed(2)}</p>
              <p style="margin:4px 0 0;"><strong>Late Fee:</strong> $${feeAmount.toFixed(2)}</p>
              <p style="margin:4px 0 0;font-size:18px;font-weight:700;color:#dc4a4a;">New Balance: $${(balance + feeAmount).toFixed(2)}</p>
            </div>
            ${payUrl ? `<div style="text-align:center;margin:28px 0;"><a href="${payUrl}" style="display:inline-block;padding:16px 40px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">Pay Now</a></div>` : ''}
            <p>To avoid additional fees, please pay as soon as possible. If you have questions, reply to this email or call us.</p>
          `;
          await sendEmail(inv.customer_email, `Late Fee Applied — Invoice ${inv.invoice_number}`, emailTemplate(content), null, { type: 'late_fee', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });
          results.emails_sent++;
        }
      } catch (err) { results.errors.push({ invoice_id: inv.id, error: err.message }); }
    }
  } catch (err) { results.errors.push({ error: err.message }); }
  return results;
}

// POST /api/cron/daily-automation - Combined daily cron
app.post('/api/cron/daily-automation', async (req, res) => {
  try {
    const emailsOn = await areAutomatedEmailsEnabled();
    console.log(`🔄 Running daily automation... (automated emails: ${emailsOn ? 'ON' : 'OFF'})`);
    const [recurringJobs, monthlyInvoices, lateFees] = await Promise.all([
      processRecurringJobs(),
      processMonthlyPlanInvoices(),
      processLateFees()
    ]);
    const result = { recurringJobs, monthlyInvoices, lateFees, automated_emails_enabled: emailsOn, timestamp: new Date().toISOString() };
    console.log('✅ Daily automation complete:', JSON.stringify(result));
    res.json({ success: true, ...result });
  } catch (error) {
    console.error('Daily automation error:', error);
    serverError(res, error);
  }
});

// GET /api/cron/daily-automation - Allow GET for cron-job.org
app.get('/api/cron/daily-automation', async (req, res) => {
  try {
    const emailsOn = await areAutomatedEmailsEnabled();
    console.log(`🔄 Running daily automation (GET)... (automated emails: ${emailsOn ? 'ON' : 'OFF'})`);
    const [recurringJobs, monthlyInvoices, lateFees] = await Promise.all([
      processRecurringJobs(),
      processMonthlyPlanInvoices(),
      processLateFees()
    ]);
    const result = { recurringJobs, monthlyInvoices, lateFees, automated_emails_enabled: emailsOn, timestamp: new Date().toISOString() };
    console.log('✅ Daily automation complete:', JSON.stringify(result));
    res.json({ success: true, ...result });
  } catch (error) {
    console.error('Daily automation error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ KPI DASHBOARD ═══════════════════════════════════════
// ═══════════════════════════════════════════════════════════

function generateCoachingSuggestions(metrics) {
  const suggestions = [];
  if (metrics.closeRatio && metrics.closeRatio.value < 50) {
    suggestions.push({ severity: 'warning', title: 'Close ratio below 50%', suggestion: 'Follow up faster with personal calls within 24 hours of sending quotes. Consider offering limited-time discounts to create urgency.' });
  } else if (metrics.closeRatio && metrics.closeRatio.value > 70) {
    suggestions.push({ severity: 'success', title: 'Excellent close ratio!', suggestion: 'Your pricing may be too low. Consider raising prices 5-10% — you can still close deals while increasing margins.' });
  }
  if (metrics.customerAcquisitionCost && metrics.customerAcquisitionCost.value > 100) {
    suggestions.push({ severity: 'warning', title: 'High customer acquisition cost', suggestion: 'Focus on referral programs and Google reviews. Ask happy customers for referrals — they cost almost nothing.' });
  }
  if (metrics.arAging && (metrics.arAging.days60 + metrics.arAging.days90plus) > 2000) {
    suggestions.push({ severity: 'danger', title: 'High accounts receivable', suggestion: 'Enable automatic late fees and send payment reminders. Consider requiring deposits for new customers.' });
  }
  if (metrics.monthlyRecurringRevenue && metrics.monthlyRecurringRevenue.customerCount < 10) {
    suggestions.push({ severity: 'info', title: 'Low recurring revenue', suggestion: 'Convert more customers to monthly plans. Offer a 5% discount for monthly plan customers to encourage sign-ups.' });
  }
  if (metrics.laborEfficiency && metrics.laborEfficiency.value < 80) {
    suggestions.push({ severity: 'warning', title: 'Low labor efficiency', suggestion: 'Optimize routing to reduce drive time. Group nearby jobs on the same day and use the dispatch board for planning.' });
  }
  return suggestions;
}

app.get('/api/kpi/dashboard', async (req, res) => {
  try {
    const period = req.query.period || 'month';
    let dateFilter;
    if (period === 'week') dateFilter = "NOW() - INTERVAL '7 days'";
    else if (period === 'month') dateFilter = "NOW() - INTERVAL '1 month'";
    else if (period === 'quarter') dateFilter = "NOW() - INTERVAL '3 months'";
    else dateFilter = "NOW() - INTERVAL '1 year'";

    await ensureInvoicesTable();

    const [closeRatio, avgJobValue, revenuePerCrew, cac, laborEff, mrr, arAging, clv, outstandingTrend] = await Promise.all([
      // Close ratio
      pool.query(`SELECT
        COUNT(CASE WHEN status IN ('signed','contracted') THEN 1 END) as signed,
        COUNT(CASE WHEN status != 'draft' THEN 1 END) as sent
        FROM sent_quotes WHERE created_at >= ${dateFilter}`),
      // Avg job value
      pool.query(`SELECT COALESCE(AVG(service_price), 0) as value, COUNT(*) as count
        FROM scheduled_jobs WHERE status = 'completed' AND updated_at >= ${dateFilter}`),
      // Revenue per crew
      pool.query(`SELECT crew_assigned as crew_name, COALESCE(SUM(service_price), 0) as revenue, COUNT(*) as jobs
        FROM scheduled_jobs WHERE status = 'completed' AND crew_assigned IS NOT NULL AND updated_at >= ${dateFilter}
        GROUP BY crew_assigned ORDER BY revenue DESC`),
      // Customer acquisition cost
      pool.query(`SELECT
        COALESCE((SELECT SUM(amount) FROM expenses WHERE category ILIKE '%marketing%' AND expense_date >= (${dateFilter})::date), 0) as marketing,
        (SELECT COUNT(*) FROM customers WHERE created_at >= ${dateFilter}) as new_customers`),
      // Labor efficiency
      pool.query(`SELECT
        COALESCE(SUM(service_price), 0) as revenue,
        COALESCE(SUM(estimated_duration), 0) / 60.0 as hours
        FROM scheduled_jobs WHERE status = 'completed' AND updated_at >= ${dateFilter}`),
      // Monthly recurring revenue
      pool.query(`SELECT COALESCE(SUM(monthly_plan_amount), 0) as value, COUNT(*) as customer_count FROM customers WHERE monthly_plan_amount > 0`),
      // AR aging
      pool.query(`SELECT
        COALESCE(SUM(CASE WHEN due_date >= CURRENT_DATE THEN total - amount_paid END), 0) as current,
        COALESCE(SUM(CASE WHEN due_date < CURRENT_DATE AND due_date >= CURRENT_DATE - 30 THEN total - amount_paid END), 0) as days30,
        COALESCE(SUM(CASE WHEN due_date < CURRENT_DATE - 30 AND due_date >= CURRENT_DATE - 60 THEN total - amount_paid END), 0) as days60,
        COALESCE(SUM(CASE WHEN due_date < CURRENT_DATE - 60 THEN total - amount_paid END), 0) as days90plus
        FROM invoices WHERE status IN ('sent','overdue','partial') AND amount_paid < total`),
      // Customer lifetime value
      pool.query(`SELECT
        COALESCE(SUM(amount_paid), 0) as total_revenue,
        (SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '1 year') as active_customers
        FROM invoices WHERE status = 'paid'`),
      // Outstanding trend (6 months)
      pool.query(`SELECT to_char(date_trunc('month', created_at), 'YYYY-MM') as month,
        COALESCE(SUM(total - amount_paid), 0) as amount
        FROM invoices WHERE status IN ('sent','overdue','partial') AND created_at >= NOW() - INTERVAL '6 months'
        GROUP BY date_trunc('month', created_at) ORDER BY month`)
    ]);

    const cr = closeRatio.rows[0];
    const cacRow = cac.rows[0];
    const leRow = laborEff.rows[0];
    const clvRow = clv.rows[0];

    const metrics = {
      closeRatio: { value: cr.sent > 0 ? Math.round((cr.signed / cr.sent) * 100) : 0, signed: parseInt(cr.signed), sent: parseInt(cr.sent) },
      avgJobValue: { value: parseFloat(avgJobValue.rows[0].value), count: parseInt(avgJobValue.rows[0].count) },
      revenuePerCrew: revenuePerCrew.rows.map(r => ({ crew_name: r.crew_name, revenue: parseFloat(r.revenue), jobs: parseInt(r.jobs) })),
      customerAcquisitionCost: { value: cacRow.new_customers > 0 ? parseFloat(cacRow.marketing) / parseInt(cacRow.new_customers) : 0, marketing: parseFloat(cacRow.marketing), newCustomers: parseInt(cacRow.new_customers) },
      laborEfficiency: { value: leRow.hours > 0 ? parseFloat(leRow.revenue) / parseFloat(leRow.hours) : 0, revenue: parseFloat(leRow.revenue), hours: parseFloat(leRow.hours) },
      monthlyRecurringRevenue: { value: parseFloat(mrr.rows[0].value), customerCount: parseInt(mrr.rows[0].customer_count) },
      arAging: { current: parseFloat(arAging.rows[0].current), days30: parseFloat(arAging.rows[0].days30), days60: parseFloat(arAging.rows[0].days60), days90plus: parseFloat(arAging.rows[0].days90plus) },
      customerLifetimeValue: { value: clvRow.active_customers > 0 ? parseFloat(clvRow.total_revenue) / parseInt(clvRow.active_customers) : 0, totalRevenue: parseFloat(clvRow.total_revenue), activeCustomers: parseInt(clvRow.active_customers) },
      outstandingTrend: outstandingTrend.rows.map(r => ({ month: r.month, amount: parseFloat(r.amount) }))
    };

    res.json({ success: true, metrics, coaching: generateCoachingSuggestions(metrics), period });
  } catch (error) {
    console.error('KPI dashboard error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ DISPATCH BOARD ═════════════════════════════════════════
// ═══════════════════════════════════════════════════════════

// GET /api/dispatch/board - Jobs grouped by crew
app.get('/api/dispatch/board', async (req, res) => {
  try {
    const { date, view = 'day' } = req.query;
    const targetDate = date || new Date().toISOString().split('T')[0];
    let dateCondition, params;
    if (view === 'week') {
      // Get Monday of the week
      const d = new Date(targetDate);
      const day = d.getDay();
      const monday = new Date(d);
      monday.setDate(d.getDate() - (day === 0 ? 6 : day - 1));
      const sunday = new Date(monday);
      sunday.setDate(monday.getDate() + 6);
      dateCondition = `sj.job_date::date BETWEEN $1::date AND $2::date`;
      params = [monday.toISOString().split('T')[0], sunday.toISOString().split('T')[0]];
    } else {
      dateCondition = `sj.job_date::date = $1::date`;
      params = [targetDate];
    }
    const jobs = await pool.query(`SELECT sj.*, sj.lat::float as lat, sj.lng::float as lng,
       c.street AS cust_street, c.city AS cust_city, c.state AS cust_state, c.postal_code AS cust_zip
       FROM scheduled_jobs sj
       LEFT JOIN customers c ON sj.customer_id = c.id
       WHERE ${dateCondition} ORDER BY sj.route_order ASC NULLS LAST, sj.customer_name`, params);
    const crews = await pool.query('SELECT * FROM crews ORDER BY name');

    // Group jobs by crew
    const crewMap = {};
    for (const crew of crews.rows) {
      crewMap[crew.name] = { id: crew.id, name: crew.name, members: crew.members || '', color: crew.color || '#059669', jobs: [], totalHours: 0, jobCount: 0 };
    }
    const unassigned = [];
    for (const job of jobs.rows) {
      if (job.crew_assigned && crewMap[job.crew_assigned]) {
        crewMap[job.crew_assigned].jobs.push(job);
        crewMap[job.crew_assigned].totalHours += (job.estimated_duration || 30) / 60;
        crewMap[job.crew_assigned].jobCount++;
      } else if (job.crew_assigned) {
        // Crew exists in job but not in crews table — create entry
        if (!crewMap[job.crew_assigned]) crewMap[job.crew_assigned] = { id: null, name: job.crew_assigned, members: '', color: '#6e726e', jobs: [], totalHours: 0, jobCount: 0 };
        crewMap[job.crew_assigned].jobs.push(job);
        crewMap[job.crew_assigned].totalHours += (job.estimated_duration || 30) / 60;
        crewMap[job.crew_assigned].jobCount++;
      } else {
        unassigned.push(job);
      }
    }
    res.json({ success: true, date: targetDate, view, crews: Object.values(crewMap), unassigned });
  } catch (error) { serverError(res, error); }
});

// PATCH /api/dispatch/assign - Batch reassignment (supports crew, route_order, status, job_date)
app.patch('/api/dispatch/assign', async (req, res) => {
  try {
    const { assignments } = req.body;
    if (!assignments || !Array.isArray(assignments)) return res.status(400).json({ success: false, error: 'assignments array required' });
    const updated = [];
    for (const a of assignments) {
      const sets = [];
      const vals = [];
      let idx = 1;
      if (a.crew_assigned !== undefined) { sets.push(`crew_assigned = $${idx++}`); vals.push(a.crew_assigned); }
      if (a.route_order !== undefined) { sets.push(`route_order = $${idx++}`); vals.push(a.route_order || null); }
      if (a.status !== undefined) { sets.push(`status = $${idx++}`); vals.push(a.status); }
      if (a.job_date !== undefined) { sets.push(`job_date = $${idx++}`); vals.push(a.job_date); }
      sets.push('updated_at = NOW()');
      if (sets.length <= 1) continue;
      vals.push(a.job_id);
      const result = await pool.query(
        `UPDATE scheduled_jobs SET ${sets.join(', ')} WHERE id = $${idx} RETURNING *`,
        vals
      );
      if (result.rows.length > 0) updated.push(result.rows[0]);
    }
    res.json({ success: true, updated: updated.length, jobs: updated });
  } catch (error) { serverError(res, error); }
});

// GET /api/dispatch/crew-availability - Crew workload summary
app.get('/api/dispatch/crew-availability', async (req, res) => {
  try {
    const date = req.query.date || new Date().toISOString().split('T')[0];
    const result = await pool.query(`
      SELECT crew_assigned as crew_name, COUNT(*) as job_count, COALESCE(SUM(estimated_duration), 0) / 60.0 as total_hours
      FROM scheduled_jobs WHERE job_date::date = $1::date AND crew_assigned IS NOT NULL
      GROUP BY crew_assigned ORDER BY crew_assigned
    `, [date]);
    res.json({ success: true, date, crews: result.rows });
  } catch (error) { serverError(res, error); }
});

// POST /api/dispatch/geocode - Geocode all jobs for a date and store lat/lng
app.post('/api/dispatch/geocode', async (req, res) => {
  try {
    const { date, force, jobId } = req.body;
    const targetDate = date || new Date().toISOString().split('T')[0];
    // If jobId provided, re-geocode just that one job
    // If force=true, re-geocode all jobs (even those with coords) to fix city-center duplicates
    // Otherwise only geocode jobs missing lat/lng
    let whereClause = 'sj.job_date::date = $1::date AND sj.address IS NOT NULL';
    const params = [targetDate];
    if (jobId) {
      whereClause += ` AND sj.id = $2`;
      params.push(jobId);
    } else if (!force) {
      whereClause += ' AND (sj.lat IS NULL OR sj.lng IS NULL)';
    }
    const jobs = await pool.query(
      `SELECT sj.id, sj.address, sj.customer_name, sj.service_type, sj.customer_id,
              c.street AS cust_street, c.city AS cust_city, c.state AS cust_state, c.postal_code AS cust_zip
       FROM scheduled_jobs sj
       LEFT JOIN customers c ON sj.customer_id = c.id
       WHERE ${whereClause}`,
      params
    );
    let geocoded = 0;
    const GMAPS_KEY = process.env.GOOGLE_MAPS_API_KEY;

    for (const job of jobs.rows) {
      try {
        // Priority chain for building geocode address:
        // 1. Best: customer.street with house number → "street, city, state zip"
        // 2. Fallback: extract street from customer_name → combine with job.address
        // 3. Last resort: job.address as-is
        let fullAddress = job.address;
        let addressSource = 'city'; // track quality

        if (job.cust_street && /^\d+/.test(job.cust_street.trim())) {
          // Customer has a proper street address with house number
          const city = job.cust_city || '';
          const state = job.cust_state || 'OH';
          const zip = job.cust_zip || '';
          fullAddress = `${job.cust_street.trim()}, ${city} ${state} ${zip}`.trim();
          addressSource = 'street';
        } else {
          const streetFromName = extractStreetAddress(job.customer_name, job.address);
          const streetFromService = extractStreetAddress(job.service_type, job.address);
          if (streetFromName) {
            fullAddress = streetFromName + ', ' + job.address;
            addressSource = 'street';
          } else if (streetFromService) {
            fullAddress = streetFromService + ', ' + job.address;
            addressSource = 'street';
          }
        }

        const q = encodeURIComponent(fullAddress);
        if (GMAPS_KEY) {
          const gRes = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${q}&key=${GMAPS_KEY}`);
          const gData = await gRes.json();
          if (gData.status === 'OK' && gData.results && gData.results.length > 0) {
            const loc = gData.results[0].geometry.location;
            const types = gData.results[0].types || [];
            const isStreetLevel = types.some(t => ['street_address', 'premise', 'subpremise', 'route', 'intersection'].includes(t));
            const quality = isStreetLevel ? 'street' : 'city';
            await pool.query('UPDATE scheduled_jobs SET lat = $1, lng = $2, geocode_quality = $3 WHERE id = $4',
              [loc.lat, loc.lng, quality, job.id]);
            geocoded++;
          } else {
            await pool.query('UPDATE scheduled_jobs SET geocode_quality = $1 WHERE id = $2',
              ['failed', job.id]);
          }
        } else {
          const gRes = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${q}&limit=1&countrycodes=us`);
          const gData = await gRes.json();
          if (gData && gData.length > 0) {
            await pool.query('UPDATE scheduled_jobs SET lat = $1, lng = $2, geocode_quality = $3 WHERE id = $4',
              [parseFloat(gData[0].lat), parseFloat(gData[0].lon), addressSource, job.id]);
            geocoded++;
          } else {
            await pool.query('UPDATE scheduled_jobs SET geocode_quality = $1 WHERE id = $2',
              ['failed', job.id]);
          }
          await new Promise(r => setTimeout(r, 1100));
        }
      } catch (e) { /* skip individual failures */ }
    }
    res.json({ success: true, geocoded, total: jobs.rows.length });
  } catch (error) { serverError(res, error); }
});

// Helper: extract street address from a string like "John Smith 123 Main St" or "SPRING CLEANUP 123 MAIN ST MOWING"
function extractStreetAddress(text, cityAddress) {
  if (!text) return null;
  // Look for a street number followed by a street name (allows digits like "165th", "95th")
  // Non-greedy word-by-word match, longer suffixes first to avoid partial matches
  const match = text.match(/(\d+\s+(?:[A-Za-z0-9]+\s+)*?(?:Street|Drive|Road|Avenue|Boulevard|Lane|Court|Circle|Place|Way|Pike|Trail|Parkway|Row|St|Dr|Rd|Ave|Blvd|Ln|Ct|Cir|Pl|Tr|Pkwy))\b/i);
  if (match) return match[1].trim();
  return null;
}

// POST /api/dispatch/optimize-route - Optimize route order for a crew
app.post('/api/dispatch/optimize-route', async (req, res) => {
  try {
    const { date, crew_name, start_lat, start_lng } = req.body;
    if (!date || !crew_name) return res.status(400).json({ success: false, error: 'date and crew_name required' });

    const jobs = await pool.query(
      'SELECT id, address, lat, lng, route_order, estimated_duration FROM scheduled_jobs WHERE job_date::date = $1::date AND crew_assigned = $2 AND lat IS NOT NULL AND lng IS NOT NULL',
      [date, crew_name]
    );

    if (jobs.rows.length === 0) return res.json({ success: true, message: 'No geocoded jobs found for this crew', optimized: [] });

    const stops = jobs.rows.map(j => ({ id: j.id, lat: parseFloat(j.lat), lng: parseFloat(j.lng), duration: parseInt(j.estimated_duration) || 30 }));
    // Get home base from settings (default: Pappas HQ)
    let defaultLat = 41.4268, defaultLng = -81.7356;
    try {
      const hbResult = await pool.query("SELECT value FROM business_settings WHERE key = 'home_base'");
      if (hbResult.rows.length > 0) {
        const hb = hbResult.rows[0].value;
        if (hb.lat) defaultLat = parseFloat(hb.lat);
        if (hb.lng) defaultLng = parseFloat(hb.lng);
      }
    } catch(e) { /* use defaults */ }
    const sLat = start_lat ? parseFloat(start_lat) : defaultLat;
    const sLng = start_lng ? parseFloat(start_lng) : defaultLng;

    const GMAPS_KEY = process.env.GOOGLE_MAPS_API_KEY;
    if (GMAPS_KEY && stops.length >= 2) {
      try {
        // Use Google Directions API with waypoint optimization
        const origin = `${sLat},${sLng}`;
        const destination = origin; // Return to start
        const waypoints = stops.map(s => `${s.lat},${s.lng}`).join('|');
        const url = `https://maps.googleapis.com/maps/api/directions/json?origin=${origin}&destination=${destination}&waypoints=optimize:true|${waypoints}&key=${GMAPS_KEY}`;
        const gRes = await fetch(url);
        const gData = await gRes.json();
        if (gData.status === 'OK' && gData.routes && gData.routes.length > 0) {
          const optimizedOrder = gData.routes[0].waypoint_order; // Array of original indices in optimized order
          const order = optimizedOrder.map(i => stops[i].id);
          for (let i = 0; i < order.length; i++) {
            await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [i + 1, order[i]]);
          }
          const legs = gData.routes[0].legs;
          const totalDist = legs.reduce((sum, l) => sum + (l.distance?.value || 0), 0);
          const totalDur = legs.reduce((sum, l) => sum + (l.duration?.value || 0), 0);
          return res.json({ success: true, optimized: order.map((id, i) => ({ job_id: id, route_order: i + 1 })), stats: { totalDistance: (totalDist / 1609.34).toFixed(1) + ' miles', totalDriveTime: Math.round(totalDur / 60) + ' minutes' } });
        }
      } catch (e) { console.error('Google Directions optimize failed, using fallback:', e.message); }
    }

    // Fallback: nearest-neighbor TSP
    const visited = new Set();
    const order = [];
    let curLat = sLat, curLng = sLng;
    while (order.length < stops.length) {
      let nearest = null, nearestDist = Infinity;
      for (const s of stops) {
        if (visited.has(s.id)) continue;
        const d = haversine(curLat, curLng, s.lat, s.lng);
        if (d < nearestDist) { nearestDist = d; nearest = s; }
      }
      if (!nearest) break;
      visited.add(nearest.id);
      order.push(nearest.id);
      curLat = nearest.lat; curLng = nearest.lng;
    }
    for (let i = 0; i < order.length; i++) {
      await pool.query('UPDATE scheduled_jobs SET route_order = $1 WHERE id = $2', [i + 1, order[i]]);
    }
    res.json({ success: true, optimized: order.map((id, i) => ({ job_id: id, route_order: i + 1 })) });
  } catch (error) { serverError(res, error); }
});

// POST /api/dispatch/apply-future-weeks - Apply route order to future recurring visits
app.post('/api/dispatch/apply-future-weeks', async (req, res) => {
  try {
    const { date, crew_name, frequency } = req.body;
    if (!date || !crew_name) return res.status(400).json({ success: false, error: 'date and crew_name required' });

    const sourceJobs = await pool.query(
      `SELECT id, route_order, parent_job_id, is_recurring, customer_id, service_type, address
       FROM scheduled_jobs
       WHERE job_date::date = $1::date AND crew_assigned = $2 AND route_order IS NOT NULL
       ORDER BY route_order ASC`,
      [date, crew_name]
    );

    if (sourceJobs.rows.length === 0) {
      return res.json({ success: false, error: 'No ordered jobs found for this date and crew. Save the route order for today first.' });
    }

    const sourceDate = new Date(date + 'T00:00:00');
    const sourceDayOfWeek = sourceDate.getDay();
    let totalUpdated = 0;

    for (const job of sourceJobs.rows) {
      const seriesRootId = job.parent_job_id || (job.is_recurring ? job.id : null);
      if (!seriesRootId) continue;

      const futureJobs = await pool.query(
        `SELECT id, job_date FROM scheduled_jobs
         WHERE (parent_job_id = $1 OR (id = $1 AND is_recurring = true))
           AND crew_assigned = $2
           AND job_date::date > $3::date
           AND EXTRACT(DOW FROM job_date::date) = $4
         ORDER BY job_date ASC`,
        [seriesRootId, crew_name, date, sourceDayOfWeek]
      );

      for (const futureJob of futureJobs.rows) {
        if (frequency === 'biweekly') {
          const futureDate = new Date(futureJob.job_date);
          const weeksDiff = Math.round((futureDate - sourceDate) / (7 * 24 * 60 * 60 * 1000));
          if (weeksDiff % 2 !== 0) continue;
        }
        await pool.query(
          'UPDATE scheduled_jobs SET route_order = $1, updated_at = NOW() WHERE id = $2',
          [job.route_order, futureJob.id]
        );
        totalUpdated++;
      }
    }

    res.json({ success: true, updated: totalUpdated, message: `Applied route order to ${totalUpdated} future visits (${frequency})` });
  } catch (err) {
    console.error('Apply future weeks error:', err);
    serverError(res, err);
  }
});

// Haversine distance in miles
function haversine(lat1, lon1, lat2, lon2) {
  const R = 3959; // Earth radius in miles
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ═══════════════════════════════════════════════════════════
// GENERAL ROUTES
// ═══════════════════════════════════════════════════════════
// ═══ INVOICING ════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════

async function ensureInvoicesTable() {
  await _ensureInvoicesTable(pool);
}

async function nextCustomerNumber() {
  const r = await pool.query("SELECT MAX(customer_number::int) as max_num FROM customers WHERE customer_number ~ '^[0-9]+$'");
  const maxNum = parseInt(r.rows[0]?.max_num) || 0;
  return String(maxNum + 1);
}

async function nextInvoiceNumber() {
  // Use FOR UPDATE to prevent race conditions with concurrent invoice creation
  const r = await pool.query("SELECT invoice_number FROM invoices ORDER BY id DESC LIMIT 1 FOR UPDATE");
  if (r.rows.length === 0) return 'INV-10058';
  const last = r.rows[0].invoice_number || 'INV-10057';
  const num = parseInt(last.replace(/\D/g, '')) || 10057;
  return `INV-${Math.max(num + 1, 10058)}`;
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

    const [result, countResult, monthly] = await Promise.all([
      pool.query(
        `SELECT id, invoice_number, customer_id, customer_name, customer_email,
                total, amount_paid, status, paid_at, due_date, created_at, qb_invoice_id, payment_token
         FROM invoices ${whereClause}
         ORDER BY COALESCE(paid_at, updated_at) DESC
         LIMIT $${p} OFFSET $${p+1}`,
        params
      ),
      pool.query(
        `SELECT COUNT(*) as cnt, COALESCE(SUM(amount_paid),0) as total_received
         FROM invoices ${whereClause}`,
        params.slice(0, -2)
      ),
      pool.query(`
        SELECT to_char(COALESCE(paid_at, updated_at),'YYYY-MM') as month,
               COUNT(*) as count, SUM(amount_paid) as total
        FROM invoices
        WHERE amount_paid > 0 AND COALESCE(paid_at, updated_at) >= NOW() - INTERVAL '12 months'
        GROUP BY month ORDER BY month
      `)
    ]);

    res.json({
      success: true,
      payments: result.rows,
      total: parseInt(countResult.rows[0].cnt),
      totalReceived: parseFloat(countResult.rows[0].total_received),
      monthly: monthly.rows
    });
  } catch (e) {
    console.error('Payments API error:', e);
    serverError(res, e);
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
    serverError(res, error);
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
    serverError(res, error);
  }
});

// GET /api/invoices/aging - Aging AR (must be before :id route)
app.get('/api/invoices/aging', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const result = await pool.query(`
      SELECT id, invoice_number, customer_name, total, amount_paid, due_date, status
      FROM invoices
      WHERE status IN ('sent', 'viewed', 'overdue') AND total > COALESCE(amount_paid, 0)
    `);
    const now = new Date();
    const buckets = {
      current: { count: 0, total: 0, invoices: [] },
      '1_30': { count: 0, total: 0, invoices: [] },
      '31_60': { count: 0, total: 0, invoices: [] },
      '61_90': { count: 0, total: 0, invoices: [] },
      '90_plus': { count: 0, total: 0, invoices: [] }
    };
    result.rows.forEach(inv => {
      const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
      const due = inv.due_date ? new Date(inv.due_date) : now;
      const daysOverdue = Math.floor((now - due) / (1000 * 60 * 60 * 24));
      let bucket;
      if (daysOverdue <= 0) bucket = 'current';
      else if (daysOverdue <= 30) bucket = '1_30';
      else if (daysOverdue <= 60) bucket = '31_60';
      else if (daysOverdue <= 90) bucket = '61_90';
      else bucket = '90_plus';
      buckets[bucket].count++;
      buckets[bucket].total += balance;
      buckets[bucket].invoices.push({ id: inv.id, invoice_number: inv.invoice_number, customer_name: inv.customer_name, balance, days_overdue: Math.max(0, daysOverdue) });
    });
    res.json({ success: true, buckets });
  } catch (error) {
    console.error('Error fetching aging data:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/batch - Batch invoice from jobs (must be before :id route)
app.post('/api/invoices/batch', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const { job_ids } = req.body;
    if (!job_ids || !Array.isArray(job_ids) || job_ids.length === 0) {
      return res.status(400).json({ success: false, error: 'job_ids array is required' });
    }
    const created = [];
    for (const jobId of job_ids) {
      const jobResult = await pool.query('SELECT * FROM scheduled_jobs WHERE id = $1', [jobId]);
      if (jobResult.rows.length === 0) continue;
      const job = jobResult.rows[0];
      if (job.invoice_id) continue;
      const invNum = await nextInvoiceNumber();
      const dueDate = new Date();
      dueDate.setDate(dueDate.getDate() + 30);
      const lineItems = [{ description: job.service_type || 'Service', amount: parseFloat(job.service_price || 0) }];
      const total = parseFloat(job.service_price || 0);
      let customerEmail = '';
      if (job.customer_id) {
        const custResult = await pool.query('SELECT email FROM customers WHERE id = $1', [job.customer_id]);
        if (custResult.rows.length > 0) customerEmail = custResult.rows[0].email || '';
      }
      const r = await pool.query(`INSERT INTO invoices
        (invoice_number, customer_id, customer_name, customer_email, customer_address, job_id,
         subtotal, total, due_date, line_items, notes)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
        [invNum, job.customer_id || null, job.customer_name || '', customerEmail,
         job.address || '', jobId, total, total, dueDate.toISOString().split('T')[0],
         JSON.stringify(lineItems), 'Generated from completed job #' + jobId]);
      try { await pool.query('UPDATE scheduled_jobs SET invoice_id = $1 WHERE id = $2', [r.rows[0].id, jobId]); } catch(e) {}
      created.push(r.rows[0]);
    }
    res.json({ success: true, invoices: created, count: created.length });
  } catch (error) {
    console.error('Error batch creating invoices:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/:id - Single invoice
app.get('/api/invoices/:id', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const r = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = r.rows[0];
    // Fetch payment history
    try {
      const payments = await pool.query(
        'SELECT id, payment_id, amount, method, status, card_brand, card_last4, square_receipt_url, ach_bank_name, notes, paid_at, created_at FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC',
        [inv.id]
      );
      inv.payment_history = payments.rows;
    } catch(e) { inv.payment_history = []; }
    res.json({ success: true, invoice: inv });
  } catch (error) {
    serverError(res, error);
  }
});

// POST /api/invoices - Create invoice
app.post('/api/invoices', validate(schemas.createInvoice), async (req, res) => {
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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

    // Generate payment token if needed
    let paymentToken = inv.payment_token;
    if (!paymentToken) {
      paymentToken = generateToken();
      await pool.query('UPDATE invoices SET payment_token = $1, payment_token_created_at = CURRENT_TIMESTAMP WHERE id = $2', [paymentToken, inv.id]);
    }
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const payUrl = `${baseUrl}/pay-invoice.html?token=${paymentToken}`;

    const firstName = (inv.customer_name || '').split(' ')[0] || 'there';
    const totalFormatted = '$' + parseFloat(inv.total).toFixed(2);
    const content = `
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 16px;">Hi ${firstName},</p>
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 16px;">Thank you for allowing <strong>Pappas & Co. Landscaping</strong> to care for your property!</p>
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 24px;"><strong>Your latest invoice is ready for review and payment.</strong> You can access your invoice and make an online payment by clicking the secure button below:</p>

      <div style="text-align:center;margin:28px 0 32px;">
        <a href="${payUrl}" style="display:inline-block;padding:16px 48px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">
          View &amp; Pay Invoice &mdash; ${totalFormatted}
        </a>
      </div>

      <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:20px;margin:0 0 24px;">
        <p style="font-weight:700;color:#1e293b;font-size:14px;margin:0 0 12px;">Payment Reminders:</p>
        <ul style="margin:0;padding:0 0 0 20px;color:#475569;font-size:14px;line-height:1.8;">
          <li><strong>Online Payment:</strong> The fastest and easiest way to pay is directly through the secure invoice link above. We accept <strong>credit/debit cards</strong>, <strong>Apple Pay</strong>, and <strong>bank transfers (ACH)</strong>.</li>
          <li><strong>Mail a Check:</strong> Checks can be made payable to <strong>Pappas & Co. Landscaping</strong> and mailed to our secure payment box: <strong>PO Box 770057, Lakewood, OH 44107</strong>.</li>
          <li><strong>Zelle Payments:</strong> If you prefer to pay via Zelle, please ensure you are sending funds to: <strong>hello@pappaslandscaping.com</strong>.</li>
        </ul>
      </div>

      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 16px;">We truly appreciate your business and look forward to continuing to provide top-quality service.</p>
      <p style="color:#1e293b;font-size:15px;line-height:1.7;margin:0 0 4px;">If you have any questions or concerns about your service or the invoice, please don't hesitate to reach out.</p>
    `;

    let attachments = null;
    try {
      const pdfResult = await generateInvoicePDF(inv);
      if (pdfResult && pdfResult.bytes) {
        attachments = [{
          filename: `invoice-${inv.invoice_number || inv.id}.pdf`,
          content: Buffer.from(pdfResult.bytes).toString('base64'),
          type: 'application/pdf'
        }];
      }
    } catch (pdfErr) { console.error('Invoice PDF error:', pdfErr); }

    await sendEmail(inv.customer_email, `Invoice ${inv.invoice_number} from Pappas & Co.`, emailTemplate(content), attachments, { type: 'invoice', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });
    await pool.query("UPDATE invoices SET status = 'sent', sent_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = $1", [inv.id]);
    res.json({ success: true, message: 'Invoice sent' });
  } catch (error) {
    console.error('Error sending invoice:', error);
    serverError(res, error);
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
    serverError(res, error);
  }
});

// DELETE /api/invoices/:id - Delete invoice
app.delete('/api/invoices/:id', async (req, res) => {
  try {
    await ensureInvoicesTable();
    // Delete related payments first to avoid foreign key constraint
    try { await pool.query('DELETE FROM payments WHERE invoice_id = $1', [req.params.id]); } catch(e) { /* */ }
    const r = await pool.query('DELETE FROM invoices WHERE id = $1 RETURNING id', [req.params.id]);
    if (r.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// SQUARE PAYMENT ENDPOINTS
// ═══════════════════════════════════════════════════════════

// GET /api/pay/config - Public - Square frontend config
app.get('/api/pay/config', (req, res) => {
  res.json({
    appId: SQUARE_APP_ID || '',
    locationId: SQUARE_LOCATION_ID || '',
    environment: process.env.SQUARE_ENVIRONMENT || 'sandbox'
  });
});

// GET /api/square/status - Check Square connection
app.get('/api/square/status', async (req, res) => {
  if (!squareClient) {
    return res.json({ connected: false, error: 'Square not configured' });
  }
  try {
    const response = await squareClient.locationsApi.listLocations();
    const locations = response.result.locations || [];
    const location = locations.find(l => l.id === SQUARE_LOCATION_ID) || locations[0];
    res.json({
      connected: true,
      environment: process.env.SQUARE_ENVIRONMENT || 'sandbox',
      locationId: location?.id,
      locationName: location?.name,
      currency: location?.currency
    });
  } catch (error) {
    res.json({ connected: false, error: error.message });
  }
});

// GET /api/pay/:token - Public - Fetch invoice by payment token
app.get('/api/pay/:token', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const result = await pool.query(
      `SELECT id, invoice_number, customer_id, customer_name, customer_email, customer_address,
              subtotal, tax_rate, tax_amount, total, amount_paid, status, due_date,
              line_items, notes, created_at, sent_at, paid_at
       FROM invoices WHERE payment_token = $1`,
      [req.params.token]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Invoice not found' });
    }
    const inv = result.rows[0];
    // Update viewed_at
    await pool.query('UPDATE invoices SET viewed_at = CURRENT_TIMESTAMP WHERE id = $1 AND viewed_at IS NULL', [inv.id]);

    // Get payment history
    try {
      const payments = await pool.query(
        'SELECT amount, method, status, card_last4, square_receipt_url, paid_at, created_at FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC',
        [inv.id]
      );
      inv.payment_history = payments.rows;
    } catch(e) { inv.payment_history = []; }

    // Get processing fee config
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        inv.processing_fee_config = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
      }
    } catch(e) { /* no fee config */ }

    // Get saved cards for this customer
    try {
      const cards = await pool.query(
        'SELECT id, card_brand, last4, exp_month, exp_year, cardholder_name FROM customer_saved_cards WHERE customer_id = $1 AND enabled = true ORDER BY created_at DESC',
        [inv.customer_id]
      );
      inv.saved_cards = cards.rows;
    } catch(e) { inv.saved_cards = []; }

    res.json({ success: true, invoice: inv });
  } catch (error) {
    console.error('Pay token error:', error);
    serverError(res, error);
  }
});

// POST /api/pay/:token/card - Process card/Apple Pay payment
app.post('/api/pay/:token/card', async (req, res) => {
  try {
    if (!squareClient) return res.status(503).json({ success: false, error: 'Square payments not configured' });

    const { sourceId, verificationToken, save_card } = req.body;
    if (!sourceId) return res.status(400).json({ success: false, error: 'Payment source required' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = invResult.rows[0];

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice already paid' });

    // Check processing fee config
    let processingFee = 0;
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        const feeConfig = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
        if (feeConfig.enabled) {
          const pct = parseFloat(feeConfig.card_fee_percent) || 2.9;
          const fixed = parseFloat(feeConfig.card_fee_fixed) || 0.30;
          processingFee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;
        }
      }
    } catch(e) { /* no fee */ }

    const totalCharge = balance + processingFee;
    const amountCents = Math.round(totalCharge * 100);
    const idempotencyKey = crypto.randomUUID();
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();

    // If save_card requested, save the card first then charge the saved card ID
    let paymentSourceId = sourceId;
    let savedCardInfo = null;
    if (save_card && inv.customer_id) {
      try {
        // Ensure Square customer exists
        const custResult = await pool.query('SELECT square_customer_id, name, email FROM customers WHERE id = $1', [inv.customer_id]);
        const cust = custResult.rows[0];
        let squareCustomerId = cust?.square_customer_id;
        if (!squareCustomerId && cust) {
          const { result: sqCustResult } = await squareClient.customersApi.createCustomer({
            givenName: (cust.name || '').split(' ')[0],
            familyName: (cust.name || '').split(' ').slice(1).join(' '),
            emailAddress: cust.email
          });
          squareCustomerId = sqCustResult.customer.id;
          await pool.query('UPDATE customers SET square_customer_id = $1 WHERE id = $2', [squareCustomerId, inv.customer_id]);
        }

        if (squareCustomerId) {
          // Save card on file first (consumes the single-use token)
          const { result: cardResult } = await squareClient.cardsApi.createCard({
            idempotencyKey: crypto.randomUUID(),
            sourceId: sourceId,
            card: { customerId: squareCustomerId, cardholderName: inv.customer_name }
          });
          const savedCard = cardResult.card;

          // Save to our DB
          await pool.query(
            `INSERT INTO customer_saved_cards (customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [inv.customer_id, savedCard.id, savedCard.cardBrand, savedCard.last4, savedCard.expMonth, savedCard.expYear, inv.customer_name]
          );

          // Use saved card ID for the payment
          paymentSourceId = savedCard.id;
          savedCardInfo = { brand: savedCard.cardBrand, last4: savedCard.last4 };
        }
      } catch (saveErr) {
        console.error('Save card during payment error:', saveErr);
        // Fall back to charging with original token (card won't be saved but payment still works)
        paymentSourceId = sourceId;
      }
    }

    const paymentRequest = {
      sourceId: paymentSourceId,
      idempotencyKey,
      amountMoney: { amount: BigInt(amountCents), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: inv.invoice_number,
      note: `Invoice ${inv.invoice_number} - ${inv.customer_name}${processingFee > 0 ? ' (includes service fee)' : ''}`,
    };
    if (verificationToken) paymentRequest.verificationToken = verificationToken;

    const response = await squareClient.paymentsApi.createPayment(paymentRequest);
    const sqPayment = response.result.payment;

    // Determine method
    const method = sqPayment.sourceType === 'WALLET' ? 'apple_pay' : 'card';
    const cardDetails = sqPayment.cardDetails;

    // Record payment (amount = balance only, processing_fee tracked separately)
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, square_order_id, square_receipt_url, card_brand, card_last4, paid_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, totalCharge, method, sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
       sqPayment.id, sqPayment.orderId, sqPayment.receiptUrl,
       cardDetails?.card?.cardBrand, cardDetails?.card?.last4]
    );

    // Update invoice
    const newAmountPaid = parseFloat(inv.amount_paid || 0) + balance;
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : inv.status;
    if (processingFee > 0) {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP, processing_fee = $4, processing_fee_passed = true WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id, processingFee]
      );
    } else {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id]
      );
    }

    // Send confirmation emails
    const feeNote = processingFee > 0 ? `<p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Credit Card Service Fee:</strong> $${processingFee.toFixed(2)}</p>` : '';
    try {
      // Customer confirmation
      if (inv.customer_email) {
        const custContent = `
          <h2 style="color:#2e403d;margin:0 0 16px;">Payment Confirmation</h2>
          <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
          <p>We've received your payment. Thank you!</p>
          <div style="background:#ecfdf5;border:1px solid #bbf7d0;border-radius:8px;padding:20px;margin:20px 0;">
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice amount:</strong> $${balance.toFixed(2)}</p>
            ${feeNote}
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Total charged:</strong> $${totalCharge.toFixed(2)}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice:</strong> ${inv.invoice_number}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Method:</strong> ${method === 'apple_pay' ? 'Apple Pay' : 'Card'} ${cardDetails?.card?.last4 ? '•••• ' + cardDetails.card.last4 : ''}</p>
            ${sqPayment.receiptUrl ? `<p style="margin:0;"><a href="${sqPayment.receiptUrl}" style="color:#059669;font-weight:600;">View Receipt</a></p>` : ''}
          </div>
        `;
        await sendEmail(inv.customer_email, `Payment received — ${inv.invoice_number}`, emailTemplate(custContent, { showSignature: false }), null, { type: 'payment_receipt', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });
      }
      // Admin notification
      const adminContent = `
        <h2 style="color:#2e403d;margin:0 0 16px;">Payment Received</h2>
        <p><strong>${inv.customer_name}</strong> paid <strong>$${totalCharge.toFixed(2)}</strong> on invoice <strong>${inv.invoice_number}</strong>.</p>
        ${processingFee > 0 ? `<p>Credit Card Service Fee passed to customer: $${processingFee.toFixed(2)} (Invoice balance: $${balance.toFixed(2)})</p>` : ''}
        <p>Method: ${method === 'apple_pay' ? 'Apple Pay' : 'Card'} ${cardDetails?.card?.last4 ? '•••• ' + cardDetails.card.last4 : ''}</p>
        <p>Square ID: ${sqPayment.id}</p>
        ${sqPayment.receiptUrl ? `<p><a href="${sqPayment.receiptUrl}">View Receipt</a></p>` : ''}
      `;
      await sendEmail(NOTIFICATION_EMAIL, `Payment: $${totalCharge.toFixed(2)} from ${inv.customer_name}`, emailTemplate(adminContent, { showSignature: false }), null, { type: 'admin_notification', customer_name: inv.customer_name });
    } catch (emailErr) { console.error('Payment email error:', emailErr); }

    res.json({
      success: true,
      payment: {
        id: paymentId,
        amount: totalCharge,
        invoiceAmount: balance,
        processingFee,
        status: sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
        receiptUrl: sqPayment.receiptUrl,
        cardBrand: cardDetails?.card?.cardBrand,
        cardLast4: cardDetails?.card?.last4,
        cardSaved: !!savedCardInfo
      }
    });
  } catch (error) {
    console.error('Card payment error:', error);
    const errorMessage = error instanceof SquareApiError ? (error.result?.errors?.[0]?.detail || error.message) : error.message;
    res.status(500).json({ success: false, error: errorMessage });
  }
});

// POST /api/pay/:token/saved-card - Pay invoice with saved card on file
app.post('/api/pay/:token/saved-card', async (req, res) => {
  try {
    if (!squareClient) return res.status(503).json({ success: false, error: 'Square payments not configured' });
    const { card_id } = req.body;
    if (!card_id) return res.status(400).json({ success: false, error: 'card_id required' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = invResult.rows[0];

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice already paid' });

    // Look up saved card
    const cardResult = await pool.query('SELECT square_card_id, card_brand, last4 FROM customer_saved_cards WHERE id = $1 AND customer_id = $2 AND enabled = true', [card_id, inv.customer_id]);
    if (cardResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Saved card not found' });
    const savedCard = cardResult.rows[0];

    // Check processing fee config
    let processingFee = 0;
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        const feeConfig = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
        if (feeConfig.enabled) {
          const pct = parseFloat(feeConfig.card_fee_percent) || 2.9;
          const fixed = parseFloat(feeConfig.card_fee_fixed) || 0.30;
          processingFee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;
        }
      }
    } catch(e) { /* no fee */ }

    const totalCharge = balance + processingFee;
    const amountCents = Math.round(totalCharge * 100);
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();

    const response = await squareClient.paymentsApi.createPayment({
      sourceId: savedCard.square_card_id,
      idempotencyKey: crypto.randomUUID(),
      amountMoney: { amount: BigInt(amountCents), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: inv.invoice_number,
      note: `Invoice ${inv.invoice_number} - ${inv.customer_name} (card on file)${processingFee > 0 ? ' (includes service fee)' : ''}`
    });
    const sqPayment = response.result.payment;

    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, square_order_id, square_receipt_url, card_brand, card_last4, paid_at)
       VALUES ($1, $2, $3, $4, 'card', $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, totalCharge, sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
       sqPayment.id, sqPayment.orderId, sqPayment.receiptUrl, savedCard.card_brand, savedCard.last4]
    );

    const newAmountPaid = parseFloat(inv.amount_paid || 0) + balance;
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : inv.status;
    if (processingFee > 0) {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP, processing_fee = $4, processing_fee_passed = true WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id, processingFee]
      );
    } else {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id]
      );
    }

    // Send confirmation emails
    try {
      if (inv.customer_email) {
        const custContent = `
          <h2 style="color:#2e403d;margin:0 0 16px;">Payment Confirmation</h2>
          <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
          <p>We've received your payment. Thank you!</p>
          <div style="background:#ecfdf5;border:1px solid #bbf7d0;border-radius:8px;padding:20px;margin:20px 0;">
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice amount:</strong> $${balance.toFixed(2)}</p>
            ${processingFee > 0 ? `<p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Service Fee:</strong> $${processingFee.toFixed(2)}</p>` : ''}
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Total charged:</strong> $${totalCharge.toFixed(2)}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Invoice:</strong> ${inv.invoice_number}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#166534;"><strong>Method:</strong> Card on file •••• ${savedCard.last4}</p>
            ${sqPayment.receiptUrl ? `<p style="margin:0;"><a href="${sqPayment.receiptUrl}" style="color:#059669;font-weight:600;">View Receipt</a></p>` : ''}
          </div>`;
        await sendEmail(inv.customer_email, `Payment received — ${inv.invoice_number}`, emailTemplate(custContent, { showSignature: false }), null, { type: 'payment_receipt', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });
      }
      const adminContent = `
        <h2 style="color:#2e403d;margin:0 0 16px;">Payment Received</h2>
        <p><strong>${inv.customer_name}</strong> paid <strong>$${totalCharge.toFixed(2)}</strong> on invoice <strong>${inv.invoice_number}</strong>.</p>
        ${processingFee > 0 ? `<p>Service Fee: $${processingFee.toFixed(2)}</p>` : ''}
        <p>Method: Card on file •••• ${savedCard.last4}</p>
        <p>Square ID: ${sqPayment.id}</p>`;
      await sendEmail(NOTIFICATION_EMAIL, `Payment: $${totalCharge.toFixed(2)} from ${inv.customer_name}`, emailTemplate(adminContent, { showSignature: false }), null, { type: 'admin_notification', customer_name: inv.customer_name });
    } catch (emailErr) { console.error('Saved card payment email error:', emailErr); }

    res.json({
      success: true,
      payment: { id: paymentId, amount: totalCharge, invoiceAmount: balance, processingFee, status: sqPayment.status === 'COMPLETED' ? 'completed' : 'pending', receiptUrl: sqPayment.receiptUrl, cardBrand: savedCard.card_brand, cardLast4: savedCard.last4 }
    });
  } catch (error) {
    console.error('Saved card payment error:', error);
    const errorMessage = error instanceof SquareApiError ? (error.result?.errors?.[0]?.detail || error.message) : error.message;
    res.status(500).json({ success: false, error: errorMessage });
  }
});

// POST /api/pay/:token/ach - Process ACH bank transfer
app.post('/api/pay/:token/ach', async (req, res) => {
  try {
    if (!squareClient) return res.status(503).json({ success: false, error: 'Square payments not configured' });

    const { sourceId } = req.body;
    if (!sourceId) return res.status(400).json({ success: false, error: 'Payment source required' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = invResult.rows[0];

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice already paid' });

    // Check processing fee config for ACH
    let processingFee = 0;
    try {
      const feeResult = await pool.query("SELECT value FROM business_settings WHERE key = 'processing_fee_config'");
      if (feeResult.rows.length > 0) {
        const feeConfig = typeof feeResult.rows[0].value === 'string' ? JSON.parse(feeResult.rows[0].value) : feeResult.rows[0].value;
        if (feeConfig.enabled) {
          const pct = parseFloat(feeConfig.ach_fee_percent) || 1.0;
          const fixed = parseFloat(feeConfig.ach_fee_fixed) || 0;
          processingFee = Math.round((balance * (pct / 100) + fixed) * 100) / 100;
        }
      }
    } catch(e) { /* no fee */ }

    const totalCharge = balance + processingFee;
    const amountCents = Math.round(totalCharge * 100);
    const idempotencyKey = crypto.randomUUID();
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();

    const response = await squareClient.paymentsApi.createPayment({
      sourceId,
      idempotencyKey,
      amountMoney: { amount: BigInt(amountCents), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: inv.invoice_number,
      note: `Invoice ${inv.invoice_number} - ${inv.customer_name}${processingFee > 0 ? ' (includes service fee)' : ''}`,
      acceptPartialAuthorization: false,
    });
    const sqPayment = response.result.payment;
    const bankDetails = sqPayment.bankAccountDetails;

    // ACH payments are typically PENDING until they clear
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, square_order_id, square_receipt_url, ach_bank_name, paid_at)
       VALUES ($1, $2, $3, $4, 'ach', $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, totalCharge, sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
       sqPayment.id, sqPayment.orderId, sqPayment.receiptUrl, bankDetails?.bankName]
    );

    // Update invoice
    const newAmountPaid = parseFloat(inv.amount_paid || 0) + balance;
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : inv.status;
    if (processingFee > 0) {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP, processing_fee = $4, processing_fee_passed = true WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id, processingFee]
      );
    } else {
      await pool.query(
        `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2::text = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
        [newAmountPaid, newStatus, inv.id]
      );
    }

    // Send emails
    const feeNote = processingFee > 0 ? `<p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>ACH Service Fee:</strong> $${processingFee.toFixed(2)}</p>` : '';
    try {
      if (inv.customer_email) {
        const custContent = `
          <h2 style="color:#2e403d;margin:0 0 16px;">Payment Confirmation</h2>
          <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
          <p>We've received your ACH bank transfer. It may take 3-5 business days to clear.</p>
          <div style="background:#f5f3ff;border:1px solid #ddd6fe;border-radius:8px;padding:20px;margin:20px 0;">
            <p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>Invoice amount:</strong> $${balance.toFixed(2)}</p>
            ${feeNote}
            <p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>Total charged:</strong> $${totalCharge.toFixed(2)}</p>
            <p style="margin:0 0 8px;font-size:14px;color:#5b21b6;"><strong>Invoice:</strong> ${inv.invoice_number}</p>
            <p style="margin:0;font-size:14px;color:#5b21b6;"><strong>Method:</strong> ACH Bank Transfer${bankDetails?.bankName ? ' (' + bankDetails.bankName + ')' : ''}</p>
          </div>
        `;
        await sendEmail(inv.customer_email, `Payment received — ${inv.invoice_number}`, emailTemplate(custContent, { showSignature: false }));
      }
      await sendEmail(NOTIFICATION_EMAIL, `ACH Payment: $${totalCharge.toFixed(2)} from ${inv.customer_name}`, emailTemplate(`
        <h2 style="color:#2e403d;margin:0 0 16px;">ACH Payment Received</h2>
        <p><strong>${inv.customer_name}</strong> paid <strong>$${totalCharge.toFixed(2)}</strong> via ACH on invoice <strong>${inv.invoice_number}</strong>.</p>
        ${processingFee > 0 ? `<p>ACH Service Fee passed to customer: $${processingFee.toFixed(2)} (Invoice balance: $${balance.toFixed(2)})</p>` : ''}
        <p>Status: ${sqPayment.status} (ACH payments may take 3-5 days to clear)</p>
        <p>Square ID: ${sqPayment.id}</p>
      `, { showSignature: false }));
    } catch (emailErr) { console.error('ACH email error:', emailErr); }

    res.json({
      success: true,
      payment: {
        id: paymentId,
        amount: totalCharge,
        invoiceAmount: balance,
        processingFee,
        status: sqPayment.status === 'COMPLETED' ? 'completed' : 'pending',
        receiptUrl: sqPayment.receiptUrl,
        bankName: bankDetails?.bankName,
        note: sqPayment.status !== 'COMPLETED' ? 'ACH transfers typically take 3-5 business days to clear' : undefined
      }
    });
  } catch (error) {
    console.error('ACH payment error:', error);
    const errorMessage = error instanceof SquareApiError ? (error.result?.errors?.[0]?.detail || error.message) : error.message;
    res.status(500).json({ success: false, error: errorMessage });
  }
});

// GET /api/pay/:token/pdf - Download invoice PDF (public)
app.get('/api/pay/:token/pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = result.rows[0];
    const pdfResult = await generateInvoicePDF(inv);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="invoice-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Pay PDF error:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/:id/pdf - Download invoice PDF (admin)
app.get('/api/invoices/:id/pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = result.rows[0];
    const pdfResult = await generateInvoicePDF(inv);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="invoice-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Invoice PDF error:', error);
    serverError(res, error);
  }
});

// GET /api/invoices/:id/receipt-pdf - Download payment receipt PDF (admin)
app.get('/api/invoices/:id/receipt-pdf', async (req, res) => {
  try {
    const invResult = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = invResult.rows[0];
    // Get most recent payment for this invoice
    let payment = {};
    try {
      const payResult = await pool.query('SELECT * FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC LIMIT 1', [inv.id]);
      if (payResult.rows.length > 0) payment = payResult.rows[0];
    } catch (e) { /* no payments table or no payment */ }
    if (!payment.paid_at) payment.paid_at = inv.paid_at;
    if (!payment.amount) payment.amount = inv.amount_paid || inv.total;
    const pdfResult = await generateReceiptPDF(inv, payment);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'Receipt PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="receipt-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Receipt PDF error:', error);
    serverError(res, error);
  }
});

// GET /api/pay/:token/receipt-pdf - Download payment receipt PDF (public)
app.get('/api/pay/:token/receipt-pdf', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM invoices WHERE payment_token = $1', [req.params.token]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const inv = result.rows[0];
    let payment = {};
    try {
      const payResult = await pool.query('SELECT * FROM payments WHERE invoice_id = $1 ORDER BY created_at DESC LIMIT 1', [inv.id]);
      if (payResult.rows.length > 0) payment = payResult.rows[0];
    } catch (e) { /* no payment */ }
    if (!payment.paid_at) payment.paid_at = inv.paid_at;
    if (!payment.amount) payment.amount = inv.amount_paid || inv.total;
    const pdfResult = await generateReceiptPDF(inv, payment);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'Receipt PDF generation failed' });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="receipt-${inv.invoice_number || inv.id}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Pay receipt PDF error:', error);
    serverError(res, error);
  }
});

// GET /api/customers/:id/statement-pdf - Download account statement PDF
app.get('/api/customers/:id/statement-pdf', async (req, res) => {
  try {
    const custResult = await pool.query('SELECT * FROM customers WHERE id = $1', [req.params.id]);
    if (custResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const customer = custResult.rows[0];
    const { from, to, status } = req.query;
    let query = 'SELECT * FROM invoices WHERE customer_id = $1';
    const params = [customer.id];
    let p = 2;
    if (from) { query += ` AND created_at >= $${p++}`; params.push(from); }
    if (to) { query += ` AND created_at <= $${p++}`; params.push(to); }
    if (status) { query += ` AND status = $${p++}`; params.push(status); }
    query += ' ORDER BY created_at DESC';
    const invResult = await pool.query(query, params);
    const dateRange = from || to ? `${from || 'All'} to ${to || 'Present'}` : '';
    const pdfResult = await generateStatementPDF(customer, invResult.rows, dateRange);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'Statement PDF generation failed' });
    const custName = (customer.name || customer.first_name || 'customer').replace(/\s+/g, '-').toLowerCase();
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="statement-${custName}.pdf"`);
    res.send(Buffer.from(pdfResult.bytes));
  } catch (error) {
    console.error('Statement PDF error:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/:id/record-payment - Record manual payment (cash/check)
app.post('/api/invoices/:id/record-payment', validate(schemas.recordPayment), async (req, res) => {
  try {
    await ensureInvoicesTable();
    const { amount, method, notes } = req.body;
    if (!amount || amount <= 0) return res.status(400).json({ success: false, error: 'Invalid amount' });

    const invResult = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = invResult.rows[0];

    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();
    await ensurePaymentsTables();
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, notes, paid_at)
       VALUES ($1, $2, $3, $4, $5, 'completed', $6, CURRENT_TIMESTAMP)`,
      [paymentId, inv.id, inv.customer_id, amount, method || 'cash', notes]
    );

    const newAmountPaid = parseFloat(inv.amount_paid || 0) + parseFloat(amount);
    const newStatus = newAmountPaid >= parseFloat(inv.total) ? 'paid' : inv.status;
    await pool.query(
      `UPDATE invoices SET amount_paid = $1, status = $2, paid_at = CASE WHEN $2 = 'paid' THEN CURRENT_TIMESTAMP ELSE paid_at END, updated_at = CURRENT_TIMESTAMP WHERE id = $3`,
      [newAmountPaid, newStatus, inv.id]
    );

    res.json({ success: true, paymentId, newAmountPaid, newStatus });
  } catch (error) {
    console.error('Record payment error:', error);
    serverError(res, error);
  }
});

// POST /api/invoices/:id/send-reminder - Send payment reminder email
app.post('/api/invoices/:id/send-reminder', async (req, res) => {
  try {
    const invResult = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (invResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const inv = invResult.rows[0];
    if (!inv.customer_email) return res.status(400).json({ success: false, error: 'No customer email' });

    const balance = parseFloat(inv.total) - parseFloat(inv.amount_paid || 0);
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const payUrl = inv.payment_token ? `${baseUrl}/pay-invoice.html?token=${inv.payment_token}` : '';

    const content = `
      <h2 style="color:#2e403d;margin:0 0 16px;">Payment Reminder</h2>
      <p>Hi ${(inv.customer_name || '').split(' ')[0]},</p>
      <p>This is a friendly reminder that your invoice <strong>${inv.invoice_number}</strong> has a balance of <strong>$${balance.toFixed(2)}</strong>${inv.due_date ? ' due by <strong>' + new Date(inv.due_date).toLocaleDateString('en-US', {month:'long',day:'numeric',year:'numeric'}) + '</strong>' : ''}.</p>
      ${payUrl ? `
        <div style="text-align:center;margin:28px 0;">
          <a href="${payUrl}" style="display:inline-block;padding:16px 40px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">
            Pay Now — $${balance.toFixed(2)}
          </a>
        </div>
        <p style="text-align:center;font-size:12px;color:#9ca09c;">Secure payment powered by Square</p>
      ` : ''}
      <p>If you've already sent payment, please disregard this reminder.</p>
    `;
    await sendEmail(inv.customer_email, `Reminder: Invoice ${inv.invoice_number} — $${balance.toFixed(2)} due`, emailTemplate(content), null, { type: 'invoice_reminder', customer_id: inv.customer_id, customer_name: inv.customer_name, invoice_id: inv.id });

    await pool.query(
      'UPDATE invoices SET reminder_sent_at = CURRENT_TIMESTAMP, reminder_count = COALESCE(reminder_count, 0) + 1, updated_at = CURRENT_TIMESTAMP WHERE id = $1',
      [inv.id]
    );

    res.json({ success: true, message: 'Reminder sent' });
  } catch (error) {
    console.error('Send reminder error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// CUSTOMER PORTAL ENDPOINTS
// ═══════════════════════════════════════════════════════════

// POST /api/portal/request-access - Send magic link email
app.post('/api/portal/request-access', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ success: false, error: 'Email required' });

    // Find customer by email
    const custResult = await pool.query(
      'SELECT id, name FROM customers WHERE email ILIKE $1 LIMIT 1',
      [email.trim()]
    );

    // Always return success (don't reveal if email exists)
    if (custResult.rows.length === 0) {
      return res.json({ success: true, message: 'If an account exists, a link has been sent.' });
    }

    const customer = custResult.rows[0];
    const token = generateToken();
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000); // 30 days

    await ensurePaymentsTables();
    await pool.query(
      'INSERT INTO customer_portal_tokens (token, customer_id, email, expires_at) VALUES ($1, $2, $3, $4)',
      [token, customer.id, email.trim(), expiresAt]
    );

    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const portalUrl = `${baseUrl}/customer-portal.html?token=${token}`;

    const content = `
      <h2 style="color:#2e403d;margin:0 0 16px;">Your Customer Portal Access</h2>
      <p>Hi ${(customer.name || '').split(' ')[0]},</p>
      <p>Click below to access your Pappas & Co. customer portal where you can view invoices, make payments, and see your payment history.</p>
      <div style="text-align:center;margin:28px 0;">
        <a href="${portalUrl}" style="display:inline-block;padding:16px 40px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">
          Access Your Portal
        </a>
      </div>
      <p style="font-size:13px;color:#9ca09c;">This link is valid for 30 days. If you didn't request this, please ignore this email.</p>
    `;
    await sendEmail(email, 'Your Pappas & Co. Customer Portal', emailTemplate(content, { showSignature: false }));

    res.json({ success: true, message: 'Access link sent to your email.' });
  } catch (error) {
    console.error('Portal access error:', error);
    serverError(res, error);
  }
});

// GET /api/portal/:token - Verify token, return customer data
app.get('/api/portal/:token', async (req, res) => {
  try {
    await ensurePaymentsTables();
    const result = await pool.query(
      'SELECT pt.*, c.name as customer_name, c.email as customer_email, c.phone, CONCAT_WS(\', \', c.street, c.city, c.state, c.postal_code) as address FROM customer_portal_tokens pt LEFT JOIN customers c ON pt.customer_id = c.id WHERE pt.token = $1 AND pt.expires_at > NOW()',
      [req.params.token]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Link expired or invalid' });
    }
    const data = result.rows[0];
    res.json({
      success: true,
      customer: {
        id: data.customer_id,
        name: data.customer_name,
        email: data.customer_email || data.email,
        phone: data.phone,
        address: data.address
      }
    });
  } catch (error) {
    serverError(res, error);
  }
});

// GET /api/portal/:token/invoices - Customer's invoices
app.get('/api/portal/:token/invoices', async (req, res) => {
  try {
    await ensurePaymentsTables();
    const tokenResult = await pool.query(
      'SELECT customer_id, email FROM customer_portal_tokens WHERE token = $1 AND expires_at > NOW()',
      [req.params.token]
    );
    if (tokenResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invalid token' });
    const { customer_id, email } = tokenResult.rows[0];

    await ensureInvoicesTable();
    const invoices = await pool.query(
      `SELECT id, invoice_number, customer_name, total, amount_paid, status, due_date, payment_token, created_at, sent_at, paid_at
       FROM invoices WHERE customer_id = $1 OR customer_email ILIKE $2
       ORDER BY created_at DESC`,
      [customer_id, email]
    );
    res.json({ success: true, invoices: invoices.rows });
  } catch (error) {
    serverError(res, error);
  }
});

// GET /api/portal/:token/payments - Customer's payment history
app.get('/api/portal/:token/payments', async (req, res) => {
  try {
    await ensurePaymentsTables();
    const tokenResult = await pool.query(
      'SELECT customer_id FROM customer_portal_tokens WHERE token = $1 AND expires_at > NOW()',
      [req.params.token]
    );
    if (tokenResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invalid token' });
    const { customer_id } = tokenResult.rows[0];

    const payments = await pool.query(
      `SELECT p.*, i.invoice_number FROM payments p
       LEFT JOIN invoices i ON p.invoice_id = i.id
       WHERE p.customer_id = $1
       ORDER BY p.created_at DESC`,
      [customer_id]
    );
    res.json({ success: true, payments: payments.rows });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ ENHANCED CUSTOMER PORTAL ════════════════════════════
// ═══════════════════════════════════════════════════════════

// Helper: validate portal token and return customer_id
async function validatePortalToken(token) {
  const result = await pool.query('SELECT customer_id, email FROM customer_portal_tokens WHERE token = $1 AND expires_at > NOW()', [token]);
  if (result.rows.length === 0) return null;
  return result.rows[0];
}

// POST /api/portal/:token/cards/save - Save card-on-file via Square
app.post('/api/portal/:token/cards/save', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const { source_id, cardholder_name } = req.body;
    if (!source_id) return res.status(400).json({ success: false, error: 'source_id required' });
    if (!squareClient) return res.status(500).json({ success: false, error: 'Square not configured' });

    // Ensure Square customer exists
    let squareCustomerId;
    const custResult = await pool.query('SELECT square_customer_id, name, email FROM customers WHERE id = $1', [tokenData.customer_id]);
    const cust = custResult.rows[0];
    if (cust && cust.square_customer_id) {
      squareCustomerId = cust.square_customer_id;
    } else {
      // Create Square customer
      const { result: sqResult } = await squareClient.customersApi.createCustomer({
        givenName: (cust.name || '').split(' ')[0],
        familyName: (cust.name || '').split(' ').slice(1).join(' '),
        emailAddress: cust.email || tokenData.email
      });
      squareCustomerId = sqResult.customer.id;
      await pool.query('UPDATE customers SET square_customer_id = $1 WHERE id = $2', [squareCustomerId, tokenData.customer_id]);
    }

    // Create card-on-file
    const { result: cardResult } = await squareClient.cardsApi.createCard({
      idempotencyKey: crypto.randomUUID(),
      sourceId: source_id,
      card: { customerId: squareCustomerId, cardholderName: cardholder_name || cust.name }
    });
    const card = cardResult.card;
    await pool.query(
      `INSERT INTO customer_saved_cards (customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [tokenData.customer_id, card.id, card.cardBrand, card.last4, card.expMonth, card.expYear, cardholder_name || cust.name]
    );
    res.json({ success: true, card: { id: card.id, brand: card.cardBrand, last4: card.last4, expMonth: card.expMonth, expYear: card.expYear } });
  } catch (error) {
    console.error('Save card error:', error);
    serverError(res, error);
  }
});

// POST /api/customers/:id/card-on-file - Save card during contract signing
app.post('/api/customers/:id/card-on-file', async (req, res) => {
  try {
    const { source_id } = req.body;
    if (!source_id) return res.status(400).json({ success: false, error: 'source_id required' });
    if (!squareClient) return res.status(500).json({ success: false, error: 'Square not configured' });

    const customerId = req.params.id;
    const custResult = await pool.query('SELECT square_customer_id, name, email FROM customers WHERE id = $1', [customerId]);
    if (!custResult.rows.length) return res.status(404).json({ success: false, error: 'Customer not found' });
    const cust = custResult.rows[0];

    // Ensure Square customer exists
    let squareCustomerId = cust.square_customer_id;
    if (!squareCustomerId) {
      const { result: sqResult } = await squareClient.customersApi.createCustomer({
        givenName: (cust.name || '').split(' ')[0],
        familyName: (cust.name || '').split(' ').slice(1).join(' '),
        emailAddress: cust.email
      });
      squareCustomerId = sqResult.customer.id;
      await pool.query('UPDATE customers SET square_customer_id = $1 WHERE id = $2', [squareCustomerId, customerId]);
    }

    // Create card-on-file
    const { result: cardResult } = await squareClient.cardsApi.createCard({
      idempotencyKey: crypto.randomUUID(),
      sourceId: source_id,
      card: { customerId: squareCustomerId, cardholderName: cust.name }
    });
    const card = cardResult.card;
    await pool.query(
      `INSERT INTO customer_saved_cards (customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [customerId, card.id, card.cardBrand, card.last4, card.expMonth, card.expYear, cust.name]
    );
    res.json({ success: true, card: { brand: card.cardBrand, last4: card.last4 } });
  } catch (error) {
    console.error('Card-on-file save error:', error);
    serverError(res, error);
  }
});

// GET /api/portal/:token/cards - List saved cards
app.get('/api/portal/:token/cards', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const result = await pool.query('SELECT id, card_brand, last4, exp_month, exp_year, cardholder_name, is_default FROM customer_saved_cards WHERE customer_id = $1 AND enabled = true ORDER BY created_at DESC', [tokenData.customer_id]);
    res.json({ success: true, cards: result.rows });
  } catch (error) { serverError(res, error); }
});

// DELETE /api/portal/:token/cards/:cardId - Disable/remove saved card
app.delete('/api/portal/:token/cards/:cardId', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const card = await pool.query('SELECT square_card_id FROM customer_saved_cards WHERE id = $1 AND customer_id = $2', [req.params.cardId, tokenData.customer_id]);
    if (card.rows.length === 0) return res.status(404).json({ success: false, error: 'Card not found' });
    // Disable in Square
    if (squareClient && card.rows[0].square_card_id) {
      try { await squareClient.cardsApi.disableCard(card.rows[0].square_card_id); } catch(e) { console.error('Square disable card:', e.message); }
    }
    await pool.query('UPDATE customer_saved_cards SET enabled = false WHERE id = $1', [req.params.cardId]);
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

// GET /api/customers/:id/saved-cards - Admin: get customer's saved cards
app.get('/api/customers/:id/saved-cards', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, card_brand, last4, exp_month, exp_year, created_at FROM customer_saved_cards WHERE customer_id = $1 AND enabled = true ORDER BY created_at DESC',
      [req.params.id]
    );
    res.json({ success: true, cards: result.rows });
  } catch (error) { serverError(res, error); }
});

// POST /api/customers/:id/cards/save - Admin: save card-on-file via Square
app.post('/api/customers/:id/cards/save', authenticateToken, async (req, res) => {
  try {
    const { source_id } = req.body;
    if (!source_id) return res.status(400).json({ success: false, error: 'source_id required' });
    if (!squareClient) return res.status(500).json({ success: false, error: 'Square not configured' });

    const customerId = req.params.id;
    const custResult = await pool.query('SELECT square_customer_id, name, email FROM customers WHERE id = $1', [customerId]);
    if (!custResult.rows.length) return res.status(404).json({ success: false, error: 'Customer not found' });
    const cust = custResult.rows[0];

    // Ensure Square customer exists
    let squareCustomerId = cust.square_customer_id;
    if (!squareCustomerId) {
      const { result: sqResult } = await squareClient.customersApi.createCustomer({
        givenName: (cust.name || '').split(' ')[0],
        familyName: (cust.name || '').split(' ').slice(1).join(' '),
        emailAddress: cust.email
      });
      squareCustomerId = sqResult.customer.id;
      await pool.query('UPDATE customers SET square_customer_id = $1 WHERE id = $2', [squareCustomerId, customerId]);
    }

    // Create card-on-file
    const { result: cardResult } = await squareClient.cardsApi.createCard({
      idempotencyKey: crypto.randomUUID(),
      sourceId: source_id,
      card: { customerId: squareCustomerId, cardholderName: cust.name }
    });
    const card = cardResult.card;
    await pool.query(
      `INSERT INTO customer_saved_cards (customer_id, square_card_id, card_brand, last4, exp_month, exp_year, cardholder_name) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [customerId, card.id, card.cardBrand, card.last4, card.expMonth, card.expYear, cust.name]
    );
    res.json({ success: true, card: { id: card.id, brand: card.cardBrand, last4: card.last4, expMonth: card.expMonth, expYear: card.expYear } });
  } catch (error) {
    console.error('Admin save card error:', error);
    serverError(res, error);
  }
});

// DELETE /api/customers/:id/cards/:cardId - Admin: disable/remove saved card
app.delete('/api/customers/:id/cards/:cardId', authenticateToken, async (req, res) => {
  try {
    const card = await pool.query('SELECT square_card_id FROM customer_saved_cards WHERE id = $1 AND customer_id = $2 AND enabled = true', [req.params.cardId, req.params.id]);
    if (card.rows.length === 0) return res.status(404).json({ success: false, error: 'Card not found' });
    // Disable in Square
    if (squareClient && card.rows[0].square_card_id) {
      try { await squareClient.cardsApi.disableCard(card.rows[0].square_card_id); } catch(e) { console.error('Square disable card:', e.message); }
    }
    await pool.query('UPDATE customer_saved_cards SET enabled = false WHERE id = $1', [req.params.cardId]);
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

// POST /api/invoices/:id/charge-card - Admin: charge a saved card for an invoice
app.post('/api/invoices/:id/charge-card', authenticateToken, async (req, res) => {
  try {
    const { card_id } = req.body;
    if (!card_id) return res.status(400).json({ success: false, error: 'card_id required' });
    if (!squareClient) return res.status(500).json({ success: false, error: 'Square not configured' });

    const inv = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (inv.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const invoice = inv.rows[0];

    const cardResult = await pool.query(
      'SELECT square_card_id FROM customer_saved_cards WHERE id = $1 AND customer_id = $2 AND enabled = true',
      [card_id, invoice.customer_id]
    );
    if (cardResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Card not found' });

    const balance = Math.round((parseFloat(invoice.total) - parseFloat(invoice.amount_paid || 0)) * 100);
    if (balance <= 0) return res.status(400).json({ success: false, error: 'Invoice has no balance due' });

    const { result: payResult } = await squareClient.paymentsApi.createPayment({
      idempotencyKey: crypto.randomUUID(),
      sourceId: cardResult.rows[0].square_card_id,
      amountMoney: { amount: BigInt(balance), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: invoice.invoice_number,
      note: `Invoice ${invoice.invoice_number} — admin charge card-on-file`
    });
    const payment = payResult.payment;
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, card_brand, card_last4, paid_at)
       VALUES ($1, $2, $3, $4, 'card', 'completed', $5, $6, $7, NOW())`,
      [paymentId, req.params.id, invoice.customer_id, balance / 100, payment.id, payment.cardDetails?.card?.cardBrand, payment.cardDetails?.card?.last4]
    );
    await pool.query("UPDATE invoices SET status = 'paid', amount_paid = total, paid_at = NOW(), updated_at = NOW() WHERE id = $1", [req.params.id]);
    res.json({ success: true, paymentId, receiptUrl: payment.receiptUrl });
  } catch (error) {
    console.error('Admin charge card error:', error);
    serverError(res, error);
  }
});

// POST /api/portal/:token/pay-with-saved-card - Charge saved card
app.post('/api/portal/:token/pay-with-saved-card', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const { card_id, invoice_id } = req.body;
    if (!card_id || !invoice_id) return res.status(400).json({ success: false, error: 'card_id and invoice_id required' });
    if (!squareClient) return res.status(500).json({ success: false, error: 'Square not configured' });

    const cardResult = await pool.query('SELECT square_card_id FROM customer_saved_cards WHERE id = $1 AND customer_id = $2 AND enabled = true', [card_id, tokenData.customer_id]);
    if (cardResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Card not found' });

    const inv = await pool.query('SELECT * FROM invoices WHERE id = $1', [invoice_id]);
    if (inv.rows.length === 0) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const invoice = inv.rows[0];
    const balance = Math.round((parseFloat(invoice.total) - parseFloat(invoice.amount_paid || 0)) * 100);

    const { result: payResult } = await squareClient.paymentsApi.createPayment({
      idempotencyKey: crypto.randomUUID(),
      sourceId: cardResult.rows[0].square_card_id,
      amountMoney: { amount: BigInt(balance), currency: 'USD' },
      locationId: SQUARE_LOCATION_ID,
      referenceId: invoice.invoice_number,
      note: `Invoice ${invoice.invoice_number} — card-on-file`
    });
    const payment = payResult.payment;
    const paymentId = 'PAY-' + crypto.randomUUID().slice(0, 8).toUpperCase();
    await pool.query(
      `INSERT INTO payments (payment_id, invoice_id, customer_id, amount, method, status, square_payment_id, card_brand, card_last4, paid_at)
       VALUES ($1, $2, $3, $4, 'card', 'completed', $5, $6, $7, NOW())`,
      [paymentId, invoice_id, tokenData.customer_id, balance / 100, payment.id, payment.cardDetails?.card?.cardBrand, payment.cardDetails?.card?.last4]
    );
    await pool.query("UPDATE invoices SET status = 'paid', amount_paid = total, paid_at = NOW(), updated_at = NOW() WHERE id = $1", [invoice_id]);
    res.json({ success: true, paymentId, receiptUrl: payment.receiptUrl });
  } catch (error) {
    console.error('Pay with saved card error:', error);
    serverError(res, error);
  }
});

// POST /api/portal/:token/service-requests - Submit service request
app.post('/api/portal/:token/service-requests', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const { service_type, description, preferred_date, urgency } = req.body;
    if (!description) return res.status(400).json({ success: false, error: 'description required' });
    const result = await pool.query(
      `INSERT INTO service_requests (customer_id, service_type, description, preferred_date, urgency) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [tokenData.customer_id, service_type, description, preferred_date, urgency || 'normal']
    );
    // Email admin
    const cust = await pool.query('SELECT name, email FROM customers WHERE id = $1', [tokenData.customer_id]);
    const custName = cust.rows[0]?.name || 'Customer';
    await sendEmail('hello@pappaslandscaping.com', `New Service Request from ${custName}`, emailTemplate(`
      <h2 style="color:#2e403d;margin:0 0 16px;">New Service Request</h2>
      <p><strong>${custName}</strong> submitted a service request:</p>
      <div style="background:#f8fafc;border-radius:8px;padding:16px;margin:16px 0;">
        <p style="margin:0;"><strong>Service:</strong> ${service_type || 'General'}</p>
        <p style="margin:4px 0;"><strong>Description:</strong> ${description}</p>
        ${preferred_date ? `<p style="margin:4px 0;"><strong>Preferred Date:</strong> ${preferred_date}</p>` : ''}
        <p style="margin:4px 0;"><strong>Urgency:</strong> ${urgency || 'normal'}</p>
      </div>
    `));
    res.json({ success: true, request: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// GET /api/portal/:token/service-requests - Customer's requests
app.get('/api/portal/:token/service-requests', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const result = await pool.query('SELECT * FROM service_requests WHERE customer_id = $1 ORDER BY created_at DESC', [tokenData.customer_id]);
    res.json({ success: true, requests: result.rows });
  } catch (error) { serverError(res, error); }
});

// GET /api/portal/:token/quotes - Customer's pending quotes
app.get('/api/portal/:token/quotes', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const result = await pool.query(
      `SELECT id, quote_number, customer_name, total, status, signing_token, created_at FROM sent_quotes WHERE customer_email ILIKE $1 AND status IN ('pending','viewed') ORDER BY created_at DESC`,
      [tokenData.email]
    );
    res.json({ success: true, quotes: result.rows });
  } catch (error) { serverError(res, error); }
});

// GET /api/portal/:token/service-history - Completed jobs with photos
app.get('/api/portal/:token/service-history', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const result = await pool.query(
      `SELECT id, job_date, service_type, address, status, service_price, special_notes, crew_assigned, completion_notes, completion_photos FROM scheduled_jobs WHERE customer_id = $1 AND status = 'completed' ORDER BY job_date DESC LIMIT 50`,
      [tokenData.customer_id]
    );
    res.json({ success: true, jobs: result.rows });
  } catch (error) { serverError(res, error); }
});

// GET /api/portal/:token/properties - Customer properties
app.get('/api/portal/:token/properties', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const result = await pool.query('SELECT * FROM properties WHERE customer_id = $1 ORDER BY created_at DESC', [tokenData.customer_id]);
    res.json({ success: true, properties: result.rows });
  } catch (error) { serverError(res, error); }
});

// GET /api/portal/:token/preferences - Communication preferences
app.get('/api/portal/:token/preferences', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const result = await pool.query('SELECT * FROM customer_communication_prefs WHERE customer_id = $1', [tokenData.customer_id]);
    const prefs = result.rows[0] || { email_invoices: true, email_reminders: true, email_marketing: false, sms_reminders: false, sms_marketing: false };
    res.json({ success: true, preferences: prefs });
  } catch (error) { serverError(res, error); }
});

// POST /api/portal/:token/preferences - Update communication preferences
app.post('/api/portal/:token/preferences', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });
    const { email_invoices, email_reminders, email_marketing, sms_reminders, sms_marketing } = req.body;
    const result = await pool.query(
      `INSERT INTO customer_communication_prefs (customer_id, email_invoices, email_reminders, email_marketing, sms_reminders, sms_marketing, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       ON CONFLICT (customer_id) DO UPDATE SET email_invoices = $2, email_reminders = $3, email_marketing = $4, sms_reminders = $5, sms_marketing = $6, updated_at = NOW()
       RETURNING *`,
      [tokenData.customer_id, email_invoices !== false, email_reminders !== false, email_marketing === true, sms_reminders === true, sms_marketing === true]
    );
    res.json({ success: true, preferences: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// CUSTOMER PORTAL — DASHBOARD, PHOTOS, REVIEWS
// ═══════════════════════════════════════════════════════════

// Ensure customer_reviews table exists
async function ensureCustomerReviewsTable() {
  await _ensureCustomerReviewsTable(pool);
}

// GET /api/portal/:token/dashboard - Portal dashboard summary
app.get('/api/portal/:token/dashboard', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });

    const { customer_id, email } = tokenData;

    const custResult = await pool.query('SELECT name, first_name, last_name, email FROM customers WHERE id = $1', [customer_id]);
    const cust = custResult.rows[0] || {};
    const customerName = cust.name || ((cust.first_name || '') + (cust.last_name ? ' ' + cust.last_name : '')).trim() || 'Unknown';

    const balanceResult = await pool.query(
      `SELECT COALESCE(SUM(total - COALESCE(amount_paid, 0)), 0) as outstanding_balance
       FROM invoices WHERE (customer_id = $1 OR customer_email ILIKE $2) AND status != 'paid'`,
      [customer_id, email]
    );

    const paidResult = await pool.query(
      `SELECT COALESCE(SUM(amount), 0) as total_paid FROM payments WHERE customer_id = $1 AND status = 'completed'`,
      [customer_id]
    );

    const quotesResult = await pool.query(
      `SELECT COUNT(*) as pending_quotes_count FROM sent_quotes WHERE customer_email ILIKE $1 AND status IN ('pending', 'viewed')`,
      [email]
    );

    const jobsResult = await pool.query(
      `SELECT id, job_date, service_type, address, status, service_price
       FROM scheduled_jobs WHERE customer_id = $1 AND status != 'completed'
       ORDER BY job_date ASC LIMIT 5`,
      [customer_id]
    );

    const invoicesResult = await pool.query(
      `SELECT id, invoice_number, total, amount_paid, status, due_date, payment_token, created_at
       FROM invoices WHERE customer_id = $1 OR customer_email ILIKE $2
       ORDER BY created_at DESC LIMIT 3`,
      [customer_id, email]
    );

    const requestsResult = await pool.query(
      `SELECT COUNT(*) as pending_requests_count FROM service_requests WHERE customer_id = $1 AND status = 'pending'`,
      [customer_id]
    );

    res.json({
      success: true,
      dashboard: {
        customer_name: customerName,
        customer_email: cust.email || email,
        outstanding_balance: parseFloat(balanceResult.rows[0].outstanding_balance),
        total_paid: parseFloat(paidResult.rows[0].total_paid),
        pending_quotes_count: parseInt(quotesResult.rows[0].pending_quotes_count),
        upcoming_jobs: jobsResult.rows,
        recent_invoices: invoicesResult.rows,
        pending_requests_count: parseInt(requestsResult.rows[0].pending_requests_count)
      }
    });
  } catch (error) {
    serverError(res, error, 'Portal dashboard error');
  }
});

// GET /api/portal/:token/photos - All completion photos
app.get('/api/portal/:token/photos', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });

    const result = await pool.query(
      `SELECT id, job_date, service_type, address, completion_photos
       FROM scheduled_jobs
       WHERE customer_id = $1 AND completion_photos IS NOT NULL
       ORDER BY job_date DESC`,
      [tokenData.customer_id]
    );

    const photos = result.rows.map(row => ({
      job_id: row.id,
      job_date: row.job_date,
      service_type: row.service_type,
      address: row.address,
      photos: row.completion_photos || []
    }));

    res.json({ success: true, photos });
  } catch (error) {
    serverError(res, error, 'Portal photos error');
  }
});

// POST /api/portal/:token/reviews - Submit a review
app.post('/api/portal/:token/reviews', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });

    const { rating, comment } = req.body;
    if (!rating || rating < 1 || rating > 5) {
      return res.status(400).json({ success: false, error: 'Rating must be between 1 and 5' });
    }

    await ensureCustomerReviewsTable();
    const result = await pool.query(
      `INSERT INTO customer_reviews (customer_id, rating, comment) VALUES ($1, $2, $3) RETURNING *`,
      [tokenData.customer_id, rating, comment || null]
    );

    const cust = await pool.query('SELECT name, first_name, last_name, email FROM customers WHERE id = $1', [tokenData.customer_id]);
    const c = cust.rows[0] || {};
    const custName = c.name || ((c.first_name || '') + (c.last_name ? ' ' + c.last_name : '')).trim() || 'Customer';
    const stars = '\u2605'.repeat(rating) + '\u2606'.repeat(5 - rating);

    await sendEmail('hello@pappaslandscaping.com', `New ${rating}-Star Review from ${custName}`, emailTemplate(`
      <h2 style="color:#2e403d;margin:0 0 16px;">New Customer Review</h2>
      <p><strong>${escapeHtml(custName)}</strong> left a review:</p>
      <div style="background:#f8fafc;border-radius:8px;padding:16px;margin:16px 0;">
        <p style="margin:0;font-size:24px;">${stars}</p>
        <p style="margin:8px 0 0;"><strong>Rating:</strong> ${rating}/5</p>
        ${comment ? `<p style="margin:8px 0 0;"><strong>Comment:</strong> ${escapeHtml(comment)}</p>` : ''}
      </div>
    `));

    res.json({ success: true, review: result.rows[0] });
  } catch (error) {
    serverError(res, error, 'Portal submit review error');
  }
});

// GET /api/portal/:token/reviews - Customer's own reviews
app.get('/api/portal/:token/reviews', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });

    await ensureCustomerReviewsTable();
    const result = await pool.query(
      'SELECT * FROM customer_reviews WHERE customer_id = $1 ORDER BY created_at DESC',
      [tokenData.customer_id]
    );
    res.json({ success: true, reviews: result.rows });
  } catch (error) {
    serverError(res, error, 'Portal get reviews error');
  }
});

// GET /api/portal/:token/google-review-url - Get Google review URL from settings
app.get('/api/portal/:token/google-review-url', async (req, res) => {
  try {
    const tokenData = await validatePortalToken(req.params.token);
    if (!tokenData) return res.status(404).json({ success: false, error: 'Invalid token' });

    const result = await pool.query("SELECT value FROM business_settings WHERE key = 'google_review_url'");
    const url = result.rows[0]?.value || '';
    res.json({ success: true, url });
  } catch (error) {
    serverError(res, error, 'Portal google review url error');
  }
});

// Admin: GET /api/reviews - All reviews with customer names
app.get('/api/reviews', authenticateToken, async (req, res) => {
  try {
    await ensureCustomerReviewsTable();
    const result = await pool.query(
      `SELECT cr.*, c.name as customer_name, c.email as customer_email
       FROM customer_reviews cr
       LEFT JOIN customers c ON cr.customer_id = c.id
       ORDER BY cr.created_at DESC`
    );
    res.json({ success: true, reviews: result.rows });
  } catch (error) {
    serverError(res, error, 'Admin get reviews error');
  }
});

// Admin: DELETE /api/reviews/:id - Delete a review
app.delete('/api/reviews/:id', authenticateToken, async (req, res) => {
  try {
    await ensureCustomerReviewsTable();
    const result = await pool.query('DELETE FROM customer_reviews WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Review not found' });
    res.json({ success: true });
  } catch (error) {
    serverError(res, error, 'Admin delete review error');
  }
});

// Admin: GET /api/service-requests - List service requests
app.get('/api/service-requests', async (req, res) => {
  try {
    const { status } = req.query;
    let query = `SELECT sr.*, c.name as customer_name, c.email as customer_email, c.phone as customer_phone FROM service_requests sr LEFT JOIN customers c ON sr.customer_id = c.id`;
    const params = [];
    if (status) { query += ' WHERE sr.status = $1'; params.push(status); }
    query += ' ORDER BY sr.created_at DESC';
    const result = await pool.query(query, params);
    res.json({ success: true, requests: result.rows });
  } catch (error) { serverError(res, error); }
});

// Admin: PATCH /api/service-requests/:id - Update status/notes
app.patch('/api/service-requests/:id', async (req, res) => {
  try {
    const { status, admin_notes } = req.body;
    const updates = [];
    const params = [];
    let p = 1;
    if (status) { updates.push(`status = $${p++}`); params.push(status); }
    if (admin_notes !== undefined) { updates.push(`admin_notes = $${p++}`); params.push(admin_notes); }
    updates.push('updated_at = NOW()');
    params.push(req.params.id);
    const result = await pool.query(`UPDATE service_requests SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`, params);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, request: result.rows[0] });
  } catch (error) { serverError(res, error); }
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

    const [expMonthly, allTimeRev, allTimeExp, totalInvoices, totalCustomers] = await Promise.all([
      pool.query(`SELECT to_char(expense_date,'YYYY-MM') as month,
        SUM(amount) as expenses FROM expenses WHERE expense_date >= NOW() - INTERVAL '12 months'
        GROUP BY month ORDER BY month`),
      pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid'"),
      pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses"),
      pool.query("SELECT COUNT(*) as cnt FROM invoices"),
      pool.query("SELECT COUNT(*) as cnt FROM customers")
    ]);

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
    serverError(res, error);
  }
});

// Build season kickoff email content (inner HTML for emailTemplate)
function buildKickoffContent(customerName, services, confirmUrl, properties, propertyServices) {
  const firstName = escapeHtml((customerName || 'Customer').split(' ')[0]);
  const snowFilter = s => {
    const l = s.name.toLowerCase();
    return !l.includes('snow') && !l.includes('salt') && !l.includes('deic');
  };

  // Email open tracking pixel
  const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
  const tokenMatch = confirmUrl && confirmUrl.match(/token=([a-f0-9]+)/);
  const trackingPixel = tokenMatch ? `<img src="${baseUrl}/api/season-kickoff/track/${tokenMatch[1]}" width="1" height="1" style="display:block;width:1px;height:1px;border:0;" alt="">` : '';

  const ctaButton = confirmUrl ? `
    <div style="text-align:center;margin:28px 0 24px;">
      <a href="${confirmUrl}" style="background:#2e403d;color:#ffffff;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Confirm or Request Changes</a>
    </div>
  ` : '';

  const buildTable = (svcs) => {
    const rows = svcs.filter(snowFilter).map(s => `
      <tr>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e5e5;font-size:14px;color:#334155;">${escapeHtml(s.name)}</td>
        <td style="padding:10px 16px;border-bottom:1px solid #e5e5e5;font-size:14px;color:#334155;text-align:right;font-weight:600;">${parseFloat(s.rate).toFixed(2)}</td>
      </tr>
    `).join('');
    if (!rows) return '';
    return `
    <table style="width:100%;border-collapse:collapse;border:1px solid #e5e5e5;border-radius:8px;overflow:hidden;margin-bottom:24px;">
      <thead>
        <tr style="background:#f8fafc;">
          <th style="padding:10px 16px;text-align:left;font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:2px solid #e5e5e5;">Service</th>
          <th style="padding:10px 16px;text-align:right;font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;border-bottom:2px solid #e5e5e5;">Rate</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
  };

  // Multi-property: show services grouped by property (only if 2+ properties have services)
  if (propertyServices && propertyServices.length > 1) {
    const sections = propertyServices.map(ps => {
      const table = buildTable(ps.services);
      if (!table) return '';
      return `
        <p style="font-size:15px;color:#2e403d;font-weight:700;margin:20px 0 8px;border-bottom:2px solid #e5e5e5;padding-bottom:6px;">${escapeHtml(ps.address)}</p>
        ${table}
      `;
    }).filter(Boolean).join('');

    if (!sections) return null;

    return `
      <h2 style="font-size:24px;color:#1e293b;font-weight:700;margin:0 0 20px;">You're on Our List for 2026!</h2>
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 16px;">Hi ${firstName},</p>
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 24px;">
        We hope you had a great winter! As we gear up for the 2026 season, we wanted to reach out and let you know that <strong>you're on our list</strong> for service this year.
      </p>
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 8px;">
        Here's a summary of the services we provided at each property last season:
      </p>
      ${sections}
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 24px;">
        <strong>Spring cleanups have already started</strong>, and <strong>mowing will begin in April</strong>. Let us know if everything looks right — just click below to confirm.
      </p>
      ${ctaButton}
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0;">
        Thank you for being a valued Pappas & Co. Landscaping customer. We look forward to another great season!
      </p>
      ${trackingPixel}`;
  }

  // Single property
  const filtered = services.filter(snowFilter);
  if (!filtered.length) return null;

  const props = (properties || []).filter(Boolean);
  const addressSection = props.length > 0 ? `
    <p style="font-size:15px;color:#2e403d;font-weight:700;margin:20px 0 8px;border-bottom:2px solid #e5e5e5;padding-bottom:6px;">${escapeHtml(props[0])}</p>
  ` : '';

  return `
    <h2 style="font-size:24px;color:#1e293b;font-weight:700;margin:0 0 20px;">You're on Our List for 2026!</h2>
    <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 16px;">Hi ${firstName},</p>
    <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 24px;">
      We hope you had a great winter! As we gear up for the 2026 season, we wanted to reach out and let you know that <strong>you're on our list</strong> for service this year.
    </p>
    ${addressSection}
    <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 20px;">
      Here's a summary of the services we provided for you last season:
    </p>
    ${buildTable(filtered)}
    <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 24px;">
      <strong>Spring cleanups have already started</strong>, and <strong>mowing will begin in April</strong>. Let us know if everything looks right — just click below to confirm.
    </p>
    ${ctaButton}
    <p style="font-size:15px;color:#475569;line-height:1.6;margin:0;">
      Thank you for being a valued Pappas & Co. Landscaping customer. We look forward to another great season!
    </p>
    ${trackingPixel}
  `;
}

// POST /api/season-kickoff/send-test - Send a test kickoff email
app.post('/api/season-kickoff/send-test', async (req, res) => {
  try {
    const { email, customerName, services, properties, propertyServices } = req.body;
    if (!email || !services) return res.status(400).json({ success: false, error: 'Email and services required' });
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const token = crypto.randomBytes(24).toString('hex');
    const confirmUrl = `${baseUrl}/confirm-services.html?token=${token}`;
    // Store token (for test, use a simple in-memory approach; real sends store in DB)
    await pool.query(`INSERT INTO season_kickoff_responses (token, customer_name, customer_email, services, properties, status) VALUES ($1, $2, $3, $4, $5, 'pending')`,
      [token, customerName, email, JSON.stringify(services), JSON.stringify(properties || [])]);
    const content = buildKickoffContent(customerName, services, confirmUrl, properties, propertyServices);
    if (!content) return res.status(400).json({ success: false, error: 'No eligible services' });
    const html = emailTemplate(content);
    const firstName = (customerName || 'Customer').split(' ')[0];
    await sendEmail(email, `You're on our list for 2026, ${escapeHtml(firstName)}!`, html, null, { type: 'season_kickoff', customer_name: customerName, confirm_token: token });
    res.json({ success: true });
  } catch (error) {
    serverError(res, error);
  }
});

// POST /api/season-kickoff/send-sms - Send season kickoff text message
app.post('/api/season-kickoff/send-sms', async (req, res) => {
  try {
    const { phone, customerName, services } = req.body;
    if (!phone || !services || !services.length) return res.status(400).json({ success: false, error: 'Phone and services required' });
    if (!twilioClient) return res.status(500).json({ success: false, error: 'SMS not configured' });

    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const token = crypto.randomBytes(24).toString('hex');
    const confirmUrl = `${baseUrl}/confirm-services.html?token=${token}`;

    // Store token in DB
    await pool.query(`INSERT INTO season_kickoff_responses (token, customer_name, customer_email, services, properties, status) VALUES ($1, $2, $3, $4, $5, 'pending')`,
      [token, customerName, phone, JSON.stringify(services), JSON.stringify([])]);

    const firstName = (customerName || 'Customer').split(' ')[0];
    const body = `Hi ${firstName}, it's Pappas & Co. Landscaping! We're gearing up for the 2026 season and you're on our list.\n\nSpring cleanups are underway and mowing kicks off in April. Review and confirm your services here:\n\n${confirmUrl}\n\nCall or text us anytime:\n440-886-7318`;

    let formattedTo = phone.replace(/\D/g, '');
    if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
    else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

    const twilioMessage = await twilioClient.messages.create({
      body,
      from: TWILIO_PHONE_NUMBER,
      to: formattedTo
    });

    // Log in messages table
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
    serverError(res, error);
  }
});

// POST /api/season-kickoff/send-bulk - Send kickoff emails to selected customers
app.post('/api/season-kickoff/send-bulk', async (req, res) => {
  try {
    const { customers } = req.body;
    if (!customers || !customers.length) return res.status(400).json({ success: false, error: 'No customers provided' });

    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
    const delay = (ms) => new Promise(r => setTimeout(r, ms));
    const results = { sent: 0, skipped: 0, errors: 0, details: [] };

    for (const cust of customers) {
      if (!cust.email || !cust.services || !cust.services.length) {
        results.skipped++;
        results.details.push({ name: cust.name, status: 'skipped', reason: 'No email or services' });
        continue;
      }

      try {
        const token = crypto.randomBytes(24).toString('hex');
        const confirmUrl = `${baseUrl}/confirm-services.html?token=${token}`;

        // Store token in DB
        await pool.query(
          `INSERT INTO season_kickoff_responses (token, customer_name, customer_email, services, properties, status) VALUES ($1, $2, $3, $4, $5, 'pending')`,
          [token, cust.name, cust.email, JSON.stringify(cust.services), JSON.stringify(cust.properties || [])]
        );

        const content = buildKickoffContent(cust.name, cust.services, confirmUrl, cust.properties, cust.propertyServices);
        if (!content) {
          results.skipped++;
          results.details.push({ name: cust.name, status: 'skipped', reason: 'No eligible services' });
          continue;
        }

        const html = emailTemplate(content);
        const firstName = (cust.name || 'Customer').split(' ')[0];
        const emailResult = await sendEmail(
          cust.email,
          `You're on our list for 2026, ${escapeHtml(firstName)}!`,
          html,
          null,
          { type: 'season_kickoff', customer_name: cust.name, confirm_token: token }
        );

        if (emailResult && emailResult.success) {
          results.sent++;
          results.details.push({ name: cust.name, status: 'sent' });
        } else {
          results.errors++;
          results.details.push({ name: cust.name, status: 'error' });
        }
      } catch (e) {
        console.error(`Season kickoff email error for ${cust.name}:`, e.message);
        results.errors++;
        results.details.push({ name: cust.name, status: 'error' });
      }

      // Rate limit: ~2 emails/sec to avoid Resend 429 errors
      await delay(1200);
    }

    res.json({ success: true, ...results });
  } catch (error) {
    serverError(res, error);
  }
});

// POST /api/season-kickoff/preview - Get email HTML for preview
app.post('/api/season-kickoff/preview', async (req, res) => {
  try {
    const { customerName, services, properties, propertyServices } = req.body;
    if (!services || !Array.isArray(services) || services.length === 0) {
      return res.status(400).json({ success: false, error: 'services array is required' });
    }
    const confirmUrl = '#preview';
    const content = buildKickoffContent(customerName, services, confirmUrl, properties, propertyServices);
    if (!content) return res.json({ success: false, error: 'No eligible services' });
    const html = emailTemplate(content);
    res.json({ success: true, html });
  } catch (error) {
    serverError(res, error);
  }
});

// GET /api/season-kickoff/responses - View all responses (admin)
app.get('/api/season-kickoff/responses', async (req, res) => {
  try {
    const result = await pool.query('SELECT id, customer_name, customer_email, services, properties, status, notes, viewed_at, view_count, email_opened_at, email_open_count, responded_at, created_at FROM season_kickoff_responses ORDER BY responded_at DESC NULLS LAST, created_at DESC');
    res.json({ success: true, responses: result.rows });
  } catch (error) {
    serverError(res, error);
  }
});

// PATCH /api/season-kickoff/responses/:id - Update a response's services (admin)
app.patch('/api/season-kickoff/responses/:id', async (req, res) => {
  try {
    const { services, properties, customer_email } = req.body;
    if (!services) return res.status(400).json({ success: false, error: 'Services required' });
    if (customer_email !== undefined) {
      await pool.query(
        `UPDATE season_kickoff_responses SET services = $1, properties = $2, customer_email = $3 WHERE id = $4`,
        [JSON.stringify(services), JSON.stringify(properties || []), customer_email, req.params.id]
      );
    } else {
      await pool.query(
        `UPDATE season_kickoff_responses SET services = $1, properties = $2 WHERE id = $3`,
        [JSON.stringify(services), JSON.stringify(properties || []), req.params.id]
      );
    }
    res.json({ success: true });
  } catch (error) {
    serverError(res, error);
  }
});

// DELETE /api/season-kickoff/responses/:id - Delete a response (admin)
app.delete('/api/season-kickoff/responses/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM season_kickoff_responses WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (error) {
    serverError(res, error);
  }
});

// POST /api/season-kickoff/reply - Reply to a customer's change request
app.post('/api/season-kickoff/reply', async (req, res) => {
  try {
    const { responseId, message } = req.body;
    if (!responseId || !message) return res.status(400).json({ success: false, error: 'Response ID and message required' });
    const result = await pool.query('SELECT * FROM season_kickoff_responses WHERE id = $1', [responseId]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Response not found' });
    const r = result.rows[0];
    if (!r.customer_email) return res.status(400).json({ success: false, error: 'No email address' });

    const html = emailTemplate(`
      <h2 style="font-size:22px;color:#1e293b;font-weight:700;margin:0 0 20px;">Hi ${escapeHtml((r.customer_name || 'there').split(' ')[0])},</h2>
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 16px;">
        Thanks for letting us know about the changes you'd like for the 2026 season. Here's our response:
      </p>
      <div style="background:#f8fafc;border-left:4px solid #2e403d;padding:16px 20px;margin:0 0 24px;border-radius:0 8px 8px 0;">
        <p style="font-size:15px;color:#334155;line-height:1.6;margin:0;">${escapeHtml(message)}</p>
      </div>
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0 0 16px;">
        If you have any other questions, feel free to reply to this email or call us at <strong>440-886-7318</strong>.
      </p>
      <p style="font-size:15px;color:#475569;line-height:1.6;margin:0;">
        Thank you for being a valued Pappas & Co. Landscaping customer. We look forward to another great season!
      </p>
    `);

    await sendEmail(r.customer_email, `Re: Your 2026 Service Changes — Pappas & Co. Landscaping`, html);
    res.json({ success: true });
  } catch (error) {
    serverError(res, error);
  }
});

// GET /api/season-kickoff/token-status - Check health of tokens vs emails sent
app.get('/api/season-kickoff/token-status', async (req, res) => {
  try {
    const tokens = await pool.query('SELECT COUNT(*) as count FROM season_kickoff_responses');
    const emails = await pool.query("SELECT COUNT(*) as count FROM email_log WHERE email_type = 'season_kickoff' AND status = 'sent' AND html_body IS NOT NULL");
    const missing = await pool.query(`
      SELECT COUNT(*) as count FROM email_log e
      WHERE e.email_type = 'season_kickoff' AND e.status = 'sent' AND e.html_body IS NOT NULL
        AND e.html_body ~ 'confirm-services\\.html\\?token=[a-f0-9]+'
        AND NOT EXISTS (
          SELECT 1 FROM season_kickoff_responses s
          WHERE s.token = (regexp_match(e.html_body, 'confirm-services\\.html\\?token=([a-f0-9]+)'))[1]
        )
    `);
    res.json({
      success: true,
      tokenCount: parseInt(tokens.rows[0].count),
      emailCount: parseInt(emails.rows[0].count),
      missingCount: parseInt(missing.rows[0].count)
    });
  } catch (error) {
    serverError(res, error, 'Error checking token status');
  }
});

// POST /api/season-kickoff/recover-tokens - Recover missing tokens from email_log
app.post('/api/season-kickoff/recover-tokens', async (req, res) => {
  try {
    const logs = await pool.query(
      `SELECT recipient_email, customer_name, html_body, sent_at FROM email_log
       WHERE email_type = 'season_kickoff' AND status = 'sent' AND html_body IS NOT NULL
       ORDER BY sent_at DESC`
    );

    let recovered = 0, alreadyExists = 0, noToken = 0;
    const details = [];

    for (const log of logs.rows) {
      const match = log.html_body.match(/confirm-services\.html\?token=([a-f0-9]+)/);
      if (!match) { noToken++; continue; }
      const token = match[1];

      const existing = await pool.query('SELECT id FROM season_kickoff_responses WHERE token = $1', [token]);
      if (existing.rows.length > 0) { alreadyExists++; continue; }

      const serviceRows = [];
      const serviceRegex = /<td[^>]*>([^<]+)<\/td>\s*<td[^>]*>\$([0-9.,]+)<\/td>/g;
      let sMatch;
      while ((sMatch = serviceRegex.exec(log.html_body)) !== null) {
        const name = sMatch[1].trim();
        if (name !== 'SERVICE' && name !== 'Rate') {
          serviceRows.push({ name, rate: parseFloat(sMatch[2].replace(',', '')) });
        }
      }

      await pool.query(
        `INSERT INTO season_kickoff_responses (token, customer_name, customer_email, services, status)
         VALUES ($1, $2, $3, $4, 'pending')
         ON CONFLICT (token) DO NOTHING`,
        [token, log.customer_name || '', log.recipient_email, JSON.stringify(serviceRows)]
      );

      recovered++;
      details.push({ name: log.customer_name, email: log.recipient_email, token: token.slice(0, 8) + '...' });
    }

    res.json({ success: true, recovered, alreadyExists, noToken, totalEmailsScanned: logs.rows.length, details });
  } catch (error) {
    console.error('Token recovery error:', error);
    res.status(500).json({ success: false, error: 'Something went wrong. Please try again.', debug: error.message });
  }
});

// GET /api/reports/2025-services - Customers who had services in 2025 (based on line item dates)
app.get('/api/reports/2025-services', async (req, res) => {
  try {
    // Get invoices that have 2025 line items (by item date, or by due_date when item date is missing)
    const result = await pool.query(`
      SELECT
        COALESCE(i.customer_id, c_name.id) as customer_id,
        COALESCE(
          NULLIF(COALESCE(c_id.name, c_name.name), ''),
          NULLIF(TRIM(COALESCE(c_id.first_name, c_name.first_name, '') || ' ' || COALESCE(c_id.last_name, c_name.last_name, '')), ''),
          i.customer_name,
          'Unknown'
        ) as customer_name,
        COALESCE(c_id.email, c_name.email, i.customer_email) as email,
        COALESCE(c_id.phone, c_id.mobile, c_name.phone, c_name.mobile) as phone,
        COALESCE(c_id.street, c_name.street, i.customer_address) as address,
        COALESCE(c_id.city, c_name.city) as city,
        COALESCE(c_id.status, c_name.status) as customer_status,
        i.line_items,
        i.due_date,
        i.created_at
      FROM invoices i
      LEFT JOIN customers c_id ON c_id.id = i.customer_id
      LEFT JOIN customers c_name ON i.customer_id IS NULL
        AND c_name.id = (
          SELECT c2.id FROM customers c2
          WHERE LOWER(TRIM(COALESCE(c2.name, c2.first_name || ' ' || c2.last_name))) = LOWER(TRIM(SPLIT_PART(i.customer_name, '#', 1)))
          LIMIT 1
        )
      WHERE (
        EXISTS (
          SELECT 1 FROM jsonb_array_elements(i.line_items) item
          WHERE item->>'date' >= '2025-01-01' AND item->>'date' < '2026-01-01'
        )
        OR (
          i.created_at >= '2025-05-01'
          AND i.due_date >= '2025-01-01' AND i.due_date < '2026-01-01'
          AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(i.line_items) item
            WHERE item->>'date' = '0000-00-00' OR item->>'date' IS NULL OR item->>'date' = ''
          )
        )
      )
      ORDER BY customer_name
    `);

    // Group by customer, only counting line items with 2025 service dates
    const customers = {};
    for (const inv of result.rows) {
      // Skip inactive customers
      if (inv.customer_status && inv.customer_status.toLowerCase() === 'inactive') continue;

      const cid = inv.customer_id || ('name:' + (inv.customer_name || 'Unknown').toLowerCase().trim());
      if (!customers[cid]) {
        customers[cid] = {
          customer_id: inv.customer_id,
          name: inv.customer_name,
          email: inv.email,
          phone: inv.phone,
          address: inv.address,
          city: inv.city,
          services: {},
          total_invoiced: 0
        };
      }

      // Only count line items with actual 2025 service dates
      const items = inv.line_items || [];
      for (const item of items) {
        if (!item.name) continue;
        let itemDate = item.date || '';
        // For 0000-00-00 dates, fall back to invoice due_date if the invoice was created in 2025
        const createdAt = inv.created_at ? inv.created_at.toISOString().slice(0, 10) : '';
        if ((itemDate === '0000-00-00' || itemDate === '' || !itemDate) && createdAt >= '2025-05-01') {
          const dd = inv.due_date ? inv.due_date.toISOString().slice(0, 10) : '';
          if (dd >= '2025-01-01' && dd < '2026-01-01') {
            itemDate = dd;
          } else {
            continue;
          }
        }
        if (itemDate < '2025-01-01' || itemDate >= '2026-01-01') continue;
        // Skip processing fees and fuel surcharges
        const lower = item.name.toLowerCase();
        if (lower.includes('processing fee') || lower.includes('fuel surcharge') || lower.includes('late fee')) continue;
        // Skip one-time project services that don't repeat
        if (lower.includes('landscaping') || lower.includes('river rock') || lower.includes('garbage removal') || lower.includes('mowing first cut') || lower.includes('stump grinding')) continue;
        // Skip generic "." entries
        if (item.name.trim() === '.') continue;

        let serviceName = item.name;
        const dashIdx = serviceName.indexOf(' - ');
        if (dashIdx > -1 && serviceName.toLowerCase().startsWith('property')) {
          serviceName = serviceName.substring(dashIdx + 3);
        }
        // Normalize service names
        if (serviceName.toLowerCase().trim() === 'fertilizing') serviceName = 'Fertilizing (Per Application)';
        if (serviceName.toLowerCase().trim() === 'spreading fertilizer') serviceName = 'Fertilizing (Per Application)';
        const amount = parseFloat(item.amount || 0);
        const rate = parseFloat(item.rate || 0);
        const effectiveDate = itemDate;
        if (!customers[cid].services[serviceName]) {
          customers[cid].services[serviceName] = { name: serviceName, rate, count: 0, total: 0, latestDate: effectiveDate, earliestRate: rate, earliestDate: effectiveDate, rates: {} };
        }
        if (effectiveDate <= customers[cid].services[serviceName].earliestDate) {
          customers[cid].services[serviceName].earliestRate = rate;
          customers[cid].services[serviceName].earliestDate = effectiveDate;
        }
        // Track all distinct rates with their latest date
        if (!customers[cid].services[serviceName].rates[rate] || effectiveDate >= customers[cid].services[serviceName].rates[rate]) {
          customers[cid].services[serviceName].rates[rate] = effectiveDate;
        }
        // Always use the latest rate (closest to end of season)
        if (effectiveDate >= customers[cid].services[serviceName].latestDate) {
          customers[cid].services[serviceName].rate = rate;
          customers[cid].services[serviceName].latestDate = effectiveDate;
        }
        customers[cid].services[serviceName].count++;
        customers[cid].services[serviceName].total += amount;
        customers[cid].total_invoiced += amount;
      }
    }

    // Apply minimum rates: Spring Cleanup = $100 minimum
    for (const cid of Object.keys(customers)) {
      for (const svcName of Object.keys(customers[cid].services)) {
        if (svcName.toLowerCase().includes('spring cleanup') && customers[cid].services[svcName].rate < 100) {
          customers[cid].services[svcName].rate = 100;
        }
      }
    }

    // Add manually-specified customers not captured by 2025 invoice query (scheduled in CRM for 2026)
    const manualAdditions = [
      { name: 'Beth Schaefer', services: [{ name: 'Mowing', rate: 40 }, { name: 'Spring Cleanup', rate: 450 }, { name: 'Mulch', rate: 1275 }, { name: 'Weed Control', rate: 150 }] },
      { name: 'Brennan Investments LLC', services: [{ name: 'Mowing', rate: 44 }], propertyRates: { '17427 Lake Avenue, Lakewood, OH, 44107': 44, '13000 Triskett Road, Cleveland, OH, 44111': 45 } },
      { name: 'CC Pkwy Owner LLC', services: [{ name: 'Mowing', rate: 0 }] },
      { name: 'Daniel Corrigan', services: [{ name: 'Spring Cleanup', rate: 243 }] },
      { name: 'David Fridrich', services: [{ name: 'Mowing', rate: 46 }] },
      { name: 'Eva Kovach', services: [{ name: 'Mowing', rate: 37 }] },
      { name: 'Frank Pezzano', services: [{ name: 'Mowing', rate: 36 }], propertyRates: { '3869 Silsby Road, Cleveland, OH, 44111': 36, '3587 West 146th Street, Cleveland, OH, 44111': 48 } },
      { name: 'Greg Stokley', services: [{ name: 'Mulch', rate: 250 }] },
      { name: 'John Noell', services: [{ name: 'Mowing', rate: 69 }] },
      { name: 'Lareesa Rice', services: [{ name: 'Mowing', rate: 35 }], propertyRates: { '10509 Jasper Avenue, Cleveland, OH, 44111': 35, '11917 Saint John Avenue, Cleveland, OH, 44111': 40 } },
      { name: 'Leo Oblak', services: [{ name: 'Mowing', rate: 0 }] },
      { name: 'Matthew Ditlevson', services: [{ name: 'Mowing (Bi-Weekly)', rate: 45 }], propertyRates: { '3737 West 134th Street, Cleveland, OH, 44111': 45, '3828 West 157th Street, Cleveland, OH, 44111': 45, '3319 Warren Road, Cleveland, OH, 44111': 44, '3325 Warren Road, Cleveland, OH, 44111': 50 } },
      { name: 'MLI Properties', services: [{ name: 'Mowing', rate: 39 }], propertyRates: { '13823 Clifton Boulevard, Lakewood, OH, 44107': 39, '13842 Clifton Boulevard, Lakewood, OH, 44107': 33, '1438 Owego Avenue, Lakewood, OH, 44107': 43, '1357 Riverside Drive, Lakewood, OH, 44107': 39 } },
      { name: 'Monta Demchak', services: [{ name: 'Mowing', rate: 38 }, { name: 'Mulch', rate: 220 }] },
      { name: 'The Cundiff Group', services: [{ name: 'Mowing (Bi-Weekly)', rate: 60 }], propertyRates: { '19133 Puritas Avenue, Cleveland, OH, 44135': 60, '3107 Warren Road, Cleveland, OH, 44111': 60 } },
      { name: 'Theresa Pappas', services: [{ name: 'Mowing', rate: 0 }] },
    ];
    // Look up these customers from DB and add if not already in the list
    const existingNames = new Set(Object.values(customers).map(c => (c.name || '').toLowerCase().trim()));
    for (const manual of manualAdditions) {
      if (existingNames.has(manual.name.toLowerCase().trim())) continue;
      const cLookup = await pool.query(
        `SELECT id, name, first_name, last_name, email, phone, mobile, street, city, state, postal_code, status
         FROM customers WHERE LOWER(TRIM(name)) = $1 OR LOWER(TRIM(CONCAT(first_name, ' ', last_name))) = $1
         ORDER BY CASE WHEN LOWER(TRIM(name)) = $1 THEN 0 ELSE 1 END LIMIT 1`,
        [manual.name.toLowerCase().trim()]
      );
      if (cLookup.rows.length === 0) continue;
      const c = cLookup.rows[0];
      // Don't skip inactive — these are explicitly scheduled for 2026 in CRM
      const cid = c.id;
      if (customers[cid]) continue;
      customers[cid] = {
        customer_id: c.id,
        name: c.name || ((c.first_name || '') + (c.last_name ? ' ' + c.last_name : '')).trim(),
        email: c.email,
        phone: c.phone || c.mobile,
        address: c.street,
        city: c.city,
        services: {},
        total_invoiced: 0
      };
      for (const svc of manual.services) {
        const rate = svc.name.toLowerCase().includes('spring cleanup') && svc.rate < 100 ? 100 : svc.rate;
        // For multi-property, populate multiple rates from propertyRates so the grouping logic works
        const svcRates = {};
        if (manual.propertyRates && svc.name.toLowerCase().includes('mow')) {
          for (const [, pRate] of Object.entries(manual.propertyRates)) {
            svcRates[pRate] = '2025-10-01';
          }
        } else {
          svcRates[rate] = '2025-10-01';
        }
        customers[cid].services[svc.name] = { name: svc.name, rate, count: 1, total: rate, latestDate: '2025-10-01', earliestRate: rate, earliestDate: '2025-04-01', rates: svcRates };
      }
    }

    // Get customers who were sent an annual care plan estimate — exclude them
    const acpResult = await pool.query(`SELECT DISTINCT customer_id FROM sent_quotes WHERE quote_type = 'monthly_plan' AND customer_id IS NOT NULL`);
    const acpCustomerIds = new Set(acpResult.rows.map(r => r.customer_id));

    // Fetch properties for all customers
    const customerIds = Object.values(customers).map(c => c.customer_id).filter(Boolean);
    const propsResult = customerIds.length > 0
      ? await pool.query(`SELECT customer_id, street, city, state, zip FROM properties WHERE customer_id = ANY($1) ORDER BY customer_id, street`, [customerIds])
      : { rows: [] };
    const propsMap = {};
    for (const p of propsResult.rows) {
      if (!propsMap[p.customer_id]) propsMap[p.customer_id] = [];
      const addr = [p.street, p.city, p.state, p.zip].filter(Boolean).join(', ');
      if (addr) propsMap[p.customer_id].push(addr);
    }

    // For multi-property customers, build property→rate mapping from scheduled_jobs
    const multiPropIds = Object.values(customers)
      .filter(c => c.customer_id && (propsMap[c.customer_id] || []).length > 1)
      .map(c => c.customer_id);
    // propRateMap: { customerId: { propertyAddr: mowingRate } }
    const propRateMap = {};
    if (multiPropIds.length > 0) {
      const sjResult = await pool.query(`
        SELECT DISTINCT sj.customer_id, sj.service_price::numeric as rate, p.street, p.city, p.state, p.zip
        FROM scheduled_jobs sj
        JOIN properties p ON p.customer_id = sj.customer_id AND sj.address LIKE '%' || p.zip || '%'
        WHERE sj.customer_id = ANY($1) AND sj.service_price > 0
        ORDER BY sj.customer_id, rate
      `, [multiPropIds]);
      for (const row of sjResult.rows) {
        if (!propRateMap[row.customer_id]) propRateMap[row.customer_id] = {};
        const addr = [row.street, row.city, row.state, row.zip].filter(Boolean).join(', ');
        propRateMap[row.customer_id][addr] = parseFloat(row.rate);
      }
    }

    // Inject propertyRates from manual additions into propRateMap
    for (const manual of manualAdditions) {
      if (!manual.propertyRates) continue;
      const cEntry = Object.values(customers).find(c => c.name && c.name.toLowerCase().trim() === manual.name.toLowerCase().trim());
      if (cEntry && cEntry.customer_id) {
        propRateMap[cEntry.customer_id] = {};
        for (const [addr, pRate] of Object.entries(manual.propertyRates)) {
          propRateMap[cEntry.customer_id][addr] = pRate;
        }
      }
    }

    // Convert to array, skip customers with no 2025 line items after filtering
    const list = Object.values(customers)
      .filter(c => Object.keys(c.services).length > 0)
      .filter(c => !acpCustomerIds.has(c.customer_id))
      .map(c => {
        const props = propsMap[c.customer_id] || [];
        const propRates = propRateMap[c.customer_id];
        if (props.length > 1) {
          // Multi-property customer
          const byProperty = {};
          for (const addr of props) byProperty[addr] = [];

          // Check if we can distinguish properties by rate (different zips = different rates)
          // Only distinguish if ALL properties have a matched rate AND the rates are actually different
          const propRateValues = propRates ? [...new Set(Object.values(propRates))] : [];
          const canDistinguish = propRates && propRateValues.length > 1 && Object.keys(propRates).length >= props.length;

          if (canDistinguish) {
            // Different rates per property — sort properties by mowing rate (low to high)
            const sortedProps = Object.entries(propRates).sort((a, b) => a[1] - b[1]);

            for (const svc of Object.values(c.services)) {
              const rateEntries = Object.entries(svc.rates).map(([r, d]) => ({ rate: parseFloat(r), date: d }));

              if (rateEntries.length === 1) {
                const matchAddr = sortedProps.find(([, mowRate]) => mowRate === rateEntries[0].rate);
                const addr = matchAddr ? matchAddr[0] : sortedProps[0][0];
                byProperty[addr].push({ name: svc.name, rate: svc.rate });
              } else {
                // Multiple rates — take the N most recent distinct rates
                const latestPerRate = {};
                for (const { rate, date } of rateEntries) {
                  if (!latestPerRate[rate] || date > latestPerRate[rate]) latestPerRate[rate] = date;
                }
                const recentRates = Object.entries(latestPerRate)
                  .map(([r, d]) => ({ rate: parseFloat(r), date: d }))
                  .sort((a, b) => b.date.localeCompare(a.date))
                  .slice(0, sortedProps.length);
                recentRates.sort((a, b) => a.rate - b.rate);
                for (let i = 0; i < sortedProps.length && i < recentRates.length; i++) {
                  byProperty[sortedProps[i][0]].push({ name: svc.name, rate: recentRates[i].rate });
                }
              }
            }
          } else {
            // Same zip / can't distinguish by rate — show same services under each property
            const svcs = Object.values(c.services).map(s => ({ name: s.name, rate: s.rate }));
            for (const addr of props) {
              byProperty[addr] = [...svcs];
            }
          }

          return {
            ...c,
            properties: props,
            propertyServices: Object.entries(byProperty)
              .filter(([, svcs]) => svcs.length > 0)
              .map(([addr, svcs]) => ({ address: addr, services: svcs })),
            services: Object.values(c.services).map(s => ({
              name: s.name, rate: s.rate, count: s.count, total: s.total,
              noIncrease: s.name.toLowerCase().includes('mowing') && s.rate <= s.earliestRate
            })).sort((a, b) => b.total - a.total),
            total_invoiced: Math.round(c.total_invoiced * 100) / 100
          };
        }
        // Single property: use latest rate only
        const isMowing = n => n.toLowerCase().includes('mowing');
        return {
          ...c,
          properties: props,
          services: Object.values(c.services).map(s => ({
            name: s.name, rate: s.rate, count: s.count, total: s.total,
            noIncrease: isMowing(s.name) && s.rate <= s.earliestRate
          })).sort((a, b) => b.total - a.total),
          total_invoiced: Math.round(c.total_invoiced * 100) / 100
        };
      }).sort((a, b) => a.name.localeCompare(b.name));

    res.json({ success: true, count: list.length, customers: list });
  } catch (error) {
    console.error('Error fetching 2025 services:', error);
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
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
    serverError(res, error);
  }
});

// GET /api/reports/sales-tax - Sales tax report (billed, collected, outstanding)
app.get('/api/reports/sales-tax', async (req, res) => {
  try {
    const { start_date, end_date, type = 'billed' } = req.query;
    if (!start_date || !end_date) {
      return res.status(400).json({ success: false, error: 'start_date and end_date are required' });
    }
    if (!['billed', 'collected', 'outstanding'].includes(type)) {
      return res.status(400).json({ success: false, error: 'type must be one of: billed, collected, outstanding' });
    }

    let query;
    if (type === 'billed') {
      query = `
        SELECT
          COALESCE(tax_rate, 0) as tax_rate,
          SUM(subtotal) as total_sales,
          SUM(CASE WHEN COALESCE(tax_rate, 0) > 0 THEN subtotal ELSE 0 END) as taxable_amount,
          0 as discount,
          SUM(COALESCE(tax_amount, 0)) as tax_amount
        FROM invoices
        WHERE status IN ('sent', 'paid')
          AND sent_at >= $1 AND sent_at <= $2
        GROUP BY COALESCE(tax_rate, 0)
        ORDER BY tax_rate
      `;
    } else if (type === 'collected') {
      query = `
        SELECT
          COALESCE(tax_rate, 0) as tax_rate,
          SUM(subtotal) as total_sales,
          SUM(CASE WHEN COALESCE(tax_rate, 0) > 0 THEN subtotal ELSE 0 END) as taxable_amount,
          0 as discount,
          SUM(COALESCE(tax_amount, 0)) as tax_amount
        FROM invoices
        WHERE status = 'paid'
          AND paid_at >= $1 AND paid_at <= $2
        GROUP BY COALESCE(tax_rate, 0)
        ORDER BY tax_rate
      `;
    } else {
      // outstanding
      query = `
        SELECT
          COALESCE(tax_rate, 0) as tax_rate,
          SUM(total - COALESCE(amount_paid, 0)) as total_sales,
          SUM(CASE WHEN COALESCE(tax_rate, 0) > 0 THEN (total - COALESCE(amount_paid, 0)) ELSE 0 END) as taxable_amount,
          0 as discount,
          SUM(COALESCE(tax_amount, 0)) as tax_amount
        FROM invoices
        WHERE status = 'sent'
          AND sent_at >= $1 AND sent_at <= $2
        GROUP BY COALESCE(tax_rate, 0)
        ORDER BY tax_rate
      `;
    }

    const result = await pool.query(query, [start_date, end_date]);
    const rows = result.rows.map(r => ({
      tax_rate: parseFloat(r.tax_rate) || 0,
      total_sales: parseFloat(r.total_sales) || 0,
      taxable_amount: parseFloat(r.taxable_amount) || 0,
      discount: parseFloat(r.discount) || 0,
      tax_amount: parseFloat(r.tax_amount) || 0
    }));

    const summary = {
      total_sales: rows.reduce((sum, r) => sum + r.total_sales, 0),
      taxable_amount: rows.reduce((sum, r) => sum + r.taxable_amount, 0),
      discount: rows.reduce((sum, r) => sum + r.discount, 0),
      tax_amount: rows.reduce((sum, r) => sum + r.tax_amount, 0)
    };

    // Processing fees from payments table
    const feesResult = await pool.query(`
      SELECT COALESCE(SUM(CASE WHEN method = 'square' THEN amount * 0.029 + 0.30 ELSE 0 END), 0) as processing_fees
      FROM payments
      WHERE status = 'succeeded' AND paid_at >= $1 AND paid_at <= $2
    `, [start_date, end_date]);

    const not_taxable = {
      processing_fees: parseFloat(feesResult.rows[0].processing_fees) || 0,
      tips: 0
    };

    res.json({
      success: true,
      type,
      start_date,
      end_date,
      rows,
      summary,
      not_taxable
    });
  } catch (error) {
    console.error('Sales tax report error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// QUICKBOOKS INTEGRATION (One-Way Sync: QB → Pappas)
// ═══════════════════════════════════════════════════════════

// --- QB Database Tables ---
async function ensureQBTables() {
  await _ensureQBTables(pool);
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
  // Encode the origin so the callback knows where to redirect back to
  const origin = req.query.origin || (req.protocol + '://' + req.get('host'));
  const oauthClient = createOAuthClient();
  const authUri = oauthClient.authorizeUri({
    scope: [OAuthClient.scopes.Accounting, OAuthClient.scopes.OpenId],
    state: 'origin:' + origin
  });
  console.log('🔑 QB Auth - redirect_uri:', process.env.QB_REDIRECT_URI);
  console.log('🔑 QB Auth - environment:', process.env.QB_ENVIRONMENT);
  console.log('🔑 QB Auth - origin:', origin);
  res.redirect(authUri);
});

// GET /api/quickbooks/callback - Handle OAuth callback
app.get('/api/quickbooks/callback', async (req, res) => {
  try {
    const oauthClient = createOAuthClient();
    // Build the full callback URL using the registered redirect URI for token exchange
    const redirectUri = process.env.QB_REDIRECT_URI || (req.protocol + '://' + req.get('host') + '/api/quickbooks/callback');
    const callbackUrl = redirectUri + '?' + new URL(req.protocol + '://' + req.get('host') + req.originalUrl).searchParams.toString();
    const authResponse = await oauthClient.createToken(callbackUrl);
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

    // Redirect back to the origin (localhost in dev, production URL in prod)
    const state = req.query.state || '';
    const originMatch = state.match(/^origin:(.+)$/);
    const returnTo = originMatch ? originMatch[1] : '';
    if (returnTo && returnTo.startsWith('http://localhost')) {
      res.redirect(returnTo + '/settings.html?qb=connected');
    } else {
      res.redirect('/settings.html?qb=connected');
    }
  } catch (e) {
    console.error('QB callback error:', e);
    res.redirect('/settings.html?qb=error&msg=' + encodeURIComponent(e.message));
  }
});

// GET /api/quickbooks/status - Check connection
app.get('/api/quickbooks/status', async (req, res) => {
  try {
    const [tokenRow, lastSync] = await Promise.all([
      pool.query('SELECT realm_id, expires_at, updated_at FROM qb_tokens ORDER BY id DESC LIMIT 1'),
      pool.query('SELECT * FROM qb_sync_log ORDER BY id DESC LIMIT 1')
    ]);

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
    serverError(res, e);
  }
});

// POST /api/quickbooks/disconnect - Remove tokens
app.post('/api/quickbooks/disconnect', async (req, res) => {
  try {
    await pool.query('DELETE FROM qb_tokens');
    res.json({ success: true, message: 'QuickBooks disconnected' });
  } catch (e) {
    serverError(res, e);
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

      // Upsert: match on qb_id first, then try name or email to prevent duplicates
      let existing = await pool.query('SELECT id FROM customers WHERE qb_id = $1', [qbId]);
      if (existing.rows.length === 0 && email) {
        existing = await pool.query('SELECT id FROM customers WHERE LOWER(TRIM(email)) = LOWER(TRIM($1)) AND (qb_id IS NULL OR qb_id = $2)', [email, qbId]);
      }
      if (existing.rows.length === 0 && name) {
        existing = await pool.query('SELECT id FROM customers WHERE LOWER(TRIM(name)) = LOWER(TRIM($1)) AND qb_id IS NULL', [name]);
      }
      if (existing.rows.length > 0) {
        await pool.query(
          `UPDATE customers SET name=COALESCE(NULLIF($1,''), name), email=COALESCE($2, email),
           phone=COALESCE($3, phone), mobile=COALESCE($4, mobile),
           street=COALESCE(NULLIF($5,''), street), street2=COALESCE($6, street2),
           city=COALESCE(NULLIF($7,''), city), state=COALESCE(NULLIF($8,''), state),
           postal_code=COALESCE(NULLIF($9,''), postal_code),
           customer_company_name=COALESCE($10, customer_company_name),
           qb_id=$11, updated_at=NOW() WHERE id=$12`,
          [name, email, phone, mobile, street, street2, city, state, zip, company, qbId, existing.rows[0].id]
        );
      } else {
        const newCustNum = await nextCustomerNumber();
        await pool.query(
          `INSERT INTO customers (customer_number, name, email, phone, mobile, street, street2, city, state, postal_code,
           customer_company_name, qb_id, status, created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'Active',NOW())`,
          [newCustNum, name, email, phone, mobile, street, street2, city, state, zip, company, qbId]
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

      // Skip invoices before 6000
      const numericInvNum = parseInt(invoiceNumber, 10);
      if (!isNaN(numericInvNum) && numericInvNum < 6000) continue;

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

  // Ensure qb_payment_id column exists
  try { await pool.query('ALTER TABLE payments ALTER COLUMN invoice_id DROP NOT NULL'); } catch(e) {}
  try { await pool.query('ALTER TABLE payments ADD COLUMN IF NOT EXISTS qb_payment_id VARCHAR(100)'); } catch(e) {}

  while (true) {
    const sinceFilter = changedSince ? ` WHERE Metadata.LastUpdatedTime >= '${changedSince}'` : '';
    const query = `SELECT * FROM Payment${sinceFilter} STARTPOSITION ${startPos} MAXRESULTS ${pageSize}`;
    const data = await qbApiGet(`query?query=${encodeURIComponent(query)}`);
    const payments = data?.QueryResponse?.Payment || [];
    if (payments.length === 0) break;

    for (const pmt of payments) {
      const qbPaymentId = String(pmt.Id);
      const paidAt = pmt.TxnDate || null;
      const totalAmount = parseFloat(pmt.TotalAmt) || 0;
      const customerName = pmt.CustomerRef?.name || 'Unknown';

      // Determine payment method
      let method = 'Other';
      if (pmt.PaymentMethodRef?.name) {
        method = pmt.PaymentMethodRef.name;
      } else if (pmt.CreditCardPayment) {
        method = 'Credit Card';
      }

      // Find customer
      let customerId = null;
      if (pmt.CustomerRef?.value) {
        const localCust = await pool.query('SELECT id FROM customers WHERE qb_id = $1', [String(pmt.CustomerRef.value)]);
        if (localCust.rows.length > 0) customerId = localCust.rows[0].id;
      }

      // Process each line to link to invoices
      const lines = pmt.Line || [];
      let linkedInvoiceId = null;
      for (const line of lines) {
        const invoiceRef = line.LinkedTxn?.find(lt => lt.TxnType === 'Invoice');
        if (invoiceRef) {
          const localInv = await pool.query('SELECT id FROM invoices WHERE qb_invoice_id = $1', [String(invoiceRef.TxnId)]);
          if (localInv.rows.length > 0) {
            linkedInvoiceId = localInv.rows[0].id;
            break;
          }
        }
      }

      // Upsert payment record
      const existing = await pool.query('SELECT id FROM payments WHERE qb_payment_id = $1', [qbPaymentId]);
      if (existing.rows.length > 0) {
        await pool.query(
          `UPDATE payments SET amount=$1, method=$2, status=$3, customer_id=$4, invoice_id=$5,
           paid_at=$6, updated_at=NOW() WHERE qb_payment_id=$7`,
          [totalAmount, method, 'completed', customerId, linkedInvoiceId, paidAt, qbPaymentId]
        );
      } else {
        await pool.query(
          `INSERT INTO payments (payment_id, qb_payment_id, amount, method, status, customer_id, invoice_id, paid_at, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
           ON CONFLICT (payment_id) DO UPDATE SET
             amount=EXCLUDED.amount, method=EXCLUDED.method, status=EXCLUDED.status,
             customer_id=EXCLUDED.customer_id, invoice_id=EXCLUDED.invoice_id,
             paid_at=EXCLUDED.paid_at, updated_at=NOW()`,
          ['QB-' + qbPaymentId, qbPaymentId, totalAmount, method, 'completed', customerId, linkedInvoiceId, paidAt]
        );
      }
      count++;
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
    serverError(res, e);
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
    serverError(res, e);
  }
});

// ═══════════════════════════════════════════════════════════
// ═══ TEMPLATE ENGINE & CRUD ══════════════════════════════
// ═══════════════════════════════════════════════════════════

// Template variable replacement
function replaceTemplateVars(str, data) {
  if (!str) return str;
  return str.replace(/\{(\w+)\}/g, (match, key) => {
    return data[key] !== undefined ? data[key] : match;
  });
}

// Get template from DB by slug
async function getTemplate(slug) {
  try {
    const result = await pool.query('SELECT * FROM email_templates WHERE slug = $1 AND is_active = true', [slug]);
    return result.rows[0] || null;
  } catch(e) { return null; }
}

// Render a template with fallback to hardcoded HTML
async function renderTemplate(slug, vars, fallbackSubject, fallbackHtml) {
  const template = await getTemplate(slug);
  if (template) {
    const subject = replaceTemplateVars(template.subject, vars);
    const body = replaceTemplateVars(template.body, vars);
    const wrapperOption = template.options?.wrapper || 'full';
    let html = wrapperOption === 'none' ? body : emailTemplate(body);
    // Replace unsubscribe_email in wrapper footer
    if (vars.unsubscribe_email) {
      html = html.replace(/\{unsubscribe_email\}/g, vars.unsubscribe_email);
    } else if (vars.customer_email) {
      html = html.replace(/\{unsubscribe_email\}/g, encodeURIComponent(vars.customer_email));
    }
    return { subject, html, fromTemplate: true };
  }
  return { subject: fallbackSubject, html: fallbackHtml, fromTemplate: false };
}

// Render SMS template with fallback
async function renderSmsTemplate(slug, vars, fallbackText) {
  const template = await getTemplate(slug);
  if (template && template.sms_body) {
    return { text: replaceTemplateVars(template.sms_body, vars), fromTemplate: true };
  }
  return { text: fallbackText, fromTemplate: false };
}

// Default template seeds
const DEFAULT_TEMPLATES = [
  { name: 'Quote Sent', slug: 'quote_sent', category: 'quotes', subject: 'Your Quote from Pappas & Co. Landscaping', body: '<h2 style="color:#2e403d">Your Quote is Ready</h2><p>Hi {customer_first_name},</p><p>We\'ve prepared a custom quote for your landscaping needs.</p><p><strong>Quote #{quote_number}</strong> — <strong>${quote_total}</strong></p><p><a href="{quote_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">View Your Quote</a></p>', sms_body: 'Hi {customer_first_name}, your quote #{quote_number} for ${quote_total} from Pappas & Co. is ready! View it here: {quote_link}', variables: '["customer_name","customer_first_name","quote_number","quote_total","quote_link","services_list"]' },
  { name: 'Follow-up Stage 1', slug: 'followup_stage_1', category: 'followups', subject: 'Following up on your quote — Pappas & Co.', body: '<h2 style="color:#2e403d">Still Interested?</h2><p>Hi {customer_first_name},</p><p>Just checking in on your quote #{quote_number}. We\'d love to get your lawn looking great!</p><p><a href="{quote_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Review Quote</a></p>', sms_body: 'Hi {customer_first_name}, just following up on your Pappas & Co. quote #{quote_number}. Any questions? Reply here or call (440) 886-7318.', variables: '["customer_first_name","quote_number","quote_total","quote_link"]' },
  { name: 'Follow-up Stage 2', slug: 'followup_stage_2', category: 'followups', subject: 'Your lawn care quote expires soon', body: '<h2 style="color:#2e403d">Don\'t Miss Out</h2><p>Hi {customer_first_name},</p><p>Your quote #{quote_number} is still available. We have limited availability this season — lock in your spot!</p><p><a href="{quote_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Accept Quote</a></p>', sms_body: 'Hi {customer_first_name}, your Pappas & Co. quote #{quote_number} expires soon! Lock in your spot: {quote_link}', variables: '["customer_first_name","quote_number","quote_link"]' },
  { name: 'Follow-up Stage 3', slug: 'followup_stage_3', category: 'followups', subject: 'Last chance — lawn care quote', body: '<h2 style="color:#2e403d">Last Chance</h2><p>Hi {customer_first_name},</p><p>This is our final follow-up on quote #{quote_number}. If you\'re still interested, we\'d love to work with you!</p><p><a href="{quote_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">View Quote</a></p>', sms_body: 'Last call, {customer_first_name}! Your Pappas & Co. quote #{quote_number} expires soon. Questions? Call us at (440) 886-7318.', variables: '["customer_first_name","quote_number","quote_link"]' },
  { name: 'Follow-up Stage 4', slug: 'followup_stage_4', category: 'followups', subject: 'We\'d love your feedback — Pappas & Co.', body: '<h2 style="color:#2e403d">We Value Your Feedback</h2><p>Hi {customer_first_name},</p><p>We noticed you haven\'t accepted quote #{quote_number}. Was there something we could improve? Your feedback helps us serve our community better.</p>', sms_body: '', variables: '["customer_first_name","quote_number"]' },
  { name: 'Invoice Sent', slug: 'invoice_sent', category: 'invoices', subject: 'Invoice {invoice_number} from Pappas & Co.', body: '<h2 style="color:#2e403d">Invoice {invoice_number}</h2><p>Hi {customer_first_name},</p><p>Here\'s your invoice from Pappas & Co. Landscaping.</p><div style="background:#f8fafc;border-radius:8px;padding:20px;margin:20px 0;text-align:center;"><p style="font-size:24px;font-weight:700;color:#2e403d;margin:0;">${invoice_total}</p><p style="color:#666;margin:4px 0;">Due: {invoice_due_date}</p></div><p><a href="{payment_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Pay Now</a></p>', sms_body: 'Hi {customer_first_name}, invoice {invoice_number} for ${invoice_total} from Pappas & Co. is ready. Pay here: {payment_link}', variables: '["customer_first_name","invoice_number","invoice_total","invoice_due_date","payment_link","balance_due"]' },
  { name: 'Payment Confirmation — Customer', slug: 'payment_confirmation_customer', category: 'payments', subject: 'Payment received — Thank you!', body: '<h2 style="color:#2e403d">Payment Received!</h2><p>Hi {customer_first_name},</p><p>We\'ve received your payment of <strong>${amount_paid}</strong> for invoice <strong>{invoice_number}</strong>.</p><p>Thank you for your business!</p>', sms_body: 'Thanks {customer_first_name}! We received your ${amount_paid} payment for invoice {invoice_number}. - Pappas & Co.', variables: '["customer_first_name","invoice_number","amount_paid"]' },
  { name: 'Payment Confirmation — Admin', slug: 'payment_confirmation_admin', category: 'payments', subject: 'Payment received: {invoice_number}', body: '<h2 style="color:#2e403d">Payment Received</h2><p><strong>{customer_name}</strong> paid <strong>${amount_paid}</strong> for invoice <strong>{invoice_number}</strong>.</p>', sms_body: '', variables: '["customer_name","invoice_number","amount_paid"]' },
  { name: 'Payment Reminder', slug: 'payment_reminder', category: 'invoices', subject: 'Reminder: Invoice {invoice_number} — ${balance_due} due', body: '<h2 style="color:#2e403d">Payment Reminder</h2><p>Hi {customer_first_name},</p><p>This is a friendly reminder that invoice <strong>{invoice_number}</strong> has a balance of <strong>${balance_due}</strong>{invoice_due_date}.</p><p><a href="{payment_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Pay Now — ${balance_due}</a></p><p>If you\'ve already sent payment, please disregard this.</p>', sms_body: 'Reminder: Invoice {invoice_number} has ${balance_due} due. Pay online: {payment_link} - Pappas & Co.', variables: '["customer_first_name","invoice_number","balance_due","invoice_due_date","payment_link"]' },
  { name: 'Portal Magic Link', slug: 'portal_magic_link', category: 'portal', subject: 'Your Pappas & Co. Customer Portal', body: '<h2 style="color:#2e403d">Your Customer Portal Access</h2><p>Hi {customer_first_name},</p><p>Click below to access your Pappas & Co. customer portal.</p><p><a href="{portal_link}" style="display:inline-block;padding:16px 40px;background:#2e403d;color:white;border-radius:8px;font-weight:700;font-size:16px;text-decoration:none;">Access Your Portal</a></p><p style="font-size:13px;color:#9ca09c;">This link is valid for 30 days.</p>', sms_body: 'Access your Pappas & Co. portal: {portal_link}', variables: '["customer_first_name","portal_link"]' },
  { name: 'Late Fee Applied', slug: 'late_fee_applied', category: 'invoices', subject: 'Late Fee Applied — Invoice {invoice_number}', body: '<h2 style="color:#dc4a4a">Late Fee Applied</h2><p>Hi {customer_first_name},</p><p>A late fee has been applied to invoice <strong>{invoice_number}</strong>, which is past due.</p><p><a href="{payment_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Pay Now</a></p>', sms_body: 'A late fee has been applied to your Pappas & Co. invoice {invoice_number}. Pay now: {payment_link}', variables: '["customer_first_name","invoice_number","balance_due","payment_link"]' },
  { name: 'Monthly Invoice', slug: 'monthly_invoice', category: 'invoices', subject: 'Monthly Invoice {invoice_number} — ${invoice_total}', body: '<h2 style="color:#2e403d">Monthly Invoice {invoice_number}</h2><p>Hi {customer_first_name},</p><p>Your monthly lawn care invoice is ready.</p><div style="background:#f8fafc;border-radius:8px;padding:20px;margin:20px 0;text-align:center;"><p style="font-size:28px;font-weight:700;color:#2e403d;margin:0;">${invoice_total}</p><p style="color:#666;margin:4px 0;">Monthly Lawn Care Plan</p></div><p><a href="{payment_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Pay Now</a></p>', sms_body: 'Your monthly Pappas & Co. invoice {invoice_number} for ${invoice_total} is ready. Pay: {payment_link}', variables: '["customer_first_name","invoice_number","invoice_total","payment_link"]' },
  { name: 'Service Request Received', slug: 'service_request_received', category: 'portal', subject: 'Service Request Received — {service_type}', body: '<h2 style="color:#2e403d">Service Request Received</h2><p>Hi {customer_first_name},</p><p>We\'ve received your service request and will review it shortly.</p><p><strong>Service:</strong> {service_type}</p><p>We\'ll be in touch soon!</p>', sms_body: 'We received your service request, {customer_first_name}! We\'ll review and get back to you soon. - Pappas & Co.', variables: '["customer_first_name","service_type"]' },
  { name: 'Quote Accepted — Admin', slug: 'quote_accepted_admin', category: 'quotes', subject: 'Quote #{quote_number} Accepted!', body: '<h2 style="color:#2e403d">Quote Accepted!</h2><p><strong>{customer_name}</strong> accepted quote <strong>#{quote_number}</strong> for <strong>${quote_total}</strong>.</p>', sms_body: '{customer_name} accepted quote #{quote_number} (${quote_total})!', variables: '["customer_name","quote_number","quote_total"]' },
  { name: 'Quote Declined — Admin', slug: 'quote_declined_admin', category: 'quotes', subject: 'Quote #{quote_number} Declined', body: '<h2 style="color:#dc4a4a">Quote Declined</h2><p><strong>{customer_name}</strong> declined quote <strong>#{quote_number}</strong>.</p>', sms_body: '', variables: '["customer_name","quote_number"]' },
  { name: 'Contract Signed', slug: 'contract_signed', category: 'quotes', subject: 'Contract Signed — {customer_name}', body: '<h2 style="color:#2e403d">Contract Signed!</h2><p><strong>{customer_name}</strong> has signed the service agreement for quote <strong>#{quote_number}</strong>.</p>', sms_body: '', variables: '["customer_name","quote_number","quote_total"]' },
  { name: 'Job Completed', slug: 'job_completed', category: 'system', subject: 'Service Completed — {service_type}', body: '<h2 style="color:#2e403d">Service Completed</h2><p>Hi {customer_first_name},</p><p>Your <strong>{service_type}</strong> service at <strong>{address}</strong> has been completed by {crew_name}.</p><p>Thank you for choosing Pappas & Co.!</p>', sms_body: 'Your {service_type} service has been completed! Thanks for choosing Pappas & Co. - (440) 886-7318', variables: '["customer_first_name","service_type","address","crew_name","job_date"]' },
  { name: 'Welcome Email', slug: 'welcome_email', category: 'marketing', subject: 'Welcome to Pappas & Co. Landscaping!', body: '<h2 style="color:#2e403d">Welcome to the Family!</h2><p>Hi {customer_first_name},</p><p>Thank you for choosing Pappas & Co. Landscaping! We\'re excited to help you keep your property looking beautiful.</p><p>If you ever need anything, just reply to this email or call us at (440) 886-7318.</p>', sms_body: 'Welcome to Pappas & Co., {customer_first_name}! We\'re excited to serve you. Questions? Call (440) 886-7318.', variables: '["customer_first_name","customer_name"]' },
  { name: 'Seasonal Promo', slug: 'seasonal_promo', category: 'marketing', subject: 'Spring Special — Save on Lawn Care!', body: '<h2 style="color:#2e403d">Spring Special!</h2><p>Hi {customer_first_name},</p><p>Spring is here! Book your spring cleanup and get 10% off. Contact us today.</p>', sms_body: 'Spring special from Pappas & Co.! Book a spring cleanup and save 10%. Call (440) 886-7318 to schedule.', variables: '["customer_first_name","customer_name"]' },
  { name: 'Review Request', slug: 'review_request', category: 'marketing', subject: 'How did we do? — Pappas & Co.', body: '<h2 style="color:#2e403d">How Did We Do?</h2><p>Hi {customer_first_name},</p><p>We hope you\'re happy with your recent service! If so, we\'d love a Google review. It helps us grow and serve more neighbors like you.</p>', sms_body: 'Hi {customer_first_name}! Enjoy your recent service from Pappas & Co.? We\'d love a Google review! It really helps us out.', variables: '["customer_first_name"]' },
  { name: 'Appointment Reminder', slug: 'appointment_reminder', category: 'system', subject: 'Service Tomorrow — {service_type}', body: '<h2 style="color:#2e403d">Service Reminder</h2><p>Hi {customer_first_name},</p><p>Just a reminder that your <strong>{service_type}</strong> service is scheduled for <strong>{job_date}</strong> at <strong>{address}</strong>.</p><p>Please ensure gates are unlocked and the area is accessible.</p>', sms_body: 'Reminder: Your {service_type} with Pappas & Co. is tomorrow at {address}. Please unlock gates! Questions? (440) 886-7318', variables: '["customer_first_name","service_type","job_date","address"]' },
  { name: 'Campaign Email', slug: 'campaign_email', category: 'marketing', subject: '{subject}', body: '<p>{body}</p>', sms_body: '{body}', variables: '["customer_first_name","customer_name","subject","body","company_name","company_phone"]' },
  { name: 'Contract Unsigned Reminder', slug: 'contract_unsigned_reminder', category: 'quotes', subject: 'One more step — sign your service agreement', body: '<h2 style="color:#2e403d">Almost There!</h2><p>Hi {customer_first_name},</p><p>Great news — you accepted your quote <strong>#{quote_number}</strong>! There\'s just one more step before we can get you on the schedule.</p><p>Please sign your service agreement so we can lock in your spot:</p><p><a href="{contract_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Sign Service Agreement</a></p><p style="color:#666;">It only takes about 30 seconds. If you have any questions, reply to this email or call us at (440) 886-7318.</p>', sms_body: 'Hi {customer_first_name}! You accepted your quote from Pappas & Co. but we still need your signature to get started. Sign here: {contract_link}', variables: '["customer_first_name","customer_name","quote_number","quote_total","contract_link"]' },
  { name: 'Contract Unsigned — Final Reminder', slug: 'contract_unsigned_final', category: 'quotes', subject: 'Don\'t lose your spot — service agreement still needs your signature', body: '<h2 style="color:#2e403d">Your Spot is Waiting</h2><p>Hi {customer_first_name},</p><p>We still need your signed service agreement for quote <strong>#{quote_number}</strong> before we can schedule your service.</p><p>Our schedule fills up fast — please sign today so we don\'t have to give away your spot:</p><p><a href="{contract_link}" style="display:inline-block;padding:14px 32px;background:#2e403d;color:white;border-radius:8px;font-weight:700;text-decoration:none;">Sign Now — Takes 30 Seconds</a></p><p style="color:#666;">Questions? Call us at (440) 886-7318 or just reply to this email.</p>', sms_body: 'Hi {customer_first_name}, friendly reminder — we need your signed agreement before we can schedule your service. Sign here: {contract_link}', variables: '["customer_first_name","customer_name","quote_number","contract_link"]' },
  { name: 'Referral Announcement', slug: 'referral_announcement', category: 'marketing', subject: 'Know a neighbor who needs a landscaper? Get a free mow.', body: '<h2 style="color:#2e403d;margin:0 0 4px;">Refer a Neighbor, Get a Free Mow</h2><p style="font-size:13px;color:#94a3b8;margin:0 0 24px;">No limit. No codes. No forms.</p><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 8px;">Hi {customer_first_name}, it\'s Tim.</p><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 20px;">If you know a neighbor who could use a good landscaper, send them our way. For every neighbor who signs up and mentions your name, you\'ll get a free mow on us.</p><table width="100%" cellpadding="0" cellspacing="0" style="background:#e8f0e4;border-radius:10px;margin:0 0 24px;"><tr><td style="padding:20px 24px;text-align:center;"><p style="font-size:16px;font-weight:700;color:#2e403d;margin:0 0 4px;">1 Referral = 1 Free Mow</p><p style="font-size:13px;color:#4a5568;margin:0;">No limit. The more neighbors you refer, the more free mows you earn.</p></td></tr></table><p style="font-size:14px;font-weight:700;color:#2e403d;margin:0 0 10px;">How it works:</p><table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 24px;"><tr><td style="padding:8px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:32px;vertical-align:top;"><span style="display:inline-block;width:24px;height:24px;background:#c9dd80;color:#2e403d;border-radius:50%;text-align:center;line-height:24px;font-weight:700;font-size:12px;">1</span></td><td style="font-size:14px;color:#4a5568;line-height:1.5;">Tell your neighbor about us</td></tr></table></td></tr><tr><td style="padding:8px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:32px;vertical-align:top;"><span style="display:inline-block;width:24px;height:24px;background:#c9dd80;color:#2e403d;border-radius:50%;text-align:center;line-height:24px;font-weight:700;font-size:12px;">2</span></td><td style="font-size:14px;color:#4a5568;line-height:1.5;">They call or text us and mention your name</td></tr></table></td></tr><tr><td style="padding:8px 0;"><table cellpadding="0" cellspacing="0"><tr><td style="width:32px;vertical-align:top;"><span style="display:inline-block;width:24px;height:24px;background:#c9dd80;color:#2e403d;border-radius:50%;text-align:center;line-height:24px;font-weight:700;font-size:12px;">3</span></td><td style="font-size:14px;color:#4a5568;line-height:1.5;">You get a free mow on your next service</td></tr></table></td></tr></table><p style="font-size:12px;color:#94a3b8;text-align:center;margin:0 0 20px;">We service Lakewood, Bay Village, Brook Park, and Westpark.</p><p style="text-align:center;margin:0 0 6px;"><a href="tel:4408867318" style="display:inline-block;padding:12px 36px;background:#c9dd80;color:#2e403d;border-radius:50px;font-weight:700;font-size:14px;text-decoration:none;">Send Them Our Way</a></p><p style="text-align:center;font-size:12px;color:#94a3b8;margin:6px 0 0;">Call or text (440) 886-7318</p>', sms_body: 'Hi {customer_first_name}, it\'s Tim from Pappas & Co. Got a neighbor who could use a good landscaper? Send them our way and you\'ll get a free mow. No limit. They just mention your name when they reach out. We service Lakewood, Bay Village, Brook Park, and Westpark.', variables: '["customer_first_name","customer_name"]' },
  { name: 'Referral Follow-up', slug: 'referral_followup', category: 'marketing', subject: 'A free mow is waiting for you', body: '<h2 style="color:#2e403d;margin:0 0 24px;">A Free Mow is Waiting for You</h2><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 8px;">Hi {customer_first_name}, just wanted to make sure you saw this.</p><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 20px;">We\'re offering a free mow for every neighbor you refer to Pappas & Co. No forms, no codes. They just mention your name when they call or text us.</p><table width="100%" cellpadding="0" cellspacing="0" style="background:#e8f0e4;border-radius:10px;margin:0 0 24px;"><tr><td style="padding:20px 24px;text-align:center;"><p style="font-size:16px;font-weight:700;color:#2e403d;margin:0 0 4px;">1 Referral = 1 Free Mow</p><p style="font-size:13px;color:#4a5568;margin:0;">No limit. Tell a neighbor, get a free mow. Simple as that.</p></td></tr></table><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 20px;">Know someone whose yard could use some help?</p><p style="text-align:center;margin:0 0 6px;"><a href="tel:4408867318" style="display:inline-block;padding:12px 36px;background:#c9dd80;color:#2e403d;border-radius:50px;font-weight:700;font-size:14px;text-decoration:none;">Send Them Our Way</a></p><p style="text-align:center;font-size:12px;color:#94a3b8;margin:6px 0 0;">Call or text (440) 886-7318</p>', sms_body: 'Hi {customer_first_name}, Tim from Pappas & Co. here. Just making sure you saw this. Free mow for every neighbor you refer. No forms, no codes. They just mention your name when they call or text us at (440) 886-7318.', variables: '["customer_first_name","customer_name"]' },
  { name: 'Referral Thank You', slug: 'referral_thank_you', category: 'marketing', subject: 'You earned a free mow!', body: '<h2 style="color:#2e403d;margin:0 0 24px;">You Earned a Free Mow!</h2><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 8px;">Hey {customer_first_name}!</p><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 20px;">Your neighbor <strong>{referred_name}</strong> signed up and mentioned your name, so you\'ve got a free mow coming. We\'ll apply it to your next service.</p><table width="100%" cellpadding="0" cellspacing="0" style="background:#e8f0e4;border-radius:10px;margin:0 0 24px;"><tr><td style="padding:16px 24px;text-align:center;"><p style="font-size:15px;font-weight:700;color:#2e403d;margin:0;">Free mow credit applied</p></td></tr></table><p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 0;">Thanks for helping us grow in the neighborhood. Know another neighbor? Every referral earns another free mow.</p>', sms_body: 'Hey {customer_first_name}! It\'s Tim from Pappas & Co. Your neighbor {referred_name} signed up and mentioned your name, so you\'ve got a free mow coming. We\'ll apply it to your next service. Thanks for helping us grow in the neighborhood!', variables: '["customer_first_name","customer_name","referred_name"]' },
];

// Seed default templates
async function seedDefaultTemplates() {
  for (const t of DEFAULT_TEMPLATES) {
    try {
      await pool.query(
        `INSERT INTO email_templates (name, slug, category, subject, body, sms_body, variables, is_default, is_active)
         VALUES ($1, $2, $3, $4, $5, $6, $7, true, true)
         ON CONFLICT (slug) DO UPDATE SET body = EXCLUDED.body, sms_body = EXCLUDED.sms_body, subject = EXCLUDED.subject, variables = EXCLUDED.variables WHERE email_templates.is_default = true`,
        [t.name, t.slug, t.category, t.subject, t.body, t.sms_body, t.variables]
      );
    } catch(e) { /* ignore dups */ }
  }
}
// Run seed after tables are created (deferred via setTimeout to let migrations complete)
setTimeout(() => seedDefaultTemplates().then(() => console.log('✅ Default templates seeded')).catch(e => console.error('Template seed error:', e.message)), 5000);

// Template CRUD endpoints
app.get('/api/templates', async (req, res) => {
  try {
    const { category } = req.query;
    let query = 'SELECT * FROM email_templates';
    const params = [];
    if (category) { query += ' WHERE category = $1'; params.push(category); }
    query += ' ORDER BY category, name';
    const result = await pool.query(query, params);
    res.json({ success: true, templates: result.rows });
  } catch (error) { serverError(res, error); }
});

app.post('/api/templates', async (req, res) => {
  try {
    const { name, slug, category, channel, subject, body, sms_body, variables, is_active, options } = req.body;
    if (!name || !slug) return res.status(400).json({ success: false, error: 'name and slug required' });
    const result = await pool.query(
      `INSERT INTO email_templates (name, slug, category, channel, subject, body, sms_body, variables, is_active, options)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING *`,
      [name, slug, category || 'system', channel || 'email', subject, body, sms_body, JSON.stringify(variables || []), is_active !== false, JSON.stringify(options || {})]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/templates/:id', async (req, res) => {
  try {
    const fields = ['name', 'slug', 'category', 'channel', 'subject', 'body', 'sms_body', 'variables', 'is_active', 'is_default', 'options'];
    const updates = [];
    const params = [];
    let p = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) {
        const val = (f === 'variables' || f === 'options') ? JSON.stringify(req.body[f]) : req.body[f];
        updates.push(`${f} = $${p++}`);
        params.push(val);
      }
    }
    if (updates.length === 0) return res.status(400).json({ success: false, error: 'No fields to update' });
    updates.push('updated_at = NOW()');
    params.push(req.params.id);
    const result = await pool.query(`UPDATE email_templates SET ${updates.join(', ')} WHERE id = $${p} RETURNING *`, params);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/templates/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM email_templates WHERE id = $1 AND is_default = false RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(400).json({ success: false, error: 'Cannot delete default template or not found' });
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

app.post('/api/templates/:id/duplicate', async (req, res) => {
  try {
    const orig = await pool.query('SELECT * FROM email_templates WHERE id = $1', [req.params.id]);
    if (orig.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
    const t = orig.rows[0];
    const newSlug = t.slug + '_copy_' + Date.now();
    const result = await pool.query(
      `INSERT INTO email_templates (name, slug, category, subject, body, sms_body, variables, is_active, options)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [t.name + ' (Copy)', newSlug, t.category, t.subject, t.body, t.sms_body, JSON.stringify(t.variables), true, JSON.stringify(t.options)]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/templates/preview', async (req, res) => {
  try {
    const { slug, vars = {} } = req.body;
    const template = await getTemplate(slug);
    if (!template) return res.status(404).json({ success: false, error: 'Template not found' });
    const subject = replaceTemplateVars(template.subject, vars);
    const body = replaceTemplateVars(template.body, vars);
    res.json({ success: true, subject, html: emailTemplate(body) });
  } catch (error) { serverError(res, error); }
});

app.post('/api/templates/send-preview', async (req, res) => {
  try {
    const { template_id, slug, subject: directSubject, html_content: directHtml, to } = req.body;
    const sampleVars = { customer_name: 'Jane Smith', customer_first_name: 'Jane', customer_email: 'jane@example.com', customer_phone: '(440) 555-0123', customer_address: '123 Main St, Lakewood OH 44107', invoice_number: 'INV-1234', invoice_total: '285.00', invoice_due_date: 'March 15, 2026', amount_paid: '285.00', balance_due: '285.00', payment_link: '#preview', quote_number: 'Q-5678', quote_total: '1,250.00', quote_link: '#preview', services_list: 'Weekly Mowing, Spring Cleanup', job_date: 'March 10, 2026', service_type: 'Weekly Mowing', crew_name: 'Crew A', address: '123 Main St, Lakewood OH', company_name: 'Pappas & Co. Landscaping', company_phone: '(440) 886-7318', company_email: 'hello@pappaslandscaping.com', company_website: 'pappaslandscaping.com', portal_link: '#preview' };

    let subject, body;

    if (directSubject && directHtml) {
      // Direct content from the new templates editor
      subject = directSubject;
      body = directHtml;
    } else {
      // Look up from database
      let template;
      if (template_id) {
        // Try message_templates first (new table), then email_templates (legacy)
        let r = await pool.query('SELECT * FROM message_templates WHERE id = $1', [template_id]).catch(() => ({ rows: [] }));
        if (r.rows.length > 0) {
          template = { subject: r.rows[0].subject, body: r.rows[0].html_content };
        } else {
          r = await pool.query('SELECT * FROM email_templates WHERE id = $1', [template_id]).catch(() => ({ rows: [] }));
          template = r.rows[0];
        }
      } else if (slug) {
        template = await getTemplate(slug);
      }
      if (!template) return res.status(404).json({ success: false, error: 'Template not found' });
      subject = template.subject;
      body = template.body || template.html_content;
    }

    const finalSubject = replaceTemplateVars(subject, sampleVars);
    const finalBody = replaceTemplateVars(body, sampleVars);
    const recipient = to || 'hello@pappaslandscaping.com';
    await sendEmail(recipient, `[TEST] ${finalSubject}`, emailTemplate(finalBody));
    res.json({ success: true, message: 'Test email sent to ' + recipient });
  } catch (error) { serverError(res, error); }
});

app.get('/api/templates/variables', (req, res) => {
  res.json({
    success: true,
    variables: {
      customer: ['customer_name', 'customer_first_name', 'customer_email', 'customer_phone', 'customer_address'],
      invoice: ['invoice_number', 'invoice_total', 'invoice_due_date', 'amount_paid', 'balance_due', 'payment_link'],
      quote: ['quote_number', 'quote_total', 'quote_link', 'services_list'],
      job: ['job_date', 'service_type', 'crew_name', 'address'],
      company: ['company_name', 'company_phone', 'company_email', 'company_website', 'portal_link']
    }
  });
});

// GET /api/templates/library - Pre-built professional template library
app.get('/api/templates/library', (req, res) => {
  const library = [
    {
      id: 'spring-cleanup',
      name: 'Spring Cleanup Promotion',
      category: 'marketing',
      description: 'Seasonal promo for spring cleanup services with CTA',
      subject: 'Spring is here — time to refresh your yard!',
      sms_body: 'Hi {customer_first_name}! Spring is here and your yard is calling. Book your spring cleanup today: {portal_link} — Tim, Pappas & Co.',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Spring Is Here, {customer_first_name}!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">The snow has melted, and your property is ready for some fresh attention. Our spring cleanup crew is booking fast — let&rsquo;s get your yard looking its best before the growing season kicks off.</p>
<table width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0;" role="presentation"><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F33F;</td><td><strong style="color:#2e403d;">Debris &amp; Leaf Removal</strong><br><span style="color:#64748b;font-size:13px;">Clear winter buildup from beds, lawn, and hardscapes</span></td></tr></table></td></tr><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x2702;&#xFE0F;</td><td><strong style="color:#2e403d;">Bed Edging &amp; Mulch Prep</strong><br><span style="color:#64748b;font-size:13px;">Crisp edges and fresh beds ready for mulch</span></td></tr></table></td></tr><tr><td style="padding:12px 0;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F3E1;</td><td><strong style="color:#2e403d;">First Mow of the Season</strong><br><span style="color:#64748b;font-size:13px;">Get your lawn off to the right start</span></td></tr></table></td></tr></table>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Book Spring Cleanup</a></div>`
    },
    {
      id: 'fall-leaf-removal',
      name: 'Fall Leaf Removal Campaign',
      category: 'marketing',
      description: 'Seasonal promo for fall leaf removal and winterization',
      subject: 'Leaves are falling — let us handle the cleanup',
      sms_body: 'Hi {customer_first_name}! Leaves piling up? We\'ve got you. Book fall cleanup before spots fill up: {portal_link} — Tim',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Fall Cleanup Time, {customer_first_name}</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">The leaves are coming down, and your lawn needs protection before winter. Our fall cleanup includes everything to get your property ready for the cold months ahead.</p>
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px 24px;margin:24px 0;text-align:center;"><p style="color:#2e403d;font-size:18px;font-weight:700;margin:0 0 6px;">Fall Cleanup Includes</p><p style="color:#374151;font-size:14px;margin:0;">Leaf removal &bull; Gutter clearing &bull; Bed cleanup &bull; Final mow &bull; Winterization prep</p></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Spots fill up fast this time of year. Let us know if you'd like to get on the schedule.</p>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Schedule Fall Cleanup</a></div>`
    },
    {
      id: 'service-recap',
      name: 'Monthly Service Recap',
      category: 'system',
      description: 'Summary of completed work this month',
      subject: 'Your monthly service recap from Pappas & Co.',
      sms_body: 'Hi {customer_first_name}, your monthly service recap is ready! Check your email for details. — Tim, Pappas & Co.',
      body: `<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">Your Monthly Recap</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name}, here&rsquo;s a summary of the work we completed at your property this month.</p>
<hr style="border:none;border-top:2px solid #e5e7eb;margin:24px 0;">
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;"><strong style="color:#2e403d;">Services Completed:</strong> {services_list}</p>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;"><strong style="color:#2e403d;">Address:</strong> {customer_address}</p>
<hr style="border:none;border-top:2px solid #e5e7eb;margin:24px 0;">
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Have questions or want to adjust your services? Just reply to this email or give us a call.</p>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">View Your Portal</a></div>`
    },
    {
      id: 'new-customer-welcome',
      name: 'New Customer Welcome',
      category: 'portal',
      description: 'Welcome email for new customers with portal intro',
      subject: 'Welcome to Pappas & Co. Landscaping!',
      sms_body: 'Welcome to Pappas & Co., {customer_first_name}! We\'re excited to work with you. Check your email for your portal access. — Tim',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Welcome to the Family, {customer_first_name}!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">We&rsquo;re thrilled to have you as part of the Pappas &amp; Co. Landscaping family. Tim and the team are looking forward to taking care of your property.</p>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Your customer portal is ready. Here&rsquo;s what you can do:</p>
<table width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0;" role="presentation"><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F4C5;</td><td><strong style="color:#2e403d;">View Your Schedule</strong><br><span style="color:#64748b;font-size:13px;">See upcoming services and past visits</span></td></tr></table></td></tr><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F4B3;</td><td><strong style="color:#2e403d;">Pay Invoices Online</strong><br><span style="color:#64748b;font-size:13px;">Quick, secure payments anytime</span></td></tr></table></td></tr><tr><td style="padding:12px 0;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F4AC;</td><td><strong style="color:#2e403d;">Message Us Directly</strong><br><span style="color:#64748b;font-size:13px;">Questions, requests, or feedback — we&rsquo;re here</span></td></tr></table></td></tr></table>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Access Your Portal</a></div>`
    },
    {
      id: 'service-reminder',
      name: 'Service Day Reminder',
      category: 'system',
      description: 'Remind customer about tomorrow\'s scheduled service',
      subject: 'Reminder: {service_type} tomorrow at your property',
      sms_body: 'Hi {customer_first_name}! Friendly reminder — we\'ll be at {address} tomorrow for {service_type}. See you then! — Tim',
      body: `<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">Service Reminder</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name}, just a quick heads-up that we&rsquo;ll be at your property tomorrow!</p>
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px 24px;margin:24px 0;"><table width="100%" cellpadding="0" cellspacing="0"><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Service:</strong> <span style="color:#374151;">{service_type}</span></td></tr><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Date:</strong> <span style="color:#374151;">{job_date}</span></td></tr><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Address:</strong> <span style="color:#374151;">{address}</span></td></tr><tr><td style="padding:6px 0;"><strong style="color:#2e403d;">Crew:</strong> <span style="color:#374151;">{crew_name}</span></td></tr></table></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">No need to be home — we&rsquo;ll take care of everything. If you have any special instructions, just reply to this email.</p>`
    },
    {
      id: 'rate-adjustment',
      name: 'Annual Rate Adjustment Notice',
      category: 'invoices',
      description: 'Professional notification of pricing changes',
      subject: 'A note about your 2026 service rates',
      sms_body: 'Hi {customer_first_name}, we sent you an important update about your service rates for the coming year. Please check your email when you get a chance. — Tim',
      body: `<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">A Note About Your Service Rates</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name},</p>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">I wanted to reach out personally about a small adjustment to our service rates for the coming season. As costs for fuel, equipment, and materials continue to rise, we&rsquo;re making a modest increase to keep delivering the same quality you&rsquo;ve come to expect.</p>
<blockquote style="border-left:4px solid #c9dd80;padding:16px 20px;margin:24px 0;background:#f8fafc;border-radius:0 8px 8px 0;"><p style="color:#374151;font-size:15px;line-height:1.8;margin:0;font-style:italic;">We value your business and work hard to keep our pricing fair while maintaining the high standards you deserve.</p></blockquote>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Your updated rates will take effect at the start of the new season. If you have any questions, please don&rsquo;t hesitate to reach out — I&rsquo;m always happy to chat.</p>`
    },
    {
      id: 'winter-dormancy',
      name: 'Winter Season End',
      category: 'marketing',
      description: '"See you in spring" end-of-season message',
      subject: 'Wrapping up for the season — see you in spring!',
      sms_body: 'Hi {customer_first_name}! Another great season in the books. We\'ll see you in spring. Have a wonderful winter! — Tim, Pappas & Co.',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Another Great Season in the Books!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Hi {customer_first_name},</p>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">As the season wraps up, I just wanted to say thank you for trusting Pappas &amp; Co. with your property this year. It&rsquo;s been a pleasure taking care of your lawn and landscaping.</p>
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:20px 24px;margin:24px 0;text-align:center;"><p style="color:#2e403d;font-size:18px;font-weight:700;margin:0 0 6px;">Want to Lock In Your Spring Spot?</p><p style="color:#374151;font-size:14px;margin:0 0 16px;">Early-bird customers get priority scheduling when the season starts back up.</p><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px;display:inline-block;">Reserve My Spot</a></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">Wishing you and your family a wonderful winter. We&rsquo;ll see you in spring!</p>`
    },
    {
      id: 'winback',
      name: 'Win-Back / Re-engagement',
      category: 'marketing',
      description: 'Re-engage inactive customers with a personal touch',
      subject: 'We miss taking care of your lawn, {customer_first_name}',
      sms_body: 'Hi {customer_first_name}, it\'s Tim from Pappas & Co. It\'s been a while! If your yard needs some love, we\'d be happy to help: {portal_link}',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;">Hey {customer_first_name}, It&rsquo;s Been a While!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">We noticed it&rsquo;s been some time since we last worked together, and I wanted to check in. Whether your needs changed or life just got busy, we&rsquo;d love to help with your property again.</p>
<table width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0;" role="presentation"><tr><td style="width:48%;vertical-align:top;padding-right:12px;"><h3 style="color:#2e403d;font-size:16px;margin:0 0 8px;">One-Time Service</h3><p style="color:#374151;font-size:14px;line-height:1.7;margin:0;">Need a cleanup, mulch job, or one-time mow? We&rsquo;re happy to help with just a single visit.</p></td><td style="width:4%;"></td><td style="width:48%;vertical-align:top;padding-left:12px;"><h3 style="color:#2e403d;font-size:16px;margin:0 0 8px;">Regular Service</h3><p style="color:#374151;font-size:14px;line-height:1.7;margin:0;">Ready to get back on a regular schedule? We&rsquo;ll pick up right where we left off.</p></td></tr></table>
<div style="text-align:center;margin:28px 0;"><a href="{portal_link}" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Let&rsquo;s Reconnect</a></div>`
    },
    {
      id: 'holiday-thank-you',
      name: 'Holiday Thank You',
      category: 'marketing',
      description: 'End-of-year gratitude message to all customers',
      subject: 'Happy Holidays from Pappas & Co. Landscaping',
      sms_body: 'Happy Holidays from Tim and the whole Pappas & Co. team! Thank you for a wonderful year. Wishing you and your family all the best.',
      body: `<h2 style="color:#2e403d;font-size:24px;font-weight:700;margin:0 0 16px;text-align:center;">Happy Holidays, {customer_first_name}!</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;text-align:center;">From our family to yours, we want to say <strong>thank you</strong> for trusting Pappas &amp; Co. Landscaping with your property this year.</p>
<hr style="border:none;border-top:2px solid #e5e7eb;margin:28px 0;">
<blockquote style="border-left:4px solid #c9dd80;padding:16px 20px;margin:24px 0;background:#f8fafc;border-radius:0 8px 8px 0;"><p style="color:#374151;font-size:15px;line-height:1.8;margin:0;font-style:italic;">&ldquo;Every client is part of our extended family. We don&rsquo;t just care for your lawn — we care about your experience from the first call to the last leaf.&rdquo;</p><p style="color:#64748b;font-size:13px;margin:8px 0 0;font-weight:600;">&mdash; Tim Pappas</p></blockquote>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;text-align:center;">Wishing you a joyful holiday season and a wonderful new year. We&rsquo;ll see you in the spring!</p>`
    },
    {
      id: 'emergency-service',
      name: 'Emergency / Storm Service',
      category: 'marketing',
      description: 'Urgent service availability after storms',
      subject: 'Storm cleanup help available — Pappas & Co.',
      sms_body: 'Hi {customer_first_name}, storm damage? Our crew is available for emergency cleanup. Call us at (440) 886-7318 or reply here. — Tim, Pappas & Co.',
      body: `<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:12px;padding:20px 24px;margin:0 0 24px;text-align:center;"><p style="color:#991b1b;font-size:18px;font-weight:700;margin:0 0 6px;">Emergency Cleanup Available</p><p style="color:#374151;font-size:14px;margin:0;">Our crews are ready to help with storm damage and debris removal</p></div>
<h2 style="color:#2e403d;font-size:22px;font-weight:700;margin:0 0 16px;">We&rsquo;re Here to Help, {customer_first_name}</h2>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;">If the recent storms left damage on your property, our team is ready to help with emergency cleanup. We&rsquo;re prioritizing our existing customers first.</p>
<table width="100%" cellpadding="0" cellspacing="0" style="margin:20px 0;" role="presentation"><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F333;</td><td><strong style="color:#2e403d;">Fallen Tree &amp; Branch Removal</strong></td></tr></table></td></tr><tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F9F9;</td><td><strong style="color:#2e403d;">Debris &amp; Yard Cleanup</strong></td></tr></table></td></tr><tr><td style="padding:12px 0;"><table cellpadding="0" cellspacing="0"><tr><td style="width:40px;vertical-align:top;font-size:20px;">&#x1F6A8;</td><td><strong style="color:#2e403d;">Priority Scheduling for Current Customers</strong></td></tr></table></td></tr></table>
<div style="text-align:center;margin:28px 0;"><a href="tel:4408867318" style="background:#2e403d;color:#c9dd80;padding:14px 36px;border-radius:8px;text-decoration:none;font-weight:700;font-size:15px;display:inline-block;">Call (440) 886-7318</a></div>
<p style="color:#374151;font-size:15px;line-height:1.8;margin:0 0 16px;text-align:center;">Or reply to this email and we&rsquo;ll get back to you right away.</p>`
    }
  ];
  res.json({ success: true, templates: library });
});

// ═══════════════════════════════════════════════════════════
// ═══ CAMPAIGN BULK SEND & TRACKING ════════════════════════
// ═══════════════════════════════════════════════════════════

// POST /api/campaigns/:id/send - Bulk send campaign with template
app.post('/api/campaigns/:id/send', async (req, res) => {
  try {
    const { template_id, customer_ids, segment } = req.body;
    if (!template_id) return res.status(400).json({ success: false, error: 'template_id required' });
    const template = await pool.query('SELECT * FROM email_templates WHERE id = $1', [template_id]);
    if (template.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
    const tmpl = template.rows[0];

    // Get target customers
    let customers;
    if (customer_ids && customer_ids.length > 0) {
      customers = await pool.query('SELECT * FROM customers WHERE id = ANY($1)', [customer_ids]);
    } else if (segment === 'all') {
      customers = await pool.query('SELECT * FROM customers WHERE email IS NOT NULL AND email != \'\'');
    } else if (segment === 'monthly_plan') {
      customers = await pool.query('SELECT * FROM customers WHERE monthly_plan_amount > 0 AND email IS NOT NULL');
    } else if (segment === 'active') {
      customers = await pool.query(`SELECT DISTINCT c.* FROM customers c JOIN scheduled_jobs j ON c.id = j.customer_id WHERE j.created_at >= NOW() - INTERVAL '6 months' AND c.email IS NOT NULL`);
    } else {
      return res.status(400).json({ success: false, error: 'customer_ids or segment required' });
    }

    const results = { sent: 0, errors: 0 };
    for (const cust of customers.rows) {
      try {
        const trackingId = crypto.randomUUID().replace(/-/g, '').slice(0, 24);
        const vars = {
          customer_name: cust.name, customer_first_name: cust.first_name || cust.name,
          customer_email: cust.email, company_name: 'Pappas & Co. Landscaping',
          company_phone: '(440) 886-7318', company_website: 'pappaslandscaping.com',
          unsubscribe_email: encodeURIComponent(cust.email || '')
        };
        const subject = replaceTemplateVars(tmpl.subject, vars);
        let body = replaceTemplateVars(tmpl.body, vars);
        // Add tracking pixel
        const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
        body += `<img src="${baseUrl}/api/t/${trackingId}/open.png" width="1" height="1" style="display:none;" />`;
        const finalHtml = replaceTemplateVars(emailTemplate(body), vars);
        await sendEmail(cust.email, subject, finalHtml, null, { type: 'campaign', customer_id: cust.id, customer_name: cust.name });
        await pool.query(
          'INSERT INTO campaign_sends (campaign_id, template_id, customer_id, customer_email, status, tracking_id) VALUES ($1, $2, $3, $4, $5, $6)',
          [req.params.id, template_id, cust.id, cust.email, 'sent', trackingId]
        );
        results.sent++;
      } catch(e) { results.errors++; }
    }
    // Update campaign stats
    await pool.query('UPDATE campaigns SET template_id = $1, send_count = COALESCE(send_count, 0) + $2 WHERE id = $3', [template_id, results.sent, req.params.id]);
    res.json({ success: true, ...results });
  } catch (error) {
    console.error('Campaign send error:', error);
    serverError(res, error);
  }
});

// GET /api/campaigns/:id/send-history - Send stats
app.get('/api/campaigns/:id/send-history', async (req, res) => {
  try {
    const [sends, stats] = await Promise.all([
      pool.query(`SELECT cs.*, c.name as customer_name FROM campaign_sends cs LEFT JOIN customers c ON cs.customer_id = c.id WHERE cs.campaign_id = $1 ORDER BY cs.sent_at DESC`, [req.params.id]),
      pool.query(`SELECT COUNT(*) as total, COUNT(opened_at) as opens, COUNT(clicked_at) as clicks FROM campaign_sends WHERE campaign_id = $1`, [req.params.id])
    ]);
    res.json({ success: true, sends: sends.rows, stats: stats.rows[0] });
  } catch (error) { serverError(res, error); }
});

// POST /api/unsubscribe - Public endpoint for email unsubscribe
app.post('/api/unsubscribe', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ success: false, error: 'Email required' });

    const cleanEmail = email.toLowerCase().trim();
    // Find customer by email
    const cust = await pool.query('SELECT id FROM customers WHERE LOWER(email) = $1', [cleanEmail]);
    if (cust.rows.length === 0) {
      return res.json({ success: true, message: 'Unsubscribed' }); // Don't reveal if email exists
    }

    const customerId = cust.rows[0].id;
    // Update or insert communication prefs
    await pool.query(`
      INSERT INTO customer_communication_prefs (customer_id, email_marketing, sms_marketing, updated_at)
      VALUES ($1, false, false, NOW())
      ON CONFLICT (customer_id) DO UPDATE SET email_marketing = false, sms_marketing = false, updated_at = NOW()
    `, [customerId]);

    // Also add 'Unsubscribed' tag if not already present
    await pool.query(`
      UPDATE customers SET tags = CASE
        WHEN tags IS NULL OR tags = '' THEN 'Unsubscribed'
        WHEN tags ILIKE '%Unsubscribed%' THEN tags
        ELSE tags || ', Unsubscribed'
      END, updated_at = CURRENT_TIMESTAMP
      WHERE id = $1
    `, [customerId]);

    res.json({ success: true, message: 'Unsubscribed' });
  } catch (error) {
    console.error('Unsubscribe error:', error);
    res.status(500).json({ success: false, error: 'Something went wrong' });
  }
});

// Tracking pixel — records open
app.get('/api/t/:trackingId/open.png', async (req, res) => {
  try {
    await pool.query('UPDATE campaign_sends SET opened_at = COALESCE(opened_at, NOW()) WHERE tracking_id = $1', [req.params.trackingId]);
    await pool.query(`UPDATE campaigns SET open_count = (SELECT COUNT(opened_at) FROM campaign_sends WHERE campaign_id = campaigns.id) WHERE id = (SELECT campaign_id FROM campaign_sends WHERE tracking_id = $1)`, [req.params.trackingId]);
  } catch(e) { /* silently fail */ }
  // Return 1x1 transparent PNG
  const pixel = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==', 'base64');
  res.set({ 'Content-Type': 'image/png', 'Cache-Control': 'no-store, no-cache, must-revalidate', 'Content-Length': pixel.length });
  res.send(pixel);
});

// Click tracking redirect
app.get('/api/t/:trackingId/click', async (req, res) => {
  const { url } = req.query;
  try {
    await pool.query('UPDATE campaign_sends SET clicked_at = COALESCE(clicked_at, NOW()) WHERE tracking_id = $1', [req.params.trackingId]);
    await pool.query(`UPDATE campaigns SET click_count = (SELECT COUNT(clicked_at) FROM campaign_sends WHERE campaign_id = campaigns.id) WHERE id = (SELECT campaign_id FROM campaign_sends WHERE tracking_id = $1)`, [req.params.trackingId]);
  } catch(e) { /* silently fail */ }
  res.redirect(url || '/');
});

// ═══════════════════════════════════════════════════════════
// ═══ BROADCAST ENDPOINTS ══════════════════════════════════
// ═══════════════════════════════════════════════════════════

// GET /api/broadcasts/filter-options - Available filter values for broadcast audience builder
app.get('/api/broadcasts/filter-options', async (req, res) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ success: false, error: 'No token' });
  try {
    jwt.verify(token, JWT_SECRET);
  } catch (err) { return res.status(401).json({ success: false, error: 'Invalid token' }); }

  try {
    // Get all unique tags (comma-separated field, need to split and deduplicate)
    const [tagsResult, cities, postalCodes, statuses, customerTypes] = await Promise.all([
      pool.query(`SELECT DISTINCT tags FROM customers WHERE tags IS NOT NULL AND tags != ''`),
      pool.query(`SELECT DISTINCT city FROM customers WHERE city IS NOT NULL AND city != '' ORDER BY city`),
      pool.query(`SELECT DISTINCT postal_code FROM customers WHERE postal_code IS NOT NULL AND postal_code != '' ORDER BY postal_code`),
      pool.query(`SELECT DISTINCT status FROM customers WHERE status IS NOT NULL AND status != '' ORDER BY status`),
      pool.query(`SELECT DISTINCT customer_type FROM customers WHERE customer_type IS NOT NULL AND customer_type != '' ORDER BY customer_type`)
    ]);
    const tagSet = new Set();
    for (const row of tagsResult.rows) {
      (row.tags || '').split(',').forEach(t => { const trimmed = t.trim(); if (trimmed) tagSet.add(trimmed); });
    }

    res.json({
      success: true,
      tags: Array.from(tagSet).sort(),
      cities: cities.rows.map(r => r.city),
      postal_codes: postalCodes.rows.map(r => r.postal_code),
      statuses: statuses.rows.map(r => r.status),
      customer_types: customerTypes.rows.map(r => r.customer_type)
    });
  } catch (error) {
    console.error('Broadcast filter-options error:', error);
    serverError(res, error);
  }
});

// POST /api/broadcasts/preview - Preview audience with filters
app.post('/api/broadcasts/preview', async (req, res) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ success: false, error: 'No token' });
  try {
    jwt.verify(token, JWT_SECRET);
  } catch (err) { return res.status(401).json({ success: false, error: 'Invalid token' }); }

  try {
    const filters = req.body.filters || {};
    const conditions = [];
    const params = [];
    let paramIdx = 1;

    // Tags filter (comma-separated text field, match ANY of the provided tags)
    if (filters.tags && filters.tags.length > 0) {
      const tagConditions = filters.tags.map(tag => {
        params.push(`%${tag}%`);
        return `c.tags ILIKE $${paramIdx++}`;
      });
      conditions.push(`(${tagConditions.join(' OR ')})`);
    }

    // Postal codes
    if (filters.postal_codes && filters.postal_codes.length > 0) {
      params.push(filters.postal_codes);
      conditions.push(`c.postal_code = ANY($${paramIdx++})`);
    }

    // Cities (case-insensitive)
    if (filters.cities && filters.cities.length > 0) {
      params.push(filters.cities.map(c => c.toLowerCase()));
      conditions.push(`LOWER(c.city) = ANY($${paramIdx++})`);
    }

    // Status
    if (filters.status) {
      params.push(filters.status);
      conditions.push(`c.status = $${paramIdx++}`);
    }

    // Customer type
    if (filters.customer_type) {
      params.push(filters.customer_type);
      conditions.push(`c.customer_type = $${paramIdx++}`);
    }

    // Has email
    if (filters.has_email) {
      conditions.push(`c.email IS NOT NULL AND c.email != ''`);
    }

    // Has mobile
    if (filters.has_mobile) {
      conditions.push(`c.mobile IS NOT NULL AND c.mobile != ''`);
    }

    // Monthly plan
    if (filters.monthly_plan) {
      conditions.push(`c.monthly_plan_amount > 0`);
    }

    // Active since N months (had jobs in last N months)
    if (filters.active_since_months) {
      params.push(filters.active_since_months);
      conditions.push(`c.id IN (SELECT DISTINCT customer_id FROM scheduled_jobs WHERE created_at >= NOW() - ($${paramIdx++} || ' months')::INTERVAL)`);
    }

    // Scheduled on specific date (for daily reminders)
    if (filters.job_date) {
      params.push(filters.job_date);
      conditions.push(`c.id IN (SELECT DISTINCT customer_id FROM scheduled_jobs WHERE job_date::date = $${paramIdx++}::date AND customer_id IS NOT NULL)`);
    }

    const whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';
    const query = `SELECT c.id, c.name, c.first_name, c.last_name, c.email, c.mobile, c.city, c.postal_code, c.tags FROM customers c ${whereClause} ORDER BY c.name`;
    const result = await pool.query(query, params);

    // Normalize names
    const customers = result.rows.map(c => ({
      id: c.id,
      name: c.name || ((c.first_name || '') + (c.last_name ? ' ' + c.last_name : '')).trim() || 'Unknown',
      email: c.email,
      mobile: c.mobile,
      city: c.city,
      postal_code: c.postal_code,
      tags: c.tags
    }));

    // Build summary stats
    const summary = {
      total: customers.length,
      with_email: customers.filter(c => c.email && c.email.trim()).length,
      with_mobile: customers.filter(c => c.mobile && c.mobile.trim()).length,
      email_opted_in: customers.length, // default: assume opted in
      sms_opted_in: 0
    };

    // Check communication prefs for opted-in counts
    if (customers.length > 0) {
      const custIds = customers.map(c => c.id);
      const prefs = await pool.query('SELECT customer_id, email_marketing, sms_marketing FROM customer_communication_prefs WHERE customer_id = ANY($1)', [custIds]);
      const prefsMap = {};
      prefs.rows.forEach(p => { prefsMap[p.customer_id] = p; });
      let emailOptIn = 0, smsOptIn = 0;
      customers.forEach(c => {
        const p = prefsMap[c.id];
        if (!p || p.email_marketing !== false) emailOptIn++;
        if (p && p.sms_marketing === true) smsOptIn++;
      });
      summary.email_opted_in = emailOptIn;
      summary.sms_opted_in = smsOptIn;
    }

    res.json({ success: true, count: customers.length, customers, summary });
  } catch (error) {
    console.error('Broadcast preview error:', error);
    serverError(res, error);
  }
});

// POST /api/broadcasts/send - Send broadcast email and/or SMS
app.post('/api/broadcasts/send', async (req, res) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ success: false, error: 'No token' });
  try {
    jwt.verify(token, JWT_SECRET);
  } catch (err) { return res.status(401).json({ success: false, error: 'Invalid token' }); }

  try {
    const { channel, template_id, sms_body, customer_ids, campaign_id, job_date } = req.body;
    if (!channel || !['email', 'sms', 'both'].includes(channel)) {
      return res.status(400).json({ success: false, error: 'channel must be email, sms, or both' });
    }
    if (!customer_ids || customer_ids.length === 0) {
      return res.status(400).json({ success: false, error: 'customer_ids required' });
    }
    if ((channel === 'email' || channel === 'both') && !template_id) {
      return res.status(400).json({ success: false, error: 'template_id required for email' });
    }
    if ((channel === 'sms' || channel === 'both') && !sms_body) {
      return res.status(400).json({ success: false, error: 'sms_body required for SMS' });
    }

    // Load template for email
    let tmpl = null;
    if (template_id) {
      const templateResult = await pool.query('SELECT * FROM email_templates WHERE id = $1', [template_id]);
      if (templateResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Template not found' });
      tmpl = templateResult.rows[0];
    }

    // Load customers
    const custResult = await pool.query('SELECT * FROM customers WHERE id = ANY($1)', [customer_ids]);

    // Load communication preferences for all target customers
    const prefsResult = await pool.query('SELECT * FROM customer_communication_prefs WHERE customer_id = ANY($1)', [customer_ids]);
    const prefsMap = {};
    for (const p of prefsResult.rows) { prefsMap[p.customer_id] = p; }

    const results = { email_sent: 0, email_skipped: 0, email_errors: 0, sms_sent: 0, sms_skipped: 0, sms_errors: 0 };
    const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';

    for (const cust of custResult.rows) {
      const custName = cust.name || ((cust.first_name || '') + (cust.last_name ? ' ' + cust.last_name : '')).trim() || 'Unknown';
      const vars = {
        customer_name: custName,
        customer_first_name: cust.first_name || custName,
        customer_email: cust.email,
        customer_phone: cust.phone || cust.mobile,
        customer_address: [cust.street, cust.city, cust.state, cust.postal_code].filter(Boolean).join(', '),
        company_name: 'Pappas & Co. Landscaping',
        company_phone: '(440) 886-7318',
        company_email: 'hello@pappaslandscaping.com',
        company_website: 'pappaslandscaping.com',
        portal_link: `${baseUrl}/customer-portal.html`,
        unsubscribe_email: encodeURIComponent(cust.email || '')
      };

      // If job_date provided, look up ALL job details for this customer on that date
      if (job_date) {
        try {
          const jobResult = await pool.query(
            `SELECT service_type, address, service_price, job_date FROM scheduled_jobs
             WHERE customer_id = $1 AND job_date::date = $2::date
             ORDER BY id ASC`,
            [cust.id, job_date]
          );
          if (jobResult.rows.length > 0) {
            if (jobResult.rows.length === 1) {
              // Single job — keep simple format
              const job = jobResult.rows[0];
              vars.service_type = job.service_type || '';
              const fullAddr = job.address || vars.customer_address || '';
              vars.address = fullAddr.split(',')[0].trim();
              vars.service_list = `${vars.service_type} at ${vars.address}`;
              vars.services_list = vars.service_list;
              vars.service_price = job.service_price ? '$' + Number(job.service_price).toFixed(2) : '';
            } else {
              // Multiple jobs — build "Mowing at 123 Main St and Spring Cleanup at 456 Oak Ave"
              const jobParts = jobResult.rows.map(j => {
                const svc = j.service_type || '';
                const fa = j.address || vars.customer_address || '';
                const street = fa.split(',')[0].trim();
                return `${svc} at ${street}`;
              });
              vars.service_list = jobParts.join(' and ');
              vars.services_list = vars.service_list;
              vars.service_type = jobResult.rows.map(j => j.service_type || '').join(' & ');
              vars.address = jobResult.rows.map(j => {
                const fa = j.address || vars.customer_address || '';
                return fa.split(',')[0].trim();
              }).join(' & ');
              const total = jobResult.rows.reduce((sum, j) => sum + (j.service_price ? Number(j.service_price) : 0), 0);
              vars.service_price = total > 0 ? '$' + total.toFixed(2) : '';
            }
            const firstJob = jobResult.rows[0];
            vars.job_date = firstJob.job_date ? new Date(firstJob.job_date).toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' }) : '';
          }
        } catch (e) { console.error('Job lookup error:', e.message); }
      }

      const prefs = prefsMap[cust.id];

      // Send email
      if (channel === 'email' || channel === 'both') {
        // Check prefs: default allow email if no prefs row
        const emailAllowed = prefs ? prefs.email_marketing !== false : true;
        if (!emailAllowed || !cust.email) {
          results.email_skipped++;
        } else {
          try {
            const trackingId = crypto.randomUUID().replace(/-/g, '').slice(0, 24);
            const subject = replaceTemplateVars(tmpl.subject, vars);
            let body = replaceTemplateVars(tmpl.body, vars);
            body += `<img src="${baseUrl}/api/t/${trackingId}/open.png" width="1" height="1" style="display:none;" />`;
            const finalHtml = replaceTemplateVars(emailTemplate(body), vars);
            await sendEmail(cust.email, subject, finalHtml, null, { type: 'broadcast', customer_id: cust.id, customer_name: custName });
            // Track in campaign_sends if campaign_id provided
            if (campaign_id) {
              await pool.query(
                'INSERT INTO campaign_sends (campaign_id, template_id, customer_id, customer_email, status, tracking_id) VALUES ($1, $2, $3, $4, $5, $6)',
                [campaign_id, template_id, cust.id, cust.email, 'sent', trackingId]
              );
            }
            results.email_sent++;
          } catch (e) {
            console.error(`Broadcast email error for customer ${cust.id}:`, e.message);
            results.email_errors++;
          }
        }
      }

      // Send SMS
      if (channel === 'sms' || channel === 'both') {
        // Check prefs: allow SMS by default if no prefs row exists
        const smsAllowed = prefs ? prefs.sms_marketing !== false : true;
        if (!smsAllowed || !cust.mobile) {
          results.sms_skipped++;
        } else {
          try {
            const smsText = replaceTemplateVars(sms_body, vars);
            let formattedTo = cust.mobile.replace(/\D/g, '');
            if (formattedTo.length === 10) formattedTo = '+1' + formattedTo;
            else if (!formattedTo.startsWith('+')) formattedTo = '+' + formattedTo;

            const twilioMessage = await twilioClient.messages.create({
              body: smsText,
              from: TWILIO_PHONE_NUMBER,
              to: formattedTo
            });

            // Log to messages table
            await pool.query(`
              INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, status, customer_id, read)
              VALUES ($1, 'outbound', $2, $3, $4, $5, $6, true)
            `, [twilioMessage.sid, TWILIO_PHONE_NUMBER, formattedTo, smsText, twilioMessage.status, cust.id]);

            results.sms_sent++;
          } catch (e) {
            console.error(`Broadcast SMS error for customer ${cust.id}:`, e.message);
            results.sms_errors++;
          }
        }
      }
    }

    // Update campaign stats if linked
    if (campaign_id && results.email_sent > 0) {
      await pool.query('UPDATE campaigns SET template_id = COALESCE(template_id, $1), send_count = COALESCE(send_count, 0) + $2 WHERE id = $3', [template_id, results.email_sent, campaign_id]);
    }

    res.json({ success: true, ...results });
  } catch (error) {
    console.error('Broadcast send error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════

// ─── Pipeline / Workflow Stages ────────────────────────────────────────────
// NOTE: GET /api/jobs/pipeline is defined earlier (before /api/jobs/:id) to avoid route shadowing.

// PATCH /api/jobs/:id/pipeline - Move job to a pipeline stage
app.patch('/api/jobs/:id/pipeline', async (req, res) => {
  try {
    const { stage } = req.body;
    const validStages = ['new', 'quoted', 'scheduled', 'in_progress', 'completed', 'invoiced'];
    if (!validStages.includes(stage)) return res.status(400).json({ success: false, error: 'Invalid stage' });
    const statusMap = { new: 'pending', quoted: 'pending', scheduled: 'confirmed', in_progress: 'in-progress', completed: 'completed', invoiced: 'completed' };
    const result = await pool.query(
      `UPDATE scheduled_jobs SET pipeline_stage = $1, status = $2, updated_at = NOW() WHERE id = $3 RETURNING *`,
      [stage, statusMap[stage], req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, error: 'Job not found' });
    res.json({ success: true, job: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ─── Recurring Job Scheduling (Enhanced) ───────────────────────────────────

// POST /api/jobs/:id/setup-recurring - Configure recurring schedule
app.post('/api/jobs/:id/setup-recurring', async (req, res) => {
  try {
    const { pattern, day_of_week, start_date, end_date, auto_generate_weeks } = req.body;
    // pattern: weekly, biweekly, monthly, custom
    const validPatterns = ['weekly', 'biweekly', 'monthly', 'custom'];
    if (!validPatterns.includes(pattern)) return res.status(400).json({ success: false, error: 'Invalid pattern' });

    const result = await pool.query(
      `UPDATE scheduled_jobs SET is_recurring = true, recurring_pattern = $1, recurring_day_of_week = $2, recurring_start_date = $3, recurring_end_date = $4, updated_at = NOW() WHERE id = $5 RETURNING *`,
      [pattern, day_of_week, start_date, end_date, req.params.id]
    );
    if (!result.rows.length) return res.status(404).json({ success: false, error: 'Job not found' });

    // Auto-generate upcoming jobs if requested
    let generated = [];
    if (auto_generate_weeks && auto_generate_weeks > 0) {
      const job = result.rows[0];
      const startDt = new Date(start_date || job.job_date);
      const endDt = end_date ? new Date(end_date) : new Date(startDt.getTime() + auto_generate_weeks * 7 * 86400000);

      let current = new Date(startDt);
      const intervalDays = pattern === 'weekly' ? 7 : pattern === 'biweekly' ? 14 : 30;
      current.setDate(current.getDate() + intervalDays); // skip first (it's the parent)

      while (current <= endDt) {
        const dateStr = current.toISOString().split('T')[0];
        // Check for duplicates
        const exists = await pool.query('SELECT id FROM recurring_job_log WHERE source_job_id = $1 AND generated_for_date = $2', [job.id, dateStr]);
        if (!exists.rows.length) {
          const newJob = await pool.query(
            `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_frequency, service_price, address, phone, special_notes, property_notes, status, estimated_duration, crew_assigned, parent_job_id, property_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11,$12,$13,$14) RETURNING *`,
            [dateStr, job.customer_name, job.customer_id, job.service_type, job.service_frequency, job.service_price, job.address, job.phone, job.special_notes, job.property_notes, job.estimated_duration, job.crew_assigned, job.id, job.property_id]
          );
          await pool.query('INSERT INTO recurring_job_log (source_job_id, generated_for_date, generated_job_id) VALUES ($1,$2,$3)', [job.id, dateStr, newJob.rows[0].id]);
          generated.push(newJob.rows[0]);
        }
        current.setDate(current.getDate() + intervalDays);
      }
    }

    res.json({ success: true, job: result.rows[0], generated_jobs: generated.length, jobs: generated });
  } catch (error) { serverError(res, error); }
});

// ─── Payment Schedule Splitting ────────────────────────────────────────────

// POST /api/invoices/:id/payment-schedule - Split invoice into installments
app.post('/api/invoices/:id/payment-schedule', async (req, res) => {
  try {
    const { installments } = req.body; // Array of { amount, due_date, label }
    if (!installments || !Array.isArray(installments) || installments.length < 2) {
      return res.status(400).json({ success: false, error: 'Need at least 2 installments' });
    }
    const inv = await pool.query('SELECT * FROM invoices WHERE id = $1', [req.params.id]);
    if (!inv.rows.length) return res.status(404).json({ success: false, error: 'Invoice not found' });

    const total = parseFloat(inv.rows[0].total);
    const scheduleTotal = installments.reduce((sum, i) => sum + parseFloat(i.amount), 0);
    if (Math.abs(scheduleTotal - total) > 0.01) {
      return res.status(400).json({ success: false, error: `Installments total $${scheduleTotal.toFixed(2)} doesn't match invoice total $${total.toFixed(2)}` });
    }

    const schedule = installments.map((inst, idx) => ({
      number: idx + 1,
      amount: parseFloat(inst.amount),
      due_date: inst.due_date,
      label: inst.label || `Payment ${idx + 1} of ${installments.length}`,
      status: 'pending'
    }));

    await pool.query(
      `UPDATE invoices SET payment_schedule = $1, installment_count = $2, updated_at = NOW() WHERE id = $3`,
      [JSON.stringify(schedule), installments.length, req.params.id]
    );

    res.json({ success: true, schedule, installment_count: installments.length });
  } catch (error) { serverError(res, error); }
});

// GET /api/invoices/:id/payment-schedule
app.get('/api/invoices/:id/payment-schedule', async (req, res) => {
  try {
    const inv = await pool.query('SELECT id, total, amount_paid, payment_schedule, installment_count FROM invoices WHERE id = $1', [req.params.id]);
    if (!inv.rows.length) return res.status(404).json({ success: false, error: 'Invoice not found' });
    const schedule = inv.rows[0].payment_schedule || [];
    // Mark paid installments based on amount_paid
    let remaining = parseFloat(inv.rows[0].amount_paid) || 0;
    const updated = (Array.isArray(schedule) ? schedule : []).map(inst => {
      if (remaining >= inst.amount) { remaining -= inst.amount; return { ...inst, status: 'paid' }; }
      if (remaining > 0) { const partial = remaining; remaining = 0; return { ...inst, status: 'partial', paid: partial }; }
      return { ...inst, status: 'pending' };
    });
    res.json({ success: true, schedule: updated, total: parseFloat(inv.rows[0].total), amount_paid: parseFloat(inv.rows[0].amount_paid) || 0 });
  } catch (error) { serverError(res, error); }
});

// ─── Job Detail / Profitability ────────────────────────────────────────────

// GET /api/jobs/:id/profitability - Full job P&L
app.get('/api/jobs/:id/profitability', async (req, res) => {
  try {
    const job = await pool.query('SELECT * FROM scheduled_jobs WHERE id = $1', [req.params.id]);
    if (!job.rows.length) return res.status(404).json({ success: false, error: 'Job not found' });
    const j = job.rows[0];

    // Get expenses
    let expenses = [];
    try { expenses = (await pool.query('SELECT * FROM job_expenses WHERE job_id = $1 ORDER BY created_at DESC', [j.id])).rows; } catch(e) {}

    // Get time entries for labor cost
    let timeEntries = [];
    try { timeEntries = (await pool.query('SELECT * FROM time_entries WHERE job_id = $1', [j.id])).rows; } catch(e) {}

    const laborHours = timeEntries.reduce((sum, t) => {
      if (!t.clock_in || !t.clock_out) return sum;
      return sum + (new Date(t.clock_out) - new Date(t.clock_in)) / 3600000 - (t.break_minutes || 0) / 60;
    }, 0);
    const laborRate = 35; // TODO: configurable per crew
    const laborCost = parseFloat(j.labor_cost) || (laborHours * laborRate);
    const materialCost = parseFloat(j.material_cost) || 0;
    const expenseTotal = expenses.reduce((sum, e) => sum + parseFloat(e.amount), 0);
    const revenue = parseFloat(j.service_price) || 0;
    const totalCost = laborCost + materialCost + expenseTotal;
    const profit = revenue - totalCost;
    const margin = revenue > 0 ? (profit / revenue * 100) : 0;

    res.json({
      success: true,
      profitability: {
        revenue,
        labor_cost: laborCost,
        labor_hours: Math.round(laborHours * 100) / 100,
        material_cost: materialCost,
        expense_total: expenseTotal,
        total_cost: totalCost,
        profit,
        margin: Math.round(margin * 10) / 10
      },
      expenses,
      time_entries: timeEntries
    });
  } catch (error) { serverError(res, error); }
});

// POST /api/jobs/:id/expenses - Add expense to job
app.post('/api/jobs/:id/expenses', async (req, res) => {
  try {
    const { description, category, amount, receipt_url, created_by } = req.body;
    if (!amount) return res.status(400).json({ success: false, error: 'Amount required' });
    const result = await pool.query(
      `INSERT INTO job_expenses (job_id, description, category, amount, receipt_url, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [req.params.id, description, category, parseFloat(amount), receipt_url, created_by]
    );
    // Update job expense_total
    const total = await pool.query('SELECT COALESCE(SUM(amount),0) as total FROM job_expenses WHERE job_id = $1', [req.params.id]);
    await pool.query('UPDATE scheduled_jobs SET expense_total = $1 WHERE id = $2', [total.rows[0].total, req.params.id]);
    res.json({ success: true, expense: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// DELETE /api/jobs/:id/expenses/:expenseId
app.delete('/api/jobs/:id/expenses/:expenseId', async (req, res) => {
  try {
    await pool.query('DELETE FROM job_expenses WHERE id = $1 AND job_id = $2', [req.params.expenseId, req.params.id]);
    const total = await pool.query('SELECT COALESCE(SUM(amount),0) as total FROM job_expenses WHERE job_id = $1', [req.params.id]);
    await pool.query('UPDATE scheduled_jobs SET expense_total = $1 WHERE id = $2', [total.rows[0].total, req.params.id]);
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

// ─── Internal Notes ────────────────────────────────────────────────────────

// GET /api/notes/:entityType/:entityId
app.get('/api/notes/:entityType/:entityId', async (req, res) => {
  try {
    const { entityType, entityId } = req.params;
    const result = await pool.query(
      'SELECT * FROM internal_notes WHERE entity_type = $1 AND entity_id = $2 ORDER BY pinned DESC, created_at DESC',
      [entityType, parseInt(entityId)]
    );
    res.json({ success: true, notes: result.rows });
  } catch (error) { serverError(res, error); }
});

// POST /api/notes/:entityType/:entityId
app.post('/api/notes/:entityType/:entityId', validate(schemas.createNote), async (req, res) => {
  try {
    const { entityType, entityId } = req.params;
    const { content, pinned } = req.body;
    if (!content || !content.trim()) return res.status(400).json({ success: false, error: 'Content required' });
    const authorName = req.user?.name || 'Unknown';
    const authorId = req.user?.id || null;
    const result = await pool.query(
      `INSERT INTO internal_notes (entity_type, entity_id, author_name, author_id, content, pinned) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [entityType, parseInt(entityId), authorName, authorId, content.trim(), pinned || false]
    );
    res.json({ success: true, note: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// PATCH /api/notes/:id - Update note
app.patch('/api/notes/:id', async (req, res) => {
  try {
    const { content, pinned } = req.body;
    const sets = ['updated_at = NOW()'];
    const vals = [];
    let p = 1;
    if (content !== undefined) { sets.push(`content = $${p++}`); vals.push(content); }
    if (pinned !== undefined) { sets.push(`pinned = $${p++}`); vals.push(pinned); }
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE internal_notes SET ${sets.join(',')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, note: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// DELETE /api/notes/:id
app.delete('/api/notes/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM internal_notes WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

// ─── Email Log API ─────────────────────────────────────────────────────────

// Global email log with filters
app.get('/api/email-log', async (req, res) => {
  try {
    const { type, search, days, limit = 100, offset = 0 } = req.query;
    let where = [];
    let params = [];
    let idx = 1;

    if (type && type !== 'all') {
      where.push(`email_type = $${idx++}`);
      params.push(type);
    }
    if (search) {
      where.push(`(recipient_email ILIKE $${idx} OR subject ILIKE $${idx} OR customer_name ILIKE $${idx})`);
      params.push(`%${search}%`);
      idx++;
    }
    if (days) {
      where.push(`sent_at >= NOW() - $${idx}::int * INTERVAL '1 day'`);
      params.push(parseInt(days));
      idx++;
    }

    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    const [countResult, result] = await Promise.all([
      pool.query(`SELECT COUNT(*) FROM email_log ${whereClause}`, params),
      pool.query(`SELECT * FROM email_log ${whereClause} ORDER BY sent_at DESC LIMIT $${idx} OFFSET $${idx + 1}`, [...params, parseInt(limit), parseInt(offset)])
    ]);

    res.json({ success: true, emails: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) {
    console.error('Email log error:', error);
    serverError(res, error);
  }
});

// Email log stats
app.get('/api/email-log/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '24 hours') AS last_24h,
        COUNT(*) FILTER (WHERE sent_at >= NOW() - INTERVAL '7 days') AS last_7d,
        COUNT(*) FILTER (WHERE status = 'failed') AS failed,
        COUNT(DISTINCT recipient_email) AS unique_recipients
      FROM email_log
    `);
    const byType = await pool.query(`
      SELECT email_type, COUNT(*) AS count
      FROM email_log
      GROUP BY email_type
      ORDER BY count DESC
    `);
    res.json({ success: true, stats: stats.rows[0], by_type: byType.rows });
  } catch (error) {
    serverError(res, error);
  }
});

// Customer-specific email log
app.get('/api/customers/:id/emails', async (req, res) => {
  try {
    const { id } = req.params;
    const customer = await pool.query('SELECT email FROM customers WHERE id = $1', [id]);
    let result;
    if (customer.rows.length && customer.rows[0].email) {
      result = await pool.query(
        `SELECT * FROM email_log WHERE customer_id = $1 OR recipient_email = $2 ORDER BY sent_at DESC LIMIT 100`,
        [id, customer.rows[0].email]
      );
    } else {
      result = await pool.query(
        `SELECT * FROM email_log WHERE customer_id = $1 ORDER BY sent_at DESC LIMIT 100`,
        [id]
      );
    }
    res.json({ success: true, emails: result.rows });
  } catch (error) {
    serverError(res, error);
  }
});

// GET /api/dashboard/activity-feed - Chronological activity feed across all entities
app.get('/api/dashboard/activity-feed', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const result = await pool.query(`
      (
        SELECT 'quote_sent' as type,
               'Quote sent to ' || COALESCE(customer_name, 'Unknown') || ' — ' || COALESCE(quote_number, 'Q-' || id::text) as description,
               created_at as timestamp,
               '/sent-quote-detail.html?id=' || id as link
        FROM sent_quotes
        WHERE status IN ('sent','viewed')
        ORDER BY created_at DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'quote_signed' as type,
               COALESCE(customer_name, 'Unknown') || ' signed quote ' || COALESCE(quote_number, 'Q-' || id::text) as description,
               COALESCE(signed_at, updated_at, created_at) as timestamp,
               '/sent-quote-detail.html?id=' || id as link
        FROM sent_quotes
        WHERE status IN ('signed','contracted')
        ORDER BY COALESCE(signed_at, updated_at, created_at) DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'invoice_created' as type,
               'Invoice ' || COALESCE(invoice_number, '#' || id::text) || ' created for ' || COALESCE(customer_name, 'Unknown') || ' — $' || COALESCE(total::text, '0') as description,
               created_at as timestamp,
               '/invoice-detail.html?id=' || id as link
        FROM invoices
        WHERE status = 'draft' OR status = 'sent'
        ORDER BY created_at DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'payment_received' as type,
               'Payment received from ' || COALESCE(customer_name, 'Unknown') || ' — $' || COALESCE(amount_paid::text, '0') as description,
               COALESCE(paid_at, updated_at) as timestamp,
               '/invoice-detail.html?id=' || id as link
        FROM invoices
        WHERE status = 'paid' AND amount_paid > 0
        ORDER BY COALESCE(paid_at, updated_at) DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'job_completed' as type,
               'Job completed: ' || COALESCE(service_type, 'Service') || ' for ' || COALESCE(customer_name, 'Unknown') as description,
               COALESCE(updated_at, job_date::timestamp) as timestamp,
               '/job-detail.html?id=' || id as link
        FROM scheduled_jobs
        WHERE status IN ('completed','done')
        ORDER BY COALESCE(updated_at, job_date::timestamp) DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'job_scheduled' as type,
               'Job scheduled: ' || COALESCE(service_type, 'Service') || ' for ' || COALESCE(customer_name, 'Unknown') || ' on ' || to_char(job_date, 'Mon DD') as description,
               created_at as timestamp,
               '/job-detail.html?id=' || id as link
        FROM scheduled_jobs
        WHERE status IN ('pending','scheduled') AND job_date >= CURRENT_DATE
        ORDER BY created_at DESC LIMIT 10
      )
      UNION ALL
      (
        SELECT 'new_customer' as type,
               'New customer: ' || COALESCE(name, 'Unknown') as description,
               created_at as timestamp,
               '/customer-detail.html?id=' || id as link
        FROM customers
        ORDER BY created_at DESC LIMIT 10
      )
      ORDER BY timestamp DESC NULLS LAST
      LIMIT 20
    `);
    res.json({ success: true, events: result.rows });
  } catch (error) {
    console.error('Activity feed error:', error);
    res.json({ success: true, events: [] });
  }
});

// GET /api/dashboard/today-summary - Quick counts for today's dashboard
app.get('/api/dashboard/today-summary', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const today = new Date().toISOString().split('T')[0];
    const [jobsToday, revenueToday, pendingQuotes, overdueInvoices, unreadMessages] = await Promise.all([
      pool.query('SELECT COUNT(*) as cnt FROM scheduled_jobs WHERE job_date::date = $1::date', [today]),
      pool.query("SELECT COALESCE(SUM(amount_paid),0) as amt FROM invoices WHERE status = 'paid' AND paid_at::date = $1::date", [today]).catch(() => ({ rows: [{ amt: 0 }] })),
      pool.query("SELECT COUNT(*) as cnt FROM sent_quotes WHERE status IN ('sent','viewed')").catch(() => ({ rows: [{ cnt: 0 }] })),
      pool.query("SELECT COUNT(*) as cnt FROM invoices WHERE status IN ('sent','viewed') AND due_date < CURRENT_DATE").catch(() => ({ rows: [{ cnt: 0 }] })),
      pool.query("SELECT COUNT(*) as cnt FROM messages WHERE read = false AND direction = 'inbound'").catch(() => ({ rows: [{ cnt: 0 }] }))
    ]);
    res.json({
      success: true,
      jobs_today: parseInt(jobsToday.rows[0].cnt) || 0,
      revenue_today: parseFloat(revenueToday.rows[0].amt) || 0,
      pending_quotes: parseInt(pendingQuotes.rows[0].cnt) || 0,
      overdue_invoices: parseInt(overdueInvoices.rows[0].cnt) || 0,
      unread_messages: parseInt(unreadMessages.rows[0].cnt) || 0
    });
  } catch (error) {
    console.error('Today summary error:', error);
    res.json({ success: true, jobs_today: 0, revenue_today: 0, pending_quotes: 0, overdue_invoices: 0, unread_messages: 0 });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));
app.get('/api/config/maps-key', (req, res) => res.json({ key: process.env.GOOGLE_MAPS_API_KEY || '' }));

// ═══════════════════════════════════════════════════════════
// Phase 5: Scheduling + Calendar endpoints
// ═══════════════════════════════════════════════════════════

// Migration needed: ALTER TABLE scheduled_jobs ADD COLUMN start_time TIME, ADD COLUMN end_time TIME;

// NOTE: GET /api/jobs/calendar-summary is defined above (before /api/jobs/:id) to avoid route conflict.

// POST /api/jobs/from-quote/:quoteId - Create a job from a sent quote
app.post('/api/jobs/from-quote/:quoteId', async (req, res) => {
  try {
    const { quoteId } = req.params;
    const { job_date, crew_assigned } = req.body;

    const quoteResult = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [quoteId]);
    if (quoteResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Quote not found' });
    }
    const quote = quoteResult.rows[0];

    let serviceType = 'Service';
    let totalPrice = parseFloat(quote.total) || 0;
    if (quote.services) {
      const services = typeof quote.services === 'string' ? JSON.parse(quote.services) : quote.services;
      if (Array.isArray(services) && services.length > 0) {
        serviceType = services.map(s => s.name || s.service || s.description || 'Service').join(', ');
        if (serviceType.length > 100) serviceType = serviceType.substring(0, 97) + '...';
      }
    }

    const jobDate = job_date || new Date().toISOString().split('T')[0];

    const result = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, service_price, address, phone, special_notes, status, crew_assigned, estimated_duration)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, 60)
       RETURNING *`,
      [
        jobDate,
        quote.customer_name,
        quote.customer_id || null,
        serviceType,
        totalPrice,
        quote.customer_address || '',
        quote.customer_phone || '',
        'Created from Quote #' + (quote.quote_number || quote.id),
        crew_assigned || null
      ]
    );

    res.json({ success: true, job: result.rows[0], quote_id: quote.id });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// PHASE 6 — FINANCIAL SUITE ENDPOINTS
// (aging + batch routes are registered above before /api/invoices/:id)
// ═══════════════════════════════════════════════════════════

// GET /api/expense-categories - Return distinct categories plus defaults
app.get('/api/expense-categories', async (req, res) => {
  try {
    const defaults = ['Fuel', 'Equipment', 'Supplies', 'Insurance', 'Vehicle', 'Labor', 'Materials', 'Office', 'Marketing', 'Service Repairs', 'Operating Costs', 'Cost of Goods Sold', 'Losses', 'Advertising/Accounting', 'Utilities', 'Other'];
    const result = await pool.query('SELECT DISTINCT category FROM expenses WHERE category IS NOT NULL AND category != \'\'');
    const dbCats = result.rows.map(r => r.category);
    const merged = [...new Set([...defaults, ...dbCats])].sort();
    res.json({ success: true, categories: merged });
  } catch (error) {
    console.error('Error fetching expense categories:', error);
    res.json({ success: true, categories: ['Fuel', 'Equipment', 'Supplies', 'Insurance', 'Vehicle', 'Labor', 'Materials', 'Office', 'Marketing', 'Other'] });
  }
});

// GET /api/finance/cash-flow-forecast - Expected inflows/outflows for next 90 days
app.get('/api/finance/cash-flow-forecast', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const [inflows, recentExpenses] = await Promise.all([
      pool.query(`SELECT due_date, SUM(total - COALESCE(amount_paid, 0)) as expected
        FROM invoices WHERE status IN ('sent', 'viewed', 'overdue')
        AND due_date IS NOT NULL AND due_date <= CURRENT_DATE + INTERVAL '90 days'
        GROUP BY due_date ORDER BY due_date`),
      pool.query(`SELECT category, AVG(amount) as avg_amount, COUNT(*) as count
        FROM expenses WHERE expense_date >= CURRENT_DATE - INTERVAL '3 months'
        GROUP BY category`)
    ]);
    const weeks = [];
    const weeklyExpenseEstimate = recentExpenses.rows.reduce((s, r) => s + parseFloat(r.avg_amount || 0), 0) / 13;
    for (let i = 0; i < 13; i++) {
      const weekStart = new Date();
      weekStart.setDate(weekStart.getDate() + (i * 7));
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekEnd.getDate() + 6);
      let inflow = 0;
      inflows.rows.forEach(r => {
        const d = new Date(r.due_date);
        if (d >= weekStart && d <= weekEnd) inflow += parseFloat(r.expected || 0);
      });
      weeks.push({
        week: i + 1,
        start: weekStart.toISOString().split('T')[0],
        end: weekEnd.toISOString().split('T')[0],
        expected_inflow: Math.round(inflow * 100) / 100,
        expected_outflow: Math.round(weeklyExpenseEstimate * 100) / 100,
        net: Math.round((inflow - weeklyExpenseEstimate) * 100) / 100
      });
    }
    res.json({
      success: true,
      forecast: weeks,
      total_expected_inflow: Math.round(inflows.rows.reduce((s, r) => s + parseFloat(r.expected || 0), 0) * 100) / 100,
      monthly_expense_avg: Math.round(recentExpenses.rows.reduce((s, r) => s + parseFloat(r.avg_amount || 0), 0) * 100) / 100,
      expense_categories: recentExpenses.rows
    });
  } catch (error) {
    console.error('Error fetching cash flow forecast:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// Phase 8: Polish + Missing Pages endpoints
// ═══════════════════════════════════════════════════════════

// -- DB Migration needed: CREATE TABLE service_items (id SERIAL PRIMARY KEY, name VARCHAR(255), default_rate DECIMAL(10,2), duration_minutes INTEGER, category VARCHAR(100), active BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW());

// 8.1 Crew Performance & Schedule
app.get('/api/crews/:id/performance', async (req, res) => {
  try {
    const { id } = req.params;
    const crew = await pool.query('SELECT name FROM crews WHERE id = $1', [id]);
    if (crew.rows.length === 0) return res.status(404).json({ success: false, error: 'Crew not found' });
    const crewName = crew.rows[0].name;
    const stats = await pool.query(`
      SELECT COUNT(*) as total_jobs,
        COUNT(*) FILTER (WHERE status = 'completed') as completed_jobs,
        COALESCE(SUM(service_price) FILTER (WHERE status = 'completed'), 0) as total_revenue,
        COUNT(*) FILTER (WHERE status = 'completed' AND scheduled_date >= NOW() - INTERVAL '30 days') as completed_last_30
      FROM scheduled_jobs WHERE crew_assigned = $1
    `, [crewName]);
    const s = stats.rows[0];
    const totalJobs = parseInt(s.total_jobs) || 0;
    const completedJobs = parseInt(s.completed_jobs) || 0;
    const onTimeRate = totalJobs > 0 ? (completedJobs / totalJobs) : 0;
    res.json({
      success: true,
      crew_name: crewName,
      total_jobs: totalJobs,
      completed_jobs: completedJobs,
      on_time_rate: Math.round(onTimeRate * 100),
      total_revenue: parseFloat(s.total_revenue) || 0,
      completed_last_30: parseInt(s.completed_last_30) || 0
    });
  } catch (error) {
    console.error('Crew performance error:', error);
    serverError(res, error);
  }
});

app.get('/api/crews/:id/schedule', async (req, res) => {
  try {
    const { id } = req.params;
    const crew = await pool.query('SELECT name FROM crews WHERE id = $1', [id]);
    if (crew.rows.length === 0) return res.status(404).json({ success: false, error: 'Crew not found' });
    const crewName = crew.rows[0].name;
    const jobs = await pool.query(`
      SELECT id, customer_name, service_type, service_price, address, scheduled_date, status
      FROM scheduled_jobs WHERE crew_assigned = $1 AND scheduled_date >= CURRENT_DATE
      ORDER BY scheduled_date ASC LIMIT 20
    `, [crewName]);
    res.json({ success: true, jobs: jobs.rows });
  } catch (error) {
    console.error('Crew schedule error:', error);
    serverError(res, error);
  }
});

// 8.2 Reports: Job Costing & Customer Value
app.get('/api/reports/job-costing', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const result = await pool.query(`
      SELECT sj.id, sj.customer_name, sj.service_type, sj.service_price as revenue,
        COALESCE((SELECT SUM(amount) FROM expenses WHERE LOWER(description) LIKE '%' || LOWER(sj.customer_name) || '%' OR category = sj.service_type), 0) as expenses
      FROM scheduled_jobs sj WHERE sj.status = 'completed'
      ORDER BY sj.completed_at DESC NULLS LAST LIMIT 50
    `);
    const jobs = result.rows.map(r => ({
      id: r.id,
      customer_name: r.customer_name,
      service_type: r.service_type,
      revenue: parseFloat(r.revenue) || 0,
      expenses: parseFloat(r.expenses) || 0,
      profit: (parseFloat(r.revenue) || 0) - (parseFloat(r.expenses) || 0)
    }));
    res.json(jobs);
  } catch (error) {
    console.error('Job costing error:', error);
    serverError(res, error);
  }
});

app.get('/api/reports/customer-value', async (req, res) => {
  try {
    await ensureInvoicesTable();
    const result = await pool.query(`
      SELECT c.id, c.name, c.email,
        COALESCE(SUM(i.total), 0) as total_invoiced,
        COUNT(DISTINCT i.id) as invoice_count,
        MAX(i.created_at) as last_invoice_date
      FROM customers c
      LEFT JOIN invoices i ON (i.customer_email = c.email OR i.customer_name = c.name)
      GROUP BY c.id, c.name, c.email
      HAVING COALESCE(SUM(i.total), 0) > 0
      ORDER BY total_invoiced DESC
      LIMIT 25
    `);
    res.json(result.rows.map(r => ({
      id: r.id,
      name: r.name,
      email: r.email,
      total_invoiced: parseFloat(r.total_invoiced) || 0,
      invoice_count: parseInt(r.invoice_count) || 0,
      last_invoice_date: r.last_invoice_date
    })));
  } catch (error) {
    console.error('Customer value error:', error);
    serverError(res, error);
  }
});

// 8.3 Service Items CRUD
const ensureServiceItemsTable = async () => {
  await pool.query(`CREATE TABLE IF NOT EXISTS service_items (
    id SERIAL PRIMARY KEY, name VARCHAR(255), default_rate DECIMAL(10,2),
    duration_minutes INTEGER, category VARCHAR(100), active BOOLEAN DEFAULT true,
    description TEXT, tax_rate DECIMAL(5,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
  )`);
  // Add columns if they don't exist (for existing tables)
  await pool.query(`ALTER TABLE service_items ADD COLUMN IF NOT EXISTS description TEXT`).catch(() => {});
  await pool.query(`ALTER TABLE service_items ADD COLUMN IF NOT EXISTS tax_rate DECIMAL(5,2) DEFAULT 0`).catch(() => {});
  await pool.query(`ALTER TABLE service_items ADD COLUMN IF NOT EXISTS taxable BOOLEAN DEFAULT true`).catch(() => {});
};

app.get('/api/service-items', async (req, res) => {
  try {
    await ensureServiceItemsTable();
    const result = await pool.query('SELECT * FROM service_items ORDER BY name ASC');
    res.json({ success: true, items: result.rows });
  } catch (error) {
    serverError(res, error);
  }
});

app.post('/api/service-items', async (req, res) => {
  try {
    await ensureServiceItemsTable();
    const { name, default_rate, duration_minutes, category, description, tax_rate, taxable } = req.body;
    if (!name) return res.status(400).json({ success: false, error: 'Name required' });
    const result = await pool.query(
      'INSERT INTO service_items (name, default_rate, duration_minutes, category, description, tax_rate, taxable) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *',
      [name, default_rate || 0, duration_minutes || 60, category || 'General', description || '', tax_rate || 0, taxable !== false]
    );
    res.json({ success: true, item: result.rows[0] });
  } catch (error) {
    serverError(res, error);
  }
});

app.patch('/api/service-items/:id', async (req, res) => {
  try {
    await ensureServiceItemsTable();
    const { name, default_rate, duration_minutes, category, active, description, tax_rate, taxable } = req.body;
    const sets = [], vals = [];
    let p = 1;
    if (name !== undefined) { sets.push(`name = $${p++}`); vals.push(name); }
    if (default_rate !== undefined) { sets.push(`default_rate = $${p++}`); vals.push(default_rate); }
    if (duration_minutes !== undefined) { sets.push(`duration_minutes = $${p++}`); vals.push(duration_minutes); }
    if (category !== undefined) { sets.push(`category = $${p++}`); vals.push(category); }
    if (active !== undefined) { sets.push(`active = $${p++}`); vals.push(active); }
    if (description !== undefined) { sets.push(`description = $${p++}`); vals.push(description); }
    if (tax_rate !== undefined) { sets.push(`tax_rate = $${p++}`); vals.push(tax_rate); }
    if (taxable !== undefined) { sets.push(`taxable = $${p++}`); vals.push(taxable); }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields to update' });
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE service_items SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, item: result.rows[0] });
  } catch (error) {
    serverError(res, error);
  }
});

app.delete('/api/service-items/:id', async (req, res) => {
  try {
    await ensureServiceItemsTable();
    const result = await pool.query('DELETE FROM service_items WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    serverError(res, error);
  }
});

// Bulk import service items
app.post('/api/service-items/import', async (req, res) => {
  try {
    await ensureServiceItemsTable();
    const { items } = req.body;
    if (!items || !Array.isArray(items)) return res.status(400).json({ success: false, error: 'items array required' });
    let imported = 0, skipped = 0;
    for (const item of items) {
      if (!item.name) { skipped++; continue; }
      const exists = await pool.query('SELECT id FROM service_items WHERE LOWER(name) = LOWER($1)', [item.name]);
      if (exists.rows.length > 0) { skipped++; continue; }
      await pool.query(
        'INSERT INTO service_items (name, default_rate, category, description, tax_rate) VALUES ($1, $2, $3, $4, $5)',
        [item.name, item.default_rate || 0, item.category || 'General', item.description || '', item.tax_rate || 0]
      );
      imported++;
    }
    res.json({ success: true, imported, skipped, total: items.length });
  } catch (error) {
    serverError(res, error);
  }
});

// ─── Email & SMS Templates ─────────────────────────────────────────
const ensureTemplatesTable = async () => {
  await pool.query(`CREATE TABLE IF NOT EXISTS message_templates (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(20) DEFAULT 'email',
    subject VARCHAR(500),
    html_content TEXT,
    text_content TEXT,
    category VARCHAR(100),
    tags TEXT[] DEFAULT '{}',
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
  )`);
};

app.get('/api/templates', async (req, res) => {
  try {
    await ensureTemplatesTable();
    const { type } = req.query;
    let q = 'SELECT * FROM message_templates';
    const params = [];
    if (type) { q += ' WHERE type = $1'; params.push(type); }
    q += ' ORDER BY name';
    const result = await pool.query(q, params);
    res.json({ success: true, templates: result.rows });
  } catch (error) { serverError(res, error); }
});

app.get('/api/templates/:id', async (req, res) => {
  try {
    await ensureTemplatesTable();
    const result = await pool.query('SELECT * FROM message_templates WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/templates', async (req, res) => {
  try {
    await ensureTemplatesTable();
    const { name, type, subject, html_content, text_content, category, tags } = req.body;
    if (!name) return res.status(400).json({ success: false, error: 'Name required' });
    const result = await pool.query(
      `INSERT INTO message_templates (name, type, subject, html_content, text_content, category, tags) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [name, type || 'email', subject || '', html_content || '', text_content || '', category || 'General', tags || []]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/templates/:id', async (req, res) => {
  try {
    await ensureTemplatesTable();
    const fields = ['name', 'type', 'subject', 'html_content', 'text_content', 'category', 'tags', 'active'];
    const sets = [], vals = [];
    let p = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) { sets.push(`${f} = $${p++}`); vals.push(req.body[f]); }
    }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push(`updated_at = NOW()`);
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE message_templates SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/templates/:id', async (req, res) => {
  try {
    await ensureTemplatesTable();
    const result = await pool.query('DELETE FROM message_templates WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// Duplicate a template
app.post('/api/templates/:id/duplicate', async (req, res) => {
  try {
    await ensureTemplatesTable();
    const orig = await pool.query('SELECT * FROM message_templates WHERE id = $1', [req.params.id]);
    if (orig.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    const t = orig.rows[0];
    const result = await pool.query(
      `INSERT INTO message_templates (name, type, subject, html_content, text_content, category, tags) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [t.name + ' (Copy)', t.type, t.subject, t.html_content, t.text_content, t.category, t.tags]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ─── Automations / Sequences ────────────────────────────────────────
const ensureAutomationsTable = async () => {
  await pool.query(`CREATE TABLE IF NOT EXISTS automations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    trigger_type VARCHAR(100) NOT NULL,
    trigger_config JSONB DEFAULT '{}',
    conditions JSONB DEFAULT '[]',
    actions JSONB DEFAULT '[]',
    active BOOLEAN DEFAULT true,
    review_before_exec BOOLEAN DEFAULT false,
    run_count INTEGER DEFAULT 0,
    last_run_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS automation_history (
    id SERIAL PRIMARY KEY,
    automation_id INTEGER REFERENCES automations(id) ON DELETE CASCADE,
    triggered_by VARCHAR(255),
    trigger_data JSONB DEFAULT '{}',
    actions_taken JSONB DEFAULT '[]',
    status VARCHAR(50) DEFAULT 'completed',
    created_at TIMESTAMP DEFAULT NOW()
  )`);
};

app.get('/api/automations', async (req, res) => {
  try {
    await ensureAutomationsTable();
    const result = await pool.query('SELECT * FROM automations ORDER BY created_at DESC');
    res.json({ success: true, automations: result.rows });
  } catch (error) { serverError(res, error); }
});

app.get('/api/automations/:id', async (req, res) => {
  try {
    await ensureAutomationsTable();
    const result = await pool.query('SELECT * FROM automations WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, automation: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/automations', async (req, res) => {
  try {
    await ensureAutomationsTable();
    const { name, description, trigger_type, trigger_config, conditions, actions, review_before_exec } = req.body;
    if (!name || !trigger_type) return res.status(400).json({ success: false, error: 'Name and trigger_type required' });
    const result = await pool.query(
      `INSERT INTO automations (name, description, trigger_type, trigger_config, conditions, actions, review_before_exec) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [name, description || '', trigger_type, JSON.stringify(trigger_config || {}), JSON.stringify(conditions || []), JSON.stringify(actions || []), review_before_exec || false]
    );
    res.json({ success: true, automation: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.patch('/api/automations/:id', async (req, res) => {
  try {
    await ensureAutomationsTable();
    const fields = ['name', 'description', 'trigger_type', 'trigger_config', 'conditions', 'actions', 'active', 'review_before_exec'];
    const sets = [], vals = [];
    let p = 1;
    for (const f of fields) {
      if (req.body[f] !== undefined) {
        sets.push(`${f} = $${p++}`);
        vals.push(['trigger_config','conditions','actions'].includes(f) ? JSON.stringify(req.body[f]) : req.body[f]);
      }
    }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields' });
    sets.push(`updated_at = NOW()`);
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE automations SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    res.json({ success: true, automation: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/automations/:id', async (req, res) => {
  try {
    await ensureAutomationsTable();
    await pool.query('DELETE FROM automations WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

app.get('/api/automations/:id/history', async (req, res) => {
  try {
    await ensureAutomationsTable();
    const result = await pool.query('SELECT * FROM automation_history WHERE automation_id = $1 ORDER BY created_at DESC LIMIT 50', [req.params.id]);
    res.json({ success: true, history: result.rows });
  } catch (error) { serverError(res, error); }
});

// 8.5 Customer Activity Timeline
app.get('/api/customers/:id/timeline', async (req, res) => {
  try {
    const { id } = req.params;
    const customer = await pool.query('SELECT name, email, first_name, last_name FROM customers WHERE id = $1', [id]);
    if (customer.rows.length === 0) return res.status(404).json({ success: false, error: 'Customer not found' });
    const c = customer.rows[0];
    const custName = c.name || ((c.first_name || '') + ' ' + (c.last_name || '')).trim();
    const custEmail = c.email || '';

    const events = [];

    // Quotes
    try {
      const quotes = await pool.query(
        `SELECT id, quote_number, total_amount, status, created_at FROM sent_quotes WHERE customer_email = $1 OR customer_name = $2 ORDER BY created_at DESC LIMIT 15`,
        [custEmail, custName]
      );
      quotes.rows.forEach(q => events.push({
        type: 'quote', id: q.id, title: 'Quote #' + (q.quote_number || q.id),
        detail: '$' + (parseFloat(q.total_amount) || 0).toFixed(2) + ' - ' + (q.status || 'sent'),
        date: q.created_at
      }));
    } catch(e) { /* table may not exist */ }

    // Jobs
    try {
      const jobs = await pool.query(
        `SELECT id, service_type, service_price, status, scheduled_date, created_at FROM scheduled_jobs WHERE customer_name = $1 ORDER BY COALESCE(scheduled_date, created_at) DESC LIMIT 15`,
        [custName]
      );
      jobs.rows.forEach(j => events.push({
        type: 'job', id: j.id, title: (j.service_type || 'Job') + ' - ' + (j.status || 'scheduled'),
        detail: '$' + (parseFloat(j.service_price) || 0).toFixed(2),
        date: j.scheduled_date || j.created_at
      }));
    } catch(e) {}

    // Invoices
    try {
      const invoices = await pool.query(
        `SELECT id, invoice_number, total_amount, status, created_at FROM invoices WHERE customer_email = $1 OR customer_name = $2 ORDER BY created_at DESC LIMIT 15`,
        [custEmail, custName]
      );
      invoices.rows.forEach(inv => events.push({
        type: 'invoice', id: inv.id, title: 'Invoice #' + (inv.invoice_number || inv.id),
        detail: '$' + (parseFloat(inv.total_amount) || 0).toFixed(2) + ' - ' + (inv.status || 'sent'),
        date: inv.created_at
      }));
    } catch(e) {}

    // Payments
    try {
      const payments = await pool.query(
        `SELECT p.id, p.amount, p.method, p.status, p.created_at FROM payments p JOIN invoices i ON p.invoice_id = i.id WHERE i.customer_email = $1 OR i.customer_name = $2 ORDER BY p.created_at DESC LIMIT 10`,
        [custEmail, custName]
      );
      payments.rows.forEach(pay => events.push({
        type: 'payment', id: pay.id, title: 'Payment - ' + (pay.method || 'unknown'),
        detail: '$' + (parseFloat(pay.amount) || 0).toFixed(2) + ' - ' + (pay.status || 'completed'),
        date: pay.created_at
      }));
    } catch(e) {}

    // Messages
    try {
      const msgs = await pool.query(
        `SELECT id, direction, channel, body, created_at FROM messages WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 10`,
        [id]
      );
      msgs.rows.forEach(m => events.push({
        type: 'message', id: m.id, title: (m.direction === 'inbound' ? 'Received' : 'Sent') + ' ' + (m.channel || 'message'),
        detail: (m.body || '').substring(0, 80),
        date: m.created_at
      }));
    } catch(e) {}

    // Sort all events by date desc, limit to 50
    events.sort((a, b) => new Date(b.date) - new Date(a.date));
    res.json({ success: true, timeline: events.slice(0, 50) });
  } catch (error) {
    console.error('Customer timeline error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════════
// Phase 7: CRM Intelligence (Rule-based AI features)
// ═══════════════════════════════════════════════════════════════

// 7.1 Lead Scoring — All leads
app.get('/api/ai/lead-scores', async (req, res) => {
  try {
    const customers = await pool.query('SELECT id, name, email, phone, mobile, created_at FROM customers ORDER BY name ASC');
    const results = [];
    for (const c of customers.rows) {
      const scoreData = await computeLeadScore(c);
      results.push({ id: c.id, name: c.name, score: scoreData.score, grade: scoreData.grade, factors: scoreData.factors });
    }
    res.json({ success: true, customers: results });
  } catch (error) {
    serverError(res, error);
  }
});

// 7.1 Lead Scoring — Single customer
app.get('/api/customers/:id/lead-score', async (req, res) => {
  try {
    const cust = await pool.query('SELECT id, name, email, phone, mobile, created_at FROM customers WHERE id = $1', [req.params.id]);
    if (!cust.rows.length) return res.status(404).json({ success: false, error: 'Customer not found' });
    const scoreData = await computeLeadScore(cust.rows[0]);
    res.json({ success: true, id: cust.rows[0].id, name: cust.rows[0].name, score: scoreData.score, grade: scoreData.grade, factors: scoreData.factors });
  } catch (error) {
    serverError(res, error);
  }
});

async function computeLeadScore(c) {
  let score = 0;
  const factors = [];

  // Has email AND phone
  if (c.email && (c.phone || c.mobile)) { score += 10; factors.push('Has email and phone (+10)'); }

  // Has property with address
  const props = await pool.query('SELECT id, street FROM properties WHERE customer_id = $1', [c.id]);
  const hasProperty = props.rows.some(p => p.street);
  if (hasProperty) { score += 10; factors.push('Has property with address (+10)'); }

  // Multiple properties
  if (props.rows.length > 1) { score += 10; factors.push('Multiple properties (+10)'); }

  // Has been quoted
  const quotes = await pool.query('SELECT id, status FROM sent_quotes WHERE customer_id = $1', [c.id]);
  if (quotes.rows.length > 0) { score += 15; factors.push('Has been quoted (+15)'); }

  // Quote was viewed
  const viewed = quotes.rows.some(q => q.status === 'viewed' || q.status === 'signed' || q.status === 'contracted');
  if (viewed) { score += 10; factors.push('Quote was viewed (+10)'); }

  // Quote was signed/contracted
  const contracted = quotes.rows.some(q => q.status === 'signed' || q.status === 'contracted');
  if (contracted) { score += 20; factors.push('Quote signed/contracted (+20)'); }

  // Had a job completed
  const completedJobs = await pool.query("SELECT id FROM scheduled_jobs WHERE customer_id = $1 AND status = 'completed' LIMIT 1", [c.id]);
  if (completedJobs.rows.length > 0) { score += 15; factors.push('Completed job (+15)'); }

  // Recent activity (last 30 days)
  const recentJobs = await pool.query("SELECT id FROM scheduled_jobs WHERE customer_id = $1 AND (created_at >= NOW() - INTERVAL '30 days' OR job_date >= NOW() - INTERVAL '30 days') LIMIT 1", [c.id]);
  const recentQuotes = await pool.query("SELECT id FROM sent_quotes WHERE customer_id = $1 AND created_at >= NOW() - INTERVAL '30 days' LIMIT 1", [c.id]);
  if (recentJobs.rows.length > 0 || recentQuotes.rows.length > 0) { score += 10; factors.push('Recent activity (+10)'); }

  const grade = score >= 50 ? 'Hot' : score >= 25 ? 'Warm' : 'Cold';
  return { score, grade, factors };
}

// 7.2 Smart Scheduling Suggestions
app.get('/api/ai/schedule-suggestions', async (req, res) => {
  try {
    const { date, address, service_type } = req.query;
    if (!date) return res.status(400).json({ success: false, error: 'date parameter required' });

    const crews = await pool.query('SELECT id, name FROM crews WHERE is_active = true OR is_active IS NULL ORDER BY name ASC');
    if (!crews.rows.length) return res.json({ success: true, suggestions: [] });

    // Count jobs per crew on that date
    const jobCounts = await pool.query(
      `SELECT crew_assigned, COUNT(*) as job_count FROM scheduled_jobs WHERE job_date::date = $1::date AND crew_assigned IS NOT NULL GROUP BY crew_assigned`,
      [date]
    );
    const countMap = {};
    jobCounts.rows.forEach(r => { countMap[r.crew_assigned] = parseInt(r.job_count); });

    // Get addresses for that day per crew for geographic clustering
    const dayJobs = await pool.query(
      `SELECT crew_assigned, address FROM scheduled_jobs WHERE job_date::date = $1::date AND crew_assigned IS NOT NULL`,
      [date]
    );
    const crewAddresses = {};
    dayJobs.rows.forEach(r => {
      if (!crewAddresses[r.crew_assigned]) crewAddresses[r.crew_assigned] = [];
      crewAddresses[r.crew_assigned].push((r.address || '').toLowerCase());
    });

    // Extract city/zip from requested address for matching
    const addrLower = (address || '').toLowerCase().trim();
    const addrParts = addrLower.split(/[,\s]+/);
    const zipMatch = addrParts.find(p => /^\d{5}$/.test(p));

    const suggestions = crews.rows.map(crew => {
      let crewScore = 0;
      const reasons = [];

      // Availability: fewer jobs = higher score
      const jobCount = countMap[crew.name] || countMap[String(crew.id)] || 0;
      const availScore = Math.max(0, 20 - jobCount * 5);
      crewScore += availScore;
      if (jobCount === 0) reasons.push('No jobs scheduled - fully available');
      else reasons.push(jobCount + ' job(s) scheduled');

      // Geographic clustering
      const addrs = crewAddresses[crew.name] || crewAddresses[String(crew.id)] || [];
      if (address && addrs.length > 0) {
        const sameArea = addrs.some(a => {
          if (zipMatch && a.includes(zipMatch)) return true;
          return addrParts.some(part => part.length > 3 && a.includes(part));
        });
        if (sameArea) { crewScore += 15; reasons.push('Already working in same area'); }
      }

      return { crew_id: crew.id, crew_name: crew.name, reason: reasons.join('; '), score: crewScore };
    });

    suggestions.sort((a, b) => b.score - a.score);
    res.json({ success: true, suggestions: suggestions.slice(0, 3) });
  } catch (error) {
    serverError(res, error);
  }
});

// 7.3 Review Requests (UI ONLY — no actual sending)
// -- DB Migration needed: CREATE TABLE review_requests (id SERIAL PRIMARY KEY, job_id INTEGER, customer_id INTEGER, status VARCHAR(20) DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW());
app.post('/api/ai/review-request/:jobId', async (req, res) => {
  try {
    // Just return success — no email/SMS sent
    res.json({ success: true, message: 'Review request queued (sending disabled)' });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════════
// REFERRAL PROGRAM
// ═══════════════════════════════════════════════════════════════

// POST /api/referrals - Log a new referral
app.post('/api/referrals', authenticateToken, async (req, res) => {
  try {
    const { referrer_id, referred_name, referred_customer_id, notes } = req.body;
    if (!referrer_id || !referred_name) {
      return res.status(400).json({ success: false, error: 'referrer_id and referred_name are required' });
    }
    const result = await pool.query(
      `INSERT INTO referrals (referrer_id, referred_name, referred_customer_id, notes) VALUES ($1, $2, $3, $4) RETURNING *`,
      [referrer_id, referred_name.trim(), referred_customer_id || null, notes || null]
    );
    res.json({ success: true, referral: result.rows[0] });
  } catch (error) { serverError(res, error, 'Create referral error'); }
});

// GET /api/referrals - List referrals with optional filters
app.get('/api/referrals', authenticateToken, async (req, res) => {
  try {
    const { status, referrer_id } = req.query;
    const where = [];
    const params = [];
    let p = 1;
    if (status) { where.push(`r.status = $${p}`); params.push(status); p++; }
    if (referrer_id) { where.push(`r.referrer_id = $${p}`); params.push(referrer_id); p++; }
    const whereClause = where.length ? 'WHERE ' + where.join(' AND ') : '';
    const result = await pool.query(`
      SELECT r.*, c.name AS referrer_name, c.first_name AS referrer_first_name
      FROM referrals r
      LEFT JOIN customers c ON c.id = r.referrer_id
      ${whereClause}
      ORDER BY r.created_at DESC
    `, params);
    res.json({ success: true, referrals: result.rows });
  } catch (error) { serverError(res, error, 'List referrals error'); }
});

// GET /api/customers/:id/referrals - Referrals for a specific customer
app.get('/api/customers/:id/referrals', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM referrals WHERE referrer_id = $1 ORDER BY created_at DESC`,
      [req.params.id]
    );
    res.json({ success: true, referrals: result.rows });
  } catch (error) { serverError(res, error, 'Customer referrals error'); }
});

// PATCH /api/referrals/:id - Update referral status or details
app.patch('/api/referrals/:id', authenticateToken, async (req, res) => {
  try {
    const { status, referred_customer_id, notes } = req.body;
    const sets = [];
    const params = [];
    let p = 1;
    if (status) { sets.push(`status = $${p}`); params.push(status); p++; if (status === 'credited') { sets.push(`credited_at = NOW()`); } }
    if (referred_customer_id !== undefined) { sets.push(`referred_customer_id = $${p}`); params.push(referred_customer_id); p++; }
    if (notes !== undefined) { sets.push(`notes = $${p}`); params.push(notes); p++; }
    if (sets.length === 0) return res.status(400).json({ success: false, error: 'No fields to update' });
    params.push(req.params.id);
    const result = await pool.query(`UPDATE referrals SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, params);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Referral not found' });
    res.json({ success: true, referral: result.rows[0] });
  } catch (error) { serverError(res, error, 'Update referral error'); }
});

// POST /api/referrals/:id/apply-credit - Apply free mow credit + send thank-you SMS
app.post('/api/referrals/:id/apply-credit', authenticateToken, async (req, res) => {
  try {
    const { mow_price } = req.body;
    const price = parseFloat(mow_price);
    if (!price || price <= 0) return res.status(400).json({ success: false, error: 'mow_price is required and must be positive' });

    // Get the referral
    const ref = await pool.query('SELECT * FROM referrals WHERE id = $1', [req.params.id]);
    if (ref.rows.length === 0) return res.status(404).json({ success: false, error: 'Referral not found' });
    const referral = ref.rows[0];
    if (referral.status === 'credited') return res.status(400).json({ success: false, error: 'Credit already applied for this referral' });

    // Get referrer info
    const cust = await pool.query('SELECT id, name, first_name, last_name, email, phone, mobile, address FROM customers WHERE id = $1', [referral.referrer_id]);
    if (cust.rows.length === 0) return res.status(404).json({ success: false, error: 'Referrer customer not found' });
    const customer = cust.rows[0];
    const custName = customer.name || ((customer.first_name || '') + (customer.last_name ? ' ' + customer.last_name : '')).trim() || 'Unknown';

    // Add negative line item to current draft invoice or create one
    const creditItem = {
      name: 'Mowing',
      description: `FREE MOW - Referral credit (referred: ${referral.referred_name})`,
      quantity: 1,
      rate: -price,
      amount: -price
    };

    const now = new Date();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split('T')[0];
    const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().split('T')[0];

    const existingInv = await pool.query(
      `SELECT id, line_items, subtotal, tax_amount, total FROM invoices
       WHERE customer_id = $1 AND status = 'draft'
       AND created_at >= $2 AND created_at <= ($3::date + interval '1 day')
       ORDER BY created_at DESC LIMIT 1`,
      [customer.id, monthStart, monthEnd]
    );

    let invoiceId;
    if (existingInv.rows.length > 0) {
      const inv = existingInv.rows[0];
      let items = inv.line_items || [];
      if (typeof items === 'string') items = JSON.parse(items);
      items.push(creditItem);
      const taxResult = await calculateTax(customer.id, null, items);
      await pool.query(
        `UPDATE invoices SET line_items = $1, subtotal = $2, tax_amount = $3, total = $4, updated_at = CURRENT_TIMESTAMP WHERE id = $5`,
        [JSON.stringify(taxResult.lineItems), taxResult.subtotal, taxResult.taxAmount, taxResult.total, inv.id]
      );
      invoiceId = inv.id;
    } else {
      const invNum = await nextInvoiceNumber();
      const dueDate = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().split('T')[0];
      const taxResult = await calculateTax(customer.id, null, [creditItem]);
      const invResult = await pool.query(
        `INSERT INTO invoices (invoice_number, customer_id, customer_name, customer_email, customer_address, status, subtotal, tax_rate, tax_amount, total, due_date, notes, line_items)
         VALUES ($1, $2, $3, $4, $5, 'draft', $6, 0, $7, $8, $9, $10, $11) RETURNING id`,
        [invNum, customer.id, custName, customer.email || '', customer.address || '', taxResult.subtotal, taxResult.taxAmount, taxResult.total, dueDate,
         'Referral credit - ' + now.toLocaleDateString('en-US', {month:'long', year:'numeric'}),
         JSON.stringify(taxResult.lineItems)]
      );
      invoiceId = invResult.rows[0].id;
    }

    // Mark referral as credited
    await pool.query('UPDATE referrals SET status = $1, credited_at = NOW() WHERE id = $2', ['credited', req.params.id]);

    // Send thank-you SMS
    let smsSent = false;
    const phone = customer.mobile || customer.phone;
    if (phone && twilioClient) {
      try {
        const firstName = customer.first_name || custName;
        const smsBody = `Hey ${firstName}! It's Tim from Pappas & Co. Your neighbor ${referral.referred_name} signed up and mentioned your name, so you've got a free mow coming. We'll apply it to your next service. Thanks for helping us grow in the neighborhood!`;
        const digits = phone.replace(/\D/g, '');
        const toNumber = digits.length === 10 ? '+1' + digits : digits.length === 11 ? '+' + digits : null;
        if (toNumber) {
          await twilioClient.messages.create({ body: smsBody, from: TWILIO_PHONE_NUMBER, to: toNumber });
          smsSent = true;
          // Log to messages table
          try {
            await pool.query(
              `INSERT INTO messages (twilio_sid, direction, from_number, to_number, body, media_urls, status, read) VALUES ($1, 'outbound', $2, $3, $4, '{}', 'sent', true)`,
              ['referral-' + Date.now(), TWILIO_PHONE_NUMBER, toNumber, smsBody]
            );
          } catch (e) { /* ignore logging error */ }
        }
      } catch (smsErr) {
        console.error('Referral thank-you SMS error:', smsErr.message);
      }
    }

    console.log(`🎉 Referral credit applied: ${custName} gets free mow for referring ${referral.referred_name}`);
    res.json({ success: true, invoiceId, smsSent, referral: { ...referral, status: 'credited' } });
  } catch (error) { serverError(res, error, 'Apply referral credit error'); }
});

// 7.4 Churn Prediction
app.get('/api/ai/churn-risk', async (req, res) => {
  try {
    const customers = await pool.query('SELECT id, name FROM customers ORDER BY name ASC');
    const results = [];

    for (const c of customers.rows) {
      let riskScore = 0;
      const factors = [];

      // No jobs in last 90 days but had jobs before
      const recentJobs = await pool.query("SELECT id FROM scheduled_jobs WHERE customer_id = $1 AND job_date >= NOW() - INTERVAL '90 days' LIMIT 1", [c.id]);
      const anyJobs = await pool.query("SELECT id FROM scheduled_jobs WHERE customer_id = $1 LIMIT 1", [c.id]);
      if (anyJobs.rows.length > 0 && recentJobs.rows.length === 0) {
        riskScore += 30; factors.push('No jobs in last 90 days (+30)');
      }

      // Has unpaid invoices over 60 days
      const overdueInv = await pool.query(
        "SELECT id FROM invoices WHERE customer_id = $1 AND status NOT IN ('paid', 'draft') AND due_date < NOW() - INTERVAL '60 days' LIMIT 1",
        [c.id]
      );
      if (overdueInv.rows.length > 0) {
        riskScore += 25; factors.push('Unpaid invoices over 60 days (+25)');
      }

      // Had a cancellation
      const cancellations = await pool.query(
        "SELECT id FROM cancellations WHERE LOWER(customer_name) = LOWER($1) LIMIT 1",
        [c.name || '']
      );
      if (cancellations.rows.length > 0) {
        riskScore += 20; factors.push('Has cancellation record (+20)');
      }

      // No upcoming scheduled jobs
      const upcomingJobs = await pool.query(
        "SELECT id FROM scheduled_jobs WHERE customer_id = $1 AND job_date >= CURRENT_DATE AND status != 'completed' LIMIT 1",
        [c.id]
      );
      if (anyJobs.rows.length > 0 && upcomingJobs.rows.length === 0) {
        riskScore += 15; factors.push('No upcoming scheduled jobs (+15)');
      }

      // Declining job frequency
      const thisQ = await pool.query(
        "SELECT COUNT(*) FROM scheduled_jobs WHERE customer_id = $1 AND job_date >= NOW() - INTERVAL '90 days'", [c.id]
      );
      const lastQ = await pool.query(
        "SELECT COUNT(*) FROM scheduled_jobs WHERE customer_id = $1 AND job_date >= NOW() - INTERVAL '180 days' AND job_date < NOW() - INTERVAL '90 days'", [c.id]
      );
      const thisQCount = parseInt(thisQ.rows[0].count);
      const lastQCount = parseInt(lastQ.rows[0].count);
      if (lastQCount > 0 && thisQCount < lastQCount) {
        riskScore += 10; factors.push('Declining job frequency (+10)');
      }

      if (riskScore > 0) {
        const risk_level = riskScore >= 50 ? 'High' : riskScore >= 25 ? 'Medium' : 'Low';
        results.push({ id: c.id, name: c.name, risk_level, risk_score: riskScore, factors });
      }
    }

    results.sort((a, b) => b.risk_score - a.risk_score);
    res.json({ success: true, customers: results });
  } catch (error) {
    serverError(res, error);
  }
});

// 7.5 Revenue Forecasting
app.get('/api/ai/revenue-forecast', async (req, res) => {
  try {
    const months = parseInt(req.query.months) || 3;
    const forecast = [];

    // Pipeline and historical don't change per iteration — query once
    const [pipeline, historical] = await Promise.all([
      pool.query(`SELECT COALESCE(SUM(total), 0) as total FROM sent_quotes
         WHERE status IN ('signed', 'contracted')
         AND id NOT IN (SELECT sent_quote_id FROM invoices WHERE sent_quote_id IS NOT NULL)`),
      pool.query(`SELECT COALESCE(AVG(monthly_total), 0) as avg_monthly FROM (
           SELECT DATE_TRUNC('month', paid_at) as m, SUM(amount_paid) as monthly_total
           FROM invoices WHERE paid_at >= NOW() - INTERVAL '6 months' AND status = 'paid'
           GROUP BY DATE_TRUNC('month', paid_at)) sub`)
    ]);
    const pipelineRevPerMonth = ((parseFloat(pipeline.rows[0].total) || 0) * 0.8) / months;
    const historicalRev = parseFloat(historical.rows[0].avg_monthly) || 0;

    // Fetch all scheduled revenue in parallel
    const scheduledResults = await Promise.all(
      Array.from({ length: months }, (_, idx) => pool.query(
        `SELECT COALESCE(SUM(service_price), 0) as total FROM scheduled_jobs
         WHERE job_date >= DATE_TRUNC('month', CURRENT_DATE + ($1::text || ' months')::INTERVAL)
         AND job_date < DATE_TRUNC('month', CURRENT_DATE + ($1::text || ' months')::INTERVAL) + INTERVAL '1 month'
         AND status != 'cancelled'`,
        [idx + 1]
      ))
    );

    for (let i = 1; i <= months; i++) {
      const scheduledRev = parseFloat(scheduledResults[i - 1].rows[0].total) || 0;
      const pipelineRev = pipelineRevPerMonth;

      const targetDate = new Date();
      targetDate.setMonth(targetDate.getMonth() + i);
      const monthLabel = targetDate.toLocaleString('en-US', { month: 'long', year: 'numeric' });

      const predicted = scheduledRev + pipelineRev + historicalRev;
      forecast.push({
        month: monthLabel,
        predicted_revenue: Math.round(predicted * 100) / 100,
        breakdown: {
          scheduled: Math.round(scheduledRev * 100) / 100,
          pipeline: Math.round(pipelineRev * 100) / 100,
          historical: Math.round(historicalRev * 100) / 100
        }
      });
    }

    res.json({ success: true, forecast });
  } catch (error) {
    serverError(res, error);
  }
});

// 7.6 Smart Campaign Segments
app.get('/api/ai/campaign-segments', async (req, res) => {
  try {
    const segments = [];

    // "Inactive 90+ days" - had service but none recently
    const inactive = await pool.query(
      `SELECT DISTINCT c.id FROM customers c
       INNER JOIN scheduled_jobs j ON j.customer_id = c.id
       WHERE c.id NOT IN (
         SELECT DISTINCT customer_id FROM scheduled_jobs WHERE customer_id IS NOT NULL AND job_date >= NOW() - INTERVAL '90 days'
       )`
    );
    segments.push({ name: 'Inactive 90+ days', description: 'Had service but none in last 90 days', count: inactive.rows.length, customer_ids: inactive.rows.map(r => r.id) });

    // "High Value" - top 20% by total invoice amount
    const allInvTotals = await pool.query(
      `SELECT customer_id, SUM(total) as total_spend FROM invoices WHERE customer_id IS NOT NULL GROUP BY customer_id ORDER BY total_spend DESC`
    );
    const top20Pct = Math.max(1, Math.ceil(allInvTotals.rows.length * 0.2));
    const highValueIds = allInvTotals.rows.slice(0, top20Pct).map(r => r.customer_id);
    segments.push({ name: 'High Value', description: 'Top 20% by total invoice amount', count: highValueIds.length, customer_ids: highValueIds });

    // "New Leads" - created in last 30 days, no jobs yet
    const newLeads = await pool.query(
      `SELECT c.id FROM customers c
       WHERE c.created_at >= NOW() - INTERVAL '30 days'
       AND c.id NOT IN (SELECT DISTINCT customer_id FROM scheduled_jobs WHERE customer_id IS NOT NULL)`
    );
    segments.push({ name: 'New Leads', description: 'Created in last 30 days, no jobs yet', count: newLeads.rows.length, customer_ids: newLeads.rows.map(r => r.id) });

    // "Repeat Customers" - 3+ completed jobs
    const repeat = await pool.query(
      `SELECT customer_id FROM scheduled_jobs WHERE customer_id IS NOT NULL AND status = 'completed' GROUP BY customer_id HAVING COUNT(*) >= 3`
    );
    segments.push({ name: 'Repeat Customers', description: '3+ completed jobs', count: repeat.rows.length, customer_ids: repeat.rows.map(r => r.customer_id) });

    // "Overdue Payments" - has unpaid invoices past due
    const overdue = await pool.query(
      `SELECT DISTINCT customer_id FROM invoices WHERE customer_id IS NOT NULL AND status NOT IN ('paid', 'draft') AND due_date < CURRENT_DATE`
    );
    segments.push({ name: 'Overdue Payments', description: 'Has unpaid invoices past due', count: overdue.rows.length, customer_ids: overdue.rows.map(r => r.customer_id) });

    res.json({ success: true, segments });
  } catch (error) {
    serverError(res, error);
  }
});

// ─── Twilio Voice SDK: Access Token ─────────────────────────────────────────
app.get('/api/app/voice/token', authenticateToken, (req, res) => {
  try {
    const AccessToken = twilio.jwt.AccessToken;
    const VoiceGrant = AccessToken.VoiceGrant;

    const identity = req.user.email;

    const accessToken = new AccessToken(
      TWILIO_ACCOUNT_SID,
      process.env.TWILIO_API_KEY_SID,
      process.env.TWILIO_API_KEY_SECRET,
      { identity }
    );

    const pushCredentialSid = process.env.TWILIO_PUSH_CREDENTIAL_SID || 'CR0cf89f77173745be7d6de6eac56cad7d';

    const voiceGrant = new VoiceGrant({
      outgoingApplicationSid: process.env.TWILIO_TWIML_APP_SID,
      incomingAllow: true,
      pushCredentialSid: pushCredentialSid,
    });

    accessToken.addGrant(voiceGrant);

    console.log('Voice token generated for:', identity, 'push:', pushCredentialSid);
    res.json({ token: accessToken.toJwt(), identity });
  } catch (error) {
    console.error('Voice token error:', error);
    res.status(500).json({ error: 'Failed to generate voice token' });
  }
});

// ─── Twilio Voice SDK: TwiML for outgoing calls from app ────────────────────
app.all('/api/voice/twiml', (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  const to = req.body.To || req.query.To;

  if (to) {
    const dial = twiml.dial({
      callerId: req.body.From || TWILIO_PHONE_NUMBER,
    });

    if (to.startsWith('client:')) {
      dial.client(to.replace('client:', ''));
    } else {
      dial.number(to);
    }
  } else {
    twiml.say('No destination specified.');
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

// ─── Voicemails (proxy to webhook server) ─────────────────────────────────────

const WEBHOOK_BASE = 'https://pappas-twilio-webhook-production.up.railway.app';

app.get('/api/app/voicemails', authenticateToken, async (req, res) => {
  try {
    const filter = req.query.filter || 'active';
    let url;
    if (filter === 'handled') {
      url = `${WEBHOOK_BASE}/api/calls?status=handled&limit=100`;
    } else if (filter === 'archived') {
      url = `${WEBHOOK_BASE}/api/calls?status=archived&limit=100`;
    } else {
      url = `${WEBHOOK_BASE}/api/calls?status=voicemail&limit=100`;
    }
    const response = await fetch(url);
    if (!response.ok) throw new Error('Webhook fetch failed');
    const data = await response.json();
    const voicemails = (data.calls || []).map(c => ({
      id: c.id,
      phoneNumber: c.from_number || '',
      contactName: c.customer_name || null,
      duration: c.duration ? parseInt(c.duration) : 0,
      transcription: c.transcription || null,
      timestamp: c.created_at || '',
      audioUrl: c.recording_url || null,
      listened: c.read || false,
      status: c.status || 'voicemail',
    }));
    res.json({ voicemails });
  } catch (err) {
    console.error('Voicemails proxy error:', err);
    res.status(500).json({ error: 'Failed to fetch voicemails' });
  }
});

app.post('/api/app/voicemails/:id/play', authenticateToken, async (req, res) => {
  try {
    const response = await fetch(`${WEBHOOK_BASE}/api/calls/${req.params.id}/read`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
    });
    if (!response.ok) throw new Error('Webhook update failed');
    res.json({ success: true });
  } catch (err) {
    console.error('Voicemail mark-played error:', err);
    res.status(500).json({ error: 'Failed to mark voicemail as played' });
  }
});

app.post('/api/app/voicemails/:id/handle', authenticateToken, async (req, res) => {
  try {
    const response = await fetch(`${WEBHOOK_BASE}/api/calls/${req.params.id}/status`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: 'handled' }),
    });
    if (!response.ok) throw new Error('Webhook update failed');
    res.json({ success: true });
  } catch (err) {
    console.error('Voicemail handle error:', err);
    res.status(500).json({ error: 'Failed to update voicemail' });
  }
});

app.delete('/api/app/voicemails/:id', authenticateToken, async (req, res) => {
  try {
    const response = await fetch(`${WEBHOOK_BASE}/api/calls/${req.params.id}/status`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: 'archived' }),
    });
    if (!response.ok) throw new Error('Webhook update failed');
    res.json({ success: true });
  } catch (err) {
    console.error('Voicemail archive error:', err);
    res.status(500).json({ error: 'Failed to archive voicemail' });
  }
});

// NOTE: Catch-all moved to end of file after all route definitions

// Diagnostic: test quote PDF generation directly
app.get('/api/test-quote-pdf/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM sent_quotes WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'Quote not found' });
    const quote = result.rows[0];
    console.log('🧪 Testing PDF generation for quote #' + (quote.quote_number || quote.id));
    const pdfResult = await generateQuotePDF(quote);
    if (!pdfResult || !pdfResult.bytes) return res.status(500).json({ error: 'PDF generation failed', pdfError: pdfResult ? pdfResult.error : 'returned null' });
    if (pdfResult.error) {
      console.log('⚠️ Test PDF used fallback. Main error: ' + pdfResult.error);
    }
    console.log('🧪 PDF type: ' + pdfResult.type + ', size: ' + pdfResult.bytes.length);
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'inline; filename="test-quote-' + (quote.quote_number || quote.id) + '.pdf"');
    res.setHeader('X-PDF-Type', pdfResult.type);
    if (pdfResult.error) res.setHeader('X-PDF-Error', pdfResult.error.substring(0, 200));
    res.send(Buffer.from(pdfResult.bytes));
  } catch (err) {
    console.error('Test PDF error:', err);
    serverError(res, err);
  }
});

// Payments table helper — delegates to lib/startup-schema.js
async function ensurePaymentsTables() {
  await _ensurePaymentsTables(pool);
}

// ═══════════════════════════════════════════════════════════
// STARTUP MIGRATIONS — delegated to lib/startup-schema.js
// ═══════════════════════════════════════════════════════════
runStartupMigrations(pool);


// ═══════════════════════════════════════════════════════════
// ═══ WORK REQUESTS ENDPOINTS ══════════════════════════════
// ═══════════════════════════════════════════════════════════

app.get('/api/work-requests/stats', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
        COUNT(*) FILTER (WHERE status = 'in-progress')::int AS in_progress,
        COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
        COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled
      FROM service_requests
    `);
    res.json({ success: true, stats: result.rows[0] });
  } catch (error) {
    console.error('Work requests stats error:', error);
    serverError(res, error);
  }
});

app.get('/api/work-requests', async (req, res) => {
  try {
    const { status, search, limit = 50, offset = 0 } = req.query;
    let query = `
      SELECT sr.*, c.name as customer_name, c.email as customer_email, c.phone as customer_phone, c.street as customer_address
      FROM service_requests sr
      LEFT JOIN customers c ON sr.customer_id = c.id
      WHERE 1=1
    `;
    let countQuery = 'SELECT COUNT(*) FROM service_requests WHERE 1=1';
    const params = [], countParams = [];
    let p = 1, cp = 1;

    if (status) {
      query += ` AND sr.status = $${p++}`;
      countQuery += ` AND status = $${cp++}`;
      params.push(status);
      countParams.push(status);
    }
    if (search) {
      query += ` AND (c.name ILIKE $${p} OR sr.service_type ILIKE $${p} OR sr.description ILIKE $${p})`;
      countQuery += ` AND (service_type ILIKE $${cp} OR description ILIKE $${cp})`;
      params.push(`%${search}%`);
      countParams.push(`%${search}%`);
      p++; cp++;
    }

    query += ` ORDER BY sr.created_at DESC LIMIT $${p++} OFFSET $${p}`;
    params.push(limit, offset);

    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query(countQuery, countParams)
    ]);
    res.json({ success: true, requests: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) {
    console.error('Work requests error:', error);
    serverError(res, error);
  }
});

app.get('/api/work-requests/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT sr.*, c.name as customer_name, c.email as customer_email, c.phone as customer_phone, c.street as customer_address
      FROM service_requests sr
      LEFT JOIN customers c ON sr.customer_id = c.id
      WHERE sr.id = $1
    `, [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, request: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.put('/api/work-requests/:id', async (req, res) => {
  try {
    const { status, admin_notes } = req.body;
    const sets = ['updated_at = CURRENT_TIMESTAMP'];
    const vals = [];
    let p = 1;
    if (status) { sets.push(`status = $${p++}`); vals.push(status); }
    if (admin_notes !== undefined) { sets.push(`admin_notes = $${p++}`); vals.push(admin_notes); }
    vals.push(req.params.id);
    const result = await pool.query(`UPDATE service_requests SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`, vals);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, request: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// ═══ TIME TRACKING ENDPOINTS ══════════════════════════════
// ═══════════════════════════════════════════════════════════

app.get('/api/time-entries', async (req, res) => {
  try {
    const { date, crew_id, status, limit = 100, offset = 0 } = req.query;
    let query = 'SELECT * FROM time_entries WHERE 1=1';
    const params = [];
    let p = 1;
    if (date) { query += ` AND clock_in::date = $${p++}`; params.push(date); }
    if (crew_id) { query += ` AND crew_id = $${p++}`; params.push(crew_id); }
    if (status) { query += ` AND status = $${p++}`; params.push(status); }
    query += ` ORDER BY clock_in DESC LIMIT $${p++} OFFSET $${p}`;
    params.push(limit, offset);
    const [result, countResult] = await Promise.all([
      pool.query(query, params),
      pool.query('SELECT COUNT(*) FROM time_entries')
    ]);
    res.json({ success: true, entries: result.rows, total: parseInt(countResult.rows[0].count) });
  } catch (error) { serverError(res, error); }
});

app.get('/api/time-entries/stats', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const [active, todayEntries, todayHours, weekHours, activeEntries] = await Promise.all([
      pool.query("SELECT COUNT(*) FROM time_entries WHERE status = 'active' AND clock_out IS NULL"),
      pool.query("SELECT COUNT(*) FROM time_entries WHERE clock_in::date = $1", [today]),
      pool.query("SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(clock_out, NOW()) - clock_in)) / 3600), 0)::numeric as hours FROM time_entries WHERE clock_in::date = $1", [today]),
      pool.query("SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(clock_out, NOW()) - clock_in)) / 3600), 0)::numeric as hours FROM time_entries WHERE clock_in >= date_trunc('week', CURRENT_DATE)"),
      pool.query("SELECT * FROM time_entries WHERE status = 'active' AND clock_out IS NULL ORDER BY clock_in DESC")
    ]);
    res.json({ success: true, stats: {
      activeClockedIn: parseInt(active.rows[0].count),
      todayEntries: parseInt(todayEntries.rows[0].count),
      todayHours: parseFloat(todayHours.rows[0].hours).toFixed(1),
      weekHours: parseFloat(weekHours.rows[0].hours).toFixed(1),
      activeEntries: activeEntries.rows
    }});
  } catch (error) { serverError(res, error); }
});

app.post('/api/time-entries/clock-in', async (req, res) => {
  try {
    const { crew_id, crew_name, job_id, customer_name, address, service_type, notes } = req.body;
    if (!crew_name) return res.status(400).json({ success: false, error: 'Crew name required' });
    const result = await pool.query(
      `INSERT INTO time_entries (crew_id, crew_name, job_id, customer_name, address, service_type, clock_in, notes) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7) RETURNING *`,
      [crew_id, crew_name, job_id, customer_name, address, service_type, notes]
    );
    res.json({ success: true, entry: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/time-entries/:id/clock-out', async (req, res) => {
  try {
    const { break_minutes, notes } = req.body;
    const sets = ["clock_out = NOW()", "status = 'completed'"];
    const params = [];
    let p = 1;
    if (break_minutes !== undefined) { sets.push(`break_minutes = $${p++}`); params.push(break_minutes); }
    if (notes !== undefined) { sets.push(`notes = $${p++}`); params.push(notes); }
    params.push(req.params.id);
    const result = await pool.query(`UPDATE time_entries SET ${sets.join(', ')} WHERE id = $${p} AND clock_out IS NULL RETURNING *`, params);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Entry not found or already clocked out' });
    res.json({ success: true, entry: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.put('/api/time-entries/:id', async (req, res) => {
  try {
    const { clock_in, clock_out, break_minutes, notes, crew_name, customer_name, address, service_type } = req.body;
    const result = await pool.query(
      `UPDATE time_entries SET clock_in = COALESCE($1, clock_in), clock_out = COALESCE($2, clock_out), break_minutes = COALESCE($3, break_minutes), notes = COALESCE($4, notes), crew_name = COALESCE($5, crew_name), customer_name = COALESCE($6, customer_name), address = COALESCE($7, address), service_type = COALESCE($8, service_type) WHERE id = $9 RETURNING *`,
      [clock_in, clock_out, break_minutes, notes, crew_name, customer_name, address, service_type, req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, entry: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/time-entries/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM time_entries WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.get('/api/time-entries/weekly-report', async (req, res) => {
  try {
    const { week_start } = req.query;
    const startDate = week_start || new Date(Date.now() - new Date().getDay() * 86400000).toISOString().split('T')[0];
    const result = await pool.query(`
      SELECT crew_name,
        COUNT(*) as total_entries,
        COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(clock_out, NOW()) - clock_in)) / 3600), 0)::numeric as total_hours,
        COALESCE(SUM(break_minutes), 0) as total_break_minutes
      FROM time_entries
      WHERE clock_in::date >= $1 AND clock_in::date < ($1::date + INTERVAL '7 days')
      GROUP BY crew_name ORDER BY crew_name
    `, [startDate]);
    res.json({ success: true, report: result.rows, weekStart: startDate });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// ═══ TIMECLOCK PDF PARSER ═════════════════════════════════
// ═══════════════════════════════════════════════════════════

app.post('/api/timeclock/parse-pdf', uploadPdf.single('pdf'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: 'No PDF file uploaded' });

    const pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js');
    const data = new Uint8Array(req.file.buffer);
    const doc = await pdfjsLib.getDocument({ data, disableFontFace: true, useSystemFonts: true }).promise;
    let allText = '';
    for (let i = 1; i <= doc.numPages; i++) {
      const page = await doc.getPage(i);
      const content = await page.getTextContent();
      allText += content.items.map(item => item.str).join(' ') + '\n';
    }
    doc.destroy();

    // Parse Jobber-style time tracking rows from extracted text
    // Format: "Mar 19, 2026   Camacho, Wilkyn   ,   7:48 am   7:05 pm   11 hrs. 17 min."
    const entries = [];
    const rowPattern = /(\w{3}\s+\d{1,2},\s*\d{4})\s+([A-Za-z]+,\s*[A-Za-z]+)\s+,?\s+(\d{1,2}:\d{2}\s*[ap]m)\s+(\d{1,2}:\d{2}\s*[ap]m)\s+(\d+)\s*hrs?\.?\s*(\d+)\s*min/gi;
    let match;
    while ((match = rowPattern.exec(allText)) !== null) {
      const hours = parseInt(match[5]);
      const minutes = parseInt(match[6]);
      const employee = match[2].trim().replace(/,\s*/, ', ');
      entries.push({
        employee,
        date: match[1],
        start: match[3],
        end: match[4],
        hours,
        minutes,
        totalHours: parseFloat((hours + minutes / 60).toFixed(2))
      });
    }

    // Fallback: try more generic pattern if Jobber format didn't match
    if (entries.length === 0) {
      const genericRow = /(\w+\s+\d{1,2},?\s*\d{4}|\d{1,2}\/\d{1,2}\/\d{2,4})\s+(.+?)\s+(\d{1,2}:\d{2}\s*[ap]m)\s+(\d{1,2}:\d{2}\s*[ap]m)\s+(\d+)\s*(?:hrs?\.?|h)\s*(\d+)\s*(?:mins?\.?|m)/gi;
      while ((match = genericRow.exec(allText)) !== null) {
        const hours = parseInt(match[5]);
        const minutes = parseInt(match[6]);
        entries.push({
          employee: match[2].trim().replace(/\s*,\s*$/, ''),
          date: match[1],
          start: match[3],
          end: match[4],
          hours,
          minutes,
          totalHours: parseFloat((hours + minutes / 60).toFixed(2))
        });
      }
    }

    // If still no entries, try calculating from start/end times
    if (entries.length === 0) {
      const timeOnly = /(\w+\s+\d{1,2},?\s*\d{4}|\d{1,2}\/\d{1,2}\/\d{2,4})\s+(.+?)\s+(\d{1,2}:\d{2}\s*[ap]m)\s+(\d{1,2}:\d{2}\s*[ap]m)/gi;
      while ((match = timeOnly.exec(allText)) !== null) {
        const parseTime = (t) => {
          const m = t.match(/(\d+):(\d+)\s*([ap]m)/i);
          if (!m) return 0;
          let h = parseInt(m[1]), min = parseInt(m[2]);
          if (m[3].toLowerCase() === 'pm' && h !== 12) h += 12;
          if (m[3].toLowerCase() === 'am' && h === 12) h = 0;
          return h * 60 + min;
        };
        const startMin = parseTime(match[3]);
        const endMin = parseTime(match[4]);
        const diff = endMin > startMin ? endMin - startMin : (24 * 60 - startMin + endMin);
        const hours = Math.floor(diff / 60);
        const minutes = diff % 60;
        entries.push({
          employee: match[2].trim().replace(/\s*,\s*$/, ''),
          date: match[1],
          start: match[3],
          end: match[4],
          hours,
          minutes,
          totalHours: parseFloat((hours + minutes / 60).toFixed(2))
        });
      }
    }

    // Group by employee
    const byEmployee = {};
    for (const e of entries) {
      if (!byEmployee[e.employee]) {
        byEmployee[e.employee] = { entries: [], totalHours: 0, totalMinutes: 0, decimalHours: 0 };
      }
      byEmployee[e.employee].entries.push(e);
      byEmployee[e.employee].totalHours += e.hours;
      byEmployee[e.employee].totalMinutes += e.minutes;
      byEmployee[e.employee].decimalHours += e.totalHours;
    }

    // Normalize minutes overflow
    for (const emp of Object.keys(byEmployee)) {
      const d = byEmployee[emp];
      d.totalHours += Math.floor(d.totalMinutes / 60);
      d.totalMinutes = d.totalMinutes % 60;
    }

    // Check for report total — matches "Total Working time: 20 hrs. 26 mins." and similar
    const reportTotalMatch = allText.match(/total\s+(?:working\s+)?time:\s*(\d+)\s*hrs?\.?\s*(\d+)\s*mins?\.?/i);
    const reportTotal = reportTotalMatch ? { hours: parseInt(reportTotalMatch[1]), minutes: parseInt(reportTotalMatch[2]) } : null;

    // Load saved pay rates and attach to response
    const ratesResult = await pool.query("SELECT value FROM business_settings WHERE key = 'crew_pay_rates'");
    const payRates = ratesResult.rows.length ? (typeof ratesResult.rows[0].value === 'string' ? JSON.parse(ratesResult.rows[0].value) : ratesResult.rows[0].value) : {};

    res.json({ success: true, entries, byEmployee, reportTotal, payRates });
  } catch (error) { serverError(res, error, 'Timeclock PDF parse'); }
});

// Save crew pay rates
app.put('/api/timeclock/pay-rates', async (req, res) => {
  try {
    const { rates } = req.body;
    if (!rates || typeof rates !== 'object') return res.status(400).json({ success: false, error: 'Invalid rates' });
    await pool.query(
      `INSERT INTO business_settings (key, value) VALUES ('crew_pay_rates', $1)
       ON CONFLICT (key) DO UPDATE SET value = $1`,
      [JSON.stringify(rates)]
    );
    res.json({ success: true });
  } catch (error) { serverError(res, error, 'Save pay rates'); }
});

// Get crew pay rates
app.get('/api/timeclock/pay-rates', async (req, res) => {
  try {
    const result = await pool.query("SELECT value FROM business_settings WHERE key = 'crew_pay_rates'");
    const rates = result.rows.length ? (typeof result.rows[0].value === 'string' ? JSON.parse(result.rows[0].value) : result.rows[0].value) : {};
    res.json({ success: true, rates });
  } catch (error) { serverError(res, error, 'Get pay rates'); }
});

// ═══════════════════════════════════════════════════════════
// ═══ DISPATCH TEMPLATES ENDPOINTS ═════════════════════════
// ═══════════════════════════════════════════════════════════

app.get('/api/dispatch-templates', async (req, res) => {
  try {
    const result = await pool.query('SELECT dt.*, c.name as crew_display_name FROM dispatch_templates dt LEFT JOIN crews c ON dt.crew_id = c.id ORDER BY dt.name');
    res.json({ success: true, templates: result.rows });
  } catch (error) { serverError(res, error); }
});

app.post('/api/dispatch-templates', async (req, res) => {
  try {
    const { name, zip_codes, crew_id, service_type, default_duration, notes } = req.body;
    if (!name) return res.status(400).json({ success: false, error: 'Name required' });
    const result = await pool.query(
      'INSERT INTO dispatch_templates (name, zip_codes, crew_id, service_type, default_duration, notes) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
      [name, zip_codes, crew_id, service_type, default_duration || 30, notes]
    );
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.put('/api/dispatch-templates/:id', async (req, res) => {
  try {
    const { name, zip_codes, crew_id, service_type, default_duration, notes } = req.body;
    const result = await pool.query(
      'UPDATE dispatch_templates SET name=$1, zip_codes=$2, crew_id=$3, service_type=$4, default_duration=$5, notes=$6 WHERE id=$7 RETURNING *',
      [name, zip_codes, crew_id, service_type, default_duration, notes, req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, template: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.delete('/api/dispatch-templates/:id', async (req, res) => {
  try {
    const result = await pool.query('DELETE FROM dispatch_templates WHERE id = $1 RETURNING *', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

// Quick dispatch: find template by zip code, create job
app.post('/api/dispatch-templates/quick-dispatch', async (req, res) => {
  try {
    const { address, zip_code, customer_name, customer_id, job_date } = req.body;
    if (!zip_code || !job_date) return res.status(400).json({ success: false, error: 'Zip code and date required' });
    // Find matching template
    const templates = await pool.query("SELECT * FROM dispatch_templates WHERE zip_codes LIKE $1", [`%${zip_code}%`]);
    if (templates.rows.length === 0) return res.status(404).json({ success: false, error: 'No template found for zip ' + zip_code });
    const t = templates.rows[0];
    const job = await pool.query(
      `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, address, crew_assigned, estimated_duration, status) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending') RETURNING *`,
      [job_date, customer_name, customer_id, t.service_type, address, t.crew_id, t.default_duration]
    );
    res.json({ success: true, job: job.rows[0], template: t });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// ═══ SERVICE PROGRAMS ENDPOINTS ═══════════════════════════
// ═══════════════════════════════════════════════════════════

app.get('/api/service-programs', async (req, res) => {
  try {
    const programs = await pool.query('SELECT * FROM service_programs ORDER BY name');
    // Get step counts
    const steps = await pool.query('SELECT program_id, COUNT(*) as step_count FROM program_steps GROUP BY program_id');
    const stepMap = {};
    steps.rows.forEach(s => stepMap[s.program_id] = parseInt(s.step_count));
    const result = programs.rows.map(p => ({ ...p, step_count: stepMap[p.id] || 0 }));
    res.json({ success: true, programs: result });
  } catch (error) { serverError(res, error); }
});

app.get('/api/service-programs/:id', async (req, res) => {
  try {
    const [program, steps, enrollments] = await Promise.all([
      pool.query('SELECT * FROM service_programs WHERE id = $1', [req.params.id]),
      pool.query('SELECT * FROM program_steps WHERE program_id = $1 ORDER BY step_order', [req.params.id]),
      pool.query('SELECT cp.*, c.name as customer_name FROM customer_programs cp LEFT JOIN customers c ON cp.customer_id = c.id WHERE cp.program_id = $1 ORDER BY cp.created_at DESC', [req.params.id])
    ]);
    if (program.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, program: program.rows[0], steps: steps.rows, enrollments: enrollments.rows });
  } catch (error) { serverError(res, error); }
});

app.post('/api/service-programs', async (req, res) => {
  try {
    const { name, description, steps } = req.body;
    if (!name) return res.status(400).json({ success: false, error: 'Name required' });
    const program = await pool.query('INSERT INTO service_programs (name, description) VALUES ($1, $2) RETURNING *', [name, description]);
    const programId = program.rows[0].id;
    if (steps && steps.length > 0) {
      for (let i = 0; i < steps.length; i++) {
        const s = steps[i];
        await pool.query('INSERT INTO program_steps (program_id, step_order, service_type, description, estimated_duration, offset_days, price) VALUES ($1, $2, $3, $4, $5, $6, $7)',
          [programId, i + 1, s.service_type, s.description, s.estimated_duration || 30, s.offset_days || 0, s.price]);
      }
    }
    res.json({ success: true, program: program.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.put('/api/service-programs/:id', async (req, res) => {
  try {
    const { name, description, status, steps } = req.body;
    await pool.query('UPDATE service_programs SET name = COALESCE($1, name), description = COALESCE($2, description), status = COALESCE($3, status) WHERE id = $4', [name, description, status, req.params.id]);
    if (steps) {
      await pool.query('DELETE FROM program_steps WHERE program_id = $1', [req.params.id]);
      for (let i = 0; i < steps.length; i++) {
        const s = steps[i];
        await pool.query('INSERT INTO program_steps (program_id, step_order, service_type, description, estimated_duration, offset_days, price) VALUES ($1, $2, $3, $4, $5, $6, $7)',
          [req.params.id, i + 1, s.service_type, s.description, s.estimated_duration || 30, s.offset_days || 0, s.price]);
      }
    }
    const updated = await pool.query('SELECT * FROM service_programs WHERE id = $1', [req.params.id]);
    res.json({ success: true, program: updated.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/service-programs/:id/enroll', async (req, res) => {
  try {
    const { customer_id, property_id, start_date } = req.body;
    if (!customer_id || !start_date) return res.status(400).json({ success: false, error: 'Customer ID and start date required' });
    const enrollment = await pool.query(
      'INSERT INTO customer_programs (customer_id, program_id, property_id, start_date) VALUES ($1, $2, $3, $4) RETURNING *',
      [customer_id, req.params.id, property_id, start_date]
    );
    // Generate jobs for all steps
    const steps = await pool.query('SELECT * FROM program_steps WHERE program_id = $1 ORDER BY step_order', [req.params.id]);
    const customer = await pool.query('SELECT * FROM customers WHERE id = $1', [customer_id]);
    const cust = customer.rows[0] || {};
    for (const step of steps.rows) {
      const jobDate = new Date(new Date(start_date).getTime() + step.offset_days * 86400000).toISOString().split('T')[0];
      await pool.query(
        `INSERT INTO scheduled_jobs (job_date, customer_name, customer_id, service_type, address, service_price, estimated_duration, status, special_notes) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)`,
        [jobDate, cust.name, customer_id, step.service_type, cust.street || '', step.price || 0, step.estimated_duration, `Program step ${step.step_order}: ${step.description || ''}`]
      );
    }
    res.json({ success: true, enrollment: enrollment.rows[0], jobsCreated: steps.rows.length });
  } catch (error) { serverError(res, error); }
});

// ═══════════════════════════════════════════════════════════
// ═══ KPI DETAILED ENDPOINT ════════════════════════════════
// ═══════════════════════════════════════════════════════════

app.get('/api/kpi/detailed', async (req, res) => {
  try {
    const now = new Date();
    const thisMonth = now.toISOString().slice(0, 7);
    const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1).toISOString().slice(0, 7);

    const [monthlyRevenue, lastMonthRevenue, totalQuotesSent, acceptedQuotes, avgClose, jobsThisWeek, jobsThisMonth, revenueByService, newCustomers, totalCustomerCount, totalRevenueAll, pendingQuotes, bookedOut, activeLeads, revenueTrend] = await Promise.all([
      pool.query("SELECT COALESCE(SUM(amount), 0)::numeric as total FROM payments WHERE created_at >= date_trunc('month', CURRENT_DATE)"),
      pool.query("SELECT COALESCE(SUM(amount), 0)::numeric as total FROM payments WHERE created_at >= date_trunc('month', CURRENT_DATE - INTERVAL '1 month') AND created_at < date_trunc('month', CURRENT_DATE)"),
      pool.query("SELECT COUNT(*) FROM sent_quotes WHERE created_at >= date_trunc('month', CURRENT_DATE)"),
      pool.query("SELECT COUNT(*) FROM sent_quotes WHERE status IN ('accepted','signed','contracted') AND created_at >= date_trunc('month', CURRENT_DATE)"),
      pool.query("SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 86400)::numeric as avg_days FROM sent_quotes WHERE status IN ('accepted','signed','contracted') AND updated_at > created_at"),
      pool.query("SELECT COUNT(*) FROM scheduled_jobs WHERE status IN ('completed','done') AND job_date >= date_trunc('week', CURRENT_DATE)"),
      pool.query("SELECT COUNT(*) FROM scheduled_jobs WHERE status IN ('completed','done') AND job_date >= date_trunc('month', CURRENT_DATE)"),
      pool.query("SELECT service_type, COUNT(*) as job_count, COALESCE(SUM(service_price), 0)::numeric as revenue FROM scheduled_jobs WHERE status IN ('completed','done') AND job_date >= date_trunc('month', CURRENT_DATE) GROUP BY service_type ORDER BY revenue DESC LIMIT 10"),
      pool.query("SELECT COUNT(*) FROM customers WHERE created_at >= date_trunc('month', CURRENT_DATE)"),
      pool.query("SELECT COUNT(*) FROM customers"),
      pool.query("SELECT COALESCE(SUM(amount), 0)::numeric as total FROM payments"),
      pool.query("SELECT COUNT(*) as count, COALESCE(SUM(total), 0)::numeric as value FROM sent_quotes WHERE status IN ('sent','viewed','pending')"),
      pool.query("SELECT MAX(job_date) as furthest_date FROM scheduled_jobs WHERE status IN ('pending','scheduled') AND job_date >= CURRENT_DATE"),
      pool.query("SELECT COUNT(*) FROM customers WHERE customer_type = 'lead'"),
      pool.query(`SELECT to_char(date_trunc('month', created_at), 'YYYY-MM') as month,
             COALESCE(SUM(amount), 0)::numeric as revenue
      FROM payments WHERE created_at >= CURRENT_DATE - INTERVAL '6 months'
      GROUP BY date_trunc('month', created_at) ORDER BY month ASC`)
    ]);

    const totalSent = parseInt(totalQuotesSent.rows[0].count);
    const totalAccepted = parseInt(acceptedQuotes.rows[0].count);
    const closeRatio = totalSent > 0 ? Math.round((totalAccepted / totalSent) * 100) : 0;
    const custCount = parseInt(totalCustomerCount.rows[0].count);
    const ltv = custCount > 0 ? (parseFloat(totalRevenueAll.rows[0].total) / custCount).toFixed(2) : 0;
    const furthestDate = bookedOut.rows[0].furthest_date;
    const bookedOutDays = furthestDate ? Math.ceil((new Date(furthestDate) - now) / 86400000) : 0;

    res.json({ success: true, kpi: {
      monthlyRevenue: parseFloat(monthlyRevenue.rows[0].total),
      lastMonthRevenue: parseFloat(lastMonthRevenue.rows[0].total),
      closeRatio,
      avgDaysToClose: avgClose.rows[0].avg_days ? parseFloat(avgClose.rows[0].avg_days).toFixed(1) : null,
      jobsThisWeek: parseInt(jobsThisWeek.rows[0].count),
      jobsThisMonth: parseInt(jobsThisMonth.rows[0].count),
      revenueByService: revenueByService.rows,
      newCustomers: parseInt(newCustomers.rows[0].count),
      customerLTV: parseFloat(ltv),
      pendingQuotes: { count: parseInt(pendingQuotes.rows[0].count), value: parseFloat(pendingQuotes.rows[0].value) },
      bookedOutDays,
      activeLeads: parseInt(activeLeads.rows[0].count),
      revenueTrend: revenueTrend.rows
    }});
  } catch (error) {
    console.error('KPI detailed error:', error);
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// AI-POWERED FEATURES (Claude SDK)
// ═══════════════════════════════════════════════════════════

// 7.10 AI Quote Writer
app.post('/api/ai/generate-quote', async (req, res) => {
  try {
    if (!anthropicClient) {
      return res.status(503).json({ success: false, error: 'AI service not configured. ANTHROPIC_API_KEY is not set.' });
    }

    const { address, customer_name, services, notes } = req.body;
    if (!address || !customer_name) {
      return res.status(400).json({ success: false, error: 'address and customer_name are required' });
    }

    const availableServices = Object.keys(SERVICE_DESCRIPTIONS);
    const currentMonth = new Date().toLocaleString('default', { month: 'long' });

    const prompt = `You are the quoting assistant for Pappas & Co. Landscaping, a professional landscaping company based in Northeast Ohio (Lakewood / Cleveland West Side).

The current month is ${currentMonth}. Generate a professional landscaping quote for the following customer:

Customer: ${customer_name}
Address: ${address}
${services && services.length > 0 ? `Requested services: ${services.join(', ')}` : 'No specific services requested — recommend appropriate seasonal services.'}
${notes ? `Additional notes: ${notes}` : ''}

Available services we offer: ${availableServices.join(', ')}

Guidelines:
- For March/spring, prioritize: Spring Cleanup, Mowing (Weekly), Mulching, Aeration, Fertilizing - Early Spring
- Use typical Northeast Ohio residential landscaping pricing (e.g., weekly mowing $35-65, spring cleanup $150-350, mulching $200-500, aeration $80-200, fertilizer application $50-100)
- Write professional but friendly service descriptions (2-3 sentences each)
- Include a frequency for recurring services (e.g., "weekly", "one-time", "per application")

Respond ONLY with valid JSON in this exact format (no markdown, no code fences):
{
  "services": [
    {"name": "Service Name", "description": "Professional description", "price": 150, "frequency": "one-time"}
  ],
  "total": 500,
  "notes": "Any relevant notes about the quote",
  "summary": "One paragraph professional summary of the quote"
}`;

    const message = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      messages: [{ role: 'user', content: prompt }]
    });

    const responseText = message.content[0].text.trim();
    let quote;
    try {
      quote = JSON.parse(responseText);
    } catch (parseErr) {
      const jsonMatch = responseText.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        quote = JSON.parse(jsonMatch[0]);
      } else {
        throw new Error('Failed to parse AI response as JSON');
      }
    }

    res.json({ success: true, quote });
  } catch (error) {
    console.error('AI generate-quote error:', error);
    serverError(res, error);
  }
});

// 7.11 AI Follow-up Message Generator
app.post('/api/ai/generate-followup', async (req, res) => {
  try {
    if (!anthropicClient) {
      return res.status(503).json({ success: false, error: 'AI service not configured. ANTHROPIC_API_KEY is not set.' });
    }

    const { customer_name, service_type, quote_amount, days_since_sent, channel } = req.body;
    if (!customer_name || !channel) {
      return res.status(400).json({ success: false, error: 'customer_name and channel (email|sms) are required' });
    }

    const isSMS = channel === 'sms';

    const prompt = `You are the follow-up assistant for Pappas & Co. Landscaping, a professional landscaping company in Northeast Ohio (Lakewood / Cleveland West Side).

Generate a follow-up message to close the deal with this customer:

Customer: ${customer_name}
Service: ${service_type || 'landscaping services'}
Quote amount: ${quote_amount ? '$' + quote_amount : 'not specified'}
Days since quote was sent: ${days_since_sent || 'unknown'}
Channel: ${channel}

${isSMS ? `IMPORTANT: This is an SMS message. Keep it under 160 characters. Be casual but professional. Do not include a subject line.` : `This is an email. Write a full professional email with a subject line. Use HTML formatting for the body (paragraphs, bold for key points). Keep it concise but warm and persuasive.`}

Guidelines:
- Reference the specific service and amount if provided
- Create urgency if appropriate (seasonal timing, booking up fast, etc.)
- Be friendly and professional, not pushy
- Sign off as "Pappas & Co. Landscaping" or "The Pappas Team"

Respond ONLY with valid JSON in this exact format (no markdown, no code fences):
${isSMS ? '{"body": "The SMS message text"}' : '{"subject": "Email subject line", "body": "<p>HTML email body</p>"}'}`;

    const message = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      messages: [{ role: 'user', content: prompt }]
    });

    const responseText = message.content[0].text.trim();
    let result;
    try {
      result = JSON.parse(responseText);
    } catch (parseErr) {
      const jsonMatch = responseText.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        result = JSON.parse(jsonMatch[0]);
      } else {
        throw new Error('Failed to parse AI response as JSON');
      }
    }

    res.json({ success: true, message: result });
  } catch (error) {
    console.error('AI generate-followup error:', error);
    serverError(res, error);
  }
});

// 7.12 AI Chat / Ask Anything
app.post('/api/ai/chat', async (req, res) => {
  try {
    if (!anthropicClient) {
      return res.status(503).json({ success: false, error: 'AI service not configured. ANTHROPIC_API_KEY is not set.' });
    }

    const { message: userMessage, context } = req.body;
    if (!userMessage) {
      return res.status(400).json({ success: false, error: 'message is required' });
    }

    const availableServices = Object.keys(SERVICE_DESCRIPTIONS);

    const systemPrompt = `You are the AI assistant for Pappas & Co. Landscaping, a professional landscaping company based in Northeast Ohio (Lakewood / Cleveland West Side).

You help the business owner and staff with:
- Answering questions about landscaping services, pricing, and scheduling
- Drafting customer messages (emails, texts)
- Suggesting pricing based on typical Northeast Ohio residential rates
- Providing seasonal landscaping advice relevant to the Cleveland/Lakewood climate
- General business advice for a landscaping company

Available services: ${availableServices.join(', ')}

Typical pricing ranges (Northeast Ohio residential):
- Weekly mowing: $35-65/visit
- Spring cleanup: $150-350
- Fall cleanup: $200-400
- Mulching: $200-500 (depends on bed size)
- Aeration: $80-200
- Overseeding: $100-250
- Fertilizer application: $50-100/application
- Shrub trimming: $100-300
- Power washing: $150-400
- Bed edging: $75-200

Keep responses concise, practical, and professional. If asked about specific customer situations, provide helpful advice based on the context given.`;

    const messages = [{ role: 'user', content: userMessage }];

    if (context) {
      messages[0].content = `Context: ${JSON.stringify(context)}\n\nQuestion: ${userMessage}`;
    }

    const response = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      system: systemPrompt,
      messages
    });

    const reply = response.content[0].text;
    res.json({ success: true, reply });
  } catch (error) {
    console.error('AI chat error:', error);
    serverError(res, error);
  }
});

// ─── AI Template Generator (Chat-based) ──────────────────────────
app.post('/api/ai/generate-template', async (req, res) => {
  try {
    if (!anthropicClient) {
      return res.status(503).json({ success: false, error: 'AI service not configured.' });
    }
    const { prompt, type, action, history, apply } = req.body;
    if (!prompt) return res.status(400).json({ success: false, error: 'prompt is required' });

    const systemPrompt = `You are an interactive AI template assistant for Pappas & Co. Landscaping, a professional landscaping company in Northeast Ohio (Cleveland West Side).

You help create, edit, and improve email and SMS templates through conversation. You can:
- Generate new email templates
- Rewrite or tweak existing templates
- Suggest subject lines
- Write SMS versions
- Answer questions about email marketing best practices
- Make specific changes the user asks for (tone, length, wording, etc.)

BRAND GUIDELINES:
- Dark forest green: #2e403d
- Lime accent: #c9dd80
- Company: Pappas & Co. Landscaping
- Tagline: "Your Property, Our Priority."
- Location: Northeast Ohio (Cleveland West Side)

Available merge tags: {customer_name}, {customer_first_name}, {company_name}, {company_phone}, {company_email}, {company_website}, {address}, {quote_link}, {invoice_link}, {quote_total}, {services_list}, {payment_link}, {invoice_number}, {invoice_total}, {job_date}, {service_type}, {crew_name}, {portal_link}, {contract_link}, {quote_number}, {balance_due}, {amount_paid}, {invoice_due_date}

BUTTON URL RULES — When creating buttons, ALWAYS use the appropriate merge tag as the URL:
- "View Quote" / "Review Quote" / "Accept Quote" → url: "{quote_link}"
- "Pay Now" / "Pay Invoice" / "Make Payment" / "View Invoice" → url: "{payment_link}"
- "Visit Portal" / "Access Portal" / "My Account" → url: "{portal_link}"
- "Sign Agreement" / "Sign Contract" → url: "{contract_link}"
Never use "#" or placeholder URLs. Always use the matching merge tag so buttons work when the email is sent.

RESPONSE FORMAT — Return ONLY valid JSON (no markdown, no backticks). Choose the appropriate format:

1. When generating/rewriting an EMAIL template:
{"message": "Brief description of what you created/changed", "subject": "Email subject line", "blocks": [{"type":"title|paragraph|button|list|divider", "content":"..."}]}
Block types:
- title: {"type":"title","content":"heading text"}
- paragraph: {"type":"paragraph","content":"text, can include <strong> tags"}
- button: {"type":"button","content":"Button Text","url":"{merge_tag_link}"}
- list: {"type":"list","content":["item 1","item 2"]}
- divider: {"type":"divider"}

2. When suggesting SUBJECT LINES:
{"message": "Here are some options:", "subjects": ["Subject 1", "Subject 2", "Subject 3", "Subject 4", "Subject 5"]}

3. When generating an SMS:
{"message": "Here's the SMS:", "sms": "The SMS text under 160 chars"}

4. When just answering a question or chatting (no template output):
{"message": "Your conversational response here"}

5. When the user asks to CREATE A CAMPAIGN (e.g. "create a spring cleanup campaign", "set up a campaign for..."):
{"message": "Description of the campaign", "campaign": {"name": "Campaign Name", "description": "Brief campaign description", "subject": "Email subject line", "blocks": [same block format as email templates], "sms": "Optional SMS version under 160 chars", "audience": "all|monthly_plan|active"}}
- audience: "all" = all customers, "monthly_plan" = monthly plan customers, "active" = customers with jobs in last 6 months
- Always include email blocks AND an SMS version for campaigns
- Campaign names should be catchy but professional (e.g. "Spring Cleanup 2026", "Fall Leaf Removal Special")

Keep the tone professional but warm and friendly. Use merge tags where appropriate. Sign off emails as "The Pappas & Co. Landscaping Team".`;

    // Build messages array with conversation history
    const messages = [];
    if (Array.isArray(history) && history.length > 0) {
      // Include up to last 10 exchanges for context
      const recentHistory = history.slice(-20);
      for (const msg of recentHistory) {
        if (msg.role === 'user' || msg.role === 'assistant') {
          messages.push({ role: msg.role, content: msg.content });
        }
      }
    }
    // Add current user message
    let userMsg = prompt;
    if (apply) userMsg += '\n\n[INSTRUCTION: Generate the template and mark it for auto-apply.]';
    messages.push({ role: 'user', content: userMsg });

    const response = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2048,
      system: systemPrompt,
      messages
    });

    const text = response.content[0].text;
    let parsed;
    try {
      const jsonMatch = text.match(/[\[{][\s\S]*[\]}]/);
      parsed = jsonMatch ? JSON.parse(jsonMatch[0]) : JSON.parse(text);
    } catch (e) {
      // Fallback: treat as plain text message
      parsed = { message: text };
    }
    // Mark auto-apply if requested
    if (apply && parsed.blocks) parsed._auto_apply = true;
    res.json({ success: true, result: parsed });
  } catch (error) {
    console.error('AI template generation error:', error);
    serverError(res, error);
  }
});

// AI-powered campaign creation
app.post('/api/ai/create-campaign', async (req, res) => {
  try {
    const { name, description, subject, body, sms_body, audience } = req.body;
    if (!name) return res.status(400).json({ success: false, error: 'Campaign name is required' });

    // 1. Create email template for this campaign
    let templateId = null;
    if (subject && body) {
      const slug = 'ai_campaign_' + name.toLowerCase().replace(/[^a-z0-9]+/g, '_').slice(0, 50) + '_' + Date.now();
      const tmplResult = await pool.query(
        `INSERT INTO email_templates (name, slug, category, subject, body, sms_body, is_active, created_at, updated_at)
         VALUES ($1, $2, 'marketing', $3, $4, $5, true, NOW(), NOW()) RETURNING id`,
        [name, slug, subject, body, sms_body || '']
      );
      templateId = tmplResult.rows[0].id;
    }

    // 2. Create the campaign
    const campResult = await pool.query(
      `INSERT INTO campaigns (name, description, template_id, status, created_at, updated_at)
       VALUES ($1, $2, $3, 'active', NOW(), NOW()) RETURNING *`,
      [name, description || '', templateId]
    );
    const campaign = campResult.rows[0];

    res.json({ success: true, campaign, template_id: templateId });
  } catch (error) {
    console.error('AI campaign creation error:', error);
    serverError(res, error);
  }
});

// ─── AI Service Title & Description Suggestions ──────────────────
app.post('/api/ai/suggest-service', async (req, res) => {
  try {
    if (!anthropicClient) {
      return res.status(503).json({ success: false, error: 'AI service not configured.' });
    }
    const { service_name, context } = req.body;
    if (!service_name) return res.status(400).json({ success: false, error: 'service_name is required' });

    const prompt = `You are the service description writer for Pappas & Co. Landscaping, a professional landscaping company in Northeast Ohio (Cleveland West Side).

Given the service name or rough idea below, provide:
1. A polished, professional service title
2. A detailed service description (3-6 bullet points written as short paragraphs, each starting with a bold label like "Label: description text"). The description should explain what's included, how the work is done, and set customer expectations.

Service name/idea: "${service_name}"
${context ? `Additional context: ${context}` : ''}

Respond in JSON format:
{
  "title": "Professional Service Title",
  "description": "Label1: Description paragraph.\\nLabel2: Description paragraph.\\nLabel3: Description paragraph."
}

Guidelines:
- Write in first-person plural ("We will...", "Our team...")
- Be specific about equipment, methods, and cleanup
- Keep descriptions professional but approachable
- Each bullet point should be 1-2 sentences
- Use landscaping industry terminology
- DO NOT include pricing`;

    const response = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      messages: [{ role: 'user', content: prompt }]
    });

    const text = response.content[0].text;
    let parsed;
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      parsed = jsonMatch ? JSON.parse(jsonMatch[0]) : JSON.parse(text);
    } catch (e) {
      parsed = { title: service_name, description: text };
    }
    res.json({ success: true, suggestion: parsed });
  } catch (error) {
    console.error('AI service suggestion error:', error);
    serverError(res, error);
  }
});

// ─── Social Media AI Generator ──────────────────────────
app.post('/api/social-media/generate', authenticateToken, async (req, res) => {
  try {
    if (!anthropicClient) {
      return res.status(503).json({ success: false, error: 'AI service not configured.' });
    }

    const { postType, tone, context, platform } = req.body;
    if (!context) {
      return res.status(400).json({ success: false, error: 'Please provide content context' });
    }

    const platforms = platform
      ? [platform]
      : ['facebook', 'instagram', 'nextdoor', 'tiktok', 'google', 'twitter'];

    const systemPrompt = `You are a social media manager for Pappas & Co. Landscaping, a professional landscaping company in Lakewood/Cleveland, Ohio.

Company info:
- Name: Pappas & Co. Landscaping (ALWAYS use the full name "Pappas & Co. Landscaping" — NEVER shorten to "Pappas & Co." or "Pappas")
- Owner: Tim Pappas
- Areas served: Lakewood, Brook Park, Bay Village, and Westpark
- Services: mowing, spring/fall cleanup, mulching, aeration, weed control, shrub trimming, landscaping
- Phone: 440-886-7318
- Instagram/TikTok: @pappaslandscaping
- Voice: Friendly, community-focused, professional but approachable. Tim is the face of the company.

Generate social media posts for the following platforms: ${platforms.join(', ')}

Rules:
- Each platform should have a tailored version (e.g., shorter for Twitter/X with 280 char limit, hashtag-heavy for Instagram, community-focused for Nextdoor, professional for Google Business)
- Use relevant hashtags for Instagram and TikTok
- Nextdoor posts should feel neighborly — mention specific neighborhoods when relevant
- Facebook posts can be longer and more conversational
- Google posts should be professional and include a call to action
- Twitter/X must be under 280 characters
- Include emojis where appropriate for the platform
- Tone: ${tone || 'professional'}

Respond ONLY with valid JSON in this exact format, no markdown or extra text:
{
  "facebook": { "text": "..." },
  "instagram": { "text": "..." },
  "nextdoor": { "text": "..." },
  "tiktok": { "text": "..." },
  "google": { "text": "..." },
  "twitter": { "text": "..." }
}

${platform ? `Only generate for: ${platform}` : ''}`;

    const response = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2048,
      system: systemPrompt,
      messages: [{ role: 'user', content: `Post topic: ${context}\nTone: ${tone || 'professional'}` }]
    });

    const text = response.content[0].text;
    let posts;
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      posts = jsonMatch ? JSON.parse(jsonMatch[0]) : JSON.parse(text);
    } catch (e) {
      console.error('Social media AI parse error:', e.message, 'Raw:', text);
      return res.status(500).json({ success: false, error: 'Failed to parse AI response' });
    }

    // Save to history
    try {
      await pool.query(`CREATE TABLE IF NOT EXISTS social_media_posts (
        id SERIAL PRIMARY KEY,
        post_type VARCHAR(100),
        tone VARCHAR(50),
        context TEXT,
        posts JSONB,
        created_by INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`);
      await pool.query(
        'INSERT INTO social_media_posts (post_type, tone, context, posts, created_by) VALUES ($1, $2, $3, $4, $5)',
        [postType || 'Custom', tone, context, JSON.stringify(posts), req.user?.id]
      );
    } catch (dbErr) {
      console.error('Social media history save error:', dbErr.message);
    }

    res.json({ success: true, posts });
  } catch (error) {
    console.error('Social media generate error:', error);
    serverError(res, error);
  }
});

// POST /api/social-media/refine - Refine existing posts or have a conversation
app.post('/api/social-media/refine', authenticateToken, async (req, res) => {
  try {
    if (!anthropicClient) {
      return res.status(503).json({ success: false, error: 'AI service not configured.' });
    }

    const { currentPosts, instruction, tone, originalContext } = req.body;
    if (!instruction) {
      return res.status(400).json({ success: false, error: 'Please provide an instruction' });
    }

    const systemPrompt = `You are a social media manager for Pappas & Co. Landscaping in Lakewood/Cleveland, Ohio.
ALWAYS use the full name "Pappas & Co. Landscaping" — NEVER shorten to "Pappas & Co." or "Pappas".
Owner: Tim Pappas. Phone: 440-886-7318. Instagram/TikTok: @pappaslandscaping.
Voice: Friendly, community-focused, professional but approachable.

You're in an ongoing conversation helping create and refine social media posts. The user has already generated posts and is now asking for changes, feedback, or new ideas.

Current posts:
${JSON.stringify(currentPosts, null, 2)}

Original topic: ${originalContext || 'not specified'}
Tone: ${tone || 'professional'}

The user may:
1. Ask you to edit specific posts (e.g., "make the Facebook one shorter", "add more hashtags to Instagram")
2. Ask for your opinion or suggestions (e.g., "which one is best?", "what hashtags should I use?")
3. Ask about best practices (e.g., "what time should I post?", "should I use a photo?")
4. Request completely new variations

If the user is asking to MODIFY posts, respond with JSON like:
{"reply": "your conversational message", "posts": { "facebook": {"text": "..."}, ... }}
Only include platforms that changed in the posts object.

If the user is asking a QUESTION or for advice (not modifying posts), respond with just:
{"reply": "your helpful answer"}

Always respond with valid JSON only, no markdown.`;

    const response = await anthropicClient.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2048,
      system: systemPrompt,
      messages: [{ role: 'user', content: instruction }]
    });

    const text = response.content[0].text;
    let parsed;
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      parsed = jsonMatch ? JSON.parse(jsonMatch[0]) : JSON.parse(text);
    } catch (e) {
      // If AI didn't return JSON, treat the whole response as a reply
      parsed = { reply: text };
    }

    res.json({ success: true, reply: parsed.reply || null, posts: parsed.posts || null });
  } catch (error) {
    console.error('Social media refine error:', error);
    serverError(res, error);
  }
});

// GET /api/social-media/history
app.get('/api/social-media/history', authenticateToken, async (req, res) => {
  try {
    await pool.query(`CREATE TABLE IF NOT EXISTS social_media_posts (
      id SERIAL PRIMARY KEY,
      post_type VARCHAR(100),
      tone VARCHAR(50),
      context TEXT,
      posts JSONB,
      created_by INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    const result = await pool.query('SELECT * FROM social_media_posts ORDER BY created_at DESC LIMIT 20');
    res.json({ success: true, posts: result.rows });
  } catch (error) {
    console.error('Social media history error:', error);
    serverError(res, error);
  }
});

// GET /api/quotes/next-number - Get next sequential quote number
app.get('/api/quotes/next-number', async (req, res) => {
  try {
    // Ensure sent_quotes table exists before querying
    await pool.query(`CREATE TABLE IF NOT EXISTS sent_quotes (
      id SERIAL PRIMARY KEY, quote_number VARCHAR(50), customer_name VARCHAR(255),
      customer_email VARCHAR(255), status VARCHAR(50) DEFAULT 'draft',
      sign_token VARCHAR(255), services JSONB, total DECIMAL(10,2),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);
    const result = await pool.query(
      `SELECT MAX(CAST(quote_number AS INTEGER)) as max_num
       FROM sent_quotes
       WHERE quote_number ~ '^[0-9]+$'`
    );
    const maxNum = result.rows[0]?.max_num || 1500;
    res.json({ success: true, next_number: maxNum + 1 });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// HOME BASE ADDRESS SETTINGS
// ═══════════════════════════════════════════════════════════

// GET /api/settings/home-base — returns the home base address
app.get('/api/settings/home-base', async (req, res) => {
  try {
    const result = await pool.query("SELECT value FROM business_settings WHERE key = 'home_base'");
    if (result.rows.length > 0) {
      return res.json({ success: true, ...result.rows[0].value });
    }
    res.json({ success: true, address: '', lat: 41.4268, lng: -81.7356 });
  } catch (error) {
    serverError(res, error);
  }
});

// POST /api/settings/home-base — saves address and geocodes it
app.post('/api/settings/home-base', async (req, res) => {
  try {
    const { address } = req.body;
    if (!address || !address.trim()) return res.status(400).json({ success: false, error: 'Address is required' });

    // Geocode the address
    let lat = null, lng = null;
    const GMAPS_KEY = process.env.GOOGLE_MAPS_API_KEY;
    if (GMAPS_KEY) {
      try {
        const gRes = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(address)}&key=${GMAPS_KEY}`);
        const gData = await gRes.json();
        if (gData.status === 'OK' && gData.results && gData.results.length > 0) {
          lat = gData.results[0].geometry.location.lat;
          lng = gData.results[0].geometry.location.lng;
        }
      } catch (e) { console.error('Google geocode failed for home base, trying Nominatim:', e.message); }
    }
    if (!lat || !lng) {
      try {
        const nRes = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(address)}&limit=1&countrycodes=us`);
        const nData = await nRes.json();
        if (nData && nData.length > 0) {
          lat = parseFloat(nData[0].lat);
          lng = parseFloat(nData[0].lon);
        }
      } catch (e) { console.error('Nominatim geocode failed for home base:', e.message); }
    }
    if (!lat || !lng) return res.status(400).json({ success: false, error: 'Could not geocode the address. Please check and try again.' });

    const value = JSON.stringify({ address: address.trim(), lat, lng });
    await pool.query(
      `INSERT INTO business_settings (key, value, updated_at) VALUES ('home_base', $1::jsonb, NOW())
       ON CONFLICT (key) DO UPDATE SET value = $1::jsonb, updated_at = NOW()`,
      [value]
    );
    res.json({ success: true, address: address.trim(), lat, lng });
  } catch (error) {
    serverError(res, error);
  }
});

// ═══════════════════════════════════════════════════════════
// COPILOT ROUTE SYNC
// ═══════════════════════════════════════════════════════════

async function ensureCopilotSyncTables() {
  await _ensureCopilotSyncTables(pool);
}

async function getCopilotToken() {
  const result = await pool.query("SELECT key, value FROM copilot_sync_settings WHERE key IN ('copilot_token', 'copilot_cookies')");
  const settings = {};
  for (const row of result.rows) settings[row.key] = row.value;

  // Full cookie string takes priority — contains session cookies + JWT
  const cookies = settings.copilot_cookies || null;
  const token = settings.copilot_token || null;

  if (!cookies && !token) return null;

  // Build the cookie header: use full cookie string if available, otherwise just the JWT
  const cookieHeader = cookies || `copilotApiAccessToken=${token}`;

  // Try to extract JWT expiry from the cookie string or standalone token
  let expiresAt = null, daysUntilExpiry = null;
  const jwtMatch = (cookies || '').match(/copilotApiAccessToken=([^;]+)/) || (token ? [null, token] : null);
  if (jwtMatch && jwtMatch[1]) {
    try {
      const payload = JSON.parse(Buffer.from(jwtMatch[1].split('.')[1], 'base64').toString());
      expiresAt = new Date(payload.exp * 1000);
      daysUntilExpiry = (expiresAt - new Date()) / (1000 * 60 * 60 * 24);
    } catch { /* not a valid JWT — that's ok */ }
  }

  return { cookieHeader, expiresAt, daysUntilExpiry };
}

function parseCopilotRouteHtml(html, employeesArray) {
  const $ = cheerio.load(html);
  const jobs = [];
  $('tr[data-row-event-id]').each((i, row) => {
    const $row = $(row);
    const eventId = $row.attr('data-row-event-id');

    // Customer name + ID from link
    const customerLink = $row.find('td.column-3 a');
    const customerName = customerLink.text().trim();
    const customerHref = customerLink.attr('href') || '';
    const customerIdMatch = customerHref.match(/\/(\d+)/);
    const customerId = customerIdMatch ? customerIdMatch[1] : null;

    // Crew name — first text node of span.row-crew-label (before the <small>)
    const crewLabel = $row.find('span.row-crew-label');
    const crewName = crewLabel.contents().filter(function() { return this.nodeType === 3; }).first().text().trim();

    // Employees from small tag
    const employeesText = $row.find('span.row-crew-label small').text().trim();

    // Address
    const address = $row.find('td.column-13').text().trim();

    // Status
    const status = $row.find('span.status-label').text().trim();

    // Visit total
    const visitTotal = $row.find('td.column-11.visit-total-column').text().trim();

    // Job title
    const jobTitle = $row.find('td.column-2 span.mr-1').text().trim();

    // Stop order
    const stopOrderText = $row.find('div.dispatch__order_number').text().trim();
    const stopOrder = parseInt(stopOrderText, 10) || null;

    const parsed = {
      event_id: eventId,
      customer_name: customerName,
      customer_id: customerId,
      crew_name: crewName,
      employees: employeesText,
      address,
      status,
      visit_total: visitTotal,
      job_title: jobTitle,
      stop_order: stopOrder
    };

    jobs.push(parsed);
  });
  return jobs;
}

// POST /api/copilot/sync — fetch CopilotCRM route data and sync to local DB
app.post('/api/copilot/sync', authenticateToken, async (req, res) => {
  try {
    await ensureCopilotSyncTables();

    // Date range — defaults to today
    const today = new Date().toISOString().slice(0, 10);
    const startDate = req.body.startDate || today;
    const endDate = req.body.endDate || startDate;

    // Get token
    const tokenInfo = await getCopilotToken();
    if (!tokenInfo || !tokenInfo.cookieHeader) {
      return res.status(500).json({ success: false, error: 'No CopilotCRM cookies configured. Insert full browser cookie string into copilot_sync_settings with key=copilot_cookies.' });
    }

    // Warn if expiring soon
    let tokenWarning = null;
    if (tokenInfo.daysUntilExpiry !== null && tokenInfo.daysUntilExpiry < 7) {
      tokenWarning = `CopilotCRM token expires in ${Math.round(tokenInfo.daysUntilExpiry)} days (${tokenInfo.expiresAt.toISOString().slice(0, 10)}). Refresh soon.`;
    }

    // Fetch from CopilotCRM
    // Format dates as "Mar 26, 2026" to match CopilotCRM's expected format
    function formatCopilotDate(dateStr) {
      const d = new Date(dateStr + 'T00:00:00');
      return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }
    const sDateFormatted = formatCopilotDate(startDate);
    const eDateFormatted = formatCopilotDate(endDate);

    const formData = new URLSearchParams();
    formData.append('accessFrom', 'route');
    formData.append('bs4', '1');
    formData.append('sDate', sDateFormatted);
    formData.append('eDate', eDateFormatted);
    formData.append('optimizationFlag', '1');
    formData.append('count', '-1');
    // Event types: 1-5 + 0 (all route event types)
    for (const t of ['1', '2', '3', '4', '5', '0']) {
      formData.append('evtypes_route[]', t);
    }
    formData.append('isdate', '0');
    formData.append('sdate', sDateFormatted);
    formData.append('edate', eDateFormatted);
    formData.append('erec', 'all');
    formData.append('estatus', 'any');
    formData.append('esort', '');
    formData.append('einvstatus', 'any');

    const copilotRes = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': tokenInfo.cookieHeader,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest'
      },
      body: formData.toString()
    });

    const debug = req.query.debug === '1' || req.body.debug === true;

    if (!copilotRes.ok) {
      const errBody = await copilotRes.text().catch(() => '');
      return res.status(502).json({ success: false, error: `CopilotCRM returned ${copilotRes.status}`, ...(debug && { responseBody: errBody.substring(0, 1000) }) });
    }

    const data = await copilotRes.json();

    if (debug) {
      console.log(`🔍 CopilotCRM sync debug: status=${data.status}, totalEventCount=${data.totalEventCount}, htmlLength=${(data.html || '').length}, employeesCount=${(data.employees || []).length}`);
    }

    if (data.status !== undefined && data.status !== 1 && data.status !== '1' && data.status !== true) {
      return res.status(502).json({ success: false, error: 'CopilotCRM returned non-success status', copilot_status: data.status, ...(debug && { rawKeys: Object.keys(data) }) });
    }

    // Parse HTML
    const jobs = parseCopilotRouteHtml(data.html || '', data.employees || []);

    // Check for parse mismatch
    const expectedCount = data.totalEventCount || 0;
    if (expectedCount > 0 && jobs.length === 0) {
      return res.status(500).json({ success: false, error: 'parse_mismatch', expected: expectedCount, got: 0 });
    }

    // Upsert each job
    let inserted = 0;
    let updated = 0;

    for (const job of jobs) {
      const result = await pool.query(
        `INSERT INTO copilot_sync_jobs (sync_date, event_id, customer_name, customer_id, crew_name, employees, address, status, visit_total, job_title, stop_order, raw_data, synced_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
         ON CONFLICT (sync_date, event_id) DO UPDATE SET
           customer_name = EXCLUDED.customer_name,
           customer_id = EXCLUDED.customer_id,
           crew_name = EXCLUDED.crew_name,
           employees = EXCLUDED.employees,
           address = EXCLUDED.address,
           status = EXCLUDED.status,
           visit_total = EXCLUDED.visit_total,
           job_title = EXCLUDED.job_title,
           stop_order = EXCLUDED.stop_order,
           raw_data = EXCLUDED.raw_data,
           synced_at = NOW()
         RETURNING (xmax = 0) AS is_insert`,
        [startDate, job.event_id, job.customer_name, job.customer_id, job.crew_name, job.employees, job.address, job.status, job.visit_total, job.job_title, job.stop_order, JSON.stringify(job)]
      );
      if (result.rows[0].is_insert) inserted++;
      else updated++;
    }

    const response = {
      success: true,
      startDate,
      endDate,
      total: jobs.length,
      inserted,
      updated,
      totalEventCount: expectedCount,
      overallVisitTotal: data.overallVisitTotal || null
    };
    if (tokenWarning) response.tokenWarning = tokenWarning;
    if (debug) {
      response.debug = {
        copilotStatus: data.status,
        htmlLength: (data.html || '').length,
        htmlPreview: (data.html || '').substring(0, 500),
        employeesCount: (data.employees || []).length,
        rawKeys: Object.keys(data),
        cookieHeaderLength: tokenInfo.cookieHeader.length,
        cookieHeaderPreview: tokenInfo.cookieHeader.substring(0, 80) + '...',
      };
    }

    res.json(response);
  } catch (error) {
    serverError(res, error, 'CopilotCRM sync failed');
  }
});

// GET/POST CopilotCRM settings — view and update auth cookies
app.get('/api/copilot/settings', authenticateToken, async (req, res) => {
  try {
    await ensureCopilotSyncTables();
    const tokenInfo = await getCopilotToken();
    res.json({
      success: true,
      hasCookies: !!tokenInfo,
      expiresAt: tokenInfo?.expiresAt || null,
      daysUntilExpiry: tokenInfo?.daysUntilExpiry ? Math.round(tokenInfo.daysUntilExpiry) : null,
    });
  } catch (error) {
    serverError(res, error, 'CopilotCRM settings fetch failed');
  }
});

app.post('/api/copilot/settings', authenticateToken, async (req, res) => {
  const { cookies } = req.body;
  if (!cookies || typeof cookies !== 'string') {
    return res.status(400).json({ success: false, error: 'cookies string is required' });
  }
  try {
    await ensureCopilotSyncTables();
    await pool.query(
      `INSERT INTO copilot_sync_settings (key, value, updated_at) VALUES ('copilot_cookies', $1, NOW()) ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()`,
      [cookies.trim()]
    );
    // Verify the token works
    const tokenInfo = await getCopilotToken();
    console.log(`✅ CopilotCRM cookies updated. Expires: ${tokenInfo?.expiresAt || 'unknown'}`);
    res.json({
      success: true,
      message: 'CopilotCRM cookies updated',
      expiresAt: tokenInfo?.expiresAt || null,
      daysUntilExpiry: tokenInfo?.daysUntilExpiry ? Math.round(tokenInfo.daysUntilExpiry) : null,
    });
  } catch (error) {
    serverError(res, error, 'CopilotCRM settings update failed');
  }
});

// ═══════════════════════════════════════════════════════════════
// COPILOT — automated cookie refresh via API login
// ═══════════════════════════════════════════════════════════════

app.post('/api/copilot/refresh-cookies', authenticateToken, async (req, res) => {
  const username = process.env.COPILOT_USERNAME || process.env.COPILOTCRM_USERNAME;
  const password = process.env.COPILOT_PASSWORD || process.env.COPILOTCRM_PASSWORD;
  if (!username || !password) {
    return res.status(500).json({ success: false, error: 'COPILOT_USERNAME and COPILOT_PASSWORD env vars are not set' });
  }

  try {
    console.log('🔄 CopilotCRM cookie refresh: logging in via API...');
    const cookieJar = new Map(); // name → value

    // Step 1: API login to get accessToken (same as contract signing)
    const loginRes = await fetch('https://api.copilotcrm.com/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Origin': 'https://secure.copilotcrm.com' },
      body: JSON.stringify({ username, password }),
    });

    const loginText = await loginRes.text();
    let loginData;
    try { loginData = JSON.parse(loginText); } catch (e) {
      throw new Error(`CopilotCRM login returned non-JSON (status ${loginRes.status}): ${loginText.substring(0, 200)}`);
    }

    if (!loginData.accessToken) {
      throw new Error(`CopilotCRM login failed (status ${loginRes.status}): ${loginText.substring(0, 200)}`);
    }

    cookieJar.set('copilotApiAccessToken', loginData.accessToken);

    // Capture any Set-Cookie headers from the API login
    const apiSetCookies = loginRes.headers.getSetCookie?.() || [];
    for (const sc of apiSetCookies) {
      const [pair] = sc.split(';');
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) cookieJar.set(pair.substring(0, eqIdx).trim(), pair.substring(eqIdx + 1).trim());
    }

    // Step 2: Hit secure.copilotcrm.com with the token to establish a full session
    // The scheduler endpoint may require session cookies that only come from the web app domain
    const sessionCookie = `copilotApiAccessToken=${loginData.accessToken}`;
    const sessionRes = await fetch('https://secure.copilotcrm.com/dashboard', {
      method: 'GET',
      headers: {
        'Cookie': sessionCookie,
        'Origin': 'https://secure.copilotcrm.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
      redirect: 'manual', // Don't follow redirects — we want the Set-Cookie headers
    });

    const webSetCookies = sessionRes.headers.getSetCookie?.() || [];
    for (const sc of webSetCookies) {
      const [pair] = sc.split(';');
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) cookieJar.set(pair.substring(0, eqIdx).trim(), pair.substring(eqIdx + 1).trim());
    }
    console.log(`🔑 CopilotCRM session: API login cookies=${apiSetCookies.length}, web session cookies=${webSetCookies.length}, total unique=${cookieJar.size}`);

    // Step 3: Also try the scheduler page to grab any scheduler-specific session cookies
    const fullCookieSoFar = [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
    const schedRes = await fetch('https://secure.copilotcrm.com/scheduler', {
      method: 'GET',
      headers: {
        'Cookie': fullCookieSoFar,
        'Origin': 'https://secure.copilotcrm.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
      redirect: 'manual',
    });

    const schedSetCookies = schedRes.headers.getSetCookie?.() || [];
    for (const sc of schedSetCookies) {
      const [pair] = sc.split(';');
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) cookieJar.set(pair.substring(0, eqIdx).trim(), pair.substring(eqIdx + 1).trim());
    }
    if (schedSetCookies.length > 0) console.log(`🔑 CopilotCRM scheduler page added ${schedSetCookies.length} more cookies`);

    // Build final cookie string
    const cookieString = [...cookieJar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');

    // Store in copilot_sync_settings
    await ensureCopilotSyncTables();
    await pool.query(
      `INSERT INTO copilot_sync_settings (key, value, updated_at) VALUES ('copilot_cookies', $1, NOW()) ON CONFLICT (key) DO UPDATE SET value = $1, updated_at = NOW()`,
      [cookieString]
    );

    // Quick verification: try the actual scheduler endpoint
    const testFormData = new URLSearchParams();
    testFormData.append('accessFrom', 'route');
    testFormData.append('bs4', '1');
    const today = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    testFormData.append('sDate', today);
    testFormData.append('eDate', today);
    testFormData.append('count', '-1');
    for (const t of ['1', '2', '3', '4', '5', '0']) testFormData.append('evtypes_route[]', t);
    testFormData.append('erec', 'all');
    testFormData.append('estatus', 'any');
    testFormData.append('einvstatus', 'any');

    const verifyRes = await fetch('https://secure.copilotcrm.com/scheduler/all/list', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': cookieString,
        'Origin': 'https://secure.copilotcrm.com',
        'Referer': 'https://secure.copilotcrm.com/',
        'X-Requested-With': 'XMLHttpRequest',
      },
      body: testFormData.toString(),
    });
    let verifyEventCount = null;
    if (verifyRes.ok) {
      try {
        const verifyData = await verifyRes.json();
        verifyEventCount = verifyData.totalEventCount || 0;
        console.log(`✅ CopilotCRM scheduler verification: ${verifyEventCount} events for today`);
      } catch { /* non-JSON response */ }
    } else {
      console.log(`⚠️ CopilotCRM scheduler verification returned ${verifyRes.status}`);
    }

    const tokenInfo = await getCopilotToken();
    console.log(`✅ CopilotCRM cookies refreshed. ${cookieJar.size} cookies stored. Expires: ${tokenInfo?.expiresAt || 'unknown'}`);

    res.json({
      success: true,
      message: 'CopilotCRM cookies refreshed via API login',
      cookieCount: cookieJar.size,
      verifyEventCount,
      expiresAt: tokenInfo?.expiresAt || null,
      daysUntilExpiry: tokenInfo?.daysUntilExpiry ? Math.round(tokenInfo.daysUntilExpiry) : null,
    });
  } catch (error) {
    console.error('❌ CopilotCRM cookie refresh failed:', error.message);
    serverError(res, error, 'CopilotCRM cookie refresh failed');
  }
});

// ═══════════════════════════════════════════════════════════════
// TELEGRAM — send a message to Telegram
// ═══════════════════════════════════════════════════════════════

app.post('/api/telegram/send', authenticateToken, async (req, res) => {
  try {
    const { message } = req.body;
    if (!message) return res.status(400).json({ success: false, error: 'message is required' });

    const tgSettings = await pool.query(
      "SELECT key, value FROM copilot_sync_settings WHERE key IN ('telegram_bot_token', 'telegram_chat_id')"
    );
    const tg = {};
    for (const row of tgSettings.rows) tg[row.key] = row.value;

    if (!tg.telegram_bot_token || !tg.telegram_chat_id) {
      return res.status(500).json({ success: false, error: 'Telegram bot token or chat ID not configured' });
    }

    const tgRes = await fetch(`https://api.telegram.org/bot${tg.telegram_bot_token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: tg.telegram_chat_id, text: message })
    });
    const tgData = await tgRes.json();

    if (!tgData.ok) {
      console.error('Telegram send failed:', tgData);
      return res.status(502).json({ success: false, error: tgData.description || 'Telegram error' });
    }

    res.json({ success: true, message_id: tgData.result.message_id });
  } catch (error) {
    serverError(res, error, 'Telegram send failed');
  }
});

// ═══════════════════════════════════════════════════════════════
// MORNING BRIEFING — assembles daily summary and sends to Telegram
// ═══════════════════════════════════════════════════════════════

async function assembleMorningBriefing() {
  const today = new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric', timeZone: 'America/New_York' });
  const todayDate = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' }); // YYYY-MM-DD
  const sections = {};
  const errors = [];
  const stats = { totalJobs: 0, crewCount: 0, invoiceCount: 0, invoiceTotal: 0, stripeFailures: 0, stripeConfigured: false };

  // ── Section 1: Today's Jobs by Crew ──
  try {
    const jobsResult = await pool.query(
      `SELECT crew_name, employees, COUNT(*) AS job_count
       FROM copilot_sync_jobs WHERE sync_date = $1 GROUP BY crew_name, employees ORDER BY crew_name`,
      [todayDate]
    );
    const crews = jobsResult.rows;
    if (crews.length === 0) {
      sections.jobs = `📋 TODAY'S JOBS (${today})\nNo jobs synced for today. Run the Copilot sync first or check the schedule.`;
    } else {
      const totalJobs = crews.reduce((sum, c) => sum + parseInt(c.job_count), 0);
      stats.totalJobs = totalJobs;
      stats.crewCount = crews.length;
      stats.crewNames = crews.map(c => (c.crew_name || 'Unassigned').replace(/ (Mowing |Landscaping )?Crew/i, ''));
      let jobText = `📋 TODAY'S JOBS (${today}) — ${totalJobs} total\n`;
      for (const c of crews) {
        const crew = c.crew_name || 'Unassigned';
        const count = parseInt(c.job_count);
        jobText += `${crew}${c.employees ? ' (' + c.employees + ')' : ''} — ${count} job${count === 1 ? '' : 's'}\n`;
      }
      sections.jobs = jobText.trim();
    }
  } catch (err) {
    console.error('Morning briefing — jobs error:', err);
    sections.jobs = `📋 TODAY'S JOBS\n⚠️ Error fetching jobs: ${err.message}`;
    errors.push('jobs');
  }

  // ── Section 2: Past Due Invoices from CopilotCRM ──
  try {
    const tokenInfo = await getCopilotToken();
    if (!tokenInfo) {
      sections.invoices = `💰 PAST DUE INVOICES\n⚠️ No CopilotCRM cookies configured. Cannot fetch invoices.`;
    } else {
      const copilotRes = await fetch('https://secure.copilotcrm.com/finances/invoices/getInvoicesListAjax', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Cookie': tokenInfo.cookieHeader,
          'Origin': 'https://secure.copilotcrm.com',
          'Referer': 'https://secure.copilotcrm.com/',
          'X-Requested-With': 'XMLHttpRequest'
        },
        body: 'postData[invoice_status][]=4&pagination[]=p=1'
      });

      if (!copilotRes.ok) {
        sections.invoices = `💰 PAST DUE INVOICES\n⚠️ CopilotCRM returned HTTP ${copilotRes.status}`;
      } else {
        const data = await copilotRes.json();
        const $ = cheerio.load(data.html || '');
        const invoices = [];

        $('tbody tr').each((i, row) => {
          const tds = $(row).find('td');
          if (tds.length < 14) return;
          const invoiceNum = tds.eq(1).text().trim();
          if (!invoiceNum) return; // skip summary/totals rows
          const date = tds.eq(2).text().trim();
          const customer = tds.eq(3).text().trim().split('\n')[0].trim();
          const property = tds.eq(5).text().trim();
          const invoiceTotal = tds.eq(9).text().trim();
          const totalDue = tds.eq(10).text().trim();
          const dueAmount = parseFloat(totalDue.replace(/[^0-9.-]/g, '')) || 0;
          if (dueAmount > 0) {
            invoices.push({ invoiceNum, date, customer, property, invoiceTotal, totalDue, dueAmount });
          }
        });

        if (invoices.length === 0) {
          sections.invoices = `💰 PAST DUE INVOICES\n✅ All clear — no past due invoices!`;
        } else {
          const totalDueSum = invoices.reduce((sum, inv) => sum + inv.dueAmount, 0);
          stats.invoiceCount = invoices.length;
          stats.invoiceTotal = totalDueSum;
          // Sort by amount descending for top invoices, then by date ascending for display
          stats.topInvoices = [...invoices].sort((a, b) => b.dueAmount - a.dueAmount).slice(0, 3);
          // Sort by date ascending (oldest first)
          invoices.sort((a, b) => new Date(a.date) - new Date(b.date));
          const top5 = invoices.slice(0, 5);

          let invText = `💰 PAST DUE INVOICES — ${invoices.length} invoices totaling $${totalDueSum.toFixed(2)}\n\nTop 5 oldest:\n`;
          for (const inv of top5) {
            const daysAgo = Math.floor((new Date() - new Date(inv.date)) / (1000 * 60 * 60 * 24));
            invText += `  • INV-${inv.invoiceNum} | ${inv.customer} | ${inv.totalDue} | ${daysAgo} days old\n`;
          }
          if (invoices.length > 5) {
            invText += `  ... and ${invoices.length - 5} more`;
          }
          sections.invoices = invText.trim();
        }
      }
    }
  } catch (err) {
    console.error('Morning briefing — invoices error:', err);
    sections.invoices = `💰 PAST DUE INVOICES\n⚠️ Error fetching invoices: ${err.message}`;
    errors.push('invoices');
  }

  // ── Section 3: Stripe Failed Payments ──
  try {
    if (process.env.STRIPE_SECRET_KEY) {
      stats.stripeConfigured = true;
      const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
      const oneDayAgo = Math.floor((Date.now() - 24 * 60 * 60 * 1000) / 1000);
      const charges = await stripe.charges.list({ created: { gte: oneDayAgo }, limit: 100 });
      const failed = charges.data.filter(c => c.status === 'failed');
      stats.stripeFailures = failed.length;
      if (failed.length === 0) {
        sections.stripe = `💳 STRIPE\n✅ All clear — no failed payments in the last 24 hours.`;
      } else {
        let stripeText = `💳 STRIPE — ${failed.length} failed payment(s) in the last 24 hours\n`;
        for (const c of failed.slice(0, 5)) {
          const amt = (c.amount / 100).toFixed(2);
          const email = c.billing_details?.email || c.receipt_email || 'unknown';
          stripeText += `  • $${amt} — ${email} — ${c.failure_message || 'no details'}\n`;
        }
        sections.stripe = stripeText.trim();
      }
    } else {
      sections.stripe = `💳 STRIPE\nNot configured yet (no STRIPE_SECRET_KEY).`;
    }
  } catch (err) {
    console.error('Morning briefing — stripe error:', err);
    sections.stripe = `💳 STRIPE\n⚠️ Error checking Stripe: ${err.message}`;
    errors.push('stripe');
  }

  // ── Section 4: Stripe Upcoming Deposits ──
  try {
    if (process.env.STRIPE_SECRET_KEY) {
      const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
      const threeDaysAgo = Math.floor(Date.now() / 1000) - 86400 * 3;

      // Fetch both balance and recent payouts
      const [balance, payouts] = await Promise.all([
        stripe.balance.retrieve(),
        stripe.payouts.list({ limit: 10, created: { gte: threeDaysAgo } }),
      ]);
      console.log('Stripe balance:', JSON.stringify(balance));
      console.log('Recent payouts:', JSON.stringify(payouts.data.map(p => ({ id: p.id, status: p.status, amount: p.amount, arrival: p.arrival_date, created: p.created }))));

      const pendingTotal = balance.pending.reduce((sum, b) => sum + b.amount, 0) / 100;

      if (payouts.data.length > 0) {
        // Show each payout with arrival date and amount
        const sorted = [...payouts.data].sort((a, b) => a.arrival_date - b.arrival_date);
        const total = sorted.reduce((sum, p) => sum + p.amount, 0) / 100;
        let depText = `💰 STRIPE DEPOSITS\n─────────────────────\n`;
        for (const p of sorted) {
          const amt = (p.amount / 100).toFixed(2);
          const arrival = new Date(p.arrival_date * 1000).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'America/New_York' });
          const statusTag = p.status === 'paid' ? ' ✓' : p.status === 'in_transit' ? ' →' : '';
          depText += `${arrival}: $${amt}${statusTag}\n`;
        }
        depText += `Total incoming: $${total.toFixed(2)}`;
        sections.deposit = depText;
        stats.depositAmount = total;
        stats.depositCount = sorted.length;
      } else if (pendingTotal > 0) {
        sections.deposit = `💰 STRIPE DEPOSITS\nPending balance: $${pendingTotal.toFixed(2)} (arriving within 1-2 business days)`;
        stats.depositAmount = pendingTotal;
        stats.depositCount = 0;
      } else {
        sections.deposit = `💰 STRIPE DEPOSITS\nNo upcoming deposits.`;
        stats.depositAmount = 0;
        stats.depositCount = 0;
      }
    } else {
      sections.deposit = `💰 STRIPE DEPOSITS\nStripe not configured.`;
    }
  } catch (err) {
    console.error('Morning briefing — deposit error:', err);
    sections.deposit = `💰 STRIPE DEPOSITS\n⚠️ Error fetching payouts: ${err.message}`;
    errors.push('deposit');
  }

  // ── Assemble briefing ──
  const briefing = `Good morning Theresa! Here's your daily briefing:\n\n${sections.jobs}\n\n${sections.deposit}\n\n${sections.invoices}\n\n${sections.stripe}`;
  return { briefing, sections, errors, stats };
}

app.post('/api/morning-briefing', authenticateToken, async (req, res) => {
  try {
    let { briefing, sections, errors, stats } = await assembleMorningBriefing();

    // Append Gmail summary if provided
    const gmailText = req.body.gmailText || null;
    if (gmailText) {
      briefing += '\n\n' + gmailText;
    }

    // Send to Telegram — split into multiple messages if too long
    let telegramSent = false;
    let telegramError = null;
    try {
      const tgSettings = await pool.query(
        "SELECT key, value FROM copilot_sync_settings WHERE key IN ('telegram_bot_token', 'telegram_chat_id')"
      );
      const tg = {};
      for (const row of tgSettings.rows) tg[row.key] = row.value;

      if (tg.telegram_bot_token && tg.telegram_chat_id) {
        const sendTg = async (text) => {
          const r = await fetch(`https://api.telegram.org/bot${tg.telegram_bot_token}/sendMessage`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: tg.telegram_chat_id, text, disable_web_page_preview: true })
          });
          return r.json();
        };

        // If briefing is long and we have gmailText, send as two messages
        const mainBriefing = `Good morning Theresa! Here's your daily briefing:\n\n${sections.jobs}\n\n${sections.deposit}\n\n${sections.invoices}\n\n${sections.stripe}`;
        if (gmailText && mainBriefing.length + gmailText.length > 4000) {
          const tgData1 = await sendTg(mainBriefing);
          const tgData2 = await sendTg(gmailText);
          if (tgData1.ok && tgData2.ok) {
            telegramSent = true;
          } else {
            telegramError = (!tgData1.ok ? tgData1.description : tgData2.description) || 'Unknown Telegram error';
            console.error('Telegram send failed:', !tgData1.ok ? tgData1 : tgData2);
          }
        } else {
          const tgData = await sendTg(briefing);
          if (tgData.ok) {
            telegramSent = true;
          } else {
            telegramError = tgData.description || 'Unknown Telegram error';
            console.error('Telegram send failed:', tgData);
          }
        }
      } else {
        telegramError = 'Telegram bot token or chat ID not configured in copilot_sync_settings';
      }
    } catch (err) {
      telegramError = err.message;
      console.error('Telegram send error:', err);
    }

    // Send full briefing via SMS (split into multiple messages if needed)
    let smsSent = false;
    let smsError = null;
    try {
      const twilioSid = process.env.TWILIO_ACCOUNT_SID;
      const twilioAuth = process.env.TWILIO_AUTH_TOKEN;
      const twilioFrom = process.env.TWILIO_PHONE_NUMBER;
      const phones = [process.env.THERESA_PHONE_NUMBER, process.env.TIM_PHONE_NUMBER].filter(Boolean);

      if (twilioSid && twilioAuth && twilioFrom && phones.length > 0) {
        const today = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'America/New_York' });

        // Build full SMS briefing
        const smsParts = [];
        let sms1 = `Good morning! Pappas & Co. daily briefing (${today}):\n\n`;
        sms1 += sections.jobs + '\n\n';
        sms1 += sections.deposit;
        smsParts.push(sms1);

        // Part 2: Invoices
        let sms2 = sections.invoices;
        smsParts.push(sms2);

        // Part 3: Stripe + email
        let sms3 = sections.stripe;
        const gmailSmsText = req.body.gmailText || '';
        if (gmailSmsText) {
          sms3 += '\n\n' + gmailSmsText;
        }
        smsParts.push(sms3);

        // Filter out empty parts and split any that exceed 1600 chars (Twilio limit)
        const smsMessages = [];
        for (const part of smsParts) {
          const trimmed = part.trim();
          if (!trimmed) continue;
          if (trimmed.length <= 1600) {
            smsMessages.push(trimmed);
          } else {
            // Split on newlines at ~1500 char boundaries
            let remaining = trimmed;
            while (remaining.length > 0) {
              if (remaining.length <= 1600) {
                smsMessages.push(remaining);
                break;
              }
              let splitIdx = remaining.lastIndexOf('\n', 1500);
              if (splitIdx < 500) splitIdx = 1500;
              smsMessages.push(remaining.substring(0, splitIdx).trim());
              remaining = remaining.substring(splitIdx).trim();
            }
          }
        }

        const twilioUrl = `https://api.twilio.com/2010-04-01/Accounts/${twilioSid}/Messages.json`;
        const twilioHeaders = {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': 'Basic ' + Buffer.from(`${twilioSid}:${twilioAuth}`).toString('base64')
        };

        // Send each message part sequentially to preserve order
        const allResults = [];
        for (const phone of phones) {
          for (const msg of smsMessages) {
            const body = new URLSearchParams({ To: phone, From: twilioFrom, Body: msg });
            try {
              const r = await fetch(twilioUrl, { method: 'POST', headers: twilioHeaders, body: body.toString() });
              allResults.push(await r.json());
            } catch (e) {
              allResults.push({ error_code: true, message: e.message });
            }
          }
        }

        const failures = allResults.filter(r => r.error_code);
        if (failures.length === 0) {
          smsSent = true;
          console.log(`✅ Morning briefing SMS: ${smsMessages.length} message(s) sent to ${phones.length} recipient(s)`);
        } else {
          smsError = failures.map(f => f.message || 'unknown').join('; ');
          console.error('SMS send failures:', failures);
        }
      } else {
        const missing = [];
        if (!twilioSid) missing.push('TWILIO_ACCOUNT_SID');
        if (!twilioAuth) missing.push('TWILIO_AUTH_TOKEN');
        if (!twilioFrom) missing.push('TWILIO_PHONE_NUMBER');
        if (phones.length === 0) missing.push('THERESA_PHONE_NUMBER and/or TIM_PHONE_NUMBER');
        smsError = `Missing env vars: ${missing.join(', ')}`;
        console.error('SMS config missing:', missing);
      }
    } catch (err) {
      smsError = err.message;
      console.error('SMS send error:', err);
    }

    res.json({
      success: true,
      briefing,
      sections,
      stats,
      errors,
      telegram: { sent: telegramSent, error: telegramError },
      sms: { sent: smsSent, error: smsError },
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    serverError(res, error, 'Morning briefing failed');
  }
});

// ═══════════════════════════════════════════════════════════════
// POST-SERVICE NOTIFICATION EMAIL (Zapier webhook from CopilotCRM)
// ═══════════════════════════════════════════════════════════════

const SERVICE_LOOKUP = {
  "Early Spring": {
    displayName: "Early Spring Fertilizer & Crabgrass Control",
    description: "Today we applied an early spring fertilizer with pre-emergent crabgrass control to your lawn. The pre-emergent is formulated to reduce infestation of annual grasses such as crabgrass, goosegrass, foxtail, and barnyard grass. It works by establishing a barrier at the soil surface that interrupts the development of these grasses. The fertilizer helps your lawn recover from winter stresses and promotes spring greening without excessive top growth.",
    tips: "Do not mow for 24 hours after today's application to allow the product to settle into the soil. When you do mow, keep your blade at the highest setting (3\u20133.5 inches) \u2014 taller grass helps crowd out weeds. Water your lawn within 2\u20133 days if rain is not in the forecast, as the pre-emergent needs moisture to activate. Avoid raking or dethatching for at least 4 weeks \u2014 disturbing the soil can break the crabgrass barrier. Per state law, please leave the posting flag in place for 24 hours."
  },
  "Late Spring": {
    displayName: "Late Spring Weed Control & Fertilizer",
    description: "Today we applied a weed control treatment and fertilizer to your lawn. The low-volume liquid weed control is formulated to target existing broadleaf weeds such as dandelions, plantain, chickweed, thistle, spurge, and clover. The fertilizer provides nutrients to improve your lawn's color, heartiness, and density.",
    tips: "Do not mow for 24\u201348 hours after this application. The weed control needs time to be absorbed through the weed leaves \u2014 mowing too soon removes the treated foliage before it can work. Do not water for 24 hours to allow the treatment to dry and absorb. You should see weeds curling and yellowing within 7\u201314 days. Please mow at the highest setting to keep your lawn looking its best, and avoid cutting off more than 1/3 of the grass blade. Per state law, please leave the posting flag in place for 24 hours."
  },
  "Early Summer": {
    displayName: "Early Summer Fertilizer, Insect & Weed Control",
    description: "Today we applied a slow-release granular fertilizer to your lawn along with insect control and weed treatment. The insect-control product helps prevent infestation of lawn-damaging surface insects such as chinch bugs, billbugs, and sod webworms, as well as subsurface insects such as white grubs. The fertilizer provides nutrients to improve your lawn's tolerance to summer heat and drought. Broadleaf weed control was applied as needed to help maintain a weed-free lawn.",
    tips: "Do not mow for 24 hours after today's service. Keep children and pets off the treated area for 24 hours. During summer, mow at the highest setting (3.5\u20134 inches) \u2014 taller grass shades the soil, keeps roots cooler, and retains moisture. Water your lawn deeply but infrequently \u2014 about 1 inch per week, ideally in the early morning. Avoid cutting more than 1/3 of the grass blade for best color. Per state law, please leave the posting flag in place for 24 hours."
  },
  "Late Summer": {
    displayName: "Late Summer Fertilizer & Weed Treatment",
    description: "Today we applied a slow-release granular fertilizer to your lawn and treated for weeds and surface insects as needed. The fertilizer will help your lawn recover from the stresses of summer and build new roots, tillers, and grass plants. Cooler temperatures and better moisture will accelerate plant growth and increase density through early fall. Broadleaf weed control was applied as needed to maintain a weed-free lawn.",
    tips: "Do not mow for 24 hours after today's application. Keep children and pets off the lawn for 24 hours. Please mow at the highest setting to keep the lawn looking its best \u2014 avoid cutting off more than 1/3 of the grass blade for best color. Water your lawn more often as summer heat continues. As temperatures cool, your lawn will start growing more vigorously. If you're considering aeration, now is the perfect time \u2014 contact us to schedule! Per state law, please leave the posting flag in place for 24 hours."
  },
  "Fall": {
    displayName: "Fall Winterizer Fertilizer",
    description: "Today we applied a fall winterizer fertilizer to your lawn. This fertilizer promotes healthy root growth and development, which takes place from late fall into early winter. It replenishes important nutrient reserves in the soil, providing extra energy for winter survival that is stored and used for an early spring green-up.",
    tips: "Continue mowing until the grass stops growing \u2014 typically into November. On your last mow of the season, lower your blade to about 2.5 inches to help prevent snow mold. Keep fallen leaves off your lawn by raking or mulch-mowing \u2014 a thick layer of leaves can smother the grass and invite disease. Your lawn will go dormant soon, but the fertilizer is working underground to build strong roots all winter for a great spring green-up."
  },
  "Grub Preventer": {
    displayName: "Merit Grub Preventer Application",
    description: "Today we applied a Merit grub preventer to your lawn. This product creates a protective zone in the soil that eliminates grub larvae before they can damage your lawn's root system. Grubs are the #1 cause of brown, dead patches in Northeast Ohio lawns in late summer and fall.",
    tips: "PLEASE water your lawn as soon as possible \u2014 irrigation or rainfall is needed to activate this product and guarantee results. Water deeply (about 0.5 inches). Keep children and pets off the lawn for 24 hours or until the product has been watered in and the lawn has dried. This treatment is preventive \u2014 you won't see immediate visible results, but it's working underground to protect your lawn. Per state law, please leave the posting flag in place for 24 hours."
  },
  "Aeration": {
    displayName: "Core Aeration",
    description: "Today we core-aerated your lawn. Aeration pulls small plugs of soil from the ground, relieving compaction and allowing air, water, and nutrients to reach the roots more effectively. This is one of the best things you can do for your lawn's long-term health \u2014 especially in Northeast Ohio's heavy clay soils.",
    tips: "Leave the soil plugs on the lawn \u2014 they break down naturally in 1\u20132 weeks and return nutrients to the soil. You can mow and use your lawn normally right away. Water your lawn within a day or two if rain is not in the forecast \u2014 moisture helps the soil settle and speeds plug breakdown. This is also a great time to fertilize, as nutrients can now reach deeper into the root zone."
  },
  "Lime Application": {
    displayName: "Granular Lime Application",
    description: "Today we applied granular lime to your lawn to correct soil acidity. Ohio soils tend to become acidic over time, making it harder for grass to absorb fertilizer and nutrients. Lime raises the soil pH back to a healthy range so your lawn gets the full benefit of each fertilizer application.",
    tips: "Water your lawn within 2\u20133 days if rain is not expected \u2014 moisture helps the lime break down into the soil. Lime is slow-acting (2\u20133 months to fully adjust pH), so results are gradual. You can mow and use your lawn normally right away. No special precautions needed."
  },
  "Weed Control": {
    displayName: "Professional Weed Control Treatment",
    description: "Today we applied a professional-grade herbicide to your landscape beds to eliminate actively growing weeds. This targeted application ensures effective weed control while minimizing disruption to surrounding plants. We focused on high-growth areas and problem spots to maximize effectiveness.",
    tips: "Avoid watering or disturbing the treated areas for at least 24 hours to allow the herbicide to absorb into the weeds. Treated weeds will begin to wither and die within 7\u201314 days \u2014 no manual removal is needed, as they will break down naturally over time. This service is performed monthly from April through September to maintain a weed-free landscape."
  }
};

async function getWeather(city, state) {
  const apiKey = process.env.OPENWEATHER_API_KEY;
  if (!apiKey) return null;
  try {
    const resp = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=${encodeURIComponent(city)},${state},US&appid=${apiKey}&units=imperial`);
    if (!resp.ok) return null;
    const data = await resp.json();
    return {
      temp: Math.round(data.main.temp),
      description: data.weather[0].description,
      icon: data.weather[0].icon
    };
  } catch (err) {
    console.error('Weather API error:', err.message);
    return null;
  }
}

function serviceCompleteEmailTemplate(data) {
  const baseUrl = process.env.BASE_URL || 'https://app.pappaslandscaping.com';
  const assetsUrl = process.env.EMAIL_ASSETS_URL || baseUrl;
  const SOCIAL_FB = `${assetsUrl}/email-assets/fb-white.png`;
  const SOCIAL_IG = `${assetsUrl}/email-assets/ig-white.png`;
  const SOCIAL_ND = `${assetsUrl}/email-assets/nd-white.png`;
  const reviewLink = process.env.GOOGLE_REVIEW_LINK || 'https://g.page/r/CXOm9gkatDbPEAE/review';

  const service = SERVICE_LOOKUP[data.serviceTitle] || {
    displayName: data.serviceTitle,
    description: `Today we completed ${data.serviceTitle} service on your lawn.`,
    tips: 'Water your lawn if rain is not in the forecast within the next few days.'
  };
  const serviceDisplayName = service.displayName || data.serviceTitle;

  // Replace "Today we" with "We" for past service dates
  let description = service.description;
  let tips = service.tips;
  if (!data.isRecent) {
    description = description.replace(/^Today we /i, 'We ').replace(/today's /gi, 'the ');
    tips = tips.replace(/today's /gi, 'the ').replace(/after today's /gi, 'after the ');
  }

  const weatherRow = data.weather ? `
    <tr>
      <td style="padding:8px 0;font-size:13px;color:#4a5568;border-bottom:1px solid #d4e4d0;">Weather</td>
      <td style="padding:8px 0;font-size:13px;color:#2e403d;font-weight:600;text-align:right;border-bottom:1px solid #d4e4d0;">
        <img src="https://openweathermap.org/img/wn/${data.weather.icon}.png" alt="" style="width:24px;height:24px;vertical-align:middle;margin-right:4px;">${data.weather.temp}\u00b0F, ${data.weather.description}
      </td>
    </tr>` : '';

  const tipsList = tips.split(/(?<=\.) /).map(tip =>
    `<li style="margin-bottom:8px;color:#4a5568;">${tip}</li>`
  ).join('');

  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:32px 16px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.06);">

  <!-- Header -->
  <tr><td style="background:#2e403d;padding:36px 48px;text-align:center;">
    <img src="${LOGO_URL}" alt="Pappas & Co. Landscaping" style="max-height:100px;max-width:400px;width:auto;">
  </td></tr>

  <!-- Heading -->
  <tr><td style="padding:40px 48px 8px;text-align:center;">
    <p style="margin:0;font-size:26px;font-weight:700;color:#2e403d;font-family:'DM Sans',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">Your Lawn Service is Complete \u2705</p>
  </td></tr>

  <!-- Body -->
  <tr><td style="padding:24px 48px 12px;">

    <!-- Greeting -->
    <p style="font-size:16px;color:#2e403d;font-weight:600;margin:0 0 8px;">Hi ${escapeHtml(data.customerFirstName || data.customerName)}!</p>
    <p style="font-size:15px;color:#4a5568;line-height:1.7;margin:0 0 24px;">${data.isRecent ? "We just finished servicing your lawn. Here's a summary of what we did today and some tips to help you get the most out of it." : "Here's a summary of your recent lawn service and some tips to help you get the most out of it."}</p>

    <!-- Service details card -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#e8f0e4;border-radius:10px;margin:0 0 28px;">
      <tr><td style="padding:20px 24px;">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td style="padding:8px 0;font-size:13px;color:#4a5568;border-bottom:1px solid #d4e4d0;">Service</td>
            <td style="padding:8px 0;font-size:13px;color:#2e403d;font-weight:700;text-align:right;border-bottom:1px solid #d4e4d0;">${escapeHtml(serviceDisplayName)}</td>
          </tr>
          <tr>
            <td style="padding:8px 0;font-size:13px;color:#4a5568;border-bottom:1px solid #d4e4d0;">Date</td>
            <td style="padding:8px 0;font-size:13px;color:#2e403d;font-weight:600;text-align:right;border-bottom:1px solid #d4e4d0;">${escapeHtml(data.serviceDate)}${data.serviceTime ? ' at ' + escapeHtml(data.serviceTime) : ''}</td>
          </tr>
          <tr>
            <td style="padding:8px 0;font-size:13px;color:#4a5568;border-bottom:1px solid #d4e4d0;">Technician</td>
            <td style="padding:8px 0;font-size:13px;color:#2e403d;font-weight:600;text-align:right;border-bottom:1px solid #d4e4d0;">${escapeHtml(data.technicianName)}</td>
          </tr>
          ${weatherRow}
          <tr>
            <td style="padding:8px 0;font-size:13px;color:#4a5568;">Location</td>
            <td style="padding:8px 0;font-size:13px;color:#2e403d;font-weight:600;text-align:right;">${escapeHtml(data.serviceAddress)}, ${escapeHtml(data.serviceCity)}</td>
          </tr>
        </table>
      </td></tr>
    </table>

    <!-- What We Applied -->
    <p style="font-size:15px;color:#2e403d;font-weight:700;margin:0 0 10px;">\u{1f33f} What We Applied</p>
    <p style="font-size:14px;color:#4a5568;line-height:1.7;margin:0 0 28px;">${description}</p>

    <!-- Lawn Care Tips -->
    <p style="font-size:15px;color:#2e403d;font-weight:700;margin:0 0 10px;">\u{1f4a1} Lawn Care Tips</p>
    <ul style="font-size:14px;line-height:1.7;margin:0 0 28px;padding-left:20px;">
      ${tipsList}
    </ul>

    <!-- Lime divider -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin:8px 0 24px;"><tr>
      <td style="width:30%;height:1px;background:transparent;"></td>
      <td style="width:40%;height:2px;background:#c9dd80;border-radius:1px;"></td>
      <td style="width:30%;height:1px;background:transparent;"></td>
    </tr></table>

    <!-- Google Review CTA -->
    <p style="font-size:14px;color:#4a5568;text-align:center;margin:0 0 16px;">Enjoying our service? A quick review means the world to us!</p>
    <div style="text-align:center;margin:0 0 20px;">
      <a href="${reviewLink}" style="background:#c9dd80;color:#2e403d;padding:14px 44px;text-decoration:none;border-radius:50px;font-weight:700;font-size:14px;display:inline-block;">Leave a Google Review \u2b50</a>
    </div>

    <!-- Contact CTA -->
    <p style="font-size:13px;color:#94a3b8;text-align:center;margin:0 0 8px;">Questions about today's service? Just reply to this email.</p>

  </td></tr>

  <!-- Signature -->
  <tr><td style="padding:0 48px 36px;">
    <img src="${SIGNATURE_IMAGE}" alt="Timothy Pappas" style="max-width:400px;width:100%;height:auto;">
  </td></tr>

  <!-- Footer -->
  <tr><td style="background:#2e403d;padding:28px 40px;text-align:center;">
    <p style="margin:0 0 6px;font-size:14px;color:#c9dd80;font-weight:600;letter-spacing:0.5px;">Quality Care for Every Season</p>
    <p style="margin:0 0 14px;font-size:13px;color:#a3b8a0;">Questions? Reply to this email or call <a href="tel:4408867318" style="color:#c9dd80;font-weight:600;text-decoration:none;">(440) 886-7318</a></p>
    <table cellpadding="0" cellspacing="0" style="margin:0 auto 16px;">
      <tr>
        <td style="padding:0 8px;"><a href="https://www.facebook.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_FB}" alt="Facebook" style="width:28px;height:28px;"></a></td>
        <td style="padding:0 8px;"><a href="https://www.instagram.com/pappaslandscaping" style="text-decoration:none;"><img src="${SOCIAL_IG}" alt="Instagram" style="width:28px;height:28px;"></a></td>
        <td style="padding:0 8px;"><a href="https://nextdoor.com/profile/01ZjZkwxhPWdnML2k" style="text-decoration:none;"><img src="${SOCIAL_ND}" alt="Nextdoor" style="width:28px;height:28px;"></a></td>
      </tr>
    </table>
    <p style="margin:0 0 3px;font-size:12px;color:#7a9477;">Pappas & Co. Landscaping</p>
    <p style="margin:0 0 3px;font-size:11px;color:#5a7a57;">PO Box 770057 &bull; Lakewood, Ohio 44107</p>
    <p style="margin:0 0 10px;font-size:11px;"><a href="https://pappaslandscaping.com" style="color:#c9dd80;text-decoration:none;">pappaslandscaping.com</a></p>
    <p style="margin:0;font-size:10px;color:#5a7a57;"><a href="${baseUrl}/unsubscribe.html?email=${encodeURIComponent(data.customerEmail || '')}" style="color:#7a9477;text-decoration:underline;">Unsubscribe</a> from marketing emails</p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>`;
}

app.post('/api/service-complete-email', async (req, res) => {
  try {
    const { customerName, customerFirstName, customerEmail, serviceTitle, serviceDate, serviceTime, technicianName, serviceAddress, serviceCity, serviceState } = req.body;

    if (!customerEmail) {
      console.log('⏭️ Service complete email skipped — no email for', customerName || 'unknown');
      return res.json({ success: true, skipped: true, reason: 'No customer email provided' });
    }
    if (!serviceTitle) {
      console.log('⏭️ Service complete email skipped — no service title for', customerName || 'unknown');
      return res.json({ success: true, skipped: true, reason: 'No service title provided' });
    }

    // Skip service complete emails for recurring mowing — only send for one-time services like fertilizing, aeration, etc.
    const skipServices = /^mowing/i;
    if (skipServices.test(serviceTitle.trim())) {
      console.log(`⏭️ Service complete email skipped — "${serviceTitle}" is a recurring mowing service for ${customerName || 'unknown'}`);
      return res.json({ success: true, skipped: true, reason: `Service complete email not sent for "${serviceTitle}"` });
    }

    // Match service title (fuzzy — try exact, then partial)
    let matchedTitle = Object.keys(SERVICE_LOOKUP).find(k => k.toLowerCase() === serviceTitle.toLowerCase());
    if (!matchedTitle) {
      matchedTitle = Object.keys(SERVICE_LOOKUP).find(k => serviceTitle.toLowerCase().includes(k.toLowerCase()));
    }

    // Parse technician name — could be "Event Closed By" string or crew name like "Rob Mowing Crew"
    let parsedTechName = technicianName;
    let parsedTime = serviceTime;
    if (technicianName && /closed by/i.test(technicianName)) {
      // "visit Event closed by - Robert Ellison at 10:33 am on Apr 03, 2026"
      const match = technicianName.match(/closed by\s*-?\s*([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+at\s+(\d{1,2}:\d{2}\s*[ap]m)/i);
      if (match) {
        parsedTechName = match[1].split(' ')[0];
        if (!serviceTime) parsedTime = match[2];
      }
    } else if (technicianName) {
      // "Rob Mowing Crew" → "Rob", or "Robert Ellison" → "Robert"
      parsedTechName = technicianName.split(' ')[0];
    }

    // Parse full address — "15520 Delaware Avenue Lakewood OH 44107"
    // Extract city from address if serviceCity not provided separately
    let parsedCity = serviceCity;
    let parsedAddress = serviceAddress || '';
    let parsedState = serviceState || 'OH';
    if (serviceAddress && !serviceCity) {
      // Try to match: street, city, state, zip
      const addrMatch = serviceAddress.match(/^(.+?)\s+(Lakewood|Bay Village|Brook Park|Westlake|Rocky River|Fairview Park|North Olmsted|Avon|Avon Lake|Westpark|Cleveland|Parma|North Royalton|Strongsville|Berea|Middleburg Heights|Olmsted Falls)\s+([A-Z]{2})\s+(\d{5})$/i);
      if (addrMatch) {
        parsedAddress = addrMatch[1];
        parsedCity = addrMatch[2];
        parsedState = addrMatch[3];
      } else {
        // Fallback: try splitting on common OH cities or just grab last parts
        const ohMatch = serviceAddress.match(/^(.+?)\s+(\w[\w\s]*?)\s+OH\s+\d{5}$/i);
        if (ohMatch) {
          parsedAddress = ohMatch[1];
          parsedCity = ohMatch[2];
          parsedState = 'OH';
        }
      }
    }

    // Check if service was today
    const today = new Date().toLocaleDateString('en-US', { timeZone: 'America/New_York', month: 'short', day: '2-digit', year: 'numeric' });
    const svcDateStr = serviceDate || '';
    const isToday = svcDateStr.includes(new Date().getFullYear().toString()) && new Date(svcDateStr).toDateString() === new Date().toDateString();

    // Fetch weather
    const weather = parsedCity ? await getWeather(parsedCity, parsedState) : null;

    const emailData = {
      customerName: customerName || 'Valued Customer',
      customerFirstName: customerFirstName || customerName?.split(' ')[0] || 'there',
      customerEmail,
      serviceTitle: matchedTitle || serviceTitle,
      serviceDate: serviceDate || new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' }),
      serviceTime: serviceTime || parsedTime || null,
      technicianName: parsedTechName || 'Our crew',
      serviceAddress: parsedAddress,
      serviceCity: parsedCity || '',
      weather,
      isRecent: isToday
    };

    const html = serviceCompleteEmailTemplate(emailData);
    const matchedService = SERVICE_LOOKUP[emailData.serviceTitle];
    const displayName = matchedService?.displayName || emailData.serviceTitle;
    const subject = `Your ${displayName} is complete! \u{1f33f}`;

    await sendEmail(customerEmail, subject, html, null, {
      type: 'service_complete',
      customer_name: customerName
    });

    console.log(`\u{1f4e7} Service complete email sent to ${customerEmail} for ${emailData.serviceTitle}`);

    res.json({
      success: true,
      email: customerEmail,
      service: emailData.serviceTitle,
      matched: !!matchedTitle,
      weather: weather ? `${weather.temp}\u00b0F, ${weather.description}` : 'unavailable'
    });
  } catch (error) {
    console.error('Service complete email error:', error);
    serverError(res, error, 'Failed to send service complete email');
  }
});

// ═══════════════════════════════════════════════════════════
// CENTRAL ERROR HANDLER — catches ApiError/ValidationError from routes
// ═══════════════════════════════════════════════════════════
app.use((err, req, res, _next) => {
  if (err instanceof ApiError) {
    return res.status(err.status).json(err.toJSON());
  }
  // Unexpected errors
  console.error(`❌ Unhandled error on ${req.method} ${req.path}:`, err);
  res.status(500).json({ success: false, error: 'Internal server error', code: 'SERVER_ERROR' });
});

// ═══════════════════════════════════════════════════════════════
// CATCH-ALL: Must be LAST route — serves static files or falls back to index.html
// ═══════════════════════════════════════════════════════════════
app.get('*', (req, res) => {
  const filePath = path.join(__dirname, 'public', req.path);
  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    return res.sendFile(filePath);
  }
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ═══════════════════════════════════════════════════════════
// STARTUP TABLE INITIALIZATION — delegated to lib/startup-schema.js
// ═══════════════════════════════════════════════════════════
runStartupTableInit(pool);

app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));

process.on('SIGTERM', async () => { await pool.end(); process.exit(0); });
