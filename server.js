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
const { parseInvoiceListHtml } = require('./scripts/parse-copilot-invoices');
const { parseInvoiceDetailHtml } = require('./scripts/parse-copilot-invoice-detail');
const { syncInvoicesToDatabase, syncInvoiceDetailsToDatabase, mergeDetailIdentityFromListRow } = require('./scripts/import-copilot-invoices');
const {
  getCopilotToken: getCopilotTokenFromService,
  parseCopilotRouteHtml,
} = require('./services/copilot/client');
const {
  normalizeStoredInvoiceStatus,
  isOutstandingInvoice,
} = require('./lib/invoice-status');
const {
  LIVE_COPILOT_SOURCE,
  PERSISTED_COPILOT_SNAPSHOT_SOURCE,
  getCopilotRevenueWindow,
  extractCopilotRevenueReportTotal,
  normalizeCopilotRevenueSnapshot,
  getRevenueSnapshotExpiry,
  isRevenueSnapshotForWindow,
  buildRevenueMetric,
} = require('./lib/copilot-finance');
const {
  ADMIN_USERS_TABLE,
  hashPassword,
  ensureCopilotSyncTables,
  ensureQuoteEventsTable: _ensureQuoteEventsTable,
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

// Trust first proxy (Railway terminates SSL at proxy, forwards X-Forwarded-For)
// Required for express-rate-limit to read real client IPs instead of proxy IP
app.set('trust proxy', 1);

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

async function getCopilotToken() {
  return getCopilotTokenFromService(pool);
}

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
  if (!req.user || !req.user.isAdmin || req.user.isEmployee) {
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
      const token = jwt.sign({ id: emp.id, email: emp.login_email, name: empName, role: emp.title || 'employee', isAdmin: false, isEmployee: true, employeeId: emp.id, permissions: emp.permissions }, JWT_SECRET, { expiresIn: '7d' });
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
  if (!req.user) return res.status(401).json({ success: false, error: 'Not authorized' });
  res.json({ success: true, user: { email: req.user.email, name: req.user.name, role: req.user.role, isAdmin: !!req.user.isAdmin, isEmployee: !!req.user.isEmployee, permissions: req.user.permissions || null } });
});

// Generate long-lived service token for N8N / automation use
app.post('/api/auth/service-token', authenticateToken, (req, res) => {
  if (!req.user.isAdmin || req.user.isEmployee) return res.status(403).json({ success: false, error: 'Admin access required' });
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
    if (!req.user?.isAdmin || req.user.isEmployee) return res.status(403).json({ success: false, error: 'Admin access required' });
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
// ═══════════════════════════════════════════════════════════
// QUOTES & CONTRACTS — routes/quotes.js
// ═══════════════════════════════════════════════════════════
const quoteRoutes = require('./routes/quotes')({
  pool, sendEmail, escapeHtml, serverError, authenticateToken,
  verifyRecaptcha, RECAPTCHA_SECRET_KEY, NOTIFICATION_EMAIL,
  LOGO_URL, FROM_EMAIL, COMPANY_NAME, SERVICE_DESCRIPTIONS,
  getServiceDescription, nextCustomerNumber, anthropicClient,
  ensureQuoteEventsTable: () => _ensureQuoteEventsTable(pool), generateQuotePDF, emailTemplate,
});
app.use(quoteRoutes);

// ═══════════════════════════════════════════════════════════
// CUSTOMERS & PROPERTIES — routes/customers.js
// ═══════════════════════════════════════════════════════════
const customerRoutes = require('./routes/customers')({
  pool, serverError, authenticateToken, nextCustomerNumber, upload,
});
app.use(customerRoutes);

// ═══════════════════════════════════════════════════════════
// JOBS, CREWS & DISPATCH — routes/jobs.js
// ═══════════════════════════════════════════════════════════
const jobRoutes = require('./routes/jobs')({
  pool, serverError, authenticateToken, nextInvoiceNumber, upload,
});
app.use(jobRoutes);

// ═══════════════════════════════════════════════════════════
// INVOICES & PAYMENTS — routes/invoices.js
// ═══════════════════════════════════════════════════════════
const invoiceRoutes = require('./routes/invoices')({
  pool, sendEmail, emailTemplate, escapeHtml, serverError, authenticateToken,
  nextInvoiceNumber,
  squareClient, SQUARE_APP_ID, SQUARE_LOCATION_ID, SquareApiError,
  NOTIFICATION_EMAIL, LOGO_URL, FROM_EMAIL, COMPANY_NAME, getCopilotToken,
});
app.use(invoiceRoutes);

// ═══════════════════════════════════════════════════════════
// TEMPLATES — routes/templates.js
// ═══════════════════════════════════════════════════════════
const templateRoutes = require('./routes/templates')({
  pool, sendEmail, emailTemplate, serverError, getTemplate, replaceTemplateVars,
});
app.use(templateRoutes);

// ═══════════════════════════════════════════════════════════
// CAMPAIGNS — routes/campaigns.js
// ═══════════════════════════════════════════════════════════
const campaignRoutes = require('./routes/campaigns')({
  pool, sendEmail, emailTemplate, serverError, NOTIFICATION_EMAIL,
});
app.use(campaignRoutes);

// ═══════════════════════════════════════════════════════════
// COMMUNICATIONS — routes/communications.js
// ═══════════════════════════════════════════════════════════
const communicationRoutes = require('./routes/communications')({
  pool, sendEmail, emailTemplate, escapeHtml, serverError,
  twilioClient, TWILIO_PHONE_NUMBER, NOTIFICATION_EMAIL,
});
app.use(communicationRoutes);

// ═══════════════════════════════════════════════════════════
// COPILOTCRM SYNC — routes/copilot.js + services/copilot/client.js
// ═══════════════════════════════════════════════════════════
const copilotRoutes = require('./routes/copilot')({
  pool, serverError, authenticateToken,
});
app.use(copilotRoutes);

// ═══════════════════════════════════════════════════════════
// QUICKBOOKS — routes/quickbooks.js + services/quickbooks/client.js
// ═══════════════════════════════════════════════════════════
const quickbooksRoutes = require('./routes/quickbooks')({
  pool, serverError, nextCustomerNumber,
});
app.use(quickbooksRoutes);

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



// GET /api/services - Get list of predefined services
app.get('/api/services', (req, res) => {
  const services = Object.entries(SERVICE_DESCRIPTIONS).map(([name, description]) => ({
    name,
    description: description.replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '')
  }));
  res.json({ success: true, services });
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
// GENERAL ROUTES
// ═══════════════════════════════════════════════════════════
// ═══ INVOICING ════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════

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
    const tokenResult = await pool.query(
      'SELECT customer_id, email FROM customer_portal_tokens WHERE token = $1 AND expires_at > NOW()',
      [req.params.token]
    );
    if (tokenResult.rows.length === 0) return res.status(404).json({ success: false, error: 'Invalid token' });
    const { customer_id, email } = tokenResult.rows[0];

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
      `SELECT id, job_date, job_date AS scheduled_date, service_type, address, status, service_price
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

    const [paidMonth, paidLastMonth, paidYear, expMonth, expLastMonth, expYear, financeInvoices, byService, monthly] = await Promise.all([
      pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid' AND paid_at >= $1", [thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid' AND paid_at >= $1 AND paid_at < $2", [lastMonthStart, thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(total),0) as amt FROM invoices WHERE status='paid' AND paid_at >= $1", [thisYearStart]),
      pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses WHERE expense_date >= $1", [thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses WHERE expense_date >= $1 AND expense_date < $2", [lastMonthStart, thisMonthStart]),
      pool.query("SELECT COALESCE(SUM(amount),0) as amt FROM expenses WHERE expense_date >= $1", [thisYearStart]),
      pool.query("SELECT status, total, amount_paid, due_date FROM invoices"),
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
    let totalOutstanding = 0;
    let overdueCount = 0;
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    financeInvoices.rows.forEach((inv) => {
      const effectiveStatus = normalizeStoredInvoiceStatus(inv.status, inv.due_date, inv.total, inv.amount_paid);
      if (!isOutstandingInvoice(effectiveStatus, inv.total, inv.amount_paid)) return;
      const balance = Math.max(0, (parseFloat(inv.total) || 0) - (parseFloat(inv.amount_paid) || 0));
      totalOutstanding += balance;
      if (inv.due_date) {
        const due = new Date(inv.due_date);
        if (!Number.isNaN(due.getTime())) {
          due.setHours(0, 0, 0, 0);
          if (due < today) overdueCount += 1;
        }
      }
    });

    let copilotRevenueSnapshot = null;
    try {
      copilotRevenueSnapshot = await fetchCopilotCollectedRevenueSnapshot();
    } catch (error) {
      console.error('Error fetching Copilot collected revenue snapshot:', error);
    }

    const revenueSummary = buildRevenueMetric({
      copilotRevenueSnapshot,
      revenueMonth,
      now,
    });
    const revenueSourceLog = {
      source: revenueSummary.revenue_source,
      asOf: revenueSummary.revenue_as_of,
      periodStart: revenueSummary.revenue_period_start,
      periodEnd: revenueSummary.revenue_period_end,
    };
    if (revenueSummary.revenue_source === 'database_fallback') {
      console.warn('Finance summary revenue fallback', revenueSourceLog);
    } else {
      console.info('Finance summary revenue source', revenueSourceLog);
    }

    res.json({
      thisMonth: {
        revenue: revenueSummary.revenue,
        expenses: expensesMonth,
        revenue_source: revenueSummary.revenue_source,
        revenue_as_of: revenueSummary.revenue_as_of,
        revenue_period_start: revenueSummary.revenue_period_start,
        revenue_period_end: revenueSummary.revenue_period_end,
      },
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
        totalOutstanding: Number(totalOutstanding.toFixed(2)),
        overdueCount
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




// ═══════════════════════════════════════════════════════════
// ═══ BROADCAST ENDPOINTS ══════════════════════════════════
// ═══════════════════════════════════════════════════════════


// ═══════════════════════════════════════════════════════════

// ─── Pipeline / Workflow Stages ────────────────────────────────────────────
// NOTE: GET /api/jobs/pipeline is defined earlier (before /api/jobs/:id) to avoid route shadowing.


// ─── Job Detail / Profitability ────────────────────────────────────────────

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

// 8.2 Reports: Job Costing & Customer Value
app.get('/api/reports/job-costing', async (req, res) => {
  try {
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

// Service items, templates, automations tables created at startup via lib/startup-schema.js

app.get('/api/service-items', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM service_items ORDER BY name ASC');
    res.json({ success: true, items: result.rows });
  } catch (error) {
    serverError(res, error);
  }
});

app.post('/api/service-items', async (req, res) => {
  try {
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
    const result = await pool.query('DELETE FROM service_items WHERE id = $1 RETURNING *', [req.params.id]);
    res.json({ success: true, deleted: result.rows[0] });
  } catch (error) {
    serverError(res, error);
  }
});

// Bulk import service items
app.post('/api/service-items/import', async (req, res) => {
  try {
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


// ─── Automations / Sequences ────────────────────────────────────────

app.get('/api/automations', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM automations ORDER BY created_at DESC');
    res.json({ success: true, automations: result.rows });
  } catch (error) { serverError(res, error); }
});

app.get('/api/automations/:id', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM automations WHERE id = $1', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ success: false, error: 'Not found' });
    res.json({ success: true, automation: result.rows[0] });
  } catch (error) { serverError(res, error); }
});

app.post('/api/automations', async (req, res) => {
  try {
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
    await pool.query('DELETE FROM automations WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (error) { serverError(res, error); }
});

app.get('/api/automations/:id/history', async (req, res) => {
  try {
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
        `SELECT id, service_type, service_price, status, job_date, job_date AS scheduled_date, created_at
         FROM scheduled_jobs
         WHERE customer_name = $1
         ORDER BY COALESCE(job_date, created_at) DESC LIMIT 15`,
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
      if (status === 'open') {
        query += ` AND sr.status NOT IN ('completed', 'cancelled')`;
        countQuery += ` AND status NOT IN ('completed', 'cancelled')`;
      } else {
        query += ` AND sr.status = $${p++}`;
        countQuery += ` AND status = $${cp++}`;
        params.push(status);
        countParams.push(status);
      }
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

function buildCopilotInvoiceRequestBody({ page = 1, pageSize = 100, sort = 'datedesc', invoiceStatuses = [] } = {}) {
  const formData = new URLSearchParams();
  formData.append('pagination[]', `p=${page}`);
  formData.append('pagination[]', `iop=${pageSize}`);
  formData.append('pagination[]', `sort=${sort}`);

  for (const status of invoiceStatuses) {
    formData.append('postData[invoice_status][]', String(status));
  }

  return formData.toString();
}

function extractCopilotInvoiceTotalCount(html) {
  const match = String(html || '').match(/(\d+)\s*-\s*(\d+)\s+of\s+(\d+)/i);
  if (!match) return null;
  const total = parseInt(match[3], 10);
  return Number.isFinite(total) ? total : null;
}

const COPILOT_REVENUE_SNAPSHOT_KEY = 'copilot_revenue_this_month_collected';
const COPILOT_REVENUE_CACHE_TTL_MS = 5 * 60 * 1000;
let cachedCopilotRevenue = null;
let cachedCopilotRevenuePromise = null;

async function readPersistedCopilotRevenueSnapshot(window) {
  try {
    const result = await pool.query(
      `SELECT value
         FROM copilot_sync_settings
        WHERE key = $1`,
      [COPILOT_REVENUE_SNAPSHOT_KEY]
    );
    if (!result.rows[0]?.value) return null;
    const parsed = normalizeCopilotRevenueSnapshot(
      JSON.parse(result.rows[0].value),
      PERSISTED_COPILOT_SNAPSHOT_SOURCE
    );
    if (!parsed || !isRevenueSnapshotForWindow(parsed, window)) return null;
    return parsed;
  } catch (error) {
    return null;
  }
}

async function persistCopilotRevenueSnapshot(snapshot) {
  const normalized = normalizeCopilotRevenueSnapshot(snapshot, LIVE_COPILOT_SOURCE);
  if (!normalized) return;
  await pool.query(
    `INSERT INTO copilot_sync_settings (key, value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE
       SET value = EXCLUDED.value,
           updated_at = NOW()`,
    [COPILOT_REVENUE_SNAPSHOT_KEY, JSON.stringify(normalized)]
  );
}

async function fetchCopilotCollectedRevenueSnapshot() {
  const window = getCopilotRevenueWindow();
  const nowMs = Date.now();
  const sameWindowCachedRevenue = isRevenueSnapshotForWindow(cachedCopilotRevenue?.value, window)
    ? cachedCopilotRevenue?.value
    : null;

  if (!cachedCopilotRevenue) {
    const persisted = await readPersistedCopilotRevenueSnapshot(window);
    if (persisted) {
      cachedCopilotRevenue = {
        value: persisted,
        expiresAt: getRevenueSnapshotExpiry(persisted, window),
      };
    }
  }

  if (cachedCopilotRevenue?.expiresAt > nowMs) {
    return cachedCopilotRevenue.value;
  }
  if (cachedCopilotRevenuePromise) {
    return sameWindowCachedRevenue || cachedCopilotRevenuePromise;
  }

  const refreshPromise = (async () => {
    const tokenInfo = await getCopilotToken();
    if (!tokenInfo?.cookieHeader) return sameWindowCachedRevenue || null;

    const reportUrl = `https://secure.copilotcrm.com/reports/revenue-by-crew/?sdate=${window.start}&edate=${window.end}&crew_id=0&type=collected`;
    const response = await fetch(reportUrl, {
      headers: {
        'Cookie': tokenInfo.cookieHeader,
        'Referer': 'https://secure.copilotcrm.com/reports/revenue-by-crew',
      },
    });

    if (!response.ok) {
      return sameWindowCachedRevenue || null;
    }

    const html = await response.text();
    const total = extractCopilotRevenueReportTotal(html);
    if (!Number.isFinite(total)) {
      console.warn('Unable to parse Copilot revenue by crew total', {
        periodStart: window.start,
        periodEnd: window.end,
      });
      return sameWindowCachedRevenue || null;
    }

    const value = normalizeCopilotRevenueSnapshot({
      source: LIVE_COPILOT_SOURCE,
      as_of: new Date().toISOString(),
      period_start: window.start,
      period_end: window.end,
      total,
    }, LIVE_COPILOT_SOURCE);
    await persistCopilotRevenueSnapshot(value).catch((error) => {
      console.error('Error persisting Copilot revenue snapshot:', error);
    });
    cachedCopilotRevenue = {
      value,
      expiresAt: Date.now() + COPILOT_REVENUE_CACHE_TTL_MS,
    };
    return value;
  })();

  cachedCopilotRevenuePromise = refreshPromise.finally(() => {
    cachedCopilotRevenuePromise = null;
  });

  if (cachedCopilotRevenue) {
    return cachedCopilotRevenue.value;
  }
  return cachedCopilotRevenuePromise;
}

async function fetchCopilotInvoicePage({ cookieHeader, page = 1, pageSize = 100, sort = 'datedesc', invoiceStatuses = [] } = {}) {
  const copilotRes = await fetch('https://secure.copilotcrm.com/finances/invoices/getInvoicesListAjax', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Cookie': cookieHeader,
      'Origin': 'https://secure.copilotcrm.com',
      'Referer': 'https://secure.copilotcrm.com/',
      'X-Requested-With': 'XMLHttpRequest'
    },
    body: buildCopilotInvoiceRequestBody({ page, pageSize, sort, invoiceStatuses }),
  });

  if (!copilotRes.ok) {
    const errBody = await copilotRes.text().catch(() => '');
    throw new Error(`Copilot invoice page ${page} failed (${copilotRes.status}): ${errBody.substring(0, 200)}`);
  }

  const data = await copilotRes.json();
  return {
    data,
    invoices: parseInvoiceListHtml(data.html || ''),
    totalCount: extractCopilotInvoiceTotalCount(data.html || ''),
  };
}

async function fetchCopilotInvoiceDetail({ cookieHeader, viewPath, externalInvoiceId } = {}) {
  const detailPath = viewPath || (externalInvoiceId ? `/finances/invoices/view/${externalInvoiceId}` : null);
  if (!detailPath) throw new Error('Missing Copilot invoice detail path');
  const detailUrl = detailPath.startsWith('http') ? detailPath : `https://secure.copilotcrm.com${detailPath}`;
  const detailRes = await fetch(detailUrl, {
    headers: {
      'Cookie': cookieHeader,
      'Referer': 'https://secure.copilotcrm.com/finances/invoices',
    },
  });

  if (!detailRes.ok) {
    const errBody = await detailRes.text().catch(() => '');
    throw new Error(`Copilot invoice detail ${externalInvoiceId || detailPath} failed (${detailRes.status}): ${errBody.substring(0, 200)}`);
  }

  const html = await detailRes.text();
  const detail = parseInvoiceDetailHtml(html);
  if ((detail.parse_diagnostics?.description_row_count || 0) > 0 && (!detail.line_items || detail.line_items.length === 0)) {
    console.warn('Copilot detail parsed without line items', {
      externalInvoiceId: externalInvoiceId || null,
      detailPath,
      diagnostics: detail.parse_diagnostics,
    });
  }
  return detail;
}

async function syncCopilotInvoices({
  pageSize = 100,
  maxPages = 150,
  linkCustomers = true,
  invoiceStatuses = [],
  sort = 'datedesc',
  detailMode = 'missing',
  detailLimit = 0,
} = {}) {
  await ensureCopilotSyncTables(pool);
  const tokenInfo = await getCopilotToken();

  if (!tokenInfo || !tokenInfo.cookieHeader) {
    throw new Error('No CopilotCRM cookies configured.');
  }

  const allInvoices = [];
  const seenFirstIds = new Set();
  const pages = [];
  let totalCount = null;

  for (let page = 1; page <= maxPages; page += 1) {
    const pageResult = await fetchCopilotInvoicePage({
      cookieHeader: tokenInfo.cookieHeader,
      page,
      pageSize,
      sort,
      invoiceStatuses,
    });

    const invoices = pageResult.invoices;
    if (!invoices.length) break;

    const firstId = invoices[0].external_invoice_id;
    if (firstId && seenFirstIds.has(firstId)) break;
    if (firstId) seenFirstIds.add(firstId);

    pages.push({
      page,
      count: invoices.length,
      firstId: firstId || null,
    });
    allInvoices.push(...invoices);

    if (pageResult.totalCount) totalCount = pageResult.totalCount;
    if (invoices.length < pageSize) break;
    if (totalCount && page * pageSize >= totalCount) break;
  }

  const syncResult = await syncInvoicesToDatabase(pool, allInvoices, { linkCustomers });
  let detailResult = { total: 0, inserted: 0, updated: 0, errors: 0 };

  if (detailMode !== 'off' && allInvoices.length > 0) {
    const rowByExternalId = new Map(allInvoices.map(row => [String(row.external_invoice_id || ''), row]));
    let candidates = allInvoices;

    if (detailMode === 'missing') {
      const externalIds = allInvoices
        .map(row => row.external_invoice_id)
        .filter(Boolean)
        .map(id => String(id));

      if (externalIds.length) {
        const detailParams = [externalIds];
        let detailQuery = `SELECT external_invoice_id
             FROM invoices
            WHERE external_source = 'copilotcrm'
              AND external_invoice_id = ANY($1::text[])
              AND (
                COALESCE(external_metadata->>'detail_synced_at', '') = ''
                OR
                jsonb_array_length(COALESCE(line_items, '[]'::jsonb)) = 0
                OR customer_name LIKE '%@%'
                OR COALESCE(external_metadata->>'detail_parse_warning', '') <> ''
              )
            ORDER BY created_at DESC NULLS LAST`;
        if (Number(detailLimit) > 0) {
          detailParams.push(Number(detailLimit));
          detailQuery += ` LIMIT $2`;
        }
        const missingDetail = await pool.query(detailQuery, detailParams);
        candidates = missingDetail.rows
          .map(row => rowByExternalId.get(String(row.external_invoice_id || '')))
          .filter(Boolean);
      }
    } else if (detailLimit > 0) {
      candidates = allInvoices.slice(0, Number(detailLimit) || 40);
    }

    const details = [];
    for (const row of candidates) {
      try {
        const detail = await fetchCopilotInvoiceDetail({
          cookieHeader: tokenInfo.cookieHeader,
          viewPath: row.view_path,
          externalInvoiceId: row.external_invoice_id,
        });
        const normalizedDetail = mergeDetailIdentityFromListRow(detail, row);
        if (!normalizedDetail.sent_status && row.sent_status) normalizedDetail.sent_status = row.sent_status;
        if (!normalizedDetail.raw_status && row.raw_status) normalizedDetail.raw_status = row.raw_status;
        if (!normalizedDetail.status && row.status) normalizedDetail.status = row.status;
        details.push(normalizedDetail);
      } catch (error) {
        detailResult.errors += 1;
        console.error('Copilot detail enrichment error:', error.message);
      }
    }

    if (details.length > 0) {
      const applied = await syncInvoiceDetailsToDatabase(pool, details, { linkCustomers });
      detailResult = {
        ...detailResult,
        total: applied.total,
        inserted: applied.inserted,
        updated: applied.updated,
      };
    }
  }

  await pool.query(
    `INSERT INTO copilot_sync_settings (key, value, updated_at)
     VALUES
       ('copilot_invoice_last_sync_at', $1, NOW()),
       ('copilot_invoice_last_sync_summary', $2, NOW())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
    [
      new Date().toISOString(),
      JSON.stringify({
        totalCount,
        pages,
        inserted: syncResult.inserted,
        updated: syncResult.updated,
        total: syncResult.total,
        detailMode,
        detail: detailResult,
        refreshed: detailResult.total > 0,
      }),
    ]
  );

  return {
    success: true,
    totalCount,
    pages,
    refreshed: detailResult.total > 0,
    detail: detailResult,
    ...syncResult,
  };
}

function assertCronSecret(req, res) {
  const configuredSecret = process.env.CRON_SECRET || process.env.CRON_API_KEY || '';
  if (!configuredSecret) return true;

  const provided = req.get('x-cron-secret') || req.query.key || req.query.token || req.body?.key || req.body?.token || '';
  if (provided === configuredSecret) return true;

  res.status(401).json({ success: false, error: 'Invalid cron secret' });
  return false;
}

function readBoundedInt(rawValue, fallback, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

async function runCopilotInvoiceRepair(req, res) {
  if (!assertCronSecret(req, res)) return;
  try {
    const detailLimit = readBoundedInt(req.body?.detailLimit ?? req.query.detailLimit, 40, { min: 1, max: 500 });
    const maxPages = readBoundedInt(req.body?.maxPages ?? req.query.maxPages, 25, { min: 1, max: 150 });
    const pageSize = readBoundedInt(req.body?.pageSize ?? req.query.pageSize, 100, { min: 1, max: 250 });

    const result = await syncCopilotInvoices({
      pageSize,
      maxPages,
      linkCustomers: req.body?.linkCustomers !== false && req.query.linkCustomers !== 'false',
      detailMode: 'missing',
      detailLimit,
    });

    res.json({
      success: true,
      trigger: 'cron-repair',
      detail: {
        total: result.detail?.total || 0,
        updated: result.detail?.updated || 0,
        errors: result.detail?.errors || 0,
      },
      pageSize,
      maxPages,
      detailLimit,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    serverError(res, error, 'CopilotCRM cron invoice repair failed');
  }
}

app.post('/api/copilot/invoices/sync', authenticateToken, async (req, res) => {
  try {
    const result = await syncCopilotInvoices({
      pageSize: Number(req.body.pageSize || 100),
      maxPages: Number(req.body.maxPages || 150),
      linkCustomers: req.body.linkCustomers !== false,
      detailMode: req.body.detailMode || 'missing',
      detailLimit: Number(req.body.detailLimit || 0),
    });
    res.json(result);
  } catch (error) {
    serverError(res, error, 'CopilotCRM invoice sync failed');
  }
});

app.get('/api/copilot/invoices/status', authenticateToken, async (req, res) => {
  try {
    await ensureCopilotSyncTables(pool);
    const result = await pool.query(
      "SELECT key, value, updated_at FROM copilot_sync_settings WHERE key IN ('copilot_invoice_last_sync_at', 'copilot_invoice_last_sync_summary', 'copilot_cookies', 'copilot_token') ORDER BY key"
    );
    const settings = {};
    for (const row of result.rows) settings[row.key] = row.value;
    const tokenInfo = await getCopilotToken();
    res.json({
      success: true,
      lastSyncAt: settings.copilot_invoice_last_sync_at || null,
      lastSyncSummary: settings.copilot_invoice_last_sync_summary ? JSON.parse(settings.copilot_invoice_last_sync_summary) : null,
      hasCopilotAuth: !!(settings.copilot_cookies || settings.copilot_token),
      expiresAt: tokenInfo?.expiresAt || null,
      daysUntilExpiry: tokenInfo?.daysUntilExpiry ? Math.round(tokenInfo.daysUntilExpiry) : null,
    });
  } catch (error) {
    serverError(res, error, 'CopilotCRM invoice sync status failed');
  }
});

app.post('/api/cron/copilot-invoices-sync', async (req, res) => {
  if (!assertCronSecret(req, res)) return;
  try {
    const result = await syncCopilotInvoices({
      pageSize: Number(req.body?.pageSize || req.query.pageSize || 100),
      maxPages: Number(req.body?.maxPages || req.query.maxPages || 150),
      linkCustomers: req.body?.linkCustomers !== false && req.query.linkCustomers !== 'false',
      detailMode: req.body?.detailMode || req.query.detailMode || 'missing',
      detailLimit: Number(req.body?.detailLimit || req.query.detailLimit || 40),
    });
    res.json({ success: true, trigger: 'cron', ...result, timestamp: new Date().toISOString() });
  } catch (error) {
    serverError(res, error, 'CopilotCRM cron invoice sync failed');
  }
});

app.get('/api/cron/copilot-invoices-sync', async (req, res) => {
  if (!assertCronSecret(req, res)) return;
  try {
    const result = await syncCopilotInvoices({
      pageSize: Number(req.query.pageSize || 100),
      maxPages: Number(req.query.maxPages || 150),
      linkCustomers: req.query.linkCustomers !== 'false',
      minDaysRemaining: Number(req.query.minDaysRemaining || 1),
      detailMode: req.query.detailMode || 'missing',
      detailLimit: Number(req.query.detailLimit || 40),
    });
    res.json({ success: true, trigger: 'cron', ...result, timestamp: new Date().toISOString() });
  } catch (error) {
    serverError(res, error, 'CopilotCRM cron invoice sync failed');
  }
});

app.post('/api/cron/copilot-invoices-repair', async (req, res) => {
  await runCopilotInvoiceRepair(req, res);
});

app.get('/api/cron/copilot-invoices-repair', async (req, res) => {
  await runCopilotInvoiceRepair(req, res);
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
