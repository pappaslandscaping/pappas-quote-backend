# Changelog

## 2026-04-14

### Route Sheet — Name & Address Parsing Fixes
- Addresses no longer get truncated when a city name appears inside the street (e.g., "14950 Lakewood Heights Boulevard", "1283 Westlake Avenue" — previously collapsed to just the house number)
- Personal names now use the surname only. "Mary Ann Marsal" → "Marsal", "Dallas Marie Holifield" → "Holifield" (previously included middle names)
- "Ready" removed from business-keyword list so "Martha Ready" → "Ready" instead of showing the full name
- Names with lowercase internal words (e.g., "Mews at Rockport") now render in full instead of being stripped to "at Rockport"
- Added "COA" to business-keyword list so "Kirtland House COA" renders in full
- Non-mowing service types (e.g., "Weed Whacking - Pavement Area") now appear in the Comments column instead of being dropped

## 2026-03-27

### Morning Briefing Endpoint
- New `POST /api/morning-briefing` endpoint assembles a daily summary and sends it to Telegram
- Sections: today's jobs by crew (from copilot_sync_jobs), past due invoices (live from CopilotCRM), Stripe failed payments (when configured)
- Briefing sent to Telegram via bot API; also returned in JSON response for N8N logging
- Protected with auth middleware — N8N uses existing service token

## 2026-03-20

### Contract Signing → Copilot Portal
- Replaced Square card-on-file step with redirect to Copilot client portal after contract signing
- Auto-sends branded portal invite email via Copilot's sendMail API as part of CopilotCRM sync
- Email emphasizes adding card on file with emoji-styled feature list (💳 Card on File, 📄 Quotes & Invoices, 📅 Service Schedule, 💬 Direct Messaging)
- Links directly to forgot-password page (`secure.copilotcrm.com/client/forget?co=5261`) so new customers skip the login step
- Removed Square Web Payments SDK from sign-contract.html

### Send Estimates by Text
- New `POST /api/sent-quotes/:id/send-sms` endpoint sends quote link via Twilio SMS
- SMS uses Tim's personal tone matching the Copilot "New Estimate" template
- "Send by Text" button added to sent-quote-detail.html and sent-quotes.html list view
- Timeline shows "💬 Sent by Text" label with phone number

### Quote Edit Modal — Editable Descriptions
- Each service row in the Edit Quote modal now has a description textarea
- Descriptions are fully editable before resending (previously always pulled from predefined list)
- Selecting a predefined service pre-fills the description; custom services start blank

### Topbar Buttons Fix
- Fixed invisible action buttons on sent-quote-detail.html (Edit, Resend, Send by Text, Download PDF, Delete)
- Root cause: `topbar-right` div was missing `display: flex` CSS

### Social Media AI
- Built `POST /api/social-media/generate` — Claude creates tailored posts for Facebook, Instagram, Nextdoor, TikTok, Google, and Twitter/X
- Each platform gets appropriate style (hashtags for IG, 280 chars for X, neighborly for Nextdoor)
- Built `POST /api/social-media/refine` — ongoing conversation to refine posts ("make it shorter", "add hashtags", "which is best?")
- Built `GET /api/social-media/history` — returns recent generated posts
- New `social_media_posts` table stores generation history
- Chat is now fully conversational — follow-ups refine in-place, say "new post" to start fresh
