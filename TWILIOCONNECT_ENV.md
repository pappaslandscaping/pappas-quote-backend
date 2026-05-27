# TwilioConnect Environment

Required backend variables:
- `DATABASE_URL`
- `JWT_SECRET`
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`

Required for replying to text notification emails:
- `RESEND_API_KEY`
- `SMS_REPLY_DOMAIN` or `RESEND_INBOUND_DOMAIN` — the Resend receiving domain used for reply addresses

Reply-by-email setup:
- Configure a Resend `email.received` webhook to `https://app.pappaslandscaping.com/api/email/sms-reply`.
- Incoming SMS notification emails use a signed `Reply-To` address on the reply domain. Replies to those emails are sent back to the original phone number through Twilio.
- Optional: set `SMS_REPLY_SECRET` to rotate the reply-address signing secret. If omitted, `JWT_SECRET` is used.
- Optional: set `SMS_REPLY_ALLOWED_SENDERS` to a comma-separated list of email addresses allowed to send SMS replies. Defaults to `hello@pappaslandscaping.com` and `NOTIFICATION_EMAIL`.

Required for in-app Twilio Voice SDK calling:
- `TWILIO_API_KEY_SID`
- `TWILIO_API_KEY_SECRET`
- `TWILIO_TWIML_APP_SID`

Voice SDK notes:
- `TWILIO_API_KEY_SID` starts with `SK`.
- `TWILIO_TWIML_APP_SID` starts with `AP`.
- The TwiML App Voice Request URL should point to `https://app.pappaslandscaping.com/api/voice/twiml` with method `POST`.
- If the Voice SDK env vars are missing, `/api/app/voice/token` returns `503` with a `missing` list.
