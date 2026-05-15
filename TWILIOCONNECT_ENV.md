# TwilioConnect Environment

Required backend variables:
- `DATABASE_URL`
- `JWT_SECRET`
- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`

Required for in-app Twilio Voice SDK calling:
- `TWILIO_API_KEY_SID`
- `TWILIO_API_KEY_SECRET`
- `TWILIO_TWIML_APP_SID`

Voice SDK notes:
- `TWILIO_API_KEY_SID` starts with `SK`.
- `TWILIO_TWIML_APP_SID` starts with `AP`.
- The TwiML App Voice Request URL should point to `https://app.pappaslandscaping.com/api/voice/twiml` with method `POST`.
- If the Voice SDK env vars are missing, `/api/app/voice/token` returns `503` with a `missing` list.
