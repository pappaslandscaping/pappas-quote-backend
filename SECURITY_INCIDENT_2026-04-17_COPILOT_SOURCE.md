# Copilot Source Exposure Cleanup

Date: `2026-04-17`

This note records the cleanup work after raw Copilot page source was copied during production debugging.

## What Was Exposed

The copied Copilot page source contained these categories of values:

### Must rotate now

- `CRON_SECRET`
  - Not from the Copilot HTML itself, but exposed during the same debugging session through a live cron URL.
  - This secret protects `/api/cron/copilot-invoices-sync` and `/api/cron/copilot-invoices-repair`.

### Should rotate / invalidate in third-party systems

- Intercom user JWT
  - Session-bearing token embedded in the page.
- Intercom `login_url`
  - Treat as a magic-login or delegated-login URL until proven otherwise.
- Copilot `client_portal_token`
  - Embedded client token rendered into page JavaScript.
- ProfitWell auth token
  - Embedded client-side auth value.

### Low-risk or public client-side identifiers

- Mixpanel project token
- LaunchDarkly client-side ID
- Pusher key
- Sentry browser DSN
- `location-user-token`
  - Encodes company/user identifiers and does not appear to be a standalone secret.

## Actions Taken

- Rotated the production `CRON_SECRET` in Railway.
- Verified the old exposed cron key is now rejected.
- Verified the new cron key is accepted by the production repair endpoint.
- Removed local copied report source artifacts from `/tmp`.
- Closed Chrome `view-source:` tabs used during debugging.
- Cleared the local clipboard.

## Manual Follow-Up Required

These values are controlled by Copilot or its integrated vendors and should be reviewed there:

1. Invalidate or rotate the Intercom user JWT flow if supported.
2. Review and invalidate the Intercom `login_url` flow if it is tokenized or reusable.
3. Rotate the Copilot `client_portal_token` if Copilot support can do so.
4. Rotate the ProfitWell auth token if vendor controls allow it.

## Configuration Locations

- Railway production environment:
  - `CRON_SECRET`
- Application config references:
  - `server.js`
  - `.env.example`
  - `README.md`
  - `DEPLOY.md`

## Remaining Risk

- Chat transcripts and any external logs that captured the pasted Copilot source cannot be scrubbed from this repository.
- Third-party vendor tokens embedded by Copilot remain a residual risk until they are invalidated upstream.
- Any automation that used the old cron key must be updated to the new `CRON_SECRET`.
