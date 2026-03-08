---
name: test-page
description: Test a page against the running app to verify it loads and data flows correctly. ALWAYS run this after creating or modifying a page.
argument-hint: [page-name]
---

Test `public/$ARGUMENTS.html` against the running app:

1. **Check the server is running:**
   ```bash
   curl -s http://localhost:3000/ -o /dev/null -w "%{http_code}"
   ```
   If not 200, warn the user to start the server with `npm run dev`.

2. **Get an auth token** (if page requires auth):
   ```bash
   curl -s http://localhost:3000/api/auth/login -X POST \
     -H "Content-Type: application/json" \
     -d '{"email":"EMAIL","password":"PASSWORD"}' | jq -r '.token'
   ```
   Ask the user for credentials if needed.

3. **Fetch the page HTML:**
   ```bash
   curl -s http://localhost:3000/$ARGUMENTS.html | head -50
   ```
   Verify it returns valid HTML (not a 404 or error).

4. **Test API endpoints the page depends on:**
   Read the page's JavaScript to identify which `/api/` calls it makes, then curl each one with the auth token to verify they return data.

5. **Report results:**
   - Page loads: YES/NO
   - APIs return data: list each endpoint + status
   - Any issues found

Do NOT tell the user "it works" until ALL checks pass.
