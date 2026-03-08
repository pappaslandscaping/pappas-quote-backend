---
name: add-endpoint
description: Add a new API endpoint to server.js with proper auth, error handling, and database query. Use when adding backend routes.
argument-hint: [METHOD /api/path]
---

Add a new endpoint to `server.js` with this pattern:

```js
app.METHOD('/api/path', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query('SQL HERE', [params]);
    res.json({ data: result.rows });
  } catch (err) {
    console.error('Error description:', err);
    res.status(500).json({ error: 'User-friendly error message' });
  }
});
```

Checklist:
1. Read `server.js` first to find the right location (group with related endpoints)
2. Add `authenticateToken` middleware unless this is explicitly a public endpoint
3. Use parameterized queries (`$1`, `$2`) — NEVER interpolate user input into SQL
4. Return consistent JSON shape matching existing endpoints
5. Log errors with `console.error` and descriptive context
6. If the endpoint creates/updates data, validate required fields before the query
7. After adding, test with curl:
   ```bash
   curl -H "Authorization: Bearer TOKEN" http://localhost:3000/api/path
   ```
