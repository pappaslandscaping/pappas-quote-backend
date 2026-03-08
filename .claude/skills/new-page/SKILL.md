---
name: new-page
description: Scaffold a new internal page with shell.js, shared.css, and proper structure. Use when creating any new HTML page for YardDesk.
argument-hint: [page-name]
---

Create a new internal page at `public/$ARGUMENTS.html` following this exact structure:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PAGE_TITLE — YardDesk</title>
  <link rel="stylesheet" href="shared.css">
  <style>
    /* Page-specific styles only. Use shared.css classes first. */
  </style>
</head>
<body>
<script src="shell.js"></script>

<div class="main-content">
  <div class="topbar">
    <h1>PAGE_TITLE</h1>
  </div>
  <div class="content-body">
    <!-- Page content here -->
  </div>
</div>

<script>
document.addEventListener('DOMContentLoaded', async () => {
  const token = localStorage.getItem('token');
  // Fetch data and render
});
</script>
</body>
</html>
```

Checklist:
1. Use `shell.js` for auth and sidebar — do NOT add inline auth or manual sidebar
2. Use classes from `shared.css` before writing new CSS
3. Match existing YardDesk pages in look and feel (Jobber-style)
4. Brand colors: forest green `#2e403d`, lime `#c9dd80`
5. Add the page to the sidebar nav in `shell.js` if it should appear there
6. After creating, run `/test-page $ARGUMENTS` to verify it loads
