---
name: add-feature
description: End-to-end feature implementation with backend, frontend, and testing. Use for any substantial new feature.
argument-hint: [feature description]
---

Implement the feature: $ARGUMENTS

Follow this sequence — do NOT skip steps:

## 1. Plan
- Identify which existing files need changes (read them first)
- Identify if new files are needed
- Check if similar features exist to follow the same patterns
- List the API endpoints needed
- List the UI components needed

## 2. Backend First
- Add database table/columns if needed (write the SQL migration)
- Add API endpoints to `server.js` (use `/add-endpoint` pattern)
- Test each endpoint with curl before moving to frontend

## 3. Frontend
- Create or modify HTML pages (use `/new-page` pattern for new pages)
- Use `shared.css` classes before writing new CSS
- Follow existing page patterns for consistency
- Ensure shell.js is used for auth/nav

## 4. Connect the Workflows
- Wire up navigation: sidebar links, breadcrumbs, action buttons
- Add cross-links to related records (customer detail, job detail, etc.)
- Add quick-action buttons where relevant
- Everything should connect — no dead-end pages

## 5. Test
- Run `/test-page` for any new or modified pages
- Verify data flows end-to-end
- Check that existing features still work

## 6. Update Memory
- Log the feature in `.claude/memory/memory-sessions.md`
- Update `.claude/memory/memory-decisions.md` if architectural decisions were made
- Update `CLAUDE.md` if the feature changes the system status table
