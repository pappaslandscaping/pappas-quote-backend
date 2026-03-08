---
name: debug-issue
description: Systematically debug a problem by checking logs, tracing data flow, and testing fixes. Use when something isn't working.
argument-hint: [description of the problem]
---

Debug: $ARGUMENTS

Follow this sequence:

## 1. Reproduce
- Understand exactly what's failing and how to trigger it
- Check if the server is running
- Try to reproduce with curl or by reading error messages

## 2. Trace the data flow
- **Frontend:** Read the page's JS to find which API calls it makes
- **API:** Find the endpoint in `server.js`, read the handler
- **Database:** Check the SQL query and verify the table/columns exist
- **Response:** Verify the response shape matches what the frontend expects

## 3. Check common YardDesk issues
- Auth: Is the token being sent? Is `authenticateToken` on the route?
- Customer name: Using the name resolution pattern? (`c.name || ...`)
- Shell: Is `shell.js` loaded? Is there a `.main-content` wrapper?
- SQL: Are column names correct? Check actual table schema with `\d tablename`

## 4. Fix and verify
- Make the smallest fix possible
- Test with curl to confirm the fix works
- Check that related features aren't broken

## 5. Log it
- Add the root cause to `.claude/memory/memory-mistakes.md`
- Format: `- [DATE] Bug: description → Root cause → Fix`
