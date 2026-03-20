# Mistakes & Lessons
Format: - [date] What went wrong → What to do instead

- [2026-03-08] Tried to write CLAUDE.md without asking user what they wanted → Always ask about preferences first before creating config files
- [2026-03-20] Season kickoff backend code (all endpoints + table creation) was accidentally deleted from server.js in commit 7b48855 — customers got "link invalid/expired" when clicking confirm links. → (1) Never remove endpoint code without checking if frontend pages depend on it. (2) Store tokens redundantly (now in email_log meta too). (3) Built token recovery from email_log. (4) Always verify existing features still work after large refactors.
