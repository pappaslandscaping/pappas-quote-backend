---
name: reflect
description: Capture learnings, mistakes, and decisions from the current session into memory files. Run at the end of sessions with fixes or discoveries.
---

Review this session and update the memory files:

## 1. Check for mistakes or repeated instructions
If anything went wrong, was corrected, or had to be explained twice:
→ Add to `.claude/memory/memory-mistakes.md`
Format: `- [DATE] What went wrong → What to do instead`

## 2. Check for decisions
If any architectural, design, or workflow decisions were made:
→ Add to `.claude/memory/memory-decisions.md`
Format: `- [DATE] Decision description`

## 3. Check for new user preferences
If the user expressed how they want things done:
→ Add to `.claude/memory/memory-preferences.md`

## 4. Check for profile updates
If the user shared new facts about themselves or their business:
→ Add to `.claude/memory/memory-profile.md`

## 5. Log the session
→ Add a one-line summary to `.claude/memory/memory-sessions.md`
Format: `- [DATE] What was accomplished`

## 6. Update CLAUDE.md if needed
If the system status table changed (feature moved from Future → Partial → Done), update it.

Be concise. Don't add noise. Only log things that would be useful in future sessions.
