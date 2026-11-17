# Instructions for migration documentation

Keep migration execution guidance, decisions, handoffs, progress and cleanup
instructions inside this directory. Do not add migration-specific guidance to
the repository root AGENTS.md or other project documentation.

Before work, read README.md, decisions.json, STATE.json and approved-plan.json.
The latest direct user instruction is: **do not write implementation code until
the user explicitly asks**. Local documentation updates and read-only preparation
are allowed. Creating/managing worker chats is authorized; it does not authorize
starting implementation while the code hold is active.

If a required tool, permission or environment capability is missing, report the
specific blocker to the coordinator/user rather than spend time building a long
or complicated workaround. Normal per-command PATH/JAVA_HOME selection is fine.

Use the project's established performance tools and conventions; see PERFORMANCE.md.
Keep ANSI=true correctness coverage separate from benchmark fixture settings.

Maintain cleanup-ledger.json before/after downloads, builds and benchmarks, and
finish CLEANUP.md with exact resource identities, sizes and targeted cleanup
commands. Treat baseline/shared caches and user-provided checkouts as shared/user
resources, never automatically disposable migration outputs. Do not delete them.

Use HANDOFF.md for cross-chat transfer. Each real worker prompt must explicitly
point here because there is deliberately no migration pointer in root AGENTS.md.
Record progress and evidence without silently changing the approved 58-card scope.

The main chat is the default implementation/integration owner; internal helpers
perform bounded research and independent review. The coordinator chooses delegation
and occasional user-visible sessions autonomously within the authorized scope.
Nine feature branches do not require nine separate chats. See HANDOFF.md and
decisions.json#execution_model. The current code hold remains active.
