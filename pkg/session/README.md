# Session Cache Implementation

This package provides session cache implementations for the Baton SDK. It includes both in-memory and gRPC-based implementations.

## Overview

The session cache is used to store temporary data during sync operations. It provides a key-value store interface with support for:

- Basic CRUD operations (Get, Set, Delete, Clear)
- Batch operations (GetMany, SetMany)
- Namespace isolation using sync IDs
- Prefix support for key organization
- Context-based configuration
- An implemention will be chosen at runtime (the grpc interface will be used if the build tag is specified).

## Durability and staleness (what connectors may assume)

Sessions are **scratch cache space**, not durable connector state:

- The namespace is per **sync ID**. Sessions never cross syncs: they are
  cleared after the sync ends and do not ship in the saved `.c1z`.
- Session state **persists across crash/resume**, including writes from
  beyond the restored checkpoint: session writes are durable but commit
  **independently of the syncer's checkpoints**, so after a crash the
  resumed attempt's cursor rolls back to the last checkpoint while every
  session write survives. Two consequences (CO-6b-009 in
  `docs/verification/sync-replay-6b/plan.md`):
  - The work between the checkpoint and the crash **re-runs**, and during
    that window the connector can observe its own dead attempt's "future"
    writes. Never use sessions for once-only decisions — whether to emit
    rows, whether a page was "already handled", how to shape a paginated
    response. At-least-once re-execution must be safe with session state
    present from the first run.
  - Conversely, state written during **completed** actions is preserved
    and legitimately consumable later in the sync — resume never re-runs
    completed actions, and the SDK deliberately does **not** clear
    sessions on resume for exactly this reason (an accumulate-then-consume
    cache would be unrecoverable).
- Under the **source-cache protocol**, session-derived caches carry extra
  obligations — see `pkg/sourcecache` (SESSION STORE section), including
  why replay/record verdicts must never come from session-cached answers.

These semantics are formally captured: the walker model's `P6C` monitor
(`formal/walker/`) and the trace oracle's `session_ckpt_consistency`
policy (`formal/occult/`) both state that post-crash session state must
match the restored checkpoint — the shipped durable-at-op-commit
behavior violates it in the zombie direction (a standing expected-red
pin on a real execution's trace), and a resume-time clear violates it in
the amnesia direction. The registered fix is checkpoint-consistent
sessions (CO-6b-009).