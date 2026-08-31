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
- Session writes are durable within a sync but commit **independently of
  the syncer's checkpoints**. After a crash, the resumed attempt's cursor
  rolls back to the last checkpoint while session writes from beyond it
  survive — session state can describe work the syncer will re-execute.
  Never use sessions to decide whether to emit rows or how to shape a
  paginated response; at-least-once re-execution must be safe.
- Under the **source-cache protocol** (`SourceCacheCapability`
  MODE_READ_WRITE), sessions are additionally **attempt-scoped**: the SDK
  clears the sync's session namespace when it resumes an interrupted sync,
  before any connector call (CO-6b-009 in
  `docs/verification/sync-replay-6b/plan.md`). Treat sessions as a cache
  that can vanish between any two calls and rebuild on miss. See
  `pkg/sourcecache` for the full obligations, including why replay/record
  verdicts must never come from session-cached answers.