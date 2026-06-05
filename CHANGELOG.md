# Changelog

All notable changes to baton-sdk will be documented in this file.

## [v0.12.4] - 2026-06-04

### Fixed

- Fixed grant expansion edge cases (#923).

## [v0.12.3] - 2026-06-02

### Fixed

- Demoted already-closed `Close` log to Debug — defensive, idempotent path
  that no longer produces noisy warnings (#920).

## [v0.12.2] - 2026-05-29

### Added

- `baton rollback-expansion` subcommand for reverting grant expansion (#906).
- c1z sanitizer library and CLI for inspecting sync artifacts (#875).

### Fixed

- **Grant expansion clobbering** (#897): The Pebble storage engine was
  re-stamping `discovered_at` to the current time whenever a grant was
  re-written during expansion. This caused the timestamp to drift forward,
  breaking staleness-based reconciliation logic. The fix preserves the original
  `discovered_at` from the prior record, matching the behavior of the previous
  SQLite-backed engine.

## [v0.12.1] - 2026-05-27

### Added

- `WithSkipVacuum` option to skip the VACUUM step inside `Cleanup` (#900).

## [v0.12.0] - 2026-05-26

### Changed

- **Removed c1z manager** (#901). The c1z manager abstraction has been removed.
  Callers that previously used the manager should interact with the storage
  engine directly.

## [v0.11.0] - 2026-05-22

### Added

- NHI Phase-1: spine traits and Agent Data Model for non-human identity
  tracking (#902, #903).

## [v0.10.0] - 2026-05-15

### Changed

- **Pebble storage engine** (#874): Replaced the SQLite-backed storage engine
  with a Pebble-based engine. This is the most significant infrastructure change
  in this release cycle. The new engine changes how grants are persisted during
  sync and expansion.

- **Syncer simplification** (#879): Removed the separate sequential sync
  implementation and unified all sync paths through the parallel syncer (with a
  default concurrency of 1). This simplification does not change sync semantics
  but reduces code paths and makes behavior more predictable.

### Important: Grant Sync Behavior Change

Prior to v0.10.0, when a connector returned an empty grant list for a resource
type (e.g., because no `grants` config section was defined in baton-http), the
syncer's interaction with the old SQLite storage engine could cause previously
discovered grants to be marked stale and eventually removed during
reconciliation. This led to **premature grant removal** — grants were revoked
before their intended expiry.

Starting with v0.10.0, the Pebble storage engine and syncer changes fixed this
behavior. The syncer now only writes what the connector returns; an empty grant
list is effectively a no-op that does not trigger removal of previously
discovered grants.

**Impact for baton-http users**: If you previously added a `grants` config
section solely to work around premature grant removal, that workaround is no
longer necessary. However, keeping the `grants` section is still recommended
for full grant visibility and reconciliation in ConductorOne.
