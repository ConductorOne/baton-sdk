# Requirements Document

## Introduction

This document outlines the requirements for implementing a Pebble-based storage engine as an alternative to the current SQLite-based storage backend in the Baton SDK. The new storage engine will provide a Go-native, high-performance key-value store that eliminates cgo dependencies while maintaining full compatibility with the existing dotc1z/engine.StorageBackend interface.

## Requirements

### Requirement 1

**User Story:** As a Baton SDK developer, I want a Pebble-based storage engine implementation, so that I can leverage a Go-native, high-performance storage backend without cgo dependencies.

#### Acceptance Criteria

1. WHEN the Pebble storage engine is implemented THEN it SHALL implement the complete dotc1z/engine.StorageBackend interface
2. WHEN using the Pebble storage engine THEN it SHALL maintain full API compatibility with the existing SQLite implementation
3. WHEN the Pebble storage engine is used THEN it SHALL eliminate all cgo dependencies from the storage layer
4. WHEN operations are performed THEN the Pebble engine SHALL provide equivalent or better performance compared to SQLite

### Requirement 2

**User Story:** As a connector developer, I want seamless data model compatibility, so that existing connectors work without modification when switching storage backends.

#### Acceptance Criteria

1. WHEN storing resource types THEN the system SHALL maintain the same external_id-based identification
2. WHEN storing resources THEN the system SHALL preserve parent-child relationships and resource_type associations
3. WHEN storing entitlements THEN the system SHALL maintain resource associations and external_id uniqueness
4. WHEN storing grants THEN the system SHALL preserve all relationship mappings (resource, principal, entitlement)
5. WHEN storing assets THEN the system SHALL maintain content_type metadata and binary data integrity
6. WHEN managing sync runs THEN the system SHALL preserve all metadata (started_at, ended_at, token, type, parent)

### Requirement 3

**User Story:** As a system operator, I want efficient query performance, so that list operations and filtering work at scale with large datasets.

#### Acceptance Criteria

1. WHEN listing resources by type THEN the system SHALL use optimized key prefixes for efficient range scans
2. WHEN filtering entitlements by resource THEN the system SHALL use secondary indexes for fast lookups
3. WHEN querying grants by principal, resource, or entitlement THEN the system SHALL use appropriate secondary indexes
4. WHEN performing pagination THEN the system SHALL use key-based tokens instead of integer offsets
5. WHEN executing conditional upserts THEN the system SHALL compare discovered_at timestamps efficiently

### Requirement 4

**User Story:** As a sync process, I want proper sync lifecycle management, so that I can start, checkpoint, and complete syncs with proper cleanup.

#### Acceptance Criteria

1. WHEN starting a new sync THEN the system SHALL create sync metadata and assign a unique sync_id
2. WHEN checkpointing a sync THEN the system SHALL update the sync token atomically
3. WHEN ending a sync THEN the system SHALL update ended_at timestamp and create completion indexes
4. WHEN cleaning up old syncs THEN the system SHALL preserve the latest N full syncs and remove older ones
5. WHEN performing cleanup THEN the system SHALL use efficient range deletions and trigger compaction

### Requirement 5

**User Story:** As a data consumer, I want diff and clone operations, so that I can generate incremental changes and create portable sync snapshots.

#### Acceptance Criteria

1. WHEN generating a sync diff THEN the system SHALL identify records present in applied sync but not in base sync
2. WHEN cloning a sync THEN the system SHALL create a consistent snapshot of all sync data
3. WHEN viewing a specific sync THEN the system SHALL isolate reads to that sync's data only
4. WHEN performing diff operations THEN the system SHALL maintain referential integrity across all entity types

### Requirement 6

**User Story:** As a system administrator, I want proper asset handling, so that binary assets are stored and retrieved efficiently with appropriate size limits.

#### Acceptance Criteria

1. WHEN storing assets THEN the system SHALL preserve content_type metadata
2. WHEN retrieving assets THEN the system SHALL return data as an io.Reader interface
3. WHEN handling large assets THEN the system SHALL define and enforce reasonable size limits
4. WHEN storing asset data THEN the system SHALL maintain data integrity and support efficient retrieval

### Requirement 7

**User Story:** As a developer, I want comprehensive testing and compatibility verification, so that I can trust the new storage engine works correctly.

#### Acceptance Criteria

1. WHEN testing compatibility THEN the system SHALL provide a "tee" mode that writes to both engines and compares results
2. WHEN running tests THEN the system SHALL include property-based tests for key encoding/decoding
3. WHEN validating functionality THEN the system SHALL include cross-engine equivalence tests for all Reader APIs
4. WHEN performing stress testing THEN the system SHALL include fuzzing and metamorphic tests for random sync sequences

### Requirement 8

**User Story:** As a system operator, I want observability and maintenance tools, so that I can monitor performance and maintain data integrity.

#### Acceptance Criteria

1. WHEN monitoring performance THEN the system SHALL expose metrics for write/read operations, compaction stats, and cache hit rates
2. WHEN debugging issues THEN the system SHALL provide logging for slow operations with query details
3. WHEN maintaining data integrity THEN the system SHALL provide tools to verify indexes against primary data
4. WHEN managing storage THEN the system SHALL provide manual compaction and vacuum capabilities