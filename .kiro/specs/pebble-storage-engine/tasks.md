# Implementation Plan

- [x] 1. Set up core Pebble engine structure and key encoding
  - Create `pkg/dotc1z/engine/pebble/engine.go` with PebbleEngine struct implementing StorageEngine interface
  - Implement `pkg/dotc1z/engine/pebble/keys.go` with binary key encoding/decoding functions
  - Create comprehensive unit tests for key encoding with proper sort order verification
  - _Requirements: 1.1, 1.2_

- [x] 2. Implement value serialization with metadata envelope
  - Create `pkg/dotc1z/engine/pebble/values.go` with ValueEnvelope protobuf and codec functions
  - Implement serialization/deserialization for discovered_at timestamps and content_type
  - Write unit tests for value encoding roundtrips and metadata preservation
  - _Requirements: 1.2, 2.1, 2.6_

- [x] 3. Implement basic database lifecycle operations
  - Code `NewPebbleEngine` constructor with proper Pebble database initialization
  - Implement `Close()`, `Dirty()`, and `OutputFilepath()` methods
  - Add database validation and error handling for connection issues
  - Write unit tests for engine lifecycle and configuration
  - _Requirements: 1.1, 1.3_

- [x] 4. Implement sync lifecycle management
  - Create `pkg/dotc1z/engine/pebble/sync.go` with sync run management
  - Implement `StartSync`, `StartNewSync`, `StartNewSyncV2`, `SetCurrentSync`, `CheckpointSync`, `EndSync`
  - Add sync metadata storage using `v1|sr|{sync_id}` key pattern
  - Write unit tests for sync state transitions and metadata persistence
  - _Requirements: 4.1, 4.2, 4.3_

- [x] 5. Implement resource type storage and retrieval
  - Code `PutResourceTypes` and `PutResourceTypesIfNewer` with batch operations
  - Implement `ListResourceTypes` with pagination using key-based tokens
  - Add `GetResourceType` for point lookups using primary keys
  - Write unit tests for resource type CRUD operations and pagination
  - _Requirements: 2.1, 3.1, 3.4_

- [x] 6. Implement resource storage with parent-child relationships
  - Code `PutResources` and `PutResourcesIfNewer` with proper key structure
  - Implement `ListResources` with optional resource type filtering
  - Add `GetResource` for point lookups with composite keys
  - Write unit tests for resource operations and relationship preservation
  - _Requirements: 2.2, 3.1, 3.4_

- [x] 7. Implement entitlement storage with resource associations
  - Code `PutEntitlements` and `PutEntitlementsIfNewer` operations
  - Implement `ListEntitlements` with resource filtering support
  - Add `GetEntitlement` for point lookups
  - Write unit tests for entitlement operations and resource associations
  - _Requirements: 2.3, 3.1, 3.4_

- [x] 8. Create secondary index management system
  - Create `pkg/dotc1z/engine/pebble/indexes.go` with index key generation
  - Implement index maintenance for entitlements-by-resource relationships
  - Add index creation and cleanup during entity operations
  - Write unit tests for index consistency and lookup performance
  - _Requirements: 3.2, 3.3_

- [x] 9. Implement grant storage with multiple relationship indexes
  - Code `PutGrants`, `PutGrantsIfNewer`, and `DeleteGrant` operations
  - Implement secondary indexes for grants-by-resource, grants-by-principal, grants-by-entitlement
  - Add `ListGrants` with filtering by resource, principal, or entitlement
  - Write unit tests for grant operations and all index types
  - _Requirements: 2.4, 3.2, 3.3, 3.4_

- [x] 10. Implement asset storage with binary data handling
  - Code `PutAsset` with content type metadata preservation
  - Implement `GetAsset` returning io.Reader interface for binary data
  - Add proper handling of large asset sizes and memory management
  - Write unit tests for asset storage, retrieval, and content type handling
  - _Requirements: 2.5, 2.6, 6.1, 6.2, 6.3_

- [x] 11. Implement conditional upsert logic for IfNewer operations
  - Add discovered_at timestamp comparison logic in all IfNewer methods
  - Implement atomic read-modify-write operations using Pebble batches
  - Ensure proper error handling and idempotency for concurrent operations
  - Write unit tests for conditional upsert behavior and timestamp comparisons
  - _Requirements: 3.5, 4.1_

- [x] 12. Implement pagination system with key-based tokens
  - Create `pkg/dotc1z/engine/pebble/pagination.go` with token encoding/decoding
  - Replace integer-based pagination with key-based continuation tokens
  - Implement stable pagination across all List operations
  - Write unit tests for pagination consistency and boundary conditions
  - Keep the pagination system extremely simple and concise, think MVP
  - _Requirements: 3.4_

- [x] 13. Implement sync cleanup and maintenance operations
  - Code `Cleanup()` method with efficient range deletions for old syncs
  - Implement preservation logic for latest N full syncs
  - Add manual compaction triggers after large deletions
  - Write unit tests for cleanup operations and space reclamation
  - _Requirements: 4.4, 4.5_

- [ ] 14. Implement diff and clone operations
  - Code `GenerateSyncDiff` with set-difference logic between sync ranges
  - Implement `CloneSync` with consistent snapshot creation
  - Add `ViewSync` for isolating reads to specific sync data
  - Write unit tests for diff generation accuracy and clone consistency
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [ ] 15. Implement remaining StorageEngine interface methods
  - Code `Stats()` method for resource counting across entity types
  - Implement `ListSyncRuns` with proper ordering and pagination
  - Add `ListGrantsForPrincipal` with principal-based filtering
  - Write unit tests for all remaining interface methods
  - _Requirements: 1.1, 1.2_

- [ ] 17. Implement performance optimizations and monitoring
  - Add configurable batch sizes and sync policies for write operations
  - Implement iterator pooling and proper resource cleanup
  - Add performance metrics collection for operations and storage
  - Write benchmark tests comparing performance against SQLite implementation
  - _Requirements: 1.4, 8.1, 8.2_

- [ ] 18. Create comprehensive integration test suite
  - Write cross-engine compatibility tests using identical test data
  - Implement property-based tests for key ordering and pagination consistency
  - Add stress tests for large datasets and concurrent operations
  - Create fuzzing tests for random sync sequences and edge cases
  - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [ ] 19. Add observability and maintenance tooling
  - Implement metrics collection for write/read operations and compaction stats
  - Add slow operation logging with configurable thresholds
  - Create integrity checking tools for index validation
  - Write unit tests for monitoring and maintenance functionality
  - _Requirements: 8.1, 8.2, 8.3, 8.4_