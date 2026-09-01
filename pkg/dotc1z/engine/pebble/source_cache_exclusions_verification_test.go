package pebble

import "testing"

// These are executable Phase 6a boundary exclusions, not silently omitted
// matrix cells. Removing an exclusion requires replacing it with evidence.
func TestVerificationPhase6aExecutableExclusions(t *testing.T) {
	t.Run("C19-scoped-bulk-input", func(t *testing.T) {
		t.Skip("BulkSyncImport consumes conversion v2 rows and has no source-scope or manifest input; scoped bulk shapes are not representable at the Phase 6a API boundary")
	})
	// C25-transitional-empty-overlay-validator: exclusion REMOVED in Phase
	// 6b. The transitional semantics now exist at orchestration level and
	// are pinned by pkg/sync's chaos collection suite: a record round with
	// no validator stamps rows and publishes no entry (fresh-page suite,
	// empty-validator cell), and a later record's validator wins over a
	// replay page's (collection suite, record-wins-validator cell).
	t.Run("C28-invalid-UTF8-protobuf-ID", func(t *testing.T) {
		t.Skip("protobuf string fields cannot encode invalid UTF-8; the representable hostile-ID corpus covers " +
			"empty, NUL, normalization neighbors, max/oversized opaque IDs, and malformed resource BIDs")
	})
	t.Run("C28-empty-resource-BID-tombstone", func(t *testing.T) {
		t.Skip("an empty opaque resource id can be stored and replayed, but bid.MakeResourceBid rejects it, so no canonical resource tombstone selector can represent that cell")
	})
	// C30-compatibility-record-lifecycle: exclusion REMOVED in Phase 6b.
	// The compat writer/reader now exist and the lifecycle boundary is
	// verified by TestSourceCacheCompatRecordLifecycle
	// (source_cache_compat_lifecycle_test.go); eligibility/matching
	// semantics are verified by the sync-level chaos gate suite.
	// C34-transitional-overlay-annotation: exclusion REMOVED in Phase 6b.
	// overlay=false with emitted rows is now an implemented orchestration
	// shape — tolerated as replacement-plus-page-rows — pinned by pkg/sync's
	// chaos collection suite (overlay-false-with-rows cell).
	//
	// source-compatibility-policy: exclusion REMOVED in Phase 6b.
	// Cross-version compatibility matching is implemented as the compat
	// record byte-match gate plus the CO-017 materialization-witness fence,
	// pinned by pkg/sync's gate-matrix chaos suite and the old-fold-shape
	// eligibility case in pebble_etag_replay_test.go.
	t.Run("C37-generate-sync-diff", func(t *testing.T) {
		t.Skip("GenerateSyncDiff creates a partial delta sync rather than a standalone source artifact; CO-013 separately verifies that non-FULL artifacts are replay-ineligible")
	})
}
