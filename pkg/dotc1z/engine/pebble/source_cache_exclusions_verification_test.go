package pebble

import "testing"

// These are executable Phase 6a boundary exclusions, not silently omitted
// matrix cells. Removing an exclusion requires replacing it with evidence.
func TestVerificationPhase6aExecutableExclusions(t *testing.T) {
	t.Run("C19-scoped-bulk-input", func(t *testing.T) {
		t.Skip("BulkSyncImport consumes conversion v2 rows and has no source-scope or manifest input; scoped bulk shapes are not representable at the Phase 6a API boundary")
	})
	t.Run("C25-transitional-empty-overlay-validator", func(t *testing.T) {
		t.Skip("SourceCacheStore rejects empty manifest validators and does not receive page-level transitional validators; later-overlay publication belongs to deferred syncer orchestration")
	})
	t.Run("C28-invalid-UTF8-protobuf-ID", func(t *testing.T) {
		t.Skip("protobuf string fields cannot encode invalid UTF-8; the representable hostile-ID corpus covers " +
			"empty, NUL, normalization neighbors, max/oversized opaque IDs, and malformed resource BIDs")
	})
	t.Run("C28-empty-resource-BID-tombstone", func(t *testing.T) {
		t.Skip("an empty opaque resource id can be stored and replayed, but bid.MakeResourceBid rejects it, so no canonical resource tombstone selector can represent that cell")
	})
	t.Run("C30-compatibility-record-lifecycle", func(t *testing.T) {
		t.Skip("SourceCacheCompatRecord is schema-only in Phase 6a; no Pebble compatibility-family writer or key exists to exercise without implementing deferred compatibility behavior")
	})
	t.Run("C34-transitional-overlay-annotation", func(t *testing.T) {
		t.Skip("overlay=false with emitted rows is a syncer annotation/orchestration shape; SourceCacheStore receives only replay, ordinary puts, and tombstone calls")
	})
	t.Run("source-compatibility-policy", func(t *testing.T) {
		t.Skip("FULL/non-compacted eligibility is implemented by CO-013; cross-version compatibility matching remains deferred")
	})
	t.Run("C37-generate-sync-diff", func(t *testing.T) {
		t.Skip("GenerateSyncDiff creates a partial delta sync rather than a standalone source artifact; CO-013 separately verifies that non-FULL artifacts are replay-ineligible")
	})
}
