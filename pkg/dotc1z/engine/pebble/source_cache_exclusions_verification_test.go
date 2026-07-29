package pebble

import "testing"

// These are executable Phase 6a boundary exclusions, not silently omitted
// matrix cells. Removing an exclusion requires replacing it with evidence.
func TestVerificationPhase6aExecutableExclusions(t *testing.T) {
	t.Run("C19-scoped-bulk-input", func(t *testing.T) {
		t.Skip("BulkSyncImport consumes conversion v2 rows and has no source-scope or manifest input; scoped bulk shapes are not representable at the Phase 6a API boundary")
	})
	t.Run("C30-compatibility-record-lifecycle", func(t *testing.T) {
		t.Skip("SourceCacheCompatRecord is schema-only in Phase 6a; no Pebble compatibility-family writer or key exists to exercise without implementing deferred compatibility behavior")
	})
	t.Run("C34-transitional-overlay-annotation", func(t *testing.T) {
		t.Skip("overlay=false with emitted rows is a syncer annotation/orchestration shape; SourceCacheStore receives only replay, ordinary puts, and tombstone calls")
	})
	t.Run("source-eligibility-policy", func(t *testing.T) {
		t.Skip("compacted/non-FULL source eligibility and compatibility gating are explicitly deferred beyond Phase 6a")
	})
}
