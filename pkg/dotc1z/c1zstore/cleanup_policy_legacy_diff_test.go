package c1zstore

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSelectSyncsToDelete_LegacyDiffTypesBucketAsFull documents the
// post-removal retention behavior for sync_runs rows written by old SDKs
// with the removed diff-sync types. The dedicated keep-latest-pair branch
// is gone; unrecognized types now fall into the default (full-sync) bucket
// and compete for the retention limit like any snapshot. This is a
// deliberate behavior change, not an accident — pin it so a future reader
// finds the decision in a test instead of a git archaeology session.
func TestSelectSyncsToDelete_LegacyDiffTypesBucketAsFull(t *testing.T) {
	cands := []SyncRun{
		makeCandidate("d_upserts", connectorstore.SyncType("partial_upserts"), 0, true),
		makeCandidate("d_deletions", connectorstore.SyncType("partial_deletions"), 10, true),
		makeCandidate("f1", connectorstore.SyncTypeFull, 20, true),
		makeCandidate("f2", connectorstore.SyncTypeFull, 30, true),
	}
	// Four rows in the full-sync bucket, limit 2 ⇒ the two oldest are
	// selected, which are the legacy diff pair.
	got := SelectSyncsToDelete(cands, "", 2)
	assertDeletes(t, got, []string{"d_upserts", "d_deletions"})
}
