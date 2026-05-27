package pebble

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestDiscoveredAtIsNewer locks in the SQLite-parity contract for
// the engine-level IfNewer predicate.
//
// Adapter-level PutXxxIfNewer methods stamp DiscoveredAt with
// time.Now() before calling here (see pkg/dotc1z/engine/pebble/
// adapter_grants_store.go), so the nil-incoming branch is
// unreachable through the public API today. The branch exists as a
// contract for direct engine callers (e.g. a future migration or
// compactor that bypasses the adapter) and as a NULL-propagation
// match for SQLite's `EXCLUDED.discovered_at > X.discovered_at`
// upsert semantic.
//
// This test pins the contract so a future edit that "simplifies"
// the nil branch doesn't silently regress to the pre-fix behavior
// of writing on nil incoming (which would let any direct caller
// clobber existing records with stamp-less writes).
func TestDiscoveredAtIsNewer(t *testing.T) {
	t1 := timestamppb.New(time.Unix(1_700_000_000, 0))
	t2 := timestamppb.New(time.Unix(1_700_000_100, 0)) // strictly after t1

	tests := []struct {
		name     string
		incoming *timestamppb.Timestamp
		existing *timestamppb.Timestamp
		want     bool
	}{
		// SQLite `NULL > anything` is NULL (don't write). The
		// pre-fix Pebble behavior returned true here, which would
		// have let any direct engine caller without a DiscoveredAt
		// clobber an existing record.
		{"nil incoming, nil existing — do not write", nil, nil, false},
		{"nil incoming, real existing — do not write", nil, t1, false},

		// No prior record at this key → SQLite's INSERT-on-conflict
		// reduces to a plain INSERT. Write unconditionally.
		{"real incoming, nil existing — write", t1, nil, true},

		// Strict After: equal timestamps are NOT newer (matches
		// SQLite's > operator, exercised by TestIfNewerSkipsStaleGrants
		// in adapter_reader_test.go via the full PutXxxRecordsIfNewer
		// path).
		{"strictly newer incoming — write", t2, t1, true},
		{"equal timestamps — do not write", t1, t1, false},
		{"older incoming — do not write", t1, t2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := discoveredAtIsNewer(tt.incoming, tt.existing)
			if got != tt.want {
				t.Fatalf("discoveredAtIsNewer(incoming=%v, existing=%v) = %v, want %v",
					tt.incoming, tt.existing, got, tt.want)
			}
		})
	}
}
