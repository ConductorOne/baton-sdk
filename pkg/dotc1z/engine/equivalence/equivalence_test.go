package equivalence

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func openPebble(t *testing.T) *enginepkg.Engine {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "engine")
	e, err := enginepkg.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("Open pebble: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func mkGrant(syncID, externalID, entID, principalRT, principalID string) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		SyncId:     syncID,
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
			EntitlementId:  entID,
		}.Build(),
		Principal: v3.PrincipalRef_builder{
			ResourceTypeId: principalRT,
			ResourceId:     principalID,
		}.Build(),
	}.Build()
}

// TestPebbleMatchesMemoryRef is the canonical equivalence assertion:
// for a representative workload of Puts (and a sprinkling of Deletes),
// the Pebble engine and the in-memory reference must produce the same
// Result.
func TestPebbleMatchesMemoryRef(t *testing.T) {
	syncID := ksuid.New().String()

	// Workload: 200 grants across 5 entitlements and 50 principals,
	// then delete 25 (12.5%) to exercise the index-cleanup path.
	ops := make([]Op, 0, 250)
	externalIDs := make([]string, 0, 200)
	entPool := []string{"ent-A", "ent-B", "ent-C", "ent-D", "ent-E"}
	for i := 0; i < 200; i++ {
		ext := ksuid.New().String()
		externalIDs = append(externalIDs, ext)
		ent := entPool[i%len(entPool)] //nolint:gosec // i%len(entPool) is in range
		principalRT := "user"
		principalID := ksuid.New().String()
		// Reuse 10 principals' IDs to make the per-principal index
		// non-empty; pick from a fixed pool.
		switch i % 4 {
		case 0:
			principalID = "shared-principal-1"
		case 1:
			principalID = "shared-principal-2"
		}
		ops = append(ops, Op{Kind: OpPut, Record: mkGrant(syncID, ext, ent, principalRT, principalID)})
	}
	// Delete every 8th grant.
	for i := 0; i < len(externalIDs); i += 8 {
		ops = append(ops, Op{Kind: OpDelete, DeleteExternalID: externalIDs[i]})
	}

	w := Workload{
		Name:   "mixed-puts-deletes",
		SyncID: syncID,
		Ops:    ops,
	}

	mem := NewMemoryRef()
	peb := openPebble(t)

	if err := Compare(context.Background(), mem, peb, w); err != nil {
		t.Fatalf("equivalence mismatch: %v", err)
	}
}

// TestEmptyWorkload — two empty Results are equal.
func TestEmptyWorkload(t *testing.T) {
	syncID := ksuid.New().String()
	w := Workload{Name: "empty", SyncID: syncID, Ops: nil}

	mem := NewMemoryRef()
	peb := openPebble(t)
	if err := Compare(context.Background(), mem, peb, w); err != nil {
		t.Fatalf("empty workload mismatch: %v", err)
	}
}

// TestSingleGrant — minimal non-trivial workload.
func TestSingleGrant(t *testing.T) {
	syncID := ksuid.New().String()
	r := mkGrant(syncID, "ext-1", "ent-1", "user", "alice")
	w := Workload{
		Name:   "single-grant",
		SyncID: syncID,
		Ops:    []Op{{Kind: OpPut, Record: r}},
	}

	mem := NewMemoryRef()
	peb := openPebble(t)
	if err := Compare(context.Background(), mem, peb, w); err != nil {
		t.Fatalf("single-grant mismatch: %v", err)
	}
}

// TestPutOverwrite — putting the same external_id twice updates the
// indexes correctly: the old principal is no longer indexed.
func TestPutOverwrite(t *testing.T) {
	syncID := ksuid.New().String()
	old := mkGrant(syncID, "ext-1", "ent-1", "user", "alice")
	upd := mkGrant(syncID, "ext-1", "ent-1", "user", "bob")
	w := Workload{
		Name:   "overwrite",
		SyncID: syncID,
		Ops: []Op{
			{Kind: OpPut, Record: old},
			{Kind: OpPut, Record: upd},
		},
	}

	mem := NewMemoryRef()
	peb := openPebble(t)
	if err := Compare(context.Background(), mem, peb, w); err != nil {
		t.Fatalf("overwrite mismatch: %v", err)
	}
}
