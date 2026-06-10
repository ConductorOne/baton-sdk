package pebble

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// putEnt writes an entitlement record whose external_id is entID — the
// same string grants reference via EntitlementRef.EntitlementId, which
// is what BuildAllMerkleTrees keys each tree on.
func putEnt(t testing.TB, e *Engine, ctx context.Context, syncID, entID string) {
	t.Helper()
	rec := v3.EntitlementRecord_builder{
		SyncId:     syncID,
		ExternalId: entID,
		Resource: v3.ResourceRef_builder{
			ResourceTypeId: "app",
			ResourceId:     "github",
		}.Build(),
	}.Build()
	if err := e.PutEntitlementRecord(ctx, rec); err != nil {
		t.Fatalf("PutEntitlementRecord: %v", err)
	}
}

// makeGrantWithSources is makeGrant plus an optional source-entitlement
// set, which grantContentHash folds in — so two grants with the same
// (entitlement, principal, external_id) but different sources produce
// different content hashes while keeping the SAME index key.
func makeGrantWithSources(syncID, externalID, entID, principalID string, sources ...string) *v3.GrantRecord {
	g := makeGrant(syncID, externalID, entID, principalID)
	if len(sources) > 0 {
		m := make(map[string]*v3.GrantSourceRecord, len(sources))
		for _, s := range sources {
			m[s] = v3.GrantSourceRecord_builder{}.Build()
		}
		g.SetSources(m)
	}
	return g
}

// merkleNodeCount counts stored merkle nodes for a sync (across all
// entitlements).
func merkleNodeCount(t testing.TB, e *Engine, syncID string) int {
	t.Helper()
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		t.Fatalf("resolveSyncBytes: %v", err)
	}
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: MerkleSyncLowerBound(idBytes),
		UpperBound: MerkleSyncUpperBound(idBytes),
	})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter: %v", err)
	}
	return n
}

// seedEntitlement writes the entitlement record + grants and builds the
// tree, returning the syncID.
func seedEntitlement(t testing.TB, e *Engine, entID string, grants []*v3.GrantRecord) string {
	t.Helper()
	ctx := context.Background()
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, syncID, entID)
	for _, g := range grants {
		g.SetSyncId(syncID)
		if err := e.PutGrantRecord(ctx, g); err != nil {
			t.Fatalf("PutGrantRecord: %v", err)
		}
	}
	if err := e.BuildAllMerkleTrees(ctx, syncID); err != nil {
		t.Fatalf("BuildAllMerkleTrees: %v", err)
	}
	return syncID
}

// seedEntitlementAtDepth is seedEntitlement but forces a specific tree
// depth instead of deriving it from the grant count, so a test can build
// two trees of different heights over a small grant set.
func seedEntitlementAtDepth(t testing.TB, e *Engine, entID string, grants []*v3.GrantRecord, depth int) string {
	t.Helper()
	ctx := context.Background()
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, syncID, entID)
	for _, g := range grants {
		g.SetSyncId(syncID)
		if err := e.PutGrantRecord(ctx, g); err != nil {
			t.Fatalf("PutGrantRecord: %v", err)
		}
	}
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		t.Fatalf("resolveSyncBytes: %v", err)
	}
	if err := e.buildEntitlementMerkleAtDepth(ctx, idBytes, entID, depth); err != nil {
		t.Fatalf("buildEntitlementMerkleAtDepth: %v", err)
	}
	return syncID
}

// TestMerkleDifferentDepthsComparison builds two trees of different
// heights (depth 1 vs depth 2) over the SAME entitlement and exercises
// DirtyEntitlementBuckets across them. It validates two things the
// equal-depth tests cannot:
//
//   - depth-independence: identical grant content yields the same root
//     hash regardless of tree depth, and compares as zero dirty buckets;
//   - the mismatched-depth descent: after one principal's grant changes,
//     the comparison (at compareDepth = min(1,2) = 1) localizes the
//     change to that principal's bucket — the deeper (depth-2) side
//     folds its index on demand for the depth-1 prefixes — and leaves a
//     known principal in a different bucket clean.
func TestMerkleDifferentDepthsComparison(t *testing.T) {
	ctx := context.Background()

	const nPrincipals = 40
	principals := make([]string, nPrincipals)
	for i := range principals {
		principals[i] = fmt.Sprintf("user-%03d", i)
	}
	mkGrants := func() []*v3.GrantRecord {
		gs := make([]*v3.GrantRecord, 0, nPrincipals)
		for i, p := range principals {
			gs = append(gs, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", p))
		}
		return gs
	}

	// depth1Prefix returns the depth-1 bucket prefix (1 raw hash byte)
	// for a principal, matching how grants are keyed (principal type
	// "user" per makeGrant).
	depth1Prefix := func(principalID string) byte {
		return principalBucketHash("user", principalID)[0]
	}

	ea, _ := newTestEngine(t)
	eb, _ := newTestEngine(t)
	syncA := seedEntitlementAtDepth(t, ea, "ent-A", mkGrants(), 1)
	syncB := seedEntitlementAtDepth(t, eb, "ent-A", mkGrants(), 2)

	ra, okA, err := ea.GetEntitlementMerkleRoot(ctx, syncA, "ent-A")
	if err != nil || !okA {
		t.Fatalf("root A: ok=%v err=%v", okA, err)
	}
	rb, okB, err := eb.GetEntitlementMerkleRoot(ctx, syncB, "ent-A")
	if err != nil || !okB {
		t.Fatalf("root B: ok=%v err=%v", okB, err)
	}
	if ra.Depth != 1 || rb.Depth != 2 {
		t.Fatalf("depths = %d, %d; want 1, 2", ra.Depth, rb.Depth)
	}
	// Depth-independence: identical content -> identical root despite
	// different tree heights.
	if !bytes.Equal(ra.Hash, rb.Hash) {
		t.Fatalf("different-depth trees over identical content disagree on root:\n A(d1)=%x\n B(d2)=%x", ra.Hash, rb.Hash)
	}
	dirty, err := ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatalf("DirtyEntitlementBuckets (identical): %v", err)
	}
	if len(dirty) != 0 {
		t.Fatalf("identical content across depths: dirty=%d, want 0", len(dirty))
	}

	// Pick the principal to change and a "clean" principal known to sit
	// in a different depth-1 bucket.
	changed := principals[0]
	changedPrefix := depth1Prefix(changed)
	cleanP := ""
	for _, p := range principals[1:] {
		if depth1Prefix(p) != changedPrefix {
			cleanP = p
			break
		}
	}
	if cleanP == "" {
		t.Skip("no principal landed in a different depth-1 bucket from the changed one; can't assert localization")
	}

	// Mutate the changed principal's grant in B (same external_id ->
	// same index key, new content hash via an added source) and rebuild
	// B's tree at depth 2.
	g := makeGrantWithSources(syncB, "g-000", "ent-A", changed, "src-ent")
	if err := eb.PutGrantRecord(ctx, g); err != nil {
		t.Fatalf("PutGrantRecord (mutate): %v", err)
	}
	idBytesB, err := eb.resolveSyncBytes(syncB)
	if err != nil {
		t.Fatal(err)
	}
	if err := eb.buildEntitlementMerkleAtDepth(ctx, idBytesB, "ent-A", 2); err != nil {
		t.Fatalf("rebuild B: %v", err)
	}

	rb2, _, _ := eb.GetEntitlementMerkleRoot(ctx, syncB, "ent-A")
	if bytes.Equal(ra.Hash, rb2.Hash) {
		t.Fatal("mutation did not change B's root")
	}

	dirty, err = ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatalf("DirtyEntitlementBuckets (changed): %v", err)
	}
	if len(dirty) == 0 {
		t.Fatal("changed principal across depths produced no dirty buckets")
	}
	// Localization: every dirty entry is a depth-1 prefix (not the
	// whole-entitlement empty prefix).
	for _, p := range dirty {
		if len(p) != 1 {
			t.Fatalf("dirty prefix length = %d, want 1 (compareDepth); got whole-entitlement or wrong-depth bucket", len(p))
		}
	}
	// Loading the dirty buckets in B surfaces the changed principal and
	// excludes the known-clean principal.
	loaded := map[string]bool{}
	for _, prefix := range dirty {
		if err := eb.IterateGrantsByEntitlementBucket(ctx, syncB, "ent-A", prefix, func(g *v3.GrantRecord) bool {
			loaded[g.GetPrincipal().GetResourceId()] = true
			return true
		}); err != nil {
			t.Fatalf("IterateGrantsByEntitlementBucket: %v", err)
		}
	}
	if !loaded[changed] {
		t.Fatalf("dirty buckets did not include the changed principal %q; loaded=%v", changed, loaded)
	}
	if loaded[cleanP] {
		t.Fatalf("dirty buckets wrongly included clean principal %q (different bucket); change was not localized", cleanP)
	}
}

func TestMerkleEmptyEntitlementSingleRoot(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := seedEntitlement(t, e, "ent-empty", nil)

	if got := merkleNodeCount(t, e, syncID); got != 1 {
		t.Fatalf("empty entitlement: merkle node count = %d, want 1 (root only)", got)
	}
	root, ok, err := e.GetEntitlementMerkleRoot(ctx, syncID, "ent-empty")
	if err != nil || !ok {
		t.Fatalf("GetEntitlementMerkleRoot: ok=%v err=%v", ok, err)
	}
	if root.Depth != 0 {
		t.Fatalf("empty entitlement depth = %d, want 0", root.Depth)
	}
	if root.Count != 0 {
		t.Fatalf("empty entitlement count = %d, want 0", root.Count)
	}
}

func TestMerkleIdenticalGrantsSameRoot(t *testing.T) {
	ctx := context.Background()
	mk := func() []*v3.GrantRecord {
		return []*v3.GrantRecord{
			makeGrant("", "g1", "ent-A", "alice"),
			makeGrant("", "g2", "ent-A", "bob"),
			makeGrant("", "g3", "ent-A", "carol"),
		}
	}
	ea, _ := newTestEngine(t)
	eb, _ := newTestEngine(t)
	syncA := seedEntitlement(t, ea, "ent-A", mk())
	syncB := seedEntitlement(t, eb, "ent-A", mk())

	ra, okA, err := ea.GetEntitlementMerkleRoot(ctx, syncA, "ent-A")
	if err != nil || !okA {
		t.Fatalf("root A: ok=%v err=%v", okA, err)
	}
	rb, okB, err := eb.GetEntitlementMerkleRoot(ctx, syncB, "ent-A")
	if err != nil || !okB {
		t.Fatalf("root B: ok=%v err=%v", okB, err)
	}
	if !bytes.Equal(ra.Hash, rb.Hash) {
		t.Fatalf("identical grants produced different roots:\n A=%x\n B=%x", ra.Hash, rb.Hash)
	}
	dirty, err := ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatalf("DirtyEntitlementBuckets: %v", err)
	}
	if len(dirty) != 0 {
		t.Fatalf("identical grants: dirty buckets = %d, want 0", len(dirty))
	}
}

func TestMerkleContentChangeDirtyBucket(t *testing.T) {
	ctx := context.Background()
	// Base set: same in both engines except bob's grant gains a source
	// in B. external_id is unchanged, so the index KEY is identical and
	// only the content hash (and thus bob's bucket) differs.
	baseA := []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrant("", "g2", "ent-A", "bob"),
		makeGrant("", "g3", "ent-A", "carol"),
	}
	baseB := []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrantWithSources("", "g2", "ent-A", "bob", "src-ent"),
		makeGrant("", "g3", "ent-A", "carol"),
	}
	ea, _ := newTestEngine(t)
	eb, _ := newTestEngine(t)
	syncA := seedEntitlement(t, ea, "ent-A", baseA)
	syncB := seedEntitlement(t, eb, "ent-A", baseB)

	ra, _, _ := ea.GetEntitlementMerkleRoot(ctx, syncA, "ent-A")
	rb, _, _ := eb.GetEntitlementMerkleRoot(ctx, syncB, "ent-A")
	if bytes.Equal(ra.Hash, rb.Hash) {
		t.Fatal("content change did not change the root hash")
	}

	dirty, err := ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatalf("DirtyEntitlementBuckets: %v", err)
	}
	if len(dirty) == 0 {
		t.Fatal("content change produced no dirty buckets")
	}

	// Loading the dirty buckets in B must surface bob (the changed
	// principal) and must NOT require touching alice/carol's buckets.
	found := map[string]bool{}
	for _, prefix := range dirty {
		if err := eb.IterateGrantsByEntitlementBucket(ctx, syncB, "ent-A", prefix, func(g *v3.GrantRecord) bool {
			found[g.GetPrincipal().GetResourceId()] = true
			return true
		}); err != nil {
			t.Fatalf("IterateGrantsByEntitlementBucket: %v", err)
		}
	}
	if !found["bob"] {
		t.Fatalf("dirty buckets did not include the changed principal bob; found=%v", found)
	}
}

func TestMerkleAddedGrantDirtyBucket(t *testing.T) {
	ctx := context.Background()
	baseA := []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrant("", "g2", "ent-A", "bob"),
	}
	baseB := []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrant("", "g2", "ent-A", "bob"),
		makeGrant("", "g3", "ent-A", "dave"), // added in B
	}
	ea, _ := newTestEngine(t)
	eb, _ := newTestEngine(t)
	syncA := seedEntitlement(t, ea, "ent-A", baseA)
	syncB := seedEntitlement(t, eb, "ent-A", baseB)

	dirty, err := ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatalf("DirtyEntitlementBuckets: %v", err)
	}
	if len(dirty) == 0 {
		t.Fatal("added grant produced no dirty buckets")
	}
	found := map[string]bool{}
	for _, prefix := range dirty {
		if err := eb.IterateGrantsByEntitlementBucket(ctx, syncB, "ent-A", prefix, func(g *v3.GrantRecord) bool {
			found[g.GetPrincipal().GetResourceId()] = true
			return true
		}); err != nil {
			t.Fatalf("IterateGrantsByEntitlementBucket: %v", err)
		}
	}
	if !found["dave"] {
		t.Fatalf("dirty buckets did not include the added principal dave; found=%v", found)
	}
}

func TestMerkleVariableHeight(t *testing.T) {
	ctx := context.Background()

	// Small entitlement: under one target bucket -> depth 0, single node.
	small := make([]*v3.GrantRecord, 0, 10)
	for i := 0; i < 10; i++ {
		small = append(small, makeGrant("", ksuid.New().String(), "ent-small", ksuid.New().String()))
	}
	es, _ := newTestEngine(t)
	syncS := seedEntitlement(t, es, "ent-small", small)
	rootS, ok, err := es.GetEntitlementMerkleRoot(ctx, syncS, "ent-small")
	if err != nil || !ok {
		t.Fatalf("small root: ok=%v err=%v", ok, err)
	}
	if rootS.Depth != 0 {
		t.Fatalf("small entitlement depth = %d, want 0", rootS.Depth)
	}
	if rootS.Count != 10 {
		t.Fatalf("small entitlement count = %d, want 10", rootS.Count)
	}
	if got := merkleNodeCount(t, es, syncS); got != 1 {
		t.Fatalf("small entitlement node count = %d, want 1", got)
	}

	// Large entitlement: well over the target bucket size -> depth grows,
	// and the tree gains leaf nodes beyond the root.
	const n = merkleTargetBucketSize*3 + 7
	large := make([]*v3.GrantRecord, 0, n)
	for i := 0; i < n; i++ {
		large = append(large, makeGrant("", ksuid.New().String(), "ent-large", ksuid.New().String()))
	}
	el, _ := newTestEngine(t)
	syncL := seedEntitlement(t, el, "ent-large", large)
	rootL, ok, err := el.GetEntitlementMerkleRoot(ctx, syncL, "ent-large")
	if err != nil || !ok {
		t.Fatalf("large root: ok=%v err=%v", ok, err)
	}
	if rootL.Depth < 1 {
		t.Fatalf("large entitlement depth = %d, want >= 1", rootL.Depth)
	}
	if rootL.Count != int64(n) {
		t.Fatalf("large entitlement count = %d, want %d", rootL.Count, n)
	}
	// root + at least 2 leaves (depth>=1 over n grants spreads across
	// many buckets).
	if got := merkleNodeCount(t, el, syncL); got < 3 {
		t.Fatalf("large entitlement node count = %d, want >= 3 (root + leaves)", got)
	}
}

// TestHashIndexIsHashOrdered verifies the new index iterates in
// hash(principal) order: the embedded bucket-hash region is
// non-decreasing across the entitlement's index range.
func TestHashIndexIsHashOrdered(t *testing.T) {
	e, _ := newTestEngine(t)
	grants := make([]*v3.GrantRecord, 0, 200)
	for i := 0; i < 200; i++ {
		grants = append(grants, makeGrant("", ksuid.New().String(), "ent-A", ksuid.New().String()))
	}
	syncID := seedEntitlement(t, e, "ent-A", grants)

	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		t.Fatal(err)
	}
	entPrefix := encodeGrantByEntPrincHashEntPrefix(idBytes, "ent-A")
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: entPrefix, UpperBound: upperBoundOf(entPrefix)})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	var prev []byte
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		bh, _, _, _, ok := decodeEntPrincHashTail(iter.Key(), entPrefix)
		if !ok {
			t.Fatal("failed to decode index tail")
		}
		if prev != nil && bytes.Compare(bh, prev) < 0 {
			t.Fatalf("index not hash-ordered: %x < %x", bh, prev)
		}
		prev = append(prev[:0], bh...)
		count++
	}
	if count != 200 {
		t.Fatalf("hash index entry count = %d, want 200", count)
	}
}
