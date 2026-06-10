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

// dumpMerkleNodes snapshots every merkle node key/value for a sync.
// Used to byte-compare an incrementally-maintained tree against a
// from-scratch rebuild.
func dumpMerkleNodes(t testing.TB, e *Engine, syncID string) map[string][]byte {
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
	out := map[string][]byte{}
	for iter.First(); iter.Valid(); iter.Next() {
		out[string(iter.Key())] = append([]byte(nil), iter.Value()...)
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter: %v", err)
	}
	return out
}

// requireSameMerkleNodes fails with a per-key diff when two node
// snapshots differ.
func requireSameMerkleNodes(t *testing.T, got, want map[string][]byte) {
	t.Helper()
	for k, wv := range want {
		gv, ok := got[k]
		if !ok {
			t.Errorf("missing node %x (want %x)", k, wv)
			continue
		}
		if !bytes.Equal(gv, wv) {
			t.Errorf("node %x differs:\n got %x\nwant %x", k, gv, wv)
		}
	}
	for k, gv := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("extra node %x = %x", k, gv)
		}
	}
}

// TestMerkleAllLevelsSparseConsistent verifies the all-levels build:
// every stored node is non-empty, each interior node is exactly the XOR
// (and count-sum) of its children, the root is the fold of level 1, no
// nodes exist beyond the chosen depth, and a stored node byte-matches
// the authoritative on-demand fold of its bucket.
func TestMerkleAllLevelsSparseConsistent(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	const n = 60
	grants := make([]*v3.GrantRecord, 0, n)
	for i := 0; i < n; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	syncID := seedEntitlementAtDepth(t, e, "ent-A", grants, 2)
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		t.Fatal(err)
	}

	root, ok, err := e.GetEntitlementMerkleRoot(ctx, syncID, "ent-A")
	if err != nil || !ok {
		t.Fatalf("root: ok=%v err=%v", ok, err)
	}
	if root.Depth != 2 || root.Count != n {
		t.Fatalf("root depth=%d count=%d, want 2, %d", root.Depth, root.Count, n)
	}

	level1, err := e.merkleChildPrefixes(ctx, idBytes, "ent-A", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(level1) == 0 {
		t.Fatal("no level-1 nodes stored")
	}
	var (
		rootXor   [hashLen]byte
		rootCount int64
		level2N   int
	)
	for _, p1 := range level1 {
		c1, d1, present, err := e.getMerkleNode(idBytes, "ent-A", 1, p1)
		if err != nil || !present {
			t.Fatalf("level-1 node %x: present=%v err=%v", p1, present, err)
		}
		if c1 < 1 {
			t.Fatalf("level-1 node %x stored with count %d; empty nodes must not be materialized", p1, c1)
		}
		children, err := e.merkleChildPrefixes(ctx, idBytes, "ent-A", 2, p1)
		if err != nil {
			t.Fatal(err)
		}
		if len(children) == 0 {
			t.Fatalf("level-1 node %x has no stored children", p1)
		}
		var (
			childXor   [hashLen]byte
			childCount int64
		)
		for _, p2 := range children {
			c2, d2, present2, err := e.getMerkleNode(idBytes, "ent-A", 2, p2)
			if err != nil || !present2 {
				t.Fatalf("level-2 node %x: present=%v err=%v", p2, present2, err)
			}
			if c2 < 1 {
				t.Fatalf("level-2 node %x stored with count %d", p2, c2)
			}
			xorInto(childXor[:], d2)
			childCount += c2
		}
		if childCount != c1 || !bytes.Equal(childXor[:], d1) {
			t.Fatalf("interior node %x != fold of children: count %d vs %d", p1, c1, childCount)
		}
		level2N += len(children)
		xorInto(rootXor[:], d1)
		rootCount += c1
	}
	if rootCount != root.Count || !bytes.Equal(rootXor[:], root.Hash) {
		t.Fatalf("root != fold of level 1: count %d vs %d", root.Count, rootCount)
	}

	// Exactly root + level1 + level2 nodes — nothing beyond the depth.
	if got, want := merkleNodeCount(t, e, syncID), 1+len(level1)+level2N; got != want {
		t.Fatalf("total node count = %d, want %d (root + L1 + L2 only)", got, want)
	}

	// A stored node is a cache of the authoritative fold.
	h, c, err := e.ComputeBucketHash(ctx, syncID, "ent-A", level1[0])
	if err != nil {
		t.Fatal(err)
	}
	c1, d1, _, err := e.getMerkleNode(idBytes, "ent-A", 1, level1[0])
	if err != nil {
		t.Fatal(err)
	}
	if c != c1 || !bytes.Equal(h, d1) {
		t.Fatalf("stored node disagrees with ComputeBucketHash: count %d vs %d", c1, c)
	}
}

// TestMerkleRebuildClearsStaleNodes verifies the leading DeleteRange in
// the build: a rebuild at a shallower depth must remove the deeper
// levels of the prior build, or the comparison descent would read them.
func TestMerkleRebuildClearsStaleNodes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	grants := make([]*v3.GrantRecord, 0, 40)
	for i := 0; i < 40; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	syncID := seedEntitlementAtDepth(t, e, "ent-A", grants, 2)
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		t.Fatal(err)
	}

	level2, err := e.merkleChildPrefixes(ctx, idBytes, "ent-A", 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(level2) == 0 {
		t.Fatal("depth-2 build produced no level-2 nodes")
	}
	rootBefore, _, err := e.GetEntitlementMerkleRoot(ctx, syncID, "ent-A")
	if err != nil {
		t.Fatal(err)
	}

	if err := e.buildEntitlementMerkleAtDepth(ctx, idBytes, "ent-A", 1); err != nil {
		t.Fatalf("rebuild at depth 1: %v", err)
	}

	rootAfter, ok, err := e.GetEntitlementMerkleRoot(ctx, syncID, "ent-A")
	if err != nil || !ok {
		t.Fatalf("root after rebuild: ok=%v err=%v", ok, err)
	}
	if rootAfter.Depth != 1 {
		t.Fatalf("root depth after rebuild = %d, want 1", rootAfter.Depth)
	}
	// Depth-independence: same content, same root digest.
	if !bytes.Equal(rootBefore.Hash, rootAfter.Hash) || rootBefore.Count != rootAfter.Count {
		t.Fatal("rebuild at different depth changed the root digest/count over identical content")
	}
	level2After, err := e.merkleChildPrefixes(ctx, idBytes, "ent-A", 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(level2After) != 0 {
		t.Fatalf("%d stale level-2 nodes survived the depth-1 rebuild", len(level2After))
	}
}

// TestMerkleIncrementalEqualsRebuild is the §7 keystone invariant: after
// a sequence of post-seal inserts, content overwrites, a bucket-moving
// (principal-changing) overwrite, an excluded-field no-op overwrite,
// deletes, and a multi-record batch, the incrementally-maintained tree
// byte-equals a from-scratch rebuild.
func TestMerkleIncrementalEqualsRebuild(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	grants := make([]*v3.GrantRecord, 0, 30)
	for i := 0; i < 30; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	syncID := seedEntitlementAtDepth(t, e, "ent-A", grants, 2)
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		t.Fatal(err)
	}

	put := func(g *v3.GrantRecord) {
		t.Helper()
		g.SetSyncId(syncID)
		if err := e.PutGrantRecord(ctx, g); err != nil {
			t.Fatalf("PutGrantRecord: %v", err)
		}
	}

	// Post-seal inserts.
	put(makeGrant(syncID, "g-100", "ent-A", "new-user-1"))
	put(makeGrant(syncID, "g-101", "ent-A", "new-user-2"))
	// Content overwrite: same index key, sources changed.
	put(makeGrantWithSources(syncID, "g-005", "ent-A", "user-005", "src-ent"))
	// Bucket-moving overwrite: same external_id, principal changed —
	// must apply as remove(old path) + add(new path).
	put(makeGrant(syncID, "g-006", "ent-A", "user-moved"))
	// Excluded-field overwrite: needs_expansion is not part of the
	// content hash, so this must leave the tree untouched.
	noop := makeGrant(syncID, "g-007", "ent-A", "user-007")
	noop.SetNeedsExpansion(true)
	put(noop)
	// Deletes.
	for _, ext := range []string{"g-008", "g-009"} {
		if err := e.DeleteGrantRecord(ctx, syncID, ext); err != nil {
			t.Fatalf("DeleteGrantRecord(%s): %v", ext, err)
		}
	}
	// Multi-record batch: inserts + an overwrite in one PutGrantRecords
	// call, exercising the per-node delta accumulator (all of them
	// share the root).
	batch := []*v3.GrantRecord{
		makeGrant(syncID, "g-110", "ent-A", "batch-user-1"),
		makeGrant(syncID, "g-111", "ent-A", "batch-user-2"),
		makeGrant(syncID, "g-112", "ent-A", "batch-user-3"),
		makeGrantWithSources(syncID, "g-010", "ent-A", "user-010", "src-2"),
	}
	if err := e.PutGrantRecords(ctx, batch...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}

	// Sparsity restored on delete: unless another remaining principal
	// shares user-008's depth-2 prefix, its leaf must be gone.
	remaining := []string{"user-moved", "new-user-1", "new-user-2", "batch-user-1", "batch-user-2", "batch-user-3"}
	for i := 0; i < 30; i++ {
		if i == 6 || i == 8 || i == 9 {
			continue // moved or deleted
		}
		remaining = append(remaining, fmt.Sprintf("user-%03d", i))
	}
	deletedPrefix := principalBucketHash("user", "user-008")[:2]
	shared := false
	for _, p := range remaining {
		if bytes.Equal(principalBucketHash("user", p)[:2], deletedPrefix) {
			shared = true
			break
		}
	}
	if !shared {
		_, _, present, err := e.getMerkleNode(idBytes, "ent-A", 2, deletedPrefix)
		if err != nil {
			t.Fatal(err)
		}
		if present {
			t.Fatal("emptied leaf node survived an incremental delete; sparsity not restored")
		}
	}

	incremental := dumpMerkleNodes(t, e, syncID)
	if err := e.buildEntitlementMerkleAtDepth(ctx, idBytes, "ent-A", 2); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	rebuilt := dumpMerkleNodes(t, e, syncID)
	requireSameMerkleNodes(t, incremental, rebuilt)
}

// TestMerkleSameBatchSameBucketRMW pins the accumulator behavior: one
// PutGrantRecords batch adds several grants that land in the SAME
// depth-1 bucket. A naive read-modify-write against the batch would
// lose all but one delta (a plain pebble.Batch doesn't read through its
// own writes); the accumulator must fold all of them into one node
// write.
func TestMerkleSameBatchSameBucketRMW(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	// Find three principals whose bucket hashes share a first byte.
	collide := []string{"seed-principal"}
	target := principalBucketHash("user", collide[0])[0]
	for i := 0; len(collide) < 3; i++ {
		p := fmt.Sprintf("cand-%d", i)
		if principalBucketHash("user", p)[0] == target {
			collide = append(collide, p)
		}
	}

	base := make([]*v3.GrantRecord, 0, 5)
	for i := 0; i < 5; i++ {
		base = append(base, makeGrant("", fmt.Sprintf("b-%d", i), "ent-A", fmt.Sprintf("base-%d", i)))
	}
	syncID := seedEntitlementAtDepth(t, e, "ent-A", base, 1)
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		t.Fatal(err)
	}

	gs := make([]*v3.GrantRecord, 0, len(collide))
	for i, p := range collide {
		gs = append(gs, makeGrant(syncID, fmt.Sprintf("x-%d", i), "ent-A", p))
	}
	if err := e.PutGrantRecords(ctx, gs...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}

	want := int64(len(collide))
	for i := 0; i < 5; i++ {
		if principalBucketHash("user", fmt.Sprintf("base-%d", i))[0] == target {
			want++
		}
	}
	count, _, present, err := e.getMerkleNode(idBytes, "ent-A", 1, []byte{target})
	if err != nil || !present {
		t.Fatalf("bucket node %x: present=%v err=%v", target, present, err)
	}
	if count != want {
		t.Fatalf("same-batch deltas lost: bucket count = %d, want %d", count, want)
	}

	incremental := dumpMerkleNodes(t, e, syncID)
	if err := e.buildEntitlementMerkleAtDepth(ctx, idBytes, "ent-A", 1); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	requireSameMerkleNodes(t, incremental, dumpMerkleNodes(t, e, syncID))
}

// TestMerkleMissingRootFallback: a missing root means "tree never
// built", not "no grants". Comparison against a populated-but-unbuilt
// side must fall back to the authoritative fold — clean when content is
// identical, whole-entitlement dirty when it differs.
func TestMerkleMissingRootFallback(t *testing.T) {
	ctx := context.Background()
	mk := func() []*v3.GrantRecord {
		return []*v3.GrantRecord{
			makeGrant("", "g1", "ent-A", "alice"),
			makeGrant("", "g2", "ent-A", "bob"),
			makeGrant("", "g3", "ent-A", "carol"),
		}
	}
	ea, _ := newTestEngine(t)
	syncA := seedEntitlement(t, ea, "ent-A", mk())

	// B holds the same grants but never builds a tree.
	eb, _ := newTestEngine(t)
	syncB := ksuid.New().String()
	if err := eb.SetCurrentSync(syncB); err != nil {
		t.Fatal(err)
	}
	putEnt(t, eb, ctx, syncB, "ent-A")
	for _, g := range mk() {
		g.SetSyncId(syncB)
		if err := eb.PutGrantRecord(ctx, g); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, err := eb.GetEntitlementMerkleRoot(ctx, syncB, "ent-A"); err != nil || ok {
		t.Fatalf("B unexpectedly has a root: ok=%v err=%v", ok, err)
	}

	for name, dirtyFn := range map[string]func() ([][]byte, error){
		"A vs B": func() ([][]byte, error) { return ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A") },
		"B vs A": func() ([][]byte, error) { return eb.DirtyEntitlementBuckets(ctx, syncB, ea, syncA, "ent-A") },
	} {
		dirty, err := dirtyFn()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(dirty) != 0 {
			t.Fatalf("%s: identical content with one tree unbuilt: dirty=%d, want 0 (fold fallback)", name, len(dirty))
		}
	}

	// Diverge B; the fallback must now flag the whole entitlement.
	if err := eb.PutGrantRecord(ctx, makeGrant(syncB, "g9", "ent-A", "zed")); err != nil {
		t.Fatal(err)
	}
	dirty, err := ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatal(err)
	}
	if len(dirty) != 1 || len(dirty[0]) != 0 {
		t.Fatalf("diverged content with one tree unbuilt: dirty=%v, want one empty prefix", dirty)
	}
}
