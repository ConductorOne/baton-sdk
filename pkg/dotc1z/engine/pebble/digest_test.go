package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/segmentio/ksuid"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// putEnt writes an entitlement record (under the engine's current
// sync) whose external_id is entID — the same string grants reference
// via EntitlementRef.EntitlementId, which is what BuildAllGrantDigests
// keys each digest on.
func putEnt(t testing.TB, e *Engine, ctx context.Context, entID string) {
	t.Helper()
	rec := v3.EntitlementRecord_builder{
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

// digestNodeCount counts stored digest nodes for a sync (across all
// partitions and digested indexes).
func digestNodeCount(t testing.TB, e *Engine, syncID string) int {
	t.Helper()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: DigestLowerBound(),
		UpperBound: DigestUpperBound(),
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

// rawLeafPrefixes returns the stored 2-byte leaf key prefixes for one
// entitlement's grant digest, in key order.
func rawLeafPrefixes(t testing.TB, e *Engine, entID string) [][]byte {
	t.Helper()
	stem := encodeDigestNodeKey(grantDigestSpec.indexID, entID, digestLevelLeaf, nil)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: stem, UpperBound: upperBoundOf(stem)})
	if err != nil {
		t.Fatalf("NewIter: %v", err)
	}
	defer iter.Close()
	var out [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != len(stem)+digestLeafPrefixLen {
			t.Fatalf("leaf key with prefix length %d, want %d", len(key)-len(stem), digestLeafPrefixLen)
		}
		out = append(out, append([]byte(nil), key[len(stem):]...))
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iter: %v", err)
	}
	return out
}

// countKeyRangeTest counts stored keys in [lo, hi).
func countKeyRangeTest(t testing.TB, e *Engine, lo, hi []byte) int {
	t.Helper()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
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

// TestGrantDigestIndexDisabled verifies WithGrantDigestIndex(false)
// suppresses the by_entitlement_principal_hash index (and thus the
// digests that fold over it) on the grant write path, while the default
// keeps it on.
func TestGrantDigestIndexDisabled(t *testing.T) {
	ctx := context.Background()
	grants := []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrant("", "g2", "ent-A", "bob"),
	}
	write := func(e *Engine) {
		t.Helper()
		if err := e.SetCurrentSync(ksuid.New().String()); err != nil {
			t.Fatalf("SetCurrentSync: %v", err)
		}
		putEnt(t, e, ctx, "ent-A")
		if err := e.PutGrantRecords(ctx, grants...); err != nil {
			t.Fatalf("PutGrantRecords: %v", err)
		}
	}

	// Default: index on — one hash-index row per grant.
	on, _ := newTestEngine(t)
	write(on)
	if got := countKeyRangeTest(t, on, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != len(grants) {
		t.Fatalf("digest index on: hash index rows = %d, want %d", got, len(grants))
	}

	// Disabled: no hash-index rows and no digest nodes from the write path.
	off, _ := newTestEngine(t, WithGrantDigestIndex(false))
	write(off)
	if got := countKeyRangeTest(t, off, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != 0 {
		t.Fatalf("digest index off: hash index rows = %d, want 0", got)
	}
	if got := countKeyRangeTest(t, off, DigestLowerBound(), DigestUpperBound()); got != 0 {
		t.Fatalf("digest index off: digest nodes = %d, want 0", got)
	}
	// The other grant indexes are still written — only the hash index is gated.
	if got := countKeyRangeTest(t, off, GrantByEntitlementLowerBound(), GrantByEntitlementUpperBound()); got != len(grants) {
		t.Fatalf("digest index off: by_entitlement rows = %d, want %d", got, len(grants))
	}
}

// TestGrantDigestIncludesExpandedGrants guards the expansion write path:
// grants written via PutExpandedGrantRecords (the scratch index helpers)
// must land in the by_entitlement_principal_hash index and therefore in
// the seal-time digest, exactly like directly-synced grants.
func TestGrantDigestIncludesExpandedGrants(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-A")

	// One directly-synced grant.
	if err := e.PutGrantRecords(ctx, makeGrant("", "g-direct", "ent-A", "alice")); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	// One expanded grant (with a source), via the expansion write path.
	exp := makeGrantWithSources("", "g-expanded", "ent-A", "bob", "src-ent")
	if err := e.PutExpandedGrantRecords(ctx, []*v3.GrantRecord{exp}); err != nil {
		t.Fatalf("PutExpandedGrantRecords: %v", err)
	}

	if got := countKeyRangeTest(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != 2 {
		t.Fatalf("hash index rows = %d, want 2 (direct + expanded)", got)
	}
	if err := e.BuildAllGrantDigests(ctx, syncID); err != nil {
		t.Fatalf("BuildAllGrantDigests: %v", err)
	}
	root, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A")
	if err != nil || !ok {
		t.Fatalf("root: ok=%v err=%v", ok, err)
	}
	if root.Count != 2 {
		t.Fatalf("digest root count = %d, want 2 (expanded grant must be in the digest)", root.Count)
	}
}

// TestAdapterGetEntitlementGrantDigest exercises the reader capability
// (connectorstore.EntitlementGrantDigestReader) end-to-end through the
// Adapter: a sealed sync resolves and returns the root hash + count, an
// unknown entitlement reports not-found, and an engine with the digest
// index disabled reports not-found even for a real entitlement.
func TestAdapterGetEntitlementGrantDigest(t *testing.T) {
	ctx := context.Background()

	seal := func(e *Engine) *Adapter {
		t.Helper()
		a := NewAdapter(e)
		if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
			t.Fatalf("StartNewSync: %v", err)
		}
		putEnt(t, e, ctx, "ent-A")
		if err := e.PutGrantRecords(ctx,
			makeGrant("", "g1", "ent-A", "alice"),
			makeGrant("", "g2", "ent-A", "bob"),
		); err != nil {
			t.Fatalf("PutGrantRecords: %v", err)
		}
		if err := a.EndSync(ctx); err != nil {
			t.Fatalf("EndSync: %v", err)
		}
		return a
	}

	// Digest index on: the entitlement resolves with its grant count.
	on, _ := newTestEngine(t)
	a := seal(on)
	d, found, err := a.GetEntitlementGrantDigest(ctx, "ent-A")
	if err != nil {
		t.Fatalf("GetEntitlementGrantDigest: %v", err)
	}
	if !found {
		t.Fatal("expected found for ent-A")
	}
	if d.Count != 2 {
		t.Fatalf("count = %d, want 2", d.Count)
	}
	if len(d.Hash) == 0 {
		t.Fatal("expected non-empty hash")
	}

	// Unknown entitlement: not found, no error.
	if _, found, err := a.GetEntitlementGrantDigest(ctx, "ent-missing"); err != nil || found {
		t.Fatalf("unknown entitlement: found=%v err=%v, want found=false err=nil", found, err)
	}

	// Digest index disabled: a real entitlement reports not-found
	// (no digest was built).
	off, _ := newTestEngine(t, WithGrantDigestIndex(false))
	aOff := seal(off)
	if _, found, err := aOff.GetEntitlementGrantDigest(ctx, "ent-A"); err != nil || found {
		t.Fatalf("digest off: found=%v err=%v, want found=false err=nil", found, err)
	}
}

// TestAdapterGrantDigestNodes exercises the rollup-node reader: the
// native level is exposed, level 0 returns the root, level == native
// returns the stored leaves (whose counts sum to the total and whose
// hashes XOR to the root), and a finer level returns the sentinel.
func TestAdapterGrantDigestNodes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-A")
	const n = 600 // > digestTargetBucketSize, so the native level is >= 1
	grants := make([]*v3.GrantRecord, 0, n)
	for i := 0; i < n; i++ {
		grants = append(grants, makeGrant("", ksuid.New().String(), "ent-A", ksuid.New().String()))
	}
	if err := e.PutGrantRecords(ctx, grants...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}

	d, found, err := a.GetEntitlementGrantDigest(ctx, "ent-A")
	if err != nil || !found {
		t.Fatalf("digest: found=%v err=%v", found, err)
	}
	if d.Count != n {
		t.Fatalf("count = %d, want %d", d.Count, n)
	}
	if d.Level < 1 {
		t.Fatalf("native level = %d, want >= 1 for %d grants", d.Level, n)
	}

	// Level 0 → a single root node matching the digest root.
	roots, found, err := a.GetEntitlementGrantDigestNodes(ctx, "ent-A", 0)
	if err != nil || !found {
		t.Fatalf("nodes(0): found=%v err=%v", found, err)
	}
	if len(roots) != 1 || roots[0].Count != n || !bytes.Equal(roots[0].Hash, d.Hash) {
		t.Fatalf("level 0 = %+v, want one root node with count %d and the root hash", roots, n)
	}

	// Level == native → the stored leaves: sparse (no zero-count nodes),
	// counts sum to the total, hashes XOR to the root.
	leaves, found, err := a.GetEntitlementGrantDigestNodes(ctx, "ent-A", d.Level)
	if err != nil || !found {
		t.Fatalf("nodes(native): found=%v err=%v", found, err)
	}
	var sum int64
	xor := make([]byte, len(d.Hash))
	for _, nd := range leaves {
		if nd.Count == 0 {
			t.Fatal("leaf node with count 0 returned; leaves must be sparse")
		}
		sum += nd.Count
		for j := range xor {
			xor[j] ^= nd.Hash[j]
		}
	}
	if sum != n {
		t.Fatalf("leaf counts sum = %d, want %d", sum, n)
	}
	if !bytes.Equal(xor, d.Hash) {
		t.Fatal("XOR of leaf hashes != root hash")
	}

	// Finer than native → index-scan fallback (no error). Same totals:
	// counts sum to n and hashes XOR to the root.
	finer, found, err := a.GetEntitlementGrantDigestNodes(ctx, "ent-A", d.Level+1)
	if err != nil || !found {
		t.Fatalf("nodes(native+1): found=%v err=%v, want a scanned result", found, err)
	}
	sum, xor = 0, make([]byte, len(d.Hash))
	for _, nd := range finer {
		sum += nd.Count
		for j := range xor {
			xor[j] ^= nd.Hash[j]
		}
	}
	if sum != n || !bytes.Equal(xor, d.Hash) {
		t.Fatalf("finer-level scan: sum=%d xor-matches-root=%v, want sum %d and matching root", sum, bytes.Equal(xor, d.Hash), n)
	}

	// Absurdly fine level → clamped to the hash resolution, still no error.
	if _, found, err := a.GetEntitlementGrantDigestNodes(ctx, "ent-A", 999); err != nil || !found {
		t.Fatalf("nodes(999): found=%v err=%v, want clamped scan with no error", found, err)
	}
}

// TestAdapterScanGrantBucket exercises the bucket grant scan: level 0
// scans the whole entitlement, per-bucket scans at the native level
// agree with the rollup node counts and partition the grants, yield can
// stop early, and an unknown entitlement scans nothing without error.
func TestAdapterScanGrantBucket(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-A")
	const n = 600 // > digestTargetBucketSize so the native level is >= 1
	grants := make([]*v3.GrantRecord, 0, n)
	for i := 0; i < n; i++ {
		grants = append(grants, makeGrant("", ksuid.New().String(), "ent-A", ksuid.New().String()))
	}
	if err := e.PutGrantRecords(ctx, grants...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}

	// Level 0 = whole entitlement → every grant.
	total := 0
	if err := a.ScanEntitlementGrantBucket(ctx, "ent-A", connectorstore.GrantDigestBucket{Level: 0}, func(*v2.Grant) bool {
		total++
		return true
	}); err != nil {
		t.Fatalf("scan level 0: %v", err)
	}
	if total != n {
		t.Fatalf("level-0 scan yielded %d grants, want %d", total, n)
	}

	// Per native-level bucket: each scan's count matches the rollup node,
	// and the buckets partition all the grants.
	d, _, err := a.GetEntitlementGrantDigest(ctx, "ent-A")
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	nodes, _, err := a.GetEntitlementGrantDigestNodes(ctx, "ent-A", d.Level)
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	sum := 0
	for _, nd := range nodes {
		c := int64(0)
		if err := a.ScanEntitlementGrantBucket(ctx, "ent-A", connectorstore.GrantDigestBucket{Level: d.Level, Index: nd.Index}, func(*v2.Grant) bool {
			c++
			return true
		}); err != nil {
			t.Fatalf("scan bucket %d: %v", nd.Index, err)
		}
		if c != nd.Count {
			t.Fatalf("bucket %d: scanned %d grants, node count says %d", nd.Index, c, nd.Count)
		}
		sum += int(c)
	}
	if sum != n {
		t.Fatalf("per-bucket scans covered %d grants, want %d", sum, n)
	}

	// yield can stop early.
	calls := 0
	if err := a.ScanEntitlementGrantBucket(ctx, "ent-A", connectorstore.GrantDigestBucket{Level: 0}, func(*v2.Grant) bool {
		calls++
		return false
	}); err != nil {
		t.Fatalf("scan early-stop: %v", err)
	}
	if calls != 1 {
		t.Fatalf("early stop: yield called %d times, want 1", calls)
	}

	// Unknown entitlement → nothing, no error.
	missing := 0
	if err := a.ScanEntitlementGrantBucket(ctx, "ent-missing", connectorstore.GrantDigestBucket{Level: 0}, func(*v2.Grant) bool {
		missing++
		return true
	}); err != nil || missing != 0 {
		t.Fatalf("scan unknown entitlement: count=%d err=%v, want 0/nil", missing, err)
	}
}

// seedEntitlement writes the entitlement record + grants and builds the
// digest, returning the syncID.
func seedEntitlement(t testing.TB, e *Engine, entID string, grants []*v3.GrantRecord) string {
	t.Helper()
	ctx := context.Background()
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, entID)
	if err := e.PutGrantRecords(ctx, grants...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	if err := e.BuildAllGrantDigests(ctx, syncID); err != nil {
		t.Fatalf("BuildAllGrantDigests: %v", err)
	}
	return syncID
}

// seedEntitlementAtWidth is seedEntitlement but forces a specific
// leaf-level width instead of deriving it from the grant count, so a
// test can build two digests of different widths over a small grant
// set.
func seedEntitlementAtWidth(t testing.TB, e *Engine, entID string, grants []*v3.GrantRecord, widthBits int) string {
	t.Helper()
	ctx := context.Background()
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	putEnt(t, e, ctx, entID)
	if err := e.PutGrantRecords(ctx, grants...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}
	if err := e.buildPartitionDigestAtWidth(ctx, grantDigestSpec, entID, widthBits); err != nil {
		t.Fatalf("buildPartitionDigestAtWidth: %v", err)
	}
	return syncID
}

// TestDigestDifferentWidthsComparison builds two digests of different
// widths (4 vs 8 bits) over the SAME entitlement and exercises
// DirtyEntitlementBuckets across them. It validates two things the
// equal-width tests cannot:
//
//   - split-independence: identical grant content yields the same root
//     hash regardless of digest width, and compares as zero dirty
//     buckets;
//   - the cross-width merge: after one principal's grant changes, the
//     comparison (at compareBits = min(4,8) = 4) localizes the change to
//     that principal's width-4 bucket — the finer (width-8) side's
//     leaves fold down to width 4 during the scan — and leaves a known
//     principal in a different bucket clean.
func TestDigestDifferentWidthsComparison(t *testing.T) {
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

	// bucket4 returns the width-4 bucket index for a principal, matching
	// how grants are keyed (principal type "user" per makeGrant).
	bucket4 := func(principalID string) uint32 {
		return bucketOfHash(principalBucketHash("user", principalID), 4).Index
	}

	ea, _ := newTestEngine(t)
	eb, _ := newTestEngine(t)
	syncA := seedEntitlementAtWidth(t, ea, "ent-A", mkGrants(), 4)
	syncB := seedEntitlementAtWidth(t, eb, "ent-A", mkGrants(), 8)

	ra, okA, err := ea.GetEntitlementDigestRoot(ctx, syncA, "ent-A")
	if err != nil || !okA {
		t.Fatalf("root A: ok=%v err=%v", okA, err)
	}
	rb, okB, err := eb.GetEntitlementDigestRoot(ctx, syncB, "ent-A")
	if err != nil || !okB {
		t.Fatalf("root B: ok=%v err=%v", okB, err)
	}
	if ra.Bits != 4 || rb.Bits != 8 {
		t.Fatalf("widths = %d, %d; want 4, 8", ra.Bits, rb.Bits)
	}
	// Split-independence: identical content -> identical root despite
	// different digest widths.
	if !bytes.Equal(ra.Hash, rb.Hash) {
		t.Fatalf("different-width digests over identical content disagree on root:\n A(w4)=%x\n B(w8)=%x", ra.Hash, rb.Hash)
	}
	dirty, err := ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatalf("DirtyEntitlementBuckets (identical): %v", err)
	}
	if len(dirty) != 0 {
		t.Fatalf("identical content across widths: dirty=%d, want 0", len(dirty))
	}

	// Pick the principal to change and a "clean" principal known to sit
	// in a different width-4 bucket.
	changed := principals[0]
	changedBucket := bucket4(changed)
	cleanP := ""
	for _, p := range principals[1:] {
		if bucket4(p) != changedBucket {
			cleanP = p
			break
		}
	}
	if cleanP == "" {
		t.Skip("no principal landed in a different width-4 bucket from the changed one; can't assert localization")
	}

	// Mutate the changed principal's grant in B (same external_id ->
	// same index key, new content hash via an added source) and rebuild
	// B's digest at width 8.
	g := makeGrantWithSources(syncB, "g-000", "ent-A", changed, "src-ent")
	if err := eb.PutGrantRecord(ctx, g); err != nil {
		t.Fatalf("PutGrantRecord (mutate): %v", err)
	}
	if err := eb.buildPartitionDigestAtWidth(ctx, grantDigestSpec, "ent-A", 8); err != nil {
		t.Fatalf("rebuild B: %v", err)
	}

	rb2, _, _ := eb.GetEntitlementDigestRoot(ctx, syncB, "ent-A")
	if bytes.Equal(ra.Hash, rb2.Hash) {
		t.Fatal("mutation did not change B's root")
	}

	dirty, err = ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A")
	if err != nil {
		t.Fatalf("DirtyEntitlementBuckets (changed): %v", err)
	}
	if len(dirty) == 0 {
		t.Fatal("changed principal across widths produced no dirty buckets")
	}
	// Localization: every dirty entry is a width-4 bucket (not the
	// whole-entitlement zero bucket).
	for _, b := range dirty {
		if b.Bits != 4 {
			t.Fatalf("dirty bucket bits = %d, want 4 (compareBits); got whole-entitlement or wrong-width bucket", b.Bits)
		}
	}
	// Loading the dirty buckets in B surfaces the changed principal and
	// excludes the known-clean principal.
	loaded := map[string]bool{}
	for _, b := range dirty {
		if err := eb.IterateGrantsByEntitlementBucket(ctx, syncB, "ent-A", b, func(g *v3.GrantRecord) bool {
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

func TestDigestEmptyEntitlementSingleRoot(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := seedEntitlement(t, e, "ent-empty", nil)

	if got := digestNodeCount(t, e, syncID); got != 1 {
		t.Fatalf("empty entitlement: digest node count = %d, want 1 (root only)", got)
	}
	root, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-empty")
	if err != nil || !ok {
		t.Fatalf("GetEntitlementDigestRoot: ok=%v err=%v", ok, err)
	}
	if root.Bits != 0 {
		t.Fatalf("empty entitlement width = %d, want 0", root.Bits)
	}
	if root.Count != 0 {
		t.Fatalf("empty entitlement count = %d, want 0", root.Count)
	}
}

func TestDigestIdenticalGrantsSameRoot(t *testing.T) {
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

	ra, okA, err := ea.GetEntitlementDigestRoot(ctx, syncA, "ent-A")
	if err != nil || !okA {
		t.Fatalf("root A: ok=%v err=%v", okA, err)
	}
	rb, okB, err := eb.GetEntitlementDigestRoot(ctx, syncB, "ent-A")
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

func TestDigestContentChangeDirtyBucket(t *testing.T) {
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

	ra, _, _ := ea.GetEntitlementDigestRoot(ctx, syncA, "ent-A")
	rb, _, _ := eb.GetEntitlementDigestRoot(ctx, syncB, "ent-A")
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
	for _, b := range dirty {
		if err := eb.IterateGrantsByEntitlementBucket(ctx, syncB, "ent-A", b, func(g *v3.GrantRecord) bool {
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

func TestDigestAddedGrantDirtyBucket(t *testing.T) {
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
	for _, b := range dirty {
		if err := eb.IterateGrantsByEntitlementBucket(ctx, syncB, "ent-A", b, func(g *v3.GrantRecord) bool {
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

func TestDigestVariableWidth(t *testing.T) {
	ctx := context.Background()

	// Small entitlement: under one target bucket -> width 0, single node.
	small := make([]*v3.GrantRecord, 0, 10)
	for i := 0; i < 10; i++ {
		small = append(small, makeGrant("", ksuid.New().String(), "ent-small", ksuid.New().String()))
	}
	es, _ := newTestEngine(t)
	syncS := seedEntitlement(t, es, "ent-small", small)
	rootS, ok, err := es.GetEntitlementDigestRoot(ctx, syncS, "ent-small")
	if err != nil || !ok {
		t.Fatalf("small root: ok=%v err=%v", ok, err)
	}
	if rootS.Bits != 0 {
		t.Fatalf("small entitlement width = %d, want 0", rootS.Bits)
	}
	if rootS.Count != 10 {
		t.Fatalf("small entitlement count = %d, want 10", rootS.Count)
	}
	if got := digestNodeCount(t, es, syncS); got != 1 {
		t.Fatalf("small entitlement node count = %d, want 1", got)
	}

	// Large entitlement: well over the target bucket size -> the width
	// grows one bit at a time, and the digest gains leaf nodes beyond
	// the root.
	const n = digestTargetBucketSize*3 + 7
	large := make([]*v3.GrantRecord, 0, n)
	for i := 0; i < n; i++ {
		large = append(large, makeGrant("", ksuid.New().String(), "ent-large", ksuid.New().String()))
	}
	el, _ := newTestEngine(t)
	syncL := seedEntitlement(t, el, "ent-large", large)
	rootL, ok, err := el.GetEntitlementDigestRoot(ctx, syncL, "ent-large")
	if err != nil || !ok {
		t.Fatalf("large root: ok=%v err=%v", ok, err)
	}
	if want := chooseDigestWidth(n); rootL.Bits != want {
		t.Fatalf("large entitlement width = %d, want %d", rootL.Bits, want)
	}
	if rootL.Count != int64(n) {
		t.Fatalf("large entitlement count = %d, want %d", rootL.Count, n)
	}
	// root + at least 2 leaves (width>=1 over n grants spreads across
	// multiple buckets).
	if got := digestNodeCount(t, el, syncL); got < 3 {
		t.Fatalf("large entitlement node count = %d, want >= 3 (root + leaves)", got)
	}
	// Capacity invariant: 2^width buckets at the target size must cover
	// the count, and width-1 must not (else the width is too large).
	if int64(1)<<rootL.Bits*digestTargetBucketSize < n {
		t.Fatalf("width %d gives capacity below count %d", rootL.Bits, n)
	}
	if rootL.Bits > 0 && int64(1)<<(rootL.Bits-1)*digestTargetBucketSize >= n {
		t.Fatalf("width %d is one bit wider than the count %d needs", rootL.Bits, n)
	}
}

// TestHashIndexIsHashOrdered verifies the index iterates in
// hash(principal) order: the embedded bucket-hash region is
// non-decreasing across the entitlement's index range.
func TestHashIndexIsHashOrdered(t *testing.T) {
	e, _ := newTestEngine(t)
	grants := make([]*v3.GrantRecord, 0, 200)
	for i := 0; i < 200; i++ {
		grants = append(grants, makeGrant("", ksuid.New().String(), "ent-A", ksuid.New().String()))
	}
	seedEntitlement(t, e, "ent-A", grants)

	entPrefix := encodeGrantByEntPrincHashEntPrefix("ent-A")
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

// dumpDigestNodes snapshots every digest node key/value for a sync.
// Used to byte-compare an incrementally-maintained digest against a
// from-scratch rebuild.
func dumpDigestNodes(t testing.TB, e *Engine, syncID string) map[string][]byte {
	t.Helper()
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: DigestLowerBound(),
		UpperBound: DigestUpperBound(),
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

// requireSameDigestNodes fails with a per-key diff when two node
// snapshots differ.
func requireSameDigestNodes(t *testing.T, got, want map[string][]byte) {
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

// TestDigestLeafFoldConsistent verifies the leaf-level build and the
// fold machinery the comparison rests on: every stored leaf is
// non-empty, the root is exactly the XOR (and count-sum) of the leaves,
// no nodes exist beyond root + leaves, folding the leaf level to a
// coarser width matches a manual regrouping, and a stored leaf
// byte-matches the authoritative on-demand fold of its bucket.
func TestDigestLeafFoldConsistent(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	const n = 60
	grants := make([]*v3.GrantRecord, 0, n)
	for i := 0; i < n; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	syncID := seedEntitlementAtWidth(t, e, "ent-A", grants, 8)

	root, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A")
	if err != nil || !ok {
		t.Fatalf("root: ok=%v err=%v", ok, err)
	}
	if root.Bits != 8 || root.Count != n {
		t.Fatalf("root width=%d count=%d, want 8, %d", root.Bits, root.Count, n)
	}

	// Folding at the build width returns the stored leaves one-to-one.
	leaves, err := e.foldedLeafBuckets(ctx, grantDigestSpec, "ent-A", 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(leaves) == 0 {
		t.Fatal("no leaf nodes stored")
	}
	var (
		rootXor   [hashLen]byte
		rootCount int64
	)
	for _, l := range leaves {
		if l.count < 1 {
			t.Fatalf("leaf %d stored with count %d; empty leaves must not be materialized", l.idx, l.count)
		}
		xorInto(rootXor[:], l.digest[:])
		rootCount += l.count
	}
	if rootCount != root.Count || !bytes.Equal(rootXor[:], root.Hash) {
		t.Fatalf("root != fold of leaves: count %d vs %d", root.Count, rootCount)
	}

	// Exactly root + leaves — nothing else in the keyspace.
	if got, want := digestNodeCount(t, e, syncID), 1+len(leaves); got != want {
		t.Fatalf("total node count = %d, want %d (root + leaves only)", got, want)
	}

	// Folding to a coarser width matches a manual regroup of the
	// build-width leaves.
	leaves4, err := e.foldedLeafBuckets(ctx, grantDigestSpec, "ent-A", 4)
	if err != nil {
		t.Fatal(err)
	}
	manual := map[uint32]*foldedBucket{}
	var order []uint32
	for _, l := range leaves {
		idx := l.idx >> 4
		fb, ok := manual[idx]
		if !ok {
			fb = &foldedBucket{idx: idx}
			manual[idx] = fb
			order = append(order, idx)
		}
		fb.count += l.count
		xorInto(fb.digest[:], l.digest[:])
	}
	if len(leaves4) != len(order) {
		t.Fatalf("fold to width 4: %d buckets, want %d", len(leaves4), len(order))
	}
	for i, idx := range order {
		got, want := leaves4[i], manual[idx]
		if got.idx != want.idx || got.count != want.count || got.digest != want.digest {
			t.Fatalf("folded bucket %d mismatch: got {%d %d %x}, want {%d %d %x}",
				i, got.idx, got.count, got.digest, want.idx, want.count, want.digest)
		}
	}

	// A stored leaf is a cache of the authoritative fold.
	b := DigestBucket{Index: leaves[0].idx, Bits: 8}
	h, c, err := e.ComputeEntitlementBucketDigest(ctx, syncID, "ent-A", b)
	if err != nil {
		t.Fatal(err)
	}
	lc, ld, present, err := e.getDigestLeaf(grantDigestSpec, "ent-A", b.leafKeyPrefix())
	if err != nil || !present {
		t.Fatalf("leaf %d: present=%v err=%v", b.Index, present, err)
	}
	if c != lc || !bytes.Equal(h, ld) {
		t.Fatalf("stored leaf disagrees with ComputeEntitlementBucketDigest: count %d vs %d", lc, c)
	}
}

// TestDigestRebuildClearsStaleNodes verifies the leading DeleteRange in
// the build: a rebuild at a narrower width must remove the prior
// build's finer-grained leaves, or the comparison merge scan would read
// them.
func TestDigestRebuildClearsStaleNodes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	grants := make([]*v3.GrantRecord, 0, 40)
	for i := 0; i < 40; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	syncID := seedEntitlementAtWidth(t, e, "ent-A", grants, 8)

	before := rawLeafPrefixes(t, e, "ent-A")
	if len(before) == 0 {
		t.Fatal("width-8 build produced no leaves")
	}
	rootBefore, _, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A")
	if err != nil {
		t.Fatal(err)
	}

	if err := e.buildPartitionDigestAtWidth(ctx, grantDigestSpec, "ent-A", 4); err != nil {
		t.Fatalf("rebuild at width 4: %v", err)
	}

	rootAfter, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A")
	if err != nil || !ok {
		t.Fatalf("root after rebuild: ok=%v err=%v", ok, err)
	}
	if rootAfter.Bits != 4 {
		t.Fatalf("root width after rebuild = %d, want 4", rootAfter.Bits)
	}
	// Split-independence: same content, same root digest.
	if !bytes.Equal(rootBefore.Hash, rootAfter.Hash) || rootBefore.Count != rootAfter.Count {
		t.Fatal("rebuild at different width changed the root digest/count over identical content")
	}
	// Every surviving leaf prefix must be width-4 aligned (low 12 bits
	// of the left-aligned prefix zero) — a width-8 leaf that escaped the
	// range-clear would fail this.
	after := rawLeafPrefixes(t, e, "ent-A")
	if len(after) == 0 || len(after) > 16 {
		t.Fatalf("width-4 rebuild stored %d leaves, want 1..16", len(after))
	}
	for _, p := range after {
		if lv := binary.BigEndian.Uint16(p); lv&0x0FFF != 0 {
			t.Fatalf("stale leaf prefix %x survived the width-4 rebuild", p)
		}
	}
}

// TestDigestIncrementalEqualsRebuild is the §7 keystone invariant: after
// a sequence of post-seal inserts, content overwrites, a bucket-moving
// (principal-changing) overwrite, an excluded-field no-op overwrite,
// deletes, and a multi-record batch, the incrementally-maintained
// digest byte-equals a from-scratch rebuild.
func TestDigestIncrementalEqualsRebuild(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	grants := make([]*v3.GrantRecord, 0, 30)
	for i := 0; i < 30; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	syncID := seedEntitlementAtWidth(t, e, "ent-A", grants, 8)

	put := func(g *v3.GrantRecord) {
		t.Helper()
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
	// content hash, so this must leave the digest untouched.
	noop := makeGrant(syncID, "g-007", "ent-A", "user-007")
	noop.SetNeedsExpansion(true)
	put(noop)
	// Deletes.
	for _, ext := range []string{"g-008", "g-009"} {
		if err := e.DeleteGrantRecord(ctx, ext); err != nil {
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
	// shares user-008's width-8 bucket, its leaf must be gone.
	remaining := []string{"user-moved", "new-user-1", "new-user-2", "batch-user-1", "batch-user-2", "batch-user-3"}
	for i := 0; i < 30; i++ {
		if i == 6 || i == 8 || i == 9 {
			continue // moved or deleted
		}
		remaining = append(remaining, fmt.Sprintf("user-%03d", i))
	}
	deletedLeaf := bucketOfHash(principalBucketHash("user", "user-008"), 8).leafKeyPrefix()
	shared := false
	for _, p := range remaining {
		if bytes.Equal(bucketOfHash(principalBucketHash("user", p), 8).leafKeyPrefix(), deletedLeaf) {
			shared = true
			break
		}
	}
	if !shared {
		_, _, present, err := e.getDigestLeaf(grantDigestSpec, "ent-A", deletedLeaf)
		if err != nil {
			t.Fatal(err)
		}
		if present {
			t.Fatal("emptied leaf node survived an incremental delete; sparsity not restored")
		}
	}

	incremental := dumpDigestNodes(t, e, syncID)
	if err := e.buildPartitionDigestAtWidth(ctx, grantDigestSpec, "ent-A", 8); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	rebuilt := dumpDigestNodes(t, e, syncID)
	requireSameDigestNodes(t, incremental, rebuilt)
}

// TestDigestSameBatchSameBucketRMW pins the accumulator behavior: one
// PutGrantRecords batch adds several grants that land in the SAME
// width-8 bucket. A naive read-modify-write against the batch would
// lose all but one delta (a plain pebble.Batch doesn't read through its
// own writes); the accumulator must fold all of them into one node
// write.
func TestDigestSameBatchSameBucketRMW(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	// Find three principals whose bucket hashes share a first byte —
	// the top 8 bits, i.e. the same width-8 bucket.
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
	syncID := seedEntitlementAtWidth(t, e, "ent-A", base, 8)

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
	count, _, present, err := e.getDigestLeaf(grantDigestSpec, "ent-A", []byte{target, 0})
	if err != nil || !present {
		t.Fatalf("bucket leaf %x: present=%v err=%v", target, present, err)
	}
	if count != want {
		t.Fatalf("same-batch deltas lost: bucket count = %d, want %d", count, want)
	}

	incremental := dumpDigestNodes(t, e, syncID)
	if err := e.buildPartitionDigestAtWidth(ctx, grantDigestSpec, "ent-A", 8); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	requireSameDigestNodes(t, incremental, dumpDigestNodes(t, e, syncID))
}

// TestDigestMissingRootFallback: a missing root means "digest never
// built", not "no grants". Comparison against a populated-but-unbuilt
// side must fall back to the authoritative fold — clean when content is
// identical, whole-entitlement dirty when it differs.
func TestDigestMissingRootFallback(t *testing.T) {
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

	// B holds the same grants but never builds a digest.
	eb, _ := newTestEngine(t)
	syncB := ksuid.New().String()
	if err := eb.SetCurrentSync(syncB); err != nil {
		t.Fatal(err)
	}
	putEnt(t, eb, ctx, "ent-A")
	for _, g := range mk() {
		if err := eb.PutGrantRecord(ctx, g); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, err := eb.GetEntitlementDigestRoot(ctx, syncB, "ent-A"); err != nil || ok {
		t.Fatalf("B unexpectedly has a root: ok=%v err=%v", ok, err)
	}

	for name, dirtyFn := range map[string]func() ([]DigestBucket, error){
		"A vs B": func() ([]DigestBucket, error) { return ea.DirtyEntitlementBuckets(ctx, syncA, eb, syncB, "ent-A") },
		"B vs A": func() ([]DigestBucket, error) { return eb.DirtyEntitlementBuckets(ctx, syncB, ea, syncA, "ent-A") },
	} {
		dirty, err := dirtyFn()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(dirty) != 0 {
			t.Fatalf("%s: identical content with one digest unbuilt: dirty=%d, want 0 (fold fallback)", name, len(dirty))
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
	if len(dirty) != 1 || dirty[0].Bits != 0 {
		t.Fatalf("diverged content with one digest unbuilt: dirty=%v, want one whole-entitlement bucket", dirty)
	}
}
