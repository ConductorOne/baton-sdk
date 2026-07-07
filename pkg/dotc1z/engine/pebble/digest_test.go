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
// via EntitlementRef.EntitlementId, which is what the seal-time digest
// build keys each digest on.
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

// TestGrantDigestIndexSealOnly verifies the seal-only lifecycle: grant
// writes never produce hash-index rows or digest nodes inline; the
// adapter's EndSync produces both when the digest index is enabled and
// neither when it is disabled.
func TestGrantDigestIndexSealOnly(t *testing.T) {
	ctx := context.Background()
	grants := []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrant("", "g2", "ent-A", "bob"),
	}
	write := func(e *Engine) *Adapter {
		t.Helper()
		a := NewAdapter(e)
		if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
			t.Fatalf("StartNewSync: %v", err)
		}
		putEnt(t, e, ctx, "ent-A")
		if err := e.PutGrantRecords(ctx, grants...); err != nil {
			t.Fatalf("PutGrantRecords: %v", err)
		}
		return a
	}

	// Default (index on): nothing inline; seal derives rows + digests.
	on, _ := newTestEngine(t)
	a := write(on)
	if got := countKeyRangeTest(t, on, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != 0 {
		t.Fatalf("pre-seal: hash index rows = %d, want 0 (never written inline)", got)
	}
	if got := countKeyRangeTest(t, on, DigestLowerBound(), DigestUpperBound()); got != 0 {
		t.Fatalf("pre-seal: digest nodes = %d, want 0 (never written inline)", got)
	}
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if got := countKeyRangeTest(t, on, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != len(grants) {
		t.Fatalf("sealed: hash index rows = %d, want %d", got, len(grants))
	}
	if got := countKeyRangeTest(t, on, DigestLowerBound(), DigestUpperBound()); got == 0 {
		t.Fatal("sealed: no digest nodes built")
	}

	// Disabled: seal skips the derivation entirely.
	off, _ := newTestEngine(t, WithGrantDigestIndex(false))
	aOff := write(off)
	if err := aOff.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if got := countKeyRangeTest(t, off, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != 0 {
		t.Fatalf("digest index off: hash index rows = %d, want 0", got)
	}
	if got := countKeyRangeTest(t, off, DigestLowerBound(), DigestUpperBound()); got != 0 {
		t.Fatalf("digest index off: digest nodes = %d, want 0", got)
	}
	// The other grant indexes are still written inline.
	if got := countKeyRangeTest(t, off, GrantByEntitlementLowerBound(), GrantByEntitlementUpperBound()); got != len(grants) {
		t.Fatalf("digest index off: by_entitlement rows = %d, want %d", got, len(grants))
	}
}

// TestGrantDigestIncludesExpandedGrants guards the seal-time
// derivation: grants written via PutExpandedGrantRecords (like
// directly-synced grants) land in the by_entitlement_principal_hash
// index and the digest when the seal build runs, because the build
// derives both from the grant primaries.
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

	if err := e.SealGrantHashIndexAndDigests(ctx); err != nil {
		t.Fatalf("SealGrantHashIndexAndDigests: %v", err)
	}
	if got := countKeyRangeTest(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != 2 {
		t.Fatalf("hash index rows = %d, want 2 (direct + expanded)", got)
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

	// Disabled: a real entitlement reports not-found (no digest built).
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

// seedEntitlement writes the entitlement record + grants and runs the
// seal-time build (hash index + digests), returning the syncID.
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
	if err := e.SealGrantHashIndexAndDigests(ctx); err != nil {
		t.Fatalf("SealGrantHashIndexAndDigests: %v", err)
	}
	return syncID
}

// rebuildDigestAtWidth re-derives the hash index from the primaries and
// builds one entitlement's digest at a forced leaf-level width (instead
// of the count-derived one), so a test can build two digests of
// different widths over a small grant set.
func rebuildDigestAtWidth(t testing.TB, e *Engine, entID string, widthBits int) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.rebuildGrantHashIndex(ctx); err != nil {
		t.Fatalf("rebuildGrantHashIndex: %v", err)
	}
	if err := e.buildPartitionDigestAtWidth(ctx, grantDigestSpec, entID, widthBits); err != nil {
		t.Fatalf("buildPartitionDigestAtWidth: %v", err)
	}
}

// seedEntitlementAtWidth is seedEntitlement but forces a specific
// leaf-level width instead of deriving it from the grant count.
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
	rebuildDigestAtWidth(t, e, entID, widthBits)
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
	// same index key, new content hash via an added source), then
	// re-derive B's index + digest at width 8 — the seal-style rebuild,
	// since mutations never maintain either inline.
	g := makeGrantWithSources(syncB, "g-000", "ent-A", changed, "src-ent")
	if err := eb.PutGrantRecord(ctx, g); err != nil {
		t.Fatalf("PutGrantRecord (mutate): %v", err)
	}
	rebuildDigestAtWidth(t, eb, "ent-A", 8)

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

// TestDigestDeleteInvalidatesAndResealRecalculates pins the
// present-means-exact lifecycle around the one mutation path that can
// run against built digests: DeleteGrantRecord drops the touched
// entitlement's digest (and its hash-index row), so the digest reads as
// "missing — recalculate"; a seal-style rebuild then byte-matches a
// from-scratch build over the surviving grants.
func TestDigestDeleteInvalidatesAndResealRecalculates(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	grants := make([]*v3.GrantRecord, 0, 30)
	for i := 0; i < 30; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	syncID := seedEntitlement(t, e, "ent-A", grants)

	if _, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A"); err != nil || !ok {
		t.Fatalf("sealed root: ok=%v err=%v", ok, err)
	}
	rows := countKeyRangeTest(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound())
	if rows != 30 {
		t.Fatalf("sealed hash index rows = %d, want 30", rows)
	}

	// Post-seal delete: the digest must be dropped, not silently stale.
	if err := e.DeleteGrantRecord(ctx, "g-008"); err != nil {
		t.Fatalf("DeleteGrantRecord: %v", err)
	}
	if _, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A"); err != nil || ok {
		t.Fatalf("root after delete: ok=%v err=%v, want missing (invalidated)", ok, err)
	}
	if got := digestNodeCount(t, e, syncID); got != 0 {
		t.Fatalf("digest nodes after delete = %d, want 0 (partition dropped)", got)
	}
	// The stale hash-index row is removed too.
	if got := countKeyRangeTest(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()); got != rows-1 {
		t.Fatalf("hash index rows after delete = %d, want %d", got, rows-1)
	}

	// Reseal: the digest is recalculated from the surviving primaries
	// and byte-matches an independent from-scratch build.
	if err := e.SealGrantHashIndexAndDigests(ctx); err != nil {
		t.Fatalf("reseal: %v", err)
	}
	root, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A")
	if err != nil || !ok {
		t.Fatalf("root after reseal: ok=%v err=%v", ok, err)
	}
	if root.Count != 29 {
		t.Fatalf("resealed root count = %d, want 29", root.Count)
	}
	resealed := dumpDigestNodes(t, e, syncID)

	fresh, _ := newTestEngine(t)
	survivors := make([]*v3.GrantRecord, 0, 29)
	for i := 0; i < 30; i++ {
		if i == 8 {
			continue
		}
		survivors = append(survivors, makeGrant("", fmt.Sprintf("g-%03d", i), "ent-A", fmt.Sprintf("user-%03d", i)))
	}
	seedEntitlement(t, fresh, "ent-A", survivors)
	requireSameDigestNodes(t, resealed, dumpDigestNodes(t, fresh, syncID))
}

// TestSealRebuildDropsStaleIndexRows verifies the seal-time index
// derivation is a full re-derivation, not an append: rows for grants
// that changed principal (or were removed) since the last seal do not
// survive a reseal, because the build range-clears the index first.
func TestSealRebuildDropsStaleIndexRows(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := seedEntitlement(t, e, "ent-A", []*v3.GrantRecord{
		makeGrant("", "g1", "ent-A", "alice"),
		makeGrant("", "g2", "ent-A", "bob"),
	})

	// Move g2 to a different principal and reseal. The old (bob) row
	// must be gone and the digest must reflect the new content.
	if err := e.PutGrantRecord(ctx, makeGrant(syncID, "g2", "ent-A", "carol")); err != nil {
		t.Fatalf("PutGrantRecord: %v", err)
	}
	if err := e.SealGrantHashIndexAndDigests(ctx); err != nil {
		t.Fatalf("reseal: %v", err)
	}

	entPrefix := encodeGrantByEntPrincHashEntPrefix("ent-A")
	principals := map[string]bool{}
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: entPrefix, UpperBound: upperBoundOf(entPrefix)})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		_, _, pid, _, ok := decodeEntPrincHashTail(iter.Key(), entPrefix)
		if !ok {
			t.Fatal("failed to decode index tail")
		}
		principals[pid] = true
	}
	if principals["bob"] || !principals["carol"] || !principals["alice"] || len(principals) != 2 {
		t.Fatalf("index principals after reseal = %v, want {alice, carol}", principals)
	}
	root, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-A")
	if err != nil || !ok {
		t.Fatalf("root: ok=%v err=%v", ok, err)
	}
	if root.Count != 2 {
		t.Fatalf("root count = %d, want 2", root.Count)
	}
}

// TestSealIndexSpillMerge forces the seal-time index rebuild through
// the multi-run spill path: a tiny arena makes every few rows cut a
// sorted run file, so the final index comes out of the k-way merge
// rather than a single in-memory sort. The merged result must
// byte-equal a large-arena (single-chunk) rebuild — same rows, same
// hash-major order (bulkSSTWriter would reject any ordering violation
// outright) — and the digests folded over it must match too.
func TestSealIndexSpillMerge(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	if err := e.SetCurrentSync(syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	// One big entitlement that will span many tiny runs, plus small
	// ones that each fit inside a single run.
	putEnt(t, e, ctx, "ent-big")
	putEnt(t, e, ctx, "ent-small-1")
	putEnt(t, e, ctx, "ent-small-2")
	var grants []*v3.GrantRecord
	for i := 0; i < 300; i++ {
		grants = append(grants, makeGrant("", fmt.Sprintf("g-big-%03d", i), "ent-big", fmt.Sprintf("user-%03d", i)))
	}
	grants = append(grants,
		makeGrant("", "g-s1", "ent-small-1", "alice"),
		makeGrant("", "g-s2", "ent-small-2", "bob"),
	)
	if err := e.PutGrantRecords(ctx, grants...); err != nil {
		t.Fatalf("PutGrantRecords: %v", err)
	}

	dumpIndex := func() map[string][]byte {
		t.Helper()
		out := map[string][]byte{}
		iter, err := e.db.NewIter(&pebble.IterOptions{
			LowerBound: GrantByEntPrincHashLowerBound(),
			UpperBound: GrantByEntPrincHashUpperBound(),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		for iter.First(); iter.Valid(); iter.Next() {
			out[string(iter.Key())] = append([]byte(nil), iter.Value()...)
		}
		if err := iter.Error(); err != nil {
			t.Fatal(err)
		}
		return out
	}

	// Tiny 512-byte arena: ~5 rows per run, so ent-big spans dozens of
	// runs and reassembles during the merge.
	countsSpill, err := e.rebuildGrantHashIndexChunked(ctx, 512)
	if err != nil {
		t.Fatalf("chunked rebuild: %v", err)
	}
	spilled := dumpIndex()

	countsMem, err := e.rebuildGrantHashIndexChunked(ctx, sealIndexSpillChunkBytes)
	if err != nil {
		t.Fatalf("single-chunk rebuild: %v", err)
	}
	inMemory := dumpIndex()

	if len(spilled) != len(grants) {
		t.Fatalf("spilled index rows = %d, want %d", len(spilled), len(grants))
	}
	for k, v := range inMemory {
		sv, ok := spilled[k]
		if !ok {
			t.Fatalf("row missing from spilled index: %x", k)
		}
		if !bytes.Equal(sv, v) {
			t.Fatalf("row %x differs: spilled %x, in-memory %x", k, sv, v)
		}
	}
	if len(countsSpill) != len(countsMem) || countsSpill["ent-big"] != 300 || countsSpill["ent-small-1"] != 1 {
		t.Fatalf("counts differ: spill=%v mem=%v", countsSpill, countsMem)
	}

	// Digests folded over the spill-merged index behave normally.
	if err := e.buildAllGrantDigests(ctx, countsSpill); err != nil {
		t.Fatalf("buildAllGrantDigests: %v", err)
	}
	root, ok, err := e.GetEntitlementDigestRoot(ctx, syncID, "ent-big")
	if err != nil || !ok {
		t.Fatalf("root: ok=%v err=%v", ok, err)
	}
	if root.Count != 300 {
		t.Fatalf("ent-big root count = %d, want 300", root.Count)
	}
}

// TestDigestMissingRootWholeDirty: a missing root means "digest never
// built (or invalidated)" — never "no grants", and never something to
// silently derive from an index whose presence can't be verified. The
// comparison must report the whole entitlement dirty, even when the
// underlying content is identical: correctness comes from re-reading,
// not from trusting an unverifiable shortcut.
func TestDigestMissingRootWholeDirty(t *testing.T) {
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
		if len(dirty) != 1 || dirty[0].Bits != 0 {
			t.Fatalf("%s: unbuilt digest on one side: dirty=%v, want one whole-entitlement bucket", name, dirty)
		}
	}
}
