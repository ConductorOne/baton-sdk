package pebble

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	cpebble "github.com/cockroachdb/pebble/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// capturingCore is a minimal zapcore.Core that records log messages so
// tests can assert which degradation transitions fired (the bucket
// state machine is function-local; logs are its only external trace).
type capturingCore struct {
	zapcore.LevelEnabler
	mu       *sync.Mutex
	messages *[]string
}

func newCapturingLogger() (*zap.Logger, func() []string) {
	mu := &sync.Mutex{}
	messages := &[]string{}
	core := capturingCore{LevelEnabler: zapcore.DebugLevel, mu: mu, messages: messages}
	return zap.New(core), func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(*messages))
		copy(out, *messages)
		return out
	}
}

func (c capturingCore) With([]zapcore.Field) zapcore.Core { return c }
func (c capturingCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}
func (c capturingCore) Write(e zapcore.Entry, _ []zapcore.Field) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*c.messages = append(*c.messages, e.Message)
	return nil
}
func (c capturingCore) Sync() error { return nil }

func containsMessage(messages []string, want string) bool {
	for _, m := range messages {
		if m == want {
			return true
		}
	}
	return false
}

// degGrants builds n grant specs named prefix-0..prefix-(n-1), all at ts.
func degGrants(prefix string, n int, ts time.Time) []kwayGrantSpec {
	out := make([]kwayGrantSpec, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, kwayGrantSpec{
			id:          fmt.Sprintf("%s-%d", prefix, i),
			principalID: "alice",
			entitlement: "member",
			discovered:  ts,
		})
	}
	return out
}

// dumpDestSyncContents snapshots every primary and secondary-index key
// under the dest sync across all buckets. Both engines under
// comparison use the SAME dest sync id, so byte-equal maps mean
// byte-equal merge output (records and derived indexes).
func dumpDestSyncContents(t *testing.T, e *enginepkg.Engine) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, bucket := range allBuckets() {
		lo, hi := bucket.syncRange()
		ranges := append([][2][]byte{{lo, hi}}, bucketIndexRanges(bucket)...)
		for _, r := range ranges {
			iter, err := e.DB().NewIter(&cpebble.IterOptions{LowerBound: r[0], UpperBound: r[1]})
			if err != nil {
				t.Fatal(err)
			}
			for iter.First(); iter.Valid(); iter.Next() {
				out[string(iter.Key())] = string(iter.Value())
			}
			if err := iter.Error(); err != nil {
				_ = iter.Close()
				t.Fatal(err)
			}
			_ = iter.Close()
		}
	}
	return out
}

// assertOverlayMatchesKWay merges sources twice — degraded overlay
// (with opts) and pure kway — into the same dest sync id, then
// requires byte-identical dest contents and equal stats records.
// Returns the captured overlay log messages so callers can assert
// which degradation transition fired.
func assertOverlayMatchesKWay(t *testing.T, ctx context.Context, sources []SourceFile, opts ...OverlayOption) []string {
	t.Helper()
	destSyncID := ksuid.New().String()

	logger, capture := newCapturingLogger()
	overlayCtx := ctxzap.ToContext(ctx, logger)
	overlayDest, _ := newEngine(t, "deg-overlay-dest")
	overlayStats, err := MergeFilesIntoOverlay(overlayCtx, overlayDest, sources, destSyncID, t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("MergeFilesIntoOverlay: %v", err)
	}

	kwayDest, _ := newEngine(t, "deg-kway-dest")
	kwayStats, err := MergeFilesInto(ctx, kwayDest, sources, destSyncID, t.TempDir())
	if err != nil {
		t.Fatalf("MergeFilesInto: %v", err)
	}

	got := dumpDestSyncContents(t, overlayDest)
	want := dumpDestSyncContents(t, kwayDest)
	if len(got) != len(want) {
		t.Fatalf("overlay dest has %d keys, kway dest has %d", len(got), len(want))
	}
	for k, v := range want {
		gv, ok := got[k]
		if !ok {
			t.Fatalf("overlay dest missing key %q", k)
		}
		if gv != v {
			t.Fatalf("overlay dest value mismatch for key %q", k)
		}
	}
	if !proto.Equal(overlayStats, kwayStats) {
		t.Fatalf("stats mismatch:\noverlay: %v\nkway:    %v", overlayStats, kwayStats)
	}
	return capture()
}

// TestOverlayDegradationParity drives every degradation transition
// with tiny limits and requires byte-parity (records, indexes, stats)
// with the pure kway merge of the same sources.
func TestOverlayDegradationParity(t *testing.T) {
	ctx := context.Background()

	t.Run("boundary resume within buffer", func(t *testing.T) {
		dir := t.TempDir()
		// Source 0 carries 11 distinct grants: soft=10 is crossed
		// mid-scan, hard=12 is not, so the source finishes inside the
		// buffer and the bucket resumes at the boundary. Sources 1-2
		// flow through the resumed (map-aware) path: g-0 with a newer
		// ts (replace), g-1 with an older ts (drop), and new keys
		// (fresh admissions).
		src0 := writeKWaySource(t, ctx, filepath.Join(dir, "src0.c1z"), degGrants("g", 11, time.Unix(2000, 0).UTC()), false)
		src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), []kwayGrantSpec{
			{id: "g-0", principalID: "bob", entitlement: "member", discovered: time.Unix(3000, 0).UTC()},
			{id: "g-1", principalID: "bob", entitlement: "member", discovered: time.Unix(1000, 0).UTC()},
			{id: "n-0", principalID: "carol", entitlement: "admin", discovered: time.Unix(1500, 0).UTC()},
		}, false)
		src2 := writeKWaySource(t, ctx, filepath.Join(dir, "src2.c1z"), []kwayGrantSpec{
			{id: "n-1", principalID: "bob", entitlement: "member", discovered: time.Unix(1200, 0).UTC(), needsExpand: true},
			{id: "g-2", principalID: "carol", entitlement: "member", discovered: time.Unix(900, 0).UTC()},
		}, false)
		sources := []SourceFile{
			{Path: src0.path, SyncID: src0.syncID},
			{Path: src1.path, SyncID: src1.syncID},
			{Path: src2.path, SyncID: src2.syncID},
		}
		messages := assertOverlayMatchesKWay(t, ctx, sources, WithOverlaySeenKeyLimit(10))
		if !containsMessage(messages, "pebble overlay merge: bucket resumed at source boundary") {
			t.Fatalf("expected boundary resume, got logs: %v", messages)
		}
	})

	t.Run("statless pre-gate resume", func(t *testing.T) {
		dir := t.TempDir()
		// Source 0 lands exactly on the gate threshold (0.9 x 10 = 9
		// keys, not over the soft limit), so the boundary check does
		// not fire — the pre-source gate resumes the bucket before
		// source 1 is scanned.
		src0 := writeKWaySource(t, ctx, filepath.Join(dir, "src0.c1z"), degGrants("g", 9, time.Unix(2000, 0).UTC()), false)
		src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), append(
			degGrants("h", 4, time.Unix(1500, 0).UTC()),
			kwayGrantSpec{id: "g-3", principalID: "bob", entitlement: "member", discovered: time.Unix(3000, 0).UTC()},
		), false)
		src2 := writeKWaySource(t, ctx, filepath.Join(dir, "src2.c1z"), degGrants("k", 3, time.Unix(1000, 0).UTC()), false)
		sources := []SourceFile{
			{Path: src0.path, SyncID: src0.syncID},
			{Path: src1.path, SyncID: src1.syncID},
			{Path: src2.path, SyncID: src2.syncID},
		}
		messages := assertOverlayMatchesKWay(t, ctx, sources, WithOverlaySeenKeyLimit(10))
		if !containsMessage(messages, "pebble overlay merge: bucket degraded at pre-source gate") {
			t.Fatalf("expected pre-gate resume, got logs: %v", messages)
		}
	})

	t.Run("hard limit restart", func(t *testing.T) {
		dir := t.TempDir()
		// Source 0 alone blows past hard=12 mid-scan: the bucket
		// restarts and the whole merge flows through the blind kway
		// path. Stats must be reset and recounted (parity asserts it).
		src0 := writeKWaySource(t, ctx, filepath.Join(dir, "src0.c1z"), degGrants("g", 30, time.Unix(2000, 0).UTC()), false)
		src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), []kwayGrantSpec{
			{id: "g-0", principalID: "bob", entitlement: "member", discovered: time.Unix(3000, 0).UTC()},
			{id: "x-0", principalID: "carol", entitlement: "admin", discovered: time.Unix(1500, 0).UTC()},
		}, false)
		sources := []SourceFile{
			{Path: src0.path, SyncID: src0.syncID},
			{Path: src1.path, SyncID: src1.syncID},
		}
		messages := assertOverlayMatchesKWay(t, ctx, sources, WithOverlaySeenKeyLimit(10))
		if !containsMessage(messages, "pebble overlay merge: bucket restarted at hard limit") {
			t.Fatalf("expected hard-limit restart, got logs: %v", messages)
		}
	})

	t.Run("stats pre-gate predicts overshoot", func(t *testing.T) {
		dir := t.TempDir()
		// All sources carry stats, so the pre-gate uses the exact
		// prediction: after source 0 admits 8 keys, source 1's count
		// of 6 predicts 14 > hard=12 and the bucket resumes without
		// scanning source 1.
		src0 := writeKWaySource(t, ctx, filepath.Join(dir, "src0.c1z"), degGrants("g", 8, time.Unix(2000, 0).UTC()), false)
		src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), append(
			degGrants("h", 5, time.Unix(1500, 0).UTC()),
			kwayGrantSpec{id: "g-1", principalID: "bob", entitlement: "member", discovered: time.Unix(3000, 0).UTC()},
		), false)
		src2 := writeKWaySource(t, ctx, filepath.Join(dir, "src2.c1z"), degGrants("k", 3, time.Unix(1000, 0).UTC()), false)
		statsFor := func(grants int64) *reader_v2.SyncStats {
			return reader_v2.SyncStats_builder{
				ResourceTypes: 2,
				Resources:     4,
				Entitlements:  2,
				Grants:        grants,
			}.Build()
		}
		sources := []SourceFile{
			{Path: src0.path, SyncID: src0.syncID, Stats: statsFor(8)},
			{Path: src1.path, SyncID: src1.syncID, Stats: statsFor(6)},
			{Path: src2.path, SyncID: src2.syncID, Stats: statsFor(3)},
		}
		messages := assertOverlayMatchesKWay(t, ctx, sources, WithOverlaySeenKeyLimit(10))
		if !containsMessage(messages, "pebble overlay merge: bucket degraded at pre-source gate") {
			t.Fatalf("expected stats pre-gate resume, got logs: %v", messages)
		}
	})

	t.Run("stats lower bound routes to kway up front", func(t *testing.T) {
		dir := t.TempDir()
		// Source 0's single-source count of 30 exceeds hard=12 — a
		// lower bound on distinct keys — so planning routes the grants
		// bucket straight to kway; no degradation events fire.
		src0 := writeKWaySource(t, ctx, filepath.Join(dir, "src0.c1z"), degGrants("g", 30, time.Unix(2000, 0).UTC()), false)
		src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), degGrants("h", 2, time.Unix(1500, 0).UTC()), false)
		statsFor := func(grants int64) *reader_v2.SyncStats {
			return reader_v2.SyncStats_builder{
				ResourceTypes: 2,
				Resources:     4,
				Entitlements:  1,
				Grants:        grants,
			}.Build()
		}
		sources := []SourceFile{
			{Path: src0.path, SyncID: src0.syncID, Stats: statsFor(30)},
			{Path: src1.path, SyncID: src1.syncID, Stats: statsFor(2)},
		}
		messages := assertOverlayMatchesKWay(t, ctx, sources, WithOverlaySeenKeyLimit(10))
		for _, m := range messages {
			switch m {
			case "pebble overlay merge: bucket resumed at source boundary",
				"pebble overlay merge: bucket degraded at pre-source gate",
				"pebble overlay merge: bucket restarted at hard limit":
				t.Fatalf("unexpected degradation event %q for up-front kway routing", m)
			}
		}
	})

	t.Run("multi-chunk restart with backfill", func(t *testing.T) {
		dir := t.TempDir()
		// Five sources at fan-in 2 span three chunks. Sources 0-3
		// share the same 5 keys (the seen set plateaus at 5, below
		// the gate of 9); source 4 carries 20 new keys and blows
		// hard=12 mid-scan in chunk 2. The restart must backfill run
		// files for chunks 0 and 1, which closed before the restart.
		var sources []SourceFile
		for i := 0; i < 4; i++ {
			src := writeKWaySource(t, ctx, filepath.Join(dir, fmt.Sprintf("src%d.c1z", i)),
				degGrants("s", 5, time.Unix(int64(2000-i), 0).UTC()), false)
			sources = append(sources, SourceFile{Path: src.path, SyncID: src.syncID})
		}
		last := writeKWaySource(t, ctx, filepath.Join(dir, "src4.c1z"),
			degGrants("base", 20, time.Unix(500, 0).UTC()), false)
		sources = append(sources, SourceFile{Path: last.path, SyncID: last.syncID})

		messages := assertOverlayMatchesKWay(t, ctx, sources,
			WithOverlaySeenKeyLimit(10), WithOverlayFanIn(2))
		if !containsMessage(messages, "pebble overlay merge: bucket restarted at hard limit") {
			t.Fatalf("expected hard-limit restart, got logs: %v", messages)
		}
	})
}

// TestOverlayResumedReplaceRawEdge pins the winner rule through the
// resumed (map-aware) materialization: an older source carrying a
// strictly newer discovered_at must replace the overlay-admitted
// record (and swap its index entries), while an equal-or-older
// discovered_at must keep it.
func TestOverlayResumedReplaceRawEdge(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// Source 0 (newest) admits 11 grants at ts=2000 and crosses
	// soft=10 → boundary resume. Source 1 (older) is materialized
	// through the frozen map: g-0 has ts=3000 (strictly newer →
	// replace, with a different principal so the index swap is
	// observable) and g-1 has ts=2000 (tie → overlay record kept).
	src0 := writeKWaySource(t, ctx, filepath.Join(dir, "src0.c1z"), degGrants("g", 11, time.Unix(2000, 0).UTC()), false)
	src1 := writeKWaySource(t, ctx, filepath.Join(dir, "src1.c1z"), []kwayGrantSpec{
		{id: "g-0", principalID: "bob", entitlement: "member", discovered: time.Unix(3000, 0).UTC()},
		{id: "g-1", principalID: "bob", entitlement: "member", discovered: time.Unix(2000, 0).UTC()},
	}, false)
	sources := []SourceFile{
		{Path: src0.path, SyncID: src0.syncID},
		{Path: src1.path, SyncID: src1.syncID},
	}

	dest, _ := newEngine(t, "resumed-replace-dest")
	destSyncID := ksuid.New().String()
	logger, capture := newCapturingLogger()
	if _, err := MergeFilesIntoOverlay(ctxzap.ToContext(ctx, logger), dest, sources, destSyncID, t.TempDir(), WithOverlaySeenKeyLimit(10)); err != nil {
		t.Fatalf("MergeFilesIntoOverlay: %v", err)
	}
	if !containsMessage(capture(), "pebble overlay merge: bucket resumed at source boundary") {
		t.Fatalf("expected boundary resume, got logs: %v", capture())
	}

	grants := grantPrincipalMap(t, ctx, dest, destSyncID)
	if got := grants["g-0"]; got != "bob" {
		t.Fatalf("g-0 principal = %q, want bob (strictly newer discovered_at must replace)", got)
	}
	if got := grants["g-1"]; got != "alice" {
		t.Fatalf("g-1 principal = %q, want alice (discovered_at tie keeps the newer source's record)", got)
	}
	// The replaced record's index entries must follow the new value.
	var bobGrants []string
	if err := dest.IterateGrantsByPrincipal(ctx, "user", "bob", func(g *v3.GrantRecord) bool {
		bobGrants = append(bobGrants, g.GetExternalId())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(bobGrants) != 1 || bobGrants[0] != "g-0" {
		t.Fatalf("by_principal(bob) = %v, want [g-0]", bobGrants)
	}
	var aliceForG0 bool
	if err := dest.IterateGrantsByPrincipal(ctx, "user", "alice", func(g *v3.GrantRecord) bool {
		if g.GetExternalId() == "g-0" {
			aliceForG0 = true
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if aliceForG0 {
		t.Fatal("stale by_principal(alice) index entry for replaced grant g-0")
	}
}

// TestMergeStatsResetBucket pins the restart path's stats contract:
// zero the restarted bucket's total and its per-RT grouping without
// touching other buckets.
func TestMergeStatsResetBucket(t *testing.T) {
	a := newMergeStatsAccumulator()
	a.countWinnerTotal(runBucketGrants)
	a.countWinnerTotal(runBucketGrants)
	a.groupGrant([]byte("group"))
	a.groupGrant([]byte("group"))
	a.countWinnerTotal(runBucketResources)
	a.groupResource([]byte("user"))
	a.countWinnerTotal(runBucketEntitlements)

	a.resetBucket(runBucketGrants)
	rec := a.record()
	if rec.GetGrants() != 0 {
		t.Fatalf("grants total after reset = %d, want 0", rec.GetGrants())
	}
	if len(rec.GetGrantsByEntitlementResourceType()) != 0 {
		t.Fatalf("grants grouping after reset = %v, want empty", rec.GetGrantsByEntitlementResourceType())
	}
	if rec.GetResources() != 1 || rec.GetResourcesByResourceType()["user"] != 1 {
		t.Fatalf("resources counts disturbed by grants reset: %v", rec)
	}
	if rec.GetEntitlements() != 1 {
		t.Fatalf("entitlements total disturbed by grants reset: %d", rec.GetEntitlements())
	}
}
