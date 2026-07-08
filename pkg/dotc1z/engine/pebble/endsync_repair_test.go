package pebble

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// capturingCore/newCapturingLogger let a test observe the zap messages
// EndSync's finalize logs, without hooking any production code —
// used here to prove a SECOND EndSync on an already-digested,
// rebound sync takes the targeted repair path (RepairMissingGrantDigests)
// rather than a full from-scratch rescan (BuildGrantDigests).
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

// TestEndSyncSecondCallTakesTargetedRepairPath is the core pin for
// wiring RepairMissingGrantDigests into Adapter.endSyncFinalize: a
// SECOND EndSync on a sync that was already fully digested and is
// REBOUND (SetCurrentSync, not StartNewSync — the shape grant
// expansion's own follow-up sync produces) must take the targeted
// repair path for the one entitlement a post-seal write touched,
// leaving every other entitlement's digest nodes byte-identical to
// what the first EndSync built — not a full rescan of every grant in
// the file.
func TestEndSyncSecondCallTakesTargetedRepairPath(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	a := NewAdapter(e)

	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	putEnt(t, e, ctx, "ent-A")
	putEnt(t, e, ctx, "ent-B")
	grantsA := make([]*v3.GrantRecord, 0, 20)
	for range 20 {
		id := ksuid.New().String()
		grantsA = append(grantsA, makeGrant("", id, "ent-A", id))
	}
	if err := e.PutGrantRecords(ctx, grantsA...); err != nil {
		t.Fatalf("PutGrantRecords(ent-A): %v", err)
	}
	if err := e.PutGrantRecords(ctx, makeGrant("", "gb1", "ent-B", "bob")); err != nil {
		t.Fatalf("PutGrantRecords(ent-B): %v", err)
	}

	// First EndSync: fresh sync, grantDigestsPresent starts false, so
	// RepairMissingGrantDigests must fall back to the full build —
	// exactly today's behavior, no regression.
	if err := a.EndSync(ctx); err != nil {
		t.Fatalf("first EndSync: %v", err)
	}
	afterFirstSeal := dumpDigestNodes(t, e)
	if len(afterFirstSeal) == 0 {
		t.Fatal("expected digest nodes after first EndSync")
	}

	// Rebind (NOT StartNewSync — that would excise the digest keyspace
	// and defeat the whole point) and write one more grant under ent-A
	// only. This is the shape grant expansion's follow-up sync takes:
	// resume an already-sealed sync, write some grants, end it again.
	if err := a.SetCurrentSync(ctx, syncID); err != nil {
		t.Fatalf("SetCurrentSync: %v", err)
	}
	if err := e.PutGrantRecord(ctx, makeGrant("", "ga-extra", "ent-A", "extra-user")); err != nil {
		t.Fatalf("PutGrantRecord (post-seal): %v", err)
	}

	// Sanity: only ent-A (and the global root) should be invalidated by
	// that write.
	if _, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-A")); err != nil || ok {
		t.Fatalf("ent-A root after post-seal write: ok=%v err=%v, want missing", ok, err)
	}
	rootB, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-B"))
	if err != nil || !ok {
		t.Fatalf("ent-B root after post-seal write: ok=%v err=%v, want intact", ok, err)
	}
	if rootB.Count != 1 {
		t.Fatalf("ent-B root count = %d, want 1 (untouched)", rootB.Count)
	}

	logger, capture := newCapturingLogger()
	endSyncCtx := ctxzap.ToContext(ctx, logger)
	if err := a.EndSync(endSyncCtx); err != nil {
		t.Fatalf("second EndSync: %v", err)
	}

	var sawTargetedRepair, sawFullBuild bool
	for _, msg := range capture() {
		switch msg {
		case "grant digest repair: rebuilt missing entitlement partitions":
			sawTargetedRepair = true
		case "grant digest build complete":
			sawFullBuild = true
		}
	}
	if !sawTargetedRepair {
		t.Error("expected the second EndSync to log a targeted repair")
	}
	if sawFullBuild {
		t.Error("second EndSync ran a full from-scratch rebuild instead of the targeted repair")
	}

	// ent-B's digest nodes must be byte-identical to what the FIRST
	// seal produced — proof the targeted repair never touched it.
	afterSecondSeal := dumpDigestNodes(t, e)
	entBPrefix := encodeDigestPartitionPrefix(grantDigestSpec.indexID, testEntPartition("ent-B"))
	for k, v := range afterFirstSeal {
		if !bytesHasPrefix(k, entBPrefix) {
			continue
		}
		got, ok := afterSecondSeal[k]
		if !ok {
			t.Fatalf("ent-B node %x missing after targeted repair", k)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("ent-B node %x changed by targeted repair: got %x, want %x", k, got, v)
		}
	}

	// ent-A must be correctly repaired (21 grants now), and the global
	// root must be recomputed and correct.
	rootA, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity("ent-A"))
	if err != nil || !ok {
		t.Fatalf("ent-A root after repair: ok=%v err=%v", ok, err)
	}
	if rootA.Count != 21 {
		t.Fatalf("ent-A root count = %d, want 21", rootA.Count)
	}
	globalRoot, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	if err != nil || !ok {
		t.Fatalf("global root after repair: ok=%v err=%v", ok, err)
	}
	if globalRoot.Count != 22 {
		t.Fatalf("global root count = %d, want 22 (21 ent-A + 1 ent-B)", globalRoot.Count)
	}
}

func bytesHasPrefix(k string, prefix []byte) bool {
	return len(k) >= len(prefix) && k[:len(prefix)] == string(prefix)
}
