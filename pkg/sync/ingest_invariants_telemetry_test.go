package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Telemetry pins for the invariant pass's log-only verdicts
// (mutation-sweep findings M10/M6): in default mode the aggregated
// warning IS the entire production verdict for dangling references,
// and the skipped-invariants line is the only evidence a store was
// never referentially validated — deleting either changed no test
// until these.

import (
	"context"
	stdsync "sync"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// capturedEntry is one log line seen by captureCore.
type capturedEntry struct {
	level   zapcore.Level
	message string
	fields  []zapcore.Field
}

// captureCore is a minimal zapcore.Core recording every entry
// (zaptest/observer is not vendored; this is its 30-line subset).
type captureCore struct {
	zapcore.LevelEnabler
	mu      *stdsync.Mutex
	entries *[]capturedEntry
	with    []zapcore.Field
}

func newCaptureCore() (*captureCore, func() []capturedEntry) {
	var mu stdsync.Mutex
	entries := &[]capturedEntry{}
	core := &captureCore{LevelEnabler: zapcore.DebugLevel, mu: &mu, entries: entries}
	return core, func() []capturedEntry {
		mu.Lock()
		defer mu.Unlock()
		out := make([]capturedEntry, len(*entries))
		copy(out, *entries)
		return out
	}
}

func (c *captureCore) With(fields []zapcore.Field) zapcore.Core {
	return &captureCore{LevelEnabler: c.LevelEnabler, mu: c.mu, entries: c.entries, with: append(append([]zapcore.Field{}, c.with...), fields...)}
}

func (c *captureCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c *captureCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*c.entries = append(*c.entries, capturedEntry{level: e.Level, message: e.Message, fields: append(append([]zapcore.Field{}, c.with...), fields...)})
	return nil
}

func (c *captureCore) Sync() error { return nil }

func findEntry(entries []capturedEntry, level zapcore.Level, msgSubstr string) *capturedEntry {
	for i := range entries {
		if entries[i].level == level && contains(entries[i].message, msgSubstr) {
			return &entries[i]
		}
	}
	return nil
}

func contains(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func fieldInt(t *testing.T, e *capturedEntry, key string) int64 {
	t.Helper()
	for _, f := range e.fields {
		if f.Key == key {
			return f.Integer
		}
	}
	t.Fatalf("log entry %q has no field %q", e.message, key)
	return 0
}

// TestIngestInvariantI9WarnTelemetry pins the default-mode aggregated
// warning: it is the production verdict for dangling principals, so it
// must fire with the right counts and attribution — not just "no error".
func TestIngestInvariantI9WarnTelemetry(t *testing.T) {
	ctx := context.Background()
	store, pass := newPebbleInvariantStore(ctx, t)

	repo, err := rs.NewResource("Repo r1", invariantRepoRT, "r1")
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(repo, "admin")
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	ghost := v2.ResourceId_builder{ResourceType: "user", Resource: "ghost"}.Build()

	require.NoError(t, store.PutResourceTypes(ctx, invariantRepoRT, userRT))
	require.NoError(t, store.PutResources(ctx, repo))
	require.NoError(t, store.PutEntitlements(ctx, ent))
	require.NoError(t, store.PutGrants(ctx, grant.NewGrant(repo, "admin", ghost)))

	core, entries := newCaptureCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))

	pass.p.FailFast = false
	require.NoError(t, pass.checkGrantPrincipalReferences(lctx))

	warn := findEntry(entries(), zapcore.WarnLevel, "ingest invariant I9")
	require.NotNil(t, warn, "the aggregated I9 warning is the default-mode verdict and must fire")
	require.EqualValues(t, 1, fieldInt(t, warn, "dangling_principals"))
	require.EqualValues(t, 1, fieldInt(t, warn, "refs_under_synced_types"),
		"the ghost principal's type row exists, so attribution is the synced-type flavor")
	require.EqualValues(t, 0, fieldInt(t, warn, "refs_into_unsynced_types"))
}

// TestIngestInvariantI5CorruptOnlyWarnTelemetry pins the corrupt-only
// aggregate on the softened (pre-sealed artifact) pass: the warning is
// the ENTIRE production verdict for artifact damage there, so it must
// fire with the corrupt count even when no group conflicts exist, and
// its copy must attribute damage — not claim the merge "manufactures
// these by design", which is only true of group conflicts.
func TestIngestInvariantI5CorruptOnlyWarnTelemetry(t *testing.T) {
	ctx := context.Background()
	store, syncID := newInvariantTestStore(ctx, t)

	r, err := rs.NewResource("user1", userResourceType, "user1")
	require.NoError(t, err)
	ent := et.NewPermissionEntitlement(r, "viewer")
	ent.SetAnnotations([]*anypb.Any{{
		TypeUrl: "type.googleapis.com/c1.connector.v2.EntitlementExclusionGroup",
		Value:   []byte{0xFF, 0xFF}, // undecodable
	}})
	require.NoError(t, store.PutEntitlements(ctx, ent))

	core, entries := newCaptureCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))

	require.NoError(t, RunIngestInvariants(lctx, store, IngestInvariantsPolicy{
		ActiveSyncID:    syncID,
		SyncType:        connectorstore.SyncTypeFull,
		CompactionMerge: true,
	}))

	warn := findEntry(entries(), zapcore.WarnLevel, "corrupt exclusion-group annotations")
	require.NotNil(t, warn, "the corrupt-only aggregate is the production verdict on the softened pass and must fire")
	require.EqualValues(t, 1, fieldInt(t, warn, "corrupt_annotations"))
	require.EqualValues(t, 0, fieldInt(t, warn, "conflict_groups"))
	require.Nil(t, findEntry(entries(), zapcore.WarnLevel, "manufacture these by design"),
		"artifact damage must not be attributed to merge manufacture")
}

// TestIngestInvariantSkipLogTelemetry pins the degradation-visibility
// line: a store without the inspection surface (SQLite) must say so —
// a sealed artifact that was never referentially validated must not be
// indistinguishable from one that passed.
func TestIngestInvariantSkipLogTelemetry(t *testing.T) {
	ctx := context.Background()
	store, syncID := newInvariantTestStore(ctx, t) // SQLite-backed

	core, entries := newCaptureCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))

	require.NoError(t, RunIngestInvariants(lctx, store, IngestInvariantsPolicy{
		ActiveSyncID: syncID,
		SyncType:     connectorstore.SyncTypeFull,
	}))

	skip := findEntry(entries(), zapcore.InfoLevel, "referential invariants were not evaluated")
	require.NotNil(t, skip, "the SQLite downgrade must be visible in the log stream")
	var hasField bool
	for _, f := range skip.fields {
		if f.Key == "skipped_invariants" {
			hasField = true
		}
	}
	require.True(t, hasField, "the skip line must name the invariants that did not run")
	require.Nil(t, findEntry(entries(), zapcore.WarnLevel, "referential invariants were not evaluated"),
		"a store with no type-scoped exposure keeps the informational level")
}

// TestIngestInvariantSkipLogEscalatesForTypeScopedStores pins the
// exposure-dependent level of the degradation line: the annotation
// registry promises I7/I8 for the type-scoped listing annotations, and
// on an engine without the inspection surface those exact checks are
// the ones skipped — a connector relying on the promised guard must see
// a WARNING, not an info line indistinguishable from benign downgrade.
func TestIngestInvariantSkipLogEscalatesForTypeScopedStores(t *testing.T) {
	ctx := context.Background()
	store, syncID := newInvariantTestStore(ctx, t) // SQLite-backed

	scoped := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Annotations: annotations.New(&v2.TypeScopedGrants{}),
	}.Build()
	require.NoError(t, store.PutResourceTypes(ctx, scoped))

	core, entries := newCaptureCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))

	require.NoError(t, RunIngestInvariants(lctx, store, IngestInvariantsPolicy{
		ActiveSyncID: syncID,
		SyncType:     connectorstore.SyncTypeFull,
	}))

	warn := findEntry(entries(), zapcore.WarnLevel, "TYPE-SCOPED listing annotations")
	require.NotNil(t, warn,
		"skipping I7/I8 on a store that carries the annotations they were promised for must escalate to a warning")
	var skipped bool
	for _, f := range warn.fields {
		if f.Key == "skipped_invariants" {
			skipped = true
		}
	}
	require.True(t, skipped, "the escalated line must still name the invariants that did not run")
	require.Nil(t, findEntry(entries(), zapcore.InfoLevel, "referential invariants were not evaluated"),
		"the verdict is one line at one level, not both")
}
