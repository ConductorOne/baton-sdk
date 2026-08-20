package dotc1z

import (
	"context"
	"path/filepath"
	"strings"
	stdsync "sync"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// TestToPebbleReportsDiscardedSyncs pins that conversion accounts for the syncs
// it drops. A Pebble c1z holds one sync, so everything else in the source is
// lost; ConvertStats is where a caller learns what that was.
func TestToPebbleReportsDiscardedSyncs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	oldFullID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	// An unfinished sync and a partial sync, both of which the conversion
	// drops and neither of which is reproducible from the converted file.
	abandonedID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.CheckpointSync(ctx, "abandoned-token"))
	require.NoError(t, src.EndSync(ctx))
	_, err = src.db.ExecContext(ctx,
		`UPDATE `+syncRuns.Name()+` SET ended_at = NULL WHERE sync_id = ?`, abandonedID)
	require.NoError(t, err)

	partialID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, src.EndSync(ctx))

	newFullID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	stats, err := src.ToPebble(ctx, filepath.Join(dir, "out.c1z"), newFullID)
	require.NoError(t, err)
	require.Equal(t, newFullID, stats.SourceSyncID)

	byID := make(map[string]DiscardedSync, len(stats.DiscardedSyncs))
	for _, d := range stats.DiscardedSyncs {
		byID[d.ID] = d
	}
	require.Len(t, byID, 3, "every sync but the selected one must be reported")
	require.NotContains(t, byID, newFullID, "the converted sync is not discarded")

	require.Equal(t, connectorstore.SyncTypeFull, byID[oldFullID].Type)
	require.NotNil(t, byID[oldFullID].EndedAt, "finished sync must report its ended_at")
	require.NotNil(t, byID[oldFullID].StartedAt)

	require.Nil(t, byID[abandonedID].EndedAt, "unfinished sync must report a nil ended_at")

	require.Equal(t, connectorstore.SyncTypePartial, byID[partialID].Type,
		"partial syncs are dropped too and must be reported")
}

// TestToPebbleDiscardedSyncTimestampsAreAbsoluteInstants pins that the reported
// timestamps mean the same thing as the ones written into the converted record.
// They come out of the same zone-less columns, and the WARN line built from them
// is the only surviving account of what an overwritten artifact held, so a
// caller comparing them against time.Now() must not be off by the host's offset.
//
// Runs in the non-UTC child of TestConversionTimestampsUnderNonUTCZone, since a
// UTC host cannot tell the two readings apart.
func TestToPebbleDiscardedSyncTimestampsAreAbsoluteInstants(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	droppedID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	// Stamped the way the writers do: local wall clock, no zone.
	startedAt := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	endedAt := startedAt.Add(time.Minute)
	_, err = src.db.ExecContext(ctx,
		`UPDATE `+syncRuns.Name()+` SET started_at = ?, ended_at = ? WHERE sync_id = ?`,
		startedAt.Format(sqliteTimeFormat), endedAt.Format(sqliteTimeFormat), droppedID,
	)
	require.NoError(t, err)

	keptID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	stats, err := src.ToPebble(ctx, filepath.Join(dir, "out.c1z"), keptID)
	require.NoError(t, err)
	require.Len(t, stats.DiscardedSyncs, 1)

	discarded := stats.DiscardedSyncs[0]
	require.Equal(t, droppedID, discarded.ID)
	require.NotNil(t, discarded.StartedAt)
	require.WithinDuration(t, startedAt, *discarded.StartedAt, time.Second,
		"reported started_at must be the instant the dropped sync started")
	require.NotNil(t, discarded.EndedAt)
	require.WithinDuration(t, endedAt, *discarded.EndedAt, time.Second,
		"reported ended_at must be the instant the dropped sync ended")
}

// TestToPebbleReportsNoDiscardedSyncsForSoleSync keeps the reporting honest in
// the common case: a source holding only the converted sync loses nothing.
func TestToPebbleReportsNoDiscardedSyncsForSoleSync(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	src, err := NewC1ZFile(ctx, filepath.Join(dir, "src.c1z"), WithTmpDir(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close(ctx)) }()

	_, err = src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))

	stats, err := src.ToPebble(ctx, filepath.Join(dir, "out.c1z"), "")
	require.NoError(t, err)
	require.Empty(t, stats.DiscardedSyncs)
}

// TestConvertOpenWarnsAboutDiscardedSyncs pins the operator-visible half. The
// convert-open rewrite replaces the v1 file, so the WARN line is the only
// surviving evidence of the syncs the artifact used to hold — deleting it
// changes no other test.
func TestConvertOpenWarnsAboutDiscardedSyncs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c1zPath := filepath.Join(dir, "multi-sync.c1z")

	src, err := NewC1ZFile(ctx, c1zPath, WithTmpDir(dir), WithEngine(c1zstore.EngineSQLite))
	require.NoError(t, err)
	droppedID, err := src.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, src.EndSync(ctx))
	keptID, err := src.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NoError(t, src.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, src.EndSync(ctx))
	require.NoError(t, src.Close(ctx))

	core, entries := newConvertCaptureCore()
	lctx := ctxzap.ToContext(ctx, zap.New(core))

	store, err := NewStore(lctx, c1zPath, WithEngine(c1zstore.EnginePebble), WithTmpDir(dir))
	require.NoError(t, err)
	require.NoError(t, store.Close(ctx))

	warn := findConvertEntry(entries(), zapcore.WarnLevel, "convert-open: discarded syncs")
	require.NotNil(t, warn, "convert-open must warn about the syncs it dropped")
	fields := convertEntryFields(warn)
	require.Equal(t, keptID, fields["kept_sync_id"])
	require.Equal(t, int64(1), fields["discarded_sync_count"])

	descriptors, ok := fields["discarded_syncs"].([]any)
	require.True(t, ok, "discarded_syncs must be logged as an array, got %T", fields["discarded_syncs"])
	require.Len(t, descriptors, 1)
	descriptor, ok := descriptors[0].(string)
	require.True(t, ok)
	require.Contains(t, descriptor, droppedID, "the dropped sync id must be in the log line")
	require.Contains(t, descriptor, "type=full")
	require.Contains(t, descriptor, "finished=true")
}

// capturedConvertEntry is one log line seen by convertCaptureCore.
type capturedConvertEntry struct {
	level   zapcore.Level
	message string
	fields  []zapcore.Field
}

// convertCaptureCore is a minimal zapcore.Core recording every entry
// (zaptest/observer is not vendored).
type convertCaptureCore struct {
	zapcore.LevelEnabler
	mu      *stdsync.Mutex
	entries *[]capturedConvertEntry
	with    []zapcore.Field
}

func newConvertCaptureCore() (*convertCaptureCore, func() []capturedConvertEntry) {
	var mu stdsync.Mutex
	entries := &[]capturedConvertEntry{}
	core := &convertCaptureCore{LevelEnabler: zapcore.DebugLevel, mu: &mu, entries: entries}
	return core, func() []capturedConvertEntry {
		mu.Lock()
		defer mu.Unlock()
		out := make([]capturedConvertEntry, len(*entries))
		copy(out, *entries)
		return out
	}
}

func (c *convertCaptureCore) With(fields []zapcore.Field) zapcore.Core {
	return &convertCaptureCore{
		LevelEnabler: c.LevelEnabler,
		mu:           c.mu,
		entries:      c.entries,
		with:         append(append([]zapcore.Field{}, c.with...), fields...),
	}
}

func (c *convertCaptureCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

func (c *convertCaptureCore) Write(e zapcore.Entry, fields []zapcore.Field) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	*c.entries = append(*c.entries, capturedConvertEntry{
		level:   e.Level,
		message: e.Message,
		fields:  append(append([]zapcore.Field{}, c.with...), fields...),
	})
	return nil
}

func (c *convertCaptureCore) Sync() error { return nil }

func findConvertEntry(entries []capturedConvertEntry, level zapcore.Level, msgSubstr string) *capturedConvertEntry {
	for i := range entries {
		if entries[i].level == level && strings.Contains(entries[i].message, msgSubstr) {
			return &entries[i]
		}
	}
	return nil
}

// convertEntryFields flattens an entry's fields to key -> value. It encodes
// through zap's own MapObjectEncoder rather than reading zapcore.Field members
// directly, so array fields (zap.Strings) come back as their element slices
// instead of an unexported wrapper type.
func convertEntryFields(e *capturedConvertEntry) map[string]any {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range e.fields {
		f.AddTo(enc)
	}
	return enc.Fields
}
