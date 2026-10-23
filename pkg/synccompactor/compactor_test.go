package synccompactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// requireTmpDirCleanedUp verifies that the temp directory has no compactor-related temp files remaining.
// The compactor creates subdirectories prefixed with "baton-sync-compactor-" which should be removed after cleanup.
func requireTmpDirCleanedUp(t *testing.T, tmpDir string) {
	t.Helper()
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, entries, 0, "tmp directory %s should be empty after cleanup", tmpDir)
}

func makeEmptySync(t *testing.T, ctx context.Context, inputSyncsDir string, opts []dotc1z.C1ZOption, syncType connectorstore.SyncType) *CompactableSync {
	// Create the first sync file
	randomID := uuid.New().String()
	syncPath := filepath.Join(inputSyncsDir, fmt.Sprintf("sync-%s.c1z", randomID))
	sync, err := dotc1z.NewStore(ctx, syncPath, opts...)
	require.NoError(t, err)

	// Start a new sync
	syncID, err := sync.StartNewSync(ctx, syncType, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	// Create resource types
	userResourceTypeID := "user"
	groupResourceTypeID := "group"
	roleResourceTypeID := "role"

	err = sync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          userResourceTypeID,
		DisplayName: "User",
	}.Build())
	require.NoError(t, err)

	err = sync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          groupResourceTypeID,
		DisplayName: "Group",
	}.Build())
	require.NoError(t, err)

	err = sync.PutResourceTypes(ctx, v2.ResourceType_builder{
		Id:          roleResourceTypeID,
		DisplayName: "Role",
	}.Build())
	require.NoError(t, err)

	// End the first sync
	err = sync.EndSync(ctx)
	require.NoError(t, err)
	err = sync.Close(ctx)
	require.NoError(t, err)

	return &CompactableSync{
		FilePath: syncPath,
		SyncID:   syncID,
	}
}

// Test cases for sync type union logic.
func TestSyncTypeUnion(t *testing.T) {
	ctx := t.Context()

	inputSyncsDir, err := os.MkdirTemp("", "compactor-sync-type-test")
	require.NoError(t, err)
	defer os.RemoveAll(inputSyncsDir)

	outputDir, err := os.MkdirTemp("", "compactor-output")
	require.NoError(t, err)
	defer os.RemoveAll(outputDir)

	// Create temporary directory for intermediate files.
	tmpDir, err := os.MkdirTemp("", "compactor-tmp")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	createCompactor := func(compactableSyncs []*CompactableSync) (*Compactor, func() error, error) {
		compactor, cleanup, err := NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(tmpDir))
		require.NoError(t, err)
		cleanupAndValidate := func() error {
			err := cleanup()
			require.NoError(t, err)
			requireTmpDirCleanedUp(t, tmpDir)
			return nil
		}
		return compactor, cleanupAndValidate, err
	}

	// Run all test cases.
	for _, tc := range allSyncTypeTestCases {
		t.Run(tc.name, func(t *testing.T) {
			runSyncTypeTest(t, ctx, inputSyncsDir, createCompactor, tc.input, tc.expected)
		})
	}
}

// getAllSyncTypeTestCases returns comprehensive test cases for sync type union logic.
var allSyncTypeTestCases = []syncTypeTestCase{
	// Two-sync combinations
	{
		name:     "Full + Full = Full",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypeFull},
		expected: connectorstore.SyncTypeFull,
	},
	{
		name:     "Full + Partial = Full",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypePartial},
		expected: connectorstore.SyncTypeFull,
	},
	{
		name:     "Partial + Full = Full",
		input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeFull},
		expected: connectorstore.SyncTypeFull,
	},
	{
		name:     "Full + ResourcesOnly = Full",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypeResourcesOnly},
		expected: connectorstore.SyncTypeFull,
	},
	{
		name:     "ResourcesOnly + Full = Full",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypeFull},
		expected: connectorstore.SyncTypeFull,
	},
	{
		name:     "ResourcesOnly + ResourcesOnly = ResourcesOnly",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypeResourcesOnly},
		expected: connectorstore.SyncTypeResourcesOnly,
	},
	{
		name:     "ResourcesOnly + Partial = ResourcesOnly",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
		expected: connectorstore.SyncTypeResourcesOnly,
	},
	{
		name:     "Partial + ResourcesOnly = ResourcesOnly",
		input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeResourcesOnly},
		expected: connectorstore.SyncTypeResourcesOnly,
	},
	{
		name:     "Partial + Partial = Partial",
		input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypePartial},
		expected: connectorstore.SyncTypePartial,
	},

	// Three-sync combinations
	{
		name:     "Full + Partial + ResourcesOnly = Full",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeFull, connectorstore.SyncTypePartial, connectorstore.SyncTypeResourcesOnly},
		expected: connectorstore.SyncTypeFull,
	},
	{
		name:     "Partial + ResourcesOnly + Partial = ResourcesOnly",
		input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
		expected: connectorstore.SyncTypeResourcesOnly,
	},
	{
		name:     "Partial + Partial + Partial = Partial",
		input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypePartial, connectorstore.SyncTypePartial},
		expected: connectorstore.SyncTypePartial,
	},

	// Four-sync combinations
	{
		name:     "ResourcesOnly + Partial + Partial + Partial = ResourcesOnly",
		input:    []connectorstore.SyncType{connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial, connectorstore.SyncTypePartial, connectorstore.SyncTypePartial},
		expected: connectorstore.SyncTypeResourcesOnly,
	},
	{
		name:     "Partial + Full + ResourcesOnly + Partial = Full",
		input:    []connectorstore.SyncType{connectorstore.SyncTypePartial, connectorstore.SyncTypeFull, connectorstore.SyncTypeResourcesOnly, connectorstore.SyncTypePartial},
		expected: connectorstore.SyncTypeFull,
	},
}

type syncTypeTestCase struct {
	name     string
	input    []connectorstore.SyncType
	expected connectorstore.SyncType
}

func runSyncTypeTest(
	t *testing.T,
	ctx context.Context,
	inputSyncsDir string,
	createCompactor func([]*CompactableSync) (*Compactor, func() error, error),
	types []connectorstore.SyncType,
	expectedSyncType connectorstore.SyncType,
) {
	opts := []dotc1z.C1ZOption{
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
	}

	// Create empty syncs for each sync type.
	var compactableSyncs []*CompactableSync
	for _, syncType := range types {
		compactableSync := makeEmptySync(t, ctx, inputSyncsDir, opts, syncType)
		compactableSyncs = append(compactableSyncs, compactableSync)
	}

	// Create compactor using the callback.
	compactor, cleanup, err := createCompactor(compactableSyncs)
	require.NoError(t, err)
	defer func() {
		err := cleanup()
		require.NoError(t, err)
	}()

	// Run the compaction.
	compactedSync, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, compactedSync)

	// Open the compacted file.
	compactedFile, err := dotc1z.NewStore(ctx, compactedSync.FilePath, append(opts, dotc1z.WithReadOnly(true))...)
	require.NoError(t, err)
	defer compactedFile.Close(ctx)

	syncRuns := listSyncRuns(t, ctx, compactedFile)
	require.Equal(t, 1, len(syncRuns))
	require.Equal(t, expectedSyncType, syncRuns[0].Type)
}

type syncRunLister interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
}

func listSyncRuns(t *testing.T, ctx context.Context, store c1zstore.Store) []*c1zstore.SyncRun {
	t.Helper()
	lister, ok := store.(syncRunLister)
	require.True(t, ok, "store %T does not list sync runs", store)
	runs, _, err := lister.ListSyncRuns(ctx, "", 10)
	require.NoError(t, err)
	return runs
}

// The compacting two partial syncs should result in a partial sync.
func TestCompactorPartialBaseStaysPartial(t *testing.T) {
	ctx := t.Context()

	outputDir, err := os.MkdirTemp("", "compactor-output")
	require.NoError(t, err)
	defer os.RemoveAll(outputDir)

	// Create temporary directory for intermediate files.
	tmpDir, err := os.MkdirTemp("", "compactor-tmp")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	baseFile := filepath.Join(outputDir, "base.c1z")
	appliedFile := filepath.Join(outputDir, "applied.c1z")

	opts := []dotc1z.C1ZOption{
		dotc1z.WithTmpDir(tmpDir),
	}

	baseDB, err := dotc1z.NewStore(ctx, baseFile, opts...)
	require.NoError(t, err)

	baseSyncID, err := baseDB.StartNewSync(ctx, connectorstore.SyncTypePartial, "some-parent")
	require.NoError(t, err)
	require.NotEmpty(t, baseSyncID)

	err = baseDB.EndSync(ctx)
	require.NoError(t, err)
	err = baseDB.Close(ctx)
	require.NoError(t, err)

	baseCompactableSync := &CompactableSync{
		FilePath: baseFile,
		SyncID:   baseSyncID,
	}

	appliedDB, err := dotc1z.NewStore(ctx, appliedFile, opts...)
	require.NoError(t, err)

	appliedSyncID, err := appliedDB.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NotEmpty(t, appliedSyncID)

	err = appliedDB.EndSync(ctx)
	require.NoError(t, err)
	err = appliedDB.Close(ctx)
	require.NoError(t, err)

	appliedCompactableSync := &CompactableSync{
		FilePath: appliedFile,
		SyncID:   appliedSyncID,
	}

	compactableSyncs := []*CompactableSync{baseCompactableSync, appliedCompactableSync}
	compactor, cleanup, err := NewCompactor(
		ctx,
		outputDir,
		compactableSyncs,
		WithTmpDir(tmpDir))
	require.NoError(t, err)
	defer func() {
		err := cleanup()
		require.NoError(t, err)
		requireTmpDirCleanedUp(t, tmpDir)
	}()

	compactedSync, err := compactor.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, compactedSync)

	compactedFile, err := dotc1z.NewStore(ctx, compactedSync.FilePath, append(opts, dotc1z.WithReadOnly(true))...)
	require.NoError(t, err)
	defer compactedFile.Close(ctx)

	// The compacted file should have one partial sync.
	syncRuns := listSyncRuns(t, ctx, compactedFile)
	require.Len(t, syncRuns, 1)
	require.Equal(t, connectorstore.SyncTypePartial, syncRuns[0].Type)
	require.Equal(t, compactedSync.SyncID, syncRuns[0].ID)
	require.NotNil(t, syncRuns[0].EndedAt)
}

// TestCompactorRunDurationExpired locks in the user-visible contract that when c.runDuration
// expires (parent ctx still alive), Compact returns the clean "compaction run duration has expired"
// error wrapping context.DeadlineExceeded. With a 1ns budget, the pre-flight select check fires
// first; the in-loop branch added by the runCtx-in-loop fix uses the same message so this also
// guards regression of that branch's error format.
func TestCompactorRunDurationExpired(t *testing.T) {
	ctx := t.Context()
	inputSyncsDir := t.TempDir()
	outputDir := t.TempDir()
	tmpDir := t.TempDir()

	opts := []dotc1z.C1ZOption{
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
	}
	syncs := []*CompactableSync{
		makeEmptySync(t, ctx, inputSyncsDir, opts, connectorstore.SyncTypeFull),
		makeEmptySync(t, ctx, inputSyncsDir, opts, connectorstore.SyncTypeFull),
	}

	compactor, cleanup, err := NewCompactor(ctx, outputDir, syncs,
		WithTmpDir(tmpDir),
		WithRunDuration(1*time.Nanosecond),
	)
	require.NoError(t, err)
	defer func() {
		_ = cleanup()
	}()

	result, err := compactor.Compact(ctx)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorContains(t, err, "compaction run duration has expired")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestCompactorParentCtxCancelDoesNotMaskAsRunDuration locks in the disambiguation
// between "runCtx fired due to c.runDuration" and "parent ctx was cancelled".
// A parent-cancelled runCtx has cause=context.Canceled (not DeadlineExceeded), so the
// pre-flight branch falls through to default and returns the raw cause — and the in-loop
// branch's ctx.Err() == nil guard rejects the clean-exit path if parent ctx is also done.
// The error must NOT be misreported as runDuration expiry.
func TestCompactorParentCtxCancelDoesNotMaskAsRunDuration(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	inputSyncsDir := t.TempDir()
	outputDir := t.TempDir()
	tmpDir := t.TempDir()

	opts := []dotc1z.C1ZOption{
		dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
	}
	syncs := []*CompactableSync{
		makeEmptySync(t, ctx, inputSyncsDir, opts, connectorstore.SyncTypeFull),
		makeEmptySync(t, ctx, inputSyncsDir, opts, connectorstore.SyncTypeFull),
	}

	compactor, cleanup, err := NewCompactor(ctx, outputDir, syncs,
		WithTmpDir(tmpDir),
		// runDuration far longer than the test would take, so it never fires.
		WithRunDuration(10*time.Minute),
	)
	require.NoError(t, err)
	defer func() {
		_ = cleanup()
	}()

	// Cancel parent before Compact starts; pre-flight observes ctx.Done() with
	// cause=context.Canceled and surfaces it as the raw error rather than the
	// runDuration message.
	cancel()

	_, err = compactor.Compact(ctx)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "compaction run duration has expired",
		"parent ctx cancel must not be misreported as runDuration expiry")
	require.ErrorIs(t, err, context.Canceled)
}
