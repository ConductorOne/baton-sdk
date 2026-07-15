package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Halt-point sweep: crash the warm sync at EVERY ordering-sensitive seam
// of the scope lifecycle (via the syncer's testSourceCacheHaltHook),
// resume the same file, and require the resumed output to be semantically
// identical to a cold sync — the FoundationDB-style "halt at interesting
// points" check, scoped to the seams whose ordering bugs would produce
// silent bad data (e.g. a manifest entry committed ahead of its rows
// would poison the NEXT sync's replay while this sync reads clean).
//
// Seams swept (see beginSourceCachePage / finishSourceCachePage):
//   - replay-copied:      after the engine replay copy commits, before
//                         overlay rows / tombstones / manifest.
//   - rows-committed:     after the page's rows committed, before
//                         tombstones.
//   - tombstones-applied: after delta tombstones applied, before the
//                         manifest entry write.
//   - manifest-written:   after the manifest entry committed — the
//                         double-execution seam (the resumed action
//                         re-runs a page whose scope already sealed).

import (
	"fmt"
	"math/rand"
	"path/filepath"
	stdsync "sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// haltOnce arms a one-shot failure at the named stage; every later hook
// call (including after resume, where the hook is still installed) passes.
type haltOnce struct {
	mu    stdsync.Mutex
	stage string
	fired bool
	scope string
}

func (h *haltOnce) hook(stage, scopeKey string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fired || stage != h.stage {
		return nil
	}
	h.fired = true
	h.scope = scopeKey
	return fmt.Errorf("injected halt at %s (scope %q)", stage, scopeKey)
}

func TestSourceCache_HaltPointSweep(t *testing.T) {
	stages := []string{"replay-copied", "rows-committed", "tombstones-applied", "manifest-written"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			runHaltPointScenario(t, stage)
		})
	}
}

func runHaltPointScenario(t *testing.T, stage string) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test randomness

	model := newEquivModel()
	warmConn := newEquivConnector(model, false)
	coldConn := newEquivConnector(model, true)
	var warmClient types.ConnectorClient = warmConn
	var coldClient types.ConnectorClient = coldConn

	extPath := filepath.Join(tmpDir, "external-r0.c1z")
	buildExternalC1z(ctx, t, model, extPath, tmpDir)
	model.extDirty = false
	syncOpts := []SyncOpt{WithExternalResourceC1ZPath(extPath)}

	// Round 0: initial warm-chain sync (a cold fetch — no previous file).
	basePath := filepath.Join(tmpDir, "warm-r0.c1z")
	require.NoError(t, runEquivSync(ctx, t, warmClient, basePath, "", tmpDir, syncOpts...))

	// Round 1 mutations: tombstones, overlays, cold refetches, external
	// churn — the full mixed shape, so every swept seam actually fires.
	model.mutate(1, rng)
	if model.extDirty {
		extPath = filepath.Join(tmpDir, "external-r1.c1z")
		buildExternalC1z(ctx, t, model, extPath, tmpDir)
		syncOpts = []SyncOpt{WithExternalResourceC1ZPath(extPath)}
	}

	// Warm sync, halted once at the stage under test, then resumed.
	halt := &haltOnce{stage: stage}
	warmPath := filepath.Join(tmpDir, "warm-halt.c1z")
	hookOpt := func(s *syncer) { s.testSourceCacheHaltHook = halt.hook }
	haltErr := runEquivSync(ctx, t, warmClient, warmPath, basePath, tmpDir, append(syncOpts, hookOpt)...)
	require.Error(t, haltErr, "the armed halt must crash the warm sync")
	require.Contains(t, haltErr.Error(), "injected halt at "+stage)
	require.NoError(t, runEquivSync(ctx, t, warmClient, warmPath, basePath, tmpDir, append(syncOpts, hookOpt)...),
		"resume after halt at %s (scope %q) must complete", stage, halt.scope)

	// Cold reference for the same model state.
	coldPath := filepath.Join(tmpDir, "cold-r1.c1z")
	require.NoError(t, runEquivSync(ctx, t, coldClient, coldPath, "", tmpDir, syncOpts...))

	coldSnap := snapshotC1z(ctx, t, coldPath)
	warmSnap := snapshotC1z(ctx, t, warmPath)
	require.Equal(t, 1, warmSnap.syncRuns,
		"halt at %s: the crashed warm sync must RESUME (one sync run), not restart", stage)
	assertColdArtifacts(t, 1, model, coldSnap)
	assertNoStaleResources(t, 1, model, "warm", warmSnap)
	requireSnapshotsEqual(t, 1, coldSnap, warmSnap)
}
