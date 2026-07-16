package c1api

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	stdsync "sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"github.com/conductorone/baton-sdk/pkg/session"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

type fullSyncHelpers interface {
	ConnectorClient() types.ConnectorClient
	Upload(ctx context.Context, r io.ReadSeeker) error
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
	HeartbeatTask(ctx context.Context, annos annotations.Annotations) (context.Context, error)
	TempDir() string
}

type fullSyncTaskHandler struct {
	task                                *v1.Task
	helpers                             fullSyncHelpers
	skipFullSync                        bool
	externalResourceC1ZPath             string
	externalResourceEntitlementIdFilter string
	targetedSyncResources               []*v2.Resource
	syncResourceTypeIDs                 []string
	workerCount                         int
	storageEngine                       c1zstore.Engine

	// previousSyncSparePath is the connector's source-cache-replay opt-in: when
	// non-empty, the handler retains one spare c1z (the last successfully
	// uploaded sync) at exactly this path and feeds it to the next sync
	// as the replay source. Empty (the default) disables the feature
	// entirely — ETags are rarely implemented, and the spare costs the
	// host a full c1z of disk. Computed once by the task manager via
	// previousSyncSparePath().
	previousSyncSparePath string

	// previousSyncC1ZPath is an OPERATOR-supplied previous-sync c1z
	// (--previous-sync-c1z), taking precedence over the SDK-owned spare.
	// Unlike the spare it is never rotated or retired: deleting an
	// operator's file is not ours to do (same contract as the local task
	// manager), so a replay-integrity retry re-runs cold but leaves the
	// file in place.
	previousSyncC1ZPath string

	// spareMu serializes every mutation of the spare slot (promotion and
	// retirement) across this manager's CONCURRENT full-sync handlers,
	// which share one deterministic spare path. Without it, a failing
	// task's replay-integrity retirement can race another task's
	// promotion and delete a fresh, uninvolved spare. Shared from the
	// task manager; nil in tests that don't exercise rotation.
	spareMu *stdsync.Mutex

	// openedSpare reports that THIS task selected the SDK spare as its
	// replay source. Retirement is gated on it: a replay-integrity
	// failure may only retire the spare when the spare was actually
	// opened — an operator-supplied --previous-sync-c1z takes precedence
	// over the spare, and a failure on the operator's file must never
	// delete an SDK spare that sat unopened on disk.
	openedSpare bool
	// openedSpareSyncID records the sync id of the spare THIS task
	// actually opened as its replay source (read at open time, before
	// any concurrent rotation can swap the file). Retirement compares it
	// against the CURRENT spare's id and only removes on a match — the
	// failing sync may only retire the artifact that failed it, never a
	// newer spare promoted underneath it.
	openedSpareSyncID string
	// openedSparePrivatePath is the task-private hardlink pinning the
	// exact inode this sync replays from (see openSpareForReplay).
	// Removed when the sync finishes.
	openedSparePrivatePath string
}

// previousSyncSparePrefix is the fixed filename prefix of the retained
// previous-sync c1z. The full name is prefix + 16 hex digest chars +
// ".c1z" — constant shape, never derived from task input — which bounds
// what the rotation logic may ever touch: we only os.Rename onto /
// os.Remove exactly this file and the per-task temp file we created,
// never a directory, never a caller-influenced path.
const previousSyncSparePrefix = "baton-previous-sync-"

// previousSyncSparePath returns the spare's location for a connector
// instance. Two properties give the spare a positive identity:
//
//   - The directory is the same one the per-task temp c1z is created in
//     (tempDir, falling back to the OS temp dir exactly like
//     os.CreateTemp does), so the post-upload promotion is a
//     same-filesystem atomic rename, and the path is deterministic
//     across process restarts — a restarted instance finds the spare
//     its last successful upload left behind.
//   - The filename embeds a digest of the instance's client id, so two
//     instances with different credentials sharing one temp dir can
//     never replay from (or clobber) each other's spare, and rotating
//     credentials orphans the old spare instead of replaying another
//     tenant's data into the new one.
func previousSyncSparePath(tempDir, clientID string) string {
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	sum := sha256.Sum256([]byte(clientID))
	return filepath.Join(tempDir, fmt.Sprintf("%s%x.c1z", previousSyncSparePrefix, sum[:8]))
}

// promoteOrRemoveC1Z disposes of the current task's uploaded c1z:
// when keep is true it atomically promotes it to the spare slot
// (replacing any prior spare in one rename — the disk footprint is
// bounded at one spare, ever); otherwise, or when the rename fails, the
// file is removed so nothing leaks. currentPath is always the temp file
// this task created.
//
// Every outcome is logged: the promotion success log carries the
// promoted artifact's sync id (read from its manifest header, no
// payload decode) so the spare a future task replays from is positively
// identifiable in the logs, and both failure paths warn — a connector
// that opted in but whose rotation keeps failing must be visible, not
// silently replay-less forever.
func promoteOrRemoveC1Z(ctx context.Context, currentPath, sparePath string, keep bool) {
	l := ctxzap.Extract(ctx)
	if keep {
		err := os.Rename(currentPath, sparePath)
		if err == nil {
			l.Info("retained uploaded c1z as the previous-sync replay spare",
				zap.String("spare_path", sparePath),
				zap.String("spare_sync_id", c1zSyncIDBestEffort(sparePath)))
			return
		}
		l.Warn("failed to retain uploaded c1z as previous-sync spare; removing it instead (source-cache replay will be inactive next sync)",
			zap.String("current_path", currentPath),
			zap.String("spare_path", sparePath),
			zap.Error(err))
	}
	if err := os.Remove(currentPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		l.Error("failed to remove temp file", zap.Error(err), zap.String("path", currentPath))
	}
}

// c1zSyncIDBestEffort returns the sync id recorded in a v3 c1z's
// manifest header, or "" when the file isn't a readable v3 envelope
// (e.g. the SQLite engine, which has no header projection). Used for
// log identification only — never for control flow.
func c1zSyncIDBestEffort(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	if err != nil {
		return ""
	}
	if runs := m.GetSyncRuns(); len(runs) > 0 {
		return runs[0].GetSyncId()
	}
	return ""
}

// fileExists reports whether path exists and is a regular file.
func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular()
}

// retireSpareIfStillImplicated removes the spare after a
// replay-integrity failure, but ONLY when it is still the artifact the
// failing sync replayed from. Concurrent full-sync tasks share one
// deterministic spare path: while this task was syncing (minutes to
// hours), another task may have finished and promoted a FRESH spare into
// the slot — that artifact is uninvolved in this failure and must
// survive, or a slow failing task permanently blinds replay that a
// successful sibling just re-armed.
//
// Identity is the sync id read from the c1z manifest header at OPEN time
// vs now. An unreadable id on either side degrades to removal (the old,
// conservative behavior): an extra cold sync is safe; replaying from an
// implicated spare is not. Callers must hold the manager's spare mutex
// so the check-then-remove cannot interleave with a promotion.
func retireSpareIfStillImplicated(ctx context.Context, sparePath, openedSyncID string) {
	l := ctxzap.Extract(ctx)
	if openedSyncID != "" {
		if currentID := c1zSyncIDBestEffort(sparePath); currentID != "" && currentID != openedSyncID {
			l.Info("previous-sync spare was rotated by a concurrent task since this sync opened it; leaving the fresh spare in place",
				zap.String("spare_path", sparePath),
				zap.String("implicated_sync_id", openedSyncID),
				zap.String("current_spare_sync_id", currentID))
			return
		}
	}
	if rmErr := os.Remove(sparePath); rmErr != nil && !os.IsNotExist(rmErr) {
		l.Warn("failed to remove implicated previous-sync spare", zap.Error(rmErr))
	}
}

// withSpareLock runs fn holding the manager-wide spare mutex (no-op
// when unset — tests that don't exercise rotation).
func (c *fullSyncTaskHandler) withSpareLock(fn func()) {
	if c.spareMu != nil {
		c.spareMu.Lock()
		defer c.spareMu.Unlock()
	}
	fn()
}

// openSpareForReplay selects the SDK spare as this task's replay source
// and returns the path the syncer should open, or "" when no spare is
// on disk. Under the spare lock it captures the spare's sync id AND pins
// the exact inode with a task-private hardlink; the syncer opens the
// link, so a concurrent promotion (an os.Rename onto the shared slot)
// can neither swap the file between identity capture and open nor
// desync the recorded id from what the sync actually read. Without the
// pin, a promotion in that gap would make a later replay-integrity
// retirement compare the WRONG id and preserve the implicated artifact.
//
// The link lives next to the spare (same directory, task-digest suffix
// — constant shape, never caller-influenced) and is removed by
// releaseSpareSnapshot when the sync finishes. A failed link degrades
// to opening the shared path directly: the tiny selection/open race
// returns, costing at worst one preserved-implicated-spare cycle that
// the next successful rotation heals.
func (c *fullSyncTaskHandler) openSpareForReplay(ctx context.Context) string {
	l := ctxzap.Extract(ctx)
	openPath := ""
	c.withSpareLock(func() {
		if !fileExists(c.previousSyncSparePath) {
			return
		}
		taskDigest := sha256.Sum256([]byte(c.task.GetId()))
		private := fmt.Sprintf("%s.open-%x", c.previousSyncSparePath, taskDigest[:8])
		_ = os.Remove(private) // stale link from a crashed run of this task id
		if linkErr := os.Link(c.previousSyncSparePath, private); linkErr == nil {
			c.openedSparePrivatePath = private
			openPath = private
		} else {
			l.Warn("failed to pin previous-sync spare with a task-private link; opening the shared path directly",
				zap.String("spare_path", c.previousSyncSparePath), zap.Error(linkErr))
			openPath = c.previousSyncSparePath
		}
		// Read the id from the path the sync will actually open: with
		// the pin this is race-free; without it, the same best-effort
		// semantics as before.
		c.openedSpareSyncID = c1zSyncIDBestEffort(openPath)
		c.openedSpare = true
	})
	return openPath
}

// releaseSpareSnapshot removes the task-private hardlink created by
// openSpareForReplay. Safe to call when none was created.
func (c *fullSyncTaskHandler) releaseSpareSnapshot(ctx context.Context) {
	if c.openedSparePrivatePath == "" {
		return
	}
	if err := os.Remove(c.openedSparePrivatePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		ctxzap.Extract(ctx).Warn("failed to remove task-private spare link", zap.Error(err),
			zap.String("path", c.openedSparePrivatePath))
	}
	c.openedSparePrivatePath = ""
}

// retireImplicatedSpare handles the spare side of a replay-integrity
// failure. Gated on openedSpare: only a sync that actually replayed
// from the spare may retire it — a failure on an operator-supplied
// previous file (which wins selection over the spare) must leave the
// unopened spare in place, and a sync that ran cold has nothing to
// retire.
func (c *fullSyncTaskHandler) retireImplicatedSpare(ctx context.Context) {
	if !c.openedSpare || c.previousSyncSparePath == "" {
		return
	}
	// Under the spare lock, and only the exact artifact this sync
	// opened — a concurrent task's freshly promoted spare is not
	// implicated and must survive.
	c.withSpareLock(func() {
		retireSpareIfStillImplicated(ctx, c.previousSyncSparePath, c.openedSpareSyncID)
	})
}

// resolveStorageEngine returns the engine this task's sync writes:
// runner config wins, then the task's own engine, then "" (the SDK
// default — SQLite).
func (c *fullSyncTaskHandler) resolveStorageEngine() c1zstore.Engine {
	if c.storageEngine != "" {
		return c.storageEngine
	}
	if taskEngine := c.task.GetSyncFull().GetStorageEngine(); taskEngine != "" {
		return c1zstore.Engine(taskEngine)
	}
	return ""
}

// spareRetentionEligible reports whether an artifact written with engine
// can ever serve source-cache replay. Only Pebble implements
// dotc1z.SourceCacheStore (SQLite replay is an explicit non-goal), so
// retaining a non-Pebble spare costs a full c1z of disk per rotation for
// a file every future sync would refuse — the retention is skipped, with
// a warning so an opted-in operator learns WHY replay never engages
// (set --storage-engine pebble) instead of silently paying for nothing.
func spareRetentionEligible(engine c1zstore.Engine) bool {
	return engine == c1zstore.EnginePebble
}

func (c *fullSyncTaskHandler) sync(ctx context.Context, c1zPath string) error {
	ctx, span := tracer.Start(ctx, "fullSyncTaskHandler.sync")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))

	if c.task.GetSyncFull() == nil {
		return errors.New("task is not a full sync task")
	}

	syncOpts := []sdkSync.SyncOpt{
		sdkSync.WithC1ZPath(c1zPath),
		sdkSync.WithTmpDir(c.helpers.TempDir()),
		sdkSync.WithWorkerCount(c.workerCount),
	}
	if engine := c.resolveStorageEngine(); engine != "" {
		syncOpts = append(syncOpts, sdkSync.WithStorageEngine(engine))
	}

	if c.task.GetSyncFull().GetSkipExpandGrants() {
		// Have C1 expand grants. This is faster & results in a smaller c1z upload.
		syncOpts = append(syncOpts, sdkSync.WithDontExpandGrants())
	}

	if resources := c.task.GetSyncFull().GetTargetedSyncResources(); len(resources) > 0 {
		syncOpts = append(syncOpts, sdkSync.WithTargetedSyncResources(resources))
	}

	if c.task.GetSyncFull().GetSkipEntitlementsAndGrants() {
		// Sync only resources. This is meant to be used for a first sync so initial data gets into the UI faster.
		syncOpts = append(syncOpts, sdkSync.WithSkipEntitlementsAndGrants(true))
	}

	if c.task.GetSyncFull().GetSkipGrants() {
		syncOpts = append(syncOpts, sdkSync.WithSkipGrants(true))
	}

	if c.externalResourceC1ZPath != "" {
		syncOpts = append(syncOpts, sdkSync.WithExternalResourceC1ZPath(c.externalResourceC1ZPath))
	}

	// source-cache replay (opt-in): an operator-supplied
	// --previous-sync-c1z path wins over the SDK-owned spare retained
	// from the last successful upload. The spare uses optional semantics
	// — a missing/corrupt/stale-format spare degrades to a plain sync
	// and is replaced by this task's rotation on success, so a bad spare
	// self-heals and can never fail the sync. Both spare branches log: a
	// persistently-absent spare on an opted-in connector means rotation
	// is broken, and that must be visible rather than silently disabling
	// source-cache replay forever.
	switch {
	case c.previousSyncC1ZPath != "":
		l.Info("using operator-supplied previous-sync c1z as the source-cache replay source",
			zap.String("previous_sync_c1z", c.previousSyncC1ZPath),
			zap.String("previous_sync_id", c1zSyncIDBestEffort(c.previousSyncC1ZPath)))
		syncOpts = append(syncOpts, sdkSync.WithPreviousSyncC1ZPath(c.previousSyncC1ZPath))
	case c.previousSyncSparePath != "":
		if openPath := c.openSpareForReplay(ctx); openPath != "" {
			defer c.releaseSpareSnapshot(ctx)
			l.Info("previous-sync spare found; source-cache replay enabled for this sync",
				zap.String("spare_path", c.previousSyncSparePath),
				zap.String("open_path", openPath),
				zap.String("spare_sync_id", c.openedSpareSyncID))
			syncOpts = append(syncOpts, sdkSync.WithOptionalPreviousSyncC1ZPath(openPath))
		} else {
			l.Info("no previous-sync spare on disk; source-cache replay inactive for this sync (expected on the first sync after enabling, or after a failed rotation)",
				zap.String("spare_path", c.previousSyncSparePath))
		}
	}

	if c.externalResourceEntitlementIdFilter != "" {
		syncOpts = append(syncOpts, sdkSync.WithExternalResourceEntitlementIdFilter(c.externalResourceEntitlementIdFilter))
	}

	if c.skipFullSync {
		syncOpts = append(syncOpts, sdkSync.WithSkipFullSync())
	}

	if len(c.targetedSyncResources) > 0 {
		syncOpts = append(syncOpts, sdkSync.WithTargetedSyncResources(c.targetedSyncResources))
	}
	cc := c.helpers.ConnectorClient()

	// Prefer the task's resource type IDs (from the server/UI) over local config.
	// The UI is the authoritative source when set; local config is the fallback.
	syncResourceTypeIDs := c.task.GetSyncFull().GetSyncResourceTypeIds()
	if len(syncResourceTypeIDs) == 0 {
		syncResourceTypeIDs = c.syncResourceTypeIDs
	}
	if len(syncResourceTypeIDs) > 0 {
		syncOpts = append(syncOpts, sdkSync.WithSyncResourceTypes(syncResourceTypeIDs))
	}

	if setSessionStore, ok := cc.(session.SetSessionStore); ok {
		syncOpts = append(syncOpts, sdkSync.WithSessionStore(setSessionStore))
	}

	runSync := func(opts []sdkSync.SyncOpt) error {
		syncer, err := sdkSync.NewSyncer(ctx, c.helpers.ConnectorClient(), opts...)
		if err != nil {
			l.Error("failed to create syncer", zap.Error(err))
			return err
		}
		err = syncer.Sync(ctx)
		if err != nil {
			// We don't defer syncer.Close() in order to capture the error without named return values.
			if closeErr := syncer.Close(ctx); closeErr != nil {
				l.Error("failed to close syncer after sync error", zap.Error(closeErr))
				err = errors.Join(err, closeErr)
			}
			return err
		}
		return syncer.Close(ctx)
	}

	// TODO(jirwin): Should we attempt to retry at all before failing the task?
	err = runSync(syncOpts)
	if errors.Is(err, sdkSync.ErrReplayIntegrity) {
		// Replay is a pure optimization: a replay-integrity failure means
		// the warm sync's state cannot be trusted, and the safe automatic
		// remediation is a cold re-run — discard the partial output (its
		// replayed rows may be wrong; resuming would keep them), retire
		// the spare (it is implicated until a fresh rotation replaces
		// it), and sync again without a previous source. One retry: a
		// cold sync cannot fail this way again.
		l.Error("source-cache replay integrity failure; discarding replay state and re-running the sync cold",
			zap.Error(err),
			zap.String("spare_path", c.previousSyncSparePath))
		if rmErr := os.Remove(c1zPath); rmErr != nil && !os.IsNotExist(rmErr) {
			return errors.Join(err, rmErr)
		}
		c.retireImplicatedSpare(ctx)
		err = runSync(append(slices.Clone(syncOpts), sdkSync.WithoutPreviousSync()))
	}
	if err != nil {
		l.Error("failed to sync", zap.Error(err))
		return err
	}

	return nil
}

// TODO(morgabra) We should handle task resumption here. The task should contain at least an active sync id so we can
// resume syncing if we get restarted or fail to heartbeat temporarily.
// TODO(morgabra) Ideally we can tell the difference between a task cancellation and a task failure via the result
// of HeartbeatTask(). If we get cancelled, we probably want to clean up our sync state. If we fail to heartbeat, we
// might want to keep our sync state around so we can resume the task.
// TODO(morgabra) If we have a task with no sync_id set, we should create one and set it via heartbeat annotations? If we have a
// task with a sync_id and it doesn't match our current state sync_id, we should reject the task. If we have a task
// with a sync_id that does match our current state, we should resume our current sync, if possible.
func (c *fullSyncTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "fullSyncTaskHandler.HandleTask")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))
	l.Info("Handling full sync task.")

	assetFile, err := os.CreateTemp(c.helpers.TempDir(), "baton-sdk-sync-upload")
	if err != nil {
		l.Error("failed to create temp file", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}
	c1zPath := assetFile.Name()
	err = assetFile.Close()
	if err != nil {
		l.Error("failed to close asset file", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	// TODO(morgabra) Add annotation for for sync_id, or come up with some other way to track sync state.
	ctx, err = c.helpers.HeartbeatTask(ctx, nil)
	if err != nil {
		l.Error("failed to heartbeat task", zap.Error(err))
		return err
	}

	err = c.sync(ctx, c1zPath)
	if err != nil {
		l.Error("failed to sync", zap.Error(err))
		// Remove the partial c1z so a failed sync doesn't leak the temp
		// file. The spare (if any) is untouched — the next attempt can
		// still replay from it.
		if removeErr := os.Remove(c1zPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			l.Error("failed to remove temp file after sync failure", zap.Error(removeErr), zap.String("path", c1zPath))
		}
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	c1zF, err := os.Open(c1zPath)
	if err != nil {
		l.Error("failed to open sync asset prior to upload", zap.Error(err))
		if removeErr := os.Remove(c1zPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			l.Error("failed to remove temp file after open failure", zap.Error(removeErr), zap.String("path", c1zPath))
		}
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}
	// Dispose of the uploaded c1z on the way out (defers run LIFO: the
	// file is closed first, then disposed). Only a successfully uploaded
	// c1z is promoted to the previous-sync spare; every other path
	// removes it. An upload failure leaves the OLD spare in place, so the
	// retained previous sync always corresponds to an artifact C1 has.
	uploaded := false
	defer func() {
		keep := c.previousSyncSparePath != "" && uploaded
		if keep && !spareRetentionEligible(c.resolveStorageEngine()) {
			// Retaining a spare that no future sync can replay from is
			// pure disk cost; see spareRetentionEligible.
			l.Warn("keep-previous-sync-c1z is enabled but this sync's storage engine cannot serve source-cache replay; not retaining a spare "+
				"(set --storage-engine pebble to activate replay)",
				zap.String("storage_engine", string(c.resolveStorageEngine())))
			keep = false
		}
		// Under the spare lock: promotion renames onto the shared slot
		// and must not interleave with another task's identity-checked
		// retirement.
		c.withSpareLock(func() {
			promoteOrRemoveC1Z(ctx, c1zPath, c.previousSyncSparePath, keep)
		})
	}()
	defer func(f *os.File) {
		if closeErr := f.Close(); closeErr != nil {
			l.Error("failed to close sync asset", zap.Error(closeErr), zap.String("path", f.Name()))
		}
	}(c1zF)

	err = c.helpers.Upload(ctx, c1zF)
	if err != nil {
		l.Error("failed to upload sync asset", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}
	uploaded = true

	err = uploadDebugLogs(ctx, c.helpers, c.task.GetDebug())
	if err != nil {
		l.Error("failed to upload debug Task logs", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	return c.helpers.FinishTask(ctx, nil, nil, nil)
}

func newFullSyncTaskHandler(
	task *v1.Task,
	helpers fullSyncHelpers,
	skipFullSync bool,
	externalResourceC1ZPath string,
	externalResourceEntitlementIdFilter string,
	targetedSyncResources []*v2.Resource,
	syncResourceTypeIDs []string,
	workerCount int,
	storageEngine c1zstore.Engine,
	previousSyncSparePath string,
	previousSyncC1ZPath string,
	spareMu *stdsync.Mutex,
) tasks.TaskHandler {
	return &fullSyncTaskHandler{
		task:                                task,
		helpers:                             helpers,
		skipFullSync:                        skipFullSync,
		externalResourceC1ZPath:             externalResourceC1ZPath,
		externalResourceEntitlementIdFilter: externalResourceEntitlementIdFilter,
		targetedSyncResources:               targetedSyncResources,
		syncResourceTypeIDs:                 syncResourceTypeIDs,
		workerCount:                         workerCount,
		storageEngine:                       storageEngine,
		previousSyncSparePath:               previousSyncSparePath,
		previousSyncC1ZPath:                 previousSyncC1ZPath,
		spareMu:                             spareMu,
	}
}

// Check if Debug logs should be uploaded to C1 and if so do so,
// otherwise silently return success.
func uploadDebugLogs(ctx context.Context, helper fullSyncHelpers, deleteDebugLogs bool) error {
	ctx, span := tracer.Start(ctx, "uploadDebugLogs")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	l := ctxzap.Extract(ctx)

	tempDir := helper.TempDir()
	if tempDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			l.Warn("unable to get the current working directory", zap.Error(err))
		}
		if wd != "" {
			l.Warn("no temporary folder found on this system according to our sync helper,"+
				" we may create files in the current working directory by mistake as a result",
				zap.String("current working directory", wd))
		} else {
			l.Warn("no temporary folder found on this system according to our sync helper")
		}
	}
	debugPath := filepath.Join(tempDir, "debug.log")

	_, err = os.Stat(debugPath)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			l.Debug("debug log file does not exist", zap.Error(err))
		case errors.Is(err, os.ErrPermission):
			l.Warn("debug log file cannot be stat'd due to lack of permissions", zap.Error(err))
		default:
			l.Warn("cannot stat debug log file", zap.Error(err))
		}
		return nil
	}

	debugfile, err := os.Open(debugPath)
	if err != nil {
		l.Error("failed to open debug log file path", zap.Error(err))
		return err
	}
	if deleteDebugLogs {
		// We only delete the debug log when asked to,
		// as Manager-required log files are not automatically rotated.
		defer func() {
			err := os.Remove(debugPath)
			if err != nil {
				l.Error("failed to delete file with debug logs", zap.Error(err), zap.String("file", debugPath))
			} else {
				l.Info("deleted debug path")
			}
		}()
	}
	defer debugfile.Close()

	l.Info("uploading debug logs", zap.String("file", debugPath))
	err = helper.Upload(ctx, debugfile)
	if err != nil {
		l.Error("failed to upload debug logs", zap.Error(err))
		return err
	}

	return nil
}
