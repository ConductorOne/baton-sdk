package c1api

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

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

	// previousSyncSparePath is the connector's ETag-replay opt-in: when
	// non-empty, the handler retains one spare c1z (the last successfully
	// uploaded sync) at exactly this path and feeds it to the next sync
	// as the replay source. Empty (the default) disables the feature
	// entirely — ETags are rarely implemented, and the spare costs the
	// host a full c1z of disk. Computed once by the task manager via
	// previousSyncSparePath().
	previousSyncSparePath string
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
// silently etag-less forever.
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
		l.Warn("failed to retain uploaded c1z as previous-sync spare; removing it instead (etag replay will be inactive next sync)",
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
	engine := c.storageEngine
	if engine == "" && c.task.GetSyncFull().GetStorageEngine() != "" {
		engine = c1zstore.Engine(c.task.GetSyncFull().GetStorageEngine())
	}
	if engine != "" {
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

	// ETag replay (opt-in): feed the spare retained from the last
	// successful upload as the previous-sync replay source. Optional
	// semantics — a missing/corrupt/stale-format spare degrades to a
	// plain sync and is replaced by this task's rotation on success,
	// so a bad spare self-heals and can never fail the sync. Both
	// branches log: a persistently-absent spare on an opted-in
	// connector means rotation is broken, and that must be visible
	// rather than silently disabling etag replay forever.
	if c.previousSyncSparePath != "" {
		if fileExists(c.previousSyncSparePath) {
			l.Info("previous-sync spare found; etag replay enabled for this sync",
				zap.String("spare_path", c.previousSyncSparePath),
				zap.String("spare_sync_id", c1zSyncIDBestEffort(c.previousSyncSparePath)))
			syncOpts = append(syncOpts, sdkSync.WithOptionalPreviousSyncC1ZPath(c.previousSyncSparePath))
		} else {
			l.Info("no previous-sync spare on disk; etag replay inactive for this sync (expected on the first sync after enabling, or after a failed rotation)",
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

	syncer, err := sdkSync.NewSyncer(ctx, c.helpers.ConnectorClient(), syncOpts...)
	if err != nil {
		l.Error("failed to create syncer", zap.Error(err))
		return err
	}

	// TODO(jirwin): Should we attempt to retry at all before failing the task?
	err = syncer.Sync(ctx)
	if err != nil {
		l.Error("failed to sync", zap.Error(err))

		// We don't defer syncer.Close() in order to capture the error without named return values.
		if closeErr := syncer.Close(ctx); closeErr != nil {
			l.Error("failed to close syncer after sync error", zap.Error(err))
			err = errors.Join(err, closeErr)
		}

		return err
	}

	if err := syncer.Close(ctx); err != nil {
		l.Error("failed to close syncer", zap.Error(err))
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
		promoteOrRemoveC1Z(ctx, c1zPath, c.previousSyncSparePath, c.previousSyncSparePath != "" && uploaded)
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
