package c1api

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/session"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
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
}

func (c *fullSyncTaskHandler) sync(ctx context.Context, c1zPath string) error {
	ctx, span := tracer.Start(ctx, "fullSyncTaskHandler.sync")
	defer span.End()

	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))

	if c.task.GetSyncFull() == nil {
		return errors.New("task is not a full sync task")
	}

	syncOpts := []sdkSync.SyncOpt{
		sdkSync.WithC1ZPath(c1zPath),
		sdkSync.WithTmpDir(c.helpers.TempDir()),
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

	if c.externalResourceC1ZPath != "" {
		syncOpts = append(syncOpts, sdkSync.WithExternalResourceC1ZPath(c.externalResourceC1ZPath))
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

	if len(c.syncResourceTypeIDs) > 0 {
		syncOpts = append(syncOpts, sdkSync.WithSyncResourceTypes(c.syncResourceTypeIDs))
	}

	if setSessionStore, ok := cc.(session.SetSessionStore); ok {
		syncOpts = append(syncOpts, sdkSync.WithSessionStore(setSessionStore))
	}

	syncer, err := sdkSync.NewSyncer(ctx, cc, syncOpts...)
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
	defer span.End()

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
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	c1zF, err := os.Open(c1zPath)
	if err != nil {
		l.Error("failed to open sync asset prior to upload", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}
	defer func(f *os.File) {
		err = f.Close()
		if err != nil {
			l.Error("failed to close sync asset", zap.Error(err), zap.String("path", f.Name()))
		}
		err = os.Remove(f.Name())
		if err != nil {
			l.Error("failed to remove temp file", zap.Error(err), zap.String("path", f.Name()))
		}
	}(c1zF)

	err = c.helpers.Upload(ctx, c1zF)
	if err != nil {
		l.Error("failed to upload sync asset", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

	err = uploadDebugLogs(ctx, c.helpers)
	if err != nil {
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
) tasks.TaskHandler {
	return &fullSyncTaskHandler{
		task:                                task,
		helpers:                             helpers,
		skipFullSync:                        skipFullSync,
		externalResourceC1ZPath:             externalResourceC1ZPath,
		externalResourceEntitlementIdFilter: externalResourceEntitlementIdFilter,
		targetedSyncResources:               targetedSyncResources,
		syncResourceTypeIDs:                 syncResourceTypeIDs,
	}
}

func uploadDebugLogs(ctx context.Context, helper fullSyncHelpers) error {
	ctx, span := tracer.Start(ctx, "uploadDebugLogs")
	defer span.End()

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

	_, err := os.Stat(debugPath)
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
		return err
	}
	defer func() {
		err := os.Remove(debugPath)
		if err != nil {
			l.Error("failed to delete file with debug logs", zap.Error(err), zap.String("file", debugPath))
		}
	}()
	defer debugfile.Close()

	l.Info("uploading debug logs", zap.String("file", debugPath))
	err = helper.Upload(ctx, debugfile)
	if err != nil {
		return err
	}

	return nil
}
