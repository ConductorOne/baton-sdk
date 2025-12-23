package c1api

import (
	"context"
	"errors"
	"os"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/session"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
)

type partialSyncTaskHandler struct {
	task    *v1.Task
	helpers fullSyncHelpers
}

func (c *partialSyncTaskHandler) sync(ctx context.Context, c1zPath string) error {
	ctx, span := tracer.Start(ctx, "partialSyncTaskHandler.sync")
	defer span.End()

	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))

	syncPartialTask := c.task.GetSyncPartial()
	if syncPartialTask == nil {
		return errors.New("task is not a partial sync task")
	}

	syncOpts := []sdkSync.SyncOpt{
		sdkSync.WithC1ZPath(c1zPath),
		sdkSync.WithTmpDir(c.helpers.TempDir()),
	}

	cc := c.helpers.ConnectorClient()

	// Check which sync mode is set
	switch {
	case syncPartialTask.HasTargetedResources():
		// Targeted resources mode - includes entitlements and grants
		targetedResources := syncPartialTask.GetTargetedResources()
		if targetedResources != nil && len(targetedResources.GetResources()) > 0 {
			l.Info("Running partial sync for targeted resources",
				zap.Int("resource_count", len(targetedResources.GetResources())))
			syncOpts = append(syncOpts, sdkSync.WithTargetedSyncResources(targetedResources.GetResources()))
		} else {
			return errors.New("targeted_resources is set but contains no resources")
		}

	case syncPartialTask.GetResourceTypeId() != "":
		// Resource type mode - skips entitlements and grants
		resourceTypeID := syncPartialTask.GetResourceTypeId()
		l.Info("Running partial sync for resource type", zap.String("resource_type_id", resourceTypeID))
		syncOpts = append(syncOpts, sdkSync.WithPartialSyncResourceType(resourceTypeID))

	default:
		return errors.New("partial sync task must specify either targeted_resources or resource_type_id")
	}

	if setSessionStore, ok := cc.(session.SetSessionStore); ok {
		syncOpts = append(syncOpts, sdkSync.WithSessionStore(setSessionStore))
	}

	syncer, err := sdkSync.NewSyncer(ctx, cc, syncOpts...)
	if err != nil {
		l.Error("failed to create syncer", zap.Error(err))
		return err
	}

	err = syncer.Sync(ctx)
	if err != nil {
		l.Error("failed to sync", zap.Error(err))

		if closeErr := syncer.Close(ctx); closeErr != nil {
			l.Error("failed to close syncer after sync error", zap.Error(closeErr))
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

func (c *partialSyncTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "partialSyncTaskHandler.HandleTask")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))
	l.Info("Handling partial sync task.")

	assetFile, err := os.CreateTemp(c.helpers.TempDir(), "baton-sdk-partial-sync-upload")
	if err != nil {
		l.Error("failed to create temp file", zap.Error(err))
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}
	c1zPath := assetFile.Name()
	err = assetFile.Close()
	if err != nil {
		return c.helpers.FinishTask(ctx, nil, nil, err)
	}

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

func newPartialSyncTaskHandler(task *v1.Task, helpers fullSyncHelpers) tasks.TaskHandler {
	return &partialSyncTaskHandler{
		task:    task,
		helpers: helpers,
	}
}

