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

type resourceTypedSyncTaskHandler struct {
	task    *v1.Task
	helpers fullSyncHelpers
}

func (c *resourceTypedSyncTaskHandler) sync(ctx context.Context, c1zPath string) error {
	ctx, span := tracer.Start(ctx, "resourceTypedSyncTaskHandler.sync")
	defer span.End()

	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))

	syncTask := c.task.GetSyncResourceTyped()
	if syncTask == nil {
		return errors.New("task is not a resource-typed sync task")
	}

	syncOpts := []sdkSync.SyncOpt{
		sdkSync.WithC1ZPath(c1zPath),
		sdkSync.WithTmpDir(c.helpers.TempDir()),
		sdkSync.WithResourceTypedSync(syncTask.GetResourceTypeId()),
	}

	if syncTask.GetSkipEntitlementsAndGrants() {
		syncOpts = append(syncOpts, sdkSync.WithSkipEntitlementsAndGrants(true))
	}

	cc := c.helpers.ConnectorClient()

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

func (c *resourceTypedSyncTaskHandler) HandleTask(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "resourceTypedSyncTaskHandler.HandleTask")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))
	l.Info("Handling resource-typed sync task.", zap.String("resource_type_id", c.task.GetSyncResourceTyped().GetResourceTypeId()))

	assetFile, err := os.CreateTemp(c.helpers.TempDir(), "baton-sdk-resource-typed-sync-upload")
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

func newResourceTypedSyncTaskHandler(task *v1.Task, helpers fullSyncHelpers) tasks.TaskHandler {
	return &resourceTypedSyncTaskHandler{
		task:    task,
		helpers: helpers,
	}
}

