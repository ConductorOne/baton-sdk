package c1api

import (
	"context"
	"io"
	"os"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type fullSyncHelpers interface {
	ConnectorClient() types.ConnectorClient
	Upload(ctx context.Context, r io.ReadSeeker) error
	FinishTask(ctx context.Context, err error) error
}

type fullSyncTaskHandler struct {
	task    *v1.Task
	helpers fullSyncHelpers
}

func (c *fullSyncTaskHandler) HandleTask(ctx context.Context) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))

	l.Info("Handling full sync task.")

	assetFile, err := os.CreateTemp("", "baton-sdk-sync-upload")
	if err != nil {
		l.Error("failed to create temp file", zap.Error(err))
		return c.helpers.FinishTask(ctx, err)
	}
	c1zPath := assetFile.Name()
	err = assetFile.Close()
	if err != nil {
		return c.helpers.FinishTask(ctx, err)
	}

	syncer, err := sdkSync.NewSyncer(ctx, c.helpers.ConnectorClient(), sdkSync.WithC1ZPath(c1zPath))
	if err != nil {
		l.Error("failed to create syncer", zap.Error(err))
		return c.helpers.FinishTask(ctx, err)
	}

	// TODO(jirwin): Should we attempt to retry at all before failing the task?
	err = syncer.Sync(ctx)
	if err != nil {
		l.Error("failed to sync", zap.Error(err))
		return c.helpers.FinishTask(ctx, err)
	}

	err = syncer.Close(ctx)
	if err != nil {
		l.Error("failed to close syncer", zap.Error(err))
		return c.helpers.FinishTask(ctx, err)
	}

	c1zF, err := os.Open(c1zPath)
	if err != nil {
		l.Error("failed to open sync asset prior to upload", zap.Error(err))
		return c.helpers.FinishTask(ctx, err)
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
		return c.helpers.FinishTask(ctx, err)
	}

	return c.helpers.FinishTask(ctx, nil)
}

func newFullSyncTaskHandler(task *v1.Task, helpers fullSyncHelpers) tasks.TaskHandler {
	return &fullSyncTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
