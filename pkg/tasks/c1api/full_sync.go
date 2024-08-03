package c1api

import (
	"context"
	"errors"
	"io"
	"os"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type fullSyncHelpers interface {
	ConnectorClient() types.ConnectorClient
	Upload(ctx context.Context, r io.ReadSeeker) error
	FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error
	HeartbeatTask(ctx context.Context, annos annotations.Annotations) (context.Context, error)
	TempDir() string
}

type fullSyncTaskHandler struct {
	task         *v1.Task
	helpers      fullSyncHelpers
	skipFullSync bool
}

func (c *fullSyncTaskHandler) sync(ctx context.Context, c1zPath string) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))

	syncOpts := []sdkSync.SyncOpt{
		sdkSync.WithC1ZPath(c1zPath),
		sdkSync.WithTmpDir(c.helpers.TempDir()),
	}

	if c.skipFullSync {
		syncOpts = append(syncOpts, sdkSync.WithSkipFullSync())
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

	return c.helpers.FinishTask(ctx, nil, nil, nil)
}

func newFullSyncTaskHandler(task *v1.Task, helpers fullSyncHelpers, skipFullSync bool) tasks.TaskHandler {
	return &fullSyncTaskHandler{
		task:         task,
		helpers:      helpers,
		skipFullSync: skipFullSync,
	}
}
