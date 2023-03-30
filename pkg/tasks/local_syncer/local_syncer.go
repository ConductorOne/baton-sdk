package local_syncer

import (
	"context"
	"errors"
	"time"

	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localSyncer struct {
	task tasks.Task
}

func (m *localSyncer) Next(ctx context.Context) (tasks.Task, time.Duration, error) {
	return m.task, 0, nil
}

func (m *localSyncer) Run(ctx context.Context, task tasks.Task, cc types.ConnectorClient) error {
	t, ok := task.(*tasks.LocalFileSync)
	if !ok {
		return errors.New("invalid task type")
	}
	m.task = nil

	syncer, err := sdkSync.NewSyncer(ctx, cc, t.DbPath)
	if err != nil {
		return err
	}

	err = syncer.Sync(ctx)
	if err != nil {
		return err
	}

	err = syncer.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

// New returns a task manager that queues a sync task.
func New(ctx context.Context, task tasks.Task) (tasks.Manager, error) {
	nm := &localSyncer{
		task: task,
	}

	return nm, nil
}
