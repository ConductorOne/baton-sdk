package local

import (
	"context"
	"errors"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localSyncer struct {
	dbPath            string
	o                 sync.Once
	tmpDir            string
	partialResourceId *v2.ResourceId
}

type Option func(*localSyncer)

func WithPartialSync(resourceId *v2.ResourceId) Option {
	return func(m *localSyncer) {
		m.partialResourceId = resourceId
	}
}

func WithTmpDir(tmpDir string) Option {
	return func(m *localSyncer) {
		m.tmpDir = tmpDir
	}
}

func (m *localSyncer) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_SyncFull{},
		}
	})
	return task, 0, nil
}

func (m *localSyncer) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	opts := []sdkSync.SyncOpt{
		sdkSync.WithC1ZPath(m.dbPath),
		sdkSync.WithTmpDir(m.tmpDir),
	}

	if m.partialResourceId != nil {
		opts = append(opts, sdkSync.WithPartialSync(m.partialResourceId))
	}
	syncer, err := sdkSync.NewSyncer(ctx, cc, opts...)
	if err != nil {
		return err
	}

	err = syncer.Sync(ctx)
	if err != nil {
		if closeErr := syncer.Close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return err
	}

	if err := syncer.Close(ctx); err != nil {
		return err
	}

	return nil
}

// NewSyncer returns a task manager that queues a sync task.
func NewSyncer(ctx context.Context, dbPath string, opts ...Option) (tasks.Manager, error) {
	nm := &localSyncer{
		dbPath: dbPath,
	}

	for _, opt := range opts {
		opt(nm)
	}

	return nm, nil
}
