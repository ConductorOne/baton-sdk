package localsyncer

import (
	"context"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localSyncer struct {
	dbPath string
	o      sync.Once
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
	syncer, err := sdkSync.NewSyncer(ctx, cc, m.dbPath)
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
func New(ctx context.Context, dbPath string) (tasks.Manager, error) {
	nm := &localSyncer{
		dbPath: dbPath,
	}

	return nm, nil
}