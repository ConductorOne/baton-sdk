package local

import (
	"context"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/provisioner"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localRevoker struct {
	dbPath string
	o      sync.Once

	grantID string
}

func (m *localRevoker) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_Revoke{},
		}
	})
	return task, 0, nil
}

func (m *localRevoker) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	granter := provisioner.NewRevoker(cc, m.dbPath, m.grantID)

	err := granter.Run(ctx)
	if err != nil {
		return err
	}

	err = granter.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

// NewRevoker returns a task manager that queues a revoke task.
func NewRevoker(ctx context.Context, dbPath string, grantID string) tasks.Manager {
	return &localRevoker{
		dbPath:  dbPath,
		grantID: grantID,
	}
}
