package local

import (
	"context"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/resource_lookup"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localResourceLookuper struct {
	lookupToken string
	o           sync.Onc
}

func (m *localResourceLookuper) GetTempDir() string {
	return ""
}

func (m *localResourceLookuper) ShouldDebug() bool {
	return false
}

func (m *localResourceLookuper) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_LookupResource{},
		}
	})
	return task, 0, nil
}

func (m *localResourceLookuper) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	accountManager := resource_lookup.NewResourceLookupper(cc, m.lookupToken)

	err := accountManager.Run(ctx)
	if err != nil {
		return err
	}

	err = accountManager.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

// NewGranter returns a task manager that queues a sync task.
func NewResourceLookerUpper(ctx context.Context, lookupToken string) tasks.Manager {
	return &localResourceLookuper{
		lookupToken: lookupToken,
	}
}
