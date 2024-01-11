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

type localGranter struct {
	dbPath string
	o      sync.Once

	entitlementID string
	principalID   string
	principalType string
}

func (m *localGranter) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_Grant{},
		}
	})
	return task, 0, nil
}

func (m *localGranter) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	granter := provisioner.NewGranter(cc, m.dbPath, m.entitlementID, m.principalID, m.principalType)

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

// NewGranter returns a task manager that queues a grant task.
func NewGranter(ctx context.Context, dbPath string, entitlementID string, principalID string, principalType string) tasks.Manager {
	return &localGranter{
		dbPath:        dbPath,
		entitlementID: entitlementID,
		principalID:   principalID,
		principalType: principalType,
	}
}
