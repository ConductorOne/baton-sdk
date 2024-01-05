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

type localAccountManager struct {
	dbPath string
	o      sync.Once

	login string
	email string
}

func (m *localAccountManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_CreateAccount{},
		}
	})
	return task, 0, nil
}

func (m *localAccountManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	accountManager := provisioner.NewCreateAccountManager(cc, m.dbPath, m.login, m.email)

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
func NewCreateAccountManager(ctx context.Context, dbPath string, login string, email string) tasks.Manager {
	return &localAccountManager{
		dbPath: dbPath,
		login:  login,
		email:  email,
	}
}
