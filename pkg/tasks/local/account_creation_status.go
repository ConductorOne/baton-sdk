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

type accountCreationStatuser struct {
	taskId string
	o      sync.Once
}

func (m *accountCreationStatuser) GetTempDir() string {
	return ""
}

func (m *accountCreationStatuser) ShouldDebug() bool {
	return false
}

func (m *accountCreationStatuser) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_GetAccountCreationStatus{},
		}
	})
	return task, 0, nil
}

func (m *accountCreationStatuser) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	accountManager := provisioner.NewAccountCreationStatuser(cc, m.taskId)

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

func NewAccountCreationStatuser(ctx context.Context, taskId string) tasks.Manager {
	return &accountCreationStatuser{
		taskId: taskId,
	}
}
