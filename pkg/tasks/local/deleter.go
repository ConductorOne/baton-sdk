package local

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/provisioner"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localResourceDeleter struct {
	dbPath string
	o      sync.Once

	resourceId   string
	resourceType string
}

func (m *localResourceDeleter) GetTempDir() string {
	return ""
}

func (m *localResourceDeleter) ShouldDebug() bool {
	return false
}

func (m *localResourceDeleter) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			DeleteResource: &v1.Task_DeleteResourceTask{},
		}.Build()
	})
	return task, 0, nil
}

func (m *localResourceDeleter) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localResourceDeleter.Process", trace.WithNewRoot())
	defer span.End()

	accountManager := provisioner.NewResourceDeleter(cc, m.dbPath, m.resourceId, m.resourceType)

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

// NewResourceDeleter returns a task manager that queues a delete resource task.
func NewResourceDeleter(ctx context.Context, dbPath string, resourceId string, resourceType string) tasks.Manager {
	return &localResourceDeleter{
		dbPath:       dbPath,
		resourceId:   resourceId,
		resourceType: resourceType,
	}
}
