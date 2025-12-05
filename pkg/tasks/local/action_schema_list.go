package local

import (
	"context"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localListActionSchemas struct {
	o              sync.Once
	resourceTypeID string // Optional: filter by resource type
}

func (m *localListActionSchemas) GetTempDir() string {
	return ""
}

func (m *localListActionSchemas) ShouldDebug() bool {
	return false
}

func (m *localListActionSchemas) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			ActionListSchemas: v1.Task_ActionListSchemasTask_builder{
				ResourceTypeId: m.resourceTypeID,
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localListActionSchemas) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)

	reqBuilder := v2.ListActionSchemasRequest_builder{}
	if m.resourceTypeID != "" {
		reqBuilder.ResourceTypeId = m.resourceTypeID
	}

	resp, err := cc.ListActionSchemas(ctx, reqBuilder.Build())
	if err != nil {
		return err
	}

	if m.resourceTypeID != "" {
		l.Info("Action Schemas",
			zap.String("resource_type_id", m.resourceTypeID),
			zap.Int("count", len(resp.GetSchemas())),
			zap.Any("schemas", resp.GetSchemas()),
		)
	} else {
		l.Info("Action Schemas",
			zap.Int("count", len(resp.GetSchemas())),
			zap.Any("schemas", resp.GetSchemas()),
		)
	}

	return nil
}

// NewListActionSchemas returns a task manager that queues a list action schemas task.
// If resourceTypeID is provided, it filters schemas for that specific resource type.
func NewListActionSchemas(ctx context.Context, resourceTypeID string) tasks.Manager {
	return &localListActionSchemas{
		resourceTypeID: resourceTypeID,
	}
}
