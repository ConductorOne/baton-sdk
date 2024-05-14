package local

import (
	"context"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type localSchema struct {
	o sync.Once
}

func (m *localSchema) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_ListSchemas{},
		}
	})
	return task, 0, nil
}

func (m *localSchema) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)

	resp, err := cc.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{})
	if err != nil {
		return err
	}

	l.Info("Schemas", zap.Any("resp", resp))

	return nil
}

// NewSchema returns a task manager that queues a list schema task.
func NewListSchema(ctx context.Context) tasks.Manager {
	return &localSchema{}
}
