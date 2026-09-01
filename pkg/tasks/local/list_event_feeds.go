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

type localListEventFeeds struct {
	o sync.Once
}

func (m *localListEventFeeds) GetTempDir() string {
	return ""
}

func (m *localListEventFeeds) ShouldDebug() bool {
	return false
}

func (m *localListEventFeeds) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			ListEventFeeds: v1.Task_ListEventFeedsTask_builder{}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localListEventFeeds) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)

	resp, err := cc.ListEventFeeds(ctx, v2.ListEventFeedsRequest_builder{}.Build())
	if err != nil {
		return err
	}

	l.Info("Event Feeds",
		zap.Int("count", len(resp.GetList())),
		zap.Any("feeds", resp.GetList()),
	)

	return nil
}

// NewListEventFeeds returns a task manager that queues a list event feeds task.
func NewListEventFeeds(ctx context.Context) tasks.Manager {
	return &localListEventFeeds{}
}
