package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

type localActionInvoker struct {
	dbPath string
	o      sync.Once

	action         string
	resourceTypeID string // Optional: if set, invokes a resource-scoped action
	args           *structpb.Struct
}

func (m *localActionInvoker) GetTempDir() string {
	return ""
}

func (m *localActionInvoker) ShouldDebug() bool {
	return false
}

func (m *localActionInvoker) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			ActionInvoke: v1.Task_ActionInvokeTask_builder{
				Name:           m.action,
				Args:           m.args,
				ResourceTypeId: m.resourceTypeID,
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localActionInvoker) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)
	ctx, span := tracer.Start(ctx, "localActionInvoker.Process", trace.WithNewRoot())
	defer span.End()

	t := task.GetActionInvoke()
	reqBuilder := v2.InvokeActionRequest_builder{
		Name:        t.GetName(),
		Args:        t.GetArgs(),
		Annotations: t.GetAnnotations(),
	}
	if resourceTypeID := t.GetResourceTypeId(); resourceTypeID != "" {
		reqBuilder.ResourceTypeId = resourceTypeID
	}
	resp, err := cc.InvokeAction(ctx, reqBuilder.Build())
	if err != nil {
		return err
	}

	status := resp.GetStatus()
	finalResp := resp.GetResponse()
	l.Info("ActionInvoke response",
		zap.String("action_id", resp.GetId()),
		zap.String("name", resp.GetName()),
		zap.String("status", resp.GetStatus().String()),
		zap.Any("response", resp.GetResponse()),
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for status == v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING || status == v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r, err := cc.GetActionStatus(ctx, &v2.GetActionStatusRequest{
				Id: resp.GetId(),
			})
			if err != nil {
				return fmt.Errorf("failed to poll action status: %w", err)
			}
			status = r.GetStatus()
			finalResp = r.GetResponse()
		}
	}

	l.Info("ActionInvoke response", zap.Any("resp", finalResp))

	if status == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		return fmt.Errorf("action invoke failed: %v", finalResp)
	}

	return nil
}

// NewActionInvoker returns a task manager that queues an action invoke task.
// If resourceTypeID is provided, it invokes a resource-scoped action.
func NewActionInvoker(ctx context.Context, dbPath string, action string, resourceTypeID string, args *structpb.Struct) tasks.Manager {
	return &localActionInvoker{
		dbPath:         dbPath,
		action:         action,
		resourceTypeID: resourceTypeID,
		args:           args,
	}
}
