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

type localResourceActionInvoker struct {
	dbPath string
	o      sync.Once

	resourceTypeID string
	actionName     string
	args           *structpb.Struct
}

func (m *localResourceActionInvoker) GetTempDir() string {
	return ""
}

func (m *localResourceActionInvoker) ShouldDebug() bool {
	return false
}

func (m *localResourceActionInvoker) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	// For resource actions, we'll call the ConnectorClient directly in Process
	// since there's no specific task type for InvokeResourceAction yet
	var task *v1.Task
	m.o.Do(func() {
		// Create a dummy task - we'll handle it in Process
		task = &v1.Task{}
	})
	return task, 0, nil
}

func (m *localResourceActionInvoker) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)
	ctx, span := tracer.Start(ctx, "localResourceActionInvoker.Process", trace.WithNewRoot())
	defer span.End()

	resp, err := cc.InvokeResourceAction(ctx, v2.InvokeResourceActionRequest_builder{
		ResourceTypeId: m.resourceTypeID,
		ActionName:     m.actionName,
		Args:           m.args,
	}.Build())
	if err != nil {
		return err
	}

	l.Info("InvokeResourceAction response",
		zap.String("action_id", resp.GetActionId()),
		zap.String("status", resp.GetStatus().String()),
		zap.Any("response", resp.GetResponse()),
	)

	if resp.GetStatus() == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		return fmt.Errorf("resource action invoke failed: %v", resp.GetResponse())
	}

	return nil
}

// NewResourceActionInvoker returns a task manager that invokes a resource action.
func NewResourceActionInvoker(ctx context.Context, dbPath string, resourceTypeID string, actionName string, args *structpb.Struct) tasks.Manager {
	return &localResourceActionInvoker{
		dbPath:         dbPath,
		resourceTypeID: resourceTypeID,
		actionName:     actionName,
		args:           args,
	}
}
