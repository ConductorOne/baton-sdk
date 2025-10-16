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

	action string
	args   *structpb.Struct
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
				Name: m.action,
				Args: m.args,
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
	resp, err := cc.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name:        t.GetName(),
		Args:        t.GetArgs(),
		Annotations: t.GetAnnotations(),
	}.Build())
	if err != nil {
		return err
	}

	l.Info("ActionInvoke response", zap.Any("resp", resp))

	if resp.GetStatus() == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED {
		return fmt.Errorf("action invoke failed: %v", resp.GetResponse())
	}

	return nil
}

// NewActionInvoker returns a task manager that queues an action invoke task.
func NewActionInvoker(ctx context.Context, dbPath string, action string, args *structpb.Struct) tasks.Manager {
	return &localActionInvoker{
		dbPath: dbPath,
		action: action,
		args:   args,
	}
}
