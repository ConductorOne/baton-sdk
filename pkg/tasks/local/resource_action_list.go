package local

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

type localResourceActionLister struct {
	dbPath string
	o      sync.Once

	resourceTypeID string
}

func (m *localResourceActionLister) GetTempDir() string {
	return ""
}

func (m *localResourceActionLister) ShouldDebug() bool {
	return false
}

func (m *localResourceActionLister) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			ListResourceActions: v1.Task_ListResourceActionsTask_builder{
				ResourceTypeId: m.resourceTypeID,
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localResourceActionLister) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	l := ctxzap.Extract(ctx)
	ctx, span := tracer.Start(ctx, "localResourceActionLister.Process", trace.WithNewRoot())
	defer span.End()

	t := task.GetListResourceActions()
	resp, err := cc.ListResourceActions(ctx, v2.ListResourceActionsRequest_builder{
		ResourceTypeId: t.GetResourceTypeId(),
		Annotations:    t.GetAnnotations(),
	}.Build())
	if err != nil {
		return err
	}

	// Output the schemas as JSON
	marshaller := protojson.MarshalOptions{
		Multiline: true,
		Indent:    "  ",
	}

	outputBytes, err := marshaller.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	// Pretty print JSON
	var jsonData interface{}
	if err := json.Unmarshal(outputBytes, &jsonData); err != nil {
		return fmt.Errorf("failed to unmarshal for pretty printing: %w", err)
	}

	prettyJSON, err := json.MarshalIndent(jsonData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal pretty JSON: %w", err)
	}

	_, err = fmt.Fprintln(os.Stdout, string(prettyJSON))
	if err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	l.Info("ListResourceActions response", zap.Int("count", len(resp.GetSchemas())))

	return nil
}

// NewResourceActionLister returns a task manager that queues a list resource actions task.
func NewResourceActionLister(ctx context.Context, dbPath string, resourceTypeID string) tasks.Manager {
	return &localResourceActionLister{
		dbPath:         dbPath,
		resourceTypeID: resourceTypeID,
	}
}

