package local

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"go.opentelemetry.io/otel/trace"
)

type localDiffer struct {
	dbPath string
	o      sync.Once

	baseSyncID string
	newSyncID  string
}

func (m *localDiffer) GetTempDir() string {
	return ""
}

func (m *localDiffer) ShouldDebug() bool {
	return false
}

func (m *localDiffer) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_CreateSyncDiff{},
		}
	})
	return task, 0, nil
}

func (m *localDiffer) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localDiffer.Process", trace.WithNewRoot())
	defer span.End()

	store, err := c1zmanager.New(ctx, m.dbPath)
	if err != nil {
		return err
	}
	file, err := store.LoadC1Z(ctx)
	if err != nil {
		return err
	}
	diffSyncID, err := file.GenerateSyncDiff(ctx, m.baseSyncID, m.newSyncID)
	if err != nil {
		return err
	}

	// TODO: Remove?
	fmt.Println(diffSyncID)

	return nil
}

// NewRevoker returns a task manager that queues a revoke task.
func NewDiffer(ctx context.Context, dbPath string, baseSyncID string, newSyncID string) tasks.Manager {
	return &localDiffer{
		dbPath:     dbPath,
		baseSyncID: baseSyncID,
		newSyncID:  newSyncID,
	}
}
