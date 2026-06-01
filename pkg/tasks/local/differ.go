package local

import (
	"context"
	"errors"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/conductorone/baton-sdk/pkg/uotel/uotelzap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type localDiffer struct {
	dbPath string
	o      sync.Once

	baseSyncID    string
	appliedSyncID string
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
		task = v1.Task_builder{
			CreateSyncDiff: &v1.Task_CreateSyncDiffTask{},
		}.Build()
	})
	return task, 0, nil
}

func (m *localDiffer) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localDiffer.Process", trace.WithNewRoot())
	ctx = uotelzap.WithSpanLogFields(ctx)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	log := ctxzap.Extract(ctx)

	if m.baseSyncID == "" || m.appliedSyncID == "" {
		return errors.New("missing base sync ID or applied sync ID")
	}

	file, err := dotc1z.NewC1ZFile(ctx, m.dbPath)
	if err != nil {
		return err
	}

	newSyncID, err := file.GenerateSyncDiff(ctx, m.baseSyncID, m.appliedSyncID)
	if err != nil {
		return err
	}

	if err := file.Close(ctx); err != nil {
		log.Error("failed to close store", zap.Error(err))
		return err
	}

	log.Info("generated diff of syncs", zap.String("new_sync_id", newSyncID))

	return nil
}

// NewDiffer returns a task manager that queues a revoke task.
func NewDiffer(ctx context.Context, dbPath string, baseSyncID string, appliedSyncID string) tasks.Manager {
	return &localDiffer{
		dbPath:        dbPath,
		baseSyncID:    baseSyncID,
		appliedSyncID: appliedSyncID,
	}
}
