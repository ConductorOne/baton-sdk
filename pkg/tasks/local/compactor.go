package local

import (
	"context"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type localCompactor struct {
	o sync.Once

	compactableSyncs []*synccompactor.CompactableSync
	outputPath       string
}

func (m *localCompactor) GetTempDir() string {
	return ""
}

func (m *localCompactor) ShouldDebug() bool {
	return false
}

func (m *localCompactor) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_CompactSyncs_{},
		}
	})
	return task, 0, nil
}

func (m *localCompactor) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localCompactor.Process", trace.WithNewRoot())
	defer span.End()
	log := ctxzap.Extract(ctx)

	compactor, cleanup, err := synccompactor.NewCompactor(ctx, m.outputPath, m.compactableSyncs)
	if err != nil {
		return err
	}
	defer func() {
		_ = cleanup()
	}()

	compacted, err := compactor.Compact(ctx)
	if err != nil {
		return err
	}

	log.Info("compacted file", zap.String("file_path", compacted.FilePath), zap.String("sync_id", compacted.SyncID))

	return nil
}

// NewLocalCompactor returns a task manager that queues a revoke task.
func NewLocalCompactor(ctx context.Context, outputPath string, compactableSyncs []*synccompactor.CompactableSync) tasks.Manager {
	return &localCompactor{
		compactableSyncs: compactableSyncs,
		outputPath:       outputPath,
	}
}
