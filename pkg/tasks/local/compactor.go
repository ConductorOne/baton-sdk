package local

import (
	"context"
	"sync"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/conductorone/baton-sdk/pkg/uotel/uotelzap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type localCompactor struct {
	o sync.Once

	compactableSyncs []*synccompactor.CompactableSync
	outputPath       string
	tmpDir           string
	storageEngine    c1zstore.Engine
}

type CompactorOption func(*localCompactor)

func WithCompactorStorageEngine(engine c1zstore.Engine) CompactorOption {
	return func(m *localCompactor) {
		m.storageEngine = engine
	}
}

func (m *localCompactor) GetTempDir() string {
	return m.tmpDir
}

func (m *localCompactor) ShouldDebug() bool {
	return false
}

func (m *localCompactor) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			CompactSyncs: &v1.Task_CompactSyncs{},
		}.Build()
	})
	return task, 0, nil
}

func (m *localCompactor) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localCompactor.Process", trace.WithNewRoot())
	ctx = uotelzap.WithSpanLogFields(ctx)
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	log := ctxzap.Extract(ctx)

	var compactOpts []synccompactor.Option
	if m.tmpDir != "" {
		compactOpts = append(compactOpts, synccompactor.WithTmpDir(m.tmpDir))
	}
	if m.storageEngine != "" {
		compactOpts = append(compactOpts, synccompactor.WithEngine(m.storageEngine))
	}
	compactor, cleanup, err := synccompactor.NewCompactor(ctx, m.outputPath, m.compactableSyncs, compactOpts...)
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

// NewLocalCompactor returns a task manager that queues a compaction task.
func NewLocalCompactor(ctx context.Context, outputPath string, compactableSyncs []*synccompactor.CompactableSync, tmpDir string, opts ...CompactorOption) tasks.Manager {
	m := &localCompactor{
		compactableSyncs: compactableSyncs,
		outputPath:       outputPath,
		tmpDir:           tmpDir,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}
