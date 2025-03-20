package local

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localSyncer struct {
	dbPath                              string
	o                                   sync.Once
	tmpDir                              string
	externalResourceC1Z                 string
	externalResourceEntitlementIdFilter string
}

type Option func(*localSyncer)

func WithTmpDir(tmpDir string) Option {
	return func(m *localSyncer) {
		m.tmpDir = tmpDir
	}
}

func WithExternalResourceC1Z(externalResourceC1Z string) Option {
	return func(m *localSyncer) {
		m.externalResourceC1Z = externalResourceC1Z
	}
}

func WithExternalResourceEntitlementIdFilter(entitlementId string) Option {
	return func(m *localSyncer) {
		m.externalResourceEntitlementIdFilter = entitlementId
	}
}

func (m *localSyncer) GetTempDir() string {
	return ""
}

func (m *localSyncer) ShouldDebug() bool {
	return false
}

func (m *localSyncer) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = &v1.Task{
			TaskType: &v1.Task_SyncFull{},
		}
	})
	return task, 0, nil
}

func (m *localSyncer) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localSyncer.Process", trace.WithNewRoot())
	defer span.End()

	syncer, err := sdkSync.NewSyncer(ctx, cc,
		sdkSync.WithC1ZPath(m.dbPath),
		sdkSync.WithTmpDir(m.tmpDir),
		sdkSync.WithExternalResourceC1ZPath(m.externalResourceC1Z),
		sdkSync.WithExternalResourceEntitlementIdFilter(m.externalResourceEntitlementIdFilter))
	if err != nil {
		return err
	}

	err = syncer.Sync(ctx)
	if err != nil {
		if closeErr := syncer.Close(ctx); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return err
	}

	if err := syncer.Close(ctx); err != nil {
		return err
	}

	return nil
}

// NewSyncer returns a task manager that queues a sync task.
func NewSyncer(ctx context.Context, dbPath string, opts ...Option) (tasks.Manager, error) {
	nm := &localSyncer{
		dbPath: dbPath,
	}

	for _, opt := range opts {
		opt(nm)
	}

	return nm, nil
}
