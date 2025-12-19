package local

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/session"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type localPartialSyncResourceType struct {
	dbPath         string
	resourceTypeID string
	tmpDir         string
	o              sync.Once
}

type PartialSyncResourceTypeOption func(*localPartialSyncResourceType)

func (m *localPartialSyncResourceType) GetTempDir() string {
	return ""
}

func (m *localPartialSyncResourceType) ShouldDebug() bool {
	return false
}

func (m *localPartialSyncResourceType) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			SyncResourceTyped: v1.Task_SyncResourceTypedTask_builder{
				ResourceTypeId: m.resourceTypeID,
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localPartialSyncResourceType) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localPartialSyncResourceType.Process", trace.WithNewRoot())
	defer span.End()

	var setSessionStore session.SetSessionStore
	if ssetSessionStore, ok := cc.(session.SetSessionStore); ok {
		setSessionStore = ssetSessionStore
	}

	syncTask := task.GetSyncResourceTyped()
	syncer, err := sdkSync.NewSyncer(ctx, cc,
		sdkSync.WithC1ZPath(m.dbPath),
		sdkSync.WithTmpDir(m.tmpDir),
		sdkSync.WithPartialSyncResourceType(syncTask.GetResourceTypeId()),
		sdkSync.WithSessionStore(setSessionStore),
	)
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

// NewPartialSyncResourceType returns a task manager that queues a partial sync task for a single resource type.
// It automatically discovers parent resource types and syncs them first to ensure child resources can be discovered.
// Partial syncs only sync resources - entitlements and grants are never synced.
func NewPartialSyncResourceType(ctx context.Context, dbPath string, resourceTypeID string, opts ...PartialSyncResourceTypeOption) (tasks.Manager, error) {
	nm := &localPartialSyncResourceType{
		dbPath:         dbPath,
		resourceTypeID: resourceTypeID,
	}

	for _, opt := range opts {
		opt(nm)
	}

	return nm, nil
}

// PartialSyncWithTmpDir sets the temp directory for the partial sync.
func PartialSyncWithTmpDir(tmpDir string) PartialSyncResourceTypeOption {
	return func(m *localPartialSyncResourceType) {
		m.tmpDir = tmpDir
	}
}
