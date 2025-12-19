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

type localResourceTypedSyncer struct {
	dbPath                    string
	resourceTypeID            string
	tmpDir                    string
	skipEntitlementsAndGrants bool
	skipGrants                bool
	o                         sync.Once
}

type ResourceTypedSyncerOption func(*localResourceTypedSyncer)

func (m *localResourceTypedSyncer) GetTempDir() string {
	return ""
}

func (m *localResourceTypedSyncer) ShouldDebug() bool {
	return false
}

func (m *localResourceTypedSyncer) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		task = v1.Task_builder{
			SyncResourceTyped: v1.Task_SyncResourceTypedTask_builder{
				ResourceTypeId:            m.resourceTypeID,
				SkipEntitlementsAndGrants: m.skipEntitlementsAndGrants,
			}.Build(),
		}.Build()
	})
	return task, 0, nil
}

func (m *localResourceTypedSyncer) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localResourceTypedSyncer.Process", trace.WithNewRoot())
	defer span.End()

	var setSessionStore session.SetSessionStore
	if ssetSessionStore, ok := cc.(session.SetSessionStore); ok {
		setSessionStore = ssetSessionStore
	}

	syncTask := task.GetSyncResourceTyped()
	syncer, err := sdkSync.NewSyncer(ctx, cc,
		sdkSync.WithC1ZPath(m.dbPath),
		sdkSync.WithTmpDir(m.tmpDir),
		sdkSync.WithResourceTypedSync(syncTask.GetResourceTypeId()),
		sdkSync.WithSkipEntitlementsAndGrants(syncTask.GetSkipEntitlementsAndGrants()),
		sdkSync.WithSkipGrants(m.skipGrants),
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

// NewResourceTypedSyncer returns a task manager that queues a resource-typed sync task.
func NewResourceTypedSyncer(ctx context.Context, dbPath string, resourceTypeID string, opts ...ResourceTypedSyncerOption) (tasks.Manager, error) {
	nm := &localResourceTypedSyncer{
		dbPath:         dbPath,
		resourceTypeID: resourceTypeID,
	}

	for _, opt := range opts {
		opt(nm)
	}

	return nm, nil
}

// ResourceTypedWithTmpDir sets the temp directory for the resource-typed syncer.
func ResourceTypedWithTmpDir(tmpDir string) ResourceTypedSyncerOption {
	return func(m *localResourceTypedSyncer) {
		m.tmpDir = tmpDir
	}
}

// ResourceTypedWithSkipEntitlementsAndGrants sets whether to skip entitlements and grants.
func ResourceTypedWithSkipEntitlementsAndGrants(skip bool) ResourceTypedSyncerOption {
	return func(m *localResourceTypedSyncer) {
		m.skipEntitlementsAndGrants = skip
	}
}

// ResourceTypedWithSkipGrants sets whether to skip grants only.
func ResourceTypedWithSkipGrants(skip bool) ResourceTypedSyncerOption {
	return func(m *localResourceTypedSyncer) {
		m.skipGrants = skip
	}
}

