package local

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/session"
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
	targetedSyncResources               []*v2.Resource
	skipEntitlementsAndGrants           bool
	skipGrants                          bool
	syncResourceTypeIDs                 []string
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

func WithTargetedSyncResources(resources []*v2.Resource) Option {
	return func(m *localSyncer) {
		m.targetedSyncResources = resources
	}
}

func WithSyncResourceTypeIDs(resourceTypeIDs []string) Option {
	return func(m *localSyncer) {
		m.syncResourceTypeIDs = resourceTypeIDs
	}
}

func WithSkipEntitlementsAndGrants(skip bool) Option {
	return func(m *localSyncer) {
		m.skipEntitlementsAndGrants = skip
	}
}

func WithSkipGrants(skip bool) Option {
	return func(m *localSyncer) {
		m.skipGrants = skip
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
		task = v1.Task_builder{
			SyncFull: &v1.Task_SyncFullTask{},
		}.Build()
	})
	return task, 0, nil
}

func (m *localSyncer) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localSyncer.Process", trace.WithNewRoot())
	defer span.End()

	var setSessionStore session.SetSessionStore
	if ssetSessionStore, ok := cc.(session.SetSessionStore); ok {
		setSessionStore = ssetSessionStore
	}
	syncer, err := sdkSync.NewSyncer(ctx, cc,
		sdkSync.WithC1ZPath(m.dbPath),
		sdkSync.WithTmpDir(m.tmpDir),
		sdkSync.WithExternalResourceC1ZPath(m.externalResourceC1Z),
		sdkSync.WithExternalResourceEntitlementIdFilter(m.externalResourceEntitlementIdFilter),
		sdkSync.WithTargetedSyncResources(m.targetedSyncResources),
		sdkSync.WithSkipEntitlementsAndGrants(m.skipEntitlementsAndGrants),
		sdkSync.WithSkipGrants(m.skipGrants),
		sdkSync.WithSessionStore(setSessionStore),
		sdkSync.WithSyncResourceTypes(m.syncResourceTypeIDs),
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
