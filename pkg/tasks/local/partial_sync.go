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

type localPartialSync struct {
	dbPath            string
	resourceTypeID    string
	targetedResources []*v2.Resource
	tmpDir            string
	o                 sync.Once
}

type PartialSyncOption func(*localPartialSync)

func (m *localPartialSync) GetTempDir() string {
	return ""
}

func (m *localPartialSync) ShouldDebug() bool {
	return false
}

func (m *localPartialSync) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	var task *v1.Task
	m.o.Do(func() {
		taskBuilder := v1.Task_builder{}

		syncPartialBuilder := v1.Task_SyncPartialTask_builder{}

		if len(m.targetedResources) > 0 {
			// Targeted resources mode
			syncPartialBuilder.TargetedResources = v1.Task_SyncPartialTask_TargetedResources_builder{
				Resources: m.targetedResources,
			}.Build()
		} else if m.resourceTypeID != "" {
			// Resource type mode
			syncPartialBuilder.ResourceTypeId = &m.resourceTypeID
		}

		taskBuilder.SyncPartial = syncPartialBuilder.Build()
		task = taskBuilder.Build()
	})
	return task, 0, nil
}

func (m *localPartialSync) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	ctx, span := tracer.Start(ctx, "localPartialSync.Process", trace.WithNewRoot())
	defer span.End()

	var setSessionStore session.SetSessionStore
	if ssetSessionStore, ok := cc.(session.SetSessionStore); ok {
		setSessionStore = ssetSessionStore
	}

	syncTask := task.GetSyncPartial()
	if syncTask == nil {
		return errors.New("task is not a partial sync task")
	}

	syncOpts := []sdkSync.SyncOpt{
		sdkSync.WithC1ZPath(m.dbPath),
		sdkSync.WithTmpDir(m.tmpDir),
		sdkSync.WithSessionStore(setSessionStore),
	}

	// Check which sync mode is set
	switch {
	case syncTask.HasTargetedResources():
		// Targeted resources mode - includes entitlements and grants
		targetedResources := syncTask.GetTargetedResources()
		if targetedResources != nil && len(targetedResources.GetResources()) > 0 {
			syncOpts = append(syncOpts, sdkSync.WithTargetedSyncResources(targetedResources.GetResources()))
		} else {
			return errors.New("targeted_resources is set but contains no resources")
		}

	case syncTask.GetResourceTypeId() != "":
		// Resource type mode - skips entitlements and grants
		syncOpts = append(syncOpts, sdkSync.WithPartialSyncResourceType(syncTask.GetResourceTypeId()))

	default:
		return errors.New("partial sync task must specify either targeted_resources or resource_type_id")
	}

	syncer, err := sdkSync.NewSyncer(ctx, cc, syncOpts...)
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

// NewPartialSync returns a task manager that queues a partial sync task.
// It supports two modes:
// - Resource type mode: Syncs resources of a single type (with parent discovery). Entitlements and grants are skipped.
// - Targeted resources mode: Syncs specific resources with their entitlements and grants.
func NewPartialSync(ctx context.Context, dbPath string, opts ...PartialSyncOption) (tasks.Manager, error) {
	nm := &localPartialSync{
		dbPath: dbPath,
	}

	for _, opt := range opts {
		opt(nm)
	}

	if nm.resourceTypeID == "" && len(nm.targetedResources) == 0 {
		return nil, errors.New("partial sync requires either a resource type ID or targeted resources")
	}

	return nm, nil
}

// WithPartialSyncResourceType sets the resource type ID for resource type mode.
// In this mode, only resources are synced - entitlements and grants are skipped.
func WithPartialSyncResourceType(resourceTypeID string) PartialSyncOption {
	return func(m *localPartialSync) {
		m.resourceTypeID = resourceTypeID
	}
}

// WithPartialSyncTargetedResources sets the targeted resources for targeted resources mode.
// In this mode, full sync data (resources, entitlements, grants) is synced for the specified resources.
func WithPartialSyncTargetedResources(resources []*v2.Resource) PartialSyncOption {
	return func(m *localPartialSync) {
		m.targetedResources = resources
	}
}

// WithPartialSyncTmpDir sets the temp directory for the partial sync.
func WithPartialSyncTmpDir(tmpDir string) PartialSyncOption {
	return func(m *localPartialSync) {
		m.tmpDir = tmpDir
	}
}

