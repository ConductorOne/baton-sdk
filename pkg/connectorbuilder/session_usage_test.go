package connectorbuilder

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// mapSessionStore is a minimal in-memory SessionStore backing for tests.
type mapSessionStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *mapSessionStore) Get(_ context.Context, key string, _ ...sessions.SessionStoreOption) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok, nil
}

func (m *mapSessionStore) GetMany(_ context.Context, keys []string, _ ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := m.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil, nil
}

func (m *mapSessionStore) Set(_ context.Context, key string, value []byte, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *mapSessionStore) SetMany(_ context.Context, values map[string][]byte, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range values {
		m.data[k] = v
	}
	return nil
}

func (m *mapSessionStore) Delete(_ context.Context, key string, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *mapSessionStore) Clear(_ context.Context, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = map[string][]byte{}
	return nil
}

func (m *mapSessionStore) GetAll(_ context.Context, _ string, _ ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		out[k] = v
	}
	return out, "", nil
}

// sessionUsingSyncer exercises the session store from within Grants, the way
// a real connector's sync op would.
type sessionUsingSyncer struct {
	resourceType *v2.ResourceType
}

func (s *sessionUsingSyncer) ResourceType(_ context.Context) *v2.ResourceType {
	return s.resourceType
}

func (s *sessionUsingSyncer) List(_ context.Context, _ *v2.ResourceId, _ resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	return nil, &resource.SyncOpResults{}, nil
}

func (s *sessionUsingSyncer) Entitlements(_ context.Context, _ *v2.Resource, _ resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return nil, &resource.SyncOpResults{}, nil
}

func (s *sessionUsingSyncer) Grants(ctx context.Context, _ *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	if err := opts.Session.Set(ctx, "cursor", []byte("page-2")); err != nil {
		return nil, nil, err
	}
	if _, _, err := opts.Session.Get(ctx, "cursor"); err != nil {
		return nil, nil, err
	}
	if _, _, err := opts.Session.Get(ctx, "missing"); err != nil {
		return nil, nil, err
	}
	return nil, &resource.SyncOpResults{}, nil
}

// TestListGrantsReportsSessionUsage drives the composed path a real
// session-using connector takes: handler installs the request collector, the
// instrumented store records through the WithSyncId wrapper, and the response
// annotation carries the usage back to the caller.
func TestListGrantsReportsSessionUsage(t *testing.T) {
	ctx := context.Background()

	backing := session.NewInstrumentedSessionStore(
		&mapSessionStore{data: map[string][]byte{}},
		"connector_backend", "", nil,
	)
	syncer := &sessionUsingSyncer{resourceType: v2.ResourceType_builder{Id: "repo"}.Build()}
	connector, err := NewConnector(ctx,
		&testConnector2{resourceSyncers: []ResourceSyncerV2{syncer}},
		WithSessionStore(backing),
	)
	require.NoError(t, err)

	resp, err := connector.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "repo", Resource: "r1"}.Build(),
		}.Build(),
		ActiveSyncId: "sync-1",
	}.Build())
	require.NoError(t, err)

	usage := &v2.SessionStoreUsage{}
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	ok, err := respAnnos.Pick(usage)
	require.NoError(t, err)
	require.True(t, ok, "session-using ListGrants must report usage on the response")
	require.Equal(t, "connector_backend", usage.GetKind())

	byOp := map[string]*v2.SessionStoreUsage_OpStats{}
	for _, op := range usage.GetOps() {
		byOp[op.GetOp()] = op
	}
	require.EqualValues(t, 1, byOp[session.SessionOpSet].GetCount())
	require.EqualValues(t, 2, byOp[session.SessionOpGet].GetCount())
	require.Zero(t, byOp[session.SessionOpGet].GetErrors())

	// A request that performs no session ops reports nothing.
	entResp, err := connector.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "repo", Resource: "r1"}.Build(),
		}.Build(),
		ActiveSyncId: "sync-1",
	}.Build())
	require.NoError(t, err)
	quiet := &v2.SessionStoreUsage{}
	quietAnnos := annotations.Annotations(entResp.GetAnnotations())
	ok, err = quietAnnos.Pick(quiet)
	require.NoError(t, err)
	require.False(t, ok, "no session ops → no annotation")
}
