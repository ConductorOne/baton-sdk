package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Composed end-to-end test for the lookup continuation over the REAL
// Lambda stack: a connectorbuilder-built connector (unchanged connector
// code, lookup-before-fetch, errors propagated) registered on the actual
// gRPC-over-Lambda server, reached through the actual transport encodings
// (Request/Response MarshalJSON/UnmarshalJSON — dual-encoded requests,
// frame/legacy responses), driven by the real syncer. This is the
// production Lambda topology minus AWS: every layer between the syncer's
// bounce loop and the connector's SyncOpAttrs lookup is the shipping code
// path, including the wire round trip of the Offer/Ask/Answers
// annotations.

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/connector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorclient"
	c1lambda "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// jsonWireTransport is the Lambda invoke minus AWS: the request is
// marshaled to the invoke payload and unmarshaled server-side, the
// response likewise back — both through the exact MarshalJSON /
// UnmarshalJSON paths the production transport uses.
type jsonWireTransport struct {
	server *c1lambda.Server
}

func (t jsonWireTransport) RoundTrip(ctx context.Context, req *c1lambda.Request) (*c1lambda.Response, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("wire: marshal request: %w", err)
	}
	serverReq := &c1lambda.Request{}
	if err := json.Unmarshal(payload, serverReq); err != nil {
		return nil, fmt.Errorf("wire: unmarshal request: %w", err)
	}
	resp, err := t.server.Handler(ctx, serverReq)
	if err != nil {
		return nil, err
	}
	respPayload, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("wire: marshal response: %w", err)
	}
	clientResp := &c1lambda.Response{}
	if err := json.Unmarshal(respPayload, clientResp); err != nil {
		return nil, fmt.Errorf("wire: unmarshal response: %w", err)
	}
	return clientResp, nil
}

// lambdaE2EBuilder is a plain ConnectorBuilderV2 — the shape a real
// connector ships. No SetLookup, no test hooks in the sync path.
type lambdaE2EBuilder struct {
	syncer *lambdaE2ESyncer
}

func (b *lambdaE2EBuilder) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{DisplayName: "lambda-e2e"}.Build(), nil
}

func (b *lambdaE2EBuilder) Validate(context.Context) (annotations.Annotations, error) {
	return annotations.New(v2.SourceCacheCapability_builder{
		Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
	}.Build()), nil
}

func (b *lambdaE2EBuilder) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{b.syncer, &lambdaE2EUserSyncer{}}
}

type lambdaE2EUserSyncer struct{}

func (s *lambdaE2EUserSyncer) ResourceType(context.Context) *v2.ResourceType {
	return userResourceType
}

func (s *lambdaE2EUserSyncer) List(_ context.Context, _ *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	var out []*v2.Resource
	for i := 0; i < 4; i++ {
		u, err := rs.NewUserResource(fmt.Sprintf("user-%02d", i), userResourceType, fmt.Sprintf("user-%02d", i), nil,
			rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
		if err != nil {
			return nil, nil, err
		}
		out = append(out, u)
	}
	return out, &rs.SyncOpResults{}, nil
}

func (s *lambdaE2EUserSyncer) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, &rs.SyncOpResults{}, nil
}

func (s *lambdaE2EUserSyncer) Grants(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, &rs.SyncOpResults{}, nil
}

// lambdaE2ESyncer serves group member grants with per-group scopes,
// written exactly the way the contract tells connector authors to:
// lookup FIRST (propagating errors with %w), replay on hit, cold + scope
// on miss.
type lambdaE2ESyncer struct {
	mu          sync.Mutex
	members     map[string][]string
	etagByGroup map[string]string
	lookups     int
	replays     int
	colds       int
}

func (s *lambdaE2ESyncer) ResourceType(context.Context) *v2.ResourceType {
	return groupResourceType
}

func (s *lambdaE2ESyncer) group(gid string) (*v2.Resource, error) {
	return rs.NewGroupResource(gid, groupResourceType, gid, nil)
}

func (s *lambdaE2ESyncer) List(_ context.Context, _ *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*v2.Resource
	for gid := range s.members {
		g, err := s.group(gid)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, g)
	}
	return out, &rs.SyncOpResults{}, nil
}

func (s *lambdaE2ESyncer) Entitlements(_ context.Context, r *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	ent := et.NewAssignmentEntitlement(r, "member", et.WithGrantableTo(userResourceType))
	ent.SetSlug("member")
	return []*v2.Entitlement{ent}, &rs.SyncOpResults{}, nil
}

func (s *lambdaE2ESyncer) Grants(ctx context.Context, r *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	gid := r.GetId().GetResource()
	scope := "e2e/groups/" + gid + "/members"

	s.mu.Lock()
	s.lookups++
	etag := s.etagByGroup[gid]
	uids := append([]string(nil), s.members[gid]...)
	s.mu.Unlock()

	entry, found, err := opts.SourceCache.LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, scope)
	if err != nil {
		return nil, nil, fmt.Errorf("revalidating %s: %w", gid, err)
	}
	ret := &rs.SyncOpResults{Annotations: annotations.Annotations{}}
	if found && entry.ETag == etag {
		s.mu.Lock()
		s.replays++
		s.mu.Unlock()
		ret.Annotations.Update(v2.SourceCacheReplay_builder{ScopeHash: scope, Etag: etag}.Build())
		return nil, ret, nil
	}

	s.mu.Lock()
	s.colds++
	s.mu.Unlock()
	grants := make([]*v2.Grant, 0, len(uids))
	for _, uid := range uids {
		u, err := rs.NewUserResource(uid, userResourceType, uid, nil)
		if err != nil {
			return nil, nil, err
		}
		grants = append(grants, gt.NewGrant(r, "member", u.GetId()))
	}
	ret.Annotations.Update(v2.SourceCacheScope_builder{ScopeHash: scope, Etag: etag}.Build())
	return grants, ret, nil
}

// TestSourceCacheContinuation_LambdaStackE2E composes the shipping layers:
// connectorbuilder (per-request ContinuationLookup + deferral interception)
// behind internal/connector.Register on the gRPC-over-Lambda server,
// through the real JSON wire encodings, under the real syncer bounce loop.
func TestSourceCacheContinuation_LambdaStackE2E(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	es := &lambdaE2ESyncer{
		members: map[string][]string{
			"group-00": {"user-00", "user-01"},
			"group-01": {"user-02", "user-03"},
		},
		etagByGroup: map[string]string{"group-00": "e1", "group-01": "e1"},
	}
	cb := &lambdaE2EBuilder{syncer: es}

	// The Lambda server path: connectorbuilder WITHOUT any runner-supplied
	// lookup (RunTimeOpts.SourceCacheLookup is not wired on Lambda), so
	// the continuation is the only lookup transport available.
	cs, err := connectorbuilder.NewConnector(ctx, cb)
	require.NoError(t, err)
	server := c1lambda.NewServer(nil)
	connector.Register(ctx, server, cs, nil)
	client := connectorclient.NewConnectorClient(ctx, c1lambda.NewClientConn(jsonWireTransport{server: server}))

	// Sync 1: cold. No previous c1z → no offer → the connector's lookup is
	// NoopLookup; everything fetches cold with zero bounces.
	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	require.NoError(t, runContinuationSync(ctx, t, client, sync1, "", tmpDir, 0))
	require.Equal(t, 2, es.colds)
	require.Zero(t, es.replays)
	grants1 := listGrantsInFile(ctx, t, sync1)
	require.Len(t, grants1, 4)

	// Sync 2: warm. The offer rides the wire, the connector's first
	// execution defers (ErrLookupDeferred through the builder, ask through
	// the transport), the re-invoke carries answers, and both groups
	// replay. lookups counts handler EXECUTIONS: 2 phase-1 + 2 phase-2.
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	require.NoError(t, runContinuationSync(ctx, t, client, sync2, sync1, tmpDir, 0))
	require.Equal(t, 2, es.replays, "both groups replay via ask/answer over the wire")
	require.Equal(t, 2, es.colds, "no cold fetches on the warm sync")
	require.Equal(t, 6, es.lookups, "2 cold + 2 deferred phase-1 + 2 answered phase-2 handler executions")
	requireSameGrantIDs(t, grants1, listGrantsInFile(ctx, t, sync2), "lambda-stack warm vs cold")

	// Churn one group; warm sync 3: stale answer → cold refetch for it,
	// replay for the other; equivalence against a cold control.
	es.mu.Lock()
	es.members["group-01"] = []string{"user-02"}
	es.etagByGroup["group-01"] = "e2"
	es.mu.Unlock()

	sync3 := filepath.Join(tmpDir, "sync3.c1z")
	require.NoError(t, runContinuationSync(ctx, t, client, sync3, sync2, tmpDir, 0))
	require.Equal(t, 3, es.replays)
	require.Equal(t, 3, es.colds)

	control := filepath.Join(tmpDir, "control.c1z")
	require.NoError(t, runContinuationSync(ctx, t, client, control, "", tmpDir, 0))
	requireSameGrantIDs(t, listGrantsInFile(ctx, t, control), listGrantsInFile(ctx, t, sync3), "churned lambda-stack warm vs cold control")
}
