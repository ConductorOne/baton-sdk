package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/conductorone/baton-sdk/internal/connector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/connectorclient"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

// expiringPageServer is an HTTP list endpoint in the shape of a provider
// that expires its continuation tokens. Tokens carry a generation; the
// first request for page two bumps the generation and answers 400, so
// every token issued before it is dead. Page one is a plain 200 GET, which
// uhttp caches.
type expiringPageServer struct {
	mu      native_sync.Mutex
	gen     int
	expired bool
	hits    map[string]int
}

type expiringPage struct {
	Value    []string `json:"value"`
	NextLink string   `json:"nextLink"`
}

func (s *expiringPageServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := r.URL.Query().Get("token")
	s.hits[token]++
	w.Header().Set("Content-Type", "application/json")
	var page string
	var gen int
	if token != "" {
		if _, err := fmt.Sscanf(token, "%1s.g%d", &page, &gen); err != nil || gen != s.gen {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"token expired"}`))
			return
		}
		if page == "1" && !s.expired {
			s.expired = true
			s.gen++
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"token expired"}`))
			return
		}
	}
	var body expiringPage
	switch token {
	case "":
		body = expiringPage{Value: []string{"g-p0"}, NextLink: fmt.Sprintf("1.g%d", s.gen)}
	default:
		switch page {
		case "1":
			body = expiringPage{Value: []string{"g-p1"}, NextLink: fmt.Sprintf("2.g%d", s.gen)}
		default:
			body = expiringPage{Value: []string{"g-p2"}}
		}
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (s *expiringPageServer) hitsFor(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[token]
}

// httpGroupSyncer lists groups through a uhttp client, the way a built
// connector does, and answers the provider's 400 with the helper's error
// passed through wrap (identity when nil).
type httpGroupSyncer struct {
	client *uhttp.BaseHttpClient
	base   *url.URL
	wrap   func(error) error
}

func (s *httpGroupSyncer) ResourceType(context.Context) *v2.ResourceType { return groupResourceType }

func (s *httpGroupSyncer) List(ctx context.Context, _ *v2.ResourceId, opts resource.SyncOpAttrs) ([]*v2.Resource, *resource.SyncOpResults, error) {
	target := *s.base
	q := target.Query()
	q.Set("token", opts.PageToken.Token)
	target.RawQuery = q.Encode()
	req, err := s.client.NewRequest(ctx, http.MethodGet, &target)
	if err != nil {
		return nil, nil, err
	}
	var page expiringPage
	resp, err := s.client.Do(req, uhttp.WithJSONResponse(&page))
	if resp != nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusBadRequest {
			restart := pagination.NewRestartActionError("", "provider: token expired")
			if s.wrap != nil {
				restart = s.wrap(restart)
			}
			return nil, nil, restart
		}
	}
	if err != nil {
		return nil, nil, err
	}
	out := make([]*v2.Resource, 0, len(page.Value))
	for _, id := range page.Value {
		group, err := resource.NewGroupResource(id, groupResourceType, id, nil)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, group)
	}
	return out, &resource.SyncOpResults{NextPageToken: page.NextLink}, nil
}

func (s *httpGroupSyncer) Entitlements(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return nil, nil, nil
}

func (s *httpGroupSyncer) Grants(context.Context, *v2.Resource, resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	return nil, nil, nil
}

type httpGroupConnector struct{ syncer *httpGroupSyncer }

func (c *httpGroupConnector) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{DisplayName: "http-groups"}.Build(), nil
}
func (c *httpGroupConnector) Validate(context.Context) (annotations.Annotations, error) {
	return nil, nil
}
func (c *httpGroupConnector) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{c.syncer}
}

// grpcClientFor serves a built connector over an in-memory gRPC server, so
// the helper's error crosses real status serialization.
func grpcClientFor(t *testing.T, ctx context.Context, server types.ConnectorServer) types.ConnectorClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	connector.Register(ctx, grpcServer, server, nil)
	go func() { _ = grpcServer.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///http-groups",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	})
	return connectorclient.NewConnectorClient(ctx, conn)
}

// The reference case with the real HTTP layer in place: a built connector
// lists through uhttp (response cache on), the provider expires the page
// two token once, the connector returns the helper's error, and the sync
// finishes with every group. The restart's page one reaches the provider
// (two hits for token "") instead of replaying from the cache; without the
// cache clear in connectorbuilder the cached chain leads back to the dead
// token until the per-item cap fails the sync.
//
// The error crosses a real gRPC boundary in each shape a connector author
// is likely to return: bare, under %w, and behind another status error
// (uhttp.WrapErrors, errors.Join after a status). Without
// pagination.SurfaceRestartAction in connectorbuilder the last two
// serialize as the outer status and the sync fails instead of restarting.
func TestActionInvalidationThroughHTTPCache(t *testing.T) {
	for name, wrap := range map[string]func(error) error{
		"bare":    nil,
		"wrapped": func(err error) error { return fmt.Errorf("list groups: %w", err) },
		"behind uhttp.WrapErrors": func(err error) error {
			return uhttp.WrapErrors(codes.Unavailable, "503 Service Unavailable", err)
		},
		"joined after a status": func(err error) error {
			return errors.Join(status.Error(codes.Internal, "http 400"), err)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			provider := &expiringPageServer{hits: map[string]int{}}
			server := httptest.NewServer(provider)
			defer server.Close()
			client, err := uhttp.NewBaseHttpClientWithContext(ctx, server.Client())
			require.NoError(t, err)
			base, err := url.Parse(server.URL + "/groups")
			require.NoError(t, err)

			probe, err := url.Parse(server.URL + "/groups?token=2.g0")
			require.NoError(t, err)
			for range 2 {
				req, err := client.NewRequest(ctx, http.MethodGet, probe)
				require.NoError(t, err)
				resp, err := client.Do(req)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
			}
			if provider.hitsFor("2.g0") != 1 {
				t.Skip("uhttp cache disabled in this environment")
			}

			built, err := connectorbuilder.NewConnector(ctx, &httpGroupConnector{syncer: &httpGroupSyncer{client: client, base: base, wrap: wrap}})
			require.NoError(t, err)
			f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "http.c1z"), false)
			s, err := NewSyncer(ctx, grpcClientFor(t, ctx, built), WithConnectorStore(f.store), WithWorkerCount(1), WithSkipEntitlementsAndGrants(true))
			require.NoError(t, err)
			require.NoError(t, s.Sync(ctx))

			require.Equal(t, []string{"g-p0", "g-p1", "g-p2"}, storedResourceIDs(t, f.store, groupResourceType.GetId()))
			require.Equal(t, 2, provider.hitsFor(""), "page one fetched again after the restart, not replayed from cache")
			require.Equal(t, 1, provider.hitsFor("1.g0"), "the dead token was tried once")
			require.Equal(t, 1, provider.hitsFor("1.g1"))
			_, total := s.(*syncer).run.restartCounts()
			require.EqualValues(t, 1, total)
		})
	}
}
