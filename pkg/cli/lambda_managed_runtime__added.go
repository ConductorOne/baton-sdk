//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

const lambdaConnectorDrainTimeout = 30 * time.Second

type managedRuntimeFetcher struct {
	client     v1.ConnectorConfigServiceClient
	privateKey ed25519.PrivateKey
}

func newManagedRuntimeFetcher(client v1.ConnectorConfigServiceClient, privateKey ed25519.PrivateKey) *managedRuntimeFetcher {
	return &managedRuntimeFetcher{
		client:     client,
		privateKey: privateKey,
	}
}

func (f *managedRuntimeFetcher) Fetch(ctx context.Context) (*ManagedRuntimeSnapshot, error) {
	resp, err := f.client.GetManagedRuntimeSnapshot(ctx, &v1.GetManagedRuntimeSnapshotRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return f.fetchLegacyConfig(ctx)
		}
		return nil, err
	}

	snapshot := resp.GetSnapshot()
	if snapshot == nil {
		return nil, fmt.Errorf("managed runtime snapshot response was empty")
	}

	return f.decryptSnapshot(snapshot.GetRevision(), snapshot.GetEncryptedConfig(), snapshot.GetAssets())
}

func (f *managedRuntimeFetcher) fetchLegacyConfig(ctx context.Context) (*ManagedRuntimeSnapshot, error) {
	resp, err := f.client.GetConnectorConfig(ctx, &v1.GetConnectorConfigRequest{})
	if err != nil {
		return nil, err
	}

	revision := ""
	if ts := resp.GetLastUpdated(); ts != nil {
		revision = ts.AsTime().UTC().Format(time.RFC3339Nano)
	}

	return f.decryptSnapshot(revision, resp.GetConfig(), nil)
}

func (f *managedRuntimeFetcher) decryptSnapshot(revision string, encryptedConfig []byte, pbAssets []*v1.ManagedRuntimeAsset) (*ManagedRuntimeSnapshot, error) {
	decrypted, err := jwk.DecryptED25519(f.privateKey, encryptedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}

	configStruct := structpb.Struct{}
	err = json.Unmarshal(decrypted, &configStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal decrypted config: %w", err)
	}

	assets := make([]ManagedRuntimeAsset, 0, len(pbAssets))
	for _, asset := range pbAssets {
		if asset == nil {
			continue
		}
		assets = append(assets, ManagedRuntimeAsset{
			Role:      asset.GetRole(),
			MediaType: asset.GetMediaType(),
			Bytes:     append([]byte(nil), asset.GetBody()...),
		})
	}

	return &ManagedRuntimeSnapshot{
		Revision: revision,
		Config:   &configStruct,
		Assets:   assets,
	}, nil
}

type lambdaConnectorGeneration struct {
	server    *c1_lambda_grpc.Server
	connector types.ConnectorServer
	revision  string
	active    sync.WaitGroup
}

type lambdaConnectorBuildFunc func(context.Context, *ManagedRuntimeSnapshot) (*lambdaConnectorGeneration, error)

type lambdaConnectorReloader struct {
	mu      sync.RWMutex
	reload  sync.Mutex
	current *lambdaConnectorGeneration

	fetch func(context.Context) (*ManagedRuntimeSnapshot, error)
	build lambdaConnectorBuildFunc
}

func newLambdaConnectorReloader(
	initial *lambdaConnectorGeneration,
	fetch func(context.Context) (*ManagedRuntimeSnapshot, error),
	build lambdaConnectorBuildFunc,
) *lambdaConnectorReloader {
	return &lambdaConnectorReloader{
		current: initial,
		fetch:   fetch,
		build:   build,
	}
}

func (r *lambdaConnectorReloader) Handler(ctx context.Context, req *c1_lambda_grpc.Request) (*c1_lambda_grpc.Response, error) {
	if revision := requestedManagedRuntimeRevision(req); revision != "" && revision != r.currentRevision() {
		if err := r.reloadToLatest(ctx); err != nil {
			return c1_lambda_grpc.ErrorResponse(status.Errorf(codes.Unavailable, "failed to refresh runtime snapshot: %v", err)), nil
		}
	}

	gen := r.acquireGeneration()
	defer gen.active.Done()
	return gen.server.Handler(ctx, req)
}

func requestedManagedRuntimeRevision(req *c1_lambda_grpc.Request) string {
	values := req.Headers().Get(ManagedRuntimeRevisionHeader)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (r *lambdaConnectorReloader) currentRevision() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return ""
	}
	return r.current.revision
}

func (r *lambdaConnectorReloader) acquireGeneration() *lambdaConnectorGeneration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.current.active.Add(1)
	return r.current
}

func (r *lambdaConnectorReloader) reloadToLatest(ctx context.Context) error {
	r.reload.Lock()
	defer r.reload.Unlock()

	snapshot, err := r.fetch(ctx)
	if err != nil {
		return err
	}

	if snapshot.GetRevision() == r.currentRevision() {
		return nil
	}

	next, err := r.build(ctx, snapshot)
	if err != nil {
		return err
	}

	r.mu.Lock()
	previous := r.current
	r.current = next
	r.mu.Unlock()

	go drainAndCloseLambdaGeneration(context.WithoutCancel(ctx), previous)
	return nil
}

func drainAndCloseLambdaGeneration(ctx context.Context, gen *lambdaConnectorGeneration) {
	if gen == nil || gen.connector == nil {
		return
	}

	done := make(chan struct{})
	go func() {
		gen.active.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(lambdaConnectorDrainTimeout):
		zap.L().Warn("timed out draining previous lambda connector generation", zap.String("revision", gen.revision))
	}

	closeCtx, cancel := context.WithTimeout(ctx, lambdaConnectorDrainTimeout)
	defer cancel()
	if err := closeManagedConnector(closeCtx, gen.connector); err != nil {
		zap.L().Warn("failed to close previous lambda connector generation", zap.String("revision", gen.revision), zap.Error(err))
	}
}

func closeManagedConnector(ctx context.Context, connector types.ConnectorServer) error {
	switch c := any(connector).(type) {
	case interface{ Close(context.Context) error }:
		return c.Close(ctx)
	case io.Closer:
		return c.Close()
	default:
		return nil
	}
}
