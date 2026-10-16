package session

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ v1.BatonSessionServiceServer = (*GRPCSessionServer)(nil)
var _ sessions.SessionStoreBinder = (*GRPCSessionServer)(nil)

type GRPCSessionServer struct {
	mu       sync.RWMutex
	bindings map[string]*boundSessionStore
}

type boundSessionStore struct {
	store    sessions.SessionStore
	inflight sync.WaitGroup
}

func NewGRPCSessionServer() *GRPCSessionServer {
	return &GRPCSessionServer{}
}

func (s *GRPCSessionServer) BindSessionStore(ctx context.Context, syncID string, store sessions.SessionStore) (sessions.UnbindSessionStore, error) {
	if syncID == "" {
		return nil, errors.New("session store binding requires a sync id")
	}
	if store == nil {
		return nil, fmt.Errorf("session store binding for sync %s has no store", syncID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.bindings[syncID]; ok {
		return nil, fmt.Errorf("session store already bound for sync %s", syncID)
	}
	if s.bindings == nil {
		s.bindings = make(map[string]*boundSessionStore)
	}
	b := &boundSessionStore{store: store}
	s.bindings[syncID] = b
	var once sync.Once
	return func(ctx context.Context) error {
		var err error
		once.Do(func() { err = s.unbind(ctx, syncID, b) })
		return err
	}, nil
}

func (s *GRPCSessionServer) unbind(ctx context.Context, syncID string, b *boundSessionStore) error {
	s.mu.Lock()
	delete(s.bindings, syncID)
	s.mu.Unlock()

	drained := make(chan struct{})
	go func() {
		b.inflight.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("session store for sync %s still has requests in flight: %w", syncID, context.Cause(ctx))
	}
}

// The caller runs the returned func when the request is done; unbind waits on it.
func (s *GRPCSessionServer) acquire(syncID string) (sessions.SessionStore, func(), error) {
	if syncID == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "session request has no sync id")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bindings[syncID]
	if !ok {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "no session store bound for sync %s", syncID)
	}
	b.inflight.Add(1)
	return b.store, b.inflight.Done, nil
}

func (s *GRPCSessionServer) Get(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if err != nil {
		return nil, err
	}
	defer release()

	value, found, err := store.Get(ctx, req.GetKey(), sessions.WithSyncID(req.GetSyncId()), sessions.WithPrefix(req.GetPrefix()))
	if err != nil {
		return nil, fmt.Errorf("failed to get value from cache: %w", err)
	}

	return v1.GetResponse_builder{
		Value: value,
		Found: found,
	}.Build(), nil
}

func (s *GRPCSessionServer) GetMany(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if err != nil {
		return nil, err
	}
	defer release()

	values, unprocessedKeys, err := store.GetMany(
		ctx,
		req.GetKeys(),
		sessions.WithSyncID(req.GetSyncId()),
		sessions.WithPrefix(req.GetPrefix()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get many values from cache: %w", err)
	}

	// Convert the map to items array
	items := make([]*v1.GetManyItem, 0, len(values))
	for key, value := range values {
		items = append(items, v1.GetManyItem_builder{
			Key:   key,
			Value: value,
		}.Build())
	}

	return v1.GetManyResponse_builder{
		Items:           items,
		UnprocessedKeys: unprocessedKeys,
	}.Build(), nil
}

func (s *GRPCSessionServer) Set(ctx context.Context, req *v1.SetRequest) (*v1.SetResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if err != nil {
		return nil, err
	}
	defer release()

	err = store.Set(ctx, req.GetKey(), req.GetValue(), sessions.WithSyncID(req.GetSyncId()), sessions.WithPrefix(req.GetPrefix()))
	if err != nil {
		return nil, fmt.Errorf("failed to set value in cache: %w", err)
	}

	return &v1.SetResponse{}, nil
}

func (s *GRPCSessionServer) SetMany(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if err != nil {
		return nil, err
	}
	defer release()

	err = store.SetMany(ctx, req.GetValues(), sessions.WithSyncID(req.GetSyncId()), sessions.WithPrefix(req.GetPrefix()))
	if err != nil {
		return nil, fmt.Errorf("failed to set many values in cache: %w", err)
	}

	return &v1.SetManyResponse{}, nil
}

func (s *GRPCSessionServer) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if err != nil {
		return nil, err
	}
	defer release()

	err = store.Delete(ctx, req.GetKey(), sessions.WithSyncID(req.GetSyncId()), sessions.WithPrefix(req.GetPrefix()))
	if err != nil {
		return nil, fmt.Errorf("failed to delete value from cache: %w", err)
	}

	return &v1.DeleteResponse{}, nil
}

func (s *GRPCSessionServer) DeleteMany(ctx context.Context, req *v1.DeleteManyRequest) (*v1.DeleteManyResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if err != nil {
		return nil, err
	}
	defer release()

	for _, key := range req.GetKeys() {
		err := store.Delete(
			ctx,
			key,
			sessions.WithSyncID(req.GetSyncId()),
			sessions.WithPrefix(req.GetPrefix()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to delete value for key %s: %w", key, err)
		}
	}

	return &v1.DeleteManyResponse{}, nil
}

func (s *GRPCSessionServer) Clear(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if status.Code(err) == codes.FailedPrecondition {
		ctxzap.Extract(ctx).Warn("no session store bound to clear", zap.String("sync_id", req.GetSyncId()))
		return &v1.ClearResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer release()

	err = store.Clear(ctx, sessions.WithSyncID(req.GetSyncId()), sessions.WithPrefix(req.GetPrefix()))
	if err != nil {
		return nil, fmt.Errorf("failed to clear cache: %w", err)
	}

	return &v1.ClearResponse{}, nil
}

func (s *GRPCSessionServer) GetAll(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
	store, release, err := s.acquire(req.GetSyncId())
	if err != nil {
		return nil, err
	}
	defer release()

	values, nextPageToken, err := store.GetAll(
		ctx,
		req.PageToken,
		sessions.WithSyncID(req.GetSyncId()),
		sessions.WithPrefix(req.GetPrefix()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get all values from cache: %w", err)
	}

	// Convert the map to items array
	items := make([]*v1.GetAllItem, 0, len(values))
	for key, value := range values {
		items = append(items, v1.GetAllItem_builder{
			Key:   key,
			Value: value,
		}.Build())
	}

	return v1.GetAllResponse_builder{
		Items:     items,
		PageToken: nextPageToken,
	}.Build(), nil
}

func StartGRPCSessionServerWithOptions(ctx context.Context, listener net.Listener, sessionServer v1.BatonSessionServiceServer, opts ...grpc.ServerOption) error {
	// Create the gRPC server with custom options
	server := grpc.NewServer(opts...)

	// Create and register the session service
	v1.RegisterBatonSessionServiceServer(server, sessionServer)

	defer listener.Close()

	// Start serving
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("gRPC session server failed: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	server.GracefulStop()

	return nil
}
