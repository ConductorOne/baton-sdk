package session

import (
	"context"
	"fmt"
	"log"
	"net"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"google.golang.org/grpc"
)

var _ v1.BatonSessionServiceServer = (*GRPCSessionServer)(nil)

type GRPCSessionServer struct {
	// v1.UnimplementedBatonSessionServiceServer
	store sessions.SessionStore
}

func NewGRPCSessionServer() *GRPCSessionServer {
	return &GRPCSessionServer{}
}

type SetSessionStore interface {
	SetSessionStore(ctx context.Context, store sessions.SessionStore)
}

func (s *GRPCSessionServer) SetSessionStore(ctx context.Context, store sessions.SessionStore) {
	s.store = store
}

func (s *GRPCSessionServer) Validate() error {
	if s.store == nil {
		return fmt.Errorf("session store is not set")
	}

	return nil
}

func (s *GRPCSessionServer) Get(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	value, found, err := s.store.Get(ctx, req.Key, sessions.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to get value from cache: %w", err)
	}

	if !found {
		return &v1.GetResponse{
			Value: nil,
		}, nil
	}

	return &v1.GetResponse{
		Value: value,
	}, nil
}

func (s *GRPCSessionServer) GetMany(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	values, err := s.store.GetMany(ctx, req.Keys, sessions.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to get many values from cache: %w", err)
	}

	// Convert the map to items array
	items := make([]*v1.GetManyItem, 0, len(values))
	for key, value := range values {
		items = append(items, &v1.GetManyItem{
			Key:   key,
			Value: value,
		})
	}

	return &v1.GetManyResponse{
		Items: items,
	}, nil
}

func (s *GRPCSessionServer) Set(ctx context.Context, req *v1.SetRequest) (*v1.SetResponse, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	err := s.store.Set(ctx, req.Key, req.Value, sessions.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to set value in cache: %w", err)
	}

	return &v1.SetResponse{}, nil
}

func (s *GRPCSessionServer) SetMany(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	err := s.store.SetMany(ctx, req.Values, sessions.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to set many values in cache: %w", err)
	}

	return &v1.SetManyResponse{}, nil
}

func (s *GRPCSessionServer) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	err := s.store.Delete(ctx, req.Key, sessions.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to delete value from cache: %w", err)
	}

	return &v1.DeleteResponse{}, nil
}

func (s *GRPCSessionServer) DeleteMany(ctx context.Context, req *v1.DeleteManyRequest) (*v1.DeleteManyResponse, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	for _, key := range req.Keys {
		err := s.store.Delete(ctx, key, sessions.WithSyncID(req.SyncId))
		if err != nil {
			return nil, fmt.Errorf("failed to delete value for key %s: %w", key, err)
		}
	}

	return &v1.DeleteManyResponse{}, nil
}

func (s *GRPCSessionServer) Clear(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error) {
	if s.store == nil {
		// we sometimes clean up the session store after the connector is done
		ctxzap.Extract(ctx).Warn("session store is not set")
		return &v1.ClearResponse{}, nil
	}

	err := s.store.Clear(ctx, sessions.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to clear cache: %w", err)
	}

	return &v1.ClearResponse{}, nil
}

func (s *GRPCSessionServer) GetAll(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	values, err := s.store.GetAll(ctx, sessions.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to get all values from cache: %w", err)
	}

	// Convert the map to items array
	items := make([]*v1.GetAllItem, 0, len(values))
	for key, value := range values {
		items = append(items, &v1.GetAllItem{
			Key:   key,
			Value: value,
		})
	}

	return &v1.GetAllResponse{
		Items: items,
	}, nil
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
