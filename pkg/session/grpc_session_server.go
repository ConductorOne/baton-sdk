package session

import (
	"context"
	"fmt"
	"log"
	"net"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
	"google.golang.org/grpc"
)

var _ v1.BatonSessionServiceServer = (*GRPCSessionServer)(nil)

type GRPCSessionServer struct {
	// v1.UnimplementedBatonSessionServiceServer
	store types.SessionStore
}

func NewGRPCSessionServer() *GRPCSessionServer {
	return &GRPCSessionServer{}
}

type SetSessionStore interface {
	SetSessionStore(ctx context.Context, store types.SessionStore)
}

func (s *GRPCSessionServer) SetSessionStore(ctx context.Context, store types.SessionStore) {
	s.store = store
}

func (s *GRPCSessionServer) Get(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	value, found, err := s.store.Get(ctx, req.Key, types.WithSyncID(req.SyncId))
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
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	values, err := s.store.GetMany(ctx, req.Keys, types.WithSyncID(req.SyncId))
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
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.store.Set(ctx, req.Key, req.Value, types.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to set value in cache: %w", err)
	}

	return &v1.SetResponse{}, nil
}

func (s *GRPCSessionServer) SetMany(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.store.SetMany(ctx, req.Values, types.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to set many values in cache: %w", err)
	}

	return &v1.SetManyResponse{}, nil
}

func (s *GRPCSessionServer) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.store.Delete(ctx, req.Key, types.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to delete value from cache: %w", err)
	}

	return &v1.DeleteResponse{}, nil
}

func (s *GRPCSessionServer) DeleteMany(ctx context.Context, req *v1.DeleteManyRequest) (*v1.DeleteManyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	for _, key := range req.Keys {
		err := s.store.Delete(ctx, key, types.WithSyncID(req.SyncId))
		if err != nil {
			return nil, fmt.Errorf("failed to delete value for key %s: %w", key, err)
		}
	}

	return &v1.DeleteManyResponse{}, nil
}

func (s *GRPCSessionServer) Clear(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.store.Clear(ctx, types.WithSyncID(req.SyncId))
	if err != nil {
		return nil, fmt.Errorf("failed to clear cache: %w", err)
	}

	return &v1.ClearResponse{}, nil
}

func (s *GRPCSessionServer) GetAll(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	values, err := s.store.GetAll(ctx, types.WithSyncID(req.SyncId))
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

func StartGRPCSessionServer(ctx context.Context, addr string, sessionServer *GRPCSessionServer) error {
	// Create the gRPC server
	server := grpc.NewServer()

	// Create and register the session service
	v1.RegisterBatonSessionServiceServer(server, sessionServer)

	// Create listener
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
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

func StartGRPCSessionServerWithOptions(ctx context.Context, listener net.Listener, sessionServer v1.BatonSessionServiceServer, opts ...grpc.ServerOption) error {
	// Create the gRPC server with custom options
	server := grpc.NewServer(opts...)

	// grpc.Creds(credentials.NewTLS(tlsConfig))
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
