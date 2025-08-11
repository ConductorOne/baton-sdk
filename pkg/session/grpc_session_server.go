package session

import (
	"context"
	"fmt"
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
	SetSessionStore(store types.SessionStore)
}

func (s *GRPCSessionServer) SetSessionStore(store types.SessionStore) {
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

func (s *GRPCSessionServer) GetMany(req *v1.GetManyRequest, stream v1.BatonSessionService_GetManyServer) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}

	values, err := s.store.GetMany(stream.Context(), req.Keys, types.WithSyncID(req.SyncId))
	if err != nil {
		return fmt.Errorf("failed to get many values from cache: %w", err)
	}

	for key, value := range values {
		resp := &v1.GetManyResponse{
			Key:   key,
			Value: value,
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}

	return nil
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

func (s *GRPCSessionServer) DeleteMany(context.Context, *v1.DeleteManyRequest) (*v1.DeleteManyResponse, error) {
	panic("unimplemented")
}

func (s *GRPCSessionServer) GetAll(req *v1.GetAllRequest, stream v1.BatonSessionService_GetAllServer) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}

	values, err := s.store.GetAll(stream.Context(), types.WithSyncID(req.SyncId))
	if err != nil {
		return fmt.Errorf("failed to get all values from cache: %w", err)
	}

	// Send all key-value pairs
	for key, value := range values {
		resp := &v1.GetAllResponse{
			Key:   key,
			Value: value,
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("failed to send response: %w", err)
		}
	}

	return nil
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
			// Log error but don't return it since this is running in a goroutine
			fmt.Printf("gRPC session server failed: %v\n", err)
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
			// Log error but don't return it since this is running in a goroutine
			fmt.Printf("gRPC session server failed: %v\n", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Graceful shutdown
	server.GracefulStop()

	return nil
}
