package connector

import (
	"context"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SessionService implements BatonSessionServiceServer interface
type SessionService struct {
	v1.UnimplementedBatonSessionServiceServer
	sessionCache types.SessionStore
}

// NewSessionService creates a new session service with the given session cache
func NewSessionService(sessionCache types.SessionStore) *SessionService {
	return &SessionService{
		sessionCache: sessionCache,
	}
}

// Get retrieves a value from the session cache
func (s *SessionService) Get(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	if req.SyncId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sync_id is required")
	}
	if req.Key == "" {
		return nil, status.Errorf(codes.InvalidArgument, "key is required")
	}

	// Set sync ID in context
	ctx = types.SetSyncIDInContext(ctx, req.SyncId)

	value, found, err := s.sessionCache.Get(ctx, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get value: %v", err)
	}

	if !found {
		return &v1.GetResponse{}, nil
	}

	return &v1.GetResponse{Value: value}, nil
}

// GetMany retrieves multiple values from the session cache
func (s *SessionService) GetMany(req *v1.GetManyRequest, stream v1.BatonSessionService_GetManyServer) error {
	if req.SyncId == "" {
		return status.Errorf(codes.InvalidArgument, "sync_id is required")
	}
	if len(req.Keys) == 0 {
		return status.Errorf(codes.InvalidArgument, "keys are required")
	}

	// Set sync ID in context
	ctx := types.SetSyncIDInContext(stream.Context(), req.SyncId)

	values, err := s.sessionCache.GetMany(ctx, req.Keys)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get values: %v", err)
	}

	// Stream responses
	for key, value := range values {
		resp := &v1.GetManyResponse{
			Key:   key,
			Value: value,
		}
		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "failed to send response: %v", err)
		}
	}

	return nil
}

// GetAll retrieves all values from the session cache
func (s *SessionService) GetAll(req *v1.GetAllRequest, stream v1.BatonSessionService_GetAllServer) error {
	if req.SyncId == "" {
		return status.Errorf(codes.InvalidArgument, "sync_id is required")
	}

	// Set sync ID in context
	ctx := types.SetSyncIDInContext(stream.Context(), req.SyncId)

	values, err := s.sessionCache.GetAll(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get all values: %v", err)
	}

	// Stream responses
	for key, value := range values {
		resp := &v1.GetAllResponse{
			Key:   key,
			Value: value,
		}
		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "failed to send response: %v", err)
		}
	}

	return nil
}

// Set stores a value in the session cache
func (s *SessionService) Set(ctx context.Context, req *v1.SetRequest) (*v1.SetResponse, error) {
	if req.SyncId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sync_id is required")
	}
	if req.Key == "" {
		return nil, status.Errorf(codes.InvalidArgument, "key is required")
	}

	// Set sync ID in context
	ctx = types.SetSyncIDInContext(ctx, req.SyncId)

	err := s.sessionCache.Set(ctx, req.Key, req.Value)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set value: %v", err)
	}

	return &v1.SetResponse{}, nil
}

// SetMany stores multiple values in the session cache
func (s *SessionService) SetMany(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
	if req.SyncId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sync_id is required")
	}
	if len(req.Values) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "values are required")
	}

	// Set sync ID in context
	ctx = types.SetSyncIDInContext(ctx, req.SyncId)

	// Convert map[string][]byte to map[string][]byte
	values := make(map[string][]byte)
	for key, value := range req.Values {
		values[key] = value
	}

	err := s.sessionCache.SetMany(ctx, values)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set values: %v", err)
	}

	return &v1.SetManyResponse{}, nil
}

// Delete removes a value from the session cache
func (s *SessionService) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	if req.SyncId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sync_id is required")
	}
	if req.Key == "" {
		return nil, status.Errorf(codes.InvalidArgument, "key is required")
	}

	// Set sync ID in context
	ctx = types.SetSyncIDInContext(ctx, req.SyncId)

	err := s.sessionCache.Delete(ctx, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete value: %v", err)
	}

	return &v1.DeleteResponse{}, nil
}

// DeleteMany removes multiple values from the session cache
func (s *SessionService) DeleteMany(ctx context.Context, req *v1.DeleteManyRequest) (*v1.DeleteManyResponse, error) {
	if req.SyncId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sync_id is required")
	}
	if len(req.Keys) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "keys are required")
	}

	// Set sync ID in context
	ctx = types.SetSyncIDInContext(ctx, req.SyncId)

	// Delete keys one by one since the interface doesn't support batch delete
	for _, key := range req.Keys {
		err := s.sessionCache.Delete(ctx, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to delete key %s: %v", key, err)
		}
	}

	return &v1.DeleteManyResponse{}, nil
}

// Clear removes all values for a sync ID from the session cache
func (s *SessionService) Clear(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error) {
	if req.SyncId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "sync_id is required")
	}

	// Set sync ID in context
	ctx = types.SetSyncIDInContext(ctx, req.SyncId)

	err := s.sessionCache.Clear(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to clear values: %v", err)
	}

	return &v1.ClearResponse{}, nil
}
