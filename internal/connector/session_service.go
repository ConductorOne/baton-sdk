package connector

import (
	"context"
	"fmt"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

type SessionService struct {
	sessionCache sessions.SessionStore
}

func NewSessionService(sessionCache sessions.SessionStore) *SessionService {
	return &SessionService{
		sessionCache: sessionCache,
	}
}

func (s *SessionService) Get(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	value, found, err := s.sessionCache.Get(ctx, req.GetKey(), sessions.WithSyncID(req.GetSyncId()))
	if err != nil {
		return nil, fmt.Errorf("failed to get value from cache: %w", err)
	}

	if !found {
		return v1.GetResponse_builder{
			Value: nil,
		}.Build(), nil
	}

	return v1.GetResponse_builder{
		Value: value,
	}.Build(), nil
}

func (s *SessionService) GetMany(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	if len(req.GetKeys()) == 0 {
		return v1.GetManyResponse_builder{
			Items: []*v1.GetManyItem{},
		}.Build(), nil
	}

	values, err := s.sessionCache.GetMany(ctx, req.GetKeys(), sessions.WithSyncID(req.GetSyncId()))
	if err != nil {
		return nil, fmt.Errorf("failed to get many values from cache: %w", err)
	}

	items := make([]*v1.GetManyItem, 0, len(values))
	for key, value := range values {
		items = append(items, v1.GetManyItem_builder{
			Key:   key,
			Value: value,
		}.Build())
	}

	return v1.GetManyResponse_builder{
		Items: items,
	}.Build(), nil
}

func (s *SessionService) GetAll(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	values, err := s.sessionCache.GetAll(ctx, sessions.WithSyncID(req.GetSyncId()))
	if err != nil {
		return nil, fmt.Errorf("failed to get all values from cache: %w", err)
	}

	items := make([]*v1.GetAllItem, 0, len(values))
	for key, value := range values {
		items = append(items, v1.GetAllItem_builder{
			Key:   key,
			Value: value,
		}.Build())
	}

	return v1.GetAllResponse_builder{
		Items: items,
	}.Build(), nil
}

func (s *SessionService) Set(ctx context.Context, req *v1.SetRequest) (*v1.SetResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.sessionCache.Set(ctx, req.GetKey(), req.GetValue(), sessions.WithSyncID(req.GetSyncId()))
	if err != nil {
		return nil, fmt.Errorf("failed to set value in cache: %w", err)
	}

	return &v1.SetResponse{}, nil
}

func (s *SessionService) SetMany(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.sessionCache.SetMany(ctx, req.GetValues(), sessions.WithSyncID(req.GetSyncId()))
	if err != nil {
		return nil, fmt.Errorf("failed to set many values in cache: %w", err)
	}

	return &v1.SetManyResponse{}, nil
}

func (s *SessionService) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.sessionCache.Delete(ctx, req.GetKey(), sessions.WithSyncID(req.GetSyncId()))
	if err != nil {
		return nil, fmt.Errorf("failed to delete value from cache: %w", err)
	}

	return &v1.DeleteResponse{}, nil
}

func (s *SessionService) DeleteMany(ctx context.Context, req *v1.DeleteManyRequest) (*v1.DeleteManyResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	for _, key := range req.GetKeys() {
		err := s.sessionCache.Delete(ctx, key, sessions.WithSyncID(req.GetSyncId()))
		if err != nil {
			return nil, fmt.Errorf("failed to delete value for key %s: %w", key, err)
		}
	}

	return &v1.DeleteManyResponse{}, nil
}

func (s *SessionService) Clear(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	err := s.sessionCache.Clear(ctx, sessions.WithSyncID(req.GetSyncId()))
	if err != nil {
		return nil, fmt.Errorf("failed to clear cache: %w", err)
	}

	return &v1.ClearResponse{}, nil
}
