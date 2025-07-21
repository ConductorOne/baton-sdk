package session

import (
	"context"
	"encoding/json"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

func GetManyJSON[T any](ctx context.Context, ss sessions.SessionStore, keys []string, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	// Get the raw bytes from cache
	rawMap, err := ss.GetMany(ctx, keys, opt...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]T)
	// Unmarshal each item to the generic type
	for key, bytes := range rawMap {
		var item T
		err = json.Unmarshal(bytes, &item)
		if err != nil {
			return nil, err
		}
		result[key] = item
	}

	return result, nil
}

func SetManyJSON[T any](ctx context.Context, ss sessions.SessionStore, items map[string]T, opt ...sessions.SessionStoreOption) error {
	// Marshal each item to JSON bytes
	bytesMap := make(map[string][]byte)
	for key, item := range items {
		bytes, err := json.Marshal(item)
		if err != nil {
			return err
		}
		bytesMap[key] = bytes
	}

	// Store in cache
	return ss.SetMany(ctx, bytesMap, opt...)
}

func GetJSON[T any](ctx context.Context, ss sessions.SessionStore, key string, opt ...sessions.SessionStoreOption) (T, bool, error) {
	var zero T

	// Get the raw bytes from cache
	bytes, found, err := ss.Get(ctx, key, opt...)
	if err != nil || !found {
		return zero, found, err
	}

	// Unmarshal to the generic type
	var item T
	err = json.Unmarshal(bytes, &item)
	if err != nil {
		return zero, false, err
	}

	return item, true, nil
}

func SetJSON[T any](ctx context.Context, ss sessions.SessionStore, key string, item T, opt ...sessions.SessionStoreOption) error {
	// Marshal the item to JSON bytes
	bytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	// Store in cache
	return ss.Set(ctx, key, bytes, opt...)
}

func DeleteJSON(ctx context.Context, ss sessions.SessionStore, key string, opt ...sessions.SessionStoreOption) error {
	return ss.Delete(ctx, key, opt...)
}

func ClearJSON(ctx context.Context, ss sessions.SessionStore, opt ...sessions.SessionStoreOption) error {
	return ss.Clear(ctx, opt...)
}

func GetAllJSON[T any](ctx context.Context, ss sessions.SessionStore, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	rawMap, err := ss.GetAll(ctx, opt...)
	if err != nil {
		return nil, err
	}

	// Get all raw bytes from cache
	result := make(map[string]T)
	// Unmarshal each item to the generic type
	for key, bytes := range rawMap {
		var item T
		err = json.Unmarshal(bytes, &item)
		if err != nil {
			return nil, err
		}
		result[key] = item
	}

	return result, nil
}
