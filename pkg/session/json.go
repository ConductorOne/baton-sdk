package session

import (
	"context"
	"encoding/json"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

func GetManyJSON[T any](ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	cache, err := GetSession(ctx)
	if err != nil {
		return nil, err
	}

	// Get the raw bytes from cache
	rawMap, err := cache.GetMany(ctx, keys, opt...)
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

func SetManyJSON[T any](ctx context.Context, items map[string]T, opt ...sessions.SessionStoreOption) error {
	cache, err := GetSession(ctx)
	if err != nil {
		return err
	}

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
	return cache.SetMany(ctx, bytesMap, opt...)
}

func GetJSON[T any](ctx context.Context, key string, opt ...sessions.SessionStoreOption) (T, bool, error) {
	var zero T
	cache, err := GetSession(ctx)
	if err != nil {
		return zero, false, err
	}

	// Get the raw bytes from cache
	bytes, found, err := cache.Get(ctx, key, opt...)
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

func SetJSON[T any](ctx context.Context, key string, item T, opt ...sessions.SessionStoreOption) error {
	cache, err := GetSession(ctx)
	if err != nil {
		return err
	}

	// Marshal the item to JSON bytes
	bytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	// Store in cache
	return cache.Set(ctx, key, bytes, opt...)
}

func DeleteJSON(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	cache, err := GetSession(ctx)
	if err != nil {
		return err
	}

	return cache.Delete(ctx, key, opt...)
}

func ClearJSON(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	cache, err := GetSession(ctx)
	if err != nil {
		return err
	}

	return cache.Clear(ctx, opt...)
}

func GetAllJSON[T any](ctx context.Context, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	cache, err := GetSession(ctx)
	if err != nil {
		return nil, err
	}

	// Get all raw bytes from cache
	rawMap, err := cache.GetAll(ctx, opt...)
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
