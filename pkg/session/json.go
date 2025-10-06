package session

import (
	"context"
	"encoding/json"

	"github.com/conductorone/baton-sdk/pkg/types"
)

func GetManyJSON[T any](ctx context.Context, ss types.SessionStore, keys []string, opt ...types.SessionOption) (map[string]T, error) {
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

func SetManyJSON[T any](ctx context.Context, ss types.SessionStore, items map[string]T, opt ...types.SessionOption) error {
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

func GetJSON[T any](ctx context.Context, ss types.SessionStore, key string, opt ...types.SessionOption) (T, bool, error) {
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

func SetJSON[T any](ctx context.Context, ss types.SessionStore, key string, item T, opt ...types.SessionOption) error {
	// Marshal the item to JSON bytes
	bytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	// Store in cache
	return ss.Set(ctx, key, bytes, opt...)
}

func DeleteJSON(ctx context.Context, ss types.SessionStore, key string, opt ...types.SessionOption) error {
	return ss.Delete(ctx, key, opt...)
}

func ClearJSON(ctx context.Context, ss types.SessionStore, opt ...types.SessionOption) error {
	return ss.Clear(ctx, opt...)
}

func GetAllJSON[T any](ctx context.Context, ss types.SessionStore, opt ...types.SessionOption) (map[string]T, error) {
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
