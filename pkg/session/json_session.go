package session

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// See GRPC validation rules for eg GetManyRequest.

func GetManyJSON[T any](ctx context.Context, ss sessions.SessionStore, keys []string, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	allBytes, err := UnrollGetMany[[]byte](ctx, ss, keys, opt...)
	if err != nil {
		return nil, err
	}

	result := make(map[string]T)
	for key, bytes := range allBytes {
		var item T
		err = json.Unmarshal(bytes, &item)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal item for key %s: %w", key, err)
		}
		result[key] = item
	}

	return result, nil
}

func SetManyJSON[T any](ctx context.Context, ss sessions.SessionStore, items map[string]T, opt ...sessions.SessionStoreOption) error {
	// Lazy iterator that marshals items on demand, yielding (item, error) pairs
	sizedItems := func(yield func(SizedItem[[]byte], error) bool) {
		for key, item := range items {
			bytes, err := json.Marshal(item)
			if err != nil {
				yield(SizedItem[[]byte]{}, fmt.Errorf("failed to marshal item for key %s: %w", key, err))
				return
			}
			if !yield(SizedItem[[]byte]{
				Key:   key,
				Value: bytes,
				Size:  len(key) + len(bytes) + 20,
			}, nil) {
				return
			}
		}
	}

	return UnrollSetMany(ctx, ss, sizedItems, opt...)
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
	result := make(map[string]T)
	pageToken := ""
	for {
		rawMap, nextPageToken, err := ss.GetAll(ctx, pageToken, opt...)
		if err != nil {
			return nil, err
		}
		for key, bytes := range rawMap {
			var item T
			err = json.Unmarshal(bytes, &item)
			if err != nil {
				return nil, err
			}
			result[key] = item
		}
		if nextPageToken == "" {
			break
		}
		if pageToken == nextPageToken {
			return nil, fmt.Errorf("page token is the same as the next page token")
		}
		pageToken = nextPageToken
	}

	return result, nil
}
