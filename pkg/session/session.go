package session

import (
	"context"
	"fmt"
	"iter"
	"maps"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

func Chunk[T any](items []T, chunkSize int) iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		for i := 0; i < len(items); i += chunkSize {
			end := min(i+chunkSize, len(items))
			if !yield(items[i:end]) {
				return
			}
		}
	}
}

type GetManyable[T any] interface {
	GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string]T, []string, error)
}

func UnrollGetMany[T any](ctx context.Context, ss GetManyable[T], keys []string, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	all := make(map[string]T)
	if len(keys) == 0 {
		return all, nil
	}

	// TODO(Kans): parallelize this?
	for keyChunk := range Chunk(keys, sessions.MaxKeysPerRequest) {
		// For each chunk, unroll any unprocessed keys until all are processed
		remainingKeys := keyChunk
		for {
			some, unprocessedKeys, err := ss.GetMany(ctx, remainingKeys, opt...)
			if err != nil {
				return nil, err
			}

			// Accumulate results
			maps.Copy(all, some)

			// If no unprocessed keys, we're done with this chunk
			if len(unprocessedKeys) == 0 {
				break
			}

			// Check for infinite loop: if unprocessed keys haven't been reduced, something is wrong
			if len(unprocessedKeys) == len(remainingKeys) {
				return nil, fmt.Errorf("unprocessed keys not reduced: %d unprocessed out of %d requested", len(unprocessedKeys), len(remainingKeys))
			}

			// Continue with unprocessed keys
			remainingKeys = unprocessedKeys
		}
	}
	return all, nil
}

type SetManyable[T any] interface {
	SetMany(ctx context.Context, values map[string]T, opt ...sessions.SessionStoreOption) error
}

// SizedItem represents a key-value pair with its size in bytes.
type SizedItem[T any] struct {
	Key   string
	Value T
	Size  int // size in bytes of key + value
}

// UnrollSetMany takes an iterator of sized items and batches them into SetMany calls,
// respecting both MaxKeysPerRequest and MaxSessionStoreSizeLimit.
// The iterator yields (item, error) pairs; iteration stops on the first error.
func UnrollSetMany[T any](ctx context.Context, ss SetManyable[T], items iter.Seq2[SizedItem[T], error], opt ...sessions.SessionStoreOption) error {
	currentChunk := make(map[string]T)
	currentSize := 0

	flush := func() error {
		if len(currentChunk) == 0 {
			return nil
		}
		err := ss.SetMany(ctx, currentChunk, opt...)
		if err != nil {
			return err
		}
		currentChunk = make(map[string]T)
		currentSize = 0
		return nil
	}

	for item, err := range items {
		if err != nil {
			return err
		}

		// Flush if adding this item would exceed either limit
		if len(currentChunk) >= sessions.MaxKeysPerRequest || (currentSize+item.Size >= sessions.MaxSessionStoreSizeLimit && len(currentChunk) > 0) {
			if err := flush(); err != nil {
				return err
			}
		}

		currentChunk[item.Key] = item.Value
		currentSize += item.Size
	}

	return flush()
}

type GetAllable[T any] interface {
	GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string]T, string, error)
}

func UnrollGetAll[T any](ctx context.Context, ss GetAllable[T], opt ...sessions.SessionStoreOption) (map[string]T, error) {
	pageToken := ""
	all := make(map[string]T)
	for {
		// TODO(Kans): parallelize this?
		some, nextPageToken, err := ss.GetAll(ctx, pageToken, opt...)
		if err != nil {
			return nil, err
		}
		maps.Copy(all, some)
		if nextPageToken == "" {
			break
		}
		if pageToken == nextPageToken {
			return nil, fmt.Errorf("page token is the same as the next page token: %s", pageToken)
		}

		pageToken = nextPageToken
	}
	return all, nil
}
