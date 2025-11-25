package session

import (
	"context"
	"fmt"
	"iter"
	"maps"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

const MaxKeysPerRequest = 100

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
	GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string]T, error)
}

func UnrollGetMany[T any](ctx context.Context, ss GetManyable[T], keys []string, opt ...sessions.SessionStoreOption) (map[string]T, error) {
	all := make(map[string]T)
	if len(keys) == 0 {
		return all, nil
	}

	// TODO(Kans): parallelize this?
	for keys := range Chunk(keys, MaxKeysPerRequest) {
		some, err := ss.GetMany(ctx, keys, opt...)
		if err != nil {
			return nil, err
		}
		maps.Copy(all, some)
	}
	return all, nil
}

type SetManyable[T any] interface {
	SetMany(ctx context.Context, values map[string]T, opt ...sessions.SessionStoreOption) error
}

func UnrollSetMany[T any](ctx context.Context, ss SetManyable[T], items map[string]T, opt ...sessions.SessionStoreOption) error {
	if len(items) == 0 {
		return nil
	}
	if len(items) <= MaxKeysPerRequest {
		return ss.SetMany(ctx, items, opt...)
	}

	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}

	// TODO(Kans): parallelize this?
	for keyChunk := range Chunk(keys, MaxKeysPerRequest) {
		some := make(map[string]T)
		for _, key := range keyChunk {
			some[key] = items[key]
		}
		err := ss.SetMany(ctx, some, opt...)
		if err != nil {
			return err
		}
	}
	return nil
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
