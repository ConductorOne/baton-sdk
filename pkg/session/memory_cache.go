package session

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/maypok86/otter/v2"
	"go.uber.org/zap"
)

var _ sessions.SessionStore = (*MemorySessionCache)(nil)

func NewMemorySessionCache(otterOptions *otter.Options[string, []byte], ss sessions.SessionStore) (*MemorySessionCache, error) {
	cache, err := otter.New(otterOptions)
	if err != nil {
		return nil, err
	}
	return &MemorySessionCache{cache: cache, ss: ss}, nil
}

type MemorySessionCache struct {
	cache *otter.Cache[string, []byte]
	ss    sessions.SessionStore
}

// The cache is potentially used across syncs.
// Cross sync isolation is achieved by using the syncID in the cache key.
func cacheKey(bag *sessions.SessionStoreBag, key string) string {
	return fmt.Sprintf("%s/%s/%s", bag.SyncID, bag.Prefix, key)
}

func cacheKeys(bag *sessions.SessionStoreBag, keys []string) []string {
	newKeys := make([]string, len(keys))
	prefix := fmt.Sprintf("%s/%s/", bag.SyncID, bag.Prefix)
	for i, key := range keys {
		newKeys[i] = fmt.Sprintf("%s%s", prefix, key)
	}
	return newKeys
}

func stripPrefix(bag *sessions.SessionStoreBag, key string) string {
	prefix := fmt.Sprintf("%s/%s/", bag.SyncID, bag.Prefix)
	return strings.TrimPrefix(key, prefix)
}

func stripPrefixes(bag *sessions.SessionStoreBag, keys []string) []string {
	prefix := fmt.Sprintf("%s/%s/", bag.SyncID, bag.Prefix)
	newKeys := make([]string, len(keys))
	for i, key := range keys {
		newKeys[i] = strings.TrimPrefix(key, prefix)
	}
	return newKeys
}

func (m *MemorySessionCache) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	l := ctxzap.Extract(ctx)
	s := m.cache.Stats()
	l.Info(
		"MemorySessionCache Stats",
		zap.Uint64("hits", s.Hits),
		zap.Uint64("misses", s.Misses),
		zap.Int("estimatedEntries", m.cache.EstimatedSize()),
		zap.Uint64("weightedSize", m.cache.WeightedSize()),
	)

	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}
	err = m.ss.Clear(ctx, opt...)
	if err != nil {
		return err
	}
	prefix := fmt.Sprintf("%s/", bag.SyncID)
	if bag.Prefix != "" {
		prefix = cacheKey(bag, "")
	}

	var keysToInvalidate []string
	for key := range m.cache.Keys() {
		if strings.HasPrefix(key, prefix) {
			keysToInvalidate = append(keysToInvalidate, key)
		}
	}
	for _, key := range keysToInvalidate {
		_, _ = m.cache.Invalidate(key)
	}

	return nil
}

func (m *MemorySessionCache) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	err = m.ss.Delete(ctx, key, opt...)
	if err != nil {
		return err
	}
	_, _ = m.cache.Invalidate(cacheKey(bag, key))
	return nil
}

type CacheItem struct {
	Value []byte
}

func (m *MemorySessionCache) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	v, err := m.cache.Get(ctx, cacheKey(bag, key), otter.LoaderFunc[string, []byte](func(ctx context.Context, _ string) ([]byte, error) {
		v, found, err := m.ss.Get(ctx, key, opt...)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, otter.ErrNotFound
		}
		return v, nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// GetAll always calls the backing store and caches the results.
func (m *MemorySessionCache) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, "", err
	}
	values, nextPageToken, err := m.ss.GetAll(ctx, pageToken, opt...)
	if err != nil {
		return nil, "", err
	}
	for key, value := range values {
		_, _ = m.cache.Set(cacheKey(bag, key), value)
	}

	return values, nextPageToken, nil
}

func (m *MemorySessionCache) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, nil, err
	}
	values, err := m.cache.BulkGet(ctx, cacheKeys(bag, keys), otter.BulkLoaderFunc[string, []byte](func(ctx context.Context, cacheKeys []string) (map[string][]byte, error) {
		backingValues, unprocessedKeys, err := m.ss.GetMany(ctx, stripPrefixes(bag, cacheKeys), opt...)
		if err != nil {
			return nil, err
		}
		if len(unprocessedKeys) > 0 {
			return nil, fmt.Errorf("get many returned unprocessed keys")
		}
		cacheKeyValues := make(map[string][]byte, len(backingValues))
		for k, v := range backingValues {
			cacheKeyValues[cacheKey(bag, k)] = v
		}

		return cacheKeyValues, nil
	}))

	if err != nil {
		return nil, nil, err
	}
	unprefixedValues := make(map[string][]byte)
	for k, v := range values {
		// NOTE(kans): GetMany returns nil values for missing keys, so we need to filter them out.
		//  We do not allow nil values in the session store.
		if v == nil {
			continue
		}
		unprefixedValues[stripPrefix(bag, k)] = v
	}
	return unprefixedValues, nil, nil
}

func (m *MemorySessionCache) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}
	err = m.ss.Set(ctx, key, value, opt...)
	if err != nil {
		return err
	}
	_, _ = m.cache.Set(cacheKey(bag, key), value)
	return nil
}

func (m *MemorySessionCache) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}
	err = m.ss.SetMany(ctx, values, opt...)
	if err != nil {
		return err
	}
	for key, value := range values {
		_, _ = m.cache.Set(cacheKey(bag, key), value)
	}
	return nil
}
