package session

import (
	"context"
	"errors"
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
	if bag.Prefix == "" {
		m.cache.InvalidateAll()
		return nil
	}

	var keysToInvalidate []string
	for key := range m.cache.Keys() {
		if strings.HasPrefix(key, bag.Prefix) {
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
	_, _ = m.cache.Invalidate(bag.Prefix + key)
	return nil
}

func (m *MemorySessionCache) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	v, err := m.cache.Get(ctx, bag.Prefix+key, otter.LoaderFunc[string, []byte](func(ctx context.Context, _ string) ([]byte, error) {
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
		_, _ = m.cache.Set(bag.Prefix+key, value)
	}

	return values, nextPageToken, nil
}

func (m *MemorySessionCache) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}
	prefixedKeys := keys
	if bag.Prefix != "" {
		prefixedKeys = make([]string, len(keys))
		for i := range keys {
			prefixedKeys[i] = bag.Prefix + keys[i]
		}
	}
	values, err := m.cache.BulkGet(ctx, prefixedKeys, otter.BulkLoaderFunc[string, []byte](func(ctx context.Context, keys []string) (map[string][]byte, error) {
		// we already applied the prefix to the keys
		opt := append(opt, sessions.WithPrefix(""))
		return m.ss.GetMany(ctx, keys, opt...)
	}))
	if err != nil {
		return nil, err
	}

	if bag.Prefix == "" {
		return values, nil
	}
	unprefixedValues := make(map[string][]byte, len(values))
	for k, v := range values {
		unprefixedValues[strings.TrimPrefix(k, bag.Prefix)] = v
	}
	return unprefixedValues, nil
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
	_, _ = m.cache.Set(bag.Prefix+key, value)
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
		_, _ = m.cache.Set(bag.Prefix+key, value)
	}
	return nil
}
