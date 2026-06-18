package session

import (
	"context"
	"fmt"
	"sync"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

func TestGRPCSessionServer_ConcurrentSyncs(t *testing.T) {
	server := NewGRPCSessionServer()
	ctx := context.Background()

	syncA := "sync-A"
	syncB := "sync-B"

	storeA := &inMemorySessionStore{data: make(map[storeKey][]byte)}
	storeB := &inMemorySessionStore{data: make(map[storeKey][]byte)}

	server.SetSessionStore(ctx, syncA, storeA)
	server.SetSessionStore(ctx, syncB, storeB)

	_, err := server.Set(ctx, v1.SetRequest_builder{
		SyncId: syncA,
		Key:    "key1",
		Value:  []byte("value-A"),
	}.Build())
	require.NoError(t, err)

	_, err = server.Set(ctx, v1.SetRequest_builder{
		SyncId: syncB,
		Key:    "key1",
		Value:  []byte("value-B"),
	}.Build())
	require.NoError(t, err)

	respA, err := server.Get(ctx, v1.GetRequest_builder{
		SyncId: syncA,
		Key:    "key1",
	}.Build())
	require.NoError(t, err)
	require.True(t, respA.GetFound())
	require.Equal(t, []byte("value-A"), respA.GetValue())

	respB, err := server.Get(ctx, v1.GetRequest_builder{
		SyncId: syncB,
		Key:    "key1",
	}.Build())
	require.NoError(t, err)
	require.True(t, respB.GetFound())
	require.Equal(t, []byte("value-B"), respB.GetValue())

	// Sync A should not see sync B's data and vice versa.
	respA2, err := server.Get(ctx, v1.GetRequest_builder{
		SyncId: syncA,
		Key:    "key1",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, []byte("value-A"), respA2.GetValue(), "sync A read sync B's data")
}

func TestGRPCSessionServer_ConcurrentReadWrite(t *testing.T) {
	server := NewGRPCSessionServer()
	ctx := context.Background()

	const numSyncs = 10
	const numOps = 50

	stores := make([]*inMemorySessionStore, numSyncs)
	for i := range numSyncs {
		stores[i] = &inMemorySessionStore{data: make(map[storeKey][]byte)}
		server.SetSessionStore(ctx, fmt.Sprintf("sync-%d", i), stores[i])
	}

	var wg sync.WaitGroup
	for i := range numSyncs {
		wg.Add(1)
		go func(syncIdx int) {
			defer wg.Done()
			syncID := fmt.Sprintf("sync-%d", syncIdx)
			for j := range numOps {
				key := fmt.Sprintf("key-%d", j)
				value := []byte(fmt.Sprintf("value-%d-%d", syncIdx, j))

				_, err := server.Set(ctx, v1.SetRequest_builder{
					SyncId: syncID,
					Key:    key,
					Value:  value,
				}.Build())
				require.NoError(t, err)

				resp, err := server.Get(ctx, v1.GetRequest_builder{
					SyncId: syncID,
					Key:    key,
				}.Build())
				require.NoError(t, err)
				require.True(t, resp.GetFound())
				require.Equal(t, value, resp.GetValue(),
					"sync %d read wrong value for %s", syncIdx, key)
			}
		}(i)
	}
	wg.Wait()
}

func TestGRPCSessionServer_RemoveSessionStore(t *testing.T) {
	server := NewGRPCSessionServer()
	ctx := context.Background()

	store := &inMemorySessionStore{data: make(map[storeKey][]byte)}
	server.SetSessionStore(ctx, "sync-1", store)

	_, err := server.Set(ctx, v1.SetRequest_builder{
		SyncId: "sync-1",
		Key:    "key1",
		Value:  []byte("value1"),
	}.Build())
	require.NoError(t, err)

	server.RemoveSessionStore(ctx, "sync-1")

	_, err = server.Get(ctx, v1.GetRequest_builder{
		SyncId: "sync-1",
		Key:    "key1",
	}.Build())
	require.Error(t, err)
	require.Contains(t, err.Error(), "session store not found")
}

func TestGRPCSessionServer_UnregisteredSyncID(t *testing.T) {
	server := NewGRPCSessionServer()
	ctx := context.Background()

	_, err := server.Get(ctx, v1.GetRequest_builder{
		SyncId: "nonexistent",
		Key:    "key1",
	}.Build())
	require.Error(t, err)
	require.Contains(t, err.Error(), "session store not found")
}

func TestGRPCSessionServer_ClearUnregistered(t *testing.T) {
	server := NewGRPCSessionServer()
	ctx := context.Background()

	resp, err := server.Clear(ctx, v1.ClearRequest_builder{
		SyncId: "nonexistent",
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// inMemorySessionStore is a minimal in-memory session store for testing.
type storeKey struct {
	syncID string
	key    string
}

type inMemorySessionStore struct {
	mu   sync.RWMutex
	data map[storeKey][]byte
}

func (s *inMemorySessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	bag, err := applyOpts(ctx, opt...)
	if err != nil {
		return nil, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[storeKey{syncID: bag.SyncID, key: bag.Prefix + key}]
	return v, ok, nil
}

func (s *inMemorySessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	bag, err := applyOpts(ctx, opt...)
	if err != nil {
		return nil, nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]byte)
	for _, k := range keys {
		if v, ok := s.data[storeKey{syncID: bag.SyncID, key: bag.Prefix + k}]; ok {
			result[k] = v
		}
	}
	return result, nil, nil
}

func (s *inMemorySessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOpts(ctx, opt...)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[storeKey{syncID: bag.SyncID, key: bag.Prefix + key}] = value
	return nil
}

func (s *inMemorySessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOpts(ctx, opt...)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range values {
		s.data[storeKey{syncID: bag.SyncID, key: bag.Prefix + k}] = v
	}
	return nil
}

func (s *inMemorySessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOpts(ctx, opt...)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, storeKey{syncID: bag.SyncID, key: bag.Prefix + key})
	return nil
}

func (s *inMemorySessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOpts(ctx, opt...)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.data {
		if k.syncID == bag.SyncID {
			delete(s.data, k)
		}
	}
	return nil
}

func (s *inMemorySessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	bag, err := applyOpts(ctx, opt...)
	if err != nil {
		return nil, "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]byte)
	for k, v := range s.data {
		if k.syncID == bag.SyncID {
			result[k.key] = v
		}
	}
	return result, "", nil
}

func applyOpts(ctx context.Context, opt ...sessions.SessionStoreOption) (*sessions.SessionStoreBag, error) {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		if err := o(ctx, bag); err != nil {
			return nil, err
		}
	}
	return bag, nil
}
