package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// scriptedSessionStore returns the configured error per op.
type scriptedSessionStore struct {
	errs map[string]error
}

func (s *scriptedSessionStore) Get(context.Context, string, ...sessions.SessionStoreOption) ([]byte, bool, error) {
	return nil, false, s.errs[SessionOpGet]
}

func (s *scriptedSessionStore) GetMany(context.Context, []string, ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	return nil, nil, s.errs[SessionOpGetMany]
}

func (s *scriptedSessionStore) Set(context.Context, string, []byte, ...sessions.SessionStoreOption) error {
	return s.errs[SessionOpSet]
}

func (s *scriptedSessionStore) SetMany(context.Context, map[string][]byte, ...sessions.SessionStoreOption) error {
	return s.errs[SessionOpSetMany]
}

func (s *scriptedSessionStore) Delete(context.Context, string, ...sessions.SessionStoreOption) error {
	return s.errs[SessionOpDelete]
}

func (s *scriptedSessionStore) Clear(context.Context, ...sessions.SessionStoreOption) error {
	return s.errs[SessionOpClear]
}

func (s *scriptedSessionStore) GetAll(context.Context, string, ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	return nil, "", s.errs[SessionOpGetAll]
}

// fakeKVSessionStore is a minimal in-memory backing store for layering tests.
type fakeKVSessionStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newFakeKVSessionStore() *fakeKVSessionStore {
	return &fakeKVSessionStore{data: map[string][]byte{}}
}

func (f *fakeKVSessionStore) Get(_ context.Context, key string, _ ...sessions.SessionStoreOption) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	return v, ok, nil
}

func (f *fakeKVSessionStore) GetMany(_ context.Context, keys []string, _ ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := f.data[k]; ok {
			out[k] = v
		}
	}
	return out, nil, nil
}

func (f *fakeKVSessionStore) Set(_ context.Context, key string, value []byte, _ ...sessions.SessionStoreOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	return nil
}

func (f *fakeKVSessionStore) SetMany(_ context.Context, values map[string][]byte, _ ...sessions.SessionStoreOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range values {
		f.data[k] = v
	}
	return nil
}

func (f *fakeKVSessionStore) Delete(_ context.Context, key string, _ ...sessions.SessionStoreOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, key)
	return nil
}

func (f *fakeKVSessionStore) Clear(_ context.Context, _ ...sessions.SessionStoreOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = map[string][]byte{}
	return nil
}

func (f *fakeKVSessionStore) GetAll(_ context.Context, _ string, _ ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][]byte, len(f.data))
	for k, v := range f.data {
		out[k] = v
	}
	return out, "", nil
}

func testOtterOptions() *otter.Options[string, []byte] {
	return &otter.Options[string, []byte]{
		MaximumWeight: 1 << 20,
		Weigher: func(key string, value []byte) uint32 {
			return uint32(32 + len(key) + len(value)) //nolint:gosec // test sizes are tiny.
		},
	}
}

func opStatsByName(usage *v2.SessionStoreUsage) map[string]*v2.SessionStoreUsage_OpStats {
	out := make(map[string]*v2.SessionStoreUsage_OpStats, len(usage.GetOps()))
	for _, op := range usage.GetOps() {
		out[op.GetOp()] = op
	}
	return out
}

func TestInstrumentedSessionStoreRecordsToContextCollector(t *testing.T) {
	backing := &scriptedSessionStore{errs: map[string]error{
		SessionOpGet:     status.Error(codes.DeadlineExceeded, "backend timed out"),
		SessionOpSet:     errors.New("boom"),
		SessionOpGetMany: fmt.Errorf("wrapped: %w", context.DeadlineExceeded),
	}}

	var observed []string
	store := NewInstrumentedSessionStore(backing, "test_backend", "", func(op string, _ time.Duration, _ error) {
		observed = append(observed, op)
	})
	require.Equal(t, "test_backend", store.Kind())

	ctx, collector := WithUsageCollector(t.Context())
	require.Same(t, collector, UsageCollectorFromContext(ctx))

	_, _, err := store.Get(ctx, "k")
	require.Error(t, err)
	_, _, err = store.Get(ctx, "k")
	require.Error(t, err)
	_, _, err = store.GetMany(ctx, []string{"k"})
	require.Error(t, err)
	require.Error(t, store.Set(ctx, "k", nil))
	require.NoError(t, store.Delete(ctx, "k"))
	_, _, err = store.GetAll(ctx, "")
	require.NoError(t, err)

	usage := collector.Annotation()
	require.NotNil(t, usage)
	require.Equal(t, "test_backend", usage.GetKind())
	ops := opStatsByName(usage)
	require.EqualValues(t, 2, ops[SessionOpGet].GetCount())
	require.EqualValues(t, 2, ops[SessionOpGet].GetErrors())
	require.EqualValues(t, 2, ops[SessionOpGet].GetTimeouts(), "gRPC DeadlineExceeded must classify as timeout")
	require.EqualValues(t, 1, ops[SessionOpGetMany].GetTimeouts(), "wrapped context.DeadlineExceeded must classify as timeout")
	require.EqualValues(t, 1, ops[SessionOpSet].GetErrors())
	require.Zero(t, ops[SessionOpSet].GetTimeouts(), "plain errors are not timeouts")
	require.EqualValues(t, 1, ops[SessionOpDelete].GetCount())
	require.Zero(t, ops[SessionOpDelete].GetErrors())

	// The observer fires regardless of collector presence.
	require.Equal(t, []string{
		SessionOpGet, SessionOpGet, SessionOpGetMany,
		SessionOpSet, SessionOpDelete, SessionOpGetAll,
	}, observed)
}

func TestInstrumentedSessionStoreWithoutCollector(t *testing.T) {
	store := NewInstrumentedSessionStore(&scriptedSessionStore{}, "test_backend", "", nil)
	// No collector in ctx: ops must pass through without recording anywhere.
	_, _, err := store.Get(t.Context(), "k")
	require.NoError(t, err)
	require.NoError(t, store.Clear(t.Context()))
	require.Nil(t, UsageCollectorFromContext(t.Context()))
}

// TestLayeredInstrumentationDistinguishesCacheFromBackend pins the two-layer
// wrap used by the connector session stack: the cache.-prefixed layer counts
// every op, the unprefixed layer only what reached the backend, and both
// share one collector without colliding.
func TestLayeredInstrumentationDistinguishesCacheFromBackend(t *testing.T) {
	backend := NewInstrumentedSessionStore(newFakeKVSessionStore(), "connector_backend", "", nil)
	cache, err := NewMemorySessionCache(testOtterOptions(), backend)
	require.NoError(t, err)
	store := NewInstrumentedSessionStore(cache, "", "cache.", nil)

	ctx, collector := WithUsageCollector(t.Context())
	syncOpt := sessions.WithSyncID("sync-1")

	// Write-through: hits both layers.
	require.NoError(t, store.Set(ctx, "k", []byte("v"), syncOpt))
	// Cached read: cache layer only.
	_, found, err := store.Get(ctx, "k", syncOpt)
	require.NoError(t, err)
	require.True(t, found)
	// Missing key: falls through to the backend.
	_, found, err = store.Get(ctx, "missing", syncOpt)
	require.NoError(t, err)
	require.False(t, found)

	usage := collector.Annotation()
	require.NotNil(t, usage)
	require.Equal(t, "connector_backend", usage.GetKind(), "the backend layer claims the kind")
	ops := opStatsByName(usage)
	require.EqualValues(t, 1, ops["cache."+SessionOpSet].GetCount())
	require.EqualValues(t, 1, ops[SessionOpSet].GetCount(), "write-through reaches the backend")
	require.EqualValues(t, 2, ops["cache."+SessionOpGet].GetCount(), "cache layer sees every read")
	require.EqualValues(t, 1, ops[SessionOpGet].GetCount(), "only the miss reaches the backend")
}

func TestUsageCollectorEmptyAnnotation(t *testing.T) {
	_, collector := WithUsageCollector(t.Context())
	require.Nil(t, collector.Annotation(), "no ops recorded → no annotation")
	var nilCollector *UsageCollector
	require.Nil(t, nilCollector.Annotation())
}

func TestUsageCollectorConcurrency(t *testing.T) {
	store := NewInstrumentedSessionStore(&scriptedSessionStore{}, "test_backend", "", nil)
	ctx, collector := WithUsageCollector(t.Context())

	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 200; j++ {
				_, _, _ = store.Get(ctx, "k")
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}

	usage := collector.Annotation()
	require.NotNil(t, usage)
	require.EqualValues(t, 8*200, opStatsByName(usage)[SessionOpGet].GetCount())
}

func TestIsDeadlineExceeded(t *testing.T) {
	require.False(t, IsDeadlineExceeded(nil))
	require.False(t, IsDeadlineExceeded(errors.New("boom")))
	require.False(t, IsDeadlineExceeded(status.Error(codes.Unavailable, "nope")))
	require.True(t, IsDeadlineExceeded(context.DeadlineExceeded))
	require.True(t, IsDeadlineExceeded(fmt.Errorf("wrap: %w", context.DeadlineExceeded)))
	require.True(t, IsDeadlineExceeded(status.Error(codes.DeadlineExceeded, "deadline")))
}
