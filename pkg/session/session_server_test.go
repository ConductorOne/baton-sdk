package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

type getOnlyStore struct{ sessions.SessionStore }

func (getOnlyStore) Get(context.Context, string, ...sessions.SessionStoreOption) ([]byte, bool, error) {
	return nil, false, nil
}

// No assertion: this fails only under -race.
func TestGRPCSessionServerAttachWhileServing(t *testing.T) {
	ctx := t.Context()
	srv := NewGRPCSessionServer()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			unbind, err := srv.BindSessionStore(ctx, "sync", getOnlyStore{})
			if err == nil {
				_ = unbind(ctx)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = srv.Get(ctx, v1.GetRequest_builder{SyncId: "sync", Key: "k"}.Build())
			_, _ = srv.Get(ctx, v1.GetRequest_builder{SyncId: "other", Key: "k"}.Build())
		}
	}()
	wg.Wait()
}

type kvStore struct {
	mu      sync.Mutex
	values  map[string][]byte
	syncIDs map[string]int
	calls   int
}

func newKVStore() *kvStore {
	return &kvStore{values: map[string][]byte{}, syncIDs: map[string]int{}}
}

func (s *kvStore) note(opt []sessions.SessionStoreOption) {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		_ = o(context.Background(), bag)
	}
	s.calls++
	s.syncIDs[bag.SyncID]++
}

func (s *kvStore) Get(_ context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.note(opt)
	v, ok := s.values[key]
	return v, ok, nil
}

func (s *kvStore) Set(_ context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.note(opt)
	s.values[key] = value
	return nil
}

func (s *kvStore) GetMany(_ context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.note(opt)
	out := map[string][]byte{}
	for _, k := range keys {
		if v, ok := s.values[k]; ok {
			out[k] = v
		}
	}
	return out, nil, nil
}

func (s *kvStore) SetMany(_ context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.note(opt)
	for k, v := range values {
		s.values[k] = v
	}
	return nil
}

func (s *kvStore) Delete(_ context.Context, key string, opt ...sessions.SessionStoreOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.note(opt)
	delete(s.values, key)
	return nil
}

func (s *kvStore) GetAll(_ context.Context, _ string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.note(opt)
	out := make(map[string][]byte, len(s.values))
	for k, v := range s.values {
		out[k] = v
	}
	return out, "", nil
}

func (s *kvStore) Clear(_ context.Context, opt ...sessions.SessionStoreOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.note(opt)
	clear(s.values)
	return nil
}

func (s *kvStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *kvStore) seenSyncIDs() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.syncIDs))
	for k, v := range s.syncIDs {
		out[k] = v
	}
	return out
}

type blockingGetStore struct {
	*kvStore
	entered chan struct{}
	release chan struct{}
}

func newBlockingGetStore() *blockingGetStore {
	return &blockingGetStore{kvStore: newKVStore(), entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingGetStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	close(s.entered)
	<-s.release
	return s.kvStore.Get(ctx, key, opt...)
}

const (
	syncA = "syncaaaaaaaaaaaaaaaaaaaaaaa"
	syncB = "syncbbbbbbbbbbbbbbbbbbbbbbb"
)

func bind(t *testing.T, srv *GRPCSessionServer, syncID string, store sessions.SessionStore) sessions.UnbindSessionStore {
	t.Helper()
	unbind, err := srv.BindSessionStore(t.Context(), syncID, store)
	require.NoError(t, err)
	return unbind
}

func get(ctx context.Context, srv *GRPCSessionServer, syncID, key string) (*v1.GetResponse, error) {
	return srv.Get(ctx, v1.GetRequest_builder{SyncId: syncID, Key: key}.Build())
}

func TestGRPCSessionServerRoutesBySyncID(t *testing.T) {
	ctx := t.Context()
	srv := NewGRPCSessionServer()
	storeA, storeB := newKVStore(), newKVStore()
	bind(t, srv, syncA, storeA)
	bind(t, srv, syncB, storeB)

	for _, id := range []string{syncA, syncB} {
		_, err := srv.Set(ctx, v1.SetRequest_builder{SyncId: id, Key: "k", Value: []byte(id)}.Build())
		require.NoError(t, err)
	}
	for _, id := range []string{syncA, syncB} {
		resp, err := get(ctx, srv, id, "k")
		require.NoError(t, err)
		require.Equal(t, id, string(resp.GetValue()))
	}
	require.Equal(t, map[string]int{syncA: 2}, storeA.seenSyncIDs())
	require.Equal(t, map[string]int{syncB: 2}, storeB.seenSyncIDs())
}

func TestGRPCSessionServerRejectsUnroutableRequests(t *testing.T) {
	ctx := t.Context()
	handlers := map[string]func(srv *GRPCSessionServer, syncID string) error{
		"Get": func(srv *GRPCSessionServer, id string) error {
			_, err := get(ctx, srv, id, "k")
			return err
		},
		"GetMany": func(srv *GRPCSessionServer, id string) error {
			_, err := srv.GetMany(ctx, v1.GetManyRequest_builder{SyncId: id, Keys: []string{"k"}}.Build())
			return err
		},
		"GetAll": func(srv *GRPCSessionServer, id string) error {
			_, err := srv.GetAll(ctx, v1.GetAllRequest_builder{SyncId: id}.Build())
			return err
		},
		"Set": func(srv *GRPCSessionServer, id string) error {
			_, err := srv.Set(ctx, v1.SetRequest_builder{SyncId: id, Key: "k", Value: []byte("v")}.Build())
			return err
		},
		"SetMany": func(srv *GRPCSessionServer, id string) error {
			_, err := srv.SetMany(ctx, v1.SetManyRequest_builder{SyncId: id, Values: map[string][]byte{"k": []byte("v")}}.Build())
			return err
		},
		"Delete": func(srv *GRPCSessionServer, id string) error {
			_, err := srv.Delete(ctx, v1.DeleteRequest_builder{SyncId: id, Key: "k"}.Build())
			return err
		},
		"DeleteMany": func(srv *GRPCSessionServer, id string) error {
			_, err := srv.DeleteMany(ctx, v1.DeleteManyRequest_builder{SyncId: id, Keys: []string{"k"}}.Build())
			return err
		},
		"Clear": func(srv *GRPCSessionServer, id string) error {
			_, err := srv.Clear(ctx, v1.ClearRequest_builder{SyncId: id}.Build())
			return err
		},
	}
	for name, call := range handlers {
		t.Run(name, func(t *testing.T) {
			srv := NewGRPCSessionServer()
			other := newKVStore()
			bind(t, srv, syncA, other)

			require.Equal(t, codes.InvalidArgument, status.Code(call(srv, "")), "empty sync id")
			if name == "Clear" {
				require.NoError(t, call(srv, syncB), "Clear for an unbound sync id has nothing to clear")
			} else {
				require.Equal(t, codes.FailedPrecondition, status.Code(call(srv, syncB)), "unbound sync id")
			}
			require.Zero(t, other.callCount(), "another sync's store served the request")
		})
	}
}

func TestGRPCSessionServerBindRejects(t *testing.T) {
	ctx := t.Context()
	srv := NewGRPCSessionServer()
	first := newKVStore()
	unbind := bind(t, srv, syncA, first)

	_, err := srv.BindSessionStore(ctx, syncA, newKVStore())
	require.Error(t, err, "second bind of a bound sync id")
	_, err = srv.BindSessionStore(ctx, "", newKVStore())
	require.Error(t, err, "empty sync id")
	_, err = srv.BindSessionStore(ctx, syncB, nil)
	require.Error(t, err, "nil store")

	_, err = get(ctx, srv, syncA, "k")
	require.NoError(t, err)
	require.Equal(t, 1, first.callCount(), "a rejected bind replaced the existing binding")

	require.NoError(t, unbind(ctx))
	second := newKVStore()
	unbindSecond := bind(t, srv, syncA, second)
	require.NoError(t, unbind(ctx), "repeat unbind")
	_, err = get(ctx, srv, syncA, "k")
	require.NoError(t, err, "a repeat unbind removed the next binding")
	require.Equal(t, 1, second.callCount())
	require.NoError(t, unbindSecond(ctx))
}

func TestGRPCSessionServerUnbindDrains(t *testing.T) {
	ctx := t.Context()
	srv := NewGRPCSessionServer()
	store := newBlockingGetStore()
	unbind := bind(t, srv, syncA, store)

	getErr := make(chan error, 1)
	go func() {
		_, err := get(ctx, srv, syncA, "k")
		getErr <- err
	}()
	<-store.entered

	unbound := make(chan error, 1)
	go func() { unbound <- unbind(ctx) }()

	setBeforeUnbind := 0
	require.Eventually(t, func() bool {
		_, err := srv.Set(ctx, v1.SetRequest_builder{SyncId: syncA, Key: "late", Value: []byte("v")}.Build())
		if err == nil {
			setBeforeUnbind++
		}
		return status.Code(err) == codes.FailedPrecondition
	}, 5*time.Second, time.Millisecond, "binding still routes new requests while draining")
	served := store.callCount()
	require.Equal(t, setBeforeUnbind, served, "a rejected request reached the store")
	select {
	case err := <-unbound:
		t.Fatalf("unbind returned with a request in flight: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(store.release)
	require.NoError(t, <-unbound)
	require.NoError(t, <-getErr)
	require.Equal(t, served+1, store.callCount(), "a request that arrived after unbind reached the store")
}

func TestGRPCSessionServerUnbindDrainTimeout(t *testing.T) {
	ctx := t.Context()
	srv := NewGRPCSessionServer()
	store := newBlockingGetStore()
	unbind := bind(t, srv, syncA, store)

	getErr := make(chan error, 1)
	go func() {
		_, err := get(ctx, srv, syncA, "k")
		getErr <- err
	}()
	<-store.entered

	drainCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := unbind(drainCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	_, err = get(ctx, srv, syncA, "k")
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a timed-out unbind left the binding in place")

	close(store.release)
	require.NoError(t, <-getErr)
	require.NoError(t, unbind(ctx), "repeat unbind")
}
