package session

import (
	"context"
	"sync"
	"testing"

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
	srv.SetSessionStore(ctx, getOnlyStore{})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 100 {
			srv.SetSessionStore(ctx, getOnlyStore{})
		}
	}()
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = srv.Get(ctx, v1.GetRequest_builder{SyncId: "sync", Key: "k"}.Build())
		}
	}()
	wg.Wait()
}
