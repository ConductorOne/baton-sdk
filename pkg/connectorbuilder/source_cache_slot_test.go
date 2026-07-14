package connectorbuilder

// The builder holds ONE mutable source-cache lookup slot shared by every
// sync against it (see SetSourceCache). The runner supports concurrent
// tasks, so installs/clears can race list RPCs reading the slot: the slot
// accesses must be synchronized, and an install over an existing lookup
// (two live syncs cross-wiring each other) must be loudly visible.

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// TestSetSourceCacheConcurrentWithListCalls hammers SetSourceCache
// (install/clear cycles, as concurrent syncs would) while list RPCs read
// the slot. Run under -race: unsynchronized slot access is a data race
// even when every interleaving happens to behave.
func TestSetSourceCacheConcurrentWithListCalls(t *testing.T) {
	ctx := context.Background()
	ts := &continuationTestSyncer{
		testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"},
		scope:                      "groups/g1/members",
	}
	b := newContinuationTestBuilder(t, ts)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				b.SetSourceCache(ctx, sourcecache.NoopLookup{})
			} else {
				b.SetSourceCache(ctx, nil)
			}
		}
	}()

	for i := 0; i < 200; i++ {
		_, err := b.ListGrants(ctx, continuationGrantsRequest(nil))
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
}
