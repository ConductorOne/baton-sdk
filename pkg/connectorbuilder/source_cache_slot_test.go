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

// slotTestLookup is a non-noop lookup whose hits identify their owner.
type slotTestLookup struct{ validator string }

func (l slotTestLookup) Lookup(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return sourcecache.Entry{CacheValidator: l.validator}, true, nil
}

// TestSetSourceCacheContentionBlindsSlot pins the in-process slot's
// contention defense (mirroring sourcecache.GRPCServer): two live
// installs cannot be attributed by lookups (no sync-id routing), so the
// slot must serve misses to EVERYONE until every owner clears — serving
// either lookup would cross-wire validators between previous-sync
// artifacts. After the overlap drains, a fresh install serves normally.
func TestSetSourceCacheContentionBlindsSlot(t *testing.T) {
	ctx := context.Background()
	b := &builder{}

	lookupHits := func() (string, bool) {
		lk := b.sourceCacheLookup()
		require.NotNil(t, lk)
		entry, found, err := lk.Lookup(ctx, sourcecache.RowKindGrants, "scope")
		require.NoError(t, err)
		return entry.CacheValidator, found
	}

	// Single owner: served.
	b.SetSourceCache(ctx, slotTestLookup{validator: "sync-a"})
	v, found := lookupHits()
	require.True(t, found)
	require.Equal(t, "sync-a", v)

	// Second concurrent owner: blinded for everyone.
	b.SetSourceCache(ctx, slotTestLookup{validator: "sync-b"})
	_, found = lookupHits()
	require.False(t, found, "contended slot must serve misses, not either sync's lookup")

	// One owner clears; the survivor must STAY blinded (the slot cannot
	// know which lookup the survivor owns).
	b.SetSourceCache(ctx, nil)
	_, found = lookupHits()
	require.False(t, found, "slot must stay blinded until the overlap fully drains")

	// Fully drained: a fresh install serves again.
	b.SetSourceCache(ctx, nil)
	b.SetSourceCache(ctx, slotTestLookup{validator: "sync-c"})
	v, found = lookupHits()
	require.True(t, found, "a drained slot must serve a fresh single owner")
	require.Equal(t, "sync-c", v)
}
