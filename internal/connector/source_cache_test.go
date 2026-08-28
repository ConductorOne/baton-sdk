package connector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

type recordingSetLookup struct {
	delivered []sourcecache.Lookup
}

func (r *recordingSetLookup) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	r.delivered = append(r.delivered, lookup)
}

// TestConnectorClientSourceCacheDelivery pins the production client's half
// of the deliverability contract (CO-6b-001/CO-6b-007): with no wired
// setter — every subprocess connector today — the probe reports
// undeliverable and delivery is a swallowed no-op; with a setter wired,
// the probe flips and delivery forwards the exact lookup value.
func TestConnectorClientSourceCacheDelivery(t *testing.T) {
	ctx := context.Background()

	c := &connectorClient{}
	require.False(t, c.SourceCacheLookupDeliverable(),
		"no wired setter must read as undeliverable, or the syncer would log warm into the void")
	// Delivery without a setter must be a silent no-op, not a panic: the
	// syncer calls SetSourceCache on every sync regardless of transport.
	c.SetSourceCache(ctx, sourcecache.NoopLookup{})

	setter := &recordingSetLookup{}
	c.SetSourceCacheSetter(setter)
	require.True(t, c.SourceCacheLookupDeliverable())
	lookup := sourcecache.NoopLookup{}
	c.SetSourceCache(ctx, lookup)
	c.SetSourceCache(ctx, nil) // teardown shape
	require.Len(t, setter.delivered, 2)
	require.Equal(t, sourcecache.Lookup(lookup), setter.delivered[0])
	require.Nil(t, setter.delivered[1])
}
