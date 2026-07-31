package pebble

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
)

// === V-19: concurrent readers over an immutable sealed artifact (D10) ===
//
// Per the implementation addendum §E the grant read path holds no shared
// mutable state (grantReadArena is allocated fresh per PaginateGrants call;
// reads go through pebble db.Get/NewIter over an immutable sealed DB), so one
// representative -race cell suffices. This drives many concurrent v3 readers —
// point-gets and scans mixed — over one shared reader and asserts each still
// observes the exact planted discovered_at. Run under `go test -race`.
func TestV3ConcurrentReadersRace(t *testing.T) {
	ctx := context.Background()
	e, r := newV3GrantReader(ctx, t)

	want := map[string]time.Time{
		"alice": time.Date(2020, 1, 1, 0, 0, 0, 111, time.UTC),
		"bob":   time.Date(2021, 2, 2, 0, 0, 0, 222, time.UTC),
		"carol": time.Date(2022, 3, 3, 0, 0, 0, 333, time.UTC),
	}
	for p, ts := range want {
		seedGrantWithDiscoveredAt(ctx, t, e, "g-"+p, "ent-A", p, ts)
	}

	const readers = 16
	var wg sync.WaitGroup
	wg.Add(readers)
	errCh := make(chan error, readers*8)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				// Scan route.
				lg, err := r.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
					Entitlement: v3TestEntStub("ent-A"),
					PageSize:    100,
				}.Build())
				if err != nil {
					errCh <- err
					return
				}
				for _, rec := range lg.GetList() {
					p := rec.GetPrincipal().GetResourceId()
					if rec.GetDiscoveredAt() == nil || !rec.GetDiscoveredAt().AsTime().Equal(want[p]) {
						errCh <- errMismatch(p)
						return
					}
				}
				// Point route.
				for p := range want {
					gg, err := r.GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-" + p}.Build())
					if err != nil {
						errCh <- err
						return
					}
					if !gg.GetGrant().GetDiscoveredAt().AsTime().Equal(want[p]) {
						errCh <- errMismatch(p)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

type errMismatch string

func (e errMismatch) Error() string { return "discovered_at mismatch for principal " + string(e) }
