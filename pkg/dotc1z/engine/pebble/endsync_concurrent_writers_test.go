package pebble

// Stress pin for the seal-before-finalize ordering in Adapter.EndSync:
// a record writer that passed withWrite's lock-free sealed check and
// then blocked on writeMu behind EndSync's finalize steps must NOT
// commit after the deferred by_principal rebuild ran — a row landing
// in that gap would be present in the primary keyspace but permanently
// missing from the index in the saved artifact. The ordering is
// comment-justified in EndSync (seal covers the rebuild and the
// marker clear); this hammer makes it an executable claim:
//
//   INVARIANT: every write the adapter ACCEPTED (returned nil) before
//   EndSync sealed is served by the sealed artifact's by_principal
//   index; every write after the seal is REFUSED loudly — there is no
//   third outcome (accepted-but-unindexed).
//
// Writers hammer BOTH index regimes concurrently with EndSync: the
// deferred path (StoreExpandedGrants — by_principal exists only via
// the seal-time rebuild, the dangerous shape) and the inline path
// (PutGrants). Failure tolerance is deliberately narrow: writers may
// observe only the seal/detach refusals, never a corrupt state.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

func TestEndSyncConcurrentWritersNeverMissByPrincipal(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	const workers = 8
	type accepted struct {
		entID       string // canonical entitlement id the grant carries
		principalID string
	}
	var (
		wg        sync.WaitGroup
		acceptsMu sync.Mutex
		accepts   []accepted
		refusals  atomic.Int64
		stop      atomic.Bool
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			principal := fmt.Sprintf("user%02d", w)
			for seq := 0; !stop.Load(); seq++ {
				entTail := fmt.Sprintf("ent-%02d-%05d", w, seq)
				g := mkV2Grant("", entTail, "user", principal)
				var err error
				if w%2 == 0 {
					// Deferred regime: by_principal only via the
					// seal-time rebuild — the dangerous shape.
					err = a.Grants().StoreExpandedGrants(ctx, g)
				} else {
					// Inline regime.
					err = a.PutGrants(ctx, g)
				}
				if err != nil {
					// The only legal refusals are the seal boundary
					// (mid-EndSync) and the detach boundary (EndSync
					// completed and unbound the sync); anything else is
					// a real failure.
					refusals.Add(1)
					legal := strings.Contains(err.Error(), "seal") || strings.Contains(err.Error(), "no current sync")
					require.Truef(t, legal,
						"worker %d: unexpected write failure (not a seal/detach refusal): %v", w, err)
					return
				}
				acceptsMu.Lock()
				accepts = append(accepts, accepted{
					entID:       canonicalTestEntID(entTail),
					principalID: principal,
				})
				acceptsMu.Unlock()
			}
		}(w)
	}

	// Let the writers build up steady pressure, then seal under them.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, a.EndSync(ctx))
	stop.Store(true)
	wg.Wait()

	require.NotEmpty(t, accepts, "the hammer accepted no writes; the race window was never exercised")
	require.Positive(t, refusals.Load(),
		"no writer hit the seal boundary; increase pressure or the pre-seal delay — the race window was never exercised")

	// Every accepted row must be served by the sealed by_principal
	// index. Collect per-principal entitlement sets once, then check
	// membership.
	served := map[string]map[string]bool{}
	for w := 0; w < workers; w++ {
		principal := fmt.Sprintf("user%02d", w)
		set := map[string]bool{}
		require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", principal, func(r *v3.GrantRecord) bool {
			set[r.GetEntitlement().GetEntitlementId()] = true
			return true
		}))
		served[principal] = set
	}
	for _, acc := range accepts {
		require.Truef(t, served[acc.principalID][acc.entID],
			"grant %s for %s was ACCEPTED before the seal but is missing from the sealed by_principal index — the seal-before-finalize ordering leaked a straggler commit",
			acc.entID, acc.principalID)
	}
}
