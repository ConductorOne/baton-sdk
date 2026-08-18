package dotc1z

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// batchDeleteCancelCtx cancels at the Nth cancellation check. The engine's
// bulk delete consults ctx once per committed chunk, so this lands the abort
// strictly between two chunks: earlier chunks are durably committed, later
// ones never run.
type batchDeleteCancelCtx struct {
	context.Context
	checks     int
	cancelUpon int
}

func (c *batchDeleteCancelCtx) Err() error {
	c.checks++
	if c.checks >= c.cancelUpon {
		return context.Canceled
	}
	return c.Context.Err()
}

func countStoredGrants(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	n := 0
	for _, err := range store.Grants().ListWithAnnotations(ctx) {
		require.NoError(t, err)
		n++
	}
	return n
}

// TestPebbleStoreDeleteGrantsByRefsMarksDirtyOnPartialFailure pins the
// partial-progress contract of the bulk delete. The singular
// DeleteGrantByRefs routes through markDirty, which only marks on success —
// harmless there because each call is one commit, so a failure means nothing
// was written. The bulk form commits many chunks per call, so a mid-way
// failure leaves earlier chunks durable in Pebble; if the dirty flag stayed
// unset, Close would skip the envelope save and discard the temp dir,
// silently dropping committed work.
//
// The delete runs in a REOPENED session that has written nothing else, so
// nothing but the delete itself can set the dirty flag.
func TestPebbleStoreDeleteGrantsByRefsMarksDirtyOnPartialFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "batch-delete-dirty.c1z")

	// The engine chunks at 1000; 2500 grants gives three chunks so the abort
	// can land with one chunk already committed.
	const total = 2500
	grants := make([]*v2.Grant, 0, total)
	for i := range total {
		grants = append(grants, mkV2Grant("", "ent", "user", "user-"+strconv.Itoa(i)))
	}

	store, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutGrants(ctx, grants...))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	// Fresh session: dirty starts false and only the delete below can set it.
	reopened, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, reopened.SetCurrentSync(ctx, syncID))
	require.Equal(t, total, countStoredGrants(t, ctx, reopened), "premise: all grants present")

	batchDeleter, ok := reopened.(interface {
		DeleteGrantsByRefs(context.Context, ...*v2.Grant) error
	})
	require.True(t, ok, "premise: the pebble store offers the bulk delete")

	// Pass the first boundary, abort at the second: exactly one chunk commits.
	cancelling := &batchDeleteCancelCtx{Context: ctx, cancelUpon: 2}
	err = batchDeleter.DeleteGrantsByRefs(cancelling, grants...)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, cancelling.checks, "premise: aborted at the second chunk boundary")

	// Close must SAVE, not discard: the committed chunk is real work.
	require.NoError(t, reopened.Close(ctx))

	verify, err := NewStore(ctx, path, WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, verify.SetCurrentSync(ctx, syncID))
	require.Equal(t, total-1000, countStoredGrants(t, ctx, verify),
		"the chunk committed before the abort must have survived Close")
	require.NoError(t, verify.Close(ctx))
}
