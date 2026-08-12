package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A bare engine has no envelope manifest, so it cannot carry fold_dead_bytes.
// Folding into one anyway would drop the count silently, understating
// accumulated fold waste and deferring the rebuild that reclaims it.
//
// The refusal must land before the callback runs. The shadowed-byte count is
// only knowable afterwards, so a post-hoc check would return an error for a
// fold that already mutated the engine and cannot be rolled back.
func TestWithEngineFoldMutationRejectsBareEngineBeforeMutating(t *testing.T) {
	ctx := context.Background()

	ran := false
	err := WithEngineFoldMutation(ctx, &Engine{}, func(context.Context, *Engine) (int64, error) {
		ran = true
		return 4096, nil
	})
	require.ErrorContains(t, err, "must target a store")
	require.False(t, ran, "the callback must not run against a bare engine")

	// Same refusal when the fold would have shadowed nothing: the target is
	// wrong regardless of the outcome.
	ran = false
	err = WithEngineFoldMutation(ctx, &Engine{}, func(context.Context, *Engine) (int64, error) {
		ran = true
		return 0, nil
	})
	require.ErrorContains(t, err, "must target a store")
	require.False(t, ran, "the callback must not run against a bare engine")
}
