package expand

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// TestSynthesizedGrantFastPathWired guards the synthesized-grant fast path
// (skip the read-before-write Get for brand-new expanded grants) against silent
// breakage. The optimization only engages if every store layer between the
// merge and the engine exposes StoreNewExpandedGrants; if any wrapper hides it,
// the expander's interface assertion fails and synthesized grants silently fall
// back to the Get path — correct, but 0% faster, with no test failure. This
// asserts the assertions succeed at both layers the expander uses.
func TestSynthesizedGrantFastPathWired(t *testing.T) {
	ctx := context.Background()

	// Sink-level: the ExpanderStore the merge holds must route synthesized
	// batches to the fast path.
	var s ExpanderStore = benchmarkExpanderStore{}
	_, ok := s.(newExpandedGrantStorer)
	require.True(t, ok, "benchmarkExpanderStore must satisfy newExpandedGrantStorer")

	// Store-level: the dotc1z grant store wrapper returned by store.Grants()
	// must expose the fast path too (the layer the first whale run tripped on:
	// pebbleStoreGrants wraps the engine store and previously hid the method).
	path := filepath.Join(t.TempDir(), "fastpath.pebble.c1z")
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	_, ok = store.Grants().(newExpandedGrantStorer)
	require.True(t, ok, "pebble store.Grants() must expose StoreNewExpandedGrants")
}
