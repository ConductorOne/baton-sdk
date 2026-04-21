package dotc1z

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestStreamIteratorHoldsSingleConnection verifies the hard constraint that a live
// rowIterator pins the sole sqlite connection (MaxOpenConns=1). Any additional DB
// call issued before Close() blocks until the iterator is drained/closed, so callers
// must never interleave iteration with other store operations.
func TestStreamIteratorHoldsSingleConnection(t *testing.T) {
	ctx := t.Context()

	tempDir := filepath.Join(t.TempDir(), "test.c1z")
	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer func() { _ = c1zFile.Close(ctx) }()

	_, err = c1zFile.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	rt := v2.ResourceType_builder{Id: "rt1", DisplayName: "rt1"}.Build()
	require.NoError(t, c1zFile.PutResourceTypes(ctx, rt))
	require.NoError(t, c1zFile.PutResources(ctx, generateResources(50, rt)...))
	require.NoError(t, c1zFile.EndSync(ctx))

	req := v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "rt1",
	}.Build()

	iter, err := streamConnectorObjects(ctx, c1zFile, resources.Name(), req, func() *v2.Resource { return &v2.Resource{} })
	require.NoError(t, err)
	require.True(t, iter.Next(), "expected at least one row")

	// Concurrent DB call should block until iterator is closed.
	done := make(chan error, 1)
	ctxShort, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	go func() {
		_, err := c1zFile.ListResources(ctxShort, v2.ResourcesServiceListResourcesRequest_builder{ResourceTypeId: "rt1"}.Build())
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("concurrent DB call returned while iterator was open (err=%v); expected block until Close()", err)
	case <-time.After(300 * time.Millisecond):
		// Still blocked — confirms single-connection deadlock risk for streaming iterators.
	}

	// Close iter so the concurrent call can unblock (it will still get its own ctx deadline).
	require.NoError(t, iter.Close())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("concurrent call never unblocked after iter.Close()")
	}
}
