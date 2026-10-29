package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntitlementGraphSidecarHonorsContext(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	require.NoError(t, e.Digests().PutEntitlementGraphSidecar(ctx, []byte("original")))

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	t.Run("put", func(t *testing.T) {
		err := e.Digests().PutEntitlementGraphSidecar(canceledCtx, []byte("replacement"))
		require.ErrorIs(t, err, context.Canceled)

		got, err := e.Digests().GetEntitlementGraphSidecar(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("original"), got)
	})

	t.Run("get", func(t *testing.T) {
		got, err := e.Digests().GetEntitlementGraphSidecar(canceledCtx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, got)
	})

	t.Run("delete", func(t *testing.T) {
		err := e.Digests().DeleteEntitlementGraphSidecar(canceledCtx)
		require.ErrorIs(t, err, context.Canceled)

		got, err := e.Digests().GetEntitlementGraphSidecar(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("original"), got)
	})

	require.NoError(t, e.Digests().DeleteEntitlementGraphSidecar(ctx))
	got, err := e.Digests().GetEntitlementGraphSidecar(ctx)
	require.NoError(t, err)
	require.Nil(t, got)
}
