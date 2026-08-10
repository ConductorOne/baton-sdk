package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEntitlementGraphSidecarHonorsContext(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	require.NoError(t, e.PutEntitlementGraphSidecar(ctx, []byte("original")))

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	t.Run("put", func(t *testing.T) {
		err := e.PutEntitlementGraphSidecar(canceledCtx, []byte("replacement"))
		require.ErrorIs(t, err, context.Canceled)

		got, err := e.GetEntitlementGraphSidecar(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("original"), got)
	})

	t.Run("get", func(t *testing.T) {
		got, err := e.GetEntitlementGraphSidecar(canceledCtx)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, got)
	})

	t.Run("delete", func(t *testing.T) {
		err := e.DeleteEntitlementGraphSidecar(canceledCtx)
		require.ErrorIs(t, err, context.Canceled)

		got, err := e.GetEntitlementGraphSidecar(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("original"), got)
	})

	require.NoError(t, e.DeleteEntitlementGraphSidecar(ctx))
	got, err := e.GetEntitlementGraphSidecar(ctx)
	require.NoError(t, err)
	require.Nil(t, got)
}
