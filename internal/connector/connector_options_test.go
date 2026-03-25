package connector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithGRPCMaxMsgSize(t *testing.T) {
	t.Run("sets grpcMaxMsgSize on wrapper", func(t *testing.T) {
		w := &wrapper{}
		opt := WithGRPCMaxMsgSize(16 * 1024 * 1024)
		err := opt(context.Background(), w)
		require.NoError(t, err)
		require.Equal(t, 16*1024*1024, w.grpcMaxMsgSize)
	})

	t.Run("zero value leaves field at zero", func(t *testing.T) {
		w := &wrapper{}
		opt := WithGRPCMaxMsgSize(0)
		err := opt(context.Background(), w)
		require.NoError(t, err)
		require.Equal(t, 0, w.grpcMaxMsgSize)
	})

	t.Run("last option wins", func(t *testing.T) {
		w := &wrapper{}
		opt1 := WithGRPCMaxMsgSize(8 * 1024 * 1024)
		opt2 := WithGRPCMaxMsgSize(32 * 1024 * 1024)
		require.NoError(t, opt1(context.Background(), w))
		require.NoError(t, opt2(context.Background(), w))
		require.Equal(t, 32*1024*1024, w.grpcMaxMsgSize)
	})
}
