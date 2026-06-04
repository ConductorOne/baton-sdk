package grant

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewErrGrantCancelled(t *testing.T) {
	err := NewErrGrantCancelled("rejected by policy")

	require.EqualError(t, err, "rejected by policy")

	var grantCancelled *ErrGrantCancelled
	require.True(t, errors.As(err, &grantCancelled))
	require.Equal(t, "rejected by policy", grantCancelled.Reason)
}

func TestErrGrantCancelledNil(t *testing.T) {
	require.Empty(t, (*ErrGrantCancelled)(nil).Error())
}

func TestErrGrantCancelledGRPCStatus(t *testing.T) {
	st := (&ErrGrantCancelled{Reason: "rejected by policy"}).GRPCStatus()
	require.Equal(t, codes.FailedPrecondition, st.Code())

	reason, ok := GrantCancelledReasonFromStatus(st)
	require.True(t, ok)
	require.Equal(t, "rejected by policy", reason)
}

func TestIsErrGrantCancelled(t *testing.T) {
	t.Run("typed error", func(t *testing.T) {
		reason, ok := IsErrGrantCancelled(NewErrGrantCancelled("rejected by policy"))
		require.True(t, ok)
		require.Equal(t, "rejected by policy", reason)
	})

	t.Run("wrapped typed error", func(t *testing.T) {
		wrapped := fmt.Errorf("grant failed: %w", NewErrGrantCancelled("rejected by policy"))
		reason, ok := IsErrGrantCancelled(wrapped)
		require.True(t, ok)
		require.Equal(t, "rejected by policy", reason)
	})

	t.Run("flattened gRPC status", func(t *testing.T) {
		// Simulate the connector gRPC boundary discarding the Go type but
		// preserving the status with its detail.
		flattened := (&ErrGrantCancelled{Reason: "rejected by policy"}).GRPCStatus().Err()
		reason, ok := IsErrGrantCancelled(flattened)
		require.True(t, ok)
		require.Equal(t, "rejected by policy", reason)
	})

	t.Run("unrelated error", func(t *testing.T) {
		_, ok := IsErrGrantCancelled(errors.New("boom"))
		require.False(t, ok)
	})

	t.Run("unrelated status", func(t *testing.T) {
		_, ok := IsErrGrantCancelled(status.Error(codes.FailedPrecondition, "some other precondition"))
		require.False(t, ok)
	})

	t.Run("nil", func(t *testing.T) {
		_, ok := IsErrGrantCancelled(nil)
		require.False(t, ok)
	})
}
