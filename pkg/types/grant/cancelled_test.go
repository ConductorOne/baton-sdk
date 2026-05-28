package grant

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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
