package grant

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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

func TestGrantCancelledReasonFromError(t *testing.T) {
	reason, ok := GrantCancelledReasonFromError(errors.Join(errors.New("wrapper"), NewErrGrantCancelled(" rejected by policy ")))
	require.True(t, ok)
	require.Equal(t, "rejected by policy", reason)
}

func TestGrantCancelledReasonFromError_DefaultReason(t *testing.T) {
	reason, ok := GrantCancelledReasonFromError(NewErrGrantCancelled(""))
	require.True(t, ok)
	require.Equal(t, GrantCancelledDefaultReason, reason)
}

func TestGrantCancelledReasonFromStatus(t *testing.T) {
	st, err := StatusWithGrantCancelledErrorInfo(nil, "reject_if reason")
	require.NoError(t, err)

	reason, ok := GrantCancelledReasonFromStatus(st.Proto())
	require.True(t, ok)
	require.Equal(t, "reject_if reason", reason)
}

func TestGrantCancelledReasonFromStatus_MessageFallback(t *testing.T) {
	st, err := status.New(codes.Unknown, "status message").WithDetails(&errdetails.ErrorInfo{
		Domain: GrantCancelledErrorInfoDomain,
		Reason: GrantCancelledErrorInfoReason,
	})
	require.NoError(t, err)

	reason, ok := GrantCancelledReasonFromStatus(st.Proto())
	require.True(t, ok)
	require.Equal(t, "status message", reason)
}

func TestGrantCancelledReasonFromStatus_NoMarker(t *testing.T) {
	reason, ok := GrantCancelledReasonFromStatus(status.New(codes.Unknown, "ordinary error").Proto())
	require.False(t, ok)
	require.Empty(t, reason)
}
