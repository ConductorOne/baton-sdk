package pagination

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// C01, C02: the detail is found whether the error is the typed value, a
// wrapped typed value, or the status a gRPC boundary flattened it to; the
// status carries FailedPrecondition and the detail. Errors without the
// detail, including FailedPrecondition statuses, are not detected.
func TestInvalidateActionFromError(t *testing.T) {
	t.Run("typed nil is absent", func(t *testing.T) {
		var nilTyped *ErrInvalidateAction
		detail, present := InvalidateActionFromError(nilTyped)
		require.False(t, present)
		require.Nil(t, detail)
		detail, present = InvalidateActionFromError(fmt.Errorf("wrapped: %w", nilTyped))
		require.False(t, present)
		require.Nil(t, detail)
	})
	typed := NewErrInvalidateAction("R", "expired")
	flattened := status.Convert(typed).Err()
	require.Equal(t, codes.FailedPrecondition, status.Code(typed))
	require.Equal(t, codes.FailedPrecondition, status.Code(flattened))
	require.Equal(t, "page token invalidated: expired", flattened.Error()[len("rpc error: code = FailedPrecondition desc = "):])

	for name, err := range map[string]error{
		"typed":                  typed,
		"wrapped typed":          fmt.Errorf("list: %w", typed),
		"flattened status":       flattened,
		"wrapped flattened":      fmt.Errorf("list: %w", flattened),
		"joined with another":    errors.Join(errors.New("other"), typed),
		"detail on another code": mustDetail(t, codes.Aborted, "R", "expired"),
	} {
		t.Run(name, func(t *testing.T) {
			detail, present := InvalidateActionFromError(err)
			require.True(t, present)
			require.Equal(t, "R", detail.GetRestartPageToken())
			require.Equal(t, "expired", detail.GetReason())
		})
	}
	t.Run("empty restart token is the first-page restart, not absence", func(t *testing.T) {
		detail, present := InvalidateActionFromError(NewErrInvalidateAction("", ""))
		require.True(t, present)
		require.Empty(t, detail.GetRestartPageToken())
		require.Equal(t, "page token invalidated", NewErrInvalidateAction("", "").Error())
	})
	for name, err := range map[string]error{
		"nil":                 nil,
		"plain":               errors.New("plain"),
		"failed precondition": status.Error(codes.FailedPrecondition, "expired"),
		"other detail":        mustOtherDetail(t),
	} {
		t.Run("absent/"+name, func(t *testing.T) {
			detail, present := InvalidateActionFromError(err)
			require.False(t, present)
			require.Nil(t, detail)
		})
	}
}

func mustDetail(t *testing.T, code codes.Code, restart, reason string) error {
	t.Helper()
	st, err := status.New(code, reason).WithDetails(v2.InvalidateAction_builder{RestartPageToken: restart, Reason: reason}.Build())
	require.NoError(t, err)
	return st.Err()
}

func mustOtherDetail(t *testing.T) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, "other").WithDetails(v2.RateLimitWaitReport_builder{WaitMs: 1}.Build())
	require.NoError(t, err)
	return st.Err()
}
