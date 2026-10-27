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
		var nilTyped *RestartActionError
		detail, present := RestartActionFromError(nilTyped)
		require.False(t, present)
		require.Nil(t, detail)
		detail, present = RestartActionFromError(fmt.Errorf("wrapped: %w", nilTyped))
		require.False(t, present)
		require.Nil(t, detail)
	})
	typed := NewRestartActionError("R", "expired")
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
			detail, present := RestartActionFromError(err)
			require.True(t, present)
			require.Equal(t, "R", detail.GetRestartPageToken())
			require.Equal(t, "expired", detail.GetReason())
		})
	}
	t.Run("empty restart token is the first-page restart, not absence", func(t *testing.T) {
		detail, present := RestartActionFromError(NewRestartActionError("", ""))
		require.True(t, present)
		require.Empty(t, detail.GetRestartPageToken())
		require.Equal(t, "page token invalidated", NewRestartActionError("", "").Error())
	})
	for name, err := range map[string]error{
		"nil":                   nil,
		"plain":                 errors.New("plain"),
		"failed precondition":   status.Error(codes.FailedPrecondition, "expired"),
		"other detail":          mustOtherDetail(t),
		"behind another status": errors.Join(status.Error(codes.Internal, "http 400"), typed),
	} {
		t.Run("absent/"+name, func(t *testing.T) {
			detail, present := RestartActionFromError(err)
			require.False(t, present)
			require.Nil(t, detail)
		})
	}
}

// The in-process answer equals what the syncer sees after a gRPC boundary
// flattens the same error to its first status: a connector test cannot
// restart where a deployed connector would not.
func TestInvalidateActionFromErrorMatchesWire(t *testing.T) {
	typed := NewRestartActionError("R", "expired")
	other := status.Error(codes.Internal, "http 400")
	for name, err := range map[string]error{
		"typed":                 typed,
		"wrapped once":          fmt.Errorf("list: %w", typed),
		"wrapped twice":         fmt.Errorf("sdk: %w", fmt.Errorf("list: %w", typed)),
		"joined after plain":    errors.Join(errors.New("plain"), typed),
		"joined before status":  errors.Join(typed, other),
		"joined after status":   errors.Join(other, typed),
		"wrapped behind status": fmt.Errorf("list: %w", errors.Join(other, typed)),
		"status without detail": other,
	} {
		t.Run(name, func(t *testing.T) {
			_, inProcess := RestartActionFromError(err)
			_, wire := RestartActionFromError(status.Convert(err).Err())
			require.Equal(t, wire, inProcess)
		})
	}
	_, present := RestartActionFromError(errors.Join(typed, other))
	require.True(t, present, "the detail's status first")
	_, present = RestartActionFromError(errors.Join(other, typed))
	require.False(t, present, "another status first")
}

// SurfaceRestartAction leaves alone every error the wire already carries
// correctly, and lifts a RestartActionError from behind another status so
// the serialized status is FailedPrecondition with the detail and the
// original message, still unwrapping to the original error.
func TestSurfaceRestartAction(t *testing.T) {
	typed := NewRestartActionError("R", "expired")
	other := status.Error(codes.Unavailable, "http 503")
	for name, err := range map[string]error{
		"nil":                   nil,
		"plain":                 errors.New("plain"),
		"typed":                 typed,
		"wrapped typed":         fmt.Errorf("list: %w", typed),
		"flattened status":      status.Convert(typed).Err(),
		"joined before status":  errors.Join(typed, other),
		"status without detail": other,
		"typed nil":             fmt.Errorf("list: %w", (*RestartActionError)(nil)),
	} {
		t.Run("unchanged/"+name, func(t *testing.T) {
			if err == nil {
				require.NoError(t, SurfaceRestartAction(nil))
				return
			}
			require.Same(t, err, SurfaceRestartAction(err))
		})
	}
	for name, err := range map[string]error{
		"joined after status":   errors.Join(other, typed),
		"wrapped behind status": fmt.Errorf("list: %w", errors.Join(other, typed)),
		"behind two statuses":   errors.Join(status.Error(codes.Internal, "first"), other, typed),
	} {
		t.Run("surfaced/"+name, func(t *testing.T) {
			_, before := RestartActionFromError(err)
			require.False(t, before)
			surfaced := SurfaceRestartAction(err)
			require.ErrorIs(t, surfaced, typed)
			require.ErrorIs(t, surfaced, other)
			require.Equal(t, err.Error(), surfaced.Error())
			wire := status.Convert(surfaced)
			require.Equal(t, codes.FailedPrecondition, wire.Code())
			require.Equal(t, err.Error(), wire.Message())
			detail, present := RestartActionFromError(wire.Err())
			require.True(t, present)
			require.Equal(t, "R", detail.GetRestartPageToken())
			require.Equal(t, "expired", detail.GetReason())
			require.Same(t, surfaced, SurfaceRestartAction(surfaced), "idempotent")
		})
	}
}

func mustDetail(t *testing.T, code codes.Code, restart, reason string) error {
	t.Helper()
	st, err := status.New(code, reason).WithDetails(v2.RestartAction_builder{RestartPageToken: restart, Reason: reason}.Build())
	require.NoError(t, err)
	return st.Err()
}

func mustOtherDetail(t *testing.T) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, "other").WithDetails(v2.RateLimitWaitReport_builder{WaitMs: 1}.Build())
	require.NoError(t, err)
	return st.Err()
}
