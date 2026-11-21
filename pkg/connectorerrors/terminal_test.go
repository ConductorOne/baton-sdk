package connectorerrors

import (
	"errors"
	"fmt"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewTerminalError_AttachesDetailAndCode(t *testing.T) {
	cases := []struct {
		name     string
		reason   v2.ProvisionFailureDetail_Reason
		wantCode codes.Code
	}{
		{"user_status", v2.ProvisionFailureDetail_REASON_USER_NOT_PROVISIONABLE, codes.FailedPrecondition},
		{"group_rule", v2.ProvisionFailureDetail_REASON_TARGET_MANAGED_EXTERNALLY, codes.FailedPrecondition},
		{"license", v2.ProvisionFailureDetail_REASON_LICENSE_EXHAUSTED, codes.FailedPrecondition},
		{"validation", v2.ProvisionFailureDetail_REASON_VALIDATION_FAILURE, codes.FailedPrecondition},
		{"not_permitted", v2.ProvisionFailureDetail_REASON_OPERATION_NOT_PERMITTED, codes.PermissionDenied},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewTerminalError(tc.reason, "upstream said: %s", "no soup")
			require.Error(t, err)

			assert.Equal(t, tc.wantCode, status.Code(err), "gRPC code should be picked from reason")

			detail, ok := FailureDetail(err)
			require.True(t, ok, "FailureDetail must extract the proto")
			assert.Equal(t, tc.reason, detail.GetReason())
			assert.Equal(t, "upstream said: no soup", detail.GetDetail())

			assert.True(t, IsTerminal(err), "IsTerminal should agree")
		})
	}
}

func TestNewTerminalError_RejectsUnspecified(t *testing.T) {
	err := NewTerminalError(v2.ProvisionFailureDetail_REASON_UNSPECIFIED, "should fail")
	require.Error(t, err)
	// Returned as Internal so a caller can still log it; the point of
	// the rejection is to fail the programmer error loudly, not to
	// classify the underlying upstream error.
	assert.Equal(t, codes.Internal, status.Code(err))
	_, ok := FailureDetail(err)
	assert.False(t, ok, "rejection error must not carry a ProvisionFailureDetail")
}

func TestFailureDetail_UnwrapsAcrossErrorf(t *testing.T) {
	// This mirrors the real call chain: connector returns NewTerminalError,
	// connectorbuilder wraps with fmt.Errorf("grant failed: %w", err),
	// the caller may wrap again with errors.Join. status.FromError walks
	// the chain via errors.As, so FailureDetail should still find the
	// detail.
	base := NewTerminalError(v2.ProvisionFailureDetail_REASON_USER_NOT_PROVISIONABLE,
		"Okta E0000038: user is SUSPENDED")

	wrappedOnce := fmt.Errorf("grant failed: %w", base)
	wrappedTwice := errors.Join(errors.New("ErrConnectorInvokeReturnedError"), wrappedOnce)

	t.Run("fmt.Errorf %w", func(t *testing.T) {
		detail, ok := FailureDetail(wrappedOnce)
		require.True(t, ok)
		assert.Equal(t, v2.ProvisionFailureDetail_REASON_USER_NOT_PROVISIONABLE, detail.GetReason())
	})

	t.Run("errors.Join over fmt.Errorf", func(t *testing.T) {
		detail, ok := FailureDetail(wrappedTwice)
		require.True(t, ok)
		assert.Equal(t, v2.ProvisionFailureDetail_REASON_USER_NOT_PROVISIONABLE, detail.GetReason())
	})

	t.Run("status.Code survives wrapping", func(t *testing.T) {
		assert.Equal(t, codes.FailedPrecondition, status.Code(wrappedOnce))
		assert.Equal(t, codes.FailedPrecondition, status.Code(wrappedTwice))
	})
}

func TestFailureDetail_PlainErrors(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		d, ok := FailureDetail(nil)
		assert.False(t, ok)
		assert.Nil(t, d)
		assert.False(t, IsTerminal(nil))
	})

	t.Run("plain go error", func(t *testing.T) {
		d, ok := FailureDetail(errors.New("network blip"))
		assert.False(t, ok)
		assert.Nil(t, d)
	})

	t.Run("status without detail", func(t *testing.T) {
		d, ok := FailureDetail(status.Error(codes.FailedPrecondition, "no detail attached"))
		assert.False(t, ok, "FailedPrecondition without ProvisionFailureDetail must not look terminal-typed")
		assert.Nil(t, d)
	})
}
