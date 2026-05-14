// Package connectorerrors helps connectors signal terminal (non-retryable)
// failures from Grant and Revoke in a structured way that upstream callers
// can act on.
//
// Today, when a connector returns a plain Go error, the upstream framing
// (in baton-sdk: fmt.Errorf("grant failed: %w", err), then again in the
// caller: codes.Unknown) erases information about whether the failure is
// retryable. Callers fall back to message-substring matching, which is
// brittle and per-connector.
//
// This package gives connectors a single helper that:
//   - picks the right gRPC status code for a known terminal reason, so
//     callers that inspect status.Code(err) automatically classify the
//     error as non-retryable;
//   - attaches a ProvisionFailureDetail (proto) as a gRPC status detail,
//     so callers that want a typed reason and an upstream-facing detail
//     string can read it without parsing free text.
//
// Connectors should still return ordinary errors for transient failures
// (network blips, 5xx, rate limits — those have their own annotation,
// RateLimitDescription). Only call NewTerminalError when you have
// positively identified an upstream signal that means "this will not
// succeed on retry without external state change."
package connectorerrors

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewTerminalError returns an error suitable for returning from a
// Grant or Revoke implementation when the upstream system has signaled
// a permanent failure. The returned error carries:
//
//   - a gRPC status code chosen from `reason` (see codeForReason);
//   - a free-form detail message (formatted with format/args);
//   - a ProvisionFailureDetail proto attached as a status detail, so
//     structured callers can read the reason and detail without
//     parsing the message.
//
// REASON_UNSPECIFIED is rejected — pass a real reason. If you genuinely
// don't know which bucket fits, REASON_VALIDATION_FAILURE is the
// catch-all for upstream rejections that won't change on retry.
func NewTerminalError(reason v2.ProvisionFailureDetail_Reason, format string, args ...any) error {
	if reason == v2.ProvisionFailureDetail_REASON_UNSPECIFIED {
		// Programmer error — callers must pick a reason. Return an
		// error rather than panic so this doesn't take down a worker.
		return status.Error(codes.Internal, "connectorerrors.NewTerminalError: reason must not be REASON_UNSPECIFIED")
	}

	msg := fmt.Sprintf(format, args...)
	st := status.New(codeForReason(reason), msg)

	detail := v2.ProvisionFailureDetail_builder{
		Reason: reason,
		Detail: msg,
	}.Build()

	withDetails, err := st.WithDetails(detail)
	if err != nil {
		// status.WithDetails only fails if the detail can't be
		// marshaled; for a fixed proto we control, that should not
		// happen in practice. Fall back to the status without
		// details so the code is still correct.
		return st.Err()
	}
	return withDetails.Err()
}

// FailureDetail extracts a ProvisionFailureDetail from an error chain,
// returning (detail, true) when one is present. It walks fmt.Errorf
// %w and errors.Join chains via status.FromError (which uses errors.As),
// so wrapped errors are handled transparently.
func FailureDetail(err error) (*v2.ProvisionFailureDetail, bool) {
	if err == nil {
		return nil, false
	}
	st, ok := status.FromError(err)
	if !ok || st == nil {
		return nil, false
	}
	for _, d := range st.Details() {
		// Some details fail to unmarshal (unknown proto types in the
		// caller's binary surface as *errdetails.ErrorInfo-shaped
		// errors here); skip those rather than aborting the walk.
		if pfd, ok := d.(*v2.ProvisionFailureDetail); ok {
			return pfd, true
		}
	}
	return nil, false
}

// IsTerminal reports whether the error carries a ProvisionFailureDetail.
// Equivalent to "_, ok := FailureDetail(err)" with no allocation in the
// false path; convenient when callers only need a yes/no.
func IsTerminal(err error) bool {
	_, ok := FailureDetail(err)
	return ok
}

// codeForReason maps a ProvisionFailureDetail reason to the gRPC status
// code callers should observe.
//
// The codes here are the same ones baton-sdk's existing isClientErrorCode
// logic already treats as non-retryable, so connectors adopting this
// helper get correct retry behavior on day one — even from callers that
// haven't been updated to read ProvisionFailureDetail.
//
//   - PermissionDenied for "the operation itself is not permitted"
//     (REASON_OPERATION_NOT_PERMITTED)
//   - FailedPrecondition for everything else: target state, external
//     management, license exhaustion, validation. These all mean the
//     upstream world is in a state that prevents this operation; retry
//     against the same state will fail the same way.
func codeForReason(reason v2.ProvisionFailureDetail_Reason) codes.Code {
	switch reason {
	case v2.ProvisionFailureDetail_REASON_OPERATION_NOT_PERMITTED:
		return codes.PermissionDenied
	case v2.ProvisionFailureDetail_REASON_USER_NOT_PROVISIONABLE,
		v2.ProvisionFailureDetail_REASON_TARGET_MANAGED_EXTERNALLY,
		v2.ProvisionFailureDetail_REASON_LICENSE_EXHAUSTED,
		v2.ProvisionFailureDetail_REASON_VALIDATION_FAILURE:
		return codes.FailedPrecondition
	default:
		// REASON_UNSPECIFIED is filtered upstream in NewTerminalError;
		// any future reason added without a code mapping defaults to
		// FailedPrecondition rather than Unknown so it still classifies
		// as non-retryable in current callers.
		return codes.FailedPrecondition
	}
}
