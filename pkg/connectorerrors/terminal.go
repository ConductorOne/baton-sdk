// Package connectorerrors helps connectors signal terminal (non-retryable)
// failures from Grant and Revoke in a way upstream callers can act on.
//
// Use NewTerminalError when an upstream API has signaled a permanent
// failure (target user state, externally-managed group, license cap,
// validation). For transient failures — 5xx, network blips, rate limits
// — return ordinary errors; rate limits have their own annotation
// (RateLimitDescription) on the response.
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
//
// Contract: Reason is the authoritative semantic signal; the gRPC code
// is a coarse retryability hint maintained for callers that classify
// purely on status.Code. New callers should branch on Reason.
func NewTerminalError(reason v2.ProvisionFailureDetail_Reason, format string, args ...any) error {
	if reason == v2.ProvisionFailureDetail_REASON_UNSPECIFIED {
		// Programmer error — callers must pick a reason. Return an
		// error rather than panic so this doesn't take down a worker.
		return status.Error(codes.Internal, "connectorerrors.NewTerminalError: reason must not be REASON_UNSPECIFIED")
	}

	// Pass format through verbatim when no args, so a `%` in an
	// upstream message ("E0000099: 50% capacity") isn't reinterpreted.
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	st := status.New(codeForReason(reason), msg)

	detail := v2.ProvisionFailureDetail_builder{
		Reason: reason,
		Detail: msg,
	}.Build()

	withDetails, err := st.WithDetails(detail)
	if err != nil {
		// Unreachable for a proto we own — WithDetails only fails on
		// marshal error, and ProvisionFailureDetail has no fields that
		// can fail to marshal. Panic so we notice if a future field
		// change breaks the invariant, rather than silently returning
		// a status without the detail.
		panic(fmt.Sprintf("connectorerrors: status.WithDetails failed on ProvisionFailureDetail: %v", err))
	}
	return withDetails.Err()
}

// FailureDetail extracts a ProvisionFailureDetail from an error chain,
// returning (detail, true) when one is present. It walks fmt.Errorf
// %w and errors.Join chains via status.FromError (which uses errors.As),
// so wrapped errors are handled transparently.
//
// Forward compat: proto3 preserves unknown enum integers on the wire,
// so a future REASON_* value sent by a newer connector decodes here
// without error and detail.GetReason() returns the integer. Callers
// switching on Reason must include a default case for unknown values.
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
// Equivalent to `_, ok := FailureDetail(err); return ok` — convenient
// when callers only need a yes/no.
func IsTerminal(err error) bool {
	_, ok := FailureDetail(err)
	return ok
}

// codeForReason maps a ProvisionFailureDetail reason to the gRPC status
// code callers should observe. This mapping is part of the package
// contract — connector authors should not rely on a specific code beyond
// "non-retryable", and callers wanting fine-grained classification
// should read Reason from the attached ProvisionFailureDetail.
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
