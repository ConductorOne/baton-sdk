package grant

import (
	"errors"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	GrantCancelledErrorReason = "grant_cancelled"
	GrantCancelledErrorDomain = "github.com/conductorone/baton-sdk"

	grantCancelledReasonMetadataKey = "reason"
)

// ErrGrantCancelled indicates a connector intentionally declined to create a grant.
type ErrGrantCancelled struct {
	Reason string
}

func (e *ErrGrantCancelled) Error() string {
	if e == nil {
		return ""
	}
	return e.Reason
}

// GRPCStatus encodes the cancellation as a FailedPrecondition status carrying a typed ErrorInfo detail.
// The status flattening that the connector gRPC boundary applies to handler errors otherwise discards the Go error type;
// the detail rides the wire so C1 can recognize an intentional decline rather than treating it as a connector failure.
func (e *ErrGrantCancelled) GRPCStatus() *status.Status {
	st := status.New(codes.FailedPrecondition, e.Error())
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: GrantCancelledErrorReason,
		Domain: GrantCancelledErrorDomain,
		Metadata: map[string]string{
			grantCancelledReasonMetadataKey: e.Reason,
		},
	})
	if err != nil {
		return st
	}
	return withDetails
}

func NewErrGrantCancelled(reason string) error {
	return &ErrGrantCancelled{Reason: reason}
}

// IsErrGrantCancelled reports whether err is an intentionally declined grant, returning the connector-supplied reason.
// It matches both the typed error (in-process) and the flattened gRPC status detail (across a transport).
func IsErrGrantCancelled(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var e *ErrGrantCancelled
	if errors.As(err, &e) {
		return e.Reason, true
	}
	if st, ok := status.FromError(err); ok {
		return GrantCancelledReasonFromStatus(st)
	}
	return "", false
}

// GrantCancelledReasonFromStatus returns the connector-supplied reason if status carries the grant-cancelled ErrorInfo detail.
func GrantCancelledReasonFromStatus(st *status.Status) (string, bool) {
	if st == nil {
		return "", false
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if info.GetReason() == GrantCancelledErrorReason && info.GetDomain() == GrantCancelledErrorDomain {
			return info.GetMetadata()[grantCancelledReasonMetadataKey], true
		}
	}
	return "", false
}
