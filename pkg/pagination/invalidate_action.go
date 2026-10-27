package pagination

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ErrInvalidateAction is returned by a paginated list method when the
// request's page token can no longer be used. The syncer restarts the
// action at RestartPageToken; see v2.InvalidateAction.
type ErrInvalidateAction struct {
	RestartPageToken string
	Reason           string
}

func (e *ErrInvalidateAction) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason == "" {
		return "page token invalidated"
	}
	return "page token invalidated: " + e.Reason
}

// GRPCStatus carries the detail across the connector gRPC boundary, which
// flattens handler errors to a status.
func (e *ErrInvalidateAction) GRPCStatus() *status.Status {
	if e == nil {
		return nil
	}
	st := status.New(codes.FailedPrecondition, e.Error())
	withDetails, err := st.WithDetails(v2.InvalidateAction_builder{RestartPageToken: e.RestartPageToken, Reason: e.Reason}.Build())
	if err != nil {
		return st
	}
	return withDetails
}

// NewErrInvalidateAction builds the error a connector returns when the
// provider rejected the request's page token. An empty restartPageToken
// restarts the action from its first page.
func NewErrInvalidateAction(restartPageToken, reason string) error {
	return &ErrInvalidateAction{RestartPageToken: restartPageToken, Reason: reason}
}

// InvalidateActionFromError returns the detail when err is, wraps, or is a
// status carrying an InvalidateAction. It does not check the status code;
// the caller decides which codes honor the detail.
func InvalidateActionFromError(err error) (*v2.InvalidateAction, bool) {
	if err == nil {
		return nil, false
	}
	var typed *ErrInvalidateAction
	if errors.As(err, &typed) && typed != nil {
		return v2.InvalidateAction_builder{RestartPageToken: typed.RestartPageToken, Reason: typed.Reason}.Build(), true
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil, false
	}
	for _, detail := range st.Details() {
		if detail, ok := detail.(*v2.InvalidateAction); ok {
			return detail, true
		}
	}
	return nil, false
}
