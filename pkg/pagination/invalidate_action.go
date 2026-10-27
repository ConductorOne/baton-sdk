package pagination

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ErrInvalidateAction is returned by a paginated list method when the
// request's page token can no longer be used. The syncer restarts the
// action at RestartPageToken; see v2.InvalidateAction.
//
// The restart is honored only when the error comes straight from the
// action's own list call (ListResourceTypes, or ListResources,
// ListEntitlements, ListGrants, ListStaticEntitlements for a resource
// type) with code FailedPrecondition or Aborted. Returned from any other
// call (GetResource, a spawned cursor), or under another code, it fails the
// sync as a classification error. A legacy sqlite c1z passes it through
// unchanged.
//
// Return it bare or wrapped with %w. Do not put another status error in
// front of it (uhttp.WrapErrors, errors.Join with a status error): gRPC
// serializes the first status in the chain and the detail is lost. The
// in-process check reads the same first status, so a connector test and a
// deployed connector agree.
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

// InvalidateActionFromError returns the detail when the first status in
// err's chain carries an InvalidateAction, which is what a gRPC server
// serializes from the same error. It does not check the status code; the
// caller decides which codes honor the detail.
func InvalidateActionFromError(err error) (*v2.InvalidateAction, bool) {
	if err == nil {
		return nil, false
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
