package pagination

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// RestartActionError is returned by a paginated list method when the
// request's page token can no longer be used. The syncer restarts the
// action at RestartPageToken; see v2.RestartAction.
//
// The restart is honored only when the error comes straight from the
// action's own list call (ListResourceTypes, or ListResources,
// ListEntitlements, ListGrants, ListStaticEntitlements for a resource
// type) with code FailedPrecondition or Aborted. Returned from any other
// call (GetResource, a spawned cursor), or under another code, it fails the
// sync with ErrRestartRejected. A legacy sqlite c1z passes it through
// unchanged.
//
// A connector built with connectorbuilder may return it bare, wrapped with
// %w, or behind another status error (uhttp.WrapErrors, errors.Join with a
// status error): the builder's list handlers call SurfaceRestartAction, so
// the restart wins over the outer code. A hand-rolled gRPC server must call
// SurfaceRestartAction itself or return the error bare, because gRPC
// serializes the first status in the chain and drops the rest.
type RestartActionError struct {
	RestartPageToken string
	Reason           string
}

func (e *RestartActionError) Error() string {
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
func (e *RestartActionError) GRPCStatus() *status.Status {
	if e == nil {
		return nil
	}
	st := status.New(codes.FailedPrecondition, e.Error())
	withDetails, err := st.WithDetails(v2.RestartAction_builder{RestartPageToken: e.RestartPageToken, Reason: e.Reason}.Build())
	if err != nil {
		return st
	}
	return withDetails
}

// NewRestartActionError builds the error a connector returns when the
// provider rejected the request's page token. An empty restartPageToken
// restarts the action from its first page.
func NewRestartActionError(restartPageToken, reason string) error {
	return &RestartActionError{RestartPageToken: restartPageToken, Reason: reason}
}

// SurfaceRestartAction returns err unchanged unless it wraps a
// RestartActionError that RestartActionFromError does not see because
// another status error comes first in the chain. Then it returns an error
// whose status is FailedPrecondition with the RestartAction detail and
// err's full message, wrapping err. Call it where a connector's error
// becomes a gRPC status; connectorbuilder's list handlers do.
func SurfaceRestartAction(err error) error {
	var restart *RestartActionError
	if !errors.As(err, &restart) || restart == nil {
		return err
	}
	if _, present := RestartActionFromError(err); present {
		return err
	}
	return &surfacedRestartError{restart: restart, cause: err}
}

type surfacedRestartError struct {
	restart *RestartActionError
	cause   error
}

func (e *surfacedRestartError) Error() string { return e.cause.Error() }

func (e *surfacedRestartError) Unwrap() error { return e.cause }

func (e *surfacedRestartError) GRPCStatus() *status.Status {
	st := status.New(codes.FailedPrecondition, e.cause.Error())
	withDetails, err := st.WithDetails(v2.RestartAction_builder{RestartPageToken: e.restart.RestartPageToken, Reason: e.restart.Reason}.Build())
	if err != nil {
		return st
	}
	return withDetails
}

// RestartActionFromError returns the detail when the first status in
// err's chain carries an RestartAction, which is what a gRPC server
// serializes from the same error. It does not check the status code; the
// caller decides which codes honor the detail.
func RestartActionFromError(err error) (*v2.RestartAction, bool) {
	if err == nil {
		return nil, false
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil, false
	}
	for _, detail := range st.Details() {
		if detail, ok := detail.(*v2.RestartAction); ok {
			return detail, true
		}
	}
	return nil, false
}
