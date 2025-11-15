package server

import (
	"context"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
)

// TokenValidator decouples auth from the rtun transport.
type TokenValidator interface {
	// ValidateAuth is invoked when a stream is first connected. It should authenticate the caller
	// from the gRPC context (e.g., mTLS, headers) and return the bound clientID.
	ValidateAuth(ctx context.Context) (clientID string, err error)
	// ValidateHello validates the HELLO frame contents (e.g., ports, protocol negotiation).
	ValidateHello(ctx context.Context, hello *rtunpb.Hello) error
}
