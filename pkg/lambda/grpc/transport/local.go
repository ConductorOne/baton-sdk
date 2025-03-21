package transport

import (
	"context"

	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
)

type localTransport struct {
	s *c1_lambda_grpc.Server
}

func (l *localTransport) RoundTrip(ctx context.Context, req *c1_lambda_grpc.Request) (*c1_lambda_grpc.Response, error) {
	return l.s.Handler(ctx, req)
}

// NewLocalClientTransport returns a new local client transport, which delivers messages directly to the handler of the given server.
func NewLocalClientTransport(s *c1_lambda_grpc.Server) c1_lambda_grpc.ClientTransport {
	return &localTransport{
		s: s,
	}
}
