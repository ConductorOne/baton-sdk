package server

import (
	"context"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/conductorone/baton-sdk/pkg/rtun/transport"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// Handler implements the ReverseTunnel gRPC service, binding Links to Sessions and the Registry.
type Handler struct {
	rtunpb.UnimplementedReverseTunnelServiceServer

	reg      *Registry
	serverID string
	tv       TokenValidator

	// metrics (optional)
	m *serverMetrics
}

// NewHandler constructs a ReverseTunnel gRPC handler bound to a `Registry` and `TokenValidator`.
// It optionally enables metrics if provided via options.
func NewHandler(reg *Registry, serverID string, tv TokenValidator, opts ...Option) rtunpb.ReverseTunnelServiceServer {
	var o options
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&o)
	}
	h := &Handler{reg: reg, serverID: serverID, tv: tv}
	if o.metrics != nil {
		h.m = newServerMetrics(o.metrics)
	}
	return h
}

// Link accepts a bidi stream and binds it to a transport.Session after validating HELLO.
func (h *Handler) Link(stream rtunpb.ReverseTunnelService_LinkServer) error {
	// Wrap the gRPC stream as transport.Link
	l := &grpcLink{srv: stream}

	// Authenticate and determine clientID via TokenValidator BEFORE waiting for HELLO.
	if h.tv == nil {
		return ErrProtocol
	}
	clientID, err := h.tv.ValidateAuth(stream.Context())
	if err != nil {
		return err
	}
	logger := ctxzap.Extract(stream.Context()).With(zap.String("client_id", clientID))
	logger.Info("auth ok")

	// First frame must be HELLO with timeout; if not HELLO, protocol violation
	type recvResult struct {
		fr  *rtunpb.Frame
		err error
	}
	resCh := make(chan recvResult, 1)
	go func() {
		fr, err := l.Recv()
		resCh <- recvResult{fr: fr, err: err}
	}()
	helloTimeout := 15 * time.Second
	var hello *rtunpb.Hello
	select {
	case res := <-resCh:
		if res.err != nil {
			return res.err
		}
		hello = res.fr.GetHello()
		if hello == nil {
			logger.Warn("first frame not HELLO; closing")
			if h.m != nil {
				h.m.helloRejected(stream.Context(), "not_hello")
			}
			return ErrProtocol
		}
		logger.Info("HELLO received", zap.Uint32s("ports", hello.GetPorts()))
		// enforce reasonable HELLO port count limit (2500)
		if len(hello.GetPorts()) > 2500 {
			logger.Warn("HELLO ports exceed limit", zap.Int("count", len(hello.GetPorts())))
			if h.m != nil {
				h.m.helloPortsOverLimit(stream.Context())
			}
			return ErrProtocol
		}
		if err := h.tv.ValidateHello(stream.Context(), hello); err != nil {
			if h.m != nil {
				h.m.helloRejected(stream.Context(), "validate_failed")
			}
			return err
		}
	case <-time.After(helloTimeout):
		logger.Warn("HELLO timeout")
		if h.m != nil {
			h.m.helloTimeout(stream.Context())
		}
		return ErrHelloTimeout
	}

	// Bind Session and start Recv loop.
	var sessOpts []transport.Option
	ports := hello.GetPorts()
	if len(ports) > 0 {
		sessOpts = append(sessOpts, transport.WithAllowedPorts(ports))
	}
	// Pass metrics down to transport if available
	if h.m != nil && h.m.h != nil {
		sessOpts = append(sessOpts, transport.WithMetricsHandler(h.m.h))
	}
	s := transport.NewSession(l, sessOpts...)
	if h.m != nil {
		h.m.registryRegister(stream.Context())
	}
	h.reg.Register(stream.Context(), clientID, s)
	defer h.reg.Unregister(stream.Context(), clientID)
	defer func() {
		if h.m != nil {
			h.m.registryUnregister(stream.Context())
		}
	}()

	// Start the session; recvLoop will run until link Recv errors (stream close/cancel)
	s.Start()

	// Block until stream context is done (client disconnect or server shutdown)
	<-stream.Context().Done()
	return stream.Context().Err()
}

// grpcLink adapts the gRPC server stream to transport.Link.
type grpcLink struct {
	srv rtunpb.ReverseTunnelService_LinkServer
}

func (g *grpcLink) Send(f *rtunpb.Frame) error {
	return g.srv.Send(&rtunpb.ReverseTunnelServiceLinkResponse{Frame: f})
}

func (g *grpcLink) Recv() (*rtunpb.Frame, error) {
	fr, err := g.srv.Recv()
	if err != nil {
		return nil, err
	}
	return fr.GetFrame(), nil
}

func (g *grpcLink) Context() context.Context { return g.srv.Context() }
