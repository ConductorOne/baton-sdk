package gateway

import (
	"context"
	"net"
	"testing"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/conductorone/baton-sdk/pkg/rtun/server"
	"github.com/conductorone/baton-sdk/pkg/rtun/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// testValidator for integration tests.
type testValidator struct{ id string }

func (t testValidator) ValidateAuth(ctx context.Context) (string, error)             { return t.id, nil }
func (t testValidator) ValidateHello(ctx context.Context, hello *rtunpb.Hello) error { return nil }

// clientLink adapts client bidi stream to transport.Link.
type clientLink struct {
	cli rtunpb.ReverseTunnelService_LinkClient
}

func (c *clientLink) Send(f *rtunpb.Frame) error {
	return c.cli.Send(&rtunpb.ReverseTunnelServiceLinkRequest{Frame: f})
}

func (c *clientLink) Recv() (*rtunpb.Frame, error) {
	fr, err := c.cli.Recv()
	if err != nil {
		return nil, err
	}
	return fr.GetFrame(), nil
}

func (c *clientLink) Context() context.Context { return c.cli.Context() }

// TestGatewayE2E validates the full gateway stack:
// - Client connects to server A (handler+registry).
// - Gateway server on A.
// - Remote caller uses gateway.Dialer to get net.Conn to client.
// - Caller performs gRPC health check over gateway conn.
func TestGatewayE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Server A: rtun handler + registry + gateway
	regA := server.NewRegistry()
	handlerA := server.NewHandler(regA, "server-a", testValidator{id: "client-123"})
	gwA := NewServer(regA, "server-a", nil)

	gsrvA := grpc.NewServer()
	rtunpb.RegisterReverseTunnelServiceServer(gsrvA, handlerA)
	rtunpb.RegisterReverseDialerServiceServer(gsrvA, gwA)

	lA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gsrvA.Serve(lA) }()

	// Client connects to server A
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel()

	ccA, err := grpc.Dial(lA.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	rtunClient := rtunpb.NewReverseTunnelServiceClient(ccA)
	stream, err := rtunClient.Link(clientCtx)
	require.NoError(t, err)

	// Client session: send HELLO, listen on port 1
	cl := &clientLink{cli: stream}
	sess := transport.NewSession(cl)
	require.NoError(t, cl.Send(&rtunpb.Frame{Sid: 0, Kind: &rtunpb.Frame_Hello{Hello: &rtunpb.Hello{Ports: []uint32{1}}}}))
	ln, err := sess.Listen(ctx, 1)
	require.NoError(t, err)

	// Client runs health service
	cgs := grpc.NewServer()
	hs := health.NewServer()
	healthpb.RegisterHealthServer(cgs, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = cgs.Serve(ln) }()

	// Wait for registration
	time.Sleep(50 * time.Millisecond)

	// Remote caller: use gateway.Dialer to get net.Conn to client
	dialer := NewDialer(lA.Addr().String(), insecure.NewCredentials(), nil)
	gwConn, err := dialer.DialContext(ctx, "client-123", 1)
	require.NoError(t, err)
	defer gwConn.Close()

	// Wrap gateway conn in grpc.Dial and perform health check
	callerCC, err := grpc.DialContext(ctx, "ignored",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return gwConn, nil }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer callerCC.Close()

	hc := healthpb.NewHealthClient(callerCC)
	resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

	// Cleanup
	callerCC.Close()
	gwConn.Close()
	cgs.GracefulStop()
	ln.Close()
	clientCancel()
	ccA.Close()
	gsrvA.GracefulStop()
	lA.Close()
}

func TestGatewayNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Server with gateway but no client registered
	regA := server.NewRegistry()
	gwA := NewServer(regA, "server-a", nil)

	gsrvA := grpc.NewServer()
	rtunpb.RegisterReverseDialerServiceServer(gsrvA, gwA)

	lA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lA.Close()
	go func() { _ = gsrvA.Serve(lA) }()
	defer gsrvA.GracefulStop()

	// Caller tries to dial non-existent client
	dialer := NewDialer(lA.Addr().String(), insecure.NewCredentials(), nil)
	_, err = dialer.DialContext(ctx, "client-nonexistent", 1)
	require.ErrorIs(t, err, ErrNotFound)
}
