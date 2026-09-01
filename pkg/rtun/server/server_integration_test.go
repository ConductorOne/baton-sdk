package server

import (
	"context"
	"net"
	"testing"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/conductorone/baton-sdk/pkg/rtun/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// testValidator is a production-shaped validator used only in tests.
// It authenticates the link and returns a fixed clientID; HELLO is allowed as-is.
type testValidator struct{ id string }

func (t testValidator) ValidateAuth(ctx context.Context) (string, error)             { return t.id, nil }
func (t testValidator) ValidateHello(ctx context.Context, hello *rtunpb.Hello) error { return nil }

// clientLink adapts the client bidi stream to transport.Link on the client side.
type clientLink struct {
	cli rtunpb.ReverseTunnelService_LinkClient
}

func (c clientLink) Send(f *rtunpb.Frame) error {
	return c.cli.Send(&rtunpb.ReverseTunnelServiceLinkRequest{Frame: f})
}

func (c clientLink) Recv() (*rtunpb.Frame, error) {
	resp, err := c.cli.Recv()
	if err != nil {
		return nil, err
	}
	return resp.GetFrame(), nil
}

func (c clientLink) Context() context.Context { return c.cli.Context() }

// TestReverseGrpcE2E spins up a real gRPC server with Handler, connects a real gRPC client stream for Link,
// runs the standard gRPC health service over rtun on the client, and performs a health check from the owner via Registry.DialContext.
func TestReverseGrpcE2E(t *testing.T) {
	// Server side: real gRPC server with our handler
	reg := NewRegistry()
	h := NewHandler(reg, "server-1", testValidator{id: "client-123"})
	gsrv := grpc.NewServer()
	rtunpb.RegisterReverseTunnelServiceServer(gsrv, h)
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	go func() { _ = gsrv.Serve(l) }()
	defer gsrv.GracefulStop()

	// Client side: dial server and open Link stream
	cc, err := grpc.NewClient("passthrough:///"+l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cc.Close()
	rtunClient := rtunpb.NewReverseTunnelServiceClient(cc)
	stream, err := rtunClient.Link(context.Background())
	require.NoError(t, err)

	// Wrap client stream as transport.Link and start Session
	cl := clientLink{cli: stream}
	sess := transport.NewSession(cl)
	// Send HELLO announcing port 1
	require.NoError(t, cl.Send(&rtunpb.Frame{Sid: 0, Kind: &rtunpb.Frame_Hello{Hello: &rtunpb.Hello{Ports: []uint32{1}}}}))
	ln, err := sess.Listen(context.Background(), 1)
	require.NoError(t, err)
	defer ln.Close()

	// Run the standard gRPC health service over the rtun listener
	cgs := grpc.NewServer()
	hs := health.NewServer()
	healthpb.RegisterHealthServer(cgs, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = cgs.Serve(ln) }()
	defer cgs.GracefulStop()

	// Owner side: reverse dial and perform health check
	// Wait briefly for registration to finish
	time.Sleep(50 * time.Millisecond)
	rconn, err := reg.DialContext(context.Background(), "rtun://client-123:1")
	require.NoError(t, err)
	defer rconn.Close()

	ownerCC, err := grpc.NewClient("passthrough:///ignored",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return rconn, nil }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer ownerCC.Close()

	hc := healthpb.NewHealthClient(ownerCC)
	resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}
