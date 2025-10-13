package match

import (
	"context"
	"net"
	"testing"
	"time"

	rtunpb "github.com/conductorone/baton-sdk/pb/c1/connectorapi/rtun/v1"
	"github.com/conductorone/baton-sdk/pkg/rtun/match/memory"
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

// TestOwnerRouterTwoServers simulates:
// - Server A owns client-123 (handler + registry).
// - Server B is a caller that uses OwnerRouter to find A, dials A, and uses A's registry to reverse-dial client-123.
func TestOwnerRouterTwoServers(t *testing.T) {
	presence := memory.NewPresence()
	directory := memory.NewDirectory()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Server A setup
	regA := server.NewRegistry()
	handlerA := server.NewHandler(regA, "server-a", testValidator{id: "client-123"})
	gsrvA := grpc.NewServer()
	rtunpb.RegisterReverseTunnelServiceServer(gsrvA, handlerA)
	lA, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gsrvA.Serve(lA) }()

	// Register server A in directory
	require.NoError(t, directory.Advertise(ctx, "server-a", lA.Addr().String(), 10*time.Second))

	// Client connects to server A with cancelable context
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel()

	ccA, err := grpc.Dial(lA.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	rtunClientA := rtunpb.NewReverseTunnelServiceClient(ccA)
	streamA, err := rtunClientA.Link(clientCtx)
	require.NoError(t, err)

	// Client-side session and HELLO
	clA := &clientLink{cli: streamA}
	sessA := transport.NewSession(clA)
	require.NoError(t, clA.Send(&rtunpb.Frame{Sid: 0, Kind: &rtunpb.Frame_Hello{Hello: &rtunpb.Hello{Ports: []uint32{1}}}}))
	lnA, err := sessA.Listen(ctx, 1)
	require.NoError(t, err)

	// Client runs health service
	cgsA := grpc.NewServer()
	hsA := health.NewServer()
	healthpb.RegisterHealthServer(cgsA, hsA)
	hsA.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = cgsA.Serve(lnA) }()

	// Mark client-123 online on server-a in presence
	require.NoError(t, presence.SetPorts(ctx, "client-123", []uint32{1}))
	require.NoError(t, presence.Announce(ctx, "client-123", "server-a", 10*time.Second))

	// Server B (caller) uses OwnerRouter to find and dial server A
	router := &OwnerRouter{
		Locator:   &Locator{Presence: presence},
		Directory: directory,
		DialOpts:  []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
	// Wait briefly for registration
	time.Sleep(50 * time.Millisecond)

	ownerConn, ownerID, err := router.DialOwner(ctx, "client-123")
	require.NoError(t, err)
	require.Equal(t, "server-a", ownerID)

	// Now server B has a connection to server A. In production, B would invoke a service on A
	// that internally uses regA.DialContext. For this test, simulate by directly using regA
	// (since we're in the same process).
	rconn, err := LocalReverseDial(ctx, regA, "client-123", 1)
	require.NoError(t, err)

	// Perform health check over reverse connection
	hc := healthpb.NewHealthClient(rconn)
	resp, err := hc.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

	// Clean up in correct order
	rconn.Close()
	ownerConn.Close()
	cgsA.GracefulStop()
	lnA.Close()
	clientCancel() // Close client stream
	ccA.Close()
	gsrvA.GracefulStop()
	lA.Close()
}

// clientLink adapts the client bidi stream to transport.Link.
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
