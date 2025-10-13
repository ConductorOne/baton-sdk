package gateway

import (
	"context"
	"errors"
	"io"
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

type gwEnv struct {
	addr     string
	cleanup  func()
	accepted chan net.Conn
}

// setupGateway spins up a real ReverseTunnel + ReverseDialer server and returns control.
// If silent is true, the client listener will accept a raw conn and never write to it unless tests do.
func setupGateway(t *testing.T, silent bool) *gwEnv {
	t.Helper()
	reg := server.NewRegistry()
	handler := server.NewHandler(reg, "server-a", testValidator{id: "client-xyz"})
	gw := NewServer(reg, "server-a", nil)

	gsrv := grpc.NewServer()
	rtunpb.RegisterReverseTunnelServiceServer(gsrv, handler)
	rtunpb.RegisterReverseDialerServiceServer(gsrv, gw)
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gsrv.Serve(l) }()

	// Bring up a client link and listen on port 1
	cc, err := grpc.NewClient("passthrough:///"+l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	rtunClient := rtunpb.NewReverseTunnelServiceClient(cc)
	stream, err := rtunClient.Link(context.Background())
	require.NoError(t, err)
	cl := &clientLink{cli: stream}
	sess := transport.NewSession(cl)
	require.NoError(t, cl.Send(&rtunpb.Frame{Sid: 0, Kind: &rtunpb.Frame_Hello{Hello: &rtunpb.Hello{Ports: []uint32{1}}}}))
	ln, err := sess.Listen(context.Background(), 1)
	require.NoError(t, err)

	accepted := make(chan net.Conn, 1)
	var cgs *grpc.Server
	if silent {
		// Accept one conn and expose it to tests
		go func() { c, _ := ln.Accept(); accepted <- c }()
	} else {
		// Serve a health server
		cgs = grpc.NewServer()
		hs := health.NewServer()
		healthpb.RegisterHealthServer(cgs, hs)
		hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		go func() { _ = cgs.Serve(ln) }()
	}

	cleanup := func() {
		if cgs != nil {
			cgs.GracefulStop()
		}
		_ = ln.Close()
		_ = cc.Close()
		gsrv.GracefulStop()
		_ = l.Close()
	}
	return &gwEnv{addr: l.Addr().String(), cleanup: cleanup, accepted: accepted}
}

func TestGatewayConnReadDeadline(t *testing.T) {
	env := setupGateway(t, true)
	defer env.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d := NewDialer(env.addr, insecure.NewCredentials())
	gwc, err := d.DialContext(ctx, "client-xyz", 1)
	require.NoError(t, err)
	defer gwc.Close()

	_ = gwc.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	_, err = gwc.Read(make([]byte, 1))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestGatewayConnCloseIdempotentAndWriteAfterClose(t *testing.T) {
	env := setupGateway(t, true)
	defer env.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d := NewDialer(env.addr, insecure.NewCredentials())
	gwc, err := d.DialContext(ctx, "client-xyz", 1)
	require.NoError(t, err)

	require.NoError(t, gwc.Close())
	require.NoError(t, gwc.Close())
	_, err = gwc.Write([]byte("x"))
	require.True(t, errors.Is(err, net.ErrClosed))
}

func TestGatewayConnEOFOnRemoteClose(t *testing.T) {
	env := setupGateway(t, true)
	defer env.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d := NewDialer(env.addr, insecure.NewCredentials())
	gwc, err := d.DialContext(ctx, "client-xyz", 1)
	require.NoError(t, err)
	defer gwc.Close()

	// Remote close: wait for accept and then close the accepted conn to generate FIN.
	select {
	case rc := <-env.accepted:
		_ = rc.Close()
	case <-time.After(200 * time.Millisecond):
		t.Fatal("no accepted conn")
	}

	// Read should return EOF
	_, err = gwc.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestGatewayConnWriteAndRemoteReceive(t *testing.T) {
	env := setupGateway(t, true)
	defer env.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	d := NewDialer(env.addr, insecure.NewCredentials())
	gwc, err := d.DialContext(ctx, "client-xyz", 1)
	require.NoError(t, err)
	defer gwc.Close()

	// Wait for remote accept
	var rc net.Conn
	select {
	case rc = <-env.accepted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("no accepted conn")
	}

	// Write and verify bytes arrive remotely
	msg := []byte("hello")
	n, err := gwc.Write(msg)
	require.NoError(t, err)
	require.Equal(t, len(msg), n)
	buf := make([]byte, len(msg))
	_, err = rc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, msg, buf)

	// Local FIN: close gwc; remote should see EOF
	_ = gwc.Close()
	tmp := make([]byte, 1)
	_, err = rc.Read(tmp)
	require.ErrorIs(t, err, io.EOF)
}
