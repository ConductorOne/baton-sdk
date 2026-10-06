package connectorbuilder

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// countingTypedExecutor is a real gRPC service implementation that records
// how many times the legacy Issue entrypoint was reached.
type countingTypedExecutor struct {
	v2.UnimplementedCredentialManagerServiceServer
	issueCalls atomic.Int64
	typedCalls atomic.Int64
}

func (c *countingTypedExecutor) IssueCredential(context.Context, *v2.IssueCredentialRequest) (*v2.IssueCredentialResponse, error) {
	c.issueCalls.Add(1)
	return v2.IssueCredentialResponse_builder{RequestId: "legacy"}.Build(), nil
}

func (c *countingTypedExecutor) IssueCredentialV2(context.Context, *v2.IssueCredentialRequest) (*v2.IssueCredentialResponse, error) {
	c.typedCalls.Add(1)
	return v2.IssueCredentialResponse_builder{RequestId: "typed"}.Build(), nil
}

// oldExecutorServiceDesc is the service descriptor an executor that predates the
// typed contract actually registers: the same service, without the
// IssueCredentialV2 method at all. That is stronger than relying on the
// generated Unimplemented default, which only proves what a default does when
// the method is registered -- an old binary never registers it.
func oldExecutorServiceDesc() *grpc.ServiceDesc {
	desc := v2.CredentialManagerService_ServiceDesc
	methods := make([]grpc.MethodDesc, 0, len(desc.Methods))
	for _, method := range desc.Methods {
		if method.MethodName == "IssueCredentialV2" {
			continue
		}
		methods = append(methods, method)
	}
	return &grpc.ServiceDesc{
		ServiceName: desc.ServiceName,
		HandlerType: desc.HandlerType,
		Methods:     methods,
		Streams:     desc.Streams,
		Metadata:    desc.Metadata,
	}
}

// startExecutor serves impl over a real gRPC transport. When old is true the
// typed method is absent from the registered descriptor, exactly as it is in a
// binary built before the contract existed.
func startExecutor(t *testing.T, impl v2.CredentialManagerServiceServer, old bool) v2.CredentialManagerServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	if old {
		server.RegisterService(oldExecutorServiceDesc(), impl)
	} else {
		v2.RegisterCredentialManagerServiceServer(server, impl)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return v2.NewCredentialManagerServiceClient(conn)
}

func typedTransportRequest() *v2.IssueCredentialRequest {
	return v2.IssueCredentialRequest_builder{
		IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
		CredentialOptions: apiKeyOptions(serviceAccountKey),
		RequestId:         "request-1",
		OutputContentType: typedAPIKeyV2,
	}.Build()
}

// TestTypedIssuanceRefusesOnAnOldExecutor is the downgrade regression: the call
// reaches an executor that does not register the typed method, and the provider
// is never touched.
//
// It exercises the real transport rather than the builder's own guard, because
// the guard is what a conforming implementation does and this is about what an
// old one does when it is asked anyway. The assertion that matters is the
// legacy counter: the typed call must not be serviced by the legacy entrypoint
// under any error, including Unimplemented.
func TestTypedIssuanceRefusesOnAnOldExecutor(t *testing.T) {
	ctx := context.Background()

	t.Run("an old executor refuses the typed call and never runs legacy Issue", func(t *testing.T) {
		impl := &countingTypedExecutor{}
		client := startExecutor(t, impl, true)

		_, err := client.IssueCredentialV2(ctx, typedTransportRequest())
		require.Equal(t, codes.Unimplemented, status.Code(err),
			"an executor without the method must answer Unimplemented")
		require.Zero(t, impl.issueCalls.Load(),
			"the typed call must not be serviced by the legacy Issue entrypoint")
		require.Zero(t, impl.typedCalls.Load())
	})

	t.Run("the legacy route on the same old executor still works", func(t *testing.T) {
		impl := &countingTypedExecutor{}
		client := startExecutor(t, impl, true)

		resp, err := client.IssueCredential(ctx, v2.IssueCredentialRequest_builder{
			IdentityId:        v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build(),
			CredentialOptions: apiKeyOptions(serviceAccountKey),
			RequestId:         "request-1",
		}.Build())
		require.NoError(t, err)
		require.Equal(t, "legacy", resp.GetRequestId())
		require.EqualValues(t, 1, impl.issueCalls.Load())
	})

	t.Run("an executor that registers the method serves the typed call", func(t *testing.T) {
		impl := &countingTypedExecutor{}
		client := startExecutor(t, impl, false)

		resp, err := client.IssueCredentialV2(ctx, typedTransportRequest())
		require.NoError(t, err)
		require.Equal(t, "typed", resp.GetRequestId())
		require.EqualValues(t, 1, impl.typedCalls.Load())
		require.Zero(t, impl.issueCalls.Load(),
			"a typed call must never be serviced by the legacy entrypoint")
	})
}
