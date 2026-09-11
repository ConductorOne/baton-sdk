package grpc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

type staticClientTransport struct {
	response *Response
	err      error
}

func (t *staticClientTransport) RoundTrip(context.Context, *Request) (*Response, error) {
	return t.response, t.err
}

func testResponse(t *testing.T, code codes.Code, headers, trailers metadata.MD) *Response {
	t.Helper()

	response, err := anypb.New(&structpb.Struct{})
	require.NoError(t, err)
	responseStatus, err := anypb.New(status.New(code, "response status").Proto())
	require.NoError(t, err)
	responseHeaders, err := MarshalMetadata(headers)
	require.NoError(t, err)
	responseTrailers, err := MarshalMetadata(trailers)
	require.NoError(t, err)

	return &Response{
		msg: pbtransport.Response_builder{
			Resp:     response,
			Status:   responseStatus,
			Headers:  responseHeaders,
			Trailers: responseTrailers,
		}.Build(),
	}
}

func TestLambdaClientConnPropagatesInvokeRequestIDAndResponseMetadata(t *testing.T) {
	t.Parallel()

	transportResponse := testResponse(t, codes.OK, metadata.Pairs("x-service-header", "server-value"), metadata.Pairs("x-service-trailer", "trailer-value"))
	payload, err := json.Marshal(transportResponse)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Amzn-Requestid", "invoke-request-id")
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	lambdaClient := lambda.NewFromConfig(aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(server.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("access-key", "secret-key", ""),
	})
	transport, err := NewLambdaClientTransport(context.Background(), lambdaClient, "test-function")
	require.NoError(t, err)

	var headers metadata.MD
	var trailers metadata.MD
	err = NewClientConn(transport).Invoke(
		context.Background(),
		"/test.Service/Method",
		&structpb.Struct{},
		&structpb.Struct{},
		grpc.Header(&headers),
		grpc.Trailer(&trailers),
	)
	require.NoError(t, err)
	require.Equal(t, []string{"invoke-request-id"}, headers.Get(lambdaInvokeRequestIDMetadataKey))
	require.Equal(t, []string{"server-value"}, headers.Get("x-service-header"))
	require.Equal(t, []string{"trailer-value"}, trailers.Get("x-service-trailer"))
}

func TestClientConnReturnsResponseMetadataWithStatusError(t *testing.T) {
	t.Parallel()

	transport := &staticClientTransport{
		response: testResponse(t, codes.PermissionDenied, metadata.Pairs("x-service-header", "server-value"), metadata.Pairs("x-service-trailer", "trailer-value")),
	}
	var headers metadata.MD
	var trailers metadata.MD

	err := NewClientConn(transport).Invoke(
		context.Background(),
		"/test.Service/Method",
		&structpb.Struct{},
		&structpb.Struct{},
		grpc.Header(&headers),
		grpc.Trailer(&trailers),
	)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, []string{"server-value"}, headers.Get("x-service-header"))
	require.Equal(t, []string{"trailer-value"}, trailers.Get("x-service-trailer"))
}
