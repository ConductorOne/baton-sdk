package grpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestInvokeIdentityEncodings(t *testing.T) {
	identity := pbtransport.InvokeIdentity_builder{ClientId: "client@tenant.example.com/connector", ClientSecret: "marker-secret"}.Build()
	req, err := NewRequest("/test.Service/Call", &v1.GetConnectorConfigRequest{}, metadata.Pairs("authorization", "Bearer signed-token"))
	require.NoError(t, err)
	req.msg.SetInvokeIdentity(identity)
	encoders := map[string]func() ([]byte, error){
		"dual":        req.MarshalJSON,
		"frame-only":  func() ([]byte, error) { return encodeWireFrame(req.msg) },
		"legacy-only": func() ([]byte, error) { return protojson.Marshal(req.msg) },
	}
	for name, encode := range encoders {
		t.Run(name, func(t *testing.T) {
			payload, err := encode()
			require.NoError(t, err)
			var decoded Request
			require.NoError(t, json.Unmarshal(payload, &decoded))
			require.True(t, proto.Equal(identity, decoded.InvokeIdentity()))
			md := MetadataForRequest(&decoded)
			require.Equal(t, []string{"Bearer signed-token"}, md.Get("authorization"))
			for key, values := range md {
				require.NotContains(t, key, "identity")
				require.NotContains(t, strings.Join(values, " "), "marker-secret")
			}
		})
	}
}

func TestLambdaTransportInvokeIdentity(t *testing.T) {
	for _, withIdentity := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "identity"}[withIdentity], func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				payload, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				var req Request
				require.NoError(t, json.Unmarshal(payload, &req))
				if withIdentity {
					require.Equal(t, "execution-scope", r.Header.Get("X-Amz-Tenant-Id"))
					require.Equal(t, "client@tenant.example.com/connector", req.InvokeIdentity().GetClientId())
					require.Equal(t, "marker-secret", req.InvokeIdentity().GetClientSecret())
				} else {
					require.Empty(t, r.Header.Get("X-Amz-Tenant-Id"))
					require.Nil(t, req.InvokeIdentity())
				}
				response, err := json.Marshal(ErrorResponse(nil))
				require.NoError(t, err)
				_, _ = w.Write(response)
			}))
			defer server.Close()
			client := lambda.NewFromConfig(aws.Config{Region: "us-west-2", Credentials: aws.AnonymousCredentials{}}, func(o *lambda.Options) { o.BaseEndpoint = aws.String(server.URL) })
			opts := []ClientTransportOption{}
			if withIdentity {
				opts = append(opts, WithInvokeIdentity(InvokeIdentity{ExecutionScope: "execution-scope", ClientID: "client@tenant.example.com/connector", ClientSecret: "marker-secret"}))
			}
			transport, err := NewLambdaClientTransport(context.Background(), client, "test-function", opts...)
			require.NoError(t, err)
			req, err := NewRequest("/test.Service/Call", &v1.GetConnectorConfigRequest{}, nil)
			require.NoError(t, err)
			_, err = transport.RoundTrip(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Nil(t, req.InvokeIdentity(), "transport must not retain credentials on the caller's request")
		})
	}
}

func TestRequestDecodeErrorsDoNotExposePayload(t *testing.T) {
	for _, payload := range []string{
		`{"invokeIdentity":{"clientSecret":"marker-secret"},"method":{ "marker-secret":true}}`,
		`{"v":2,"frame":"bWFya2VyLXNlY3JldA=="}`,
		`{"invokeIdentity":{"clientSecret":"marker-secret"},"req":{"@type":"marker-secret"}}`,
	} {
		var req Request
		err := json.Unmarshal([]byte(payload), &req)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "marker-secret")
	}
}

func TestResponseDecodeErrorsDoNotExposePayload(t *testing.T) {
	var response Response
	err := json.Unmarshal([]byte(`{"resp":{"@type":"marker-secret"}}`), &response)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "marker-secret")
}

func TestExpectedExecutionScopePresenceAndFieldNumbers(t *testing.T) {
	response := &v1.GetConnectorConfigResponse{}
	require.False(t, response.HasExpectedExecutionScope())
	response.SetExpectedExecutionScope("")
	require.True(t, response.HasExpectedExecutionScope())
	payload, err := proto.Marshal(response)
	require.NoError(t, err)
	decoded := &v1.GetConnectorConfigResponse{}
	require.NoError(t, proto.Unmarshal(payload, decoded))
	require.True(t, decoded.HasExpectedExecutionScope())
	require.EqualValues(t, 5, response.ProtoReflect().Descriptor().Fields().ByName("expected_execution_scope").Number())
	require.EqualValues(t, 4, (&pbtransport.Request{}).ProtoReflect().Descriptor().Fields().ByName("invoke_identity").Number())
}
