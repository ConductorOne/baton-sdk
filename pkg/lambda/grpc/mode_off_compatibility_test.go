package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type modeOffCompatibilityService struct {
	v1.UnimplementedConnectorConfigServiceServer
	code  codes.Code
	calls atomic.Int32
}

func (s *modeOffCompatibilityService) GetConnectorConfig(_ context.Context, req *v1.GetConnectorConfigRequest) (*v1.GetConnectorConfigResponse, error) {
	s.calls.Add(1)
	if s.code != codes.OK {
		return nil, status.Error(s.code, "mode-off fixture status")
	}
	response := &v1.GetConnectorConfigResponse{}
	response.SetConfigVersion(req.GetRequestedVersion())
	return response, nil
}

func TestModeOffTransportPreservesWireAcceptanceAndErrorClassification(t *testing.T) {
	legacy := protojson.Marshal
	dual := func(msg proto.Message) ([]byte, error) {
		payload, err := protojson.Marshal(msg)
		if err != nil {
			return nil, err
		}
		return spliceWireFrame(payload, msg)
	}
	unsupported := func(msg proto.Message) ([]byte, error) {
		frame, err := proto.Marshal(msg)
		if err != nil {
			return nil, err
		}
		return json.Marshal(wireFrame{V: transportWireVersion + 1, Frame: frame})
	}
	invalidFrame := func(proto.Message) ([]byte, error) {
		return json.Marshal(wireFrame{V: transportWireVersion, Frame: []byte{0}})
	}
	malformedJSON := func(proto.Message) ([]byte, error) { return []byte(`{"method":`), nil }
	for _, tc := range []struct {
		name             string
		requestEncoding  func(proto.Message) ([]byte, error)
		responseEncoding func(proto.Message) ([]byte, error)
		requestAccepted  bool
		responseAccepted bool
	}{
		{name: "legacy", requestEncoding: legacy, requestAccepted: true, responseAccepted: true},
		{name: "dual", requestEncoding: dual, requestAccepted: true, responseAccepted: true},
		{name: "frame-only", requestEncoding: encodeWireFrame, requestAccepted: true, responseAccepted: true},
		{name: "unsupported-request-version", requestEncoding: unsupported},
		{name: "invalid-request-frame", requestEncoding: invalidFrame},
		{name: "malformed-request-json", requestEncoding: malformedJSON},
		{name: "unsupported-response-version", requestEncoding: dual, responseEncoding: unsupported, requestAccepted: true},
		{name: "invalid-response-frame", requestEncoding: dual, responseEncoding: invalidFrame, requestAccepted: true},
		{name: "malformed-response-json", requestEncoding: dual, responseEncoding: malformedJSON, requestAccepted: true},
	} {
		codesToTest := []codes.Code{codes.OK}
		if tc.requestAccepted && tc.responseAccepted {
			codesToTest = []codes.Code{codes.OK, codes.Unauthenticated, codes.PermissionDenied, codes.Unavailable}
		}
		for _, applicationCode := range codesToTest {
			t.Run(tc.name+"/"+applicationCode.String(), func(t *testing.T) {
				service := &modeOffCompatibilityService{code: applicationCode}
				modeOffServer := NewServer(nil)
				v1.RegisterConnectorConfigServiceServer(modeOffServer, service)
				endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Empty(t, r.Header.Get("X-Amz-Tenant-Id"))
					incoming, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					original := &Request{}
					require.NoError(t, original.UnmarshalJSON(incoming))
					require.Nil(t, original.InvokeIdentity(), "the client must be on the no-option path")
					payload, err := tc.requestEncoding(original.msg)
					require.NoError(t, err)
					request := &Request{}
					decodeErr := request.UnmarshalJSON(payload)
					assertModeOffDecodeCompatibility(t, payload, &pbtransport.Request{}, decodeErr, tc.requestAccepted)
					if decodeErr != nil {
						w.Header().Set("X-Amz-Function-Error", "Unhandled")
						failure, err := json.Marshal(map[string]string{"errorType": "DecodeError", "errorMessage": decodeErr.Error()})
						require.NoError(t, err)
						_, _ = w.Write(failure)
						return
					}
					require.True(t, proto.Equal(original.msg, request.msg))
					response, err := modeOffServer.Handler(r.Context(), request)
					require.NoError(t, err)
					encoded, err := response.MarshalJSON()
					require.NoError(t, err)
					if tc.responseEncoding != nil {
						encoded, err = tc.responseEncoding(response.msg)
						require.NoError(t, err)
					}
					decodedResponse := &Response{}
					responseErr := decodedResponse.UnmarshalJSON(encoded)
					assertModeOffDecodeCompatibility(t, encoded, &pbtransport.Response{}, responseErr, tc.responseAccepted)
					_, _ = w.Write(encoded)
				}))
				defer endpoint.Close()
				client := lambda.NewFromConfig(aws.Config{Region: "us-west-2", Credentials: aws.AnonymousCredentials{}}, func(opts *lambda.Options) {
					opts.BaseEndpoint = aws.String(endpoint.URL)
				})
				transport, err := NewLambdaClientTransport(t.Context(), client, "mode-off-function")
				require.NoError(t, err)
				response, err := v1.NewConnectorConfigServiceClient(NewClientConn(transport)).GetConnectorConfig(t.Context(), v1.GetConnectorConfigRequest_builder{
					RequestedVersion: "mode-off-version",
				}.Build())
				expectedCode := applicationCode
				if !tc.requestAccepted || !tc.responseAccepted {
					expectedCode = codes.Unknown
				}
				require.Equal(t, expectedCode, status.Code(err))
				if expectedCode == codes.OK {
					require.NoError(t, err)
					require.Equal(t, "mode-off-version", response.GetConfigVersion())
				} else {
					require.Error(t, err)
				}
				if !tc.requestAccepted {
					var failure *LambdaInvokeFailure
					require.ErrorAs(t, err, &failure)
					require.Equal(t, FailureClassUnhandled, failure.FailureClass)
					require.Equal(t, codes.Unknown, failure.Code())
					require.Zero(t, service.calls.Load())
				} else {
					require.EqualValues(t, 1, service.calls.Load())
					if !tc.responseAccepted {
						var failure *LambdaInvokeFailure
						require.False(t, errors.As(err, &failure), "response decode errors remain transport errors, not Lambda runtime failures")
					}
				}
			})
		}
	}
}

func assertModeOffDecodeCompatibility(t *testing.T, payload []byte, original proto.Message, sanitizedErr error, accepted bool) {
	t.Helper()
	_, originalErr := unmarshalTransportJSON(payload, original)
	require.Equal(t, accepted, originalErr == nil, "the unchanged decoder must match the explicit acceptance table")
	require.Equal(t, accepted, sanitizedErr == nil, "redaction must not change acceptance")
	require.Equal(t, status.Code(originalErr), status.Code(sanitizedErr), "redaction must not change error classification")
}
