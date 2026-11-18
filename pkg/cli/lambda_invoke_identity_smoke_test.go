//go:build baton_lambda_support

package cli

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	lambdaGrpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestInvokeIdentityHTTPRejectionSmoke(t *testing.T) {
	for _, stage := range []string{"decode", "missing", "host", "token", "build"} {
		t.Run(stage, func(t *testing.T) {
			key := identityTestKey(t)
			core, records := observer.New(zap.DebugLevel)
			processCtx := ctxzap.ToContext(context.Background(), zap.New(core))
			validations, factories, closes := 0, 0, 0
			h, err := newLambdaInvokeIdentityHandler(processCtx, ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
				factories++
				return &lambdaIdentityRuntime{ctx: ctx, close: func() { closes++ }, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
					return nil, errors.New("marker-secret")
				}}, nil
			})
			require.NoError(t, err)
			defer h.Close()
			identityTestValidator(t, h, key, &validations)
			var logTail string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				payload, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				if stage == "decode" {
					var object map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(payload, &object))
					delete(object, "v")
					delete(object, "frame")
					object["req"] = json.RawMessage(`{"@type":"marker-secret"}`)
					payload, err = json.Marshal(object)
					require.NoError(t, err)
				}
				var request lambdaGrpc.Request
				if err := json.Unmarshal(payload, &request); err != nil {
					logTail = "request decode failed: " + err.Error()
					w.Header().Set("X-Amz-Function-Error", "Unhandled")
					w.Header().Set("X-Amz-Log-Result", base64.StdEncoding.EncodeToString([]byte(logTail)))
					failure, encodeErr := json.Marshal(map[string]string{"errorType": "DecodeError", "errorMessage": err.Error()})
					require.NoError(t, encodeErr)
					_, _ = w.Write(failure)
					return
				}
				invokeCtx := identityTestContext(processCtx)
				response, err := h.Handler(invokeCtx, &request)
				require.NoError(t, err)
				encoded, err := json.Marshal(response)
				require.NoError(t, err)
				_, _ = w.Write(encoded)
			}))
			defer server.Close()
			client := lambda.NewFromConfig(aws.Config{Region: "us-west-2", Credentials: aws.AnonymousCredentials{}}, func(opts *lambda.Options) { opts.BaseEndpoint = aws.String(server.URL) })
			identity := lambdaGrpc.InvokeIdentity{ExecutionScope: identityTestScope, ClientID: identityTestClient, ClientSecret: "marker-secret"}
			if stage == "host" {
				identity.ClientID = "id@not-allowed.example.net/use"
			}
			options := []lambdaGrpc.ClientTransportOption{}
			if stage != "missing" {
				options = append(options, lambdaGrpc.WithInvokeIdentity(identity))
			}
			transport, err := lambdaGrpc.NewLambdaClientTransport(processCtx, client, "test-function", options...)
			require.NoError(t, err)
			token := identityTestToken(t, key, "tenant.example.com", nil)
			if stage == "token" {
				token = "marker-secret"
			}
			invokeCtx := metadata.NewOutgoingContext(processCtx, metadata.Pairs("authorization", "Bearer "+token, lambdaConnectorConfigVersionHeader, "v1"))
			_, err = v2.NewConnectorServiceClient(lambdaGrpc.NewClientConn(transport)).Validate(invokeCtx, &v2.ConnectorServiceValidateRequest{})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "marker-secret")
			require.NotContains(t, logTail, "marker-secret")
			require.NotContains(t, fmt.Sprint(records.All()), "marker-secret")
			if stage == "decode" {
				var failure *lambdaGrpc.LambdaInvokeFailure
				require.ErrorAs(t, err, &failure)
				require.NotEmpty(t, failure.LogSummary)
				require.NotContains(t, failure.LogSummary, "marker-secret")
				require.Zero(t, validations)
			} else if stage == "build" {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Equal(t, 1, factories)
				require.Equal(t, 1, closes)
			} else {
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
				require.Zero(t, factories)
				if stage != "token" {
					require.Zero(t, validations)
				}
			}
			require.Nil(t, h.runtime)
			require.Nil(t, h.reloader)
		})
	}
}
