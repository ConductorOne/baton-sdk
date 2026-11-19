//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/field"
	lambdaGrpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/go-jose/go-jose/v4"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const identityTestScope = "execution-scope"
const identityTestClient = "client@tenant.example.com/connector"

func identityTestKey(t *testing.T) *jose.JSONWebKey {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &jose.JSONWebKey{Key: private, KeyID: "test-key", Algorithm: string(jose.EdDSA)}
}

func identityTestToken(t *testing.T, key *jose.JSONWebKey, host string, changes map[string]any) string {
	t.Helper()
	claims := map[string]any{"iss": "https://" + host, "sub": identityTestScope, "aud": identityTestScope,
		"exp": time.Now().Add(time.Minute).Unix(), "nbf": time.Now().Add(-time.Minute).Unix(), "iat": time.Now().Unix()}
	for k, v := range changes {
		claims[k] = v
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: key}, (&jose.SignerOptions{}).WithType("JWT"))
	require.NoError(t, err)
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	object, err := signer.Sign(payload)
	require.NoError(t, err)
	token, err := object.CompactSerialize()
	require.NoError(t, err)
	return token
}

func identityTestRequest(t *testing.T, clientID, secret, token, version string) *lambdaGrpc.Request {
	t.Helper()
	request, err := anypb.New(&v2.ConnectorServiceValidateRequest{})
	require.NoError(t, err)
	headers, err := lambdaGrpc.MarshalMetadata(metadata.Pairs("authorization", "Bearer "+token, lambdaConnectorConfigVersionHeader, version))
	require.NoError(t, err)
	message := pbtransport.Request_builder{Method: "/c1.connector.v2.ConnectorService/Validate", Req: request, Headers: headers}.Build()
	if clientID != "" {
		message.SetInvokeIdentity(pbtransport.InvokeIdentity_builder{ClientId: clientID, ClientSecret: secret}.Build())
	}
	payload, err := protojson.Marshal(message)
	require.NoError(t, err)
	result := &lambdaGrpc.Request{}
	require.NoError(t, json.Unmarshal(payload, result))
	return result
}

func identityTestContext(ctx context.Context) context.Context {
	return lambdacontext.NewContext(ctx, &lambdacontext.LambdaContext{TenantID: identityTestScope})
}

func identityTestCode(t *testing.T, h *lambdaInvokeIdentityHandler, ctx context.Context, req *lambdaGrpc.Request) codes.Code {
	t.Helper()
	response, err := h.Handler(ctx, req)
	require.NoError(t, err)
	st, err := response.Status()
	require.NoError(t, err)
	require.NotContains(t, st.Message(), "marker-secret")
	return st.Code()
}

func identityTestValidator(t *testing.T, h *lambdaInvokeIdentityHandler, key *jose.JSONWebKey, calls *int) {
	t.Helper()
	publicKey := key.Public()
	public, err := publicKey.MarshalJSON()
	require.NoError(t, err)
	h.newValidator = func(ctx context.Context, host, scope string) (*auth.Validator, error) {
		*calls++
		return auth.NewValidator(ctx, auth.Config{PublicKeyJWK: string(public), Issuer: "https://" + host, Subject: scope, Audience: scope})
	}
}

type identityTestConnector struct {
	testLambdaConnectorServer
	call func(context.Context) error
}

func (c *identityTestConnector) Validate(ctx context.Context, _ *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	if c.call != nil {
		if err := c.call(ctx); err != nil {
			return nil, err
		}
	}
	return &v2.ConnectorServiceValidateResponse{}, nil
}

func TestInvokeIdentityHostRejectionsMakeNoOutboundRequests(t *testing.T) {
	invalid := []string{"", "missing-delimiters", "@tenant.example.com/use", "id@tenant.example.com/", "id@tenant.example.com/extra/use"}
	for _, host := range []string{
		strings.Repeat("a", 64) + ".example.com", "-tenant.example.com", "tenant-.example.com",
		strings.Repeat("a.", 125) + "example.com", "tenant.example.com.", "tenant.other.com", "tenant.example.com.evil.test",
		"tenant.badexample.com", "extra.tenant.example.com", "example.com", "Tenant.example.com", "tenant.example.com:443",
		"127.0.0.1", "[::1]", "tenant%2eexample.com", "tenant.example.com?x", "tenant.example.com#x", "tenant..example.com",
		"tenant_example.com", "user@tenant.example.com", "https://tenant.example.com",
	} {
		invalid = append(invalid, "id@"+host+"/use")
	}
	for _, id := range invalid {
		t.Run(id, func(t *testing.T) {
			outbound, factories := 0, 0
			h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error) {
				factories++
				return nil, errors.New("marker-secret")
			})
			require.NoError(t, err)
			h.newValidator = func(context.Context, string, string) (*auth.Validator, error) {
				outbound++
				return nil, errors.New("marker-secret")
			}
			require.Equal(t, codes.FailedPrecondition, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, id, "marker-secret", "bad-token", "v2")))
			require.Zero(t, outbound)
			require.Zero(t, factories)
			require.Nil(t, h.runtime)
		})
	}
	for _, host := range []string{"a.example.com", strings.Repeat("a", 63) + ".example.com", "a--route--dev.example.com"} {
		suffix := ".example.com"
		if strings.Contains(host, "--route") {
			suffix = "--route--dev.example.com"
		}
		got, err := invokeIdentityHost("id@"+host+"/use", suffix)
		require.NoError(t, err)
		require.Equal(t, host, got)
	}
}

func TestInvokeIdentityJWTBeforeBuildAndRejectedFirstHostDoesNotBind(t *testing.T) {
	key := identityTestKey(t)
	for _, changes := range []map[string]any{{"iss": "https://wrong.example.com"}, {"sub": "wrong"}, {"aud": "wrong"},
		{"exp": time.Now().Add(-time.Hour).Unix()}, {"nbf": time.Now().Add(time.Hour).Unix()}} {
		factories, validations := 0, 0
		h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error) {
			factories++
			return nil, errors.New("marker-secret")
		})
		require.NoError(t, err)
		identityTestValidator(t, h, key, &validations)
		token := identityTestToken(t, key, "tenant.example.com", changes)
		require.Equal(t, codes.Unauthenticated, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v2")))
		require.Equal(t, 1, validations)
		require.Zero(t, factories)
		require.Nil(t, h.runtime)
	}
	factories, validations, builds := 0, 0, 0
	clientKey := identityTestKey(t)
	h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
		factories++
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, clientKey)
		return &lambdaIdentityRuntime{ctx: ctx, close: func() {}, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			builds++
			return &lambdaConnectorGeneration{connector: &identityTestConnector{}, version: "v2"}, nil
		}}, nil
	})
	require.NoError(t, err)
	defer h.Close()
	identityTestValidator(t, h, key, &validations)
	badSignature := identityTestToken(t, identityTestKey(t), "tenant.example.com", nil)
	require.Equal(t, codes.Unauthenticated, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", badSignature, "v2")))
	require.Nil(t, h.runtime)
	require.Zero(t, factories)
	goodToken := identityTestToken(t, key, "other.example.com", nil)
	require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, "id@other.example.com/use", "marker-secret", goodToken, "v2")))
	require.Equal(t, 1, factories)
	require.Equal(t, 1, builds)
	require.Equal(t, 2, validations)
	require.Equal(t, "id@other.example.com/use", h.clientID)
}

func TestInvokeIdentityBindReloadContextKeyAndLogs(t *testing.T) {
	core, records := observer.New(zap.DebugLevel)
	processCtx := ctxzap.ToContext(context.Background(), zap.New(core))
	key := identityTestKey(t)
	clientKey := identityTestKey(t)
	builds, factories, validations, calls := 0, 0, 0, 0
	var constructionCtx context.Context
	var buildErr error
	buildPanics := false
	previousLogger := zap.L()
	defer func() { zap.ReplaceGlobals(previousLogger) }()
	h, err := newLambdaInvokeIdentityHandler(processCtx, ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
		factories++
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, clientKey)
		return &lambdaIdentityRuntime{ctx: ctx, close: func() {}, build: func(_ context.Context, version string) (*lambdaConnectorGeneration, error) {
			builds++
			if buildPanics {
				panic("marker-secret")
			}
			if buildErr != nil {
				return nil, buildErr
			}
			constructionCtx = ctx
			return &lambdaConnectorGeneration{version: version, connector: &identityTestConnector{call: func(ctx context.Context) error {
				calls++
				require.Same(t, clientKey, ctx.Value(crypto.ContextClientSecretKey))
				md, ok := metadata.FromIncomingContext(ctx)
				require.True(t, ok)
				require.NotContains(t, fmt.Sprint(md), "marker-secret")
				ctxzap.Extract(ctx).Info("identity call")
				return nil
			}}}, nil
		}}, nil
	})
	require.NoError(t, err)
	defer h.Close()
	identityTestValidator(t, h, key, &validations)
	token := identityTestToken(t, key, "tenant.example.com", nil)
	invokeCtx, endInvoke := context.WithCancel(identityTestContext(context.Background()))
	req := identityTestRequest(t, identityTestClient, "marker-secret", token, "v1")
	require.Equal(t, codes.OK, identityTestCode(t, h, invokeCtx, req))
	endInvoke()
	require.NoError(t, constructionCtx.Err())
	require.Nil(t, processCtx.Value(crypto.ContextClientSecretKey))
	require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), req))
	require.Equal(t, codes.FailedPrecondition, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, "other@tenant.example.com/use", "marker-secret", token, "v2")))
	require.Equal(t, 1, builds)
	h.reloader.lastRebuildAt = time.Now().Add(-time.Minute)
	require.Equal(t, codes.Unauthenticated, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", "bad-token", "v2")))
	require.Equal(t, 1, builds)
	require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v2")))
	require.Equal(t, 2, builds)
	h.reloader.lastRebuildAt = time.Now().Add(-time.Minute)
	buildErr = errLambdaExecutionScope
	require.Zero(t, records.FilterLevelExact(zap.ErrorLevel).Len(), "successful and rejected invokes log no reload failure")
	require.Equal(t, codes.FailedPrecondition, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v3")))
	require.Equal(t, "v2", h.reloader.current.version)
	scopeLogs := records.FilterMessage("lambda-run: execution scope mismatch on reload; serving previous generation").All()
	require.Len(t, scopeLogs, 1)
	require.Equal(t, map[string]any{
		"execution_scope": identityTestScope, "client_id": identityTestClient, "requested_version": "v3", "current_version": "v2", "stage": "scope",
	}, scopeLogs[0].ContextMap())
	buildErr = &lambdaBuildStageError{stage: "fetch", err: status.Error(codes.PermissionDenied, "marker-secret")}
	require.Equal(t, codes.Unavailable, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v3")))
	reloadLogs := records.FilterMessage("lambda-run: failed to reload connector config; serving previous generation").All()
	require.Len(t, reloadLogs, 1)
	require.Equal(t, map[string]any{
		"execution_scope": identityTestScope, "client_id": identityTestClient, "requested_version": "v3", "current_version": "v2",
		"cause_code": "PermissionDenied", "stage": "fetch",
	}, reloadLogs[0].ContextMap(), "error text is withheld")
	buildErr, buildPanics = nil, true
	require.Equal(t, codes.Unavailable, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v3")))
	panicLogs := records.FilterMessage("lambda-run: connector generation panic").All()
	require.Len(t, panicLogs, 1)
	panicFields := panicLogs[0].ContextMap()
	require.NotEmpty(t, panicFields["stack"])
	delete(panicFields, "stack")
	require.Equal(t, map[string]any{
		"execution_scope": identityTestScope, "client_id": identityTestClient,
		"recovery_site": "lambda_handler", "error": "rpc error: code = Internal desc = Internal Server Error",
	}, panicFields, "the panic value is not attached")
	require.Equal(t, 3, records.FilterLevelExact(zap.ErrorLevel).Len())
	buildPanics = false
	require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v2")))
	require.Equal(t, 1, factories)
	require.Equal(t, 1, validations)
	require.Equal(t, 4, calls)
	for _, entry := range records.FilterMessage("identity call").All() {
		require.Equal(t, identityTestScope, entry.ContextMap()["execution_scope"])
		require.Equal(t, identityTestClient, entry.ContextMap()["client_id"])
	}
	require.NotContains(t, fmt.Sprint(records.All()), "marker-secret")
}

func TestInvokeIdentityFailedBuildCleansUpAndCanRetry(t *testing.T) {
	for _, buildFailure := range []error{errors.New("marker-secret"), errLambdaExecutionScope, status.Error(codes.Unauthenticated, "marker-secret")} {
		core, records := observer.New(zap.DebugLevel)
		processCtx := ctxzap.ToContext(context.Background(), zap.New(core))
		key := identityTestKey(t)
		closes, builds, validations := 0, 0, 0
		h, err := newLambdaInvokeIdentityHandler(processCtx, ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
			ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
			return &lambdaIdentityRuntime{ctx: ctx, close: func() { closes++ }, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
				builds++
				if builds == 1 {
					return nil, buildFailure
				}
				return &lambdaConnectorGeneration{version: "v1", connector: &identityTestConnector{}}, nil
			}}, nil
		})
		require.NoError(t, err)
		identityTestValidator(t, h, key, &validations)
		token := identityTestToken(t, key, "tenant.example.com", nil)
		req := identityTestRequest(t, identityTestClient, "marker-secret", token, "v1")
		want := codes.Unavailable
		wantLog := "lambda-run: failed to build connector"
		wantFields := map[string]any{
			"execution_scope": identityTestScope, "client_id": identityTestClient, "requested_version": "v1",
			"cause_code": status.Code(buildFailure).String(), "stage": lambdaBuildErrorStage(buildFailure),
		}
		if errors.Is(buildFailure, errLambdaExecutionScope) {
			want = codes.FailedPrecondition
			wantLog = "lambda-run: execution scope mismatch on first build"
			delete(wantFields, "cause_code")
		}
		require.Equal(t, want, identityTestCode(t, h, identityTestContext(context.Background()), req))
		require.Nil(t, h.runtime)
		require.Nil(t, h.reloader)
		require.Equal(t, 1, closes)
		failureLogs := records.FilterLevelExact(zap.ErrorLevel).All()
		require.Len(t, failureLogs, 1)
		require.Equal(t, wantLog, failureLogs[0].Message)
		require.Equal(t, wantFields, failureLogs[0].ContextMap())
		require.NotContains(t, fmt.Sprint(records.All()), "marker-secret")
		require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), req))
		require.Equal(t, 2, validations)
		h.Close()
		require.Equal(t, 2, closes)
	}
}

type identityConfigConn struct {
	response *v1.GetConnectorConfigResponse
	calls    int
}

func (c *identityConfigConn) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	c.calls++
	proto.Merge(reply.(proto.Message), c.response)
	return nil
}
func (*identityConfigConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected stream")
}

func TestInvokeIdentityScopeCheckedBeforeConstructionEveryBuild(t *testing.T) {
	key := identityTestKey(t)
	pub := key.Key.(ed25519.PrivateKey).Public().(ed25519.PublicKey)
	encrypted, err := jwk.EncryptED25519(pub, []byte(`{}`))
	require.NoError(t, err)
	for _, scope := range []string{"", "wrong", identityTestScope} {
		conn := &identityConfigConn{response: v1.GetConnectorConfigResponse_builder{Config: encrypted}.Build()}
		if scope != "" {
			conn.response.SetExpectedExecutionScope(scope)
		}
		constructed := 0
		build := newLambdaGenerationBuilder(context.Background(), viper.New(), field.Configuration{}, func(ctx context.Context, cfg *viper.Viper, opts RunTimeOpts) (types.ConnectorServer, error) {
			constructed++
			return &identityTestConnector{}, nil
		}, false, conn, key, true, identityTestScope)
		for _, version := range []string{"v1", "v2"} {
			_, err := build(context.Background(), version)
			if scope == identityTestScope {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errLambdaExecutionScope)
			}
		}
		require.Equal(t, 2, conn.calls)
		if scope == identityTestScope {
			require.Equal(t, 2, constructed)
		} else {
			require.Zero(t, constructed)
		}
	}
	conn := &identityConfigConn{response: v1.GetConnectorConfigResponse_builder{Config: encrypted, ExpectedExecutionScope: proto.String("wrong")}.Build()}
	build := newLambdaGenerationBuilder(context.Background(), viper.New(), field.Configuration{}, func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
		return &identityTestConnector{}, nil
	}, false, conn, key, false, "")
	_, err = build(context.Background(), "")
	require.NoError(t, err, "legacy mode ignores expected execution scope")
}

func TestLambdaGenerationBuilderRejectsInvalidConfigurationKeys(t *testing.T) {
	publicKey := identityTestKey(t).Public()
	for name, key := range map[string]*jose.JSONWebKey{
		"missing":    nil,
		"wrong type": {Key: "marker-secret"},
		"public key": &publicKey,
	} {
		for _, scope := range []string{"", identityTestScope} {
			t.Run(name+"/"+scope, func(t *testing.T) {
				conn := &identityConfigConn{response: &v1.GetConnectorConfigResponse{}}
				constructed := 0
				var build func(context.Context, string) (*lambdaConnectorGeneration, error)
				require.NotPanics(t, func() {
					build = newLambdaGenerationBuilder(context.Background(), viper.New(), field.Configuration{},
						func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
							constructed++
							return &identityTestConnector{}, nil
						}, false, conn, key, scope != "", scope)
				})
				generation, err := build(context.Background(), "v1")
				require.Error(t, err)
				require.NotContains(t, err.Error(), "marker-secret")
				require.Nil(t, generation)
				require.Zero(t, conn.calls)
				require.Zero(t, constructed)
			})
		}
	}
}

func TestIdentityGenerationBudgetAndPersistentLifetime(t *testing.T) {
	var buildCtx context.Context
	_, err := buildIdentityGeneration(context.Background(), context.Background(), time.Millisecond, func(ctx context.Context, _ string) (*lambdaConnectorGeneration, error) {
		buildCtx = ctx
		<-ctx.Done()
		return nil, ctx.Err()
	}, "v1")
	require.Error(t, err)
	require.ErrorIs(t, buildCtx.Err(), context.Canceled)
	generation, err := buildIdentityGeneration(context.Background(), context.Background(), time.Second, func(ctx context.Context, _ string) (*lambdaConnectorGeneration, error) {
		buildCtx = ctx
		return &lambdaConnectorGeneration{connector: &identityTestConnector{}}, nil
	}, "v1")
	require.NoError(t, err)
	require.NoError(t, buildCtx.Err())
	closeIdentityGeneration(context.Background(), generation)
	require.ErrorIs(t, buildCtx.Err(), context.Canceled)
}

func TestInvokeIdentityJWKSRedirectRefused(t *testing.T) {
	redirected := 0
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected++ }))
	defer target.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer server.Close()
	client, err := lambdaIdentityJWKSClient()
	require.NoError(t, err)
	client.Transport = server.Client().Transport
	_, err = auth.NewValidator(context.Background(), auth.Config{
		Issuer: "https://tenant.example.com", Subject: identityTestScope, Audience: identityTestScope,
		JWKSUrl: server.URL, JWKSClient: client,
	})
	require.Error(t, err)
	require.Zero(t, redirected)
}

func TestInvokeIdentityModeRequiresSuffixButNotLegacyCredentials(t *testing.T) {
	v := viper.New()
	v.Set(field.LambdaServerPerInvokeIdentityField.GetName(), true)
	command := &cobra.Command{Use: "test"}
	err := OptionallyAddLambdaCommand(context.Background(), "test", v, func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
		t.Fatal("must not construct")
		return nil, nil
	}, field.Configuration{}, command, false)
	require.NoError(t, err)
	command.SetArgs([]string{"lambda"})
	err = command.Execute()
	require.ErrorContains(t, err, "lambda-allowed-host-suffix")
	require.NotContains(t, err.Error(), "lambda-client-id")
}

func TestInvokeIdentitySuffixRequiresLeadingDelimiter(t *testing.T) {
	factory := func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error) {
		t.Error("must not construct")
		return nil, errors.New("unexpected construction")
	}
	for _, suffix := range []string{
		"example.com", "com", "*.example.com", " .example.com", ".com", ".", "-", ".Example.com",
		".example.com.", ".example..com", "-.example.com", ".example.com:443", "--route--dev.com",
	} {
		t.Run("rejected/"+suffix, func(t *testing.T) {
			h, err := newLambdaInvokeIdentityHandler(context.Background(), suffix, factory)
			require.ErrorContains(t, err, "lambda-allowed-host-suffix must")
			require.Nil(t, h)
		})
	}
	for _, suffix := range []string{".example.com", "--route--dev.example.com"} {
		t.Run("accepted/"+suffix, func(t *testing.T) {
			_, err := newLambdaInvokeIdentityHandler(context.Background(), suffix, factory)
			require.NoError(t, err)
		})
	}
}

func TestInvokeIdentityConcurrentFirstInvokeBindsOnce(t *testing.T) {
	key := identityTestKey(t)
	factories, validations := 0, 0
	h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
		factories++
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
		return &lambdaIdentityRuntime{ctx: ctx, close: func() {}, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			return &lambdaConnectorGeneration{connector: &identityTestConnector{}, version: "v1"}, nil
		}}, nil
	})
	require.NoError(t, err)
	defer h.Close()
	identityTestValidator(t, h, key, &validations)
	token := identityTestToken(t, key, "tenant.example.com", nil)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, "marker-secret", token, "v1")))
		})
	}
	wg.Wait()
	require.Equal(t, 1, factories)
	require.Equal(t, 1, validations)
}

func TestIdentityRuntimeKeyDecryptsDispatchedCredentials(t *testing.T) {
	t.Setenv("BATON_LAMBDA_TOKEN_HOST", "%invalid-token-host")
	t.Setenv("BATON_LAMBDA_CONFIGURATION_HOST", "%invalid-config-host")
	provider := &jwk.JWKEncryptionProvider{}
	encryptionConfig, clientKey, err := provider.GenerateKey(context.Background())
	require.NoError(t, err)
	data, err := clientKey.MarshalJSON()
	require.NoError(t, err)
	secret := "client:secret:v1:" + base64.RawURLEncoding.EncodeToString(data)
	password, err := provider.Encrypt(context.Background(), encryptionConfig, v2.PlaintextData_builder{Bytes: []byte("test-password")}.Build())
	require.NoError(t, err)
	opts := v2.CredentialOptions_builder{EncryptedPassword: v2.CredentialOptions_EncryptedPassword_builder{EncryptedPasswords: []*v2.EncryptedData{password}}.Build()}.Build()
	realFactory := lambdaIdentityRuntimeFactory(viper.New(), field.Configuration{}, func(context.Context, *viper.Viper, RunTimeOpts) (types.ConnectorServer, error) {
		return nil, errors.New("unexpected config fetch")
	}, false)
	signer := identityTestKey(t)
	validations := 0
	h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", func(ctx context.Context, id *pbtransport.InvokeIdentity, scope string) (*lambdaIdentityRuntime, error) {
		runtime, err := realFactory(ctx, id, scope)
		if err != nil {
			return nil, err
		}
		require.NotNil(t, runtime.ctx.Value(crypto.ContextClientSecretKey))
		runtime.build = func(_ context.Context, version string) (*lambdaConnectorGeneration, error) {
			require.NotNil(t, runtime.ctx.Value(crypto.ContextClientSecretKey))
			return &lambdaConnectorGeneration{version: version, connector: &identityTestConnector{call: func(ctx context.Context) error {
				key, ok := ctx.Value(crypto.ContextClientSecretKey).(*jose.JSONWebKey)
				require.True(t, ok)
				local, err := crypto.ConvertCredentialOptions(ctx, key, opts, nil)
				require.NoError(t, err)
				require.Equal(t, "test-password", local.GetPlaintextPassword().GetPlaintextPassword())
				return nil
			}}}, nil
		}
		return runtime, nil
	})
	require.NoError(t, err)
	defer h.Close()
	identityTestValidator(t, h, signer, &validations)
	token := identityTestToken(t, signer, "tenant.example.com", nil)
	require.Equal(t, codes.OK, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, secret, token, "v1")))
}

func TestInvokeIdentityMissingPlatformScopeAndPanicSanitization(t *testing.T) {
	core, records := observer.New(zap.DebugLevel)
	processCtx := ctxzap.ToContext(context.Background(), zap.New(core))
	key := identityTestKey(t)
	validations, factories, closes := 0, 0, 0
	h, err := newLambdaInvokeIdentityHandler(processCtx, ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
		factories++
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
		return &lambdaIdentityRuntime{ctx: ctx, close: func() { closes++ }, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			panic("marker-secret")
		}}, nil
	})
	require.NoError(t, err)
	identityTestValidator(t, h, key, &validations)
	token := identityTestToken(t, key, "tenant.example.com", nil)
	req := identityTestRequest(t, identityTestClient, "marker-secret", token, "v1")
	require.Equal(t, codes.FailedPrecondition, identityTestCode(t, h, context.Background(), req))
	require.Zero(t, validations)
	require.Zero(t, factories)
	require.Zero(t, records.FilterMessage("lambda-run: invoke identity handler panic").Len(), "a rejected invoke is not a panic")
	require.Equal(t, codes.Unavailable, identityTestCode(t, h, identityTestContext(context.Background()), req))
	require.Equal(t, 1, factories)
	require.Equal(t, 1, closes)
	require.Nil(t, h.runtime)
	require.Nil(t, h.reloader)
	require.NotContains(t, fmt.Sprint(records.All()), "marker-secret")
	panicLogs := records.FilterMessage("lambda-run: invoke identity handler panic").All()
	require.Len(t, panicLogs, 1, "a recovered panic must leave a fixed-text log record")
	require.NotEmpty(t, panicLogs[0].ContextMap()["stack"])
	require.NotContains(t, panicLogs[0].ContextMap(), "method")
	require.Equal(t, "lambda_handler", panicLogs[0].ContextMap()["recovery_site"])
	require.NotContains(t, panicLogs[0].ContextMap(), "panic")
	_, err = lambdaIdentityRecoveryInterceptor()(processCtx, nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) { panic("marker-secret") })
	require.Equal(t, codes.Internal, status.Code(err))
	connectorPanicLogs := records.FilterMessage("lambda-run: connector handler panic").All()
	require.Len(t, connectorPanicLogs, 1)
	require.NotEmpty(t, connectorPanicLogs[0].ContextMap()["stack"])
	require.Equal(t, "/test.Service/Call", connectorPanicLogs[0].ContextMap()["method"])
	require.Equal(t, "interceptor", connectorPanicLogs[0].ContextMap()["recovery_site"])
	require.NotContains(t, connectorPanicLogs[0].ContextMap(), "panic")
	require.NotContains(t, err.Error(), "marker-secret")
	require.NotContains(t, fmt.Sprint(records.All()), "marker-secret")
}
