//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIdentityBindingKeyLifetimeAndRedaction(t *testing.T) {
	core, records := observer.New(zap.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))
	previous := zap.L()
	defer func() { zap.ReplaceGlobals(previous) }()
	key := identityTestKey(t)
	original, err := key.MarshalJSON()
	require.NoError(t, err)
	var members map[string]any
	require.NoError(t, json.Unmarshal(original, &members))
	members["kid"] = "another-public-key-id"
	reserialized, err := json.MarshalIndent(members, "", "  ")
	require.NoError(t, err)
	other, err := identityTestKey(t).MarshalJSON()
	require.NoError(t, err)
	encode := func(data []byte) string { return "client:secret:v1:" + base64.RawURLEncoding.EncodeToString(data) }
	secret, equivalent, different := encode(original), encode(reserialized), encode(other)
	require.NotEqual(t, secret, equivalent)
	validations, factories, calls := 0, 0, 0
	h, err := newLambdaInvokeIdentityHandler(ctx, ".example.com", func(ctx context.Context, _ *pbtransport.InvokeIdentity, _ string) (*lambdaIdentityRuntime, error) {
		factories++
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
		return &lambdaIdentityRuntime{ctx: ctx, close: func() {}, build: func(context.Context, string) (*lambdaConnectorGeneration, error) {
			return &lambdaConnectorGeneration{version: "v1", connector: &identityTestConnector{call: func(ctx context.Context) error {
				calls++
				require.Same(t, key, ctx.Value(crypto.ContextClientSecretKey))
				return nil
			}}}, nil
		}}, nil
	})
	require.NoError(t, err)
	defer h.Close()
	identityTestValidator(t, h, key, &validations)
	token := identityTestToken(t, key, "tenant.example.com", nil)
	messages := []string{}
	invoke := func(clientID, secret string) codes.Code {
		response, err := h.Handler(identityTestContext(context.Background()), identityTestRequest(t, clientID, secret, token, "v1"))
		require.NoError(t, err)
		st, err := response.Status()
		require.NoError(t, err)
		messages = append(messages, st.Message())
		return st.Code()
	}
	require.Equal(t, codes.FailedPrecondition, invoke(identityTestClient, "marker-secret"))
	require.Zero(t, factories)
	require.Nil(t, h.binding)
	require.Equal(t, codes.OK, invoke(identityTestClient, secret))
	generation := h.reloader.current
	require.Equal(t, codes.OK, invoke(identityTestClient, equivalent), "same key, different serialization and kid")
	require.Equal(t, 2, calls)
	require.Equal(t, codes.FailedPrecondition, invoke(identityTestClient, different))
	changedMessage := messages[len(messages)-1]
	require.Contains(t, changedMessage, "different client key")
	require.Contains(t, changedMessage, "new execution scope")
	require.Equal(t, codes.FailedPrecondition, invoke("another@tenant.example.com/use", secret))
	require.NotEqual(t, changedMessage, messages[len(messages)-1])
	require.Equal(t, codes.FailedPrecondition, invoke(identityTestClient, "marker-secret"))
	require.Equal(t, changedMessage, messages[len(messages)-1], "unparseable keys on bound environments direct callers to a new scope")
	require.Equal(t, 2, calls, "refusals happen before dispatch")
	require.Same(t, generation, h.reloader.current)
	require.Equal(t, codes.OK, invoke(identityTestClient, secret), "original identity keeps serving")
	require.Equal(t, 1, factories)
	require.Equal(t, 3, calls)
	allOutput := fmt.Sprint(messages, records.All())
	for _, value := range []string{secret, equivalent, different, "marker-secret"} {
		digest := sha256.Sum256([]byte(value))
		require.NotContains(t, allOutput, value)
		require.NotContains(t, allOutput, hex.EncodeToString(digest[:]))
		require.NotContains(t, allOutput, base64.RawURLEncoding.EncodeToString(digest[:]))
	}
	require.Empty(t, records.All(), "key refusals emit no secret, digest or repeated key warning")
}

func TestIdentityBindingRawEqualityFastPath(t *testing.T) {
	binding, err := newLambdaIdentityBinding(testSecret(t))
	require.NoError(t, err)
	// Poison the slow-path comparison to observe that identical bytes bypass it.
	binding.thumbprint = "not-the-key-thumbprint"
	require.NoError(t, binding.check(testSecret(t)))
	require.Error(t, binding.check("marker-secret"))
}

type observedIdentityValidator struct {
	lambdaIdentityValidator
	closes *atomic.Int32
}

func (v *observedIdentityValidator) Close() { v.closes.Add(1); v.lambdaIdentityValidator.Close() }

func TestInvokeIdentityKeyLoadAndMissingKidClassification(t *testing.T) {
	for _, failure := range []string{"status", "document", "network", "timeout", "missing-kid", "refresh-load", "refresh-success"} {
		t.Run(failure, func(t *testing.T) {
			key, wrong := identityTestKey(t), identityTestKey(t)
			wrong.KeyID = "other-key"
			public := key.Public()
			wrongPublic := wrong.Public()
			publicJSON, err := json.Marshal(public)
			require.NoError(t, err)
			wrongJSON, err := json.Marshal(wrongPublic)
			require.NoError(t, err)
			var requests, closes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := requests.Add(1)
				switch {
				case failure == "status" || (failure == "refresh-load" && n%2 == 0):
					w.WriteHeader(http.StatusServiceUnavailable)
				case failure == "document":
					_, _ = w.Write([]byte("marker-secret"))
				case failure == "timeout":
					<-r.Context().Done()
				case failure == "missing-kid" || failure == "refresh-load" || (failure == "refresh-success" && n == 1):
					_, _ = fmt.Fprintf(w, `{"keys":[%s]}`, wrongJSON)
				default:
					_, _ = fmt.Fprintf(w, `{"keys":[%s]}`, publicJSON)
				}
			}))
			defer server.Close()
			if failure == "network" {
				server.Close()
			}
			client := server.Client()
			client.Timeout = 50 * time.Millisecond
			factories := 0
			h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error) {
				factories++
				return nil, errors.New("stop after authentication")
			})
			require.NoError(t, err)
			h.newValidator = func(ctx context.Context, _, _ string) (lambdaIdentityValidator, error) {
				v, err := auth.NewValidator(ctx, auth.Config{JWKSUrl: server.URL, JWKSClient: client, RefreshOnUnknownKey: true,
					Issuer: "https://tenant.example.com", Subject: identityTestScope, Audience: identityTestScope})
				if err != nil {
					return nil, err
				}
				return &observedIdentityValidator{lambdaIdentityValidator: v, closes: &closes}, nil
			}
			token := identityTestToken(t, key, "tenant.example.com", nil)
			for attempt := 1; attempt <= 2; attempt++ {
				want := codes.Unavailable
				if failure == "missing-kid" {
					want = codes.FailedPrecondition
				}
				require.Equal(t, want, identityTestCode(t, h, identityTestContext(context.Background()), identityTestRequest(t, identityTestClient, testSecret(t), token, "v1")))
				require.Nil(t, h.validator)
				require.Nil(t, h.runtime)
				require.Nil(t, h.binding)
				if failure == "missing-kid" || failure == "refresh-load" {
					require.EqualValues(t, 2*attempt, requests.Load(), "exactly one refresh per provisional validator")
					require.EqualValues(t, attempt, closes.Load())
				}
			}
			if failure == "refresh-success" {
				require.Equal(t, 2, factories)
				require.EqualValues(t, 3, requests.Load())
				require.EqualValues(t, 2, closes.Load())
			} else {
				require.Zero(t, factories)
			}
		})
	}
}

func TestIdentityBuildCancellationAndBudgetStages(t *testing.T) {
	for _, stage := range []string{"cancel", "budget"} {
		invoke, endInvoke := context.WithCancel(context.Background())
		defer endInvoke()
		_, err := buildIdentityGeneration(context.Background(), invoke, time.Millisecond,
			func(ctx context.Context, _ string) (*lambdaConnectorGeneration, error) {
				if stage == "cancel" {
					endInvoke()
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}, "v1")
		require.Equal(t, stage, lambdaBuildErrorStage(err))
		if stage == "cancel" {
			require.ErrorIs(t, err, context.Canceled)
			require.NotContains(t, err.Error(), "time budget")
		} else {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
	}
}

func TestUnswappedCloseWarningsPreserveModeOffDetails(t *testing.T) {
	for _, identityMode := range []bool{false, true} {
		core, records := observer.New(zap.DebugLevel)
		ctx := ctxzap.ToContext(context.Background(), zap.New(core))
		closeIdentityGeneration(ctx, &lambdaConnectorGeneration{connector: &feedbackCloseErrorConnector{}, version: "v2", identityMode: identityMode})
		require.Len(t, records.All(), 1)
		fields := records.All()[0].ContextMap()
		if identityMode {
			require.Empty(t, fields)
		} else {
			require.Equal(t, "v2", fields["config_version"])
			require.Equal(t, "marker-secret", fields["error"])
		}
	}
}

func TestInvokeIdentityDiscardsValidatorReturnedWithLoadError(t *testing.T) {
	key := identityTestKey(t).Public()
	data, err := key.MarshalJSON()
	require.NoError(t, err)
	validator, err := auth.NewValidator(context.Background(), auth.Config{PublicKeyJWK: string(data)})
	require.NoError(t, err)
	var closes atomic.Int32
	h, err := newLambdaInvokeIdentityHandler(context.Background(), ".example.com", nil)
	require.NoError(t, err)
	h.newValidator = func(context.Context, string, string) (lambdaIdentityValidator, error) {
		return &observedIdentityValidator{lambdaIdentityValidator: validator, closes: &closes}, errors.New("marker-secret")
	}
	require.Equal(t, codes.Unavailable, identityTestCode(t, h, identityTestContext(context.Background()),
		identityTestRequest(t, identityTestClient, testSecret(t), "signed-token", "v1")))
	require.EqualValues(t, 1, closes.Load())
	require.Nil(t, h.validator)
	require.Nil(t, h.binding)
	require.Nil(t, h.runtime)
}

func TestInvokeIdentityClientFailureStage(t *testing.T) {
	core, records := observer.New(zap.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))
	key := identityTestKey(t)
	validations, factories := 0, 0
	h, err := newLambdaInvokeIdentityHandler(ctx, ".example.com", func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error) {
		factories++
		return nil, status.Error(codes.PermissionDenied, "marker-secret")
	})
	require.NoError(t, err)
	identityTestValidator(t, h, key, &validations)
	req := identityTestRequest(t, identityTestClient, testSecret(t), identityTestToken(t, key, "tenant.example.com", nil), "v1")
	require.Equal(t, codes.Unavailable, identityTestCode(t, h, identityTestContext(context.Background()), req))
	require.Equal(t, 1, factories)
	require.Nil(t, h.runtime)
	require.Nil(t, h.binding)
	logs := records.FilterMessage("lambda-run: failed to create identity client").All()
	require.Len(t, logs, 1)
	require.Equal(t, map[string]any{
		"execution_scope": identityTestScope, "client_id": identityTestClient,
		"cause_code": "PermissionDenied", "stage": "identity-client",
	}, logs[0].ContextMap())
	require.NotContains(t, fmt.Sprint(records.All()), "marker-secret")
}
