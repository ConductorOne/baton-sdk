//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/conductorone/baton-sdk/internal/connector"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/field"
	lambdaGrpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	lambdaConfig "github.com/conductorone/baton-sdk/pkg/lambda/grpc/config"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const lambdaIdentityBuildTimeout = time.Minute

var errLambdaExecutionScope = errors.New("execution scope mismatch")

type lambdaIdentityRuntime struct {
	ctx   context.Context
	build func(context.Context, string) (*lambdaConnectorGeneration, error)
	close func()
}

type lambdaIdentityFactory func(context.Context, *pbtransport.InvokeIdentity, string) (*lambdaIdentityRuntime, error)

type lambdaInvokeIdentityHandler struct {
	mu           sync.Mutex
	processCtx   context.Context
	suffix       string
	factory      lambdaIdentityFactory
	newValidator func(context.Context, string, string) (*auth.Validator, error)

	clientID  string
	scope     string
	validator *auth.Validator
	runtime   *lambdaIdentityRuntime
	reloader  *lambdaConnectorReloader
}

func newLambdaInvokeIdentityHandler(ctx context.Context, suffix string, factory lambdaIdentityFactory) (*lambdaInvokeIdentityHandler, error) {
	if suffix == "" {
		return nil, fmt.Errorf("lambda-run: lambda-allowed-host-suffix is required with per-invoke identity")
	}
	return &lambdaInvokeIdentityHandler{
		processCtx: ctx, suffix: suffix, factory: factory,
		newValidator: func(ctx context.Context, host, scope string) (*auth.Validator, error) {
			issuer := (&url.URL{Scheme: "https", Host: host}).String()
			jwksURL := (&url.URL{Scheme: "https", Host: host, Path: "/auth/v1/jwks"}).String()
			return auth.NewValidator(ctx, auth.Config{
				Issuer: issuer, Subject: scope, Audience: scope, JWKSUrl: jwksURL,
				JWKSClient: lambdaIdentityJWKSClient(),
			})
		},
	}, nil
}

func lambdaIdentityJWKSClient() *http.Client {
	return &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func invokeIdentityHost(clientID, suffix string) (string, error) {
	parts := strings.Split(clientID, "@")
	if len(parts) != 2 || parts[0] == "" {
		return "", fmt.Errorf("invalid invoke client id")
	}
	parts = strings.Split(parts[1], "/")
	if len(parts) != 2 || parts[1] == "" {
		return "", fmt.Errorf("invalid invoke client id")
	}
	host := parts[0]
	labels := strings.Split(host, ".")
	if len(host) > 253 || len(labels) < 2 || net.ParseIP(host) != nil {
		return "", fmt.Errorf("invalid invoke host")
	}
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", fmt.Errorf("invalid invoke host")
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return "", fmt.Errorf("invalid invoke host")
			}
		}
	}
	prefix, ok := strings.CutSuffix(host, suffix)
	if suffix == "" || !ok || prefix == "" || strings.Contains(prefix, ".") {
		return "", fmt.Errorf("invoke host is not allowed")
	}
	return host, nil
}

func (h *lambdaInvokeIdentityHandler) Handler(ctx context.Context, req *lambdaGrpc.Request) (resp *lambdaGrpc.Response, err error) {
	completed := false
	defer func() {
		_ = recover()
		if !completed {
			resp, err = lambdaGrpc.ErrorResponse(status.Error(codes.Unavailable, "lambda-run: identity initialization failed")), nil
		}
	}()
	ctx, reloader, prepareErr := h.prepare(ctx, req)
	if prepareErr != nil {
		completed = true
		return lambdaGrpc.ErrorResponse(prepareErr), nil
	}
	resp, err = reloader.Handler(ctx, req)
	completed = true
	return resp, err
}

func (h *lambdaInvokeIdentityHandler) prepare(ctx context.Context, req *lambdaGrpc.Request) (context.Context, *lambdaConnectorReloader, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	identity := req.InvokeIdentity()
	lc, ok := lambdacontext.FromContext(ctx)
	if identity == nil || identity.GetClientId() == "" || identity.GetClientSecret() == "" || !ok || lc.TenantID == "" {
		return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: invoke identity and platform execution scope are required")
	}
	if h.runtime != nil && (identity.GetClientId() != h.clientID || lc.TenantID != h.scope) {
		return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: execution environment is already bound")
	}

	validator := h.validator
	keepValidator := false
	if validator == nil {
		host, err := invokeIdentityHost(identity.GetClientId(), h.suffix)
		if err != nil {
			return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: invoke host is not allowed")
		}
		validator, err = h.newValidator(h.processCtx, host, lc.TenantID)
		if err != nil {
			return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: could not authenticate invoke")
		}
		defer func() {
			if !keepValidator {
				validator.Close()
			}
		}()
	}
	values := req.Headers().Get("authorization")
	if len(values) != 1 {
		return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: could not authenticate invoke")
	}
	token, err := auth.ExtractBearerToken(values[0])
	if err == nil {
		_, err = validator.ValidateToken(ctx, token)
	}
	if err != nil {
		return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: could not authenticate invoke")
	}
	if h.runtime == nil {
		boundCtx := ctxzap.ToContext(h.processCtx, ctxzap.Extract(h.processCtx).With(
			zap.String("execution_scope", lc.TenantID), zap.String("client_id", identity.GetClientId())))
		runtime, err := h.factory(boundCtx, identity, lc.TenantID)
		if err != nil {
			return ctx, nil, status.Error(codes.Unavailable, "lambda-run: failed to create identity client")
		}
		keepRuntime := false
		defer func() {
			if !keepRuntime {
				runtime.close()
			}
		}()
		requestedVersion := ""
		if versions := req.Headers().Get(lambdaConnectorConfigVersionHeader); len(versions) > 0 {
			requestedVersion = versions[0]
		}
		generation, err := runtime.build(runtime.ctx, requestedVersion)
		if err != nil {
			if errors.Is(err, errLambdaExecutionScope) {
				return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: execution scope mismatch")
			}
			return ctx, nil, status.Error(codes.Unavailable, "lambda-run: failed to build connector")
		}
		registered := false
		defer func() {
			if !registered {
				closeIdentityGeneration(runtime.ctx, generation)
			}
		}()
		server := lambdaGrpc.NewServer(lambdaIdentityRecoveryInterceptor())
		connector.Register(runtime.ctx, server, generation.connector, &connector.RegisterOps{ProvisioningEnabled: true, TicketingEnabled: true})
		h.reloader = &lambdaConnectorReloader{
			server: server, current: generation, build: runtime.build, redactErrors: true,
			lastRebuildAt: time.Now(), lastRebuildRequestedVersion: requestedVersion,
		}
		h.runtime, h.validator = runtime, validator
		h.clientID, h.scope = identity.GetClientId(), lc.TenantID
		keepRuntime, keepValidator, registered = true, true, true
		zap.ReplaceGlobals(ctxzap.Extract(runtime.ctx))
	}
	ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, h.runtime.ctx.Value(crypto.ContextClientSecretKey))
	ctx = ctxzap.ToContext(ctx, ctxzap.Extract(h.runtime.ctx))
	return ctx, h.reloader, nil
}

func lambdaIdentityRecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		completed := false
		defer func() {
			_ = recover()
			if !completed {
				ctxzap.Extract(ctx).Error("lambda-run: connector handler panic")
				resp, err = nil, status.Error(codes.Internal, "Internal Server Error")
			}
		}()
		resp, err = handler(ctx, req)
		completed = true
		return resp, err
	}
}

func (h *lambdaInvokeIdentityHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.validator != nil {
		h.validator.Close()
	}
	if h.runtime != nil {
		h.reloader.mu.Lock()
		generation := h.reloader.current
		h.reloader.mu.Unlock()
		closeIdentityGeneration(h.runtime.ctx, generation)
		h.runtime.close()
	}
}

func closeIdentityGeneration(parent context.Context, generation *lambdaConnectorGeneration) {
	if generation == nil {
		return
	}
	if generation.cancel != nil {
		generation.cancel()
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), lambdaConnectorCloseTimeout)
	defer cancel()
	_ = closeLambdaConnectorGeneration(ctx, generation.connector)
}

func lambdaIdentityRuntimeFactory[T field.Configurable](v *viper.Viper, schema field.Configuration, getconnector GetConnectorFunc2[T], sessionStoreEnabled bool) lambdaIdentityFactory {
	return func(ctx context.Context, identity *pbtransport.InvokeIdentity, scope string) (_ *lambdaIdentityRuntime, err error) {
		ctx, cancel := context.WithCancel(ctx)
		timer := time.AfterFunc(lambdaIdentityBuildTimeout, cancel)
		defer timer.Stop()
		client, webKey, _, err := lambdaConfig.NewDPoPClient(ctx, identity.GetClientId(), identity.GetClientSecret(), lambdaConfig.WithHostOverridesDisabled())
		if err != nil {
			cancel()
			return nil, err
		}
		closeClient := func() {
			cancel()
			if closer, ok := client.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		}
		keep := false
		defer func() {
			if !keep {
				closeClient()
			}
		}()
		if _, ok := webKey.Key.(ed25519.PrivateKey); !ok {
			return nil, fmt.Errorf("invalid configuration key")
		}
		key, err := crypto.ParseClientSecret([]byte(identity.GetClientSecret()), true)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
		build := newLambdaGenerationBuilder(ctx, v, schema, getconnector, sessionStoreEnabled, client, webKey, scope)
		runtime := &lambdaIdentityRuntime{ctx: ctx, close: closeClient}
		runtime.build = func(_ context.Context, requestedVersion string) (_ *lambdaConnectorGeneration, err error) {
			return buildIdentityGeneration(ctx, lambdaIdentityBuildTimeout, build, requestedVersion)
		}
		if !timer.Stop() || ctx.Err() != nil {
			return nil, fmt.Errorf("identity client initialization exceeded its time budget")
		}
		keep = true
		return runtime, nil
	}
}

func buildIdentityGeneration(ctx context.Context, timeout time.Duration, build func(context.Context, string) (*lambdaConnectorGeneration, error), requestedVersion string) (_ *lambdaConnectorGeneration, err error) {
	generationCtx, cancel := context.WithCancel(ctx)
	// Stop the construction budget on success without canceling the generation's lifetime.
	timer := time.AfterFunc(timeout, cancel)
	keep := false
	defer func() {
		timer.Stop()
		if !keep {
			cancel()
		}
	}()
	generation, err := build(generationCtx, requestedVersion)
	if err != nil {
		return nil, err
	}
	if !timer.Stop() || generationCtx.Err() != nil {
		closeIdentityGeneration(ctx, generation)
		return nil, fmt.Errorf("connector build exceeded its time budget")
	}
	generation.cancel = cancel
	keep = true
	return generation, nil
}
