//go:build baton_lambda_support

package cli

import (
	"context"
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
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
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

type lambdaIdentityValidator interface {
	ValidateToken(context.Context, string) (map[string]interface{}, error)
	Close()
}

type lambdaInvokeIdentityHandler struct {
	mu           sync.Mutex
	processCtx   context.Context
	suffix       string
	factory      lambdaIdentityFactory
	newValidator func(context.Context, string, string) (lambdaIdentityValidator, error)
	// restoreLogLevel puts the process log level back to its startup setting.
	// It is nil when no startup level was supplied.
	restoreLogLevel func()

	clientID  string
	scope     string
	validator lambdaIdentityValidator
	binding   *lambdaIdentityBinding
	runtime   *lambdaIdentityRuntime
	reloader  *lambdaConnectorReloader
}

func newLambdaInvokeIdentityHandler(ctx context.Context, suffix string, factory lambdaIdentityFactory) (*lambdaInvokeIdentityHandler, error) {
	if suffix == "" {
		return nil, fmt.Errorf("lambda-run: lambda-allowed-host-suffix is required with per-invoke identity")
	}
	if err := validateAllowedHostSuffix(suffix); err != nil {
		return nil, err
	}
	jwksClient, err := lambdaIdentityJWKSClient()
	if err != nil {
		return nil, fmt.Errorf("lambda-run: failed to configure the identity key client: %w", err)
	}
	return &lambdaInvokeIdentityHandler{
		processCtx: ctx, suffix: suffix, factory: factory,
		newValidator: func(ctx context.Context, host, scope string) (lambdaIdentityValidator, error) {
			issuer := (&url.URL{Scheme: "https", Host: host}).String()
			jwksURL := (&url.URL{Scheme: "https", Host: host, Path: "/auth/v1/jwks"}).String()
			validator, err := auth.NewValidator(ctx, auth.Config{
				Issuer: issuer, Subject: scope, Audience: scope, JWKSUrl: jwksURL,
				JWKSClient: jwksClient, RefreshOnUnknownKey: true,
			})
			if err != nil {
				return nil, err
			}
			return validator, nil
		},
	}, nil
}

// validateAllowedHostSuffix checks DNS syntax, not public-suffix ownership.
// Operators must not use a public suffix such as ".co.uk".
func validateAllowedHostSuffix(suffix string) error {
	if suffix == "" {
		return fmt.Errorf("lambda-run: lambda-allowed-host-suffix is required with per-invoke identity")
	}
	// The suffix is compared with the host as a raw string, so it has to carry
	// its own delimiter. Without one, "example.com" would also accept
	// "evilexample.com" and trust that host's keys.
	if suffix[0] != '.' && suffix[0] != '-' {
		return fmt.Errorf("lambda-run: lambda-allowed-host-suffix must begin with \".\" or \"-\"")
	}
	// A host is one label followed by the suffix, so the shortest host this
	// suffix admits must itself be a valid host. That rejects uppercase, empty
	// labels and a trailing dot, none of which a runtime host can match.
	_, domain, _ := strings.Cut("a"+suffix, ".")
	if !validInvokeHostSyntax("a"+suffix) || !strings.Contains(domain, ".") {
		// The domain under the first label needs two labels of its own, or a
		// suffix such as ".com" would trust every host directly under it.
		return fmt.Errorf("lambda-run: lambda-allowed-host-suffix must be a lowercase DNS suffix under a domain of at least two labels, for example \".example.com\"")
	}
	return nil
}

// setStartupLogLevel records the level to return to when a first build fails
// after the builder has already applied the fetched configuration's level.
func (h *lambdaInvokeIdentityHandler) setStartupLogLevel(config lambdaLogLevelConfig) {
	h.restoreLogLevel = func() {
		if err := applyLambdaLogLevel(config, time.Now()); err != nil {
			ctxzap.Extract(h.processCtx).Warn("lambda-run: failed to restore log level after a failed first build", zap.Error(err))
		}
	}
}

// lambdaIdentityJWKSClient trusts the same roots as the token and
// configuration clients for the same host, including the CA bundle named by
// BATON_LAMBDA_CA_CERT_PATH.
func lambdaIdentityJWKSClient() (*http.Client, error) {
	tlsConfig, err := lambdaConfig.TLSConfig()
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &http.Client{
		Transport:     transport,
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
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
	if !validInvokeHostSyntax(host) {
		return "", fmt.Errorf("invalid invoke host")
	}
	prefix, ok := strings.CutSuffix(host, suffix)
	if !ok || prefix == "" || strings.Contains(prefix, ".") {
		return "", fmt.Errorf("invoke host is not allowed")
	}
	return host, nil
}

// validInvokeHostSyntax reports whether host is a lowercase DNS name of at
// least two labels, with no port, no trailing dot and no IP literal.
func validInvokeHostSyntax(host string) bool {
	labels := strings.Split(host, ".")
	if len(host) > 253 || len(labels) < 2 || net.ParseIP(host) != nil {
		return false
	}
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			isLowercaseLetter := c >= 'a' && c <= 'z'
			isDigit := c >= '0' && c <= '9'
			if !isLowercaseLetter && !isDigit && c != '-' {
				return false
			}
		}
	}
	return true
}

func (h *lambdaInvokeIdentityHandler) Handler(ctx context.Context, req *lambdaGrpc.Request) (*lambdaGrpc.Response, error) {
	var resp *lambdaGrpc.Response
	var err error
	func() {
		completed := false
		defer func() {
			_ = recover()
			if !completed {
				resp, err = lambdaGrpc.ErrorResponse(ugrpc.RecoveredPanicErrorRedacted(h.processCtx, ugrpc.RecoverySiteLambdaHandler,
					"lambda-run: invoke identity handler panic", status.Error(codes.Unavailable, "lambda-run: identity initialization failed"))), nil
			}
		}()
		callCtx, reloader, prepareErr := h.prepare(ctx, req)
		if prepareErr != nil {
			resp, err = lambdaGrpc.ErrorResponse(prepareErr), nil
			completed = true
			return
		}
		resp, err = reloader.Handler(callCtx, req)
		completed = true
	}()
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
	host := ""
	if validator == nil {
		var err error
		host, err = invokeIdentityHost(identity.GetClientId(), h.suffix)
		if err != nil {
			return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: invoke host is not allowed")
		}
	}
	// Check that a bearer token is present before building a validator: on an
	// unbound environment that is an outbound key fetch made while holding
	// h.mu, and a request without a token should not be able to cause one.
	// These are credential failures, so they carry the same code the
	// mode-off auth interceptor uses.
	values := req.Headers().Get("authorization")
	if len(values) != 1 {
		return ctx, nil, status.Error(codes.Unauthenticated, "lambda-run: could not authenticate invoke")
	}
	token, err := auth.ExtractBearerToken(values[0])
	if err != nil {
		return ctx, nil, status.Error(codes.Unauthenticated, "lambda-run: could not authenticate invoke")
	}
	keepValidator := false
	if validator == nil {
		validator, err = h.newValidator(h.processCtx, host, lc.TenantID)
		if err != nil {
			if validator != nil {
				validator.Close()
			}
			return ctx, nil, status.Error(codes.Unavailable, "lambda-run: could not load invoke verification keys")
		}
		defer func() {
			if !keepValidator {
				validator.Close()
			}
		}()
	}
	if _, err := validator.ValidateToken(ctx, token); err != nil {
		if errors.Is(err, auth.ErrKeySetUnavailable) {
			return ctx, nil, status.Error(codes.Unavailable, "lambda-run: could not load invoke verification keys")
		}
		if errors.Is(err, auth.ErrTokenKeyNotFound) {
			return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: no verification key for invoke token")
		}
		return ctx, nil, status.Error(codes.Unauthenticated, "lambda-run: could not authenticate invoke")
	}
	binding := h.binding
	if binding == nil {
		binding, err = newLambdaIdentityBinding(identity.GetClientSecret())
	} else {
		err = binding.check(identity.GetClientSecret())
	}
	if err != nil {
		return ctx, nil, err
	}
	if h.runtime == nil {
		boundCtx := ctxzap.ToContext(h.processCtx, ctxzap.Extract(h.processCtx).With(
			zap.String("execution_scope", lc.TenantID), zap.String("client_id", identity.GetClientId())))
		runtime, err := h.factory(boundCtx, identity, lc.TenantID)
		if err != nil {
			// The cause can contain credentials, so only its code and stage are logged.
			ctxzap.Extract(boundCtx).Error("lambda-run: failed to create identity client",
				zap.Stringer("cause_code", status.Code(err)), zap.String("stage", "identity-client"))
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
		registered := false
		// The builder applies the fetched configuration's log level before it
		// constructs the connector. If this first build does not end in a
		// registered generation, nothing owns that level, so put it back.
		defer func() {
			if !registered && h.restoreLogLevel != nil {
				h.restoreLogLevel()
			}
		}()
		generation, err := runtime.build(ctx, requestedVersion)
		if err != nil {
			if errors.Is(err, errLambdaExecutionScope) {
				ctxzap.Extract(boundCtx).Error("lambda-run: execution scope mismatch on first build",
					zap.String("requested_version", requestedVersion), zap.String("stage", lambdaBuildErrorStage(err)))
				return ctx, nil, status.Error(codes.FailedPrecondition, "lambda-run: execution scope mismatch")
			}
			ctxzap.Extract(boundCtx).Error("lambda-run: failed to build connector",
				zap.String("requested_version", requestedVersion), zap.Stringer("cause_code", status.Code(err)), zap.String("stage", lambdaBuildErrorStage(err)))
			return ctx, nil, status.Error(codes.Unavailable, "lambda-run: failed to build connector")
		}
		defer func() {
			if !registered {
				closeIdentityGeneration(runtime.ctx, generation)
			}
		}()
		server := lambdaGrpc.NewServer(lambdaIdentityRecoveryInterceptor())
		connector.Register(runtime.ctx, server, generation.connector, &connector.RegisterOps{ProvisioningEnabled: true, TicketingEnabled: true})
		h.reloader = &lambdaConnectorReloader{
			server: server, current: generation, build: runtime.build, identityMode: true,
			lastRebuildAt: time.Now(), lastRebuildRequestedVersion: requestedVersion,
		}
		h.runtime, h.validator = runtime, validator
		h.clientID, h.scope = identity.GetClientId(), lc.TenantID
		h.binding = binding
		keepRuntime, keepValidator, registered = true, true, true
		zap.ReplaceGlobals(ctxzap.Extract(runtime.ctx))
	}
	ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, h.runtime.ctx.Value(crypto.ContextClientSecretKey))
	ctx = ctxzap.ToContext(ctx, ctxzap.Extract(h.runtime.ctx))
	return ctx, h.reloader, nil
}

func lambdaIdentityRecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var resp any
		var err error
		func() {
			completed := false
			defer func() {
				_ = recover()
				if !completed {
					resp, err = nil, ugrpc.RecoveredPanicErrorRedacted(ctx, ugrpc.RecoverySiteInterceptor,
						"lambda-run: connector handler panic", status.Error(codes.Internal, "Internal Server Error"), zap.String("method", info.FullMethod))
				}
			}()
			resp, err = handler(ctx, req)
			completed = true
		}()
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
	if err := closeLambdaConnectorGeneration(ctx, generation.connector); err != nil {
		if generation.identityMode {
			ctxzap.Extract(parent).Warn("lambda-run: failed to close connector generation")
		} else {
			ctxzap.Extract(parent).Warn("lambda-run: failed to close connector generation",
				zap.String("config_version", generation.version), zap.Error(err))
		}
	}
}

func lambdaIdentityRuntimeFactory[T field.Configurable](v *viper.Viper, schema field.Configuration, getconnector GetConnectorFunc2[T], sessionStoreEnabled bool) lambdaIdentityFactory {
	return func(ctx context.Context, identity *pbtransport.InvokeIdentity, scope string) (*lambdaIdentityRuntime, error) {
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
				if err := closer.Close(); err != nil {
					ctxzap.Extract(ctx).Warn("lambda-run: failed to close identity client")
				}
			}
		}
		keep := false
		defer func() {
			if !keep {
				closeClient()
			}
		}()
		key, err := crypto.ParseClientSecret([]byte(identity.GetClientSecret()), true)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, crypto.ContextClientSecretKey, key)
		build := newLambdaGenerationBuilder(ctx, v, schema, getconnector, sessionStoreEnabled, client, webKey, true, scope)
		runtime := &lambdaIdentityRuntime{ctx: ctx, close: closeClient}
		runtime.build = func(invokeCtx context.Context, requestedVersion string) (*lambdaConnectorGeneration, error) {
			return buildIdentityGeneration(ctx, invokeCtx, lambdaIdentityBuildTimeout, build, requestedVersion)
		}
		if !timer.Stop() || ctx.Err() != nil {
			return nil, fmt.Errorf("identity client initialization exceeded its time budget")
		}
		keep = true
		return runtime, nil
	}
}

func buildIdentityGeneration(
	ctx, invokeCtx context.Context,
	timeout time.Duration,
	build func(context.Context, string) (*lambdaConnectorGeneration, error),
	requestedVersion string,
) (*lambdaConnectorGeneration, error) {
	generationCtx, cancelCause := context.WithCancelCause(ctx)
	cancel := func() { cancelCause(context.Canceled) }
	// Stop construction cancellation before transferring the generation's lifetime.
	timer := time.AfterFunc(timeout, func() { cancelCause(context.DeadlineExceeded) })
	stopInvoke := context.AfterFunc(invokeCtx, cancel)
	keep := false
	defer func() {
		timer.Stop()
		stopInvoke()
		if !keep {
			cancel()
		}
	}()
	generation, err := build(generationCtx, requestedVersion)
	timerStopped, invokeStopped := timer.Stop(), stopInvoke()
	if err != nil || !timerStopped || !invokeStopped || generationCtx.Err() != nil || invokeCtx.Err() != nil {
		closeIdentityGeneration(ctx, generation)
		if invokeCtx.Err() != nil || !invokeStopped {
			cause := invokeCtx.Err()
			if cause == nil {
				cause = context.Canceled
			}
			return nil, &lambdaBuildStageError{stage: "cancel", err: fmt.Errorf("connector build canceled by invoke: %w", cause)}
		}
		if !timerStopped && ctx.Err() == nil {
			return nil, &lambdaBuildStageError{stage: "budget", err: fmt.Errorf("connector build exceeded its time budget: %w", context.DeadlineExceeded)}
		}
		if cause := context.Cause(generationCtx); cause != nil {
			if errors.Is(cause, context.DeadlineExceeded) && ctx.Err() == nil {
				return nil, &lambdaBuildStageError{stage: "budget", err: fmt.Errorf("connector build exceeded its time budget: %w", cause)}
			}
			return nil, &lambdaBuildStageError{stage: "cancel", err: fmt.Errorf("connector build canceled: %w", cause)}
		}
		return nil, err
	}
	generation.cancel = cancel
	keep = true
	return generation, nil
}
