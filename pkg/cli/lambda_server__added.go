//go:build baton_lambda_support

package cli

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	aws_lambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/go-jose/go-jose/v4"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/maypok86/otter/v2"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/conductorone/baton-sdk/internal/connector"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/auth"
	"github.com/conductorone/baton-sdk/pkg/field"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	c1_lambda_config "github.com/conductorone/baton-sdk/pkg/lambda/grpc/config"
	"github.com/conductorone/baton-sdk/pkg/lambda/grpc/middleware"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"google.golang.org/grpc"
)

const (
	lambdaConnectorConfigVersionHeader = "x-baton-connector-config-version"
	lambdaConnectorDrainTimeout        = 30 * time.Second
	lambdaConnectorCloseTimeout        = 10 * time.Second

	// Served-policy envelope contract versions this SDK understands. An
	// envelope outside these resolves to a governed-with-no-hosts (deny-all)
	// policy rather than being read.
	servedPolicyEnvelopeVersion = 1
	egressSectionSchemaVersion  = 1
)

// lambdaConnectorReloadMinInterval bounds the rebuild rate when the server
// persistently serves a config_version different from the requested header:
// the served-version labeling makes the no-op guard never match, so without
// this cap every invocation would rebuild. The generation stays labeled with
// the served version, so a stale reply is still re-fetched every interval.
var lambdaConnectorReloadMinInterval = 5 * time.Second

// lambdaConnectorReloadGlobalFloor is a short, version-independent rebuild
// floor: it caps alternating requested versions (cv-2, cv-3, cv-2) that would
// otherwise each bypass the version-keyed skew cap and rebuild on every
// invocation. It is deliberately much shorter than
// lambdaConnectorReloadMinInterval so a genuinely new version is deferred by
// at most this floor, not the full interval.
var lambdaConnectorReloadGlobalFloor = 1 * time.Second

type lambdaConnectorGeneration struct {
	version   string
	connector types.ConnectorServer
	logging   lambdaLogLevelConfig
}

type lambdaLogLevelConfig struct {
	level              string
	debugModeExpiresAt time.Time
}

func (c lambdaLogLevelConfig) effective(now time.Time) string {
	if c.level == "debug" && !c.debugModeExpiresAt.IsZero() && now.After(c.debugModeExpiresAt) {
		return "info"
	}
	return c.level
}

type lambdaConnectorCloserWithContext interface {
	Close(context.Context) error
}

type lambdaConnectorCloser interface {
	Close() error
}

type lambdaConnectorReloader struct {
	mu      sync.Mutex
	server  *c1_lambda_grpc.Server
	current *lambdaConnectorGeneration
	build   func(context.Context, string) (*lambdaConnectorGeneration, error)

	// lastRebuildAt/lastRebuildRequestedVersion back the reload rate-cap. When
	// the server persistently serves a config_version different from the
	// requested header, the served-version labeling makes the no-op guard never
	// match, so the cap bounds rebuilds: the version-keyed term caps the skew
	// case (the same requested version keeps arriving) to one per
	// lambdaConnectorReloadMinInterval, while the global floor caps alternating
	// requested versions to one per lambdaConnectorReloadGlobalFloor. A
	// genuinely new version is therefore deferred by at most the short global
	// floor, not the full interval. Stamped only on the success path, so a
	// persistently failing build is uncapped — intentional, so a retry can
	// clear a transient error.
	lastRebuildAt               time.Time
	lastRebuildRequestedVersion string
	// lastCapLoggedAt bounds the rate-cap Warn to one per rebuild interval: in
	// the persistent-skew steady state the cap fires on every invocation, so
	// without this the log would storm. It is compared against lastRebuildAt,
	// so it needs no reset on the success path.
	lastCapLoggedAt time.Time
}

// Handler is a recovery wrapper around handle. Config reload and log level
// application run before the request reaches the server, and the server's own
// dispatch runs outside the chain as well, so the recovery interceptor cannot
// see a panic from any of them — connector construction on a reload is the
// widest surface. An escaping panic here would unwind into the lambda runtime,
// which reports a crashed invocation with no status for the caller to classify.
func (r *lambdaConnectorReloader) Handler(ctx context.Context, req *c1_lambda_grpc.Request) (*c1_lambda_grpc.Response, error) {
	var resp *c1_lambda_grpc.Response
	var err error
	func() {
		// Tracking completion rather than testing the recovered value, the way
		// vendored grpc_recovery does: a nil panic value under GODEBUG=panicnil=1
		// is recovered but indistinguishable from no panic, and returning a nil
		// response with a nil error would reach the invoker as an empty success.
		panicked := true
		defer func() {
			p := recover()
			if !panicked {
				return
			}
			// Internal, deliberately not the Unavailable that reload's error exits
			// return: an error from reload is an anticipated failure (config fetch,
			// decrypt, validation) that a retry can plausibly clear, while a panic
			// is a defect that would recur on retry — classifying it as retryable
			// would invite the caller to loop on a bug. Internal is also what the
			// same panic produces everywhere else in the process, so the reload
			// branch is not an exception to the panic contract.
			resp, err = c1_lambda_grpc.ErrorResponse(ugrpc.RecoveredPanicError(ctx, p, ugrpc.RecoverySiteLambdaHandler)), nil
		}()
		resp, err = r.handle(ctx, req)
		panicked = false
	}()
	return resp, err
}

func (r *lambdaConnectorReloader) handle(ctx context.Context, req *c1_lambda_grpc.Request) (*c1_lambda_grpc.Response, error) {
	requestedVersion := ""
	if values := req.Headers().Get(lambdaConnectorConfigVersionHeader); len(values) > 0 {
		requestedVersion = values[0]
	}

	if requestedVersion != "" {
		if err := r.reloadIfNeeded(ctx, requestedVersion); err != nil {
			return c1_lambda_grpc.ErrorResponse(status.Errorf(codes.Unavailable, "lambda-run: failed to reload connector config: %v", err)), nil
		}
	}

	if err := r.applyCurrentLogLevel(time.Now()); err != nil {
		return c1_lambda_grpc.ErrorResponse(status.Errorf(codes.Unavailable, "lambda-run: failed to apply log level: %v", err)), nil
	}

	return r.server.Handler(ctx, req)
}

func (r *lambdaConnectorReloader) applyCurrentLogLevel(now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current == nil {
		return nil
	}
	return applyLambdaLogLevel(r.current.logging, now)
}

func (r *lambdaConnectorReloader) reloadIfNeeded(ctx context.Context, requestedVersion string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.current != nil && r.current.version == requestedVersion {
		return nil
	}
	if r.current == nil {
		return fmt.Errorf("no current connector generation is registered")
	}
	// Rate-cap the rebuild under persistent served/requested version skew: the
	// served-version labeling makes the no-op guard above never match, so
	// without this cap every version-stamped invocation would rebuild. The
	// generation stays labeled with the served version, so a stale reply is
	// still re-fetched every interval. Two tiers: the global floor caps any
	// rebuild (so alternating requested versions cannot bypass the cap), and
	// the version-keyed term caps the skew case (the same requested version
	// keeps arriving) to one per lambdaConnectorReloadMinInterval. A genuinely
	// new version is deferred by at most the short global floor, not the full
	// interval. The Warn is emitted once per rebuild interval (lastCapLoggedAt),
	// not on every capped invocation, so the persistent-skew steady state does
	// not log-storm.
	if time.Since(r.lastRebuildAt) < lambdaConnectorReloadGlobalFloor {
		return nil
	}
	if requestedVersion == r.lastRebuildRequestedVersion && time.Since(r.lastRebuildAt) < lambdaConnectorReloadMinInterval {
		if r.lastCapLoggedAt.Before(r.lastRebuildAt) {
			ctxzap.Extract(ctx).Warn("lambda-run: reload rate-capped; serving previous generation",
				zap.String("requested_version", requestedVersion),
				zap.String("current_version", r.current.version),
				zap.Duration("remaining_interval", lambdaConnectorReloadMinInterval-time.Since(r.lastRebuildAt)))
			r.lastCapLoggedAt = time.Now()
		}
		return nil
	}

	currentLog := r.current.logging
	// r.build applies next's log level before it can fail, so every exit that
	// leaves the active generation in place owes that generation's level a
	// restore. One deferred guard covers all of them, including a panic: the
	// handler recovers those now, so the sandbox survives and a level left at
	// next's setting would persist across later invocations.
	active := r.current
	defer func() {
		if r.current != active {
			return
		}
		if err := applyLambdaLogLevel(currentLog, time.Now()); err != nil {
			zap.L().Warn("failed to restore log level after a reload left the active generation in place", zap.Error(err))
		}
	}()

	next, err := r.build(ctx, requestedVersion)
	if err != nil {
		return err
	}
	previous := r.current
	replaced, drained, err := r.server.ReplaceServiceImplementation(previous.connector, next.connector)
	if err != nil {
		return err
	}
	if replaced == 0 {
		return fmt.Errorf("no registered services matched the current connector generation")
	}

	r.current = next
	r.lastRebuildAt = time.Now()
	r.lastRebuildRequestedVersion = requestedVersion
	if err := applyLambdaLogLevel(next.logging, time.Now()); err != nil {
		return err
	}
	closeCtx := context.WithoutCancel(ctx)
	go closeConnectorGenerationAfterDrain(closeCtx, previous, drained, lambdaConnectorDrainTimeout, lambdaConnectorCloseTimeout)
	return nil
}

func closeConnectorGenerationAfterDrain(ctx context.Context, generation *lambdaConnectorGeneration, drained <-chan struct{}, drainTimeout time.Duration, closeTimeout time.Duration) {
	if generation == nil {
		return
	}

	drainTimer := time.NewTimer(drainTimeout)
	defer drainTimer.Stop()

	select {
	case <-drained:
	case <-drainTimer.C:
		zap.L().Warn("timed out waiting for stale lambda connector generation to drain", zap.String("config_version", generation.version), zap.Duration("timeout", drainTimeout))
	}

	closeCtx, cancel := context.WithTimeout(ctx, closeTimeout)
	defer cancel()

	if err := closeLambdaConnectorGeneration(closeCtx, generation.connector); err != nil {
		zap.L().Warn("error closing stale lambda connector generation", zap.String("config_version", generation.version), zap.Error(err))
	}
}

func closeLambdaConnectorGeneration(ctx context.Context, connector types.ConnectorServer) error {
	errCh := make(chan error, 1)
	if closer, ok := connector.(lambdaConnectorCloserWithContext); ok {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("panic in connector Close(ctx): %v\n%s", r, debug.Stack())
				}
			}()
			errCh <- closer.Close(ctx)
		}()
	} else if closer, ok := connector.(lambdaConnectorCloser); ok {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("panic in connector Close(): %v\n%s", r, debug.Stack())
				}
			}()
			errCh <- closer.Close()
		}()
	} else {
		return nil
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func OptionallyAddLambdaCommand[T field.Configurable](
	ctx context.Context,
	name string,
	v *viper.Viper,
	getconnector GetConnectorFunc2[T],
	connectorSchema field.Configuration,
	mainCmd *cobra.Command,
	sessionStoreEnabled bool,
) error {
	lambdaSchema := field.NewConfiguration(field.LambdaServerFields(), field.WithConstraints(field.LambdaServerRelationships...))

	lambdaCmd, err := AddCommand(mainCmd, v, &lambdaSchema, &cobra.Command{
		Use:           "lambda",
		Short:         "Run a server for a AWS Lambda function",
		SilenceErrors: true,
		SilenceUsage:  true,
	})

	if err != nil {
		return err
	}

	lambdaCmd.RunE = func(cmd *cobra.Command, args []string) error {
		err := v.BindPFlags(cmd.Flags())
		if err != nil {
			return err
		}

		startupLogLevel, err := lambdaLogLevelConfigFromViper(v)
		if err != nil {
			return err
		}

		initialLogFields := map[string]interface{}{
			"tenant_id":          os.Getenv("tenant"),
			"connector_id":       os.Getenv("connector"),
			"app_id":             os.Getenv("app"),
			"release_version":    os.Getenv("version"),
			"installation":       os.Getenv("installation"),
			"catalog_id":         os.Getenv("catalog_id"),
			"catalog_name":       os.Getenv("catalog_name"),
			"tenant_name":        os.Getenv("tenant_name"),
			"tenant_is_internal": os.Getenv("tenant_is_internal"),
		}

		runCtx, err := initLogger(
			ctx,
			name,
			logging.WithLogFormat(v.GetString("log-format")),
			logging.WithLogLevel(startupLogLevel.effective(time.Now())),
			logging.WithInitialFields(initialLogFields),
		)
		if err != nil {
			return err
		}

		runCtx, otelShutdown, err := initOtel(runCtx, name, v, initialLogFields)
		if err != nil {
			return err
		}
		defer func() {
			if otelShutdown == nil {
				return
			}
			shutdownCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(otelShutdownTimeout))
			defer cancel()
			err := otelShutdown(shutdownCtx)
			if err != nil {
				zap.L().Error("error shutting down otel", zap.Error(err))
			}
		}()

		if err := field.Validate(lambdaSchema, v); err != nil {
			return err
		}

		// Create DPoP client with authentication
		grpcClient, webKey, _, err := c1_lambda_config.NewDPoPClient(
			runCtx,
			v.GetString(field.LambdaServerClientIDField.GetName()),
			v.GetString(field.LambdaServerClientSecretField.GetName()),
		)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to create DPoP client: %w", err)
		}

		// Create connector config service client using the DPoP client
		configClient := v1.NewConnectorConfigServiceClient(grpcClient)

		ed25519PrivateKey, ok := webKey.Key.(ed25519.PrivateKey)
		if !ok {
			return fmt.Errorf("lambda-run: failed to cast webkey to ed25519.PrivateKey")
		}

		clientSecret := v.GetString("lambda-client-secret")
		if clientSecret != "" {
			secretJwk, err := crypto.ParseClientSecret([]byte(clientSecret), true)
			if err != nil {
				return err
			}
			runCtx = context.WithValue(runCtx, crypto.ContextClientSecretKey, secretJwk)
		}

		// parse content directly for lambdas, don't read from file
		readFromPath := false
		decodeOpts := field.WithAdditionalDecodeHooks(field.FileUploadDecodeHook(readFromPath))

		sessionStoreMaximumSize := v.GetInt(field.ServerSessionStoreMaximumSizeField.GetName())
		var sessionStoreConstructor sessions.SessionStoreConstructor
		if sessionStoreEnabled {
			sessionStoreConstructor = createSessionCacheConstructor(grpcClient)
		} else {
			sessionStoreConstructor = func(ctx context.Context, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
				return &session.NoOpSessionStore{}, nil
			}
		}

		buildConnectorGeneration := func(ctx context.Context, requestedVersion string) (*lambdaConnectorGeneration, error) {
			// Get configuration, convert it to viper flag values, then proceed.
			config, err := configClient.GetConnectorConfig(ctx, &v1.GetConnectorConfigRequest{
				RequestedVersion: requestedVersion,
			})
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to get connector config: %w", err)
			}

			decrypted, err := jwk.DecryptED25519(ed25519PrivateKey, config.GetConfig())
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to decrypt config: %w", err)
			}

			configStruct := structpb.Struct{}
			err = json.Unmarshal(decrypted, &configStruct)
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to unmarshal decrypted config: %w", err)
			}

			connectorConfig := configStruct.AsMap()
			effectiveConfig := effectiveLambdaConfig(v, connectorConfig, connectorSchema)
			logLevelConfig, err := lambdaLogLevelConfigFromViper(effectiveConfig)
			if err != nil {
				return nil, err
			}

			t, err := makeLambdaConnectorConfiguration[T](v, effectiveConfig, connectorConfig, decodeOpts)
			if err != nil {
				return nil, fmt.Errorf("lambda-run: failed to make generic configuration: %w", err)
			}

			var (
				fieldOptions  []field.Option
				schemaFields  []field.SchemaField
				authMethodStr string
			)
			authMethodStr = effectiveConfig.GetString("auth-method")
			if authMethodStr != "" {
				fieldOptions = append(fieldOptions, field.WithAuthMethod(authMethodStr))
			}
			schemaFieldsMap := connectorSchema.FieldGroupFields(authMethodStr)
			for _, field := range schemaFieldsMap {
				schemaFields = append(schemaFields, field)
			}

			if len(schemaFields) == 0 {
				schemaFields = connectorSchema.Fields
			}

			if err := field.Validate(connectorSchema, t, fieldOptions...); err != nil {
				return nil, fmt.Errorf("failed to validate config: %w", err)
			}

			policy, version := egressPolicyAndGenerationVersion(ctx, requestedVersion, config)
			ops := RunTimeOpts{
				SessionStore: NewLazyCachingSessionStore(sessionStoreConstructor, func(otterOptions *otter.Options[string, []byte]) {
					if sessionStoreMaximumSize <= 0 {
						otterOptions.MaximumWeight = 0
					} else {
						otterOptions.MaximumWeight = uint64(sessionStoreMaximumSize)
					}
				}),
				SelectedAuthMethod:  authMethodStr,
				SyncResourceTypeIDs: effectiveConfig.GetStringSlice("sync-resource-types"),
				EgressPolicy:        policy,
			}

			if hasOauthField(schemaFields) {
				ops.TokenSource = &lambdaTokenSource{
					ctx:    runCtx,
					webKey: webKey,
					client: configClient,
				}
			}
			// Apply the connector's configured log level before constructing it so
			// construction-time logs honor the requested level. Only do this for
			// reloads; the initial build relies on the startup log level and the
			// post-build apply below.
			if requestedVersion != "" {
				if err := applyLambdaLogLevel(logLevelConfig, time.Now()); err != nil {
					return nil, err
				}
			}

			c, err := getconnector(runCtx, t, ops)
			if err != nil {
				return nil, fmt.Errorf("failed to get connector: %w", err)
			}

			return &lambdaConnectorGeneration{
				version:   version,
				connector: c,
				logging:   logLevelConfig,
			}, nil
		}

		initialGeneration, err := buildConnectorGeneration(runCtx, "")
		if err != nil {
			return err
		}
		if err := applyLambdaLogLevel(initialGeneration.logging, time.Now()); err != nil {
			return err
		}

		// Ensure only one auth method is provided
		jwk := v.GetString(field.LambdaServerAuthJWTSigner.GetName())
		jwksUrl := v.GetString(field.LambdaServerAuthJWTJWKSUrl.GetName())
		if (jwk == "" && jwksUrl == "") || (jwk != "" && jwksUrl != "") {
			return fmt.Errorf("lambda-run: must specify exactly one of %s or %s", field.LambdaServerAuthJWTSigner.GetName(), field.LambdaServerAuthJWTJWKSUrl.GetName())
		}

		authConfig := auth.Config{
			PublicKeyJWK: jwk,
			JWKSUrl:      jwksUrl,
			Issuer:       v.GetString(field.LambdaServerAuthJWTExpectedIssuerField.GetName()),
			Subject:      v.GetString(field.LambdaServerAuthJWTExpectedSubjectField.GetName()),
			Audience:     v.GetString(field.LambdaServerAuthJWTExpectedAudienceField.GetName()),
		}

		authOpt, err := middleware.WithAuth(runCtx, authConfig)
		if err != nil {
			return fmt.Errorf("lambda-run: failed to create auth middleware: %w", err)
		}

		// TODO(morgabra/kans): This seems to be OK in practice - just don't invoke the unimplemented methods.
		opts := &connector.RegisterOps{
			Ratelimiter:         nil,
			ProvisioningEnabled: true,
			TicketingEnabled:    true,
		}

		chain := lambdaUnaryInterceptorChain(authOpt)

		s := c1_lambda_grpc.NewServer(chain)
		connector.Register(runCtx, s, initialGeneration.connector, opts)

		reloader := &lambdaConnectorReloader{
			server:  s,
			current: initialGeneration,
			build:   buildConnectorGeneration,
		}

		aws_lambda.StartWithOptions(reloader.Handler, aws_lambda.WithContext(runCtx))
		return nil
	}

	return nil
}

// lambdaUnaryInterceptorChain builds the interceptor chain the lambda transport
// serves connector RPCs through. Recovery is first, and therefore outermost, so
// a panic anywhere below it — auth, or the connector method itself — returns as
// a status instead of escaping into the lambda runtime, which reports a crashed
// invocation with no status for the caller to act on.
func lambdaUnaryInterceptorChain(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	withRecovery := append([]grpc.UnaryServerInterceptor{ugrpc.RecoveryUnaryInterceptor()}, interceptors...)
	return ugrpc.ChainUnaryInterceptors(withRecovery...)
}

// generationVersion labels a built generation with the config_version the
// server actually served, falling back to the requested version when the
// response carries none. Labeling by the served version (not the requested
// header) keeps the reload no-op guard honest: a stale reply is re-fetched
// on the next version-stamped invocation instead of being pinned as current.
func generationVersion(ctx context.Context, requestedVersion string, config *v1.GetConnectorConfigResponse) string {
	// Prefer the served config_version only when the server actually stamped
	// it: lambdaConnectorConfigVersion's last_updated fallback is not in the
	// same namespace as the requested-version header, so labeling with it would
	// make the reload no-op guard never match and rebuild on every invocation.
	version := requestedVersion
	if config.HasConfigVersion() && config.GetConfigVersion() != "" {
		version = config.GetConfigVersion()
	} else if version == "" {
		version = lambdaConnectorConfigVersion(config)
	}
	if requestedVersion != "" && version != requestedVersion {
		ctxzap.Extract(ctx).Warn("connector_authoring: served config_version differs from requested; will retry on a later invocation",
			zap.String("requested_version", requestedVersion),
			zap.String("served_version", version))
	}
	return version
}

func lambdaConnectorConfigVersion(config *v1.GetConnectorConfigResponse) string {
	if config == nil {
		return ""
	}
	// Prefer the server-stamped durable config-version handle: it is the same
	// value the invoker sends back in the config-version header, so a warm
	// generation's version matches the requested version and does not force a
	// spurious reload. Older servers omit it; fall back to last_updated.
	if config.HasConfigVersion() {
		return config.GetConfigVersion()
	}
	if config.GetLastUpdated() == nil {
		return ""
	}
	return config.GetLastUpdated().AsTime().UTC().Format(time.RFC3339Nano)
}

// egressPolicyAndGenerationVersion projects the egress policy and labels the
// generation version for a fetched config response. It is the single seam the
// lambda build path uses for the two security-load-bearing compositions
// (policy delivery + served-version labeling), so both are unit-testable
// through one function.
func egressPolicyAndGenerationVersion(ctx context.Context, requestedVersion string, config *v1.GetConnectorConfigResponse) (*EgressPolicy, string) {
	return egressPolicyFromResponse(ctx, config), generationVersion(ctx, requestedVersion, config)
}

// egressPolicyFromResponse projects the connector-facing egress policy from the
// response's served-policy envelope, or nil when no envelope is present. A
// present envelope yields a non-nil policy even when it fails a binding,
// version, or section check — such an envelope resolves to a present policy with
// no hosts so the connector fails closed (deny-all) rather than serving
// unenforced. The config-version binding (the envelope's config_version must be
// non-empty and equal to the response's) is the one cross-field check the SDK is
// uniquely positioned to make; deeper content validation is the connector's.
func egressPolicyFromResponse(ctx context.Context, config *v1.GetConnectorConfigResponse) *EgressPolicy {
	if config == nil || !config.HasServedPolicyEnvelope() {
		return nil
	}
	env := config.GetServedPolicyEnvelope()
	// The deny-all fallback enforces: empty AllowedHosts + Enforce=true = block
	// all, so a mode-honoring connector fails closed rather than observing.
	// HTTPSOnly is also forced true so a scheme-gate-only consumer fails
	// stricter (plain-http egress refused) rather than observing; the valid
	// path below overrides it from the envelope.
	policy := &EgressPolicy{Enforce: true, HTTPSOnly: true}

	if env.GetEnvelopeVersion() != servedPolicyEnvelopeVersion {
		// Version-mismatch kill-switch tradeoff: an unrecognized envelope (or
		// section) version now fails closed fleet-wide (Enforce=true). That is
		// the safe default for a security control, but it means a version
		// mismatch takes egress offline for every connector at once. A future
		// version handshake is the durable mitigation.
		ctxzap.Extract(ctx).Warn("connector_authoring: served-policy envelope rejected; egress deny-all",
			zap.String("reason", "envelope-version-mismatch"),
			zap.Uint32("envelope_version", env.GetEnvelopeVersion()),
			zap.Uint32("want_envelope_version", servedPolicyEnvelopeVersion))
		return policy
	}
	cv := env.GetConfigVersion()
	if cv == "" || cv != config.GetConfigVersion() {
		ctxzap.Extract(ctx).Warn("connector_authoring: served-policy envelope rejected; egress deny-all",
			zap.String("reason", "config-version-binding-mismatch"),
			zap.String("envelope_config_version", cv),
			zap.String("response_config_version", config.GetConfigVersion()))
		return policy
	}
	egress := env.GetEgress()
	if egress == nil || egress.GetSchemaVersion() != egressSectionSchemaVersion {
		ctxzap.Extract(ctx).Warn("connector_authoring: served-policy envelope rejected; egress deny-all",
			zap.String("reason", "egress-section-unsupported"),
			zap.Bool("egress_present", egress != nil),
			zap.Uint32("egress_schema_version", egress.GetSchemaVersion()),
			zap.Uint32("want_egress_schema_version", egressSectionSchemaVersion))
		return policy
	}
	policy.AllowedHosts = slices.Clone(egress.GetAllowedHosts())
	policy.HTTPSOnly = egress.GetHttpsOnly()
	// Intentionally lossy: only an explicit ENFORCE enforces; a future
	// enforcing mode must be added to this switch. Anything else observes.
	switch egress.GetMode() {
	case v1.EgressMode_EGRESS_MODE_ENFORCE:
		policy.Enforce = true
	default:
		policy.Enforce = false
		if _, known := v1.EgressMode_name[int32(egress.GetMode())]; !known {
			ctxzap.Extract(ctx).Warn("connector_authoring: unrecognized egress mode; observing",
				zap.Int32("mode", int32(egress.GetMode())))
		}
	}
	// The proto contract says an empty, wildcard, or IP-literal entry
	// invalidates the whole envelope. The allow-all hazard that makes this
	// fail closed only exists under ENFORCE: a wildcard entry would read as
	// allow-all to a wildcard-matching connector. Under REPORT/absent mode the
	// envelope is not rejected (Enforce stays false, so a mode-honoring
	// connector observes), but a contract-invalid entry (e.g. an on-prem
	// instance whose resolved config yields an IP-literal host) is dropped
	// from the projected allowlist — so a connector that enforces AllowedHosts
	// without reading Enforce cannot treat a "*" entry as allow-all. If the
	// dropped entry was the only host, the projection is empty, which a
	// host-gating connector reads as deny-all: dropping is not rejecting the
	// envelope, but it can still empty the allowlist. Under REPORT/UNSPECIFIED
	// every offending entry is warned in one projection; under ENFORCE the
	// first offender rejects the envelope (deny-all), so only that one is
	// warned. A port-bearing entry is dropped outright (the canonical form has
	// no port); a port suffix is split off before the IP check so a host:port
	// IP literal is caught. Entries are normalized to the canonical form
	// (lowercase, single trailing dot stripped) and path-bearing entries are
	// rejected, so those three non-canonical forms cannot silently never match
	// a resolved hostname.
	valid := make([]string, 0, len(policy.AllowedHosts))
	for _, h := range policy.AllowedHosts {
		host := h
		hasPort := false
		if hp, _, err := net.SplitHostPort(h); err == nil {
			host = hp
			hasPort = true
		}
		canonical := strings.ToLower(strings.TrimSuffix(host, "."))
		if canonical == "" || strings.Contains(h, "*") || hasPort || strings.Contains(h, "/") || net.ParseIP(strings.Trim(canonical, "[]")) != nil {
			ctxzap.Extract(ctx).Warn("connector_authoring: egress allowlist contains an empty, wildcard, IP-literal, port-bearing, or path-bearing host; envelope is invalid per contract",
				zap.String("host", h))
			if policy.Enforce {
				return &EgressPolicy{Enforce: true, HTTPSOnly: true}
			}
			continue
		}
		valid = append(valid, canonical)
	}
	policy.AllowedHosts = valid
	return policy
}

func lambdaLogLevelConfigFromViper(v *viper.Viper) (lambdaLogLevelConfig, error) {
	level, err := logging.NormalizeLogLevel(v.GetString("log-level"))
	if err != nil {
		return lambdaLogLevelConfig{}, fmt.Errorf("lambda-run: invalid log level: %w", err)
	}
	return lambdaLogLevelConfig{
		level:              level,
		debugModeExpiresAt: v.GetTime("log-level-debug-expires-at"),
	}, nil
}

func applyLambdaLogLevel(config lambdaLogLevelConfig, now time.Time) error {
	return logging.SetLogLevel(config.effective(now))
}

func cloneViperSettings(v *viper.Viper) *viper.Viper {
	cloned := viper.New()
	if v == nil {
		return cloned
	}
	for key, value := range v.AllSettings() {
		cloned.Set(key, value)
	}
	return cloned
}

func effectiveLambdaConfig(v *viper.Viper, connectorConfig map[string]any, connectorSchema field.Configuration) *viper.Viper {
	effectiveConfig := cloneViperSettings(v)
	stringMapFields := lambdaStringMapFields(connectorSchema)
	for key, value := range connectorConfig {
		if _, ok := stringMapFields[strings.ToLower(key)]; ok {
			value = lambdaStringMapValueForViper(value)
		}
		effectiveConfig.Set(key, value)
	}
	return effectiveConfig
}

func lambdaStringMapFields(connectorSchema field.Configuration) map[string]struct{} {
	out := make(map[string]struct{})
	for _, schemaField := range connectorSchema.Fields {
		if schemaField.Variant == field.StringMapVariant {
			out[strings.ToLower(schemaField.FieldName)] = struct{}{}
		}
	}
	for _, fieldGroup := range connectorSchema.FieldGroups {
		for _, schemaField := range fieldGroup.Fields {
			if schemaField.Variant == field.StringMapVariant {
				out[strings.ToLower(schemaField.FieldName)] = struct{}{}
			}
		}
	}
	return out
}

func lambdaStringMapValueForViper(value any) any {
	if _, ok := value.(string); ok {
		return value
	}
	// Viper.GetStringMap and GetStringMapString parse JSON strings, while
	// Viper.Set lowercases nested map[string]any keys.
	encoded, err := json.Marshal(cast.ToStringMapString(value))
	if err != nil {
		return value
	}
	return string(encoded)
}

// Typed configs must decode the raw lambda payload directly. Decoding them from
// effectiveConfig would send StringMap values through Viper.Set, which lowercases
// nested map keys.
func makeLambdaConnectorConfiguration[T field.Configurable](v *viper.Viper, effectiveConfig *viper.Viper, connectorConfig map[string]any, opts ...field.DecodeHookOption) (T, error) {
	var config T
	if _, ok := any(config).(*viper.Viper); ok {
		if t, ok := any(effectiveConfig).(T); ok {
			return t, nil
		}
		return config, fmt.Errorf("cannot convert *viper.Viper to %T", config)
	}

	t, err := MakeGenericConfiguration[T](v, opts...)
	if err != nil {
		return t, err
	}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: field.ComposeDecodeHookFunc(opts...),
		Result:     any(t),
	})
	if err != nil {
		return t, err
	}
	if err := decoder.Decode(connectorConfig); err != nil {
		return t, err
	}
	return t, nil
}

// createSessionCacheConstructor creates a session cache constructor function that uses the provided gRPC client.
func createSessionCacheConstructor(grpcClient grpc.ClientConnInterface) sessions.SessionStoreConstructor {
	return func(ctx context.Context, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
		// Create the gRPC session client using the same gRPC connection
		client := v1.NewBatonSessionServiceClient(grpcClient)
		// Create and return the session cache
		return session.NewGRPCSessionStore(ctx, client, opt...)
	}
}

type lambdaTokenSource struct {
	ctx    context.Context
	webKey *jose.JSONWebKey
	client v1.ConnectorConfigServiceClient
	token  *oauth2.Token
}

func (s *lambdaTokenSource) Token() (*oauth2.Token, error) {
	if s.token.Valid() {
		return s.token, nil
	}

	resp, err := s.client.GetConnectorOauthToken(s.ctx, &v1.GetConnectorOauthTokenRequest{})
	if err != nil {
		return nil, err
	}

	ed25519PrivateKey, ok := s.webKey.Key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("lambda-run: failed to cast webkey to ed25519.PrivateKey")
	}

	decrypted, err := jwk.DecryptED25519(ed25519PrivateKey, resp.Token)
	if err != nil {
		return nil, fmt.Errorf("lambda-run: failed to decrypt config: %w", err)
	}

	t := oauth2.Token{}
	err = json.Unmarshal(decrypted, &t)
	if err != nil {
		return nil, fmt.Errorf("lambda-run: failed to unmarshal decrypted config: %w", err)
	}

	s.token = &t
	return &t, nil
}

func hasOauthField(fields []field.SchemaField) bool {
	for _, f := range fields {
		if f.ConnectorConfig.FieldType == field.OAuth2 {
			return true
		}
	}
	return false
}
