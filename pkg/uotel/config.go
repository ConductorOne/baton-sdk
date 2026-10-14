package uotel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/conductorone/baton-sdk/pkg/sdk"
)

// otelConfig contains configuration for OpenTelemetry initialization.
type otelConfig struct {
	// Common configuration
	serviceName      string
	initialLogFields map[string]interface{}

	// endpoint for tracing, logging and metrics
	endpoint    string
	tlsCert     string
	tlsCertPath string
	tlsInsecure bool

	tracingDisabled bool
	loggingDisabled bool
	metricsDisabled bool

	// metricsInterval is how often the periodic reader collects and exports
	// metrics. Zero leaves the choice to the otel SDK, which honors
	// OTEL_METRIC_EXPORT_INTERVAL (default 60s). Close always performs a
	// final collect-and-export.
	metricsInterval time.Duration

	mtx      sync.Mutex
	resource *resource.Resource
	c        map[string]*grpc.ClientConn
	shutdown []providerShutdown
	// errorHandlerInstalled is set when init activated the otel error handler.
	errorHandlerInstalled bool
}

// providerShutdown is a provider's Shutdown, run by Close in ascending order.
type providerShutdown struct {
	name  string
	order int
	fn    func(context.Context) error
}

// Shutdown order: metrics first, since for one-shot runs the final metrics
// export happens only at shutdown, then traces, then logs.
const (
	shutdownOrderMetrics = iota
	shutdownOrderTraces
	shutdownOrderLogs
)

// newMetricExporter creates the OTLP metrics exporter; a variable so tests
// can make it fail.
var newMetricExporter = func(ctx context.Context, cc *grpc.ClientConn) (sdkmetric.Exporter, error) {
	return otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(cc))
}

// Option is a function that configures an otelConfig.
type Option func(*otelConfig)

// WithServiceName sets the service name for the OpenTelemetry configuration.
func WithServiceName(serviceName string) Option {
	return func(c *otelConfig) {
		c.serviceName = serviceName
	}
}

// WithInitialLogFields sets the log fields that will be added to all log messages.
func WithInitialLogFields(ilf map[string]interface{}) Option {
	return func(c *otelConfig) {
		c.initialLogFields = ilf
	}
}

// WithOtelEndpoint sets the endpoint and TLS certificate for both tracing and logging.
func WithOtelEndpoint(endpoint string, tlsCertPath string, tlsCert string) Option {
	return func(c *otelConfig) {
		c.endpoint = endpoint
		c.tlsCert = tlsCert
		c.tlsCertPath = tlsCertPath
		c.tlsInsecure = false
	}
}

// WithInsecureOtelEndpoint sets the endpoint for both tracing and logging with insecure connection.
func WithInsecureOtelEndpoint(endpoint string) Option {
	return func(c *otelConfig) {
		c.endpoint = endpoint
		c.tlsCert = ""
		c.tlsCertPath = ""
		c.tlsInsecure = true
	}
}

// WithTracingDisabled disables tracing.
func WithTracingDisabled() Option {
	return func(c *otelConfig) {
		c.tracingDisabled = true
	}
}

// WithLoggingDisabled disables logging.
func WithLoggingDisabled() Option {
	return func(c *otelConfig) {
		c.loggingDisabled = true
	}
}

// WithMetricsDisabled disables metrics export.
func WithMetricsDisabled() Option {
	return func(c *otelConfig) {
		c.metricsDisabled = true
	}
}

// WithMetricsExportInterval sets how often metrics are collected and exported.
// It takes precedence over OTEL_METRIC_EXPORT_INTERVAL. Without this option
// (or with a value <= 0) the otel SDK default applies: OTEL_METRIC_EXPORT_INTERVAL
// if set, otherwise 60s.
func WithMetricsExportInterval(d time.Duration) Option {
	return func(c *otelConfig) {
		if d > 0 {
			c.metricsInterval = d
		}
	}
}

// newConfig creates a new OpenTelemetry configuration with the given options.
func newConfig(opts ...Option) *otelConfig {
	cfg := &otelConfig{
		c: make(map[string]*grpc.ClientConn),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

func (c *otelConfig) init(ctx context.Context) (context.Context, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.endpoint == "" || (c.loggingDisabled && c.tracingDisabled && c.metricsDisabled) {
		zap.L().Debug("otel: no endpoint provided, skipping initialization")
		return ctx, nil
	}
	cc, err := c.getConnection()

	if err != nil {
		return nil, fmt.Errorf("otel: failed to create gRPC connection: %w", err)
	}

	if cc == nil {
		zap.L().Debug("otel: no endpoint provided, skipping initialization")
		return ctx, nil
	}

	installErrorHandler()
	c.errorHandlerInstalled = true

	if !c.loggingDisabled {
		ctx, err = c.initLogging(ctx, cc)
		if err != nil {
			return nil, fmt.Errorf("otel: failed to initialize logging: %w", err)
		}
	}

	if !c.tracingDisabled {
		ctx, err = c.initTracing(ctx, cc)
		if err != nil {
			return nil, fmt.Errorf("otel: failed to initialize tracing: %w", err)
		}
	}

	// Metrics are best effort: a failure leaves the global no-op MeterProvider
	// in place and the run continues with traces and logs.
	if !c.metricsDisabled {
		if err := c.initMetrics(ctx, cc); err != nil {
			zap.L().Warn("otel: failed to initialize metrics, continuing without them", zap.Error(err))
		}
	}
	return ctx, nil
}

// getConnection returns a gRPC connection for the given endpoint and TLS certificate
// If a connection already exists for the endpoint, it is reused
// precondition: c.mtx is locked
func (c *otelConfig) getConnection() (*grpc.ClientConn, error) {
	if c.endpoint == "" {
		return nil, fmt.Errorf("otel: endpoint is required")
	}

	if c.tlsCertPath != "" && c.tlsCert != "" {
		return nil, fmt.Errorf("otel: tlsCertPath and tlsCert are mutually exclusive, only one should be provided")
	}

	key := c.endpoint
	switch {
	case c.tlsCertPath != "":
		key = fmt.Sprintf("%s:path:%s", c.endpoint, c.tlsCertPath)
	case c.tlsCert != "":
		key = fmt.Sprintf("%s:cert:%s", c.endpoint, c.tlsCert)
	case c.tlsInsecure:
		key = fmt.Sprintf("%s:insecure", c.endpoint)
	}

	if conn, ok := c.c[key]; ok {
		return conn, nil
	}

	var conn *grpc.ClientConn
	var err error
	if c.tlsInsecure {
		conn, err = createInsecureGRPCConnection(c.endpoint)
	} else {
		conn, err = createGRPCConnection(c.endpoint, c.tlsCertPath, c.tlsCert)
	}

	if err != nil {
		return nil, err
	}

	c.c[key] = conn
	return conn, nil
}

func (c *otelConfig) getResource(ctx context.Context) (*resource.Resource, error) {
	if c.resource != nil {
		return c.resource, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(c.serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: failed to create otel resource: %w", err)
	}
	c.resource = res
	return res, nil
}

func (c *otelConfig) initTracing(ctx context.Context, cc *grpc.ClientConn) (context.Context, error) {
	res, err := c.getResource(ctx)
	if err != nil {
		return nil, err
	}

	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(cc))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	ssp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(ssp),
	)
	otel.SetTracerProvider(tracerProvider)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	zap.L().Debug("OpenTelemetry tracing enabled")

	c.shutdown = append(c.shutdown, providerShutdown{name: "traces", order: shutdownOrderTraces, fn: tracerProvider.Shutdown})
	// TODO(morgabra): Set tracer on ctx?
	return ctx, nil
}

// initMetrics sets up an otlp metrics exporter using an existing connection and
// resource, and installs it as the global MeterProvider. Instruments created
// from otel.Meter before this call (e.g. package-level meters) are delegated
// to the new provider by the otel global package.
func (c *otelConfig) initMetrics(ctx context.Context, cc *grpc.ClientConn) error {
	res, err := c.getResource(ctx)
	if err != nil {
		return err
	}

	// The exporter reads OTEL_EXPORTER_OTLP_(METRICS_)HEADERS even on a
	// shared connection.
	exp, err := newMetricExporter(ctx, cc)
	if err != nil {
		return fmt.Errorf("failed to create metric exporter: %w", err)
	}

	var readerOpts []sdkmetric.PeriodicReaderOption
	if c.metricsInterval > 0 {
		readerOpts = append(readerOpts, sdkmetric.WithInterval(c.metricsInterval))
	}
	reader := sdkmetric.NewPeriodicReader(exp, readerOpts...)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	)
	otel.SetMeterProvider(meterProvider)

	zap.L().Debug("OpenTelemetry metrics enabled")

	c.shutdown = append(c.shutdown, providerShutdown{name: "metrics", order: shutdownOrderMetrics, fn: meterProvider.Shutdown})
	return nil
}

// initLogging sets up a otlp logging exporter using an existing connection and resource.
// This replaces the current global zap logger with a new one that 'tees' into the otlp logging exporter, as such
// it is important to set up the zap logger (logging.Init()) before InitOtel().
func (c *otelConfig) initLogging(ctx context.Context, cc *grpc.ClientConn) (context.Context, error) {
	res, err := c.getResource(ctx)
	if err != nil {
		return nil, err
	}

	// TODO(morgabra): Whole bunch of tunables here...
	exp, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(cc))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize otlp exporter: %w", err)
	}
	// TODO(morgabra): Whole bunch of tunables _here_...
	processor := log.NewBatchProcessor(exp, log.WithExportInterval(time.Second*1))
	// TODO(morgabra): Whole bunch of tunables ALSO HERE...
	provider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(processor),
	)

	otelzapcore := otelzap.NewCore(c.serviceName, otelzap.WithVersion(sdk.Version), otelzap.WithLoggerProvider(provider))

	// Get the base logger's core to use as a level gate so that the otel core
	// doesn't enable levels (like Debug) that the base logger doesn't want.
	baseCore := zap.L().Core()
	filteredOtelCore := &levelFilteredCore{
		Core:         otelzapcore,
		levelEnabler: baseCore,
	}

	addOtel := zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return zapcore.NewTee(c, filteredOtelCore)
	})

	// NOTE(morgabra): InitialFields sent to zap.Build() for the base logger aren't accessible here, so if we also have
	// otel logging we just stomp over them with the same config.
	fields := make([]zap.Field, 0)
	for k, v := range c.initialLogFields {
		switch v := v.(type) {
		case string:
			fields = append(fields, zap.String(k, v))
		case int:
			fields = append(fields, zap.Int(k, v))
		default:
			fields = append(fields, zap.Any(k, v))
		}
	}

	l := zap.L().WithOptions(addOtel).With(fields...)
	zap.ReplaceGlobals(l)

	l.Debug("OpenTelemetry logging enabled")

	c.shutdown = append(c.shutdown, providerShutdown{name: "logs", order: shutdownOrderLogs, fn: processor.Shutdown})
	return ctxzap.ToContext(ctx, l), nil
}

// Close shuts down the meter, tracer and log providers in that order, which
// flushes what they hold, and then closes the connections they export over.
// When ctx has a deadline, each provider gets an equal share of the time left
// (unused time carries over), so a provider stuck on a slow collector cannot
// starve the ones after it.
func (c *otelConfig) Close(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	shutdowns := slices.Clone(c.shutdown)
	slices.SortStableFunc(shutdowns, func(a, b providerShutdown) int { return a.order - b.order })

	var errs []error
	for i, sd := range shutdowns {
		sdCtx, cancel := shutdownBudget(ctx, len(shutdowns)-i)
		if err := sd.fn(sdCtx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", sd.name, err))
		}
		cancel()
	}
	c.shutdown = nil

	for _, conn := range c.c {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	c.c = make(map[string]*grpc.ClientConn)

	if c.errorHandlerInstalled {
		uninstallErrorHandler()
		c.errorHandlerInstalled = false
	}

	err := errors.Join(errs...)
	if err != nil {
		return fmt.Errorf("otel: failed to close connections: %w", err)
	}
	return nil
}

// shutdownBudget returns a context for one of the remaining shutdown steps:
// an equal share of the time left before ctx's deadline, or ctx itself when
// it has none.
func shutdownBudget(ctx context.Context, remaining int) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok || remaining < 1 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, time.Until(deadline)/time.Duration(remaining))
}

// getTLSConfig creates a TLS configuration from a certificate file path or PEM-encoded certificate.
// If both tlsCertPath and tlsCert are empty, the system certificate pool is used.
// If both are provided, an error is returned as they are mutually exclusive.
func getTLSConfig(tlsCertPath, tlsCert string) (*tls.Config, error) {
	// Check if both parameters are provided
	if tlsCertPath != "" && tlsCert != "" {
		return nil, fmt.Errorf("tlsCertPath and tlsCert are mutually exclusive, only one should be provided")
	}

	// If both are empty, use the system certificate pool
	if tlsCertPath == "" && tlsCert == "" {
		zap.L().Debug("otel: no certificate provided, using system certificate pool")
		systemPool, err := x509.SystemCertPool()
		if err != nil || systemPool == nil {
			return nil, fmt.Errorf("failed to load system certificate pool: %w", err)
		}
		return &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    systemPool,
		}, nil
	}

	var certData []byte
	var err error

	// Check if tlsCertPath is provided
	if tlsCertPath != "" {
		zap.L().Debug("otel: using certificate from file", zap.String("path", tlsCertPath))
		certData, err = os.ReadFile(tlsCertPath)
		if err != nil {
			return nil, fmt.Errorf("otel: failed to read TLS certificate file: %w", err)
		}
	} else {
		zap.L().Debug("otel: using certificate from env")
		// Use tlsCert as PEM-encoded certificate

		decodedCert, err := base64.RawURLEncoding.DecodeString(tlsCert)
		if err != nil {
			return nil, fmt.Errorf("otel: failed to decode base64 TLS certificate: %w", err)
		}
		certData = decodedCert
	}

	// Create a certificate pool and add the certificate
	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(certData); !ok {
		return nil, fmt.Errorf("otel: failed to parse TLS certificate")
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    certPool,
	}, nil
}

// createGRPCConnection creates a gRPC connection with appropriate credentials.
func createGRPCConnection(endpoint, tlsCertPath, tlsCert string) (*grpc.ClientConn, error) {
	zap.L().Debug("otel: using collector", zap.String("endpoint", endpoint))

	tlsConfig, err := getTLSConfig(tlsCertPath, tlsCert)
	if err != nil {
		return nil, fmt.Errorf("otel: failed to create TLS config: %w", err)
	}

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: failed to create gRPC connection to collector: %w", err)
	}

	return conn, nil
}

// createInsecureGRPCConnection creates an insecure gRPC connection to the given endpoint.
func createInsecureGRPCConnection(endpoint string) (*grpc.ClientConn, error) {
	zap.L().Warn("otel: using INSECURE connection to collector", zap.String("endpoint", endpoint))
	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: failed to create insecure gRPC connection to collector: %w", err)
	}

	return conn, nil
}

// levelFilteredCore wraps a zapcore.Core and gates it with a LevelEnabler.
// This prevents the wrapped core from enabling levels that the base logger doesn't want,
// which is important when tee-ing cores together since NewTee enables a level if ANY core enables it.
type levelFilteredCore struct {
	zapcore.Core
	levelEnabler zapcore.LevelEnabler
}

func (c *levelFilteredCore) Enabled(lvl zapcore.Level) bool {
	return c.levelEnabler.Enabled(lvl)
}

func (c *levelFilteredCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.levelEnabler.Enabled(ent.Level) {
		return c.Core.Check(ent, ce)
	}
	return ce
}

func (c *levelFilteredCore) With(fields []zapcore.Field) zapcore.Core {
	return &levelFilteredCore{
		Core:         c.Core.With(fields),
		levelEnabler: c.levelEnabler,
	}
}
