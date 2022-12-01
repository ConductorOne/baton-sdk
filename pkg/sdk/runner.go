package sdk

import (
	"context"
	"os"
	"os/signal"
	"time"

	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conductorone/baton-sdk/internal/connector"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

type connectorRunner struct {
	syncer  sync.Syncer
	store   connectorstore.Writer
	manager manager.Manager
}

func (c *connectorRunner) shutdown(ctx context.Context) error {
	logger := ctxzap.Extract(ctx)

	err := c.Close()
	if err != nil {
		// Explicitly ignoring the error here as it is possible that things have already been closed.
		logger.Error("error closing connector runner", zap.Error(err))
	}

	err = c.manager.SaveC1Z(ctx)
	if err != nil {
		logger.Error("error saving c1z", zap.Error(err))
		return err
	}

	err = c.manager.Close(ctx)
	if err != nil {
		logger.Error("error closing c1z manager", zap.Error(err))
		return err
	}

	return nil
}

// Run starts a connector and creates a new C1Z file.
func (c *connectorRunner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func(c *connectorRunner, ctx context.Context) {
		err := c.shutdown(ctx)
		if err != nil {
			ctxzap.Extract(ctx).Error("error shutting down", zap.Error(err))
		}
	}(c, ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		for range sigChan {
			cancel()
		}
	}()

	err := c.syncer.Sync(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *connectorRunner) Close() error {
	err := c.syncer.Close()
	if err != nil {
		return err
	}

	return nil
}

type Option func(ctx context.Context, cfg *runnerConfig) error

type runnerConfig struct {
	rlCfg         *ratelimitV1.RateLimiterConfig
	rlDescriptors []*ratelimitV1.RateLimitDescriptors_Entry
}

// WithRateLimiterConfig sets the RateLimiterConfig for a runner.
func WithRateLimiterConfig(cfg *ratelimitV1.RateLimiterConfig) Option {
	return func(ctx context.Context, w *runnerConfig) error {
		if cfg != nil {
			w.rlCfg = cfg
		}

		return nil
	}
}

// WithExternalLimiter configures the connector to use an external rate limiter.
// The `opts` map is injected into the environment in order for the service to be configured.
func WithExternalLimiter(address string, opts map[string]string) Option {
	return func(ctx context.Context, w *runnerConfig) error {
		w.rlCfg = &ratelimitV1.RateLimiterConfig{
			Type: &ratelimitV1.RateLimiterConfig_External{
				External: &ratelimitV1.ExternalLimiter{
					Address: address,
					Options: opts,
				},
			},
		}

		return nil
	}
}

// WithSlidingMemoryLimiter configures the connector to use an in-memory rate limiter that adjusts to maintain fairness
// based on request headers.
// `usePercent` is value between 0 and 100.
func WithSlidingMemoryLimiter(usePercent int64) Option {
	return func(ctx context.Context, w *runnerConfig) error {
		w.rlCfg = &ratelimitV1.RateLimiterConfig{
			Type: &ratelimitV1.RateLimiterConfig_SlidingMem{
				SlidingMem: &ratelimitV1.SlidingMemoryLimiter{
					UsePercent: float64(usePercent / 100),
				},
			},
		}

		return nil
	}
}

// WithFixedMemoryLimiter configures to use a fixed-memory limiter.
// `rate` is a number on how many times it should be in the given period executed.
// `period` represents the elapsed time between two instants as an int64 nanosecond count.
func WithFixedMemoryLimiter(rate int64, period time.Duration) Option {
	return func(ctx context.Context, w *runnerConfig) error {
		w.rlCfg = &ratelimitV1.RateLimiterConfig{
			Type: &ratelimitV1.RateLimiterConfig_FixedMem{
				FixedMem: &ratelimitV1.FixedMemoryLimiter{
					Rate:   rate,
					Period: durationpb.New(period),
				},
			},
		}

		return nil
	}
}

// WithRateLimitDescriptor takes a rate limit descriptor and adds it to the list of rate limit descriptors.
func WithRateLimitDescriptor(entry *ratelimitV1.RateLimitDescriptors_Entry) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		if entry != nil {
			cfg.rlDescriptors = append(cfg.rlDescriptors, entry)
		}

		return nil
	}
}

// NewConnectorRunner creates a new connector runner.
func NewConnectorRunner(ctx context.Context, c types.ConnectorServer, dbPath string, opts ...Option) (*connectorRunner, error) {
	runner := &connectorRunner{}
	cfg := &runnerConfig{}

	for _, o := range opts {
		err := o(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}

	m, err := manager.New(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	runner.manager = m

	store, err := runner.manager.LoadC1Z(ctx)
	if err != nil {
		return nil, err
	}
	runner.store = store

	wrapperOpts := []connector.Option{}
	wrapperOpts = append(wrapperOpts, connector.WithRateLimiterConfig(cfg.rlCfg))

	for _, d := range cfg.rlDescriptors {
		wrapperOpts = append(wrapperOpts, connector.WithRateLimitDescriptor(d))
	}

	cw, err := connector.NewWrapper(ctx, c, wrapperOpts...)
	if err != nil {
		return nil, err
	}

	runner.syncer = sync.NewSyncer(store, cw)

	return runner, nil
}
