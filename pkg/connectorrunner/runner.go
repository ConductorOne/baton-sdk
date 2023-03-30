package connectorrunner

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/tasks/c1_manager"
	"github.com/conductorone/baton-sdk/pkg/tasks/local_syncer"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conductorone/baton-sdk/internal/connector"
)

type connectorRunner struct {
	cw           types.ClientWrapper
	onDemandMode bool
	tasks        tasks.Manager
}

var ErrSigTerm = errors.New("context cancelled by process shutdown")

// Run starts a connector and creates a new C1Z file.
func (c *connectorRunner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(ErrSigTerm)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		for range sigChan {
			cancel(ErrSigTerm)
		}
	}()

	err := c.run(ctx)
	if err != nil {
		return err
	}

	err = c.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *connectorRunner) handleContextCancel(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	err := context.Cause(ctx)
	if err == nil {
		return nil
	}

	// The context was cancelled due to an expected end of the process. Swallow the error.
	if errors.Is(err, ErrSigTerm) {
		return nil
	}

	l.Debug("unexpected context cancellation", zap.Error(err))
	return err
}

func (c *connectorRunner) run(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	var waitDuration time.Duration
	var nextTask tasks.Task
	var err error
	for {
		if waitDuration > 0 {
			l.Debug("waiting for next task...", zap.Duration("wait_duration", waitDuration))
		}
		select {
		case <-ctx.Done():
			return c.handleContextCancel(ctx)
		case <-time.After(waitDuration):
		}

		nextTask, waitDuration, err = c.tasks.Next(ctx)
		if err != nil {
			l.Error("error getting next task", zap.Error(err))
			continue
		}

		if nextTask == nil {
			if c.onDemandMode {
				return nil
			}
			continue
		}

		cc, err := c.cw.C(ctx)
		if err != nil {
			l.Error("error getting client", zap.Error(err))
			continue
		}
		err = c.tasks.Run(ctx, nextTask, cc)
		if err != nil {
			l.Error("error running task", zap.Error(err))
			continue
		}
	}
}

func (c *connectorRunner) Close(ctx context.Context) error {
	return nil
}

type Option func(ctx context.Context, cfg *runnerConfig) error

type runnerConfig struct {
	rlCfg         *ratelimitV1.RateLimiterConfig
	rlDescriptors []*ratelimitV1.RateLimitDescriptors_Entry
	onDemandSync  bool
	c1zPath       string
	clientAuth    bool
	clientID      string
	clientSecret  string
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

func WithClientCredentials(clientID string, clientSecret string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.clientID = clientID
		cfg.clientSecret = clientSecret
		cfg.clientAuth = true
		return nil
	}
}

func WithOnDemandSync(c1zPath string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemandSync = true
		cfg.c1zPath = c1zPath
		return nil
	}
}

// NewConnectorRunner creates a new connector runner.
func NewConnectorRunner(ctx context.Context, c types.ConnectorServer, opts ...Option) (*connectorRunner, error) {
	runner := &connectorRunner{}
	cfg := &runnerConfig{}

	for _, o := range opts {
		err := o(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}

	if cfg.onDemandSync {
		if cfg.c1zPath == "" {
			return nil, errors.New("c1zPath must be set when using onDemandSync")
		}
		tm, err := local_syncer.New(ctx, tasks.NewLocalFileSyncTask(cfg.c1zPath))
		if err != nil {
			return nil, err
		}
		runner.tasks = tm

		runner.onDemandMode = true
	} else {
		tm, err := c1_manager.NewC1TaskManager(ctx, cfg.clientID, cfg.clientSecret)
		if err != nil {
			return nil, err
		}
		runner.tasks = tm
	}

	wrapperOpts := []connector.Option{}
	wrapperOpts = append(wrapperOpts, connector.WithRateLimiterConfig(cfg.rlCfg))

	for _, d := range cfg.rlDescriptors {
		wrapperOpts = append(wrapperOpts, connector.WithRateLimitDescriptor(d))
	}

	cw, err := connector.NewWrapper(ctx, c, wrapperOpts...)
	if err != nil {
		return nil, err
	}

	runner.cw = cw

	return runner, nil
}
