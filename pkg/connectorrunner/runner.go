package connectorrunner

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/tasks/c1api"
	"github.com/conductorone/baton-sdk/pkg/tasks/local"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conductorone/baton-sdk/internal/connector"
)

type connectorRunner struct {
	cw      types.ClientWrapper
	oneShot bool
	tasks   tasks.Manager
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
	var nextTask *v1.Task
	var err error
	for {
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
			l.Info("No tasks available. Waiting to check again.", zap.Duration("wait_duration", waitDuration))
			if c.oneShot {
				l.Debug("One-shot mode enabled. Exiting.")
				return nil
			}
			continue
		}

		cc, err := c.cw.C(ctx)
		if err != nil {
			l.Error("error getting connector client", zap.Error(err), zap.String("task_id", nextTask.GetId()))
			continue
		}

		err = c.tasks.Process(ctx, nextTask, cc)
		if err != nil {
			l.Error("error processing task", zap.Error(err), zap.String("task_id", nextTask.GetId()))
			continue
		}

		l.Info("Task complete! Waiting before checking for more tasks...", zap.Duration("wait_duration", waitDuration))
	}
}

func (c *connectorRunner) Close(ctx context.Context) error {
	return nil
}

type Option func(ctx context.Context, cfg *runnerConfig) error

type grantConfig struct {
	entitlementID string
	principalType string
	principalID   string
}

type revokeConfig struct {
	grantID string
}

type runnerConfig struct {
	rlCfg               *ratelimitV1.RateLimiterConfig
	rlDescriptors       []*ratelimitV1.RateLimitDescriptors_Entry
	onDemand            bool
	c1zPath             string
	clientAuth          bool
	clientID            string
	clientSecret        string
	provisioningEnabled bool
	grantConfig         *grantConfig
	revokeConfig        *revokeConfig
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

func WithOnDemandGrant(c1zPath string, entitlementID string, principalID string, principalType string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.grantConfig = &grantConfig{
			entitlementID: entitlementID,
			principalID:   principalID,
			principalType: principalType,
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

func WithOnDemandRevoke(c1zPath string, grantID string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.revokeConfig = &revokeConfig{
			grantID: grantID,
		}
		return nil
	}
}

func WithOnDemandSync(c1zPath string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		return nil
	}
}

func WithProvisioningEnabled() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.provisioningEnabled = true
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

	var wrapperOpts []connector.Option
	wrapperOpts = append(wrapperOpts, connector.WithRateLimiterConfig(cfg.rlCfg))

	for _, d := range cfg.rlDescriptors {
		wrapperOpts = append(wrapperOpts, connector.WithRateLimitDescriptor(d))
	}

	if cfg.provisioningEnabled {
		wrapperOpts = append(wrapperOpts, connector.WithProvisioningEnabled())
	}

	cw, err := connector.NewWrapper(ctx, c, wrapperOpts...)
	if err != nil {
		return nil, err
	}

	runner.cw = cw

	if cfg.onDemand {
		if cfg.c1zPath == "" {
			return nil, errors.New("c1zPath must be set when in on-demand mode")
		}

		var tm tasks.Manager
		switch {
		case cfg.grantConfig != nil:
			tm = local.NewGranter(
				ctx,
				cfg.c1zPath,
				cfg.grantConfig.entitlementID,
				cfg.grantConfig.principalID,
				cfg.grantConfig.principalType,
			)

		case cfg.revokeConfig != nil:
			tm = local.NewRevoker(ctx, cfg.c1zPath, cfg.revokeConfig.grantID)

		default:
			tm, err = local.NewSyncer(ctx, cfg.c1zPath)
			if err != nil {
				return nil, err
			}
		}

		runner.tasks = tm

		runner.oneShot = true
		return runner, nil
	}

	tm, err := c1api.NewC1TaskManager(ctx, cfg.clientID, cfg.clientSecret)
	if err != nil {
		return nil, err
	}
	runner.tasks = tm

	return runner, nil
}
