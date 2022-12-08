package connectorrunner

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner/tasks"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conductorone/baton-sdk/internal/connector"
)

type connectorRunner struct {
	cw           types.ClientWrapper
	dbPath       string
	onDemandMode bool
	tasks        tasks.Manager
}

// Run starts a connector and creates a new C1Z file.
func (c *connectorRunner) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		for range sigChan {
			cancel()
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

func (c *connectorRunner) run(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	l.Info("waiting for work....")

	var done bool

	for !done {
		select {
		case <-ctx.Done():
			l.Info("context done - exiting.", zap.Error(ctx.Err()))
			return ctx.Err()
		default:
		}

		nextTask, err := c.tasks.Next(ctx)
		if err != nil {
			l.Error("error getting next task", zap.Error(err))
			continue
		}

		// No work for us to do, go to sleep for a bit and try again
		if nextTask == nil {
			l.Debug("no available tasks, going to sleep for 5 seconds")
			<-time.After(time.Second * 5)
			continue
		}

		switch nextTask.(type) {
		case *tasks.SyncTask:
			client, err := c.cw.C(ctx)
			if err != nil {
				return err
			}

			syncer, err := sdkSync.NewSyncer(ctx, client, c.dbPath)
			if err != nil {
				return err
			}

			err = syncer.Sync(ctx)
			if err != nil {
				return err
			}

			err = syncer.Close(ctx)
			if err != nil {
				return err
			}

			err = c.tasks.Finish(ctx, nextTask.GetTaskId())
			if err != nil {
				return err
			}

			if c.onDemandMode {
				done = true
			}

		default:
			l.Debug("unknown task type, going to sleep for 10 seconds")
			<-time.After(time.Second * 10)
			continue
		}
	}

	return nil
}

func (c *connectorRunner) Close(ctx context.Context) error {
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
func NewConnectorRunner(ctx context.Context, c1zPath string, onDemandSync bool, c types.ConnectorServer, opts ...Option) (*connectorRunner, error) {
	runner := &connectorRunner{}
	cfg := &runnerConfig{}

	for _, o := range opts {
		err := o(ctx, cfg)
		if err != nil {
			return nil, err
		}
	}

	tm, err := tasks.NewNaiveManager(ctx)
	if err != nil {
		return nil, err
	}
	runner.tasks = tm

	if c1zPath == "" {
		return nil, fmt.Errorf("connector-runner: must provide a c1z path to sync with")
	}
	runner.dbPath = c1zPath

	if onDemandSync {
		runner.onDemandMode = true
		err = runner.tasks.Add(ctx, tasks.NewSyncTask())
		if err != nil {
			return nil, err
		}
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
