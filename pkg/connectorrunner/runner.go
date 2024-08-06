package connectorrunner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	ratelimitV1 "github.com/conductorone/baton-sdk/pb/c1/ratelimit/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/tasks/c1api"
	"github.com/conductorone/baton-sdk/pkg/tasks/local"
	"github.com/conductorone/baton-sdk/pkg/types"

	"github.com/conductorone/baton-sdk/internal/connector"
)

const (
	// taskConcurrency configures how many tasks we run concurrently.
	taskConcurrency = 3
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

	l.Debug("runner: unexpected context cancellation", zap.Error(err))
	return err
}
func (c *connectorRunner) processTask(ctx context.Context, task *v1.Task) error {
	cc, err := c.cw.C(ctx)
	if err != nil {
		return fmt.Errorf("runner: error creating connector client: %w", err)
	}

	err = c.tasks.Process(ctx, task, cc)
	if err != nil {
		return fmt.Errorf("runner: error processing task: %w", err)
	}

	return nil
}

func (c *connectorRunner) backoff(ctx context.Context, errCount int) time.Duration {
	waitDuration := time.Duration(errCount*errCount) * time.Second
	if waitDuration > time.Minute {
		waitDuration = time.Minute
	}
	return waitDuration
}

func (c *connectorRunner) run(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	sem := semaphore.NewWeighted(int64(taskConcurrency))

	waitDuration := time.Second * 0
	errCount := 0
	var err error
	for {
		select {
		case <-ctx.Done():
			return c.handleContextCancel(ctx)
		case <-time.After(waitDuration):
			l.Debug("runner: claiming worker")
			// Acquire a worker slot before we call Next() so we don't claim a task before we can actually process it.
			err = sem.Acquire(ctx, 1)
			if err != nil {
				// Any error returned from Acquire() is due to the context being cancelled.
				sem.Release(1)
				return c.handleContextCancel(ctx)
			}
			l.Debug("runner: worker claimed, checking for next task")

			// Fetch the next task.
			nextTask, nextWaitDuration, err := c.tasks.Next(ctx)
			if err != nil {
				// TODO(morgabra) Use a library with jitter for this?
				errCount++
				waitDuration = c.backoff(ctx, errCount)
				l.Error("runner: error getting next task", zap.Error(err), zap.Int("err_count", errCount), zap.Duration("wait_duration", waitDuration))
				sem.Release(1)
				continue
			}

			errCount = 0
			waitDuration = nextWaitDuration

			// nil tasks mean there are no tasks to process.
			if nextTask == nil {
				sem.Release(1)
				l.Debug("runner: no tasks to process", zap.Duration("wait_duration", waitDuration))
				if c.oneShot {
					l.Debug("runner: one-shot mode enabled. Exiting.")
					return nil
				}
				continue
			}

			l.Debug("runner: got task", zap.String("task_id", nextTask.Id), zap.String("task_type", tasks.GetType(nextTask).String()))

			// If we're in one-shot mode, process the task synchronously.
			if c.oneShot {
				l.Debug("runner: one-shot mode enabled. Performing action synchronously.")
				err := c.processTask(ctx, nextTask)
				sem.Release(1)
				if err != nil {
					l.Error(
						"runner: error processing on-demand task",
						zap.Error(err),
						zap.String("task_id", nextTask.Id),
						zap.String("task_type", tasks.GetType(nextTask).String()),
					)
					return err
				}
				continue
			}

			// We got a task, so process it concurrently.
			go func(t *v1.Task) {
				l.Debug("runner: starting processing task", zap.String("task_id", t.Id), zap.String("task_type", tasks.GetType(t).String()))
				defer sem.Release(1)
				err := c.processTask(ctx, t)
				if err != nil {
					l.Error("runner: error processing task", zap.Error(err), zap.String("task_id", t.Id), zap.String("task_type", tasks.GetType(t).String()))
				}
				l.Debug("runner: task processed", zap.String("task_id", t.Id), zap.String("task_type", tasks.GetType(t).String()))
			}(nextTask)

			l.Debug("runner: dispatched task, waiting for next task", zap.Duration("wait_duration", waitDuration))
		}
	}
}

func (c *connectorRunner) Close(ctx context.Context) error {
	var retErr error

	if err := c.cw.Close(); err != nil {
		retErr = errors.Join(retErr, err)
	}

	return retErr
}

type Option func(ctx context.Context, cfg *runnerConfig) error

type getTicketConfig struct {
	ticketID string
}

type listTicketSchemasConfig struct{}

type createTicketConfig struct {
	templatePath string
}

type grantConfig struct {
	entitlementID string
	principalType string
	principalID   string
}

type revokeConfig struct {
	grantID string
}

type createAccountConfig struct {
	login string
	email string
}

type deleteResourceConfig struct {
	resourceId   string
	resourceType string
}

type rotateCredentialsConfig struct {
	resourceId   string
	resourceType string
}

type eventStreamConfig struct {
}

type runnerConfig struct {
	rlCfg                   *ratelimitV1.RateLimiterConfig
	rlDescriptors           []*ratelimitV1.RateLimitDescriptors_Entry
	onDemand                bool
	c1zPath                 string
	clientAuth              bool
	clientID                string
	clientSecret            string
	provisioningEnabled     bool
	ticketingEnabled        bool
	grantConfig             *grantConfig
	revokeConfig            *revokeConfig
	eventFeedConfig         *eventStreamConfig
	tempDir                 string
	createAccountConfig     *createAccountConfig
	deleteResourceConfig    *deleteResourceConfig
	rotateCredentialsConfig *rotateCredentialsConfig
	createTicketConfig      *createTicketConfig
	listTicketSchemasConfig *listTicketSchemasConfig
	getTicketConfig         *getTicketConfig
	skipFullSync            bool
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

func WithOnDemandCreateAccount(c1zPath string, login string, email string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.createAccountConfig = &createAccountConfig{
			login: login,
			email: email,
		}
		return nil
	}
}

func WithOnDemandDeleteResource(c1zPath string, resourceId string, resourceType string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.deleteResourceConfig = &deleteResourceConfig{
			resourceId:   resourceId,
			resourceType: resourceType,
		}
		return nil
	}
}

func WithOnDemandRotateCredentials(c1zPath string, resourceId string, resourceType string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.rotateCredentialsConfig = &rotateCredentialsConfig{
			resourceId:   resourceId,
			resourceType: resourceType,
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
func WithOnDemandEventStream() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.eventFeedConfig = &eventStreamConfig{}
		return nil
	}
}

func WithProvisioningEnabled() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.provisioningEnabled = true
		return nil
	}
}

func WithFullSyncDisabled() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.skipFullSync = true
		return nil
	}
}

func WithTicketingEnabled() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.ticketingEnabled = true
		return nil
	}
}

func WithCreateTicket(templatePath string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.createTicketConfig = &createTicketConfig{
			templatePath: templatePath,
		}
		return nil
	}
}

func WithListTicketSchemas() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.listTicketSchemasConfig = &listTicketSchemasConfig{}
		return nil
	}
}

func WithGetTicket(ticketID string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.getTicketConfig = &getTicketConfig{
			ticketID: ticketID,
		}
		return nil
	}
}

func WithTempDir(tempDir string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.tempDir = tempDir
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

	if cfg.ticketingEnabled {
		wrapperOpts = append(wrapperOpts, connector.WithTicketingEnabled())
	}

	if cfg.skipFullSync {
		wrapperOpts = append(wrapperOpts, connector.WithFullSyncDisabled())
	}

	cw, err := connector.NewWrapper(ctx, c, wrapperOpts...)
	if err != nil {
		return nil, err
	}

	runner.cw = cw

	if cfg.onDemand {
		if cfg.c1zPath == "" && cfg.eventFeedConfig == nil && cfg.createTicketConfig == nil && cfg.listTicketSchemasConfig == nil && cfg.getTicketConfig == nil {
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

		case cfg.createAccountConfig != nil:
			tm = local.NewCreateAccountManager(ctx, cfg.c1zPath, cfg.createAccountConfig.login, cfg.createAccountConfig.email)

		case cfg.deleteResourceConfig != nil:
			tm = local.NewResourceDeleter(ctx, cfg.c1zPath, cfg.deleteResourceConfig.resourceId, cfg.deleteResourceConfig.resourceType)

		case cfg.rotateCredentialsConfig != nil:
			tm = local.NewCredentialRotator(ctx, cfg.c1zPath, cfg.rotateCredentialsConfig.resourceId, cfg.rotateCredentialsConfig.resourceType)

		case cfg.eventFeedConfig != nil:
			tm = local.NewEventFeed(ctx)
		case cfg.createTicketConfig != nil:
			tm = local.NewTicket(ctx, cfg.createTicketConfig.templatePath)
		case cfg.listTicketSchemasConfig != nil:
			tm = local.NewListTicketSchema(ctx)
		case cfg.getTicketConfig != nil:
			tm = local.NewGetTicket(ctx, cfg.getTicketConfig.ticketID)
		default:
			tm, err = local.NewSyncer(ctx, cfg.c1zPath, local.WithTmpDir(cfg.tempDir))
			if err != nil {
				return nil, err
			}
		}

		runner.tasks = tm

		runner.oneShot = true
		return runner, nil
	}

	tm, err := c1api.NewC1TaskManager(ctx, cfg.clientID, cfg.clientSecret, cfg.tempDir, cfg.skipFullSync)
	if err != nil {
		return nil, err
	}
	runner.tasks = tm

	return runner, nil
}
