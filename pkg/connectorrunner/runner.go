package connectorrunner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/durationpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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
	cw        types.ClientWrapper
	oneShot   bool
	tasks     tasks.Manager
	debugFile *os.File
}

var ErrSigTerm = errors.New("context cancelled by process shutdown")

// Run starts a connector and creates a new C1Z file.
func (c *connectorRunner) Run(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(ErrSigTerm)

	if c.tasks.ShouldDebug() && c.debugFile == nil {
		var err error
		tempDir := c.tasks.GetTempDir()
		if tempDir == "" {
			wd, err := os.Getwd()
			if err != nil {
				l.Warn("unable to get the current working directory", zap.Error(err))
			}

			if wd != "" {
				l.Warn("no temporal folder found on this system according to our task manager,"+
					" we may create files in the current working directory by mistake as a result",
					zap.String("current working directory", wd))
			} else {
				l.Warn("no temporal folder found on this system according to our task manager")
			}
		}
		debugFile := filepath.Join(tempDir, "debug.log")
		c.debugFile, err = os.Create(debugFile)
		if err != nil {
			l.Warn("cannot create file", zap.String("full file path", debugFile), zap.Error(err))
		}
	}

	// modify the context to insert a logger directed to a file
	if c.debugFile != nil {
		writeSyncer := zapcore.AddSync(c.debugFile)
		encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

		l = l.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			return zapcore.NewTee(c, core)
		}))

		ctx = ctxzap.ToContext(ctx, l)
	}

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

func (c *connectorRunner) backoff(_ context.Context, errCount int) time.Duration {
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
	stopForLoop := false
	var err error
	for !stopForLoop {
		select {
		case <-ctx.Done():
			return c.handleContextCancel(ctx)
		case <-time.After(waitDuration):
			l.Debug("runner: claiming worker")
			// Acquire a worker slot before we call Next() so we don't claim a task before we can actually process it.
			err = sem.Acquire(ctx, 1)
			if err != nil {
				l.Error("runner: error acquiring semaphore to claim worker", zap.Error(err))
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

			l.Debug("runner: got task", zap.String("task_id", nextTask.GetId()), zap.String("task_type", tasks.GetType(nextTask).String()))

			// If we're in one-shot mode, process the task synchronously.
			if c.oneShot {
				l.Debug("runner: one-shot mode enabled. Performing action synchronously.")
				err := c.processTask(ctx, nextTask)
				sem.Release(1)
				if err != nil {
					l.Error(
						"runner: error processing on-demand task",
						zap.Error(err),
						zap.String("task_id", nextTask.GetId()),
						zap.String("task_type", tasks.GetType(nextTask).String()),
					)
					return err
				}
				continue
			}

			// We got a task, so process it concurrently.
			go func(t *v1.Task) {
				l.Debug("runner: starting processing task", zap.String("task_id", t.GetId()), zap.String("task_type", tasks.GetType(t).String()))
				defer sem.Release(1)
				err := c.processTask(ctx, t)
				if err != nil {
					if strings.Contains(err.Error(), "grpc: the client connection is closing") {
						stopForLoop = true
					}
					l.Error("runner: error processing task", zap.Error(err), zap.String("task_id", t.GetId()), zap.String("task_type", tasks.GetType(t).String()))
					return
				}
				l.Debug("runner: task processed", zap.String("task_id", t.GetId()), zap.String("task_type", tasks.GetType(t).String()))
			}(nextTask)

			l.Debug("runner: dispatched task, waiting for next task", zap.Duration("wait_duration", waitDuration))
		}
	}

	if stopForLoop {
		return fmt.Errorf("unable to communicate with gRPC server")
	}

	return nil
}

func (c *connectorRunner) Close(ctx context.Context) error {
	var retErr error

	if err := c.cw.Close(); err != nil {
		retErr = errors.Join(retErr, err)
	}

	if c.debugFile != nil {
		if err := c.debugFile.Close(); err != nil {
			retErr = errors.Join(retErr, err)
		}
		c.debugFile = nil
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

type bulkCreateTicketConfig struct {
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
	login          string
	email          string
	profile        *structpb.Struct
	resourceTypeID string // Optional: if set, creates an account for the specified resource type.
}

type invokeActionConfig struct {
	action         string
	resourceTypeID string // Optional: if set, invokes a resource-scoped action
	args           *structpb.Struct
}

type listActionSchemasConfig struct {
	resourceTypeID string // Optional: filter by resource type
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
	feedId  string
	startAt time.Time
	cursor  string
}

type syncDifferConfig struct {
	baseSyncID    string
	appliedSyncID string
}

type syncCompactorConfig struct {
	filePaths  []string
	syncIDs    []string
	outputPath string
}

type runnerConfig struct {
	rlCfg                               *ratelimitV1.RateLimiterConfig
	rlDescriptors                       []*ratelimitV1.RateLimitDescriptors_Entry
	onDemand                            bool
	c1zPath                             string
	clientAuth                          bool
	clientID                            string
	clientSecret                        string
	provisioningEnabled                 bool
	ticketingEnabled                    bool
	actionsEnabled                      bool
	grantConfig                         *grantConfig
	revokeConfig                        *revokeConfig
	eventFeedConfig                     *eventStreamConfig
	tempDir                             string
	createAccountConfig                 *createAccountConfig
	invokeActionConfig                  *invokeActionConfig
	listActionSchemasConfig             *listActionSchemasConfig
	deleteResourceConfig                *deleteResourceConfig
	rotateCredentialsConfig             *rotateCredentialsConfig
	createTicketConfig                  *createTicketConfig
	bulkCreateTicketConfig              *bulkCreateTicketConfig
	listTicketSchemasConfig             *listTicketSchemasConfig
	getTicketConfig                     *getTicketConfig
	syncDifferConfig                    *syncDifferConfig
	syncCompactorConfig                 *syncCompactorConfig
	skipFullSync                        bool
	targetedSyncResourceIDs             []string
	externalResourceC1Z                 string
	externalResourceEntitlementIdFilter string
	skipEntitlementsAndGrants           bool
	skipGrants                          bool
	sessionStoreEnabled                 bool
	syncResourceTypeIDs                 []string
}

func WithSessionStoreEnabled() Option {
	return func(ctx context.Context, w *runnerConfig) error {
		w.sessionStoreEnabled = true
		return nil
	}
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
		w.rlCfg = ratelimitV1.RateLimiterConfig_builder{
			External: ratelimitV1.ExternalLimiter_builder{
				Address: address,
				Options: opts,
			}.Build(),
		}.Build()

		return nil
	}
}

// WithSlidingMemoryLimiter configures the connector to use an in-memory rate limiter that adjusts to maintain fairness
// based on request headers.
// `usePercent` is value between 0 and 100.
func WithSlidingMemoryLimiter(usePercent int64) Option {
	return func(ctx context.Context, w *runnerConfig) error {
		if usePercent < 0 || usePercent > 100 {
			return fmt.Errorf("usePercent must be between 0 and 100")
		}
		w.rlCfg = ratelimitV1.RateLimiterConfig_builder{
			SlidingMem: ratelimitV1.SlidingMemoryLimiter_builder{
				UsePercent: float64(usePercent) / 100.0,
			}.Build(),
		}.Build()

		return nil
	}
}

// WithFixedMemoryLimiter configures to use a fixed-memory limiter.
// `rate` is a number on how many times it should be in the given period executed.
// `period` represents the elapsed time between two instants as an int64 nanosecond count.
func WithFixedMemoryLimiter(rate int64, period time.Duration) Option {
	return func(ctx context.Context, w *runnerConfig) error {
		w.rlCfg = ratelimitV1.RateLimiterConfig_builder{
			FixedMem: ratelimitV1.FixedMemoryLimiter_builder{
				Rate:   rate,
				Period: durationpb.New(period),
			}.Build(),
		}.Build()

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

func WithOnDemandCreateAccount(c1zPath string, login string, email string, profile *structpb.Struct, resourceTypeId string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.createAccountConfig = &createAccountConfig{
			login:          login,
			email:          email,
			profile:        profile,
			resourceTypeID: resourceTypeId,
		}
		return nil
	}
}

// WithOnDemandInvokeAction creates an option for invoking an action.
// If resourceTypeID is provided, it invokes a resource-scoped action.
func WithOnDemandInvokeAction(c1zPath string, action string, resourceTypeID string, args *structpb.Struct) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.invokeActionConfig = &invokeActionConfig{
			action:         action,
			resourceTypeID: resourceTypeID,
			args:           args,
		}
		return nil
	}
}

// WithOnDemandListActionSchemas creates an option for listing action schemas.
// If resourceTypeID is provided, it filters schemas for that specific resource type.
func WithOnDemandListActionSchemas(c1zPath string, resourceTypeID string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.listActionSchemasConfig = &listActionSchemasConfig{
			resourceTypeID: resourceTypeID,
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

func WithOnDemandEventStream(feedId string, startAt time.Time, cursor string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.eventFeedConfig = &eventStreamConfig{
			feedId:  feedId,
			startAt: startAt,
			cursor:  cursor,
		}
		return nil
	}
}

func WithProvisioningEnabled() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.provisioningEnabled = true
		return nil
	}
}

func WithActionsEnabled() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.actionsEnabled = true
		return nil
	}
}

func WithFullSyncDisabled() Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.skipFullSync = true
		return nil
	}
}

func WithTargetedSyncResources(resourceIDs []string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.targetedSyncResourceIDs = resourceIDs
		return nil
	}
}

func WithSyncResourceTypeIDs(resourceTypeIDs []string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.syncResourceTypeIDs = resourceTypeIDs
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

func WithBulkCreateTicket(templatePath string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.bulkCreateTicketConfig = &bulkCreateTicketConfig{
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

func WithExternalResourceC1Z(externalResourceC1Z string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.externalResourceC1Z = externalResourceC1Z
		return nil
	}
}

func WithExternalResourceEntitlementFilter(entitlementId string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.externalResourceEntitlementIdFilter = entitlementId
		return nil
	}
}

func WithDiffSyncs(c1zPath string, baseSyncID string, newSyncID string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = c1zPath
		cfg.syncDifferConfig = &syncDifferConfig{
			baseSyncID:    baseSyncID,
			appliedSyncID: newSyncID,
		}
		return nil
	}
}

func WithSyncCompactor(outputPath string, filePaths []string, syncIDs []string) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.onDemand = true
		cfg.c1zPath = "dummy"

		cfg.syncCompactorConfig = &syncCompactorConfig{
			filePaths:  filePaths,
			syncIDs:    syncIDs,
			outputPath: outputPath,
		}
		return nil
	}
}

func WithSkipEntitlementsAndGrants(skip bool) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		cfg.skipEntitlementsAndGrants = skip
		return nil
	}
}

func WithSkipGrants(skip bool) Option {
	return func(ctx context.Context, cfg *runnerConfig) error {
		if skip && len(cfg.targetedSyncResourceIDs) == 0 {
			return fmt.Errorf("skip-grants can only be set within a targeted sync")
		}
		cfg.skipGrants = skip
		return nil
	}
}

func IsSessionStoreEnabled(ctx context.Context, options ...Option) (bool, error) {
	cfg := &runnerConfig{}

	for _, o := range options {
		err := o(ctx, cfg)
		if err != nil {
			return false, err
		}
	}

	return cfg.sessionStoreEnabled, nil
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

	if len(cfg.targetedSyncResourceIDs) > 0 {
		wrapperOpts = append(wrapperOpts, connector.WithTargetedSyncResources(cfg.targetedSyncResourceIDs))
	}

	if cfg.sessionStoreEnabled {
		wrapperOpts = append(wrapperOpts, connector.WithSessionStoreEnabled())
	}

	if len(cfg.syncResourceTypeIDs) > 0 {
		wrapperOpts = append(wrapperOpts, connector.WithSyncResourceTypeIDs(cfg.syncResourceTypeIDs))
	}

	cw, err := connector.NewWrapper(ctx, c, wrapperOpts...)
	if err != nil {
		return nil, err
	}

	resources := make([]*v2.Resource, 0, len(cfg.targetedSyncResourceIDs))
	for _, resourceId := range cfg.targetedSyncResourceIDs {
		r, err := bid.ParseResourceBid(resourceId)
		if err != nil {
			return nil, err
		}
		resources = append(resources, r)
	}

	runner.cw = cw

	if cfg.onDemand {
		if cfg.c1zPath == "" &&
			cfg.eventFeedConfig == nil &&
			cfg.createTicketConfig == nil &&
			cfg.listTicketSchemasConfig == nil &&
			cfg.getTicketConfig == nil &&
			cfg.bulkCreateTicketConfig == nil &&
			cfg.listActionSchemasConfig == nil {
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
			tm = local.NewCreateAccountManager(ctx, cfg.c1zPath, cfg.createAccountConfig.login, cfg.createAccountConfig.email, cfg.createAccountConfig.profile, cfg.createAccountConfig.resourceTypeID)

		case cfg.invokeActionConfig != nil:
			tm = local.NewActionInvoker(ctx, cfg.c1zPath, cfg.invokeActionConfig.action, cfg.invokeActionConfig.resourceTypeID, cfg.invokeActionConfig.args)

		case cfg.listActionSchemasConfig != nil:
			tm = local.NewListActionSchemas(ctx, cfg.listActionSchemasConfig.resourceTypeID)

		case cfg.deleteResourceConfig != nil:
			tm = local.NewResourceDeleter(ctx, cfg.c1zPath, cfg.deleteResourceConfig.resourceId, cfg.deleteResourceConfig.resourceType)

		case cfg.rotateCredentialsConfig != nil:
			tm = local.NewCredentialRotator(ctx, cfg.c1zPath, cfg.rotateCredentialsConfig.resourceId, cfg.rotateCredentialsConfig.resourceType)

		case cfg.eventFeedConfig != nil:
			tm = local.NewEventFeed(ctx, cfg.eventFeedConfig.feedId, cfg.eventFeedConfig.startAt, cfg.eventFeedConfig.cursor)
		case cfg.createTicketConfig != nil:
			tm = local.NewTicket(ctx, cfg.createTicketConfig.templatePath)
		case cfg.listTicketSchemasConfig != nil:
			tm = local.NewListTicketSchema(ctx)
		case cfg.getTicketConfig != nil:
			tm = local.NewGetTicket(ctx, cfg.getTicketConfig.ticketID)
		case cfg.bulkCreateTicketConfig != nil:
			tm = local.NewBulkTicket(ctx, cfg.bulkCreateTicketConfig.templatePath)
		case cfg.syncDifferConfig != nil:
			tm = local.NewDiffer(ctx, cfg.c1zPath, cfg.syncDifferConfig.baseSyncID, cfg.syncDifferConfig.appliedSyncID)
		case cfg.syncCompactorConfig != nil:
			c := cfg.syncCompactorConfig
			if len(c.filePaths) != len(c.syncIDs) {
				return nil, errors.New("sync-compactor: must include exactly one syncID per file")
			}
			configs := make([]*synccompactor.CompactableSync, 0, len(c.filePaths))
			for i, filePath := range c.filePaths {
				configs = append(configs, &synccompactor.CompactableSync{
					FilePath: filePath,
					SyncID:   c.syncIDs[i],
				})
			}
			tm = local.NewLocalCompactor(ctx, cfg.syncCompactorConfig.outputPath, configs)
		default:
			tm, err = local.NewSyncer(ctx, cfg.c1zPath,
				local.WithTmpDir(cfg.tempDir),
				local.WithExternalResourceC1Z(cfg.externalResourceC1Z),
				local.WithExternalResourceEntitlementIdFilter(cfg.externalResourceEntitlementIdFilter),
				local.WithTargetedSyncResources(resources),
				local.WithSkipEntitlementsAndGrants(cfg.skipEntitlementsAndGrants),
				local.WithSkipGrants(cfg.skipGrants),
				local.WithSyncResourceTypeIDs(cfg.syncResourceTypeIDs),
			)
			if err != nil {
				return nil, err
			}
		}

		runner.tasks = tm

		runner.oneShot = true
		return runner, nil
	}

	tm, err := c1api.NewC1TaskManager(ctx,
		cfg.clientID,
		cfg.clientSecret,
		cfg.tempDir,
		cfg.skipFullSync,
		cfg.externalResourceC1Z,
		cfg.externalResourceEntitlementIdFilter,
		resources,
		cfg.syncResourceTypeIDs,
	)
	if err != nil {
		return nil, err
	}
	runner.tasks = tm

	return runner, nil
}
