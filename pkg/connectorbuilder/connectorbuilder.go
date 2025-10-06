package connectorbuilder

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/retry"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

var tracer = otel.Tracer("baton-sdk/pkg.connectorbuilder")

type Opt func(b *builderImpl, cb interface{}) error

func WithSessionStore(ss types.SessionStore) Opt {
	return func(b *builderImpl, cb interface{}) error {
		b.sessionStore = ss
		return nil
	}
}

func WithTicketingEnabled() Opt {
	return func(b *builderImpl, cb interface{}) error {
		if _, ok := cb.(TicketManager); ok {
			b.ticketingEnabled = true
			return nil
		}
		return errors.New("external ticketing not supported")
	}
}

func WithMetricsHandler(h metrics.Handler) Opt {
	return func(b *builderImpl, cb interface{}) error {
		b.m = metrics.New(h)
		return nil
	}
}

func (b *builderImpl) options(cb interface{}, opts ...Opt) error {
	for _, opt := range opts {
		if err := opt(b, cb); err != nil {
			return err
		}
	}

	return nil
}

type builderImpl struct {
	m                *metrics.M
	sessionStore     types.SessionStore
	nowFunc          func() time.Time
	clientSecret     *jose.JSONWebKey
	ticketingEnabled bool

	metadataProvider        MetadataProvider
	validateProvider        ValidateProvider
	accountManager          AccountManager
	actionManager           CustomActionManager
	ticketManager           TicketManager
	resourceBuilders        map[string]ResourceSyncer2
	resourceProvisioners    map[string]ResourceProvisionerV2
	resourceManagers        map[string]ResourceManagerV2
	resourceDeleters        map[string]ResourceDeleterV2
	resourceTargetedSyncers map[string]ResourceTargetedSyncer
	credentialManagers      map[string]CredentialManager
	eventFeeds              map[string]EventFeed
}

func NewConnector(ctx context.Context, in interface{}, opts ...Opt) (types.ConnectorServer, error) {
	if cs, ok := in.(types.ConnectorServer); ok {
		return cs, nil
	}

	clientSecretValue := ctx.Value(crypto.ContextClientSecretKey)
	clientSecretJWK, _ := clientSecretValue.(*jose.JSONWebKey)

	b := &builderImpl{
		metadataProvider:        nil,
		validateProvider:        nil,
		ticketManager:           nil,
		accountManager:          nil,
		actionManager:           nil,
		nowFunc:                 time.Now,
		clientSecret:            clientSecretJWK,
		resourceBuilders:        make(map[string]ResourceSyncer2),
		resourceProvisioners:    make(map[string]ResourceProvisionerV2),
		resourceManagers:        make(map[string]ResourceManagerV2),
		resourceDeleters:        make(map[string]ResourceDeleterV2),
		resourceTargetedSyncers: make(map[string]ResourceTargetedSyncer),
		credentialManagers:      make(map[string]CredentialManager),
		eventFeeds:              make(map[string]EventFeed), // feed id -> event feed
	}

	err := b.options(in, opts...)
	if err != nil {
		return nil, err
	}

	if b.m == nil {
		b.m = metrics.New(metrics.NewNoOpHandler(ctx))
	}

	if ep, ok := in.(EventProviderV2); ok {
		for _, ef := range ep.EventFeeds(ctx) {
			feedData := ef.EventFeedMetadata(ctx)
			if feedData == nil {
				return nil, fmt.Errorf("error: event feed metadata is nil")
			}
			if err := feedData.Validate(); err != nil {
				return nil, fmt.Errorf("error: event feed metadata for %s is invalid: %w", feedData.Id, err)
			}
			if _, ok := b.eventFeeds[feedData.Id]; ok {
				return nil, fmt.Errorf("error: duplicate event feed id found: %s", feedData.Id)
			}
			b.eventFeeds[feedData.Id] = ef
		}
	}

	if ep, ok := in.(EventProvider); ok {
		// Register the legacy Baton feed as a v2 event feed
		// implementing both v1 and v2 event feeds is not supported.
		if len(b.eventFeeds) != 0 {
			return nil, fmt.Errorf("error: using legacy event feed is not supported when using EventProviderV2")
		}
		b.eventFeeds[LegacyBatonFeedId] = &oldEventFeedWrapper{
			feed: ep,
		}
	}

	if ticketManager, ok := in.(TicketManager); ok {
		if b.ticketManager != nil {
			return nil, fmt.Errorf("error: cannot set multiple ticket managers")
		}
		b.ticketManager = ticketManager
	}

	if actionManager, ok := in.(CustomActionManager); ok {
		if b.actionManager != nil {
			return nil, fmt.Errorf("error: cannot set multiple action managers")
		}
		b.actionManager = actionManager
	}

	if registerActionManager, ok := in.(RegisterActionManager); ok {
		if b.actionManager != nil {
			return nil, fmt.Errorf("error: cannot register multiple action managers")
		}
		actionManager, err := registerActionManager.RegisterActionManager(ctx)
		if err != nil {
			return nil, fmt.Errorf("error: registering action manager failed: %w", err)
		}
		if actionManager == nil {
			return nil, fmt.Errorf("error: action manager is nil")
		}
		b.actionManager = actionManager
	}

	if cb, ok := in.(ConnectorBuilder); ok {
		for _, rb := range cb.ResourceSyncers(ctx) {
			if err := b.attachResourceType(ctx, rb.ResourceType(ctx).Id, rb); err != nil {
				return nil, err
			}
		}
		b.metadataProvider = cb
		b.validateProvider = cb
		return b, nil
	}

	if cb, ok := in.(ConnectorBuilder2); ok {
		for _, rb := range cb.ResourceSyncers(ctx) {
			if err := b.attachResourceType(ctx, rb.ResourceType(ctx).Id, rb); err != nil {
				return nil, err
			}
		}
		b.metadataProvider = cb
		b.validateProvider = cb
		return b, nil
	}

	return nil, fmt.Errorf("input was not a ConnectorBuilder or ConnectorBuilder2")
}

func (b *builderImpl) attachResourceType(ctx context.Context, typ string, in interface{}) error {
	if _, ok := b.resourceBuilders[typ]; ok {
		return fmt.Errorf("error: duplicate resource type found for resource builder %s", typ)
	}

	if err := validateProvisionerVersion(ctx, in, typ); err != nil {
		return err
	}

	if _, ok := in.(OldAccountManager); ok {
		return fmt.Errorf("error: old account manager interface implemented for %s", typ)
	}

	if _, ok := in.(OldCredentialManager); ok {
		return fmt.Errorf("error: old credential manager interface implemented for %s", typ)
	}

	switch t := in.(type) {
	case ResourceSyncer2:
		b.resourceBuilders[typ] = t
	case ResourceSyncer:
		b.resourceBuilders[typ] = NewResourceSyncerV1toV2(t)
	default:
		return fmt.Errorf("error: resource syncer %s is not a ResourceSyncer2 or ResourceSyncer", typ)
	}

	if provisioner, ok := in.(ResourceProvisioner); ok {
		if _, ok := b.resourceProvisioners[typ]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource provisioner %s", typ)
		}
		b.resourceProvisioners[typ] = NewResourceProvisionerV1toV2(provisioner)
	}
	if provisioner, ok := in.(ResourceProvisionerV2); ok {
		if _, ok := b.resourceProvisioners[typ]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource provisioner v2 %s", typ)
		}
		b.resourceProvisioners[typ] = provisioner
	}
	if targetedSyncer, ok := in.(ResourceTargetedSyncer); ok {
		if _, ok := b.resourceTargetedSyncers[typ]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource targeted syncer %s", typ)
		}
		b.resourceTargetedSyncers[typ] = targetedSyncer
	}

	if resourceManager, ok := in.(ResourceManager); ok {
		if _, ok := b.resourceManagers[typ]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource manager %s", typ)
		}
		b.resourceManagers[typ] = NewResourceManagerV1toV2(resourceManager)
		// Support DeleteResourceV2 if connector implements both Create and Delete
		if _, ok := b.resourceDeleters[typ]; ok {
			// This should never happen
			return fmt.Errorf("error: duplicate resource type found for resource deleter %s", typ)
		}
		b.resourceDeleters[typ] = NewResourceDeleterV1ToV2(resourceManager)
	} else {
		if resourceDeleter, ok := in.(ResourceDeleter); ok {
			if _, ok := b.resourceDeleters[typ]; ok {
				return fmt.Errorf("error: duplicate resource type found for resource deleter %s", typ)
			}
			b.resourceDeleters[typ] = NewResourceDeleterV1ToV2(resourceDeleter)
		}
	}

	if resourceManager, ok := in.(ResourceManagerV2); ok {
		if _, ok := b.resourceManagers[typ]; ok {
			return fmt.Errorf("error: duplicate resource type found for resource managerV2 %s", typ)
		}
		b.resourceManagers[typ] = resourceManager
		// Support DeleteResourceV2 if connector implements both Create and Delete
		if _, ok := b.resourceDeleters[typ]; ok {
			// This should never happen
			return fmt.Errorf("error: duplicate resource type found for resource deleterV2 %s", typ)
		}
		b.resourceDeleters[typ] = resourceManager
	} else {
		if resourceDeleter, ok := in.(ResourceDeleterV2); ok {
			if _, ok := b.resourceDeleters[typ]; ok {
				return fmt.Errorf("error: duplicate resource type found for resource deleterV2 %s", typ)
			}
			b.resourceDeleters[typ] = resourceDeleter
		}
	}

	if accountManager, ok := in.(AccountManager); ok {
		if b.accountManager != nil {
			return fmt.Errorf("error: duplicate resource type found for account manager %s", typ)
		}
		b.accountManager = accountManager
	}

	if credentialManagers, ok := in.(CredentialManager); ok {
		if _, ok := b.credentialManagers[typ]; ok {
			return fmt.Errorf("error: duplicate resource type found for credential manager %s", typ)
		}
		b.credentialManagers[typ] = credentialManagers
	}
	return nil
}

// GetMetadata gets all metadata for a connector.
func (b *builderImpl) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetMetadata")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetMetadataType
	md, err := b.metadataProvider.Metadata(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}

	md.Capabilities, err = getCapabilities(ctx, b)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}

	annos := annotations.Annotations(md.Annotations)
	if b.ticketManager != nil {
		annos.Append(&v2.ExternalTicketSettings{Enabled: b.ticketingEnabled})
	}
	md.Annotations = annos

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ConnectorServiceGetMetadataResponse{Metadata: md}, nil
}

// Validate validates the connector.
func (b *builderImpl) Validate(ctx context.Context, request *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Validate")
	defer span.End()

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 1 * time.Second,
		MaxDelay:     0,
	})

	for {
		annos, err := b.validateProvider.Validate(ctx)
		if err == nil {
			return &v2.ConnectorServiceValidateResponse{
				Annotations: annos,
				SdkVersion:  sdk.Version,
			}, nil
		}

		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}

		return nil, fmt.Errorf("validate failed: %w", err)
	}
}

func (b *builderImpl) Cleanup(ctx context.Context, request *v2.ConnectorServiceCleanupRequest) (*v2.ConnectorServiceCleanupResponse, error) {
	l := ctxzap.Extract(ctx)

	// // Clear session cache if available in context
	// sessionCache, err := session.GetSession(ctx)
	// if err != nil {
	// 	l.Warn("error getting session cache", zap.Error(err))
	// } else if request.GetActiveSyncId() != "" {
	// 	err = sessionCache.Clear(ctx, session.WithSyncID(request.GetActiveSyncId()))
	// 	if err != nil {
	// 		l.Warn("error clearing session cache", zap.Error(err))
	// 	}
	// }
	// Clear all http caches at the end of a sync. This must be run in the child process, which is why it's in this function and not in syncer.go
	err := uhttp.ClearCaches(ctx)
	if err != nil {
		l.Warn("error clearing http caches", zap.Error(err))
	}
	resp := &v2.ConnectorServiceCleanupResponse{}
	return resp, err
}

func validateCapabilityDetails(_ context.Context, credDetails *v2.CredentialDetails) error {
	if credDetails.CapabilityAccountProvisioning != nil {
		// Ensure that the preferred option is included and is part of the supported options
		if credDetails.CapabilityAccountProvisioning.PreferredCredentialOption == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "error: preferred credential creation option is not set")
		}
		if !slices.Contains(credDetails.CapabilityAccountProvisioning.SupportedCredentialOptions, credDetails.CapabilityAccountProvisioning.PreferredCredentialOption) {
			return status.Error(codes.InvalidArgument, "error: preferred credential creation option is not part of the supported options")
		}
	}

	if credDetails.CapabilityCredentialRotation != nil {
		// Ensure that the preferred option is included and is part of the supported options
		if credDetails.CapabilityCredentialRotation.PreferredCredentialOption == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "error: preferred credential rotation option is not set")
		}
		if !slices.Contains(credDetails.CapabilityCredentialRotation.SupportedCredentialOptions, credDetails.CapabilityCredentialRotation.PreferredCredentialOption) {
			return status.Error(codes.InvalidArgument, "error: preferred credential rotation option is not part of the supported options")
		}
	}

	return nil
}

func getCredentialDetails(ctx context.Context, b *builderImpl) (*v2.CredentialDetails, error) {
	l := ctxzap.Extract(ctx)
	rv := &v2.CredentialDetails{}

	for _, rb := range b.resourceBuilders {
		if am, ok := rb.(AccountManager); ok {
			accountProvisioningCapabilityDetails, _, err := am.CreateAccountCapabilityDetails(ctx)
			if err != nil {
				l.Error("error: getting account provisioning details", zap.Error(err))
				return nil, fmt.Errorf("error: getting account provisioning details: %w", err)
			}
			rv.CapabilityAccountProvisioning = accountProvisioningCapabilityDetails
		}

		if cm, ok := rb.(CredentialManager); ok {
			credentialRotationCapabilityDetails, _, err := cm.RotateCapabilityDetails(ctx)
			if err != nil {
				l.Error("error: getting credential management details", zap.Error(err))
				return nil, fmt.Errorf("error: getting credential management details: %w", err)
			}
			rv.CapabilityCredentialRotation = credentialRotationCapabilityDetails
		}
	}

	err := validateCapabilityDetails(ctx, rv)
	if err != nil {
		return nil, fmt.Errorf("error: validating capability details: %w", err)
	}
	return rv, nil
}

// getCapabilities gets all capabilities for a connector.
func getCapabilities(ctx context.Context, b *builderImpl) (*v2.ConnectorCapabilities, error) {
	connectorCaps := make(map[v2.Capability]struct{})
	resourceTypeCapabilities := []*v2.ResourceTypeCapability{}
	for _, rb := range b.resourceBuilders {
		resourceTypeCapability := &v2.ResourceTypeCapability{
			ResourceType: rb.ResourceType(ctx),
			// Currently by default all resource types support sync.
			Capabilities: []v2.Capability{v2.Capability_CAPABILITY_SYNC},
		}
		connectorCaps[v2.Capability_CAPABILITY_SYNC] = struct{}{}
		if _, ok := rb.(ResourceTargetedSyncer); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_TARGETED_SYNC)
			connectorCaps[v2.Capability_CAPABILITY_TARGETED_SYNC] = struct{}{}
		}
		if _, ok := rb.(ResourceProvisioner); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_PROVISION)
			connectorCaps[v2.Capability_CAPABILITY_PROVISION] = struct{}{}
		} else if _, ok = rb.(ResourceProvisionerV2); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_PROVISION)
			connectorCaps[v2.Capability_CAPABILITY_PROVISION] = struct{}{}
		}
		if _, ok := rb.(AccountManager); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
			connectorCaps[v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING] = struct{}{}
		}

		if _, ok := rb.(CredentialManager); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
			connectorCaps[v2.Capability_CAPABILITY_CREDENTIAL_ROTATION] = struct{}{}
		}

		if _, ok := rb.(ResourceManager); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_CREATE] = struct{}{}
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		} else if _, ok := rb.(ResourceDeleter); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		}

		if _, ok := rb.(ResourceManagerV2); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_CREATE] = struct{}{}
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		} else if _, ok := rb.(ResourceDeleterV2); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		}

		resourceTypeCapabilities = append(resourceTypeCapabilities, resourceTypeCapability)
	}
	sort.Slice(resourceTypeCapabilities, func(i, j int) bool {
		return resourceTypeCapabilities[i].ResourceType.GetId() < resourceTypeCapabilities[j].ResourceType.GetId()
	})

	if len(b.eventFeeds) > 0 {
		connectorCaps[v2.Capability_CAPABILITY_EVENT_FEED_V2] = struct{}{}
	}

	if b.ticketManager != nil {
		connectorCaps[v2.Capability_CAPABILITY_TICKETING] = struct{}{}
	}

	if b.actionManager != nil {
		connectorCaps[v2.Capability_CAPABILITY_ACTIONS] = struct{}{}
	}

	var caps []v2.Capability
	for c := range connectorCaps {
		caps = append(caps, c)
	}
	slices.Sort(caps)

	credDetails, err := getCredentialDetails(ctx, b)
	if err != nil {
		return nil, err
	}

	return &v2.ConnectorCapabilities{
		ResourceTypeCapabilities: resourceTypeCapabilities,
		ConnectorCapabilities:    caps,
		CredentialDetails:        credDetails,
	}, nil
}

func validateProvisionerVersion(_ context.Context, cb interface{}, rTypeId string) error {
	_, ok := cb.(ResourceProvisioner)
	_, okV2 := cb.(ResourceProvisionerV2)

	if ok && okV2 {
		return fmt.Errorf("error: resource type %s implements both ResourceProvisioner and ResourceProvisionerV2", rTypeId)
	}
	return nil
}
