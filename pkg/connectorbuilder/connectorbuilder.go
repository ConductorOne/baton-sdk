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
	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

var tracer = otel.Tracer("baton-sdk/pkg.connectorbuilder")

// ConnectorBuilder is the foundational interface for creating Baton connectors.
//
// This interface defines the core capabilities required by all connectors, including
// metadata, validation, and registering resource syncers. Additional functionality
// can be added by implementing extension interfaces such as:
// - RegisterActionManager: For custom action support
// - EventProvider: For event stream support
// - TicketManager: For ticket management integration.
type ConnectorBuilder interface {
	Metadata(ctx context.Context) (*v2.ConnectorMetadata, error)
	Validate(ctx context.Context) (annotations.Annotations, error)
	ResourceSyncers(ctx context.Context) []ResourceSyncer
}

type builderImpl struct {
	resourceBuilders        map[string]ResourceSyncer
	resourceProvisioners    map[string]ResourceProvisioner
	resourceProvisionersV2  map[string]ResourceProvisionerV2
	resourceManagers        map[string]ResourceManager
	resourceManagersV2      map[string]ResourceManagerV2
	resourceDeleters        map[string]ResourceDeleter
	resourceDeletersV2      map[string]ResourceDeleterV2
	resourceTargetedSyncers map[string]ResourceTargetedSyncer
	accountManager          AccountManager
	actionManager           CustomActionManager
	credentialManagers      map[string]CredentialManager
	eventFeeds              map[string]EventFeed
	cb                      ConnectorBuilder
	ticketManager           TicketManager
	ticketingEnabled        bool
	m                       *metrics.M
	nowFunc                 func() time.Time
	clientSecret            *jose.JSONWebKey
}

// NewConnector creates a new ConnectorServer for a new resource.
func NewConnector(ctx context.Context, in interface{}, opts ...Opt) (types.ConnectorServer, error) {
	switch c := in.(type) {
	case ConnectorBuilder:
		clientSecretValue := ctx.Value(crypto.ContextClientSecretKey)
		clientSecretJWK, _ := clientSecretValue.(*jose.JSONWebKey)

		ret := &builderImpl{
			resourceBuilders:        make(map[string]ResourceSyncer),
			resourceProvisioners:    make(map[string]ResourceProvisioner),
			resourceProvisionersV2:  make(map[string]ResourceProvisionerV2),
			resourceManagers:        make(map[string]ResourceManager),
			resourceManagersV2:      make(map[string]ResourceManagerV2),
			resourceDeleters:        make(map[string]ResourceDeleter),
			resourceDeletersV2:      make(map[string]ResourceDeleterV2),
			resourceTargetedSyncers: make(map[string]ResourceTargetedSyncer),
			accountManager:          nil,
			actionManager:           nil,
			credentialManagers:      make(map[string]CredentialManager),
			eventFeeds:              make(map[string]EventFeed),
			cb:                      c,
			ticketManager:           nil,
			nowFunc:                 time.Now,
			clientSecret:            clientSecretJWK,
		}

		err := ret.options(opts...)
		if err != nil {
			return nil, err
		}

		if ret.m == nil {
			ret.m = metrics.New(metrics.NewNoOpHandler(ctx))
		}

		if b, ok := c.(EventProviderV2); ok {
			for _, ef := range b.EventFeeds(ctx) {
				feedData := ef.EventFeedMetadata(ctx)
				if feedData == nil {
					return nil, fmt.Errorf("error: event feed metadata is nil")
				}
				if err := feedData.Validate(); err != nil {
					return nil, fmt.Errorf("error: event feed metadata for %s is invalid: %w", feedData.Id, err)
				}
				if _, ok := ret.eventFeeds[feedData.Id]; ok {
					return nil, fmt.Errorf("error: duplicate event feed id found: %s", feedData.Id)
				}
				ret.eventFeeds[feedData.Id] = ef
			}
		}

		if b, ok := c.(EventProvider); ok {
			// Register the legacy Baton feed as a v2 event feed
			// implementing both v1 and v2 event feeds is not supported.
			if len(ret.eventFeeds) != 0 {
				return nil, fmt.Errorf("error: using legacy event feed is not supported when using EventProviderV2")
			}
			ret.eventFeeds[LegacyBatonFeedId] = &oldEventFeedWrapper{
				feed: b,
			}
		}

		if ticketManager, ok := c.(TicketManager); ok {
			if ret.ticketManager != nil {
				return nil, fmt.Errorf("error: cannot set multiple ticket managers")
			}
			ret.ticketManager = ticketManager
		}

		if actionManager, ok := c.(CustomActionManager); ok {
			if ret.actionManager != nil {
				return nil, fmt.Errorf("error: cannot set multiple action managers")
			}
			ret.actionManager = actionManager
		}

		if registerActionManager, ok := c.(RegisterActionManager); ok {
			if ret.actionManager != nil {
				return nil, fmt.Errorf("error: cannot register multiple action managers")
			}
			actionManager, err := registerActionManager.RegisterActionManager(ctx)
			if err != nil {
				return nil, fmt.Errorf("error: registering action manager failed: %w", err)
			}
			if actionManager == nil {
				return nil, fmt.Errorf("error: action manager is nil")
			}
			ret.actionManager = actionManager
		}

		for _, rb := range c.ResourceSyncers(ctx) {
			rType := rb.ResourceType(ctx)
			if _, ok := ret.resourceBuilders[rType.Id]; ok {
				return nil, fmt.Errorf("error: duplicate resource type found for resource builder %s", rType.Id)
			}
			ret.resourceBuilders[rType.Id] = rb

			if err := validateProvisionerVersion(ctx, rb); err != nil {
				return nil, err
			}

			if provisioner, ok := rb.(ResourceProvisioner); ok {
				if _, ok := ret.resourceProvisioners[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource provisioner %s", rType.Id)
				}
				ret.resourceProvisioners[rType.Id] = provisioner
			}
			if provisioner, ok := rb.(ResourceProvisionerV2); ok {
				if _, ok := ret.resourceProvisionersV2[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource provisioner v2 %s", rType.Id)
				}
				ret.resourceProvisionersV2[rType.Id] = provisioner
			}
			if targetedSyncer, ok := rb.(ResourceTargetedSyncer); ok {
				if _, ok := ret.resourceTargetedSyncers[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource targeted syncer %s", rType.Id)
				}
				ret.resourceTargetedSyncers[rType.Id] = targetedSyncer
			}

			if resourceManager, ok := rb.(ResourceManager); ok {
				if _, ok := ret.resourceManagers[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource manager %s", rType.Id)
				}
				ret.resourceManagers[rType.Id] = resourceManager
				// Support DeleteResourceV2 if connector implements both Create and Delete
				if _, ok := ret.resourceDeleters[rType.Id]; ok {
					// This should never happen
					return nil, fmt.Errorf("error: duplicate resource type found for resource deleter %s", rType.Id)
				}
				ret.resourceDeleters[rType.Id] = resourceManager
			} else {
				if resourceDeleter, ok := rb.(ResourceDeleter); ok {
					if _, ok := ret.resourceDeleters[rType.Id]; ok {
						return nil, fmt.Errorf("error: duplicate resource type found for resource deleter %s", rType.Id)
					}
					ret.resourceDeleters[rType.Id] = resourceDeleter
				}
			}

			if resourceManager, ok := rb.(ResourceManagerV2); ok {
				if _, ok := ret.resourceManagersV2[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource managerV2 %s", rType.Id)
				}
				ret.resourceManagersV2[rType.Id] = resourceManager
				// Support DeleteResourceV2 if connector implements both Create and Delete
				if _, ok := ret.resourceDeletersV2[rType.Id]; ok {
					// This should never happen
					return nil, fmt.Errorf("error: duplicate resource type found for resource deleterV2 %s", rType.Id)
				}
				ret.resourceDeletersV2[rType.Id] = resourceManager
			} else {
				if resourceDeleter, ok := rb.(ResourceDeleterV2); ok {
					if _, ok := ret.resourceDeletersV2[rType.Id]; ok {
						return nil, fmt.Errorf("error: duplicate resource type found for resource deleterV2 %s", rType.Id)
					}
					ret.resourceDeletersV2[rType.Id] = resourceDeleter
				}
			}

			if _, ok := rb.(OldAccountManager); ok {
				return nil, fmt.Errorf("error: old account manager interface implemented for %s", rType.Id)
			}

			if accountManager, ok := rb.(AccountManager); ok {
				if ret.accountManager != nil {
					return nil, fmt.Errorf("error: duplicate resource type found for account manager %s", rType.Id)
				}
				ret.accountManager = accountManager
			}

			if _, ok := rb.(OldCredentialManager); ok {
				return nil, fmt.Errorf("error: old credential manager interface implemented for %s", rType.Id)
			}

			if credentialManagers, ok := rb.(CredentialManager); ok {
				if _, ok := ret.credentialManagers[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for credential manager %s", rType.Id)
				}
				ret.credentialManagers[rType.Id] = credentialManagers
			}
		}
		return ret, nil

	case types.ConnectorServer:
		return c, nil

	default:
		return nil, fmt.Errorf("input was not a ConnectorBuilder or a ConnectorServer")
	}
}

type Opt func(b *builderImpl) error

func WithTicketingEnabled() Opt {
	return func(b *builderImpl) error {
		if _, ok := b.cb.(TicketManager); ok {
			b.ticketingEnabled = true
			return nil
		}
		return errors.New("external ticketing not supported")
	}
}

func WithMetricsHandler(h metrics.Handler) Opt {
	return func(b *builderImpl) error {
		b.m = metrics.New(h)
		return nil
	}
}

func (b *builderImpl) options(opts ...Opt) error {
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return err
		}
	}

	return nil
}

func validateProvisionerVersion(ctx context.Context, p ResourceSyncer) error {
	_, ok := p.(ResourceProvisioner)
	_, okV2 := p.(ResourceProvisionerV2)

	if ok && okV2 {
		return fmt.Errorf("error: resource type %s implements both ResourceProvisioner and ResourceProvisionerV2", p.ResourceType(ctx).Id)
	}
	return nil
}

// GetMetadata gets all metadata for a connector.
func (b *builderImpl) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetMetadata")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetMetadataType
	md, err := b.cb.Metadata(ctx)
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

func validateCapabilityDetails(ctx context.Context, credDetails *v2.CredentialDetails) error {
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
		annos, err := b.cb.Validate(ctx)
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

	// Clear session cache if available in context
	sessionCache, err := session.GetSession(ctx)
	if err != nil {
		l.Warn("error getting session cache", zap.Error(err))
	} else if request.GetActiveSyncId() != "" {
		err = sessionCache.Clear(ctx, session.WithSyncID(request.GetActiveSyncId()))
		if err != nil {
			l.Warn("error clearing session cache", zap.Error(err))
		}
	}
	// Clear all http caches at the end of a sync. This must be run in the child process, which is why it's in this function and not in syncer.go
	err = uhttp.ClearCaches(ctx)
	if err != nil {
		l.Warn("error clearing http caches", zap.Error(err))
	}
	resp := &v2.ConnectorServiceCleanupResponse{}
	return resp, err
}
