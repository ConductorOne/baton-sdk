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

type builder struct {
	resourceBuilders        map[string]ResourceSyncer
	resourceProvisioners    map[string]ResourceProvisionerV2
	resourceManagers        map[string]ResourceManagerV2
	resourceDeleters        map[string]ResourceDeleterV2
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

		b := &builder{
			resourceBuilders:        make(map[string]ResourceSyncer),
			resourceProvisioners:    make(map[string]ResourceProvisionerV2),
			resourceManagers:        make(map[string]ResourceManagerV2),
			resourceDeleters:        make(map[string]ResourceDeleterV2),
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

		err := b.options(opts...)
		if err != nil {
			return nil, err
		}

		if b.m == nil {
			b.m = metrics.New(metrics.NewNoOpHandler(ctx))
		}

		if err := b.addEventFeed(ctx, c); err != nil {
			return nil, err
		}

		if err := b.addTicketManager(ctx, c); err != nil {
			return nil, err
		}

		if err := b.addActionManager(ctx, c); err != nil {
			return nil, err
		}

		for _, rb := range c.ResourceSyncers(ctx) {
			rType := rb.ResourceType(ctx)

			if err := b.addResourceBuilders(ctx, rType.GetId(), rb); err != nil {
				return nil, err
			}

			if err := b.addProvisioner(ctx, rType.GetId(), rb); err != nil {
				return nil, err
			}

			if err := b.addTargetedSyncer(ctx, rType.GetId(), rb); err != nil {
				return nil, err
			}

			if err := b.addResourceManager(ctx, rType.GetId(), rb); err != nil {
				return nil, err
			}

			if err := b.addAccountManager(ctx, rType.GetId(), rb); err != nil {
				return nil, err
			}

			if err := b.addCredentialManager(ctx, rType.GetId(), rb); err != nil {
				return nil, err
			}
		}
		return b, nil

	case types.ConnectorServer:
		return c, nil

	default:
		return nil, fmt.Errorf("input was not a ConnectorBuilder or a ConnectorServer")
	}
}

type Opt func(b *builder) error

func WithTicketingEnabled() Opt {
	return func(b *builder) error {
		if _, ok := b.cb.(TicketManager); ok {
			b.ticketingEnabled = true
			return nil
		}
		return errors.New("external ticketing not supported")
	}
}

func WithMetricsHandler(h metrics.Handler) Opt {
	return func(b *builder) error {
		b.m = metrics.New(h)
		return nil
	}
}

func (b *builder) options(opts ...Opt) error {
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return err
		}
	}

	return nil
}

// GetMetadata gets all metadata for a connector.
func (b *builder) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetMetadata")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetMetadataType
	md, err := b.cb.Metadata(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}

	capabilities, err := getCapabilities(ctx, b)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}
	md.SetCapabilities(capabilities)

	annos := annotations.Annotations(md.GetAnnotations())
	if b.ticketManager != nil {
		settingsBuilder := &v2.ExternalTicketSettings_builder{
			Enabled: b.ticketingEnabled,
		}
		annos.Append(settingsBuilder.Build())
	}
	md.SetAnnotations(annos)

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	responseBuilder := &v2.ConnectorServiceGetMetadataResponse_builder{
		Metadata: md,
	}
	return responseBuilder.Build(), nil
}

// Validate validates the connector.
func (b *builder) Validate(ctx context.Context, request *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.Validate")
	defer span.End()

	retryer := retry.NewRetryer(ctx, retry.RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 1 * time.Second,
		MaxDelay:     0,
	})

	for {
		annos, err := b.cb.Validate(ctx)
		if err == nil {
			responseBuilder := &v2.ConnectorServiceValidateResponse_builder{
				Annotations: annos,
				SdkVersion:  sdk.Version,
			}
			return responseBuilder.Build(), nil
		}

		if retryer.ShouldWaitAndRetry(ctx, err) {
			continue
		}

		return nil, fmt.Errorf("validate failed: %w", err)
	}
}

func (b *builder) Cleanup(ctx context.Context, request *v2.ConnectorServiceCleanupRequest) (*v2.ConnectorServiceCleanupResponse, error) {
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

// getCapabilities gets all capabilities for a connector.
func getCapabilities(ctx context.Context, b *builder) (*v2.ConnectorCapabilities, error) {
	connectorCaps := make(map[v2.Capability]struct{})
	resourceTypeCapabilities := []*v2.ResourceTypeCapability{}
	for _, rb := range b.resourceBuilders {
		resourceTypeCapabilityBuilder := &v2.ResourceTypeCapability_builder{
			ResourceType: rb.ResourceType(ctx),
			// Currently by default all resource types support sync.
			Capabilities: []v2.Capability{v2.Capability_CAPABILITY_SYNC},
		}
		connectorCaps[v2.Capability_CAPABILITY_SYNC] = struct{}{}
		if _, ok := rb.(ResourceTargetedSyncer); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_TARGETED_SYNC)
			connectorCaps[v2.Capability_CAPABILITY_TARGETED_SYNC] = struct{}{}
		}
		if _, ok := rb.(ResourceProvisioner); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_PROVISION)
			connectorCaps[v2.Capability_CAPABILITY_PROVISION] = struct{}{}
		} else if _, ok = rb.(ResourceProvisionerV2); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_PROVISION)
			connectorCaps[v2.Capability_CAPABILITY_PROVISION] = struct{}{}
		}
		if _, ok := rb.(AccountManager); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
			connectorCaps[v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING] = struct{}{}
		}

		if _, ok := rb.(CredentialManager); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
			connectorCaps[v2.Capability_CAPABILITY_CREDENTIAL_ROTATION] = struct{}{}
		}

		if _, ok := rb.(ResourceManager); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_CREATE] = struct{}{}
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		} else if _, ok := rb.(ResourceDeleter); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		}

		if _, ok := rb.(ResourceManagerV2); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_CREATE] = struct{}{}
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		} else if _, ok := rb.(ResourceDeleterV2); ok {
			resourceTypeCapabilityBuilder.Capabilities = append(resourceTypeCapabilityBuilder.Capabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		}

		resourceTypeCapabilities = append(resourceTypeCapabilities, resourceTypeCapabilityBuilder.Build())
	}
	sort.Slice(resourceTypeCapabilities, func(i, j int) bool {
		return resourceTypeCapabilities[i].GetResourceType().GetId() < resourceTypeCapabilities[j].GetResourceType().GetId()
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

	capabilitiesBuilder := &v2.ConnectorCapabilities_builder{
		ResourceTypeCapabilities: resourceTypeCapabilities,
		ConnectorCapabilities:    caps,
		CredentialDetails:        credDetails,
	}
	return capabilitiesBuilder.Build(), nil
}

func validateCapabilityDetails(_ context.Context, credDetails *v2.CredentialDetails) error {
	if credDetails.GetCapabilityAccountProvisioning() != nil {
		// Ensure that the preferred option is included and is part of the supported options
		if credDetails.GetCapabilityAccountProvisioning().GetPreferredCredentialOption() == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "error: preferred credential creation option is not set")
		}
		if !slices.Contains(credDetails.GetCapabilityAccountProvisioning().GetSupportedCredentialOptions(), credDetails.GetCapabilityAccountProvisioning().GetPreferredCredentialOption()) {
			return status.Error(codes.InvalidArgument, "error: preferred credential creation option is not part of the supported options")
		}
	}

	if credDetails.GetCapabilityCredentialRotation() != nil {
		// Ensure that the preferred option is included and is part of the supported options
		if credDetails.GetCapabilityCredentialRotation().GetPreferredCredentialOption() == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "error: preferred credential rotation option is not set")
		}
		if !slices.Contains(credDetails.GetCapabilityCredentialRotation().GetSupportedCredentialOptions(), credDetails.GetCapabilityCredentialRotation().GetPreferredCredentialOption()) {
			return status.Error(codes.InvalidArgument, "error: preferred credential rotation option is not part of the supported options")
		}
	}

	return nil
}

func getCredentialDetails(ctx context.Context, b *builder) (*v2.CredentialDetails, error) {
	l := ctxzap.Extract(ctx)
	rv := &v2.CredentialDetails{}

	for _, rb := range b.resourceBuilders {
		if am, ok := rb.(AccountManager); ok {
			accountProvisioningCapabilityDetails, _, err := am.CreateAccountCapabilityDetails(ctx)
			if err != nil {
				l.Error("error: getting account provisioning details", zap.Error(err))
				return nil, fmt.Errorf("error: getting account provisioning details: %w", err)
			}
			rv.SetCapabilityAccountProvisioning(accountProvisioningCapabilityDetails)
		}

		if cm, ok := rb.(CredentialManager); ok {
			credentialRotationCapabilityDetails, _, err := cm.RotateCapabilityDetails(ctx)
			if err != nil {
				l.Error("error: getting credential management details", zap.Error(err))
				return nil, fmt.Errorf("error: getting credential management details: %w", err)
			}
			rv.SetCapabilityCredentialRotation(credentialRotationCapabilityDetails)
		}
	}

	err := validateCapabilityDetails(ctx, rv)
	if err != nil {
		return nil, fmt.Errorf("error: validating capability details: %w", err)
	}
	return rv, nil
}
