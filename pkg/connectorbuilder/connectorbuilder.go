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
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
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

type MetadataProvider interface {
	Metadata(ctx context.Context) (*v2.ConnectorMetadata, error)
}

type ValidateProvider interface {
	Validate(ctx context.Context) (annotations.Annotations, error)
}

type ConnectorBuilder interface {
	MetadataProvider
	ValidateProvider
	ResourceSyncers(ctx context.Context) []ResourceSyncer
}

type ConnectorBuilderV2 interface {
	MetadataProvider
	ValidateProvider
	ResourceSyncers(ctx context.Context) []ResourceSyncerV2
}

type builder struct {
	ticketingEnabled        bool
	m                       *metrics.M
	nowFunc                 func() time.Time
	clientSecret            *jose.JSONWebKey
	sessionStore            sessions.SessionStore
	metadataProvider        MetadataProvider
	validateProvider        ValidateProvider
	ticketManager           TicketManagerLimited
	accountManager          AccountManagerLimited
	actionManager           CustomActionManager
	resourceSyncers         map[string]ResourceSyncerV2
	resourceProvisioners    map[string]ResourceProvisionerV2Limited
	resourceManagers        map[string]ResourceManagerV2Limited
	resourceDeleters        map[string]ResourceDeleterV2Limited
	resourceTargetedSyncers map[string]ResourceTargetedSyncerLimited
	credentialManagers      map[string]CredentialManagerLimited
	eventFeeds              map[string]EventFeed
	accountManagers         map[string]AccountManagerLimited // NOTE(kans): currently unused
}

// NewConnector creates a new ConnectorServer for a new resource.
func NewConnector(ctx context.Context, in interface{}, opts ...Opt) (types.ConnectorServer, error) {
	if in == nil {
		return nil, fmt.Errorf("input cannot be nil")
	}

	switch t := in.(type) {
	case types.ConnectorServer:
		// its likely nothing uses this code path anymore
		return t, nil
	case ConnectorBuilder, ConnectorBuilderV2:
	default:
		return nil, fmt.Errorf("input is not a ConnectorServer, ConnectorBuilder, or ConnectorBuilderV2")
	}

	clientSecretValue := ctx.Value(crypto.ContextClientSecretKey)
	clientSecretJWK, _ := clientSecretValue.(*jose.JSONWebKey)

	b := &builder{
		metadataProvider:        nil,
		validateProvider:        nil,
		ticketManager:           nil,
		accountManager:          nil,
		actionManager:           nil,
		nowFunc:                 time.Now,
		clientSecret:            clientSecretJWK,
		resourceSyncers:         make(map[string]ResourceSyncerV2),
		resourceProvisioners:    make(map[string]ResourceProvisionerV2Limited),
		resourceManagers:        make(map[string]ResourceManagerV2Limited),
		resourceDeleters:        make(map[string]ResourceDeleterV2Limited),
		resourceTargetedSyncers: make(map[string]ResourceTargetedSyncerLimited),
		credentialManagers:      make(map[string]CredentialManagerLimited),
		eventFeeds:              make(map[string]EventFeed),
		accountManagers:         make(map[string]AccountManagerLimited),
	}

	// WithTicketingEnabled checks for the ticketManager
	if err := b.addTicketManager(ctx, in); err != nil {
		return nil, err
	}

	err := b.options(opts...)
	if err != nil {
		return nil, err
	}

	if b.m == nil {
		b.m = metrics.New(metrics.NewNoOpHandler(ctx))
	}

	if err := b.addConnectorBuilderProviders(ctx, in); err != nil {
		return nil, err
	}

	if err := b.addEventFeed(ctx, in); err != nil {
		return nil, err
	}

	if err := b.addActionManager(ctx, in); err != nil {
		return nil, err
	}

	addResourceType := func(ctx context.Context, rType string, rs interface{}) error {
		if err := b.addResourceSyncers(ctx, rType, rs); err != nil {
			return err
		}

		if err := b.addProvisioner(ctx, rType, rs); err != nil {
			return err
		}

		if err := b.addTargetedSyncer(ctx, rType, rs); err != nil {
			return err
		}

		if err := b.addResourceManager(ctx, rType, rs); err != nil {
			return err
		}

		if err := b.addAccountManager(ctx, rType, rs); err != nil {
			return err
		}

		if err := b.addCredentialManager(ctx, rType, rs); err != nil {
			return err
		}

		return nil
	}

	if cb, ok := in.(ConnectorBuilder); ok {
		for _, rb := range cb.ResourceSyncers(ctx) {
			rType := rb.ResourceType(ctx)
			if err := addResourceType(ctx, rType.Id, rb); err != nil {
				return nil, err
			}
		}
		return b, nil
	}

	if cb2, ok := in.(ConnectorBuilderV2); ok {
		for _, rb := range cb2.ResourceSyncers(ctx) {
			rType := rb.ResourceType(ctx)
			if err := addResourceType(ctx, rType.Id, rb); err != nil {
				return nil, err
			}
		}
		return b, nil
	}
	return nil, fmt.Errorf("input is not a ConnectorBuilder or a ConnectorBuilderV2")
}

type Opt func(b *builder) error

func WithTicketingEnabled() Opt {
	return func(b *builder) error {
		if b.ticketManager == nil {
			return errors.New("external ticketing not supported")
		}
		b.ticketingEnabled = true
		return nil
	}
}

func WithMetricsHandler(h metrics.Handler) Opt {
	return func(b *builder) error {
		b.m = metrics.New(h)
		return nil
	}
}

func WithSessionStore(ss sessions.SessionStore) Opt {
	return func(b *builder) error {
		b.sessionStore = ss
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

func (b *builder) addConnectorBuilderProviders(_ context.Context, in interface{}) error {
	if mp, ok := in.(MetadataProvider); ok {
		b.metadataProvider = mp
	} else {
		return fmt.Errorf("error: metadata provider not implemented")
	}

	if vp, ok := in.(ValidateProvider); ok {
		b.validateProvider = vp
	} else {
		return fmt.Errorf("error: validate provider not implemented")
	}

	return nil
}

// GetMetadata gets all metadata for a connector.
func (b *builder) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.GetMetadata")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetMetadataType
	md, err := b.metadataProvider.Metadata(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}

	md.Capabilities, err = b.getCapabilities(ctx)
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
func (b *builder) Validate(ctx context.Context, request *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	ctx, span := tracer.Start(ctx, "builder.Validate")
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

func (b *builder) Cleanup(ctx context.Context, request *v2.ConnectorServiceCleanupRequest) (*v2.ConnectorServiceCleanupResponse, error) {
	l := ctxzap.Extract(ctx)
	// TODO(kans): clear the session store here.
	// Clear all http caches at the end of a sync. This must be run in the child process, which is why it's in this function and not in syncer.go
	err := uhttp.ClearCaches(ctx)
	if err != nil {
		l.Warn("error clearing http caches", zap.Error(err))
	}
	resp := &v2.ConnectorServiceCleanupResponse{}
	return resp, err
}

// getCapabilities gets all capabilities for a connector.
func (b *builder) getCapabilities(ctx context.Context) (*v2.ConnectorCapabilities, error) {
	connectorCaps := make(map[v2.Capability]struct{})
	resourceTypeCapabilities := []*v2.ResourceTypeCapability{}

	for resourceTypeID, rb := range b.resourceSyncers {
		connectorCaps[v2.Capability_CAPABILITY_SYNC] = struct{}{}
		caps := []v2.Capability{v2.Capability_CAPABILITY_SYNC}

		if _, exists := b.resourceTargetedSyncers[resourceTypeID]; exists {
			caps = append(caps, v2.Capability_CAPABILITY_TARGETED_SYNC)
		}

		if _, exists := b.resourceProvisioners[resourceTypeID]; exists {
			caps = append(caps, v2.Capability_CAPABILITY_PROVISION)
		}

		if _, exists := b.accountManagers[resourceTypeID]; exists {
			caps = append(caps, v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
		}

		if _, exists := b.resourceManagers[resourceTypeID]; exists {
			caps = append(caps, v2.Capability_CAPABILITY_RESOURCE_DELETE, v2.Capability_CAPABILITY_RESOURCE_CREATE)
		} else if _, exists := b.resourceDeleters[resourceTypeID]; exists {
			caps = append(caps, v2.Capability_CAPABILITY_RESOURCE_DELETE)
		}

		if _, exists := b.credentialManagers[resourceTypeID]; exists {
			caps = append(caps, v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
		}

		// Extend the capabilities with the resource type specificcapabilities
		for _, cap := range caps {
			connectorCaps[cap] = struct{}{}
		}

		resourceTypeCapabilities = append(resourceTypeCapabilities, &v2.ResourceTypeCapability{
			ResourceType: rb.ResourceType(ctx),
			Capabilities: caps,
		})
	}

	// Check for account provisioning capability (global, not per resource type)
	if b.accountManager != nil {
		connectorCaps[v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING] = struct{}{}
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

func getCredentialDetails(ctx context.Context, b *builder) (*v2.CredentialDetails, error) {
	l := ctxzap.Extract(ctx)
	rv := &v2.CredentialDetails{}

	// Check for account provisioning capability details
	if b.accountManager != nil {
		accountProvisioningCapabilityDetails, _, err := b.accountManager.CreateAccountCapabilityDetails(ctx)
		if err != nil {
			l.Error("error: getting account provisioning details", zap.Error(err))
			return nil, fmt.Errorf("error: getting account provisioning details: %w", err)
		}
		rv.CapabilityAccountProvisioning = accountProvisioningCapabilityDetails
	}

	// Check for credential rotation capability details
	for _, cm := range b.credentialManagers {
		credentialRotationCapabilityDetails, _, err := cm.RotateCapabilityDetails(ctx)
		if err != nil {
			l.Error("error: getting credential management details", zap.Error(err))
			return nil, fmt.Errorf("error: getting credential management details: %w", err)
		}
		rv.CapabilityCredentialRotation = credentialRotationCapabilityDetails
		break // Only need one credential manager's details
	}

	err := validateCapabilityDetails(ctx, rv)
	if err != nil {
		return nil, fmt.Errorf("error: validating capability details: %w", err)
	}
	return rv, nil
}
