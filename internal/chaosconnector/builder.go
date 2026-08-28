package chaosconnector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Builder implements the connector-author surface over one deterministic run.
type Builder struct {
	run              *Run
	fullCapabilities bool
}

// BuilderOption configures the reference connector.
type BuilderOption func(*Builder)

// WithFullCapabilities enables the complete capability skeleton. The scenario
// must include FullCapabilityResourceTypeID and IssuedSecretResourceTypeID.
func WithFullCapabilities() BuilderOption {
	return func(builder *Builder) {
		builder.fullCapabilities = true
	}
}

// NewBuilder constructs a connector-author implementation.
func NewBuilder(run *Run, opts ...BuilderOption) (*Builder, error) {
	if run == nil {
		return nil, fmt.Errorf("chaosconnector: nil run")
	}
	builder := &Builder{run: run}
	for _, opt := range opts {
		opt(builder)
	}
	if builder.fullCapabilities {
		dataset := run.dataset()
		if !hasResourceType(dataset, FullCapabilityResourceTypeID) {
			return nil, fmt.Errorf("chaosconnector: full capabilities require resource type %q", FullCapabilityResourceTypeID)
		}
		if !hasResourceType(dataset, IssuedSecretResourceTypeID) {
			return nil, fmt.Errorf("chaosconnector: full capabilities require resource type %q", IssuedSecretResourceTypeID)
		}
	}
	return builder, nil
}

func hasResourceType(dataset *Dataset, id string) bool {
	for _, resourceType := range dataset.ResourceTypes {
		if resourceType.GetId() == id {
			return true
		}
	}
	return false
}

// Server builds the normal connectorbuilder server implementation.
func (b *Builder) Server(ctx context.Context) (types.ConnectorServer, error) {
	var opts []connectorbuilder.Opt
	if b.fullCapabilities {
		opts = append(opts, connectorbuilder.WithTicketingEnabled())
	}
	return connectorbuilder.NewConnector(ctx, b, opts...)
}

func (b *Builder) Metadata(context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{
		DisplayName: "SDK Internal Chaos Connector",
		Description: "Deterministic adversarial connector for SDK verification",
	}.Build(), nil
}

func (b *Builder) Validate(context.Context) (annotations.Annotations, error) {
	// Source-cache capability is run state, not builder config: generational
	// suites rotate generations/fingerprints between the syncs of one run,
	// and the syncer re-parses the capability on every attempt (plan B1).
	if capability := b.run.SourceCacheCapability(); capability != nil {
		return annotations.New(capability), nil
	}
	return nil, nil
}

func (b *Builder) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	dataset := b.run.dataset()
	out := make([]connectorbuilder.ResourceSyncerV2, 0, len(dataset.ResourceTypes))
	for _, resourceType := range dataset.ResourceTypes {
		base := &resourceSyncer{
			run:          b.run,
			resourceType: resourceType,
		}
		if !b.fullCapabilities {
			out = append(out, base)
			continue
		}
		switch resourceType.GetId() {
		case FullCapabilityResourceTypeID:
			out = append(out, &fullSyncer{resourceSyncer: base})
		case IssuedSecretResourceTypeID:
			out = append(out, &deletableSyncer{resourceSyncer: base})
		default:
			out = append(out, base)
		}
	}
	return out
}

// EventFeeds supplies one deterministic feed when full capabilities are armed.
func (b *Builder) EventFeeds(context.Context) []connectorbuilder.EventFeed {
	if !b.fullCapabilities {
		return nil
	}
	return []connectorbuilder.EventFeed{&eventFeed{}}
}

// GlobalActions registers one deterministic action.
func (b *Builder) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	if !b.fullCapabilities {
		return nil
	}
	schema := v2.BatonActionSchema_builder{
		Name:        "chaos-echo",
		DisplayName: "Chaos Echo",
	}.Build()
	return registry.Register(ctx, schema, func(
		_ context.Context,
		args *structpb.Struct,
	) (*structpb.Struct, annotations.Annotations, error) {
		return proto.Clone(args).(*structpb.Struct), nil, nil
	})
}

// TicketManagerLimited implementation.

func (b *Builder) GetTicket(
	_ context.Context,
	ticketID string,
) (*v2.Ticket, annotations.Annotations, error) {
	return v2.Ticket_builder{Id: ticketID, DisplayName: "Chaos Ticket"}.Build(), nil, nil
}

func (b *Builder) CreateTicket(
	_ context.Context,
	ticket *v2.Ticket,
	_ *v2.TicketSchema,
) (*v2.Ticket, annotations.Annotations, error) {
	out := proto.Clone(ticket).(*v2.Ticket)
	if out.GetId() == "" {
		out.SetId("chaos-ticket-1")
	}
	return out, nil, nil
}

func (b *Builder) GetTicketSchema(
	_ context.Context,
	schemaID string,
) (*v2.TicketSchema, annotations.Annotations, error) {
	return v2.TicketSchema_builder{Id: schemaID, DisplayName: "Chaos Ticket Schema"}.Build(), nil, nil
}

func (b *Builder) ListTicketSchemas(
	_ context.Context,
	_ *pagination.Token,
) ([]*v2.TicketSchema, string, annotations.Annotations, error) {
	return []*v2.TicketSchema{
		v2.TicketSchema_builder{Id: "chaos-schema", DisplayName: "Chaos Ticket Schema"}.Build(),
	}, "", nil, nil
}

func (b *Builder) BulkCreateTickets(
	ctx context.Context,
	request *v2.TicketsServiceBulkCreateTicketsRequest,
) (*v2.TicketsServiceBulkCreateTicketsResponse, error) {
	out := make([]*v2.TicketsServiceCreateTicketResponse, 0, len(request.GetTicketRequests()))
	for _, item := range request.GetTicketRequests() {
		ticket := v2.Ticket_builder{
			Id:          "chaos-ticket-1",
			DisplayName: item.GetRequest().GetDisplayName(),
		}.Build()
		out = append(out, v2.TicketsServiceCreateTicketResponse_builder{Ticket: ticket}.Build())
	}
	return v2.TicketsServiceBulkCreateTicketsResponse_builder{Tickets: out}.Build(), nil
}

func (b *Builder) BulkGetTickets(
	ctx context.Context,
	request *v2.TicketsServiceBulkGetTicketsRequest,
) (*v2.TicketsServiceBulkGetTicketsResponse, error) {
	out := make([]*v2.TicketsServiceGetTicketResponse, 0, len(request.GetTicketRequests()))
	for _, item := range request.GetTicketRequests() {
		ticket, _, err := b.GetTicket(ctx, item.GetId())
		if err != nil {
			return nil, err
		}
		out = append(out, v2.TicketsServiceGetTicketResponse_builder{Ticket: ticket}.Build())
	}
	return v2.TicketsServiceBulkGetTicketsResponse_builder{Tickets: out}.Build(), nil
}

type eventFeed struct{}

func (f *eventFeed) EventFeedMetadata(context.Context) *v2.EventFeedMetadata {
	return v2.EventFeedMetadata_builder{
		Id:                  "chaos-events",
		SupportedEventTypes: []v2.EventType{v2.EventType_EVENT_TYPE_RESOURCE_CHANGE},
	}.Build()
}

func (f *eventFeed) ListEvents(
	context.Context,
	*timestamppb.Timestamp,
	*pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return nil, &pagination.StreamState{}, nil, nil
}

type deletableSyncer struct {
	*resourceSyncer
}

func (s *deletableSyncer) Delete(
	context.Context,
	*v2.ResourceId,
	*v2.ResourceId,
) (annotations.Annotations, error) {
	return nil, nil
}

type fullSyncer struct {
	*resourceSyncer
}

func (s *fullSyncer) Grant(
	_ context.Context,
	principal *v2.Resource,
	entitlement *v2.Entitlement,
) ([]*v2.Grant, annotations.Annotations, error) {
	return []*v2.Grant{
		v2.Grant_builder{Entitlement: entitlement, Principal: principal}.Build(),
	}, nil, nil
}

func (s *fullSyncer) Revoke(context.Context, *v2.Grant) (annotations.Annotations, error) {
	return nil, nil
}

func (s *fullSyncer) Create(
	_ context.Context,
	input *v2.Resource,
) (*v2.Resource, annotations.Annotations, error) {
	return proto.Clone(input).(*v2.Resource), nil, nil
}

func (s *fullSyncer) Delete(
	context.Context,
	*v2.ResourceId,
	*v2.ResourceId,
) (annotations.Annotations, error) {
	return nil, nil
}

func (s *fullSyncer) CreateAccount(
	context.Context,
	*v2.AccountInfo,
	*v2.LocalCredentialOptions,
) (connectorbuilder.CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	account, err := firstResource(s.run, FullCapabilityResourceTypeID)
	if err != nil {
		return nil, nil, nil, err
	}
	result := v2.CreateAccountResponse_SuccessResult_builder{
		IsCreateAccountResult: true,
		Resource:              account,
	}.Build()
	return result, nil, nil, nil
}

func (s *fullSyncer) CreateAccountCapabilityDetails(
	context.Context,
) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	option := v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY
	return v2.CredentialDetailsAccountProvisioning_builder{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{option},
		PreferredCredentialOption:  option,
	}.Build(), nil, nil
}

func (s *fullSyncer) Rotate(
	context.Context,
	*v2.ResourceId,
	*v2.LocalCredentialOptions,
) ([]*v2.PlaintextData, annotations.Annotations, error) {
	return []*v2.PlaintextData{
		v2.PlaintextData_builder{Name: "api-key", Bytes: []byte("chaos-secret")}.Build(),
	}, nil, nil
}

func (s *fullSyncer) RotateCapabilityDetails(
	context.Context,
) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error) {
	option := v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY
	return v2.CredentialDetailsCredentialRotation_builder{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{option},
		PreferredCredentialOption:  option,
	}.Build(), nil, nil
}

func (s *fullSyncer) IssueCapabilityDetails(
	context.Context,
) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	option := v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN
	return v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			v2.CredentialIssueOptionDescriptor_builder{
				Option:               option,
				ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
				SecretResourceTypeId: IssuedSecretResourceTypeID,
			}.Build(),
		},
		PreferredOption: option,
	}.Build(), nil, nil
}

func (s *fullSyncer) Issue(
	_ context.Context,
	input *connectorbuilder.CredentialIssueInput,
) (*connectorbuilder.CredentialIssueOutput, error) {
	traitOpts := []rs.SecretTraitOption{rs.WithSecretIdentityID(input.IdentityID)}
	if input.ExpiresAt != nil {
		traitOpts = append(traitOpts, rs.WithSecretExpiresAt(input.ExpiresAt.AsTime()))
	}
	secretType := findResourceType(s.run.dataset(), IssuedSecretResourceTypeID)
	secret, err := rs.NewSecretResource("Issued Chaos Secret", secretType, input.RequestID, traitOpts)
	if err != nil {
		return nil, err
	}
	return &connectorbuilder.CredentialIssueOutput{
		Secret: secret,
		PlaintextData: []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: []byte("chaos-token")}.Build(),
		},
		ResourceMode: v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_VIRTUAL,
	}, nil
}

func findResourceType(dataset *Dataset, id string) *v2.ResourceType {
	for _, resourceType := range dataset.ResourceTypes {
		if resourceType.GetId() == id {
			return resourceType
		}
	}
	return nil
}
