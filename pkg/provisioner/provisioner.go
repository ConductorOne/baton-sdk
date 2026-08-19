package provisioner

import (
	"context"
	"errors"

	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/types"
)

var tracer = otel.Tracer("baton-sdk/pkg.provisioner")

type Provisioner struct {
	dbPath    string
	connector types.ConnectorClient

	store connectorstore.Reader

	grantEntitlementID string
	grantPrincipalID   string
	grantPrincipalType string

	revokeGrantID string

	createAccountLogin        string
	createAccountEmail        string
	createAccountProfile      *structpb.Struct
	createAccountResourceType string

	deleteResourceID   string
	deleteResourceType string

	rotateCredentialsId   string
	rotateCredentialsType string
}

// makeCrypto is used by rotateCredentials and createAccount.
// FIXME(morgabra/ggreer): Huge hack for testing.
func makeCrypto(ctx context.Context) (*v2.CredentialOptions, []*v2.EncryptionConfig, error) {
	// Default to generating a random key and random password that is 12 characters long
	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	if err != nil {
		return nil, nil, err
	}

	config, _, err := provider.GenerateKey(ctx)
	if err != nil {
		return nil, nil, err
	}

	opts := v2.CredentialOptions_builder{
		RandomPassword: v2.CredentialOptions_RandomPassword_builder{
			Length: 20,
		}.Build(),
	}.Build()
	return opts, []*v2.EncryptionConfig{config}, nil
}

func (p *Provisioner) Run(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Provisioner.Run")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	switch {
	case p.revokeGrantID != "":
		return p.revoke(ctx)
	case p.grantEntitlementID != "" && p.grantPrincipalID != "" && p.grantPrincipalType != "":
		return p.grant(ctx)
	case p.createAccountLogin != "" || p.createAccountEmail != "":
		return p.createAccount(ctx)
	case p.deleteResourceID != "" && p.deleteResourceType != "":
		return p.deleteResource(ctx)
	case p.rotateCredentialsId != "" && p.rotateCredentialsType != "":
		return p.rotateCredentials(ctx)
	default:
		return errors.New("unknown provisioning action")
	}
}

func (p *Provisioner) loadStore(ctx context.Context) (connectorstore.Reader, error) {
	ctx, span := tracer.Start(ctx, "Provisioner.loadStore")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if p.store != nil {
		return p.store, nil
	}

	store, err := dotc1z.NewStore(ctx, p.dbPath)
	if err != nil {
		return nil, err
	}
	p.store = store

	return p.store, nil
}

func (p *Provisioner) Close(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Provisioner.Close")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	if p.store != nil {
		storeErr := p.store.Close(ctx)
		if storeErr != nil {
			err = errors.Join(err, storeErr)
		}
		p.store = nil
	}

	if err != nil {
		return err
	}

	return nil
}

// hydrateEntitlementResource returns a copy of e with Resource replaced by
// resource. The store's entitlement read (GetEntitlement, via
// V3EntitlementToV2 on the Pebble engine) returns Resource as an
// identity-only stub; callers that already fetched the full Resource
// separately (e.g. for the external-resource annotation check) must
// splice it back in before handing the entitlement to a connector's
// Grant/Revoke, or the connector never sees the resource's Profile,
// DisplayName, etc.
//
// GrantableTo is untouched: V3EntitlementToV2 also stubs it down to
// ResourceType id-only entries, and this helper does not re-hydrate those —
// a connector reading entitlement.GrantableTo display names/traits in
// Grant/Revoke still sees stubs on the Pebble engine.
func hydrateEntitlementResource(e *v2.Entitlement, resource *v2.Resource) *v2.Entitlement {
	if e == nil {
		return nil
	}
	hydrated, ok := proto.Clone(e).(*v2.Entitlement)
	if !ok {
		return e
	}
	hydrated.SetResource(resource)
	return hydrated
}

func (p *Provisioner) grant(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Provisioner.grant")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	store, err := p.loadStore(ctx)
	if err != nil {
		return err
	}

	entitlement, err := store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: p.grantEntitlementID,
	}.Build())
	if err != nil {
		return err
	}

	entitlementResource, err := store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: entitlement.GetEntitlement().GetResource().GetId(),
	}.Build())
	if err != nil {
		return err
	}
	entitlementResourceAnnos := entitlementResource.GetResource().GetAnnotations()
	rAnnos := annotations.Annotations(entitlementResourceAnnos)
	if rAnnos.Contains(&v2.BatonID{}) {
		return errors.New("cannot grant entitlement on external resource")
	}

	principal, err := store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			Resource:     p.grantPrincipalID,
			ResourceType: p.grantPrincipalType,
		}.Build(),
	}.Build())
	if err != nil {
		return err
	}

	_, err = p.connector.Grant(ctx, v2.GrantManagerServiceGrantRequest_builder{
		Entitlement: hydrateEntitlementResource(entitlement.GetEntitlement(), entitlementResource.GetResource()),
		Principal:   principal.GetResource(),
	}.Build())
	if err != nil {
		return err
	}

	return nil
}

func (p *Provisioner) revoke(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Provisioner.revoke")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	store, err := p.loadStore(ctx)
	if err != nil {
		return err
	}

	grant, err := store.GetGrant(ctx, reader_v2.GrantsReaderServiceGetGrantRequest_builder{
		GrantId: p.revokeGrantID,
	}.Build())
	if err != nil {
		return err
	}

	entitlement, err := store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: grant.GetGrant().GetEntitlement().GetId(),
	}.Build())
	if err != nil {
		return err
	}

	principal, err := store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: grant.GetGrant().GetPrincipal().GetId(),
	}.Build())
	if err != nil {
		return err
	}

	entitlementResource, err := store.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: entitlement.GetEntitlement().GetResource().GetId(),
	}.Build())
	if err != nil {
		return err
	}
	entitlementResourceAnnos := entitlementResource.GetResource().GetAnnotations()
	rAnnos := annotations.Annotations(entitlementResourceAnnos)
	if rAnnos.Contains(&v2.BatonID{}) {
		return errors.New("cannot revoke grant on external resource")
	}

	_, err = p.connector.Revoke(ctx, v2.GrantManagerServiceRevokeRequest_builder{
		Grant: v2.Grant_builder{
			Id:          grant.GetGrant().GetId(),
			Entitlement: hydrateEntitlementResource(entitlement.GetEntitlement(), entitlementResource.GetResource()),
			Principal:   principal.GetResource(),
			Annotations: grant.GetGrant().GetAnnotations(),
		}.Build(),
	}.Build())
	if err != nil {
		return err
	}

	return nil
}

func (p *Provisioner) createAccount(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Provisioner.createAccount")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)
	var emails []*v2.AccountInfo_Email
	if p.createAccountEmail != "" {
		emails = append(emails, v2.AccountInfo_Email_builder{
			Address:   p.createAccountEmail,
			IsPrimary: true,
		}.Build())
	}

	opts, config, err := makeCrypto(ctx)
	if err != nil {
		return err
	}

	_, err = p.connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
		ResourceTypeId: p.createAccountResourceType,
		AccountInfo: v2.AccountInfo_builder{
			Emails:  emails,
			Login:   p.createAccountLogin,
			Profile: p.createAccountProfile,
		}.Build(),
		CredentialOptions: opts,
		EncryptionConfigs: config,
	}.Build())
	if err != nil {
		return err
	}

	l.Debug("account created",
		zap.String("login", p.createAccountLogin),
		zap.String("email", p.createAccountEmail),
		zap.String("resource_type", p.createAccountResourceType),
	)

	return nil
}

func (p *Provisioner) deleteResource(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Provisioner.deleteResource")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	_, err = p.connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			Resource:     p.deleteResourceID,
			ResourceType: p.deleteResourceType,
		}.Build(),
	}.Build())
	if err != nil {
		return err
	}
	return nil
}

func (p *Provisioner) rotateCredentials(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Provisioner.rotateCredentials")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)

	opts, config, err := makeCrypto(ctx)
	if err != nil {
		return err
	}

	_, err = p.connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
		ResourceId: v2.ResourceId_builder{
			Resource:     p.rotateCredentialsId,
			ResourceType: p.rotateCredentialsType,
		}.Build(),
		CredentialOptions: opts,
		EncryptionConfigs: config,
	}.Build())
	if err != nil {
		return err
	}

	l.Debug("credentials rotated", zap.String("resource", p.rotateCredentialsId), zap.String("resource type", p.rotateCredentialsType))

	return nil
}

func NewGranter(c types.ConnectorClient, dbPath string, entitlementID string, principalID string, principalType string) *Provisioner {
	return &Provisioner{
		dbPath:             dbPath,
		connector:          c,
		grantEntitlementID: entitlementID,
		grantPrincipalID:   principalID,
		grantPrincipalType: principalType,
	}
}

func NewRevoker(c types.ConnectorClient, dbPath string, grantID string) *Provisioner {
	return &Provisioner{
		dbPath:        dbPath,
		connector:     c,
		revokeGrantID: grantID,
	}
}

func NewResourceDeleter(c types.ConnectorClient, dbPath string, resourceId string, resourceType string) *Provisioner {
	return &Provisioner{
		dbPath:             dbPath,
		connector:          c,
		deleteResourceID:   resourceId,
		deleteResourceType: resourceType,
	}
}

func NewCreateAccountManager(c types.ConnectorClient, dbPath string, login string, email string, profile *structpb.Struct, resourceType string) *Provisioner {
	return &Provisioner{
		dbPath:                    dbPath,
		connector:                 c,
		createAccountLogin:        login,
		createAccountEmail:        email,
		createAccountProfile:      profile,
		createAccountResourceType: resourceType,
	}
}

func NewCredentialRotator(c types.ConnectorClient, dbPath string, resourceId string, resourceType string) *Provisioner {
	return &Provisioner{
		dbPath:                dbPath,
		connector:             c,
		rotateCredentialsId:   resourceId,
		rotateCredentialsType: resourceType,
	}
}
