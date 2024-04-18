package provisioner

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwk"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type Provisioner struct {
	dbPath    string
	connector types.ConnectorClient

	store      connectorstore.Reader
	c1zManager c1zmanager.Manager

	grantEntitlementID string
	grantPrincipalID   string
	grantPrincipalType string

	revokeGrantID string

	createAccountLogin string
	createAccountEmail string

	deleteResourceID   string
	deleteResourceType string

	rotateCredentialsId   string
	rotateCredentialsType string
}

// makeCrypto is used by rotateCredentials and createAccount.
// FIXME(morgabra/ggreer): Huge hack for testing.
func makeCrypto(ctx context.Context) ([]byte, *v2.CredentialOptions, []*v2.EncryptionConfig, error) {
	// Default to generating a random key and random password that is 12 characters long
	provider, err := providers.GetEncryptionProvider(jwk.EncryptionProviderJwk)
	if err != nil {
		return nil, nil, nil, err
	}

	config, privateKey, err := provider.GenerateKey(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	opts := &v2.CredentialOptions{
		Options: &v2.CredentialOptions_RandomPassword_{
			RandomPassword: &v2.CredentialOptions_RandomPassword{
				Length: 20,
			},
		},
	}
	return privateKey, opts, []*v2.EncryptionConfig{config}, nil
}

func (p *Provisioner) Run(ctx context.Context) error {
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
	if p.store != nil {
		return p.store, nil
	}

	if p.c1zManager == nil {
		m, err := c1zmanager.New(ctx, p.dbPath)
		if err != nil {
			return nil, err
		}
		p.c1zManager = m
	}

	store, err := p.c1zManager.LoadC1Z(ctx)
	if err != nil {
		return nil, err
	}
	p.store = store

	return p.store, nil
}

func (p *Provisioner) Close(ctx context.Context) error {
	var err error
	if p.store != nil {
		storeErr := p.store.Close()
		if storeErr != nil {
			err = errors.Join(err, storeErr)
		}
		p.store = nil
	}

	if p.c1zManager != nil {
		managerErr := p.c1zManager.Close(ctx)
		if managerErr != nil {
			err = errors.Join(err, managerErr)
		}
		p.c1zManager = nil
	}

	if err != nil {
		return err
	}

	return nil
}

func (p *Provisioner) grant(ctx context.Context) error {
	store, err := p.loadStore(ctx)
	if err != nil {
		return err
	}

	entitlement, err := store.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: p.grantEntitlementID,
	})
	if err != nil {
		return err
	}

	principal, err := store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			Resource:     p.grantPrincipalID,
			ResourceType: p.grantPrincipalType,
		},
	})
	if err != nil {
		return err
	}

	resource := &v2.Resource{
		Id:          principal.Resource.Id,
		DisplayName: principal.Resource.DisplayName,
		Annotations: principal.Resource.Annotations,
		Description: principal.Resource.Description,
		ExternalId:  principal.Resource.ExternalId,
		// Omit parent resource ID so that behavior is the same as ConductorOne's provisioning mode
		ParentResourceId: nil,
	}

	_, err = p.connector.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Entitlement: entitlement.Entitlement,
		Principal:   resource,
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *Provisioner) revoke(ctx context.Context) error {
	store, err := p.loadStore(ctx)
	if err != nil {
		return err
	}

	grant, err := store.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: p.revokeGrantID,
	})
	if err != nil {
		return err
	}

	entitlement, err := store.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: grant.Grant.Entitlement.Id,
	})
	if err != nil {
		return err
	}

	principal, err := store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: grant.Grant.Principal.Id,
	})
	if err != nil {
		return err
	}
	resource := &v2.Resource{
		Id:          principal.Resource.Id,
		DisplayName: principal.Resource.DisplayName,
		Annotations: principal.Resource.Annotations,
		Description: principal.Resource.Description,
		ExternalId:  principal.Resource.ExternalId,
		// Omit parent resource ID so that behavior is the same as ConductorOne's provisioning mode
		ParentResourceId: nil,
	}

	_, err = p.connector.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: &v2.Grant{
			Id:          grant.Grant.Id,
			Entitlement: entitlement.Entitlement,
			Principal:   resource,
			Annotations: grant.Grant.Annotations,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *Provisioner) createAccount(ctx context.Context) error {
	l := ctxzap.Extract(ctx)
	var emails []*v2.AccountInfo_Email
	if p.createAccountEmail != "" {
		emails = append(emails, &v2.AccountInfo_Email{
			Address:   p.createAccountEmail,
			IsPrimary: true,
		})
	}

	_, opts, config, err := makeCrypto(ctx)
	if err != nil {
		return err
	}

	_, err = p.connector.CreateAccount(ctx, &v2.CreateAccountRequest{
		AccountInfo: &v2.AccountInfo{
			Emails: emails,
			Login:  p.createAccountLogin,
		},
		CredentialOptions: opts,
		EncryptionConfigs: config,
	})
	if err != nil {
		return err
	}

	l.Debug("account created", zap.String("login", p.createAccountLogin), zap.String("email", p.createAccountEmail))

	return nil
}

func (p *Provisioner) deleteResource(ctx context.Context) error {
	_, err := p.connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: &v2.ResourceId{
			Resource:     p.deleteResourceID,
			ResourceType: p.deleteResourceType,
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (p *Provisioner) rotateCredentials(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	_, opts, config, err := makeCrypto(ctx)
	if err != nil {
		return err
	}

	_, err = p.connector.RotateCredential(ctx, &v2.RotateCredentialRequest{
		ResourceId: &v2.ResourceId{
			Resource:     p.rotateCredentialsId,
			ResourceType: p.rotateCredentialsType,
		},
		CredentialOptions: opts,
		EncryptionConfigs: config,
	})
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

func NewCreateAccountManager(c types.ConnectorClient, dbPath string, login string, email string) *Provisioner {
	return &Provisioner{
		dbPath:             dbPath,
		connector:          c,
		createAccountLogin: login,
		createAccountEmail: email,
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
