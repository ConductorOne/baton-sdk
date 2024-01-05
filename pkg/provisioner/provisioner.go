package provisioner

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	c1zmanager "github.com/conductorone/baton-sdk/pkg/dotc1z/manager"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/davecgh/go-spew/spew"
	"github.com/go-jose/go-jose/v3"
	"github.com/segmentio/ksuid"
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
}

func (p *Provisioner) Run(ctx context.Context) error {
	switch {
	case p.revokeGrantID != "":
		return p.revoke(ctx)
	case p.grantEntitlementID != "" && p.grantPrincipalID != "" && p.grantPrincipalType != "":
		return p.grant(ctx)
	case p.createAccountLogin != "" || p.createAccountEmail != "":
		return p.createAccount(ctx)
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

	_, err = p.connector.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Entitlement: entitlement.Entitlement,
		Principal:   principal.Resource,
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

	result, err := p.connector.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: &v2.Grant{
			Id:          grant.Grant.Id,
			Entitlement: entitlement.Entitlement,
			Principal:   principal.Resource,
			Annotations: grant.Grant.Annotations,
		},
	})
	if err != nil {
		return err
	}

	spew.Dump(result)
	return nil
}

func genKey() (*ecdsa.PrivateKey, *jose.JSONWebKey, []byte) {
	key, _ := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)

	kid := ksuid.New().String()

	jsonPubKey := &jose.JSONWebKey{
		Key:       key.Public(),
		KeyID:     kid,
		Use:       "enc",
		Algorithm: string(jose.ECDH_ES_A256KW),
	}
	marshalledPubKey, _ := jsonPubKey.MarshalJSON()

	return key, jsonPubKey, marshalledPubKey
}

func (p *Provisioner) createAccount(ctx context.Context) error {
	var emails []*v2.AccountInfo_Email
	if p.createAccountEmail != "" {
		emails = append(emails, &v2.AccountInfo_Email{
			Address:   p.createAccountEmail,
			IsPrimary: true,
		})
	}

	privKey, _, pubKeyJWKBytes := genKey()

	// create an encryption manager
	opts := &v2.CredentialOptions{}
	config := []*v2.EncryptionConfig{
		{
			Config: &v2.EncryptionConfig_PublicKeyConfig_{
				PublicKeyConfig: &v2.EncryptionConfig_PublicKeyConfig{
					PubKey: pubKeyJWKBytes,
				},
			},
		},
	}

	result, err := p.connector.CreateAccount(ctx, &v2.CreateAccountRequest{
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

	jwe, err := jose.ParseEncrypted(string(result.EncryptedData[0].EncryptedBytes))
	if err != nil {
		return err
	}
	plaintext, err := jwe.Decrypt(privKey)
	if err != nil {
		return err
	}
	spew.Dump(plaintext)
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

func NewCreateAccountManager(c types.ConnectorClient, dbPath string, login string, email string) *Provisioner {
	return &Provisioner{
		dbPath:             dbPath,
		connector:          c,
		createAccountLogin: login,
		createAccountEmail: email,
	}
}
