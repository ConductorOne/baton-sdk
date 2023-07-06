package provisioner

import (
	"context"
	"errors"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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
}

func (p *Provisioner) Run(ctx context.Context) error {
	if p.revokeGrantID != "" {
		return p.revoke(ctx)
	}

	return p.grant(ctx)
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

	_, err = p.connector.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
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
