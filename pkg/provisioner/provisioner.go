package provisioner

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type Provisioner struct {
	store     connectorstore.Reader
	connector types.ClientWrapper

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

func (p *Provisioner) grant(ctx context.Context) error {
	c, err := p.connector.C(ctx)
	if err != nil {
		return err
	}

	entitlement, err := p.store.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: p.grantEntitlementID,
	})
	if err != nil {
		return err
	}

	principal, err := p.store.GetResource(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			Resource:     p.grantPrincipalID,
			ResourceType: p.grantPrincipalType,
		},
	})
	if err != nil {
		return err
	}

	_, err = c.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Entitlement: entitlement,
		Principal:   principal,
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *Provisioner) revoke(ctx context.Context) error {
	c, err := p.connector.C(ctx)
	if err != nil {
		return err
	}

	grant, err := p.store.GetGrant(ctx, &reader_v2.GrantsReaderServiceGetGrantRequest{
		GrantId: p.revokeGrantID,
	})
	if err != nil {
		return err
	}

	entitlement, err := p.store.GetEntitlement(ctx, &reader_v2.EntitlementsReaderServiceGetEntitlementRequest{
		EntitlementId: grant.Entitlement.Id,
	})
	if err != nil {
		return err
	}

	principal, err := p.store.GetResource(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceRequest{
		ResourceId: grant.Principal.Id,
	})
	if err != nil {
		return err
	}

	_, err = c.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: &v2.Grant{
			Id:          grant.Id,
			Entitlement: entitlement,
			Principal:   principal,
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func NewGranter(store connectorstore.Reader, cw types.ClientWrapper, entitlementID string, principalID string, principalType string) *Provisioner {
	return &Provisioner{
		store:              store,
		connector:          cw,
		grantEntitlementID: entitlementID,
		grantPrincipalID:   principalID,
		grantPrincipalType: principalType,
	}
}

func NewRevoker(store connectorstore.Reader, cw types.ClientWrapper, grantID string) *Provisioner {
	return &Provisioner{
		store:         store,
		connector:     cw,
		revokeGrantID: grantID,
	}
}
