package v1

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

var _ connectorstore.Writer = (*V1File)(nil)

func (c *V1File) StartSync(ctx context.Context) (string, bool, error) {
	return c.engine.StartSync(ctx)
}

func (c *V1File) StartNewSync(ctx context.Context) (string, error) {
	return c.engine.StartNewSync(ctx)
}

func (c *V1File) StartNewSyncV2(ctx context.Context, syncType string, parentSyncID string) (string, error) {
	return c.engine.StartNewSyncV2(ctx, syncType, parentSyncID)
}

func (c *V1File) SetCurrentSync(ctx context.Context, syncID string) error {
	return c.engine.SetCurrentSync(ctx, syncID)
}

func (c *V1File) CurrentSyncStep(ctx context.Context) (string, error) {
	return c.engine.CurrentSyncStep(ctx)
}

func (c *V1File) CheckpointSync(ctx context.Context, syncToken string) error {
	return c.engine.CheckpointSync(ctx, syncToken)
}

func (c *V1File) EndSync(ctx context.Context) error {
	return c.engine.EndSync(ctx)
}

func (c *V1File) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	return c.engine.PutAsset(ctx, assetRef, contentType, data)
}

func (c *V1File) Cleanup(ctx context.Context) error {
	return c.engine.Cleanup(ctx)
}

func (c *V1File) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	return c.engine.PutGrants(ctx, grants...)
}

func (c *V1File) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	return c.engine.PutResourceTypes(ctx, resourceTypes...)
}

func (c *V1File) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	return c.engine.PutResources(ctx, resources...)
}

func (c *V1File) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	return c.engine.PutEntitlements(ctx, entitlements...)
}

func (c *V1File) DeleteGrant(ctx context.Context, grantId string) error {
	return c.engine.DeleteGrant(ctx, grantId)
}

func (c *V1File) PutResourceTypesIfNewer(ctx context.Context, resourceTypesObjs ...*v2.ResourceType) error {
	return c.engine.PutResourceTypesIfNewer(ctx, resourceTypesObjs...)
}

func (c *V1File) PutResourcesIfNewer(ctx context.Context, resourceObjs ...*v2.Resource) error {
	return c.engine.PutResourcesIfNewer(ctx, resourceObjs...)
}

func (c *V1File) PutGrantsIfNewer(ctx context.Context, bulkGrants ...*v2.Grant) error {
	return c.engine.PutGrantsIfNewer(ctx, bulkGrants...)
}

func (c *V1File) PutEntitlementsIfNewer(ctx context.Context, entitlementObjs ...*v2.Entitlement) error {
	return c.engine.PutEntitlementsIfNewer(ctx, entitlementObjs...)
}
