package v2

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

var _ connectorstore.Writer = (*V2File)(nil)

func (c *V2File) StartSync(ctx context.Context) (string, bool, error) {
	if c.engine == nil {
		return "", false, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.StartSync(ctx)
}

func (c *V2File) StartNewSync(ctx context.Context) (string, error) {
	if c.engine == nil {
		return "", fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.StartNewSync(ctx)
}

func (c *V2File) StartNewSyncV2(ctx context.Context, syncType string, parentSyncID string) (string, error) {
	if c.engine == nil {
		return "", fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.StartNewSyncV2(ctx, syncType, parentSyncID)
}

func (c *V2File) SetCurrentSync(ctx context.Context, syncID string) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.SetCurrentSync(ctx, syncID)
}

func (c *V2File) CurrentSyncStep(ctx context.Context) (string, error) {
	if c.engine == nil {
		return "", fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.CurrentSyncStep(ctx)
}

func (c *V2File) CheckpointSync(ctx context.Context, syncToken string) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.CheckpointSync(ctx, syncToken)
}

func (c *V2File) EndSync(ctx context.Context) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.EndSync(ctx)
}

func (c *V2File) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutAsset(ctx, assetRef, contentType, data)
}

func (c *V2File) Cleanup(ctx context.Context) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.Cleanup(ctx)
}

func (c *V2File) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutGrants(ctx, grants...)
}

func (c *V2File) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutResourceTypes(ctx, resourceTypes...)
}

func (c *V2File) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutResources(ctx, resources...)
}

func (c *V2File) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutEntitlements(ctx, entitlements...)
}

func (c *V2File) DeleteGrant(ctx context.Context, grantId string) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.DeleteGrant(ctx, grantId)
}

func (c *V2File) PutResourceTypesIfNewer(ctx context.Context, resourceTypesObjs ...*v2.ResourceType) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutResourceTypesIfNewer(ctx, resourceTypesObjs...)
}

func (c *V2File) PutResourcesIfNewer(ctx context.Context, resourceObjs ...*v2.Resource) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutResourcesIfNewer(ctx, resourceObjs...)
}

func (c *V2File) PutGrantsIfNewer(ctx context.Context, bulkGrants ...*v2.Grant) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutGrantsIfNewer(ctx, bulkGrants...)
}

func (c *V2File) PutEntitlementsIfNewer(ctx context.Context, entitlementObjs ...*v2.Entitlement) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.PutEntitlementsIfNewer(ctx, entitlementObjs...)
}
