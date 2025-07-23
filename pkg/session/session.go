package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types"
)

// WithSyncID returns a SessionCacheOption that sets the sync ID for the operation.
func SetSyncIDInContext(ctx context.Context, syncID string) (context.Context, error) {
	// Check if syncID is already set in the context
	if existingSyncID, ok := ctx.Value(types.SyncIDKey{}).(string); ok && existingSyncID != "" {
		if existingSyncID != syncID {
			return ctx, fmt.Errorf("syncID mismatch: context already has syncID %q, cannot set to %q", existingSyncID, syncID)
		}
		// If they match, return the context unchanged
		return ctx, nil
	}
	return context.WithValue(ctx, types.SyncIDKey{}, syncID), nil
}

// GetSyncIDFromContext retrieves the sync ID from the context, returning empty string if not found.
func GetSyncIDFromContext(ctx context.Context) string {
	if syncID, ok := ctx.Value(types.SyncIDKey{}).(string); ok {
		return syncID
	}
	return ""
}

func WithSyncID(syncID string) types.SessionCacheOption {
	return func(ctx context.Context, bag *types.SessionCacheBag) error {
		bag.SyncID = syncID
		return nil
	}
}

func WithPrefix(prefix string) types.SessionCacheOption {
	return func(ctx context.Context, bag *types.SessionCacheBag) error {
		bag.Prefix = prefix
		return nil
	}
}
