package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types"
)

// KeyPrefixDelimiter is the delimiter used to separate prefixes from keys in the session cache.
const KeyPrefixDelimiter = "::"

// GetSession retrieves the session cache instance from the context.
// Returns an error if no session cache is found in the context.
func GetSession(ctx context.Context) (types.SessionCache, error) {
	if sessionCache, ok := ctx.Value(types.SessionCacheKey{}).(types.SessionCache); ok {
		return sessionCache, nil
	}
	return nil, fmt.Errorf("no session cache found in context")
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

// GetSyncIDFromContext retrieves the sync ID from the context, returning empty string if not found.
func GetSyncIDFromContext(ctx context.Context) string {
	if syncID, ok := ctx.Value(types.SyncIDKey{}).(string); ok {
		return syncID
	}
	return ""
}

// applyOptions applies session cache options and returns a configured bag.
func applyOptions(ctx context.Context, opt ...types.SessionCacheOption) (*types.SessionCacheBag, error) {
	bag := &types.SessionCacheBag{}

	for _, option := range opt {
		err := option(ctx, bag)
		if err != nil {
			return nil, err
		}
	}

	if bag.SyncID == "" {
		bag.SyncID = GetSyncIDFromContext(ctx)
	}
	if bag.SyncID == "" {
		return nil, fmt.Errorf("no syncID set on context or in options")
	}

	return bag, nil
}
