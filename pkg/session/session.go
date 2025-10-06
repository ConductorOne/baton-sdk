package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types"
)

// KeyPrefixDelimiter is the delimiter used to separate prefixes from keys in the session cache.
const KeyPrefixDelimiter = "::"

func WithSyncID(syncID string) types.SessionOption {
	return func(ctx context.Context, bag *types.SessionBag) error {
		bag.SyncID = syncID
		return nil
	}
}

func WithPrefix(prefix string) types.SessionOption {
	return func(ctx context.Context, bag *types.SessionBag) error {
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
func applyOptions(ctx context.Context, opt ...types.SessionOption) (*types.SessionBag, error) {
	bag := &types.SessionBag{}

	for _, option := range opt {
		err := option(ctx, bag)
		if err != nil {
			return nil, err
		}
	}

	if bag.SyncID == "" {
		return nil, fmt.Errorf("no syncID set in options")
	}

	return bag, nil
}
