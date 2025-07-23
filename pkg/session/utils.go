package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types"
)

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
