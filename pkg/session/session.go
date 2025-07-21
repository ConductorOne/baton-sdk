package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

// KeyPrefixDelimiter is the delimiter used to separate prefixes from keys in the session cache.
const KeyPrefixDelimiter = "::"

func WithSyncID(syncID string) sessions.SessionStoreOption {
	return func(ctx context.Context, bag *sessions.SessionStoreBag) error {
		bag.SyncID = syncID
		return nil
	}
}

func WithPrefix(prefix string) sessions.SessionStoreOption {
	return func(ctx context.Context, bag *sessions.SessionStoreBag) error {
		bag.Prefix = prefix
		return nil
	}
}

// applyOptions applies session cache options and returns a configured bag.
func applyOptions(ctx context.Context, opt ...sessions.SessionStoreOption) (*sessions.SessionStoreBag, error) {
	bag := &sessions.SessionStoreBag{}

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
