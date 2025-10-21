package session

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
)

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
