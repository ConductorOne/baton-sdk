package dotc1z_test

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func setSyncToken(ctx context.Context, e *pebble.Engine, token string) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return errors.New("setSyncToken: no open sync")
	}
	existing, err := e.SyncRuns().GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return err
	}
	updated := proto.Clone(existing).(*v3.SyncRunRecord)
	updated.SetSyncToken(token)
	return e.SyncRuns().PutSyncRunRecord(ctx, updated)
}
