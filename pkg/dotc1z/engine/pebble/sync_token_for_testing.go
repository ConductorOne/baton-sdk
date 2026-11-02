package pebble

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// SetSyncTokenForTesting writes token onto the open sync's run record,
// including when a ledger is active. Panics outside a test binary.
func (e *Engine) SetSyncTokenForTesting(ctx context.Context, token string) error {
	if !testing.Testing() {
		panic("pebble engine: SetSyncTokenForTesting outside a test binary")
	}
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return errors.New("SetSyncTokenForTesting: no open sync")
	}
	existing, err := e.SyncRuns().GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return err
	}
	// Clone-and-mutate so lifecycle updates preserve every metadata field,
	// including fields introduced after this code was written.
	updated := proto.Clone(existing).(*v3.SyncRunRecord)
	updated.SetSyncToken(token)
	return e.SyncRuns().PutSyncRunRecord(ctx, updated)
}
