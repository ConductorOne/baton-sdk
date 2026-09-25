package pebble

import (
	"bytes"
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

func (e *Engine) BoundSyncUnstarted(ctx context.Context) (bool, error) {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if e.db == nil {
		return false, ErrEngineClosing
	}
	id := e.CurrentSyncID()
	if id == "" {
		return false, nil
	}
	run, err := e.GetSyncRunRecord(ctx, id)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return false, err
	}
	if run.GetEndedAt() != nil || run.GetSyncToken() != "" {
		return false, nil
	}
	_, closer, err := e.db.Get(ledgerArchiveKey())
	if err == nil {
		return false, closer.Close()
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return false, err
	}
	it, err := e.db.NewIter(nil)
	if err != nil {
		return false, err
	}
	defer it.Close()
	for _, bounds := range [][2]byte{
		{rawdb.TypeResourceType, rawdb.TypeSyncRun},
		{rawdb.TypeIndex, rawdb.TypeSession},
		{rawdb.TypeDigest, rawdb.TypeEngineMeta},
	} {
		lo, hi := []byte{rawdb.VersionV3, bounds[0]}, []byte{rawdb.VersionV3, bounds[1]}
		found := it.SeekGE(lo)
		if err := it.Error(); err != nil {
			return false, err
		}
		if found && bytes.Compare(it.Key(), hi) < 0 {
			return false, nil
		}
	}
	return true, nil
}
