package pebble

import (
	cpebble "github.com/cockroachdb/pebble/v2"

	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// foldCommitFailure is an explicit internal injection argument, not mutable
// engine state. Production callers pass nil; focused tests pass a deterministic
// failure into the same ownership/commit code.
type foldCommitFailure func() error

func commitFoldBatch(batch *enginepkg.FoldBatch, opts *cpebble.WriteOptions, before foldCommitFailure) error {
	if before != nil {
		if err := before(); err != nil {
			return err
		}
	}
	return batch.Commit(opts)
}
