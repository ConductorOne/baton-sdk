package pebble

// testSeams aggregates every test-only injection point on the Engine
// behind a single field (Engine.test), so the production struct isn't
// littered with hook fields and new seams have one obvious home. All
// fields are nil/zero in production and only ever assigned by tests in
// this package; production code must treat them as read-only.
//
// These are deliberately plain fields rather than build-tag-gated
// machinery: a nil-func check is free, and tag-gating would break the
// default `go test ./...` workflow (the package under test would need
// the tag to compile the hook sites).
type testSeams struct {
	// digestBuildHook fires at named points inside
	// buildGrantDigestsFromSpill (grant_digest_build_crash_test.go);
	// digestNodeFlushBytes overrides the digest fold's batch
	// flush threshold — shared by the build's fold and the streaming
	// partition repair (repairOneGrantDigestPartitionLocked) — so a
	// small test dataset exercises the mid-stream commit paths.
	digestBuildHook      func(stage string) error
	digestNodeFlushBytes int

	// The deferred-marker arm/clear failure hooks moved to rawdb with
	// the marker itself (rawdb.SetDeferredMarkerTestHooks).

	// endSyncPreFlushHook, when non-nil, runs inside endSyncFinalize
	// IMMEDIATELY after the ended_at stamp commits — before the stats
	// sidecar write and the EndFreshSync durability flush. Tests
	// crash-clone the FS here to pin the WAL-prefix-durability
	// contract: a Sync commit's WAL fsync also hardens every earlier
	// NoSync page commit (sequential WAL; rotated WALs sync at
	// rotation), so a crash image containing the finished verdict
	// necessarily contains the pages — finished-but-incomplete is not
	// expressible. The hook sits directly after the stamp (not after
	// stats) so a stripped workload can make the stamp the ONLY Sync
	// between the pages and the cut, attributing the hardening to the
	// stamp itself rather than a neighboring Sync (review finding,
	// delta round: the original post-stats placement made that
	// attribution vacuous).
	endSyncPreFlushHook func()

	// recordCommitHook, when non-nil, runs immediately before a
	// DEFERRED-regime RecordBatch commit — the in-process analog of
	// that commit failing AFTER StageGrantPutDeferred already armed
	// the durable deferred-index marker. The obligations harness pins
	// the resulting state: marker armed (flag AND key — agreement
	// holds), zero rows committed, retry converges, sealed index
	// complete. Inline-regime commits arm nothing, so they carry no
	// post-arm obligation and no hook.
	recordCommitHook func() error

	// endSyncStampHook, when non-nil, runs immediately before the
	// ended_at stamp's PutSyncRunRecord commit in endSyncFinalize —
	// the in-process analog of the stamp commit failing. The
	// obligations harness pins that a failed stamp leaks nowhere: the
	// stored record stays unstamped and the sync stays discoverable
	// as unfinished (resumable).
	endSyncStampHook func() error
}
