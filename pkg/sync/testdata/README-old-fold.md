# old-fold-mixed-version.c1z

A REAL mixed-version compaction artifact: produced by the merge-base
SDK's fold compactor (pre-source-cache, pre-compacted-flag; commit
`604dae09`) running over two source-cache-stamped inputs written by the
CURRENT SDK. Frozen here so version-skew behavior is tested against
actual old-binary output bytes, not a synthesis of what we believe old
binaries did.

Measured properties (which make it the dangerous shape):

- run record: `type=full`, `compacted` UNSET (the old proto has no field);
- the base input's source-cache MANIFEST survived the fold intact,
  validators and all — the old engine copies keyspaces it does not know;
- the sync token carries the compaction provenance section
  (`CompactionTokenStats`), which every compactor version has written.

So every record-level replay gate passes and only the token-provenance
gate refuses it. `source_cache_skew_fixture_test.go` pins that refusal.

Regeneration (only if the fixture must be rebuilt):

1. `BATON_GEN_SKEW_INPUTS=/tmp/skew-gen go test -run TestGenerateSkewInputs ./pkg/sync/`
2. `git worktree add /tmp/skew-base <merge-base-sha>`; copy
   `cmd/genskewfold` from the worktree history (or re-create per the
   test's README reference) and run
   `go run ./cmd/genskewfold /tmp/skew-gen/base.c1z /tmp/skew-gen/newer.c1z /tmp/skew-gen/out`
   inside the worktree.
3. Copy the output over this file.

Future releases should extend this corpus via a freeze-at-tag step
(generate artifacts with the just-released binary) rather than in CI,
which runs before tags exist.
