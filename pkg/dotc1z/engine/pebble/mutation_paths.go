package pebble

// The mutation-path registry.
//
// THE LAW THIS FILE ENCODES: bug count scales with (paths × obligations)
// when cross-cutting write obligations are enforced by call-site
// convention, and with (paths + obligations) when they are derived at
// choke points. Three generations of the same bug family prove it here:
// the replay ingestion path missed the fresh path's side effects (the
// I1–I4 invariants exist because of it), the deletion paths missed
// index/manifest/dirty obligations (the dropBatcher and the
// invalidation-with-first-delete contract exist because of it), and each
// was found by review after shipping. This registry is the
// sideEffectAnnotationCoverage trick applied to code paths: every file
// in this package that touches a RAW Pebble write primitive must declare
// what it writes, how it satisfies the cross-cutting obligations, and
// (where they exist in-package) the tests that pin its contracts.
// mutation_paths_test.go enforces both directions mechanically — an
// undeclared raw-writing file and a stale declaration both fail — so a
// NEW mutation path cannot ship without having this conversation in the
// PR that adds it.
//
// THE OBLIGATIONS every declaration must account for (explicitly or as
// n/a-with-reason):
//
//   - secondary indexes: by_principal / by_source_scope /
//     needs_expansion / by_parent entries staged with the row mutation
//     (or deliberately deferred to the seal build).
//   - digest invalidation: post-seal grant mutations must stage
//     per-entitlement digest invalidation (present-means-exact).
//   - source-cache manifest: deleting rows out from under a scope's
//     validator must invalidate the entry IN THE SAME BATCH as the
//     scope's first delete (see stageSourceCacheScopeInvalidationLocked).
//   - envelope save: mutations must survive Close — the wrapper dirty
//     bit plus the Engine.Mutated latch (withWriteAllowSealed) backstop.
//   - entitlement keyspace note: entitlement-keyspace mutations must
//     invalidate the bare-id lookup map, keyed on commits that LANDED.
//   - crash ordering: multi-batch operations must leave every WAL prefix
//     safe (durable markers, idempotent re-runs, obligation-first
//     batching).
//
// SYNCER-LEVEL MUTATION PATHS (they all funnel into the files below;
// listed here so the inventory has one home):
//
//   1. fresh response pages (Put*)            — oracle: the replay-
//      equivalence harness's cold legs + model-derived expected sets.
//   2. source-cache replay copy               — oracle: warm ≡ cold
//      (source_cache_equivalence_test.go) + halt-point sweep.
//   3. deletion family (tombstones, dangling drops, replay pre-clear)
//                                             — oracle: the dirty
//      scenario family + drop crash tests; all drops via dropBatcher.
//   4. raw-SST ingestion (ToPebble conversion, compactor merge)
//                                             — oracle: byte-equivalence
//      vs the canonical write path (TestToPebbleBulkImportEquivalence,
//      pkg/dotc1z).
//   5. expansion/synth writes                 — deferred index + fused
//      digest build at seal; durable pending markers.
//   6. external-match transform (delete carriers + write transformed)
//                                             — composite path over
//      grants.go primitives; deliberately does NOT invalidate manifests
//      (transformed grants replace carriers under the SAME scope, so
//      warm/cold converge — pinned by the harness's external-churn
//      divergence trap).
//   7. I3 repair copy (CopyResourceRowFrom)   — single-row ingestion
//      from the previous artifact.
//
// PRODUCT NON-GOALS the registry records so they are not re-opened by
// accident: SQLite artifacts are never source-cache replay sources
// (SourceCacheStore is Pebble-only; SQLite rows carry no scope stamps),
// and compacted artifacts are never replay sources (keep-newer merges
// that no input's validators describe; triple-gated by the compacted
// flag, manifest clearing, and token provenance).

// rawWriteDeclaration documents one file's raw write surface.
type rawWriteDeclaration struct {
	// Writes summarizes what the file mutates and on which path(s).
	Writes string
	// Obligations states how the cross-cutting obligations are satisfied
	// (or why they do not apply).
	Obligations string
	// PinnedBy lists IN-PACKAGE test functions pinning the file's write
	// contracts; existence is enforced. Cross-package oracles are named
	// in Obligations text instead.
	PinnedBy []string
}

// rawWriteRegistry: one entry per non-test file in this package that
// invokes a raw Pebble write primitive (NewBatch, db.Set, db.Delete,
// DeleteRange, db.Ingest, IngestAndExcise, db.Apply). Enforced by
// TestRawWriteRegistryCoversAllRawWritingFiles.
var rawWriteRegistry = map[string]rawWriteDeclaration{
	"assets.go": {
		Writes:      "asset rows (put/delete) on the fresh ingestion path",
		Obligations: "no secondary indexes, digests, or scope stamps; envelope save via store wrappers + Engine.Mutated backstop",
	},
	"bulk_import.go": {
		Writes: "raw-SST bulk ingestion (ToPebble conversion from sorted SQLite sources)",
		Obligations: "maintains grantIndexFamilies inline (incl. by_source_scope; SQLite sources carry no scope stamps — SQLite replay is a non-goal); " +
			"digests deliberately absent (derived at seal); byte-equivalence vs the canonical write path pinned by TestToPebbleBulkImportEquivalence (pkg/dotc1z)",
	},
	"deferred_index.go": {
		Writes: "seal-time by_principal index rebuild + primary-grant keyspace rebuild via IngestAndExcise",
		Obligations: "crash ordering via the durable deferred-index pending marker (interrupted build re-runs at resumed EndSync); " +
			"stats stash invalidated by drops (invalidateDeferredGrantStats)",
	},
	"digest.go": {
		Writes:      "grant digest nodes and invalidation ranges",
		Obligations: "present-means-exact: mutations stage invalidation for touched entitlements; build/repair own reconstruction",
	},
	"entitlements.go": {
		Writes:      "entitlement primary rows + by_source_scope index (fresh, replay overlay, tombstones)",
		Obligations: "scope index staged with the row; bare-id lookup map invalidated via noteEntitlementKeyspaceWrite keyed on landed commits",
	},
	"grant_digest.go": {
		Writes:      "digest keyspace drops (wholesale invalidation)",
		Obligations: "drop-first recovery: every crash converts to digests-absent, which present-means-exact treats as safe",
	},
	"grant_digest_build.go": {
		Writes: "seal-time by_entitlement_principal_hash index + digest construction (fused with the deferred pass)",
		Obligations: "crash ordering via the durable digest-build pending marker (crash mid-build drops digest state at next open/repair " +
			"instead of trusting partial roots)",
	},
	"grant_digest_repair.go": {
		Writes:      "digest repair for entitlements missing roots (post-seal mutation healing)",
		Obligations: "runs under the write lock end-to-end so no grant write interleaves mid-partition; trusts only present roots",
	},
	"grants.go": {
		Writes: "grant primary rows + inline indexes (needs_expansion, by_source_scope), synth-layer SST ingestion, deferred-index marker arming",
		Obligations: "by_principal deferred to seal (durable marker); post-seal mutations stage digest invalidation; " +
			"scope index staged with the row (replay equivalence depends on it)",
	},
	"id_index_format.go": {
		Writes:      "engine-meta markers for the id-index layout (deferred-index pending, digest-build pending)",
		Obligations: "markers ARE the crash-ordering mechanism for the seal builds; durable (survive restart) by design",
	},
	"id_index_migration.go": {
		Writes:      "on-open id-index layout migration via IngestAndExcise",
		Obligations: "runs before the store serves reads; migrated-on-open forces an envelope save so the migration is never repaid",
	},
	"index_migrations.go": {
		Writes:      "on-open secondary-index backfills",
		Obligations: "same on-open contract as the id-index migration: complete before serving, dirty-on-migrate",
	},
	"if_newer.go": {
		Writes:      "keep-newer merge primitive (compaction fold path)",
		Obligations: "fold outputs are never replay sources (manifest cleared + compacted flag + token provenance — see the non-goals above)",
	},
	"ingest_facts.go": {
		Writes:      "existence-bit fact markers (external-match grants seen, etc.)",
		Obligations: "facts are monotone hints re-derived from the store at consuming seams (I2 repair); losing one is repaired, never silent",
	},
	"ingest_repair.go": {
		Writes: "dangling-reference drops (I7/I8/I9 arms) — ALL row deletion flows through dropBatcher",
		Obligations: "the choke point: secondary indexes + digest invalidation derived from raw values; manifest invalidation rides the SAME batch " +
			"as each scope's first delete; chunked commits with the crash hook between chunks; entitlement keyspace note keyed on landed commits",
		PinnedBy: []string{
			"TestDanglingDropCrashBetweenChunksKeepsInvalidation",
			"TestDanglingDropCleanCompletionInvalidates",
			"TestManifestSnapshotInvalidationIsTyped",
		},
	},
	"keyspace_version.go": {
		Writes:      "keyspace version stamp (engine-meta)",
		Obligations: "written at store creation/migration boundaries only; no row obligations",
	},
	"resource_types.go": {
		Writes:      "resource type rows (fresh ingestion)",
		Obligations: "no secondary indexes or scope stamps; envelope save via wrappers + Mutated backstop",
	},
	"resources.go": {
		Writes:      "resource primary rows + by_parent and by_source_scope indexes (fresh, replay, tombstones, I3 repair copy)",
		Obligations: "indexes staged with the row; repair copy reuses the row's own index derivation",
	},
	"session_store.go": {
		Writes:      "session key/value rows",
		Obligations: "sync-scoped scratch state; no cross-cutting obligations beyond the envelope save",
	},
	"source_cache.go": {
		Writes: "source-cache manifest entries, replay copies (pre-clear + copy + recount), delta tombstones",
		Obligations: "replay copy is the scope's first writer (resume pre-clear restores that precondition; ResumedRowsCleared marks the store dirty); " +
			"post-copy recount fails partial copies before a manifest entry can seal them; tombstones delete rows + indexes together",
		PinnedBy: []string{
			"TestSourceCacheReplayResumeAfterOverlayCommit",
			"TestDeleteGrantsByPrincipalsInScope",
		},
	},
	"sync_runs.go": {
		Writes:      "sync run records (start/end/compacted/token stamps)",
		Obligations: "run records are the replay metadata gate (UsableAsReplaySource); compacted stamping is part of the fold's refusal contract",
	},
	"sync_stats_sidecar.go": {
		Writes:      "stats sidecar records",
		Obligations: "advisory (readers fall back to O(N) iteration); drop paths invalidate the deferred stash so counts never include dropped rows",
	},
}
