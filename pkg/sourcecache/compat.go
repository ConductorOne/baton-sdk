package sourcecache

// MaterializationPolicyGeneration identifies the SDK's replayed-row
// materialization policy: the rules by which replayed rows are copied,
// transformed (e.g. expander-written Sources on direct grants are stripped
// at copy time), and re-indexed. Bump it whenever those rules change in a
// way that makes rows materialized by a PRIOR SDK unsafe to replay
// verbatim. Replay is permitted only when the previous artifact recorded a
// byte-identical generation — both in its stored CompatKey and in the c1z
// envelope-manifest witness (the CO-017 cross-version fold fence) — so a
// bump colds every pre-bump artifact exactly once per chain.
const MaterializationPolicyGeneration = "1"

// CompatKey is a sync's replay-compatibility key (stored as
// SourceCacheCompatRecord — see proto/c1/storage/v3/records.proto). The
// current sync computes its own key and byte-compares it against the
// previous artifact's stored key on lookup install; ANY difference —
// including a missing record — degrades the sync to a cold run. Empty
// fields match only empty fields.
type CompatKey struct {
	// ConnectorCacheGeneration is SourceCacheCapability.cache_generation,
	// verbatim: the connector's explicit declaration of scope/validator/row
	// compatibility.
	ConnectorCacheGeneration string

	// ConnectorConfigFingerprint is SourceCacheCapability.config_fingerprint,
	// verbatim: a digest of every configuration input that changes what
	// upstream data the connector CAN see.
	ConnectorConfigFingerprint string

	// SDKMaterializationGeneration is MaterializationPolicyGeneration as of
	// the sync that wrote the record.
	SDKMaterializationGeneration string

	// SyncSelectionFingerprint digests the sync's selection shape (resource
	// type filter, skip flags): a sync that intentionally collects less
	// must not serve as the replay base for one that collects more.
	SyncSelectionFingerprint string
}
