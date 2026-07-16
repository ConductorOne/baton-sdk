package sourcecache

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// Replay-compatibility key.
//
// Replay is OPTIONAL — a pure optimization — and is permitted only when
// the prior artifact and the current run match BYTE-EXACTLY on every
// component below. Any absent, unreadable, or mismatched component
// degrades the sync to cold (no-op lookup): a wrong cold sync does not
// exist, a wrong warm sync is silent bad data. Compatibility is never
// inferred from versions (semver says nothing about replay semantics);
// each component is an explicit declaration by the party that owns it.

// MaterializationPolicyGeneration is the SDK's replay-compatibility
// component: bump it when the SDK changes how response rows or their
// side effects MATERIALIZE into the store in a replay-visible way — row
// translation, scope stamping, index derivation semantics, replay-carried
// side effects (child scheduling), or the post-collection invariant
// policy. Bumping it costs every deployment exactly one cold sync after
// upgrade; NOT bumping it when materialization changed replays rows the
// new code would have written differently.
const MaterializationPolicyGeneration = "1"

// CompatKey is the replay-compatibility key recorded into every
// source-cache-enabled artifact and required to match exactly before
// that artifact may serve replay.
type CompatKey struct {
	// ConnectorCacheGeneration is the connector's declared cache
	// generation (SourceCacheCapability.cache_generation): scope
	// computation, validator semantics, row/id construction.
	ConnectorCacheGeneration string
	// ConnectorConfigFingerprint is the connector's declared digest of
	// config/permission inputs that change what upstream data it can see
	// (SourceCacheCapability.config_fingerprint).
	ConnectorConfigFingerprint string
	// SDKMaterializationGeneration is MaterializationPolicyGeneration at
	// record time.
	SDKMaterializationGeneration string
	// SyncSelectionFingerprint digests the SDK-side sync-shaping inputs
	// (enabled resource types, skip flags): a selection change means the
	// prior artifact's scopes cover a different row universe.
	SyncSelectionFingerprint string
}

// MismatchReason compares the current run's key against the previous
// artifact's recorded key and returns "" on an exact match, or a
// human-readable reason naming the FIRST mismatched component. Values
// are deliberately not echoed (fingerprints may derive from secrets);
// the component name is enough to act on.
func (k CompatKey) MismatchReason(prev CompatKey) string {
	switch {
	case k.ConnectorCacheGeneration != prev.ConnectorCacheGeneration:
		return "connector cache-generation changed since the previous artifact was recorded"
	case k.ConnectorConfigFingerprint != prev.ConnectorConfigFingerprint:
		return "connector configuration/permission fingerprint changed since the previous artifact was recorded"
	case k.SDKMaterializationGeneration != prev.SDKMaterializationGeneration:
		return "SDK materialization-policy generation changed since the previous artifact was recorded"
	case k.SyncSelectionFingerprint != prev.SyncSelectionFingerprint:
		return "sync-selection fingerprint (enabled types, filters) changed since the previous artifact was recorded"
	default:
		return ""
	}
}

// SelectionFingerprint digests the sync-shaping inputs into the
// SyncSelectionFingerprint component. Canonical: resource type ids are
// sorted and length-prefixed (no delimiter injection), flags appended
// explicitly. An empty selection (all types, nothing skipped) has a
// well-defined fingerprint too — "no selection" must still match only
// "no selection".
func SelectionFingerprint(resourceTypeIDs []string, skipEntitlementsAndGrants, skipGrants bool) string {
	sorted := append([]string{}, resourceTypeIDs...)
	sort.Strings(sorted)
	parts := make([]string, 0, len(sorted)+3)
	parts = append(parts, "v1\x00types:")
	for _, id := range sorted {
		parts = append(parts, strconv.Itoa(len(id))+":"+id+"\x00")
	}
	parts = append(parts,
		"skipEG:"+strconv.FormatBool(skipEntitlementsAndGrants),
		"\x00skipG:"+strconv.FormatBool(skipGrants),
	)
	sum := sha256.Sum256([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(sum[:])
}
