// Package sourcecache defines the connector-facing source-caching contract.
//
// Source caching lets a connector skip work it has already done in a previous
// sync by recording an ETag for each cacheable upstream page. On the next sync
// the connector asks the SDK whether it has a prior ETag for the same scope
// and, if so, sends a conditional upstream request. When upstream returns
// "not modified" the SDK replays the previously-stored rows from the source
// c1z and the connector returns no new rows for that page.
//
// The connector author interacts with this package via three things:
//
//   - The [Lookup] interface, called per page before the upstream request.
//     In subprocess mode this is implemented by an in-process gRPC client
//     (see [GRPCLookup]) that talks to the SDK; in in-process mode it is
//     the syncer's own lookup. Connectors should treat [NoopLookup] as the
//     correct "no previous sync" answer.
//   - The [SourceCacheCapability] validate-response annotation
//     (pb/c1/connector/v2) to opt in.
//   - The [v2.SourceCacheEntry] response annotation (to record the new
//     ETag for a page) and [v2.SourceCacheReplayRequest] (to ask the SDK
//     to replay the prior page's rows).
//
// Wire-format details are described in
// proto/c1/connectorapi/baton/v1/source_cache.proto.
package sourcecache

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// keyVersion prefixes every source cache key so we can rotate the encoding
// without confusing it with prior keys still sitting in a c1z. Bump when
// changing the parts after the prefix (e.g. switching from base64 raw URL to
// some other encoding, or adding extra fields).
const keyVersion = "v2"

var scopeHashRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// RowKind partitions source-cache entries by which c1z table they back.
// A single scope hash can have a different ETag per kind, so lookups and
// writes always carry the row kind.
type RowKind string

const (
	RowKindResources    RowKind = "resources"
	RowKindEntitlements RowKind = "entitlements"
	RowKindGrants       RowKind = "grants"
)

// Entry is a previous-sync source-cache record. Key is the opaque cache
// key (encoding scope_hash + etag) that the connector echoes back on a
// replay, and ETag is the value to send as If-None-Match upstream.
type Entry struct {
	Key  string
	ETag string
}

// Lookup is the connector-facing source-cache interface.
//
// Connectors compute a canonical source scope for each cacheable upstream
// request/page, hash it, and call LookupPreviousSourceCache before making
// that request. A hit provides the previous ETag and key needed to send
// If-None-Match. If the upstream returns "not modified", the connector
// returns a SourceCacheReplayRequest annotation with the key; otherwise it
// returns rows and a SourceCacheEntry annotation with a key built from the
// new ETag.
//
// Implementations must be safe to call concurrently.
type Lookup interface {
	LookupPreviousSourceCache(ctx context.Context, rowKind RowKind, scopeHashHex string) (Entry, bool, error)
}

// SetLookup is implemented by connector clients/servers that can receive a
// source-cache lookup implementation from the sync runner. The SDK calls
// SetSourceCache(lookup) at the start of each sync and SetSourceCache(nil)
// when the sync ends to prevent late RPCs from reading stale state.
type SetLookup interface {
	SetSourceCache(ctx context.Context, lookup Lookup)
}

// NoopLookup is the zero-value Lookup: always reports "no previous entry".
// Used when source caching is not configured for the current sync.
type NoopLookup struct{}

func (NoopLookup) LookupPreviousSourceCache(context.Context, RowKind, string) (Entry, bool, error) {
	return Entry{}, false, nil
}

// ValidateRowKind returns an error if rowKind is not one of the known
// RowKind* constants.
func ValidateRowKind(rowKind RowKind) error {
	switch rowKind {
	case RowKindResources, RowKindEntitlements, RowKindGrants:
		return nil
	default:
		return fmt.Errorf("invalid source cache row kind: %q", rowKind)
	}
}

// ValidateScopeHash returns an error if scopeHashHex is not 64 lowercase hex
// characters (i.e. a hex-encoded sha256).
func ValidateScopeHash(scopeHashHex string) error {
	if !scopeHashRe.MatchString(scopeHashHex) {
		return fmt.Errorf("invalid source cache scope hash: %q", scopeHashHex)
	}
	return nil
}

// BuildKey constructs a source cache key from a scope hash and ETag. The
// returned string is what the connector emits via SourceCacheEntry on a
// fresh page and echoes back via SourceCacheReplayRequest on a cache hit.
//
// The key is structured as "v<version>:<scope_hash_hex>:<base64url_etag>"
// so both halves can be recovered with ParseKey. ETags are URL-safe-base64
// encoded so they can contain quotes, slashes, and other characters
// upstreams sometimes put in ETag headers without breaking the key format.
func BuildKey(scopeHashHex string, etag string) (string, error) {
	if err := ValidateScopeHash(scopeHashHex); err != nil {
		return "", err
	}
	if etag == "" {
		return "", errors.New("source cache etag is required")
	}
	encodedETag := base64.RawURLEncoding.EncodeToString([]byte(etag))
	return fmt.Sprintf("%s:%s:%s", keyVersion, scopeHashHex, encodedETag), nil
}

// ParseKey reverses BuildKey, returning the scope hash and decoded ETag.
// Keys produced by an unrecognized version are rejected.
func ParseKey(key string) (string, string, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return "", "", fmt.Errorf("invalid source cache key format: %q", key)
	}
	if parts[0] != keyVersion {
		return "", "", fmt.Errorf("unsupported source cache key version: %q", parts[0])
	}
	scopeHashHex := parts[1]
	if err := ValidateScopeHash(scopeHashHex); err != nil {
		return "", "", err
	}
	if parts[2] == "" {
		return "", "", errors.New("source cache key etag segment is required")
	}
	etagBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", fmt.Errorf("invalid source cache etag encoding: %w", err)
	}
	etag := string(etagBytes)
	if etag == "" {
		return "", "", errors.New("source cache etag is required")
	}
	return scopeHashHex, etag, nil
}
