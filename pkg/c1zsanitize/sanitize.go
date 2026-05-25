// Package c1zsanitize transforms a real .c1z snapshot into an
// identity-stripped copy whose graph topology, cardinalities, and
// annotation structure are preserved. The output is suitable for
// shipping to internal development environments where the original
// customer data must not appear.
//
// The whole transform is driven by a single per-c1z HMAC-SHA256
// secret. Same input → same output within one c1z so cross-references
// stay coherent; different across c1zs whose secrets differ so an
// attacker holding multiple sanitized outputs cannot correlate them.
//
// v0.1 reads and writes the v1/v2 sqlite-zstd .c1z format via
// connectorstore.Reader / Writer. v3 c1z3 output will land in v0.2
// once the storage-engine-v4 PR stack merges.
package c1zsanitize

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Options configures a sanitization run.
type Options struct {
	// Secret is the per-c1z HMAC key. Must be at least MinSecretBytes.
	// The operator chooses whether to archive or discard it; the
	// sanitizer never persists it on its own.
	Secret []byte

	// TimestampAnchor is the wall-clock value the newest timestamp in
	// the source c1z lands on. All other timestamps shift by the same
	// delta so relative deltas are preserved. Defaults to time.Now()
	// when zero.
	TimestampAnchor time.Time

	// DropUnknownAnnotations controls behavior when an annotation's
	// Any type URL is not in the handler registry. When true (the
	// default), unknown annotations are dropped and a log line names
	// the type URL. When false, unknown annotations pass through
	// unchanged — convenient for development against new annotation
	// types, dangerous on real customer data.
	DropUnknownAnnotations bool
}

// Sanitize copies records from src to dst, transforming identifiers,
// names, free text, emails, and timestamps under the per-c1z secret.
// One destination sync is opened per source sync; parent_sync_id
// linkage is preserved via a srcSyncID → dstSyncID map maintained for
// the duration of the call.
func Sanitize(ctx context.Context, src connectorstore.Reader, dst connectorstore.Writer, opts Options) error {
	if src == nil {
		return errors.New("c1zsanitize: src reader is nil")
	}
	if dst == nil {
		return errors.New("c1zsanitize: dst writer is nil")
	}
	if len(opts.Secret) < MinSecretBytes {
		return fmt.Errorf("c1zsanitize: secret too short: got %d bytes, want at least %d", len(opts.Secret), MinSecretBytes)
	}

	anchor := opts.TimestampAnchor
	if anchor.IsZero() {
		anchor = time.Now().UTC()
	}

	srcSyncs, err := listAllSyncs(ctx, src)
	if err != nil {
		return fmt.Errorf("c1zsanitize: list source syncs: %w", err)
	}

	s := &sanitizer{
		secret:                 opts.Secret,
		idHmac:                 hmac.New(sha256.New, opts.Secret),
		domains:                newDomainMap(),
		shifter:                newTimestampShifter(anchor, findTMax(srcSyncs)),
		dropUnknownAnnotations: opts.DropUnknownAnnotations,
		log:                    ctxzap.Extract(ctx),
		handlers:               defaultAnnotationHandlers(),
		syncIDMap:              map[string]string{},
	}

	for _, sr := range srcSyncs {
		if err := s.sanitizeSync(ctx, src, dst, sr); err != nil {
			return fmt.Errorf("c1zsanitize: sanitize sync %s: %w", sr.GetId(), err)
		}
	}
	return nil
}

type sanitizer struct {
	secret                 []byte
	idHmac                 hash.Hash
	domains                *domainMap
	shifter                *timestampShifter
	dropUnknownAnnotations bool
	log                    *zap.Logger
	handlers               map[string]annotationHandler
	syncIDMap              map[string]string
}

// id is the per-sanitizer hot path. SanitizeID stays as the
// allocation-y reference implementation; this one reuses a single
// hmac.Hash so the SHA-256 key schedule isn't redone every call.
// Sanitize runs single-threaded, so no locking is needed.
func (s *sanitizer) id(input string) string {
	if input == "" {
		return ""
	}
	s.idHmac.Reset()
	s.idHmac.Write([]byte(input))
	sum := s.idHmac.Sum(nil)
	return idEncoding.EncodeToString(sum[:idTruncationBytes])
}

func (s *sanitizer) sanitizeSync(ctx context.Context, src connectorstore.Reader, dst connectorstore.Writer, sr *reader_v2.SyncRun) error {
	srcSyncID := sr.GetId()
	syncType := connectorstore.SyncType(sr.GetSyncType())
	if syncType == connectorstore.SyncTypeAny || syncType == "" {
		syncType = connectorstore.SyncTypeFull
	}

	parentDst := ""
	if parentSrc := sr.GetParentSyncId(); parentSrc != "" {
		parentDst = s.syncIDMap[parentSrc]
	}

	dstSyncID, err := dst.StartNewSync(ctx, syncType, parentDst)
	if err != nil {
		return fmt.Errorf("start dst sync: %w", err)
	}
	s.syncIDMap[srcSyncID] = dstSyncID

	assetRefs := newAssetRefSet()

	if err := s.copyResourceTypes(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}
	if err := s.copyResources(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}
	if err := s.copyEntitlements(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}
	if err := s.copyGrants(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}
	if err := s.copyAssets(ctx, src, dst, assetRefs); err != nil {
		return err
	}

	if err := dst.EndSync(ctx); err != nil {
		return fmt.Errorf("end dst sync: %w", err)
	}
	return nil
}

// listAllSyncs paginates the source SyncsReaderService and returns
// every sync run the source can see. The SQLite-backed C1File emits
// in id-asc order which matches insertion order, which is parent-
// before-child for any well-formed sync chain.
func listAllSyncs(ctx context.Context, src connectorstore.Reader) ([]*reader_v2.SyncRun, error) {
	var out []*reader_v2.SyncRun
	pageToken := ""
	for {
		req := reader_v2.SyncsReaderServiceListSyncsRequest_builder{
			PageToken: pageToken,
		}.Build()
		resp, err := src.ListSyncs(ctx, req)
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetSyncs()...)
		if resp.GetNextPageToken() == "" {
			return out, nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

func findTMax(syncs []*reader_v2.SyncRun) time.Time {
	var tMax time.Time
	for _, sr := range syncs {
		if sr.HasStartedAt() {
			if t := sr.GetStartedAt().AsTime(); t.After(tMax) {
				tMax = t
			}
		}
		if sr.HasEndedAt() {
			if t := sr.GetEndedAt().AsTime(); t.After(tMax) {
				tMax = t
			}
		}
	}
	return tMax
}

// syncIDAnnotations returns the annotation slice that scopes a list
// request to a specific source sync. The reader resolves the sync ID
// from a SyncDetails annotation; see pkg/dotc1z/sql_helpers.go.
func syncIDAnnotations(srcSyncID string) []*anypb.Any {
	a := annotations.New(c1zpb.SyncDetails_builder{Id: srcSyncID}.Build())
	return a
}
