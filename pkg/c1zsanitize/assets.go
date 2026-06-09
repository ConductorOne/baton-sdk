package c1zsanitize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// AssetRecord.data is replaced with a deterministic placeholder
// matching the original content type. The placeholder bytes are
// chosen so that consumers that inspect content_type without parsing
// the payload still see a sensible content_type/length pair.
//
// content_type values not in the explicit map fall through to a
// single zero byte; that's enough to keep the row non-empty (PutAsset
// silently drops empty data) while preserving the cross-reference.
var placeholderByContentType = map[string][]byte{
	// 1x1 transparent PNG.
	"image/png": {
		0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
		0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
		0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
		0x89, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x44, 0x41,
		0x54, 0x78, 0x9c, 0x62, 0x00, 0x01, 0x00, 0x00,
		0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00,
		0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
		0x42, 0x60, 0x82,
	},
}

// placeholderForContentType returns the bytes the sanitizer writes
// in place of the original asset payload. The single-byte fallback
// is intentional: zero-length data is silently dropped by PutAsset,
// which would break the cross-reference invariant.
func placeholderForContentType(contentType string) []byte {
	if b, ok := placeholderByContentType[strings.ToLower(contentType)]; ok {
		return b
	}
	if strings.HasPrefix(strings.ToLower(contentType), "image/") {
		return placeholderByContentType["image/png"]
	}
	return []byte{0x00}
}

// assetRefSet collects every AssetRef.Id encountered while walking
// records during a sync. The set is drained after the record walk by
// copyAssets which fetches each original asset, replaces its payload
// with a placeholder, and writes the sanitized AssetRef into dst.
type assetRefSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newAssetRefSet() *assetRefSet {
	return &assetRefSet{m: map[string]struct{}{}}
}

func (a *assetRefSet) add(id string) {
	if id == "" {
		return
	}
	a.mu.Lock()
	a.m[id] = struct{}{}
	a.mu.Unlock()
}

func (a *assetRefSet) drain() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.m))
	for id := range a.m {
		out = append(out, id)
	}
	a.m = map[string]struct{}{}
	return out
}

// drainAndClose consumes the reader and closes it if the underlying
// type also implements io.Closer. The connectorstore.Reader.GetAsset
// contract returns an io.Reader, but at least one implementation
// returns *os.File; not closing it leaks a fd per asset.
func drainAndClose(r io.Reader) error {
	if c, ok := r.(io.Closer); ok {
		defer c.Close()
	}
	_, err := io.Copy(io.Discard, r)
	return err
}

func (s *sanitizer) copyAssets(
	ctx context.Context,
	src connectorstore.Reader,
	dst connectorstore.Writer,
	refs *assetRefSet,
) error {
	ids := refs.drain()
	// copyAssets runs once per source sync over an already-drained
	// set, so the total IS known up front — pass it through startPhase
	// for true N-of-M progress on this phase.
	pp := s.startPhase("assets", "", int64(len(ids)))
	defer pp.done()
	for _, srcID := range ids {
		req := v2.AssetServiceGetAssetRequest_builder{
			Asset: v2.AssetRef_builder{Id: srcID}.Build(),
		}.Build()
		contentType, r, err := src.GetAsset(ctx, req)
		if err != nil {
			// Asset referenced from an annotation but missing from
			// the asset table. Skip — we don't fabricate placeholder
			// rows because the cross-reference invariant treats it
			// as a known dangling pointer in the source.
			s.log.Debug("c1zsanitize: asset ref not found in source", zap.String("asset_id", srcID), zap.Error(err))
			pp.page(1)
			continue
		}
		if err := drainAndClose(r); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("drain source asset %s: %w", srcID, err)
		}
		dstID := s.id(srcID)
		if err := dst.PutAsset(ctx, v2.AssetRef_builder{Id: dstID}.Build(), contentType, placeholderForContentType(contentType)); err != nil {
			return fmt.Errorf("put dst asset %s: %w", dstID, err)
		}
		pp.page(1)
	}
	return nil
}
