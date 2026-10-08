package c1zsanitize

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

var errSanitizePageInterrupted = errors.New("sanitize page interrupted")
var errSanitizeSealInterrupted = errors.New("sanitize seal interrupted")

type interruptingLedgerStore struct {
	c1zstore.Store
	c1zstore.PageLedgerStore
	failCommit int
	failAfter  bool
	commits    int
}

func newInterruptingLedgerStore(t *testing.T, store c1zstore.Store, failCommit int) *interruptingLedgerStore {
	t.Helper()
	ledger, ok := store.(c1zstore.PageLedgerStore)
	require.True(t, ok)
	return &interruptingLedgerStore{Store: store, PageLedgerStore: ledger, failCommit: failCommit}
}

func (s *interruptingLedgerStore) BeginPage() c1zstore.PageWriter {
	return &interruptingPageWriter{PageWriter: s.PageLedgerStore.BeginPage(), store: s}
}

func (s *interruptingLedgerStore) ListSyncRuns(
	ctx context.Context,
	pageToken string,
	pageSize uint32,
) ([]*c1zstore.SyncRun, string, error) {
	return s.Store.(syncRunMetadataReader).ListSyncRuns(ctx, pageToken, pageSize)
}

type interruptingPageWriter struct {
	c1zstore.PageWriter
	store *interruptingLedgerStore
}

type sealInterruptingLedgerStore struct {
	c1zstore.Store
	c1zstore.PageLedgerStore
}

type resourceTypeFailingReader struct {
	connectorstore.Reader
}

func (r resourceTypeFailingReader) ListResourceTypes(
	context.Context,
	*v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return nil, errors.New("unexpected resource-type read while sealing")
}

type assetFailingReader struct {
	connectorstore.Reader
}

func (r assetFailingReader) GetAsset(
	context.Context,
	*v2.AssetServiceGetAssetRequest,
) (string, io.Reader, error) {
	return "", nil, context.Canceled
}

type smallPageReader struct {
	connectorstore.Reader
}

func (r smallPageReader) ListResources(
	ctx context.Context,
	req *v2.ResourcesServiceListResourcesRequest,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	req = v2.ResourcesServiceListResourcesRequest_builder{
		PageSize: 2, PageToken: req.GetPageToken(), Annotations: req.GetAnnotations(),
	}.Build()
	return r.Reader.ListResources(ctx, req)
}

func (r smallPageReader) ListEntitlements(
	ctx context.Context,
	req *v2.EntitlementsServiceListEntitlementsRequest,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	req = v2.EntitlementsServiceListEntitlementsRequest_builder{
		PageSize: 2, PageToken: req.GetPageToken(), Annotations: req.GetAnnotations(),
	}.Build()
	return r.Reader.ListEntitlements(ctx, req)
}

func (r smallPageReader) ListGrants(
	ctx context.Context,
	req *v2.GrantsServiceListGrantsRequest,
) (*v2.GrantsServiceListGrantsResponse, error) {
	req = v2.GrantsServiceListGrantsRequest_builder{
		PageSize: 2, PageToken: req.GetPageToken(), Annotations: req.GetAnnotations(),
	}.Build()
	return r.Reader.ListGrants(ctx, req)
}

func (r smallPageReader) ListGrantsWithExpansion(
	ctx context.Context,
	req *v2.GrantsServiceListGrantsRequest,
) (*v2.GrantsServiceListGrantsResponse, error) {
	req = v2.GrantsServiceListGrantsRequest_builder{
		PageSize: 2, PageToken: req.GetPageToken(), Annotations: req.GetAnnotations(),
	}.Build()
	return r.Reader.(connectorstore.ExpansionGrantLister).ListGrantsWithExpansion(ctx, req)
}

func (s *sealInterruptingLedgerStore) ListSyncRuns(
	ctx context.Context,
	pageToken string,
	pageSize uint32,
) ([]*c1zstore.SyncRun, string, error) {
	return s.Store.(syncRunMetadataReader).ListSyncRuns(ctx, pageToken, pageSize)
}

func (s *sealInterruptingLedgerStore) Seal(context.Context, c1zstore.SyncStats) error {
	return errSanitizeSealInterrupted
}

func (w *interruptingPageWriter) SetTrustedImport() error {
	return w.PageWriter.(c1zstore.TrustedImportPageWriter).SetTrustedImport()
}

func (w *interruptingPageWriter) Commit(
	ctx context.Context,
	identity c1zstore.LedgerActionIdentity,
	row *c1zstore.LedgerRow,
) error {
	w.store.commits++
	if w.store.commits == w.store.failCommit {
		if !w.store.failAfter {
			return errSanitizePageInterrupted
		}
		if err := w.PageWriter.Commit(ctx, identity, row); err != nil {
			return err
		}
		return errSanitizePageInterrupted
	}
	return w.PageWriter.Commit(ctx, identity, row)
}

func TestSanitizeLedgerResumeMatchesUninterrupted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	cleanPath := filepath.Join(dir, "clean.c1z")
	secret := bytes32("ledger-resume")
	buildSyncFixture(t, ctx, srcPath, 20)
	sanitizeToFile(t, ctx, srcPath, cleanPath, secret, Options{})

	clean := openEngineStoreRO(t, ctx, cleanPath)
	defer clean.Close(ctx)
	cleanRecords := collectRecords(t, ctx, clean)
	require.Equal(t, 1, needsExpansionCount(t, ctx, clean))

	for _, failAfter := range []bool{false, true} {
		position := "before"
		if failAfter {
			position = "after"
		}
		for failCommit := 1; failCommit <= 5; failCommit++ {
			t.Run(position+"-commit-"+strconv.Itoa(failCommit), func(t *testing.T) {
				resumedPath := filepath.Join(dir, position+"-"+strconv.Itoa(failCommit)+".c1z")
				src := mustOpen(t, ctx, srcPath, true)
				partial, err := dotc1z.NewStore(ctx, resumedPath, dotc1z.WithEngine(c1zstore.EnginePebble))
				require.NoError(t, err)
				interrupted := newInterruptingLedgerStore(t, partial, failCommit)
				interrupted.failAfter = failAfter
				err = Sanitize(ctx, src, interrupted, Options{Secret: secret, TimestampAnchor: fixedAnchor})
				require.ErrorIs(t, err, errSanitizePageInterrupted)
				require.NoError(t, interrupted.Close(ctx))
				require.NoError(t, src.Close(ctx))

				src = mustOpen(t, ctx, srcPath, true)
				resume, err := dotc1z.NewStore(ctx, resumedPath, dotc1z.WithEngine(c1zstore.EnginePebble))
				require.NoError(t, err)
				require.NoError(t, Sanitize(ctx, src, resume, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
				require.NoError(t, resume.Close(ctx))
				require.NoError(t, src.Close(ctx))

				resumed := openEngineStoreRO(t, ctx, resumedPath)
				defer resumed.Close(ctx)
				require.Equal(t, cleanRecords, collectRecords(t, ctx, resumed))
				assertAssetEqual(t, ctx, clean, resumed, SanitizeID(secret, "icon-0"))
				require.Equal(t, needsExpansionCount(t, ctx, clean), needsExpansionCount(t, ctx, resumed))
			})
		}
	}
}

func needsExpansionCount(t *testing.T, ctx context.Context, store c1zstore.Store) int {
	t.Helper()
	engine, ok := pebble.AsEngine(store)
	require.True(t, ok)
	count := 0
	require.NoError(t, engine.IterateGrantsByNeedsExpansion(ctx, func(*v3.GrantRecord) bool {
		count++
		return true
	}))
	return count
}

func assertAssetEqual(t *testing.T, ctx context.Context, left, right c1zstore.Store, id string) {
	t.Helper()
	read := func(store c1zstore.Store) (string, []byte) {
		contentType, reader, err := store.GetAsset(ctx, v2.AssetServiceGetAssetRequest_builder{
			Asset: v2.AssetRef_builder{Id: id}.Build(),
		}.Build())
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, closeIfCloser(reader))
		return contentType, data
	}
	leftType, leftData := read(left)
	rightType, rightData := read(right)
	require.Equal(t, leftType, rightType)
	require.Equal(t, leftData, rightData)
}

func TestSanitizeLedgerRejectsMismatchedResumeWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	otherSrcPath := filepath.Join(dir, "other-source.c1z")
	secret := bytes32("ledger-secret")
	buildSyncFixture(t, ctx, srcPath, 10)
	buildSyncFixture(t, ctx, otherSrcPath, 10)

	tests := []struct {
		name       string
		sourcePath string
		options    Options
		want       string
	}{
		{"secret", srcPath, Options{Secret: bytes32("wrong-secret"), TimestampAnchor: fixedAnchor}, "different secret"},
		{"anchor", srcPath, Options{Secret: secret, TimestampAnchor: fixedAnchor.Add(time.Hour)}, "different timestamp anchor"},
		{"source", otherSrcPath, Options{Secret: secret, TimestampAnchor: fixedAnchor}, "different source sync"},
		{"policy", srcPath, Options{Secret: secret, TimestampAnchor: fixedAnchor, AllowUnknownAnnotations: true}, "different annotation policy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dstPath := filepath.Join(dir, "destination-"+tt.name+".c1z")
			src := mustOpen(t, ctx, srcPath, true)
			partial, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			interrupted := newInterruptingLedgerStore(t, partial, 3)
			err = Sanitize(ctx, src, interrupted, Options{Secret: secret, TimestampAnchor: fixedAnchor})
			require.ErrorIs(t, err, errSanitizePageInterrupted)
			require.NoError(t, interrupted.Close(ctx))
			require.NoError(t, src.Close(ctx))
			before, err := os.ReadFile(dstPath)
			require.NoError(t, err)

			src = mustOpen(t, ctx, tt.sourcePath, true)
			resume, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
			require.NoError(t, err)
			err = Sanitize(ctx, src, resume, tt.options)
			require.ErrorContains(t, err, tt.want)
			require.NoError(t, resume.Close(ctx))
			require.NoError(t, src.Close(ctx))
			after, err := os.ReadFile(dstPath)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestSanitizeLedgerRejectsMissingPolicyWithoutMutation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	dstPath := filepath.Join(dir, "destination.c1z")
	secret := bytes32("missing-policy")
	buildSyncFixture(t, ctx, srcPath, 1)

	src := openEngineStoreRO(t, ctx, srcPath)
	partial, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	interrupted := newInterruptingLedgerStore(t, partial, 2)
	err = Sanitize(ctx, src, interrupted, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.ErrorIs(t, err, errSanitizePageInterrupted)
	require.NoError(t, interrupted.Close(ctx))
	require.NoError(t, src.Close(ctx))

	partial, err = dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	runs, _, err := partial.(syncRunMetadataReader).ListSyncRuns(ctx, "", 2)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	_, err = partial.ResumeSync(ctx, connectorstore.SyncTypeFull, runs[0].ID)
	require.NoError(t, err)
	ledger := partial.(c1zstore.PageLedgerStore)
	facts, err := ledger.LedgerFacts(ctx)
	require.NoError(t, err)
	var config map[string]any
	require.NoError(t, json.Unmarshal([]byte(facts[sanitizeConfigFact]), &config))
	delete(config, "allow_unknown_annotations")
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	writer := ledger.BeginPage()
	require.NoError(t, writer.SetFactValue(sanitizeConfigFact, string(raw)))
	require.NoError(t, writer.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "test-config-mutation"}, nil))
	require.NoError(t, partial.Close(ctx))
	before, err := os.ReadFile(dstPath)
	require.NoError(t, err)

	src = openEngineStoreRO(t, ctx, srcPath)
	resume, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	err = Sanitize(ctx, src, resume, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.ErrorContains(t, err, "no annotation policy")
	require.NoError(t, resume.Close(ctx))
	require.NoError(t, src.Close(ctx))
	after, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestSanitizeLedgerResumeAdoptsImplicitAnchor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	dstPath := filepath.Join(dir, "destination.c1z")
	secret := bytes32("implicit-anchor")
	buildSyncFixture(t, ctx, srcPath, 10)

	src := mustOpen(t, ctx, srcPath, true)
	partial, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	interrupted := newInterruptingLedgerStore(t, partial, 3)
	err = Sanitize(ctx, src, interrupted, Options{Secret: secret})
	require.ErrorIs(t, err, errSanitizePageInterrupted)
	require.NoError(t, interrupted.Close(ctx))
	require.NoError(t, src.Close(ctx))

	src = mustOpen(t, ctx, srcPath, true)
	resume, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, Sanitize(ctx, src, resume, Options{Secret: secret}))
	require.NoError(t, resume.Close(ctx))
	require.NoError(t, src.Close(ctx))
}

func TestSanitizeLedgerResumesSealing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	dstPath := filepath.Join(dir, "destination.c1z")
	secret := bytes32("resume-sealing")
	buildSyncFixture(t, ctx, srcPath, 10)

	src := mustOpen(t, ctx, srcPath, true)
	partial, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	ledger := partial.(c1zstore.PageLedgerStore)
	interrupted := &sealInterruptingLedgerStore{Store: partial, PageLedgerStore: ledger}
	err = Sanitize(ctx, src, interrupted, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.ErrorIs(t, err, errSanitizeSealInterrupted)
	require.NoError(t, interrupted.Close(ctx))
	require.NoError(t, src.Close(ctx))

	src = mustOpen(t, ctx, srcPath, true)
	resume, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, Sanitize(ctx, resourceTypeFailingReader{Reader: src}, resume, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, resume.Close(ctx))
	require.NoError(t, src.Close(ctx))
}

func TestSanitizeLedgerDoesNotCommitAssetReadErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	dstPath := filepath.Join(dir, "destination.c1z")
	secret := bytes32("asset-read-error")
	buildSyncFixture(t, ctx, srcPath, 1)

	src := mustOpen(t, ctx, srcPath, true)
	dst, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	err = Sanitize(ctx, assetFailingReader{Reader: src}, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, dst.Close(context.WithoutCancel(ctx)))
	require.NoError(t, src.Close(ctx))

	src = mustOpen(t, ctx, srcPath, true)
	dst, err = dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, Sanitize(ctx, src, dst, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, dst.Close(ctx))
	require.NoError(t, src.Close(ctx))
}

func TestSanitizeLedgerResumesPebbleSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	dstPath := filepath.Join(dir, "destination.c1z")
	secret := bytes32("pebble-source-resume")

	srcStore := newEngineStore(t, ctx, srcPath, c1zstore.EnginePebble)
	buildParityFixture(t, ctx, srcStore)
	require.NoError(t, srcStore.Close(ctx))

	src := openEngineStoreRO(t, ctx, srcPath)
	partial, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	interrupted := newInterruptingLedgerStore(t, partial, 3)
	err = Sanitize(ctx, src, interrupted, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.ErrorIs(t, err, errSanitizePageInterrupted)
	require.NoError(t, interrupted.Close(ctx))
	require.NoError(t, src.Close(ctx))

	src = openEngineStoreRO(t, ctx, srcPath)
	resume, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, Sanitize(ctx, src, resume, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, resume.Close(ctx))
	require.NoError(t, src.Close(ctx))
}

func TestSanitizeLedgerResumesMultiPageSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.c1z")
	dstPath := filepath.Join(dir, "destination.c1z")
	secret := bytes32("multi-page-resume")
	buildSyncFixture(t, ctx, srcPath, 5)

	src := mustOpen(t, ctx, srcPath, true)
	partial, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	interrupted := newInterruptingLedgerStore(t, partial, 3)
	err = Sanitize(ctx, smallPageReader{Reader: src}, interrupted, Options{Secret: secret, TimestampAnchor: fixedAnchor})
	require.ErrorIs(t, err, errSanitizePageInterrupted)
	require.NoError(t, interrupted.Close(ctx))
	require.NoError(t, src.Close(ctx))

	src = mustOpen(t, ctx, srcPath, true)
	resume, err := dotc1z.NewStore(ctx, dstPath, dotc1z.WithEngine(c1zstore.EnginePebble))
	require.NoError(t, err)
	require.NoError(t, Sanitize(ctx, smallPageReader{Reader: src}, resume, Options{Secret: secret, TimestampAnchor: fixedAnchor}))
	require.NoError(t, resume.Close(ctx))
	require.NoError(t, src.Close(ctx))
}
