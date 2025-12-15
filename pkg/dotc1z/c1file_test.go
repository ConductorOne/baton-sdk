package dotc1z

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

const testResourceType = "test-resource-type"

var c1zTests struct {
	workingDir string
}

func TestSQLiteDialectRegistered(t *testing.T) {
	require.Equal(
		t,
		"sqlite3",
		goqu.GetDialect("sqlite3").Dialect(),
	)
}

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}

	code := m.Run()

	err = teardown()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", err.Error())
	}

	os.Exit(code)
}

func setup() error {
	tempdir, err := os.MkdirTemp("", "c1z-test")
	if err != nil {
		return err
	}
	c1zTests.workingDir = tempdir

	return nil
}

func teardown() error {
	if c1zTests.workingDir != "" {
		err := os.RemoveAll(c1zTests.workingDir)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error during teardown: %s\n", err.Error())
		}
	}
	return nil
}

func TestC1Z(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test.c1z")

	var opts []C1ZOption
	opts = append(opts, WithPragma("journal_mode", "WAL"))

	// Open file
	f, err := NewC1ZFile(ctx, testFilePath, opts...)
	require.NoError(t, err)

	// Close file without taking action
	err = f.Close()
	require.NoError(t, err)

	// The file shouldn't exist because we didn't write anything
	_, err = os.Stat(testFilePath)
	require.ErrorIs(t, err, os.ErrNotExist)

	// Open file
	f, err = NewC1ZFile(ctx, testFilePath, opts...)
	require.NoError(t, err)

	// Start a new sync
	syncID, newSync, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.True(t, newSync)
	require.NotEmpty(t, syncID)

	// Close file mid sync
	err = f.Close()
	require.NoError(t, err)

	// file should exist now that we started a sync
	fileInfo, err := os.Stat(testFilePath)
	require.NoError(t, err)

	// Open file
	f, err = NewC1ZFile(ctx, testFilePath, opts...)
	require.NoError(t, err)

	var syncID2 string
	// Resume the previous sync
	syncID2, newSync, err = f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.False(t, newSync)
	require.Equal(t, syncID, syncID2)

	resourceTypeID := testResourceType
	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	// Fetch the resource type we just saved
	rtResp, err := f.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, resourceTypeID, rtResp.GetResourceType().GetId())

	err = f.Close()
	require.NoError(t, err)

	// The file should be updated and larger than it was previously.
	fileInfo2, err := os.Stat(testFilePath)
	require.NoError(t, err)
	require.Greater(t, fileInfo2.Size(), fileInfo.Size())

	// Open file
	f, err = NewC1ZFile(ctx, testFilePath, opts...)
	require.NoError(t, err)

	// Fetch the resource type we just saved
	rtResp2, err := f.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: resourceTypeID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, resourceTypeID, rtResp2.GetResourceType().GetId())

	err = f.Close()
	require.NoError(t, err)

	// The file should be updated and larger than it was previously.
	fileInfo3, err := os.Stat(testFilePath)
	require.NoError(t, err)
	require.Equal(t, fileInfo3.ModTime(), fileInfo2.ModTime())
	require.Equal(t, fileInfo3.Size(), fileInfo2.Size())
}

func TestC1ZDecoder(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-decoder.c1z")

	// Open file
	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	// Start a new sync
	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: resourceTypeID}.Build())
	require.NoError(t, err)

	// Close the raw DB so the write ahead log is checkpointed.
	err = f.rawDb.Close()
	require.NoError(t, err)
	f.rawDb = nil

	dbFile, err := os.ReadFile(f.dbFilePath)
	require.NoError(t, err)
	require.Greater(t, len(dbFile), 40000) // arbitrary, but make sure we read some bytes

	err = f.Close()
	require.NoError(t, err)

	c1zf, err := os.Open(testFilePath)
	require.NoError(t, err)
	defer c1zf.Close()

	// Test basic decode
	d, err := NewDecoder(c1zf)
	require.NoError(t, err)
	b := bytes.NewBuffer(nil)
	n, err := io.Copy(b, d)
	require.NoError(t, err)
	err = d.Close()
	require.NoError(t, err)
	require.Equal(t, dbFile, b.Bytes())
	b.Reset()

	require.GreaterOrEqual(t, n, int64(0))

	// Test max size exact
	//nolint:gosec // No risk of overflow because n is always >= 0.
	d, err = NewDecoder(c1zf, WithDecoderMaxDecodedSize(uint64(n)))
	require.NoError(t, err)
	_, err = io.Copy(b, d)
	require.NoError(t, err)
	err = d.Close()
	require.NoError(t, err)
	require.Equal(t, dbFile, b.Bytes())
	b.Reset()

	// Test max size - 1
	require.GreaterOrEqual(t, n, int64(1))
	//nolint:gosec // No risk of overflow because n is always > 0.
	d, err = NewDecoder(c1zf, WithDecoderMaxDecodedSize(uint64(n-1)))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, d)
	require.ErrorIs(t, err, ErrMaxSizeExceeded)
	err = d.Close()
	require.NoError(t, err)

	// Test lower mem usage
	d, err = NewDecoder(c1zf, WithDecoderMaxMemory(1*1024*1024))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, d)
	require.ErrorIs(t, err, ErrWindowSizeExceeded)
	err = d.Close()
	require.NoError(t, err)

	// Test lower mem usage
	d, err = NewDecoder(c1zf, WithDecoderMaxMemory(8*1024*1024))
	require.NoError(t, err)
	_, err = io.Copy(b, d)
	require.NoError(t, err)
	err = d.Close()
	require.NoError(t, err)
	require.Equal(t, dbFile, b.Bytes())
	b.Reset()

	// Test context cancel
	ctx, cancel := context.WithCancel(context.Background())
	d, err = NewDecoder(c1zf, WithContext(ctx))
	require.NoError(t, err)
	_, err = io.Copy(b, d)
	require.NoError(t, err)
	err = d.Close()
	require.NoError(t, err)
	require.Equal(t, dbFile, b.Bytes())
	b.Reset()

	cancel()
	d, err = NewDecoder(c1zf, WithContext(ctx))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, d)
	require.ErrorIs(t, err, context.Canceled)
	err = d.Close()
	require.NoError(t, err)
}

func TestC1ZInvalidFile(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-invalid-file.c1z")

	f, err := os.Create(testFilePath)
	require.NoError(t, err)

	_, err = f.WriteString("some bytes")
	require.NoError(t, err)

	err = f.Close()
	require.NoError(t, err)

	_, err = NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.ErrorIs(t, err, ErrInvalidFile)
}

func TestC1ZStats(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-stats.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: testResourceType}.Build())
	require.NoError(t, err)

	err = f.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: testResourceType,
			Resource:     "test-resource",
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = f.EndSync(ctx)
	require.NoError(t, err)

	expectedStats := map[string]int64{
		"resource_types": 1,
		testResourceType: 1,
		"entitlements":   0,
		"grants":         0,
	}

	stats, err := f.Stats(ctx, connectorstore.SyncTypeFull, syncID)
	require.NoError(t, err)
	equalStats(t, expectedStats, stats)

	_, err = f.Stats(ctx, connectorstore.SyncTypePartial, syncID)
	require.ErrorIs(t, err, status.Errorf(codes.InvalidArgument, "sync '%s' is not of type '%s'", syncID, connectorstore.SyncTypePartial))

	stats, err = f.Stats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	equalStats(t, expectedStats, stats)

	stats, err = f.Stats(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	equalStats(t, expectedStats, stats)

	err = f.Close()
	require.NoError(t, err)
}

func TestC1ZStatsPartialSync(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-stats-partial-sync.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: testResourceType}.Build())
	require.NoError(t, err)

	err = f.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: testResourceType,
			Resource:     "test-resource",
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = f.EndSync(ctx)
	require.NoError(t, err)

	expectedStats := map[string]int64{
		"resource_types": 1,
		testResourceType: 1,
		"entitlements":   0,
		"grants":         0,
	}

	stats, err := f.Stats(ctx, connectorstore.SyncTypePartial, syncID)
	require.NoError(t, err)
	equalStats(t, expectedStats, stats)

	err = f.Close()
	require.NoError(t, err)
}

func TestC1ZStatsResourcesOnlySync(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-stats-resources-only-sync.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeResourcesOnly, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: testResourceType}.Build())
	require.NoError(t, err)

	err = f.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: testResourceType,
			Resource:     "test-resource",
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = f.EndSync(ctx)
	require.NoError(t, err)

	expectedStats := map[string]int64{
		"resource_types": 1,
		testResourceType: 1,
	}

	stats, err := f.Stats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	equalStats(t, expectedStats, stats)

	stats, err = f.Stats(ctx, connectorstore.SyncTypeResourcesOnly, syncID)
	require.NoError(t, err)
	equalStats(t, expectedStats, stats)

	err = f.Close()
	require.NoError(t, err)
}

func equalStats(t *testing.T, expectedStats map[string]int64, stats map[string]int64) {
	for k, v := range expectedStats {
		val, ok := stats[k]
		require.True(t, ok)
		require.Equal(t, v, val)
	}
	for k, v := range stats {
		val, ok := expectedStats[k]
		require.True(t, ok)
		require.Equal(t, v, val)
	}
}

func TestC1ZGrantStatsSync(t *testing.T) {
	ctx := context.Background()

	testFilePath := filepath.Join(c1zTests.workingDir, "test-grant-stats-sync.c1z")

	f, err := NewC1ZFile(ctx, testFilePath, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: testResourceType}.Build())
	require.NoError(t, err)

	err = f.PutGrants(ctx, v2.Grant_builder{
		Id: "grant-id",
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: testResourceType,
				Resource:     "principal-id",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-id",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: testResourceType,
					Resource:     "resource-id2",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = f.PutResources(ctx, v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: testResourceType,
			Resource:     "test-resource",
		}.Build(),
	}.Build())
	require.NoError(t, err)

	err = f.EndSync(ctx)
	require.NoError(t, err)

	expectedGrantStats := map[string]int64{
		testResourceType: 1,
	}

	stats, err := f.GrantStats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	for k, v := range expectedGrantStats {
		require.Equal(t, v, stats[k])
	}

	err = f.Close()
	require.NoError(t, err)
}

func TestC1ZReadOnlyMode(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test-readonly.c1z")

	// First, create a c1z file with some data
	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: testResourceType}.Build())
	require.NoError(t, err)

	err = f.Close()
	require.NoError(t, err)

	// Open the file again in read-only mode
	f, err = NewC1ZFile(ctx, testFilePath, WithReadOnly(true))
	require.NoError(t, err)

	// Read the sync
	sync, err := f.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: syncID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, string(connectorstore.SyncTypeFull), sync.GetSync().GetSyncType())
	require.Equal(t, syncID, sync.GetSync().GetId())
	// Close the file
	err = f.Close()
	require.NoError(t, err)

	// Now open it in read-only mode
	f, err = NewC1ZFile(ctx, testFilePath, WithReadOnly(true))
	require.NoError(t, err)

	// Make a modification - start a new sync
	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.ErrorIs(t, err, ErrReadOnly)

	// Try to put a resource type
	err = f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "another-resource-type"}.Build())
	require.ErrorIs(t, err, ErrReadOnly)

	err = f.Close()
	require.NoError(t, err)

	fileInfo1, err := os.Stat(testFilePath)
	require.NoError(t, err)
	// Reopen to verify no corruption
	f2, err := NewC1ZFile(ctx, testFilePath, WithReadOnly(true))
	require.NoError(t, err)
	err = f2.Close()
	require.NoError(t, err)
	fileInfo2, err := os.Stat(testFilePath)
	require.NoError(t, err)
	require.Equal(t, fileInfo1.ModTime(), fileInfo2.ModTime())
}

func TestC1FileMmapSizeEnvVar(t *testing.T) {
	ctx := context.Background()
	const mmapSizePragmaName = "mmap_size"

	// Save and restore original env var state
	originalValue := os.Getenv(sqliteMmapSizeEnvVar)
	defer func() {
		if originalValue != "" {
			os.Setenv(sqliteMmapSizeEnvVar, originalValue)
		} else {
			os.Unsetenv(sqliteMmapSizeEnvVar)
		}
	}()

	t.Run("without env var - no mmap pragma added", func(t *testing.T) {
		// Ensure env var is not set
		os.Unsetenv(sqliteMmapSizeEnvVar)

		tmpDir := t.TempDir()
		testFilePath := filepath.Join(tmpDir, "test-mmap-unset.db")

		f, err := NewC1File(ctx, testFilePath)
		require.NoError(t, err)
		defer func() {
			if f != nil {
				f.Close()
			}
		}()

		// Check that no mmap_size pragma was added
		hasMmapPragma := false
		for _, p := range f.pragmas {
			if p.name == mmapSizePragmaName {
				hasMmapPragma = true
			}
		}
		require.False(t, hasMmapPragma, "mmap_size pragma should not be added when env var is unset")
	})

	t.Run("with env var set to 1 - mmap pragma added", func(t *testing.T) {
		// Set env var to 1 MB (minimal value for testing)
		os.Setenv(sqliteMmapSizeEnvVar, "1")

		tmpDir := t.TempDir()
		testFilePath := filepath.Join(tmpDir, "test-mmap-1.db")

		f, err := NewC1File(ctx, testFilePath)
		require.NoError(t, err)
		defer func() {
			if f != nil {
				f.Close()
			}
		}()

		// Check that mmap_size pragma was added with correct value
		var mmapValue string
		hasMmapPragma := false
		for _, p := range f.pragmas {
			if p.name == mmapSizePragmaName {
				hasMmapPragma = true
				mmapValue = p.value
			}
		}
		require.True(t, hasMmapPragma, "mmap_size pragma should be added when env var is set")
		expectedBytes := int64(1 * 1024 * 1024)
		require.Equal(t, fmt.Sprintf("%d", expectedBytes), mmapValue, "mmap_size should be 1 MB in bytes")
	})

	t.Run("with env var set to 0 - no mmap pragma added", func(t *testing.T) {
		// Set env var to 0 (explicitly disabled)
		os.Setenv(sqliteMmapSizeEnvVar, "0")

		tmpDir := t.TempDir()
		testFilePath := filepath.Join(tmpDir, "test-mmap-zero.db")

		f, err := NewC1File(ctx, testFilePath)
		require.NoError(t, err)
		defer func() {
			if f != nil {
				f.Close()
			}
		}()

		// Check that no mmap_size pragma was added
		hasMmapPragma := false
		for _, p := range f.pragmas {
			if p.name == mmapSizePragmaName {
				hasMmapPragma = true
			}
		}
		require.False(t, hasMmapPragma, "mmap_size pragma should not be added when env var is 0")
	})

	t.Run("with invalid env var - no mmap pragma added", func(t *testing.T) {
		// Set env var to invalid value
		os.Setenv(sqliteMmapSizeEnvVar, "not-a-number")

		tmpDir := t.TempDir()
		testFilePath := filepath.Join(tmpDir, "test-mmap-invalid.db")

		f, err := NewC1File(ctx, testFilePath)
		require.NoError(t, err)
		defer func() {
			if f != nil {
				f.Close()
			}
		}()

		// Check that no mmap_size pragma was added (fails safely)
		hasMmapPragma := false
		for _, p := range f.pragmas {
			if p.name == mmapSizePragmaName {
				hasMmapPragma = true
			}
		}
		require.False(t, hasMmapPragma, "mmap_size pragma should not be added when env var is invalid")
	})
}
