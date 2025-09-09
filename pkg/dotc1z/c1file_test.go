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
	syncID, newSync, err := f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull)
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
	syncID2, newSync, err = f.StartOrResumeSync(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)
	require.False(t, newSync)
	require.Equal(t, syncID, syncID2)

	resourceTypeID := testResourceType
	err = f.PutResourceTypes(ctx, &v2.ResourceType{Id: resourceTypeID})
	require.NoError(t, err)

	// Fetch the resource type we just saved
	rtResp, err := f.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: resourceTypeID,
	})
	require.NoError(t, err)
	require.Equal(t, resourceTypeID, rtResp.ResourceType.Id)

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
	rtResp2, err := f.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: resourceTypeID,
	})
	require.NoError(t, err)
	require.Equal(t, resourceTypeID, rtResp2.ResourceType.Id)

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
	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull)
	require.NoError(t, err)

	resourceTypeID := testResourceType
	err = f.PutResourceTypes(ctx, &v2.ResourceType{Id: resourceTypeID})
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
