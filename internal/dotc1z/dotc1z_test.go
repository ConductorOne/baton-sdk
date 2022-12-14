package dotc1z

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

var c1zTests struct {
	workingDir string
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
		os.Exit(1)
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
		return os.RemoveAll(c1zTests.workingDir)
	}
	return nil
}

func TestC1Z(t *testing.T) {
	ctx := context.Background()
	testFilePath := filepath.Join(c1zTests.workingDir, "test.c1z")

	// Open file
	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	// Close file without taking action
	err = f.Close()
	require.NoError(t, err)

	// The file shouldn't exist because we didn't write anything
	_, err = os.Stat(testFilePath)
	require.ErrorIs(t, err, os.ErrNotExist)

	// Open file
	f, err = NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	// Start a new sync
	syncID, newSync, err := f.StartSync(ctx)
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
	f, err = NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	var syncID2 string
	// Resume the previous sync
	syncID2, newSync, err = f.StartSync(ctx)
	require.NoError(t, err)
	require.False(t, newSync)
	require.Equal(t, syncID, syncID2)

	resourceTypeID := "test-resource-type"
	err = f.PutResourceType(ctx, &v2.ResourceType{Id: resourceTypeID})
	require.NoError(t, err)

	// Fetch the resource type we just saved
	rt, err := f.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: resourceTypeID,
	})
	require.NoError(t, err)
	require.Equal(t, resourceTypeID, rt.Id)

	err = f.Close()
	require.NoError(t, err)

	// The file should be updated and larger than it was previously.
	fileInfo2, err := os.Stat(testFilePath)
	require.NoError(t, err)
	require.Greater(t, fileInfo2.Size(), fileInfo.Size())

	// Open file
	f, err = NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	// Fetch the resource type we just saved
	rt2, err := f.GetResourceType(ctx, &reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest{
		ResourceTypeId: resourceTypeID,
	})
	require.NoError(t, err)
	require.Equal(t, resourceTypeID, rt2.Id)

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
	f, err := NewC1ZFile(ctx, testFilePath)
	require.NoError(t, err)

	// Start a new sync
	_, newSync, err := f.StartSync(ctx)
	require.NoError(t, err)
	require.True(t, newSync)

	resourceTypeID := "test-resource-type"
	err = f.PutResourceType(ctx, &v2.ResourceType{Id: resourceTypeID})
	require.NoError(t, err)

	err = f.Close()
	require.NoError(t, err)

	c1zf, err := os.Open(testFilePath)
	require.NoError(t, err)
	defer f.Close()

	// Test basic decode
	d, err := NewDecoder(c1zf)
	require.NoError(t, err)
	n, err := io.Copy(io.Discard, d)
	require.NoError(t, err)
	require.True(t, n >= 40000) // arbitrary but lower than the actual size
	err = d.Close()
	require.NoError(t, err)

	// Test max size exact
	d, err = NewDecoder(c1zf, WithDecoderMaxDecodedSize(uint64(n)))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, d)
	require.NoError(t, err)
	err = d.Close()
	require.NoError(t, err)

	// Test max size - 1
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
	_, err = io.Copy(io.Discard, d)
	require.NoError(t, err)
	err = d.Close()
	require.NoError(t, err)

	// Test context cancel
	ctx, cancel := context.WithCancel(context.Background())
	d, err = NewDecoder(c1zf, WithContext(ctx))
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, d)
	require.NoError(t, err)
	err = d.Close()
	require.NoError(t, err)

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

	_, err = NewC1ZFile(ctx, testFilePath)
	require.ErrorIs(t, err, ErrInvalidFile)
}
