package v2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

const testResourceType = "test-resource-type"

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

	var opts []V2C1ZOption
	opts = append(opts, WithPragma("journal_mode", "WAL"))

	// Open file
	f, err := NewV2C1ZFile(ctx, testFilePath, opts...)
	require.NoError(t, err)

	// Close file without taking action
	err = f.Close()
	require.NoError(t, err)

	// The file shouldn't exist because we didn't write anything
	_, err = os.Stat(testFilePath)
	require.ErrorIs(t, err, os.ErrNotExist)

	// Open file
	f, err = NewV2C1ZFile(ctx, testFilePath, opts...)
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
	f, err = NewV2C1ZFile(ctx, testFilePath, opts...)
	require.NoError(t, err)

	var syncID2 string
	// Resume the previous sync
	syncID2, newSync, err = f.StartSync(ctx)
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
	f, err = NewV2C1ZFile(ctx, testFilePath, opts...)
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
