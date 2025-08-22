package pebble

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestPebbleEngine_PutAsset(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		assetRef    *v2.AssetRef
		contentType string
		data        []byte
		wantErr     bool
		errContains string
	}{
		{
			name: "valid asset",
			assetRef: &v2.AssetRef{
				Id: "test-asset-1",
			},
			contentType: "image/png",
			data:        []byte("fake-png-data"),
			wantErr:     false,
		},
		{
			name: "valid asset with json content type",
			assetRef: &v2.AssetRef{
				Id: "test-asset-2",
			},
			contentType: "application/json",
			data:        []byte(`{"key": "value"}`),
			wantErr:     false,
		},
		{
			name: "valid asset with binary data",
			assetRef: &v2.AssetRef{
				Id: "test-asset-3",
			},
			contentType: "application/octet-stream",
			data:        []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD},
			wantErr:     false,
		},
		{
			name:        "nil asset reference",
			assetRef:    nil,
			contentType: "image/png",
			data:        []byte("data"),
			wantErr:     true,
			errContains: "asset reference cannot be nil",
		},
		{
			name: "empty asset ID",
			assetRef: &v2.AssetRef{
				Id: "",
			},
			contentType: "image/png",
			data:        []byte("data"),
			wantErr:     true,
			errContains: "asset reference missing ID",
		},
		{
			name: "empty content type",
			assetRef: &v2.AssetRef{
				Id: "test-asset",
			},
			contentType: "",
			data:        []byte("data"),
			wantErr:     true,
			errContains: "content type cannot be empty",
		},
		{
			name: "large asset exceeds size limit",
			assetRef: &v2.AssetRef{
				Id: "large-asset",
			},
			contentType: "application/octet-stream",
			data:        make([]byte, 101*1024*1024), // 101MB
			wantErr:     true,
			errContains: "exceeds maximum allowed size",
		},
		{
			name: "empty data is allowed",
			assetRef: &v2.AssetRef{
				Id: "empty-asset",
			},
			contentType: "text/plain",
			data:        []byte{},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, cleanup := setupTestEngine(t)
			defer cleanup()

			// Start a sync to have an active sync ID
			syncID, err := engine.StartNewSync(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, syncID)

			err = engine.PutAsset(ctx, tt.assetRef, tt.contentType, tt.data)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
				assert.True(t, engine.Dirty())
			}
		})
	}
}

func TestPebbleEngine_PutAsset_NoActiveSync(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	assetRef := &v2.AssetRef{Id: "test-asset"}
	err := engine.PutAsset(ctx, assetRef, "text/plain", []byte("data"))

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active sync")
}

func TestPebbleEngine_GetAsset(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name            string
		setupAsset      bool
		assetID         string
		contentType     string
		data            []byte
		requestAssetID  string
		wantErr         bool
		errContains     string
		expectedContent string
		expectedData    []byte
	}{
		{
			name:            "get existing asset",
			setupAsset:      true,
			assetID:         "test-asset-1",
			contentType:     "image/png",
			data:            []byte("fake-png-data"),
			requestAssetID:  "test-asset-1",
			wantErr:         false,
			expectedContent: "image/png",
			expectedData:    []byte("fake-png-data"),
		},
		{
			name:            "get existing json asset",
			setupAsset:      true,
			assetID:         "test-asset-2",
			contentType:     "application/json",
			data:            []byte(`{"key": "value", "number": 42}`),
			requestAssetID:  "test-asset-2",
			wantErr:         false,
			expectedContent: "application/json",
			expectedData:    []byte(`{"key": "value", "number": 42}`),
		},
		{
			name:            "get existing binary asset",
			setupAsset:      true,
			assetID:         "test-asset-3",
			contentType:     "application/octet-stream",
			data:            []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD},
			requestAssetID:  "test-asset-3",
			wantErr:         false,
			expectedContent: "application/octet-stream",
			expectedData:    []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD},
		},
		{
			name:            "get empty asset",
			setupAsset:      true,
			assetID:         "empty-asset",
			contentType:     "text/plain",
			data:            []byte{},
			requestAssetID:  "empty-asset",
			wantErr:         false,
			expectedContent: "text/plain",
			expectedData:    []byte{},
		},
		{
			name:           "asset not found",
			setupAsset:     false,
			requestAssetID: "nonexistent-asset",
			wantErr:        true,
			errContains:    "asset not found",
		},
		{
			name:           "empty asset ID in request",
			setupAsset:     false,
			requestAssetID: "",
			wantErr:        true,
			errContains:    "asset reference missing ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, cleanup := setupTestEngine(t)
			defer cleanup()

			// Start a sync to have an active sync ID
			syncID, err := engine.StartNewSync(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, syncID)

			// Setup asset if needed
			if tt.setupAsset {
				assetRef := &v2.AssetRef{Id: tt.assetID}
				err := engine.PutAsset(ctx, assetRef, tt.contentType, tt.data)
				require.NoError(t, err)
			}

			// Create request
			req := &v2.AssetServiceGetAssetRequest{
				Asset: &v2.AssetRef{Id: tt.requestAssetID},
			}

			contentType, reader, err := engine.GetAsset(ctx, req)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Empty(t, contentType)
				assert.Nil(t, reader)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedContent, contentType)
				assert.NotNil(t, reader)

				// Read all data from the reader
				data, err := io.ReadAll(reader)
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedData, data)
			}
		})
	}
}

func TestPebbleEngine_GetAsset_InvalidRequests(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	tests := []struct {
		name        string
		req         *v2.AssetServiceGetAssetRequest
		errContains string
	}{
		{
			name:        "nil request",
			req:         nil,
			errContains: "asset request or asset reference cannot be nil",
		},
		{
			name:        "nil asset reference",
			req:         &v2.AssetServiceGetAssetRequest{Asset: nil},
			errContains: "asset request or asset reference cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contentType, reader, err := engine.GetAsset(ctx, tt.req)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
			assert.Empty(t, contentType)
			assert.Nil(t, reader)
		})
	}
}

func TestPebbleEngine_AssetRoundTrip(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	// Test data with various content types
	testCases := []struct {
		id          string
		contentType string
		data        []byte
	}{
		{
			id:          "text-asset",
			contentType: "text/plain",
			data:        []byte("Hello, World!"),
		},
		{
			id:          "json-asset",
			contentType: "application/json",
			data:        []byte(`{"message": "Hello, World!", "count": 42}`),
		},
		{
			id:          "binary-asset",
			contentType: "application/octet-stream",
			data:        []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}, // PNG header
		},
		{
			id:          "large-text-asset",
			contentType: "text/plain",
			data:        []byte(strings.Repeat("Lorem ipsum dolor sit amet. ", 1000)),
		},
	}

	// Store all assets
	for _, tc := range testCases {
		assetRef := &v2.AssetRef{Id: tc.id}
		err := engine.PutAsset(ctx, assetRef, tc.contentType, tc.data)
		require.NoError(t, err, "Failed to put asset %s", tc.id)
	}

	// Retrieve and verify all assets
	for _, tc := range testCases {
		req := &v2.AssetServiceGetAssetRequest{
			Asset: &v2.AssetRef{Id: tc.id},
		}

		contentType, reader, err := engine.GetAsset(ctx, req)
		require.NoError(t, err, "Failed to get asset %s", tc.id)

		assert.Equal(t, tc.contentType, contentType, "Content type mismatch for asset %s", tc.id)

		data, err := io.ReadAll(reader)
		require.NoError(t, err, "Failed to read asset data for %s", tc.id)

		assert.Equal(t, tc.data, data, "Data mismatch for asset %s", tc.id)
	}
}

func TestPebbleEngine_AssetOverwrite(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	assetRef := &v2.AssetRef{Id: "overwrite-test"}

	// Store initial asset
	initialData := []byte("initial data")
	err = engine.PutAsset(ctx, assetRef, "text/plain", initialData)
	require.NoError(t, err)

	// Verify initial asset
	req := &v2.AssetServiceGetAssetRequest{Asset: assetRef}
	contentType, reader, err := engine.GetAsset(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", contentType)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, initialData, data)

	// Overwrite with new data and content type
	newData := []byte(`{"updated": true}`)
	err = engine.PutAsset(ctx, assetRef, "application/json", newData)
	require.NoError(t, err)

	// Verify updated asset
	contentType, reader, err = engine.GetAsset(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "application/json", contentType)

	data, err = io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, newData, data)
}

func TestPebbleEngine_AssetMemoryManagement(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	// Test with moderately large asset (1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	assetRef := &v2.AssetRef{Id: "large-asset"}
	err = engine.PutAsset(ctx, assetRef, "application/octet-stream", largeData)
	require.NoError(t, err)

	// Retrieve the asset
	req := &v2.AssetServiceGetAssetRequest{Asset: assetRef}
	contentType, reader, err := engine.GetAsset(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "application/octet-stream", contentType)

	// Read in chunks to test memory efficiency
	const chunkSize = 4096
	var retrievedData []byte
	buf := make([]byte, chunkSize)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			retrievedData = append(retrievedData, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, largeData, retrievedData)
}

func TestPebbleEngine_AssetContentTypeHandling(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, syncID)

	// Test various content types
	contentTypes := []string{
		"text/plain",
		"text/html",
		"application/json",
		"application/xml",
		"application/pdf",
		"image/png",
		"image/jpeg",
		"image/gif",
		"video/mp4",
		"audio/mpeg",
		"application/octet-stream",
		"application/x-custom-type",
		"text/plain; charset=utf-8",
		"application/json; charset=utf-8",
	}

	data := []byte("test data")

	for i, contentType := range contentTypes {
		assetID := fmt.Sprintf("content-type-test-%d", i)
		assetRef := &v2.AssetRef{Id: assetID}

		// Store asset
		err := engine.PutAsset(ctx, assetRef, contentType, data)
		require.NoError(t, err, "Failed to store asset with content type: %s", contentType)

		// Retrieve asset
		req := &v2.AssetServiceGetAssetRequest{Asset: assetRef}
		retrievedContentType, reader, err := engine.GetAsset(ctx, req)
		require.NoError(t, err, "Failed to retrieve asset with content type: %s", contentType)

		assert.Equal(t, contentType, retrievedContentType, "Content type mismatch")

		retrievedData, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, data, retrievedData)
	}
}

func TestPebbleEngine_AssetSyncIsolation(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start first sync
	syncID1, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Store asset in first sync
	assetRef := &v2.AssetRef{Id: "sync-isolation-test"}
	data1 := []byte("data from sync 1")
	err = engine.PutAsset(ctx, assetRef, "text/plain", data1)
	require.NoError(t, err)

	// End first sync
	err = engine.EndSync(ctx)
	require.NoError(t, err)

	// Start second sync
	syncID2, err := engine.StartNewSync(ctx)
	require.NoError(t, err)
	require.NotEqual(t, syncID1, syncID2)

	// Store asset with same ID in second sync
	data2 := []byte("data from sync 2")
	err = engine.PutAsset(ctx, assetRef, "application/json", data2)
	require.NoError(t, err)

	// Verify we get data from current sync (sync 2)
	req := &v2.AssetServiceGetAssetRequest{Asset: assetRef}
	contentType, reader, err := engine.GetAsset(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "application/json", contentType)

	retrievedData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, data2, retrievedData)

	// TODO: Add test for viewing specific sync when ViewSync is implemented
}
