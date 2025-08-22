package pebble

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaginator_EncodeDecodeToken(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync to have a valid sync ID
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	paginator := engine.paginator

	// Test basic token encoding/decoding
	testKey := engine.keyEncoder.EncodeResourceTypeKey(syncID, "test-resource-type")

	token, err := paginator.EncodeToken(testKey)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	// Decode the token
	decodedToken, err := paginator.DecodeToken(token)
	require.NoError(t, err)
	require.NotNil(t, decodedToken)

	// Verify token contents
	assert.Equal(t, testKey, decodedToken.LastKey)
}

func TestPaginator_ValidatePageSize(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	paginator := engine.paginator

	tests := []struct {
		name     string
		input    uint32
		expected uint32
	}{
		{"zero size", 0, DefaultPageSize},
		{"normal size", 50, 50},
		{"max size", MaxPageSize, MaxPageSize},
		{"over max size", MaxPageSize + 100, MaxPageSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := paginator.ValidatePageSize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPaginator_CreateStartKey(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync to have a valid sync ID
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	paginator := engine.paginator

	// Test with no token (should return prefix)
	prefixKey := engine.keyEncoder.EncodePrefixKey(KeyTypeResourceType, syncID)
	startKey := paginator.CreateStartKey(nil, prefixKey)
	assert.Equal(t, prefixKey, startKey)

	// Test with token (should return next key after last key)
	lastKey := engine.keyEncoder.EncodeResourceTypeKey(syncID, "test-resource-type")
	token := &PaginationToken{
		LastKey: lastKey,
	}

	startKey = paginator.CreateStartKey(token, prefixKey)
	assert.NotEqual(t, lastKey, startKey)
	assert.True(t, len(startKey) >= len(lastKey))
}

func TestPaginator_CreateUpperBound(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	paginator := engine.paginator

	// Test normal case
	prefixKey := []byte{0x01, 0x00, 0x01, 0x00, 0x74, 0x65, 0x73, 0x74}
	upperBound := paginator.CreateUpperBound(prefixKey)
	assert.True(t, len(upperBound) <= len(prefixKey)+1)

	// Test with all 0xFF bytes
	allFF := []byte{0xFF, 0xFF, 0xFF}
	upperBound = paginator.CreateUpperBound(allFF)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0x00}, upperBound)
}

func TestPagination_IntegrationWithListOperations(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	_, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	// Create test data
	resourceTypes := make([]*v2.ResourceType, 5)
	for i := 0; i < 5; i++ {
		resourceTypes[i] = &v2.ResourceType{
			Id:          fmt.Sprintf("resource-type-%02d", i),
			DisplayName: fmt.Sprintf("Resource Type %d", i),
		}
	}

	// Put the resource types
	err = engine.PutResourceTypes(ctx, resourceTypes...)
	require.NoError(t, err)

	// Test pagination with ListResourceTypes
	req := &v2.ResourceTypesServiceListResourceTypesRequest{
		PageSize: 2,
	}

	// First page
	resp1, err := engine.ListResourceTypes(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp1.List, 2)
	require.NotEmpty(t, resp1.NextPageToken)

	// Verify the token can be decoded
	token, err := engine.paginator.DecodeToken(resp1.NextPageToken)
	require.NoError(t, err)
	assert.NotEmpty(t, token.LastKey)

	// Second page
	req.PageToken = resp1.NextPageToken
	resp2, err := engine.ListResourceTypes(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp2.List, 2)
	require.NotEmpty(t, resp2.NextPageToken)

	// Third page (should have remaining item)
	req.PageToken = resp2.NextPageToken
	resp3, err := engine.ListResourceTypes(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp3.List, 1)
	require.Empty(t, resp3.NextPageToken) // No more pages

	// Verify no duplicates across pages
	allIDs := make(map[string]bool)
	for _, rt := range resp1.List {
		allIDs[rt.Id] = true
	}
	for _, rt := range resp2.List {
		require.False(t, allIDs[rt.Id], "Duplicate ID found: %s", rt.Id)
		allIDs[rt.Id] = true
	}
	for _, rt := range resp3.List {
		require.False(t, allIDs[rt.Id], "Duplicate ID found: %s", rt.Id)
		allIDs[rt.Id] = true
	}

	// Verify all items were returned
	assert.Len(t, allIDs, 5)
}

func TestPagination_EmptyToken(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Test with empty token
	decodedKey, err := engine.decodePageToken("")
	require.NoError(t, err)
	assert.Nil(t, decodedKey)
}

func TestPagination_ErrorHandling(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	paginator := engine.paginator

	// Test invalid token strings
	invalidTokens := []string{
		"invalid-base64!@#",
		"dGVzdA==", // Valid base64 but will decode to "test" which is a valid key format for testing
	}

	for i, token := range invalidTokens {
		t.Run(fmt.Sprintf("invalid token %d", i), func(t *testing.T) {
			decoded, err := paginator.DecodeToken(token)
			if token == "dGVzdA==" {
				// This will decode to "test" which is too short to be a valid key
				// but the DecodeToken method only checks if the token is empty
				assert.NoError(t, err)
				assert.Equal(t, []byte("test"), decoded.LastKey)
			} else {
				assert.Error(t, err)
				assert.Nil(t, decoded)
			}
		})
	}

	// Test empty token - should return nil without error
	token, err := paginator.DecodeToken("")
	assert.NoError(t, err)
	assert.Nil(t, token)
}

func TestPagination_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	paginator := engine.paginator

	// Test concurrent token encoding/decoding
	const numGoroutines = 10
	const tokensPerGoroutine = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*tokensPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < tokensPerGoroutine; j++ {
				// Create unique test key for this goroutine and iteration
				testKey := engine.keyEncoder.EncodeResourceTypeKey(syncID, fmt.Sprintf("test-%d-%d", goroutineID, j))

				// Encode token
				token, err := paginator.EncodeToken(testKey)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d, iteration %d: encode error: %w", goroutineID, j, err)
					return
				}

				// Decode token
				decodedToken, err := paginator.DecodeToken(token)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d, iteration %d: decode error: %w", goroutineID, j, err)
					return
				}

				// Verify token contents
				if string(decodedToken.LastKey) != string(testKey) {
					errors <- fmt.Errorf("goroutine %d, iteration %d: key mismatch", goroutineID, j)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		t.Error(err)
	}
}

func TestPagination_BoundaryConditions(t *testing.T) {
	ctx := context.Background()
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	// Start a sync
	syncID, err := engine.StartNewSync(ctx)
	require.NoError(t, err)

	paginator := engine.paginator

	// Test boundary conditions for key creation
	tests := []struct {
		name        string
		key         []byte
		description string
	}{
		{
			name:        "single byte key",
			key:         []byte{0x01},
			description: "minimal key",
		},
		{
			name:        "key with 0xFF bytes",
			key:         []byte{0xFF, 0xFF, 0x01},
			description: "key with maximum bytes that need overflow handling",
		},
		{
			name:        "all 0xFF bytes",
			key:         []byte{0xFF, 0xFF, 0xFF},
			description: "key that requires appending for next key",
		},
		{
			name:        "normal resource key",
			key:         engine.keyEncoder.EncodeResourceTypeKey(syncID, "boundary-test"),
			description: "typical resource key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test token encoding/decoding
			token, err := paginator.EncodeToken(tt.key)
			require.NoError(t, err, "Failed to encode token for %s", tt.description)
			require.NotEmpty(t, token, "Token should not be empty for %s", tt.description)

			// Decode the token
			decodedToken, err := paginator.DecodeToken(token)
			require.NoError(t, err, "Failed to decode token for %s", tt.description)
			require.Equal(t, tt.key, decodedToken.LastKey, "Key mismatch for %s", tt.description)

			// Test next key creation
			nextKey := paginator.createNextKey(tt.key)
			require.NotNil(t, nextKey, "Next key should not be nil for %s", tt.description)
			require.True(t, len(nextKey) >= len(tt.key), "Next key should be at least as long as original for %s", tt.description)

			// Verify next key is lexicographically greater (using bytes.Compare for proper byte comparison)
			require.True(t, bytes.Compare(nextKey, tt.key) > 0, "Next key should be greater than original for %s", tt.description)

			// Test start key creation with token
			prefixKey := []byte("prefix")
			startKey := paginator.CreateStartKey(decodedToken, prefixKey)
			require.Equal(t, nextKey, startKey, "Start key should equal next key for %s", tt.description)

			// Test start key creation without token
			startKeyNoToken := paginator.CreateStartKey(nil, prefixKey)
			require.Equal(t, prefixKey, startKeyNoToken, "Start key should equal prefix when no token for %s", tt.description)
		})
	}
}
