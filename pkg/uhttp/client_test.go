package uhttp

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func newCacheKeyRequest(t *testing.T, headerKey, headerValue string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com/widgets?id=1", nil)
	require.NoError(t, err)
	if headerKey != "" {
		req.Header.Set(headerKey, headerValue)
	}
	return req
}

func TestCreateCacheKey_NilRequest(t *testing.T) {
	_, err := CreateCacheKey(nil)
	require.Error(t, err)
}

func TestCreateCacheKey_IdenticalRequestsMatch(t *testing.T) {
	req1 := newCacheKeyRequest(t, "Accept", "application/json")
	req2 := newCacheKeyRequest(t, "Accept", "application/json")

	key1, err := CreateCacheKey(req1)
	require.NoError(t, err)
	key2, err := CreateCacheKey(req2)
	require.NoError(t, err)
	require.Equal(t, key1, key2)
}

// TestCreateCacheKey_HeaderOutsideOldAllowlistChangesKey guards against the
// bug where only Accept, Content-Type, Cookie, and Range were folded into
// the key: any other differing header (e.g. Authorization) produced an
// identical key and the wrong cached response would be served.
func TestCreateCacheKey_HeaderOutsideOldAllowlistChangesKey(t *testing.T) {
	headers := []string{"Authorization", "X-Api-Version", "X-Tenant-Id"}
	for _, header := range headers {
		t.Run(header, func(t *testing.T) {
			reqA := newCacheKeyRequest(t, header, "value-a")
			reqB := newCacheKeyRequest(t, header, "value-b")

			keyA, err := CreateCacheKey(reqA)
			require.NoError(t, err)
			keyB, err := CreateCacheKey(reqB)
			require.NoError(t, err)
			require.NotEqual(t, keyA, keyB, "requests differing only in %s must not share a cache key", header)
		})
	}
}

func TestCreateCacheKey_PreviouslyAllowlistedHeadersStillChangeKey(t *testing.T) {
	headers := []string{"Accept", "Content-Type", "Cookie", "Range"}
	for _, header := range headers {
		t.Run(header, func(t *testing.T) {
			reqA := newCacheKeyRequest(t, header, "value-a")
			reqB := newCacheKeyRequest(t, header, "value-b")

			keyA, err := CreateCacheKey(reqA)
			require.NoError(t, err)
			keyB, err := CreateCacheKey(reqB)
			require.NoError(t, err)
			require.NotEqual(t, keyA, keyB)
		})
	}
}
