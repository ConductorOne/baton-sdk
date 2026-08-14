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

// TestCreateCacheKey_HeadersOutsideDefaultSetAreIgnoredByDefault documents
// current, intentional behavior: only the default set affects the key
// unless a caller opts in via extraCacheKeyHeaders. Folding in every header
// unconditionally would key the cache on values that have nothing to do
// with the response (transport-injected headers, tracing IDs, etc.) and
// silently tank the hit rate for every caller who never asked for that.
func TestCreateCacheKey_HeadersOutsideDefaultSetAreIgnoredByDefault(t *testing.T) {
	headers := []string{"Authorization", "X-Api-Version", "X-Tenant-Id", "User-Agent"}
	for _, header := range headers {
		t.Run(header, func(t *testing.T) {
			reqA := newCacheKeyRequest(t, header, "value-a")
			reqB := newCacheKeyRequest(t, header, "value-b")

			keyA, err := CreateCacheKey(reqA)
			require.NoError(t, err)
			keyB, err := CreateCacheKey(reqB)
			require.NoError(t, err)
			require.Equal(t, keyA, keyB, "%s is not in the default set and must not affect the key", header)
		})
	}
}

func TestCreateCacheKey_DefaultHeadersStillChangeKey(t *testing.T) {
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

// TestCreateCacheKey_WithCacheKeyHeadersOptsInAdditionalHeaders is the
// regression test for CE-1056: a caller that knows a header varies the
// response (e.g. Authorization scoping the result set) can now opt that
// header into the key instead of two requests silently colliding. The value
// folded into the key is read from req.Header, same as the default set.
func TestCreateCacheKey_WithCacheKeyHeadersOptsInAdditionalHeaders(t *testing.T) {
	reqA := newCacheKeyRequest(t, "Authorization", "value-a")
	reqB := newCacheKeyRequest(t, "Authorization", "value-b")

	keyA, err := CreateCacheKey(reqA, WithCacheKeyHeaders("Authorization"))
	require.NoError(t, err)
	keyB, err := CreateCacheKey(reqB, WithCacheKeyHeaders("Authorization"))
	require.NoError(t, err)
	require.NotEqual(t, keyA, keyB)
}

// TestCreateCacheKey_WithCacheKeyHeadersOnlyAffectsNamedHeaders confirms
// opting a header in doesn't widen the key to every header on the request --
// a header present on req.Header but absent from the CacheOption still falls
// back to the default-set rule.
func TestCreateCacheKey_WithCacheKeyHeadersOnlyAffectsNamedHeaders(t *testing.T) {
	reqA := newCacheKeyRequest(t, "X-Tenant-Id", "tenant-a")
	reqA.Header.Set("Authorization", "same-token")
	reqB := newCacheKeyRequest(t, "X-Tenant-Id", "tenant-b")
	reqB.Header.Set("Authorization", "same-token")

	extra := WithCacheKeyHeaders("Authorization")
	keyA, err := CreateCacheKey(reqA, extra)
	require.NoError(t, err)
	keyB, err := CreateCacheKey(reqB, extra)
	require.NoError(t, err)
	require.Equal(t, keyA, keyB, "X-Tenant-Id was never opted in, so it must not affect the key")
}

// TestCreateCacheKey_WithCacheKeyHeadersCanonicalizesNames confirms header
// names passed to WithCacheKeyHeaders are treated the same regardless of
// casing.
func TestCreateCacheKey_WithCacheKeyHeadersCanonicalizesNames(t *testing.T) {
	req := newCacheKeyRequest(t, "Authorization", "value-a")

	keyA, err := CreateCacheKey(req, WithCacheKeyHeaders("authorization"))
	require.NoError(t, err)
	keyB, err := CreateCacheKey(req, WithCacheKeyHeaders("Authorization"))
	require.NoError(t, err)
	require.Equal(t, keyA, keyB)
}
