package sourcecache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildParseKey(t *testing.T) {
	scopeHash := strings.Repeat("a", 64)
	etag := `W/"abc:123"`

	key, err := BuildKey(scopeHash, etag)
	require.NoError(t, err)
	require.Equal(t, "v2:"+scopeHash+":Vy8iYWJjOjEyMyI", key)

	gotScopeHash, gotETag, err := ParseKey(key)
	require.NoError(t, err)
	require.Equal(t, scopeHash, gotScopeHash)
	require.Equal(t, etag, gotETag)
}

func TestBuildKeyValidation(t *testing.T) {
	_, err := BuildKey("not-hex", `"etag"`)
	require.Error(t, err)

	_, err = BuildKey(strings.Repeat("a", 64), "")
	require.Error(t, err)
}

func TestParseKeyValidation(t *testing.T) {
	_, _, err := ParseKey("v1:" + strings.Repeat("a", 64) + ":ZXRhZw")
	require.Error(t, err)

	_, _, err = ParseKey("v2:not-hex:ZXRhZw")
	require.Error(t, err)

	_, _, err = ParseKey("v2:" + strings.Repeat("a", 64) + ":%%%")
	require.Error(t, err)
}
