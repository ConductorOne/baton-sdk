package sourcecache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidationAndHashScope(t *testing.T) {
	require.NoError(t, ValidateRowKind(RowKindResources))
	require.Error(t, ValidateRowKind(RowKind("unknown")))
	require.Error(t, ValidateScopeKey(""))
	require.Error(t, ValidateScopeKey(strings.Repeat("x", maxScopeKeyLen+1)))
	require.NoError(t, ValidateScopeKey("groups/123/members"))
	require.Equal(t, HashScope("groups/123/members"), HashScope("groups/123/members"))
	require.NotEqual(t, HashScope("groups/123/members"), HashScope("groups/124/members"))
}
