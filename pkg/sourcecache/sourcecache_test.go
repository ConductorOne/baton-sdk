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

// The degraded-mode contract: when source cache is disabled the SDK
// installs NoopLookup, and a connector must see a clean miss — never an
// error — so it falls back to full fetch. (Coverage-triage finding F8:
// this block was executed by no suite anywhere.)
func TestNoopLookupAlwaysMissesCleanly(t *testing.T) {
	entry, found, err := NoopLookup{}.LookupPreviousSourceCache(t.Context(), RowKindGrants, "any-scope")
	require.NoError(t, err, "degraded mode must not surface errors to the connector")
	require.False(t, found, "degraded mode must miss so the connector fetches fresh")
	require.Zero(t, entry)
}
