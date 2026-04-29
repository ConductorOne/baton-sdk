//go:build baton_lambda_support

package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSyncResourceTypeIDs(t *testing.T) {
	t.Run("key absent returns nil", func(t *testing.T) {
		require.Nil(t, parseSyncResourceTypeIDs(map[string]interface{}{}))
	})

	t.Run("key present with values returns ids", func(t *testing.T) {
		m := map[string]interface{}{
			"sync-resource-types": []interface{}{"user", "group"},
		}
		got := parseSyncResourceTypeIDs(m)
		require.Equal(t, []string{"user", "group"}, got)
	})

	t.Run("key present with empty slice returns empty", func(t *testing.T) {
		m := map[string]interface{}{
			"sync-resource-types": []interface{}{},
		}
		got := parseSyncResourceTypeIDs(m)
		require.Empty(t, got)
	})

	t.Run("non-slice value is ignored", func(t *testing.T) {
		m := map[string]interface{}{
			"sync-resource-types": "user",
		}
		require.Nil(t, parseSyncResourceTypeIDs(m))
	})
}
