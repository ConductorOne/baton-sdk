package uhttp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Parses the link header and returns a map of rel values to URLs.
func TestParseLinkHeader(t *testing.T) {
	//nolint:revive // This is fine
	// Example link header value: <https://api.github.com/repositories/1300192/issues?page=2>; rel="prev", <https://api.github.com/repositories/1300192/issues?page=4>; rel="next", <https://api.github.com/repositories/1300192/issues?page=515>; rel="last", <https://api.github.com/repositories/1300192/issues?page=1>; rel="first"

	//nolint:revive // This is fine
	header := `<https://api.github.com/repositories/1300192/issues?page=2>; rel="prev", <https://api.github.com/repositories/1300192/issues?page=4>; rel="next", <https://api.github.com/repositories/1300192/issues?page=515>; rel="last", <https://api.github.com/repositories/1300192/issues?page=1>; rel="first"`

	links, err := parseLinkHeader(header)
	require.Nil(t, err)
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=2", links["prev"])
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=4", links["next"])
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=515", links["last"])
	require.Equal(t, "https://api.github.com/repositories/1300192/issues?page=1", links["first"])
}
