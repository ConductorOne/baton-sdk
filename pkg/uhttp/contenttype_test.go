package uhttp

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHelpers_IsJSONContentType_Success(t *testing.T) {
	resp := &http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/vdn+json"},
		},
	}
	h := resp.Header.Get("Content-Type")
	require.True(t, IsJSONContentType(h))
}

func TestHelpers_IsJSONContentType_Failure(t *testing.T) {
	resp := &http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/xml"},
		},
	}
	h := resp.Header.Get("Content-Type")
	require.False(t, IsJSONContentType(h))
}
