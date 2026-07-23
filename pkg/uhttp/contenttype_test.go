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

func TestHelpers_IsXMLContentType_Success(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
	}{
		{"text/xml", "text/xml"},
		{"application/xml", "application/xml"},
		{"application/soap+xml", "application/soap+xml"},
		{
			"application/soap+xml with charset and action",
			`application/soap+xml; charset=utf-8; action="urn:foo"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				Header: map[string][]string{
					"Content-Type": {tt.contentType},
				},
			}
			h := resp.Header.Get("Content-Type")
			require.True(t, IsXMLContentType(h))
		})
	}
}

func TestHelpers_IsXMLContentType_Failure(t *testing.T) {
	resp := &http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},
	}
	h := resp.Header.Get("Content-Type")
	require.False(t, IsXMLContentType(h))
}

func TestHelpers_IsJSONContentType_ApplicationJSON(t *testing.T) {
	resp := &http.Response{
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},
	}
	h := resp.Header.Get("Content-Type")
	require.True(t, IsJSONContentType(h))
}
