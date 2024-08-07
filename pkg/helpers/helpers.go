package helpers

import (
	"net/http"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/ratelimit"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

// Deprecated: see resource.SplitFullName.
func SplitFullName(name string) (string, string) {
	return resource.SplitFullName(name)
}

// Deprecated: see ratelimit.ExtractRateLimitData.
func ExtractRateLimitData(statusCode int, header *http.Header) (*v2.RateLimitDescription, error) {
	return ratelimit.ExtractRateLimitData(statusCode, header)
}

// Deprecated: see contenttype.IsJSONContentType.
func IsJSONContentType(contentType string) bool {
	return uhttp.IsJSONContentType(contentType)
}

// Deprecated: see contenttype.IsXMLContentType.
func IsXMLContentType(contentType string) bool {
	return uhttp.IsXMLContentType(contentType)
}
