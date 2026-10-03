package uhttp

import (
	"strings"
)

var xmlContentTypes []string = []string{
	"text/xml",
	"application/xml",
	"application/soap+xml",
}

func IsJSONContentType(contentType string) bool {
	contentType = strings.TrimSpace(strings.ToLower(contentType))
	return strings.HasPrefix(contentType, "application") &&
		strings.Contains(contentType, "json")
}

func IsXMLContentType(contentType string) bool {
	// there are some janky APIs out there
	normalizedContentType := strings.TrimSpace(strings.ToLower(contentType))

	// Only compare against the media type itself, ignoring any parameters
	// (e.g. "; charset=utf-8; action=..."), so that lookalike media types
	// like "application/soap+xmlrpc" don't get misclassified as XML just
	// because they share a prefix with "application/soap+xml".
	baseContentType, _, _ := strings.Cut(normalizedContentType, ";")
	baseContentType = strings.TrimSpace(baseContentType)

	for _, xmlContentType := range xmlContentTypes {
		if baseContentType == xmlContentType {
			return true
		}
	}
	return false
}
