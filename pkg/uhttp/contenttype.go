package uhttp

import "strings"

var xmlContentTypes []string = []string{
	"text/xml",
	"application/xml",
}

func IsJSONContentType(contentType string) bool {
	if !strings.HasPrefix(contentType, "application") {
		return false
	}

	if !strings.Contains(contentType, "json") {
		return false
	}

	return true
}

func IsXMLContentType(contentType string) bool {
	// there are some janky APIs out there
	normalizedContentType := strings.TrimSpace(strings.ToLower(contentType))

	for _, xmlContentType := range xmlContentTypes {
		if strings.HasPrefix(normalizedContentType, xmlContentType) {
			return true
		}
	}
	return false
}
