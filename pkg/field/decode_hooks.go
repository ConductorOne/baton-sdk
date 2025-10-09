package field

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"
)

// FileUploadDecodeHook is a mapstructure.DecodeHookFunc that automatically
// converts string values to []byte for file upload fields, supporting:
// 1. Data URLs (data:application/json;base64,<content>)
// 2. File paths (reads file content)
// 3. Raw base64 content
// 4. Direct content
func FileUploadDecodeHook() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		// Only apply to string -> []byte conversions
		if f.Kind() != reflect.String || t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.Uint8 {
			return data, nil
		}

		str, ok := data.(string)
		if !ok {
			return data, nil
		}

		// Use the file upload logic
		return getFileUploadContent(str)
	}
}

// getFileUploadContent returns the file upload content from a string field value.
// It supports multiple input formats with contextual awareness:
// 1. Data URLs (data:application/json;base64,<content>) - for lambda/cloud contexts
// 2. File paths - for service/one-shot contexts
// 3. Raw base64 content - fallback for lambda contexts
// 4. Direct content - fallback for service contexts
func getFileUploadContent(fieldValue string) ([]byte, error) {
	if fieldValue == "" {
		return []byte{}, nil
	}

	// Check for data URL format first (most explicit)
	if strings.HasPrefix(fieldValue, "data:") {
		return parseDataURL(fieldValue)
	}

	// Check if it's a valid base64 string by trying to decode it
	if decoded, err := base64.StdEncoding.DecodeString(fieldValue); err == nil {
		// If it decodes successfully, it's base64 content
		return decoded, nil
	}

	// If not base64, assume it's a file path and try to read the file
	if _, err := os.Stat(fieldValue); err == nil {
		// File exists, read it
		content, err := os.ReadFile(fieldValue)
		if err != nil {
			return nil, fmt.Errorf("error reading file: %v", err)
		}
		return content, nil
	}

	// If neither base64 nor file path, return the content as-is :shrug:
	return []byte(fieldValue), nil
}

// parseDataURL parses a data URL and returns the decoded content.
// Supports formats like: data:application/json;base64,<base64-content>
func parseDataURL(dataURL string) ([]byte, error) {
	parsedURL, err := url.Parse(dataURL)
	if err != nil {
		return nil, fmt.Errorf("invalid data URL: %v", err)
	}

	if parsedURL.Scheme != "data" {
		return nil, fmt.Errorf("expected data URL scheme, got: %s", parsedURL.Scheme)
	}

	// Split the data URL into media type and data
	parts := strings.SplitN(parsedURL.Opaque, ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid data URL format: missing comma separator")
	}

	mediaType := parts[0]
	data := parts[1]

	// Check if it's base64 encoded
	if strings.HasSuffix(mediaType, ";base64") {
		decoded, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 data: %v", err)
		}
		return decoded, nil
	}

	// If not base64, return the data as-is (URL encoded)
	decoded, err := url.QueryUnescape(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode URL data: %v", err)
	}
	return []byte(decoded), nil
}
