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

const defaultFileUploadMaxFileSize = 2 * 1024 * 1024

type DecodeHookOption func(*decodeHookConfig)

type decodeHookConfig struct {
	hookFuncs []mapstructure.DecodeHookFunc
}

// ComposeDecodeHookFunc returns a mapstructure.DecodeHookFunc that composes
// the default hook functions with any additional hook functions configured.
func ComposeDecodeHookFunc(opts ...DecodeHookOption) mapstructure.DecodeHookFunc {
	config := &decodeHookConfig{
		hookFuncs: []mapstructure.DecodeHookFunc{
			// default hook functions used by viper
			mapstructure.StringToTimeDurationHookFunc(),
			StringToSliceHookFunc(","),
		},
	}
	for _, opt := range opts {
		opt(config)
	}
	return mapstructure.ComposeDecodeHookFunc(config.hookFuncs...)
}

func WithAdditionalDecodeHooks(funcs ...mapstructure.DecodeHookFunc) DecodeHookOption {
	return func(c *decodeHookConfig) {
		c.hookFuncs = append(c.hookFuncs, funcs...)
	}
}

// FileUploadDecodeMode controls how DecodeFileUploadValue interprets the value.
type FileUploadDecodeMode int

const (
	fileUploadDecodeModeUnspecified FileUploadDecodeMode = iota
	// FileUploadDecodeModeContentOnly decodes the value as file content.
	FileUploadDecodeModeContentOnly
	// FileUploadDecodeModePathOnly reads the value as a filesystem path.
	FileUploadDecodeModePathOnly
)

// FileUploadDecodeOption configures DecodeFileUploadValue.
type FileUploadDecodeOption func(*fileUploadDecodeConfig)

type fileUploadDecodeConfig struct {
	maxDecodedSize int64
	maxFileSize    int64
}

func defaultFileUploadDecodeConfig() *fileUploadDecodeConfig {
	return &fileUploadDecodeConfig{
		maxFileSize: defaultFileUploadMaxFileSize,
	}
}

// WithFileUploadMaxDecodedSize limits decoded content bytes in content-only mode.
// Values less than or equal to zero disable the limit.
func WithFileUploadMaxDecodedSize(maxBytes int64) FileUploadDecodeOption {
	return func(c *fileUploadDecodeConfig) {
		c.maxDecodedSize = maxBytes
	}
}

// DecodeFileUploadValue decodes a single file upload value using the selected mode.
// Content-only mode supports JSON base64 data URLs, raw base64 content, and raw
// unencoded content. Path-only mode reads the named file and never falls back to
// content decoding.
func DecodeFileUploadValue(value string, mode FileUploadDecodeMode, opts ...FileUploadDecodeOption) ([]byte, error) {
	config := defaultFileUploadDecodeConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(config)
		}
	}

	switch mode {
	case FileUploadDecodeModeContentOnly:
		return parseFileContent(value, config)
	case FileUploadDecodeModePathOnly:
		return getFileContentFromPath(value, config.maxFileSize)
	default:
		return nil, fmt.Errorf("unsupported file upload decode mode: %d", mode)
	}
}

// FileUploadDecodeHook returns a mapstructure.DecodeHookFunc that automatically
// converts string values to []byte for file upload fields, supporting:
// 1. File paths (reads file content)
// 2. Data URLs of JSON with base64 encoding (data:application/json;base64,<content>)
// 3. Raw base64 content
// 4. Raw unencoded content.
func FileUploadDecodeHook(readFromPath bool) mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		// Only apply to string -> []byte conversions
		if f.Kind() != reflect.String || t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.Uint8 {
			return data, nil
		}

		str, ok := data.(string)
		if !ok {
			return data, nil
		}

		if readFromPath {
			return DecodeFileUploadValue(str, FileUploadDecodeModePathOnly)
		}

		return DecodeFileUploadValue(str, FileUploadDecodeModeContentOnly)
	}
}

// getFileContentFromPath returns the file content from a path.
func getFileContentFromPath(path string, maxFileSize int64) ([]byte, error) {
	if path == "" {
		// don't error if the path is empty, leave that to the field validation rules
		return []byte{}, nil
	}

	// Check if the file exists
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("cannot access file: %w", err)
	}

	// Check file size limit (2MB)
	if maxFileSize > 0 && fileInfo.Size() > maxFileSize {
		return nil, fmt.Errorf("file too large: %d bytes exceeds limit of %d bytes", fileInfo.Size(), maxFileSize)
	}

	// Read the file
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}
	return content, nil
}

// parseFileContent returns the file upload content from a string field value.
func parseFileContent(data string, config *fileUploadDecodeConfig) ([]byte, error) {
	if data == "" {
		// don't error if the data is empty, leave that to the field validation rules
		return []byte{}, nil
	}

	// Check if it's a data URL first
	if strings.HasPrefix(data, "data:") {
		decoded, err := parseJSONBase64DataURL(data)
		return applyMaxDecodedSize(decoded, err, config.maxDecodedSize)
	}

	// Check if it's a base64 encoded string
	if decoded, err := base64.StdEncoding.DecodeString(data); err == nil {
		return applyMaxDecodedSize(decoded, nil, config.maxDecodedSize)
	}

	// Return the content as-is
	return applyMaxDecodedSize([]byte(data), nil, config.maxDecodedSize)
}

func applyMaxDecodedSize(data []byte, err error, maxDecodedSize int64) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	if maxDecodedSize > 0 && int64(len(data)) > maxDecodedSize {
		return nil, fmt.Errorf("decoded file upload content too large: %d bytes exceeds limit of %d bytes", len(data), maxDecodedSize)
	}
	return data, nil
}

// parseJSONBase64DataURL parses a data URL and returns the decoded content.
// Errors if the data is not MIME type application/json and base64 encoded.
func parseJSONBase64DataURL(dataURL string) ([]byte, error) {
	parsedURL, err := url.Parse(dataURL)
	if err != nil {
		return nil, fmt.Errorf("invalid data URL: %w", err)
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

	// Check if it's base64 encoded and MIME type application/json
	if !strings.HasSuffix(mediaType, ";base64") {
		return nil, fmt.Errorf("expected base64 data, got: %s", mediaType)
	}
	if !strings.HasPrefix(mediaType, "application/json") {
		return nil, fmt.Errorf("expected MIME type application/json, got: %s", mediaType)
	}

	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 data: %w", err)
	}
	return decoded, nil
}

// StringToSliceHookFunc returns a DecodeHookFunc that converts
// string to []string by splitting on the given sep.
// Note: this differs from mapstructure.StringToSliceHookFunc only in that it
// skips cases when the target type is []uint8 (ie []byte).
func StringToSliceHookFunc(sep string) mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String || t.Kind() != reflect.Slice || t.Elem().Kind() == reflect.Uint8 {
			return data, nil
		}

		raw := data.(string)
		if raw == "" {
			return []string{}, nil
		}

		return strings.Split(raw, sep), nil
	}
}
