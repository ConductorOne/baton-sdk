package field

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/require"
)

func TestFileUploadDecodeHook(t *testing.T) {
	// Create a temporary file for testing
	tempDir := t.TempDir()
	tempFilePath := filepath.Join(tempDir, "test.txt")
	tempFileContent := []byte("test file content")
	invalidTempFilePath := filepath.Join(tempDir, "invalid.txt")

	err := os.WriteFile(tempFilePath, tempFileContent, 0644)
	require.NoError(t, err)

	tests := []struct {
		name         string
		readFromPath bool
		from         interface{}
		to           interface{}
		expected     interface{}
		expectError  bool
	}{
		{
			name:         "valid file path input, readFromPath is true",
			readFromPath: true,
			from:         tempFilePath,
			to:           []byte{},
			expected:     tempFileContent,
			expectError:  false,
		},
		{
			name:         "invalid file path input, readFromPath is true",
			readFromPath: true,
			from:         invalidTempFilePath,
			to:           []byte{},
			expected:     []byte(nil),
			expectError:  true,
		},
		{
			name:         "raw content input, readFromPath is false",
			readFromPath: false,
			from:         "test content",
			to:           []byte{},
			expected:     []byte("test content"),
			expectError:  false,
		},
		{
			name:         "any file path input, readFromPath is false",
			readFromPath: false,
			from:         invalidTempFilePath, // could be any non-zero string
			to:           []byte{},
			expected:     []byte(invalidTempFilePath), // decodes the input as raw content
			expectError:  false,
		},
		{
			name:         "empty string, readFromPath is false",
			readFromPath: false,
			from:         "",
			to:           []byte{},
			expected:     []byte{},
			expectError:  false,
		},
		{
			name:         "empty string, readFromPath is true",
			readFromPath: true,
			from:         "",
			to:           []byte{},
			expected:     []byte{},
			expectError:  false,
		},
		{
			name:         "valid JSON string content, readFromPath is false",
			readFromPath: false,
			from:         `{"key": "value"}`,
			to:           []byte{},
			expected:     []byte(`{"key": "value"}`),
			expectError:  false,
		},
		{
			name:         "valid base64 encoded content, readFromPath is false",
			readFromPath: false,
			from:         base64.StdEncoding.EncodeToString([]byte("encoded content")),
			to:           []byte{},
			expected:     []byte("encoded content"),
			expectError:  false,
		},
		{
			name:         "valid data URL with base64 JSON, readFromPath is false",
			readFromPath: false,
			from:         "data:application/json;base64," + base64.StdEncoding.EncodeToString([]byte(`{"key": "value"}`)),
			to:           []byte{},
			expected:     []byte(`{"key": "value"}`),
			expectError:  false,
		},
		{
			name:         "error on unsupported data URL MIME type",
			readFromPath: false,
			from:         "data:text/plain;base64," + base64.StdEncoding.EncodeToString([]byte("encoded content")),
			to:           []byte{},
			expected:     []byte(nil),
			expectError:  true,
		},
		{
			name:         "error on unsupported data URL encoding format",
			readFromPath: false,
			from:         "data:application/json,{\"key\":\"value\"}",
			to:           []byte{},
			expected:     []byte(nil),
			expectError:  true,
		},
		{
			name:         "error on invalid base64 encoded data URL content",
			readFromPath: false,
			from:         "data:application/json;base64," + `{"key": "value"}`,
			to:           []byte{},
			expected:     []byte(nil),
			expectError:  true,
		},
		{
			name:         "skip non-string input, readFromPath is false",
			readFromPath: false,
			from:         123,
			to:           []byte{},
			expected:     123,
			expectError:  false,
		},
		{
			name:         "skip string slice destination, readFromPath is false",
			readFromPath: false,
			from:         "test, content",
			to:           []string{},
			expected:     "test, content",
			expectError:  false,
		},
		{
			name:         "skip string destination, readFromPath is false",
			readFromPath: false,
			from:         "test, content",
			to:           "",
			expected:     "test, content",
			expectError:  false,
		},
		{
			name:         "skip int slice destination, readFromPath is false",
			readFromPath: false,
			from:         "1,2,3",
			to:           []int{},
			expected:     "1,2,3",
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mapstructure.DecodeHookExec(
				FileUploadDecodeHook(tt.readFromPath),
				reflect.ValueOf(tt.from),
				reflect.ValueOf(tt.to),
			)
			if !tt.expectError {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestStringToSliceHookFunc(t *testing.T) {
	tests := []struct {
		name     string
		sep      string
		from     interface{}
		to       interface{}
		expected interface{}
	}{
		{
			name:     "comma separated string",
			sep:      ",",
			from:     "a,b,c",
			to:       []string{},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "semicolon separated string",
			sep:      ";",
			from:     "a;b;c",
			to:       []string{},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "space separated string",
			sep:      " ",
			from:     "a b c",
			to:       []string{},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "empty string",
			sep:      ",",
			from:     "",
			to:       []string{},
			expected: []string{},
		},
		{
			name:     "single item",
			sep:      ",",
			from:     "single",
			to:       []string{},
			expected: []string{"single"},
		},
		{
			name:     "string with separator at end",
			sep:      ",",
			from:     "a,b,c,",
			to:       []string{},
			expected: []string{"a", "b", "c", ""},
		},
		{
			name:     "string with separator at beginning",
			sep:      ",",
			from:     ",a,b,c",
			to:       []string{},
			expected: []string{"", "a", "b", "c"},
		},
		{
			name:     "string with multiple consecutive separators",
			sep:      ",",
			from:     "a,,b,c",
			to:       []string{},
			expected: []string{"a", "", "b", "c"},
		},
		{
			name: "string representing ints to slice of signed ints",
			sep:  ",",
			from: "1,2,3",
			to:   []int{},
			// still returns a []string - only splits the string doesn't convert the elements
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "skip non-string inputs",
			sep:      ",",
			from:     123,
			to:       []string{},
			expected: 123,
		},
		{
			name:     "skip non-slice destinations",
			sep:      ",",
			from:     "a,b,c",
			to:       "",
			expected: "a,b,c",
		},
		{
			name:     "skip slice of bytes destinations",
			sep:      ",",
			from:     "a,b,c",
			to:       []byte{},
			expected: "a,b,c",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mapstructure.DecodeHookExec(
				StringToSliceHookFunc(tt.sep),
				reflect.ValueOf(tt.from),
				reflect.ValueOf(tt.to),
			)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
