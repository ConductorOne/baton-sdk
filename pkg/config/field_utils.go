package config

import (
	"encoding/base64"
	"os"
)

// GetFileUpload returns the file upload content from a string field value.
func GetFileUpload(fieldValue string) []byte {
	if fieldValue == "" {
		return []byte{}
	}
	// Check if it's a valid base64 string by trying to decode it
	if decoded, err := base64.StdEncoding.DecodeString(fieldValue); err == nil {
		// If it decodes successfully, it's base64 content
		return decoded
	}

	// If not base64, assume it's a file path and try to read the file
	if _, err := os.Stat(fieldValue); err == nil {
		// File exists, read it
		content, err := os.ReadFile(fieldValue)
		if err != nil {
			panic("error reading file: " + err.Error())
		}
		return content
	}

	// If neither base64 nor file path, return the content as-is :shrug:
	return []byte(fieldValue)
}
