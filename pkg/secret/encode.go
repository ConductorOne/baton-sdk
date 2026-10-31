package secret

import (
	"encoding/json"
)

// Payload carries serialized bytes and the content type to declare with them.
type Payload struct {
	ContentType string
	Bytes       []byte
}

// EncodeJSON serializes fields without validating their schema or content type.
func EncodeJSON(contentType string, fields map[string]any) (Payload, error) {
	return encodeJSON(contentType, fields)
}

func encodeJSON(contentType string, fields any) (Payload, error) {
	encoded, err := json.Marshal(fields)
	if err != nil {
		return Payload{}, err
	}
	return Payload{ContentType: contentType, Bytes: encoded}, nil
}

// EncodeText preserves the supplied text bytes and content type.
func EncodeText(contentType, value string) (Payload, error) {
	return Payload{ContentType: contentType, Bytes: []byte(value)}, nil
}
