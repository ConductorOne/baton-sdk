package secretcontracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// errInvalidUTF8 stops a value that encoding/json would silently rewrite.
var errInvalidUTF8 = errors.New("invalid UTF-8")

// Payload is a validated secret value with the content type it was validated
// as. Seal Bytes unchanged and declare ContentType with them.
type Payload struct {
	ContentType string
	Bytes       []byte
}

// EncodeJSON builds the stored document for a JSON type from field values,
// each a string or []string. Blank optional strings and empty optional lists
// are omitted, keys follow the type's declared order, and the result passes
// ValidateProducer.
func EncodeJSON(contentType string, fields map[string]any) (Payload, error) {
	b, err := bundle()
	if err != nil {
		return Payload{}, err
	}
	c, ok := b.contracts[contentType]
	if !ok {
		return Payload{}, newError(LayerUnknownType, "unknown_content_type", "")
	}
	if c.Codec != "json_v1" {
		return Payload{}, newError(LayerInput, "not_a_json_type", "")
	}
	for name := range fields {
		if _, declared := c.properties[name]; !declared {
			return Payload{}, newError(LayerInput, "unknown_field", name)
		}
	}
	var out bytes.Buffer
	out.WriteByte('{')
	for _, name := range c.FieldOrder {
		value, present := fields[name]
		if !present {
			continue
		}
		encoded, omit, err := encodeValue(c.properties[name].isList, value)
		if errors.Is(err, errInvalidUTF8) {
			return Payload{}, newError(LayerInput, "invalid_utf8", name)
		}
		if err != nil {
			return Payload{}, newError(LayerInput, "wrong_type", name)
		}
		if omit && !c.required[name] {
			continue
		}
		if out.Len() > 1 {
			out.WriteByte(',')
		}
		key, _ := marshalNoEscape(name)
		out.Write(key)
		out.WriteByte(':')
		out.Write(encoded)
	}
	out.WriteByte('}')
	return finish(contentType, out.Bytes())
}

// EncodeText builds the stored value for a text type: the value's UTF-8
// bytes, verbatim.
func EncodeText(contentType, value string) (Payload, error) {
	b, err := bundle()
	if err != nil {
		return Payload{}, err
	}
	c, ok := b.contracts[contentType]
	if !ok {
		return Payload{}, newError(LayerUnknownType, "unknown_content_type", "")
	}
	if c.Codec != "utf8_text_v1" {
		return Payload{}, newError(LayerInput, "not_a_text_type", "")
	}
	if !utf8.ValidString(value) {
		return Payload{}, newError(LayerInput, "invalid_utf8", c.TextCodec.Field)
	}
	return finish(contentType, []byte(value))
}

func finish(contentType string, encoded []byte) (Payload, error) {
	if err := ValidateProducer(contentType, encoded); err != nil {
		return Payload{}, err
	}
	return Payload{ContentType: contentType, Bytes: encoded}, nil
}

// encodeValue returns the JSON for a field value and whether the value is
// blank, which an optional field omits.
func encodeValue(isList bool, value any) ([]byte, bool, error) {
	if isList {
		items, ok := value.([]string)
		if !ok {
			return nil, false, fmt.Errorf("not a string list")
		}
		for _, item := range items {
			if !utf8.ValidString(item) {
				return nil, false, errInvalidUTF8
			}
		}
		encoded, err := marshalNoEscape(items)
		return encoded, len(items) == 0, err
	}
	text, ok := value.(string)
	if !ok {
		return nil, false, fmt.Errorf("not a string")
	}
	if !utf8.ValidString(text) {
		return nil, false, errInvalidUTF8
	}
	encoded, err := marshalNoEscape(text)
	return encoded, strings.TrimSpace(text) == "", err
}

func marshalNoEscape(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}
